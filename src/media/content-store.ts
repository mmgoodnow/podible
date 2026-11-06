import { constants } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import { finalizeCanonicalDownloadFile, readCanonicalDownload } from "./canonical-download";

async function digest(file: string) {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const hash = createHash("sha256");
    const buffer = Buffer.alloc(1024 * 1024);
    for (;;) {
      const { bytesRead } = await handle.read(buffer);
      if (!bytesRead) return hash.digest("hex");
      hash.update(buffer.subarray(0, bytesRead));
    }
  } finally {
    await handle.close();
  }
}
async function realDirectory(directory: string) {
  try {
    await fs.mkdir(directory);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const stat = await fs.lstat(directory);
  if (!stat.isDirectory() || (await fs.realpath(directory)) !== directory)
    throw new Error("Managed directory traverses a symlink");
}

/** Finalize reservations in place; pre-canonical client layouts retain isolated legacy adoption. */
export async function adoptContentFile(input: {
  contentRoot: string;
  sourceRoot: string;
  sourcePath: string;
  fileId: string;
  expectedSize: number;
  assertActive?: () => void;
}) {
  const active = input.assertActive ?? (() => {});
  const reserved = await readCanonicalDownload(input.contentRoot, input.sourceRoot)
    ? await finalizeCanonicalDownloadFile(input.contentRoot, input.sourceRoot, input.sourcePath, input.expectedSize, active) : null;
  if (reserved) return reserved;
  const id = z.string().uuid().parse(input.fileId);
  const size = z.number().int().nonnegative().safe().parse(input.expectedSize);
  const root = path.resolve(input.contentRoot);
  const sourceRoot = path.resolve(input.sourceRoot);
  const source = path.resolve(input.sourcePath);
  if (
    sourceRoot === root ||
    root.startsWith(`${sourceRoot}${path.sep}`) ||
    sourceRoot.startsWith(`${root}${path.sep}`)
  )
    throw new Error("Content and client layouts must be separate");
  if (
    !source.startsWith(`${sourceRoot}${path.sep}`) ||
    (await fs.realpath(source)) !== source ||
    (await fs.realpath(sourceRoot)) !== sourceRoot
  )
    throw new Error("Source escapes canonical client layout");
  if ((await fs.realpath(root)) !== root)
    throw new Error("Content mount traverses a symlink");
  const marker = path.join(root, ".podible-content.json");
  if (
    !(await fs.lstat(marker)).isFile() ||
    (await fs.lstat(marker)).isSymbolicLink()
  )
    throw new Error("Invalid content ownership marker");
  z.object({ id: z.string().uuid() }).parse(
    JSON.parse(await fs.readFile(marker, "utf8")),
  );
  const extension = path.extname(source).toLowerCase();
  if (![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(extension))
    throw new Error("Unsupported content container");
  active();
  await realDirectory(path.join(root, "files"));
  const directory = path.join(root, "files", id.slice(0, 2));
  await realDirectory(directory);
  const relativePath = `files/${id.slice(0, 2)}/${id}${extension}`;
  const destination = path.join(root, relativePath);
  const handle = await fs.open(
    source,
    constants.O_RDONLY | constants.O_NOFOLLOW,
  );
  const temporary = path.join(directory, `.podible-copy-${randomUUID()}`);
  try {
    const before = await handle.stat();
    if (!before.isFile() || before.size !== size)
      throw new Error("Source size differs from verified metainfo");
    const existing = await fs
      .lstat(destination)
      .catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT") return null;
        throw error;
      });
    if (existing) {
      if (
        !existing.isFile() ||
        existing.isSymbolicLink() ||
        existing.size !== size
      )
        throw new Error(
          "Content identity is already occupied by different bytes",
        );
      const sha256 = await digest(source);
      const after = await handle.stat();
      if (
        after.mtimeMs !== before.mtimeMs ||
        after.ctimeMs !== before.ctimeMs ||
        after.size !== size
      )
        throw new Error("Source changed during adoption");
      if ((await digest(destination)) !== sha256)
        throw new Error(
          "Content identity is already occupied by different bytes",
        );
      active();
      return { id, relativePath, sizeBytes: size, sha256 };
    }
    const space = await fs.statfs(root);
    if (space.bavail * space.bsize < size + 64 * 1024 ** 2)
      throw new Error(
        "Insufficient disk headroom to retain the existing version",
      );
    const output = await fs.open(temporary, "wx", 0o600);
    const hash = createHash("sha256");
    let written = 0;
    try {
      const buffer = Buffer.alloc(1024 * 1024);
      for (;;) {
        active();
        const { bytesRead } = await handle.read(buffer);
        if (!bytesRead) break;
        written += bytesRead;
        if (written > size) throw new Error("Source grew during adoption");
        const bytes = buffer.subarray(0, bytesRead);
        hash.update(bytes);
        let offset = 0;
        while (offset < bytesRead) {
          const result = await output.write(bytes, offset, bytesRead - offset);
          if (!result.bytesWritten)
            throw new Error("Content write made no progress");
          offset += result.bytesWritten;
        }
      }
      const after = await handle.stat();
      if (
        written !== size ||
        before.mtimeMs !== after.mtimeMs ||
        before.ctimeMs !== after.ctimeMs ||
        after.size !== size
      )
        throw new Error("Source changed during adoption");
      await output.sync();
      await output.chmod(0o444);
    } finally {
      await output.close();
    }
    const sha256 = hash.digest("hex");
    active();
    try {
      await fs.link(temporary, destination);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const existing = await fs.lstat(destination);
      if (
        !existing.isFile() ||
        existing.isSymbolicLink() ||
        existing.size !== size ||
        (await digest(destination)) !== sha256
      )
        throw new Error(
          "Content identity is already occupied by different bytes",
        );
    }
    const parent = await fs.open(directory, "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    return { id, relativePath, sizeBytes: size, sha256 };
  } finally {
    await handle.close();
    await fs.unlink(temporary).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}
