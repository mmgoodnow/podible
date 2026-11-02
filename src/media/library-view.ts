import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";

const contentMarker = ".podible-content.json";
const manifestName = ".podible-view.json";
const pendingName = ".podible-view-pending.json";
const manifestSchema = z
  .object({
    contentId: z.string().uuid(),
    files: z.record(z.string(), z.array(z.string())),
  })
  .strict();
export type LibraryViewEntry = { relativePath: string; contentPath: string };

function contained(root: string, relative: string): string {
  if (
    !relative ||
    path.isAbsolute(relative) ||
    relative.includes("\\") ||
    relative.includes("\0") ||
    relative
      .split("/")
      .some(
        (part) =>
          !part || part === "." || part === ".." || part.startsWith(".podible"),
      )
  ) {
    throw new Error(`Unsafe view path: ${relative}`);
  }
  const result = path.resolve(root, relative);
  if (!result.startsWith(`${path.resolve(root)}${path.sep}`))
    throw new Error("Path escaped managed root");
  return result;
}

async function stat(file: string) {
  try {
    return await fs.lstat(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function directory(root: string, relative = "") {
  const parts = path.resolve(root).split(path.sep).filter(Boolean);
  if (relative) parts.push(...relative.split("/"));
  let current = path.parse(path.resolve(root)).root;
  for (const part of parts) {
    current = path.join(current, part);
    const existing = await stat(current);
    if (existing && (!existing.isDirectory() || existing.isSymbolicLink()))
      throw new Error(`Not a real directory: ${current}`);
    if (!existing) await fs.mkdir(current);
  }
}

async function readJson(file: string): Promise<unknown | null> {
  const existing = await stat(file);
  if (!existing) return null;
  if (!existing.isFile() || existing.isSymbolicLink())
    throw new Error(`Invalid ownership record: ${file}`);
  return JSON.parse(await fs.readFile(file, "utf8"));
}

async function writeJson(file: string, value: unknown) {
  const temporary = `${file}-${randomUUID()}`;
  const handle = await fs.open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(value));
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.rename(temporary, file);
  const parent = await fs.open(path.dirname(file), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

/** Explicit initialization only; never silently adopt a preexisting media tree. */
export async function initializeManagedContent(root: string) {
  await directory(root);
  const current = await readJson(path.join(root, contentMarker));
  if (current !== null)
    return z.object({ id: z.string().uuid() }).parse(current).id;
  if ((await fs.readdir(root)).length)
    throw new Error("Content root is not empty and has no ownership marker");
  const id = randomUUID();
  await fs.writeFile(path.join(root, contentMarker), JSON.stringify({ id }), {
    flag: "wx",
  });
  return id;
}

/** Callers serialize reconciliation for a view root. No target bytes are modified. */
export async function reconcileLibraryView(input: {
  contentRoot: string;
  viewRoot: string;
  entries: LibraryViewEntry[];
  onLinkApplied?: (relativePath: string) => void | Promise<void>;
}) {
  const contentRoot = path.resolve(input.contentRoot);
  const viewRoot = path.resolve(input.viewRoot);
  const content = z
    .object({ id: z.string().uuid() })
    .parse(await readJson(path.join(contentRoot, contentMarker)));
  if (
    contentRoot === viewRoot ||
    contentRoot.startsWith(`${viewRoot}${path.sep}`) ||
    viewRoot.startsWith(`${contentRoot}${path.sep}`)
  )
    throw new Error("Content and view roots must be separate");
  // Verify the mounted root, not merely that an empty mountpoint directory exists.
  if ((await fs.realpath(contentRoot)) !== contentRoot)
    throw new Error("Content root traverses a symlink");
  await directory(viewRoot);
  const previous = manifestSchema.parse(
    (await readJson(path.join(viewRoot, manifestName))) ?? {
      contentId: content.id,
      files: {},
    },
  );
  const pendingRaw = await readJson(path.join(viewRoot, pendingName));
  const pending = pendingRaw === null ? null : manifestSchema.parse(pendingRaw);
  if (
    previous.contentId !== content.id ||
    (pending && pending.contentId !== content.id)
  )
    throw new Error("Managed content mount identity changed");
  const files: Record<string, string> = {};
  const seen = new Set<string>();
  for (const entry of input.entries) {
    contained(viewRoot, entry.relativePath);
    const collisionKey = entry.relativePath.normalize("NFC").toLowerCase();
    if (seen.has(collisionKey))
      throw new Error(`Duplicate view path: ${entry.relativePath}`);
    seen.add(collisionKey);
    const sourceRelative = path.relative(
      contentRoot,
      path.resolve(entry.contentPath),
    );
    const source = contained(contentRoot, sourceRelative);
    if (
      (await fs.realpath(source)) !== source ||
      !(await fs.stat(source)).isFile()
    )
      throw new Error("Source must be a canonical regular file");
    files[entry.relativePath] = sourceRelative;
  }
  const knownPaths = new Set([
    ...Object.keys(previous.files),
    ...Object.keys(pending?.files ?? {}),
    ...Object.keys(files),
  ]);
  const observed = new Map<string, string | null>();
  // Preflight every path before changing any link, including retired entries.
  for (const relative of knownPaths) {
    const destination = contained(viewRoot, relative);
    const parent = path.posix.dirname(relative);
    if (parent !== ".") await directory(viewRoot, parent);
    const existing = await stat(destination);
    let target: string | null = null;
    if (existing) {
      if (!existing.isSymbolicLink())
        throw new Error(`Refusing to replace a regular file: ${relative}`);
      target = path.resolve(
        path.dirname(destination),
        await fs.readlink(destination),
      );
      const authorized = [
        ...(previous.files[relative] ?? []),
        ...(pending?.files[relative] ?? []),
      ].map((value) => contained(contentRoot, value));
      if (!authorized.includes(target))
        throw new Error(`Foreign or changed symlink: ${relative}`);
    }
    observed.set(relative, target);
  }
  const owned = Object.fromEntries(
    [...knownPaths].map((relative) => [
      relative,
      [
        ...new Set([
          ...(previous.files[relative] ?? []),
          ...(pending?.files[relative] ?? []),
          ...(files[relative] === undefined ? [] : [files[relative]]),
        ]),
      ],
    ]),
  );
  await writeJson(path.join(viewRoot, pendingName), {
    contentId: content.id,
    files: owned,
  });
  const assertUnchanged = async (relative: string) => {
    const destination = contained(viewRoot, relative);
    const parent = path.posix.dirname(relative);
    if (parent !== ".") await directory(viewRoot, parent);
    const current = await stat(destination);
    if (!current && observed.get(relative) === null) return;
    if (
      !current?.isSymbolicLink() ||
      path.resolve(
        path.dirname(destination),
        await fs.readlink(destination),
      ) !== observed.get(relative)
    ) {
      throw new Error(`View changed during reconciliation: ${relative}`);
    }
  };
  let linked = 0;
  let removed = 0;
  for (const [relative, sourceRelative] of Object.entries(files)) {
    const destination = contained(viewRoot, relative);
    const target = contained(contentRoot, sourceRelative);
    if (observed.get(relative) === target) continue;
    const temporary = path.join(
      path.dirname(destination),
      `.podible-link-${randomUUID()}`,
    );
    await fs.symlink(
      path.relative(path.dirname(destination), target),
      temporary,
    );
    await assertUnchanged(relative);
    await fs.rename(temporary, destination);
    linked++;
    await input.onLinkApplied?.(relative);
  }
  for (const relative of knownPaths) {
    if (files[relative] !== undefined || observed.get(relative) === null)
      continue;
    await assertUnchanged(relative);
    await fs.unlink(contained(viewRoot, relative));
    removed++;
  }
  await writeJson(path.join(viewRoot, manifestName), {
    contentId: content.id,
    files: Object.fromEntries(
      Object.entries(files).map(([key, value]) => [key, [value]]),
    ),
  });
  await fs.unlink(path.join(viewRoot, pendingName));
  return { linked, removed, unchanged: Object.keys(files).length - linked };
}
