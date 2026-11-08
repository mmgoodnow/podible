import { createHash, randomUUID } from "node:crypto";
import { constants, promises as fs, type BigIntStats } from "node:fs";
import path from "node:path";
import { withKeyedMutex } from "../runtime/keyed-mutex";
import { finalizedCanonicalObject } from "../media/canonical-download";
import { z } from "zod";
import {
  initializeManagedContent,
  reconcileLibraryView,
  type LibraryViewEntry,
} from "../media/library-view";

type ViewFile = {
  id: number;
  path: string;
  source_path?: string | null;
  identity_path?: string | null;
  start?: number;
};
export type BookStorageRepo = {
  listAllBooks(): readonly { id: number; title: string; author: string }[];
  listAssetsByBook(bookId: number): readonly {
    id: number;
    manifestation_id: number | null;
  }[];
  getAssetFiles(assetId: number): readonly ViewFile[];
};

const markerName = ".podible-content.json";
const recordName = ".podible-file.json";
const markerSchema = z.object({ id: z.string().uuid() });
const recordSchema = z.object({
  dev: z.string().regex(/^\d+$/),
  ino: z.string().regex(/^\d+$/),
  size: z.string().regex(/^\d+$/),
  mtimeNs: z.string().regex(/^-?\d+$/),
  name: z.string().regex(/^content(?:\.[a-z0-9]{1,16})?$/),
});
const roots = new Map<string, string>();
const owners = new Map<string, string>();

async function serialized<T>(key: string, action: () => Promise<T>): Promise<T> {
  return withKeyedMutex(key.startsWith("view:")
    ? { kind: "publication", key: path.resolve(key.slice(5)) }
    : { kind: "storage", key }, action);
}

async function lstat(file: string) {
  try {
    return await fs.lstat(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function directory(directoryPath: string) {
  let created = false;
  try {
    await fs.mkdir(directoryPath);
    created = true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const info = await fs.lstat(directoryPath);
  if (!info.isDirectory() || (await fs.realpath(directoryPath)) !== directoryPath)
    throw new Error(`Managed book directory traverses a symlink: ${directoryPath}`);
  if (created) await syncDirectory(path.dirname(directoryPath));
}

async function readRecord(file: string): Promise<unknown> {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    if (!(await handle.stat()).isFile()) throw new Error(`Invalid ownership record: ${file}`);
    return JSON.parse(await handle.readFile("utf8"));
  } finally {
    await handle.close();
  }
}

async function syncDirectory(directoryPath: string) {
  const handle = await fs.open(directoryPath, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function assertOwner(contentRoot: string) {
  if ((await fs.realpath(contentRoot)) !== contentRoot)
    throw new Error("Book content root traverses a symlink");
  const { id } = markerSchema.parse(await readRecord(path.join(contentRoot, markerName)));
  const expected = owners.get(contentRoot);
  if (expected && expected !== id) throw new Error("Book content ownership marker changed");
  return id;
}

/** Pure path derivation; async preparation resolves and remembers library-root aliases. */
export function bookContentRoot(libraryRoot: string): string {
  const resolved = path.resolve(libraryRoot);
  return path.join(roots.get(resolved) ?? resolved, ".podible", "content");
}

/** Returns the real content root. Only the configured library root may be an alias. */
export async function prepareBookStorage(libraryRoot: string): Promise<string> {
  const requested = path.resolve(libraryRoot);
  await fs.mkdir(requested, { recursive: true });
  const realRoot = await fs.realpath(requested);
  if (roots.has(requested) && roots.get(requested) !== realRoot)
    throw new Error("Book library root alias changed");
  const contentRoot = path.join(realRoot, ".podible", "content");
  await serialized(`prepare:${contentRoot}`, async () => {
    await directory(path.join(realRoot, ".podible"));
    await directory(contentRoot);
    if (owners.has(contentRoot)) {
      await assertOwner(contentRoot);
    } else {
      // initializeManagedContent refuses to take ownership of a nonempty foreign tree.
      await initializeManagedContent(contentRoot);
      const id = await assertOwner(contentRoot);
      owners.set(contentRoot, id);
      await syncDirectory(contentRoot);
    }
    await syncDirectory(path.dirname(contentRoot));
  });
  roots.set(requested, realRoot);
  roots.set(realRoot, realRoot);
  return contentRoot;
}

function fingerprint(info: BigIntStats) {
  return { dev: String(info.dev), ino: String(info.ino), size: String(info.size), mtimeNs: String(info.mtimeNs) };
}

function matches(info: BigIntStats, record: ReturnType<typeof fingerprint>) {
  return info.isFile() && Object.entries(fingerprint(info)).every(([key, value]) => record[key as keyof typeof record] === value);
}

async function assertSource(source: string, before: BigIntStats) {
  if ((await fs.realpath(path.dirname(source))) !== path.dirname(source) || !matches(await fs.lstat(source, { bigint: true }), fingerprint(before)))
    throw new Error("Book source changed or traverses a symlink");
}

async function claimRecord(file: string, record: z.infer<typeof recordSchema>) {
  if (await lstat(file)) return recordSchema.parse(await readRecord(file));
  const temporary = path.join(path.dirname(file), `.podible-claim-${randomUUID()}`);
  const handle = await fs.open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(record));
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    // Atomic, no-overwrite publication: a crash can leave a claim, never partial JSON.
    try {
      await fs.link(temporary, file);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    await syncDirectory(path.dirname(file));
    return recordSchema.parse(await readRecord(file));
  } finally {
    await fs.unlink(temporary);
  }
}

/** Sources must be finalized: hardlinks deliberately share bytes, never copy or chmod them. */
export async function adoptBookContent(libraryRoot: string, sourcePath: string, stableKey?: string): Promise<string> {
  if (stableKey !== undefined && (typeof stableKey !== "string" || !stableKey.length))
    throw new Error("Book content stable key must be a nonempty string");
  const contentRoot = await prepareBookStorage(libraryRoot);
  const source = path.resolve(sourcePath);
  if (await finalizedCanonicalObject(contentRoot, source)) return source;
  // A hardlinked leaf's realpath can name its peer on macOS; validate parents and lstat the leaf.
  if ((await fs.realpath(path.dirname(source))) !== path.dirname(source) || !(await fs.lstat(source)).isFile())
    throw new Error("Book source must be a canonical regular file without symlink traversal");
  const handle = await fs.open(source, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile()) throw new Error("Book source must be a regular file");
    const marker = await fs.lstat(path.join(contentRoot, markerName), { bigint: true });
    if (marker.dev === before.dev && marker.ino === before.ino)
      throw new Error("Cannot adopt a book ownership marker");
    if (source.startsWith(`${contentRoot}${path.sep}`)) {
      const relative = path.relative(contentRoot, source);
      if (!/^files\/[a-f0-9]{2}\/[a-f0-9]{64}\/content(?:\.[a-z0-9]{1,16})?$/.test(relative))
        throw new Error("Source is not owned canonical book content");
      const record = recordSchema.parse(await readRecord(path.join(path.dirname(source), recordName)));
      if (path.basename(source) !== record.name || !matches(before, record))
        throw new Error("Canonical book content identity changed");
      await assertOwner(contentRoot);
      return source;
    }
    const key = createHash("sha256").update(stableKey === undefined
      ? `inode\0${before.dev}:${before.ino}:${before.birthtimeNs}`
      : `key\0${stableKey}`).digest("hex");
    return await serialized(`object:${contentRoot}:${key}`, async () => {
      await assertSource(source, before);
      await assertOwner(contentRoot);
      let parent = contentRoot;
      for (const part of ["files", key.slice(0, 2), key]) {
        parent = path.join(parent, part);
        await directory(parent);
      }
      const extension = path.extname(source).toLowerCase();
      const record = await claimRecord(path.join(parent, recordName), {
        ...fingerprint(before),
        name: `content${/^\.[a-z0-9]{1,16}$/.test(extension) ? extension : ""}`,
      });
      if (!matches(before, record)) throw new Error("Book content identity is occupied by a different or changed file");
      const destination = path.join(parent, record.name);
      await assertOwner(contentRoot);
      await assertSource(source, before);
      try {
        await fs.link(source, destination);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
      const stored = await fs.lstat(destination, { bigint: true });
      if ((await fs.realpath(parent)) !== parent || !matches(stored, record))
        throw new Error("Book content destination collision or changed file");
      await assertSource(source, before);
      if (!matches(await handle.stat({ bigint: true }), record)) throw new Error("Book source changed during adoption");
      await assertOwner(contentRoot);
      await syncDirectory(parent);
      return destination;
    });
  } finally {
    await handle.close();
  }
}

function segment(value: string, fallback: string, limit = 100): string {
  const safe = value.normalize("NFC").replace(/[\\/:*?"<>|\x00-\x1f\x7f]/g, " ").replace(/\s+/g, " ").trim().replace(/[. ]+$/, "");
  let shortened = "";
  for (const character of safe) {
    if (Buffer.byteLength(shortened + character) > limit) break;
    shortened += character;
  }
  shortened = shortened.trim().replace(/[. ]+$/, "") || fallback;
  return shortened.toLowerCase().startsWith(".podible") ? `_${shortened}` : shortened;
}

export function bookViewDirectory(book: { id: number; title: string; author: string }): string {
  return path.posix.join(segment(book.author, "Unknown"), segment(book.title, `book-${book.id}`));
}

function canonicalFile(file: ViewFile, contentRoot: string): boolean {
  if (!path.isAbsolute(file.path) || path.resolve(file.path) !== file.path) return false;
  const relative = path.relative(contentRoot, file.path);
  return !!relative && !relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative)
    && relative.split(path.sep).every((part) => !part.startsWith(".podible"));
}

function comparePath(a: LibraryViewEntry, b: LibraryViewEntry) {
  return a.relativePath < b.relativePath ? -1 : a.relativePath > b.relativePath ? 1 : 0;
}

/** No filesystem reads or repo writes; legacy paths are not presentation targets. */
export function bookViewEntries(repo: BookStorageRepo, libraryRoot: string): LibraryViewEntry[] {
  const contentRoot = bookContentRoot(libraryRoot);
  const entries: LibraryViewEntry[] = [];
  for (const book of repo.listAllBooks()) {
    const folder = bookViewDirectory(book);
    for (const asset of repo.listAssetsByBook(book.id)) {
      const files = [...repo.getAssetFiles(asset.id)].sort((a, b) => (a.start ?? 0) - (b.start ?? 0) || a.id - b.id);
      for (const [index, file] of files.entries()) {
        if (!canonicalFile(file, contentRoot)) continue;
        const original = path.basename(file.identity_path || file.source_path || file.path);
        const extension = path.extname(original) || path.extname(file.path);
        const safeExtension = /^\.[a-zA-Z0-9]{1,16}$/.test(extension) ? extension.toLowerCase() : "";
        const stem = files.length > 1
          ? `${String(index + 1).padStart(Math.max(3, String(files.length).length), "0")} - ${segment(path.basename(original, path.extname(original)), "Part")}`
          : segment(book.title, `book-${book.id}`);
        entries.push({ relativePath: `${folder}/${stem} [m${asset.manifestation_id ?? 0}-a${asset.id}]${safeExtension}`, contentPath: file.path });
      }
    }
  }
  return entries.sort(comparePath);
}

function regularFilesToAdopt(repo: BookStorageRepo, libraryRoot: string): LibraryViewEntry[] {
  const contentRoot = bookContentRoot(libraryRoot);
  const entries = new Map<string, LibraryViewEntry>();
  const aliases = [...roots.entries()].sort(([a], [b]) => b.length - a.length);
  for (const book of repo.listAllBooks()) {
    for (const asset of repo.listAssetsByBook(book.id)) {
      for (const file of repo.getAssetFiles(asset.id)) {
        if (!canonicalFile(file, contentRoot) || !file.identity_path || !path.isAbsolute(file.identity_path)) continue;
        let original = path.resolve(file.identity_path);
        // Translate only roots explicitly resolved during preparation, never arbitrary symlinks.
        for (const [alias, realRoot] of aliases) {
          if (original.startsWith(`${alias}${path.sep}`)) {
            original = path.join(realRoot, path.relative(alias, original));
            break;
          }
        }
        const relative = path.relative(libraryRoot, original);
        if (!relative || original === file.path || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)
          || relative.includes("\\") || relative.includes("\0") || relative.split(path.sep).some((part) => part.startsWith(".podible"))) continue;
        const collisionKey = relative.normalize("NFC").toLowerCase();
        const previous = entries.get(collisionKey);
        if (previous && previous.contentPath !== file.path) throw new Error(`Conflicting legacy book identity: ${relative}`);
        entries.set(collisionKey, { relativePath: relative, contentPath: file.path });
      }
    }
  }
  return [...entries.values()].sort(comparePath);
}

export async function reconcileBookView(repo: BookStorageRepo, libraryRoot: string, assertActive: () => void = () => {}) {
  assertActive();
  const contentRoot = await prepareBookStorage(libraryRoot);
  const viewRoot = path.dirname(path.dirname(contentRoot));
  return serialized(`view:${viewRoot}`, async () => {
    assertActive();
    const input: Parameters<typeof reconcileLibraryView>[0] = {
      contentRoot,
      viewRoot,
      entries: bookViewEntries(repo, viewRoot),
      adoptRegularFiles: regularFilesToAdopt(repo, viewRoot),
      assertActive,
      onLinkApplied: assertActive,
    };
    return reconcileLibraryView(input);
  });
}
