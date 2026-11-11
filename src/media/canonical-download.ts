import { constants, promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import { inspectMetainfo } from "./metainfo";
import { torrentFileRole } from "./content-file-role";
import type { TorrentClient, TorrentRef } from "./torrent-client";
import { withKeyedMutex } from "../runtime/keyed-mutex";
import { effectCheckpoint } from "../runtime/execution-effects";

const relative = z.string().min(1).refine(value => !path.isAbsolute(value) &&
  !/[\\\x00-\x1f\x7f]/.test(value) && value.split("/").every(part => part && part !== "." && part !== ".."));
const count = z.number().int().nonnegative().safe();
const schema = z.object({
  version: z.literal(1), clientId: z.string().min(1), infoHash: z.string().regex(/^[a-f0-9]{40}$/),
  contentRoot: z.string(), contentOwner: z.string().uuid(), sourceRoot: z.string(), ready: z.boolean(), sealed: z.boolean(),
  files: z.array(z.object({ path: relative, size: count, id: z.string().uuid(),
    relativePath: relative.nullable(), device: z.string().optional(), inode: z.string().optional() })).min(1).max(100_000),
}).strict();
export type CanonicalDownload = z.infer<typeof schema>;
const manifestPath = (contentRoot: string, sourceRoot: string) => path.join(contentRoot, ".podible-downloads",
  createHash("sha256").update(sourceRoot).digest("hex") + ".json");
async function stat(file: string) {
  try { return await fs.lstat(file, { bigint: true }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
}
async function directory(root: string, relativePath = "", create = false) {
  if (!path.isAbsolute(root) || path.resolve(root) !== root || await fs.realpath(root) !== root)
    throw new Error("Canonical download root traverses a symlink");
  let current = root;
  for (const part of relativePath.split("/").filter(Boolean)) {
    current = path.join(current, part);
    if (create) { try { await fs.mkdir(current); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; } }
    const found = await fs.lstat(current);
    if (!found.isDirectory() || found.isSymbolicLink()) throw new Error("Canonical download ancestor is not a real directory");
  }
}
async function readJson(file: string) {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const found = await handle.stat();
    if (!found.isFile() || found.size > 32 * 1024 ** 2) throw new Error("Invalid canonical download record");
    return JSON.parse(await handle.readFile("utf8"));
  } finally { await handle.close(); }
}
async function sync(directoryPath: string) {
  const handle = await fs.open(directoryPath, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}
async function writeJson(file: string, value: unknown, replace = true) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  const handle = await fs.open(temporary, "wx", 0o600);
  try { await handle.writeFile(JSON.stringify(value)); await handle.sync(); } finally { await handle.close(); }
  try {
    if (replace) await fs.rename(temporary, file);
    else try { await fs.link(temporary, file); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    await sync(path.dirname(file));
  }
  finally { await fs.unlink(temporary).catch(error => { if (error.code !== "ENOENT") throw error; }); }
}
async function owner(root: string) {
  await directory(root);
  return z.object({ id: z.string().uuid() }).parse(await readJson(path.join(root, ".podible-content.json"))).id;
}

/** The intent lives with canonical bytes, not with a disposable torrent view. */
export async function readCanonicalDownload(contentRoot: string, sourceRoot: string): Promise<CanonicalDownload | null> {
  const file = manifestPath(contentRoot, sourceRoot);
  if (!await stat(file)) return null;
  await directory(contentRoot, ".podible-downloads");
  const plan = schema.parse(await readJson(file));
  if (plan.contentRoot !== contentRoot || plan.sourceRoot !== sourceRoot || plan.contentOwner !== await owner(contentRoot) ||
    new Set(plan.files.map(file => file.path)).size !== plan.files.length)
    throw new Error("Canonical download intent identity changed");
  for (const file of plan.files) {
    const expected = `files/${file.id.slice(0, 2)}/${file.id}${path.extname(file.path).toLowerCase()}`;
    if (file.relativePath !== null && file.relativePath !== expected) throw new Error("Canonical reservation escaped its object identity");
  }
  return plan;
}

/** Reserve private writable objects before add. Existing downloads are never converted implicitly. */
type PrepareInput = {
  contentRoot: string; sourceRoot: string; clientId: string; metainfo: Uint8Array;
  allowCreate: boolean; assertActive?: () => void;
};
async function serialized<T>(key: string, action: () => Promise<T>): Promise<T> {
  return withKeyedMutex({ kind: "storage", key: `canonical:${key}` }, action);
}
export function prepareCanonicalDownload(input: PrepareInput): Promise<CanonicalDownload | null> {
  return serialized(input.sourceRoot, () => prepare(input));
}
async function prepare(input: PrepareInput): Promise<CanonicalDownload | null> {
  const { contentRoot, sourceRoot } = input, metadata = inspectMetainfo(input.metainfo);
  const active = input.assertActive ?? (() => {});
  let plan = await readCanonicalDownload(contentRoot, sourceRoot);
  if (!plan && !input.allowCreate) return null;
  await directory(sourceRoot); await directory(contentRoot);
  if (sourceRoot === contentRoot || sourceRoot.startsWith(contentRoot + path.sep) || contentRoot.startsWith(sourceRoot + path.sep))
    throw new Error("Torrent view and canonical bytes must be separate");
  if (!plan) {
    // A reserved book directory contains its own ownership marker and nothing else.
    if ((await fs.readdir(sourceRoot)).some(name => name !== ".podible-book-torrent.json"))
      throw new Error("Refusing to replace an existing torrent layout");
    for (const file of metadata.files) if (file.path.split("/").some(part => part.startsWith(".podible")))
      throw new Error("Metainfo collides with managed layout records");
    plan = { version: 1, clientId: input.clientId, infoHash: metadata.infoHash,
      contentRoot, contentOwner: await owner(contentRoot), sourceRoot, ready: false, sealed: false,
      files: metadata.files.map(file => {
        const id = randomUUID();
        return { ...file, id, relativePath: torrentFileRole(file.path) === "content"
          ? `files/${id.slice(0, 2)}/${id}${path.extname(file.path).toLowerCase()}` : null };
      }) };
    active(); await directory(contentRoot, ".podible-downloads", true);
    await writeJson(manifestPath(contentRoot, sourceRoot), plan, false);
    plan = (await readCanonicalDownload(contentRoot, sourceRoot))!;
  }
  if (plan.clientId !== input.clientId || plan.infoHash !== metadata.infoHash || plan.files.length !== metadata.files.length ||
    plan.files.some((file, index) => file.path !== metadata.files[index]!.path || file.size !== metadata.files[index]!.size))
    throw new Error("Canonical download intent differs from metainfo/client");
  for (const file of plan.files) {
    active();
    await directory(sourceRoot, path.posix.dirname(file.path) === "." ? "" : path.posix.dirname(file.path), true);
    const view = path.join(sourceRoot, file.path);
    if (!file.relativePath) {
      const existing = await stat(view);
      if (existing && (!existing.isFile() || existing.isSymbolicLink())) throw new Error("Invalid torrent-specific sidecar");
      continue;
    }
    await directory(contentRoot, path.posix.dirname(file.relativePath), !plan.ready);
    const canonical = path.join(contentRoot, file.relativePath);
    let existing = await stat(canonical);
    if (!existing && !plan.ready) {
      // Plex may run under a different UID; publication must remain readable.
      const handle = await fs.open(canonical, "wx", 0o644);
      try { await handle.chmod(0o644); await handle.sync(); } finally { await handle.close(); }
      await sync(path.dirname(canonical)); existing = await stat(canonical);
    }
    if (!existing?.isFile() || existing.isSymbolicLink() || existing.nlink !== 1n || existing.size > BigInt(file.size) ||
      (plan.sealed && existing.size !== BigInt(file.size)) ||
      (file.device !== undefined && (file.device !== String(existing.dev) || file.inode !== String(existing.ino))))
      throw new Error("Canonical reservation is missing or was replaced");
    file.device = String(existing.dev); file.inode = String(existing.ino);
    const link = await stat(view);
    if (!link) { await fs.symlink(canonical, view); await sync(path.dirname(view)); }
    else if (!link?.isSymbolicLink() || await fs.readlink(view) !== canonical)
      throw new Error("Torrent view no longer points to its reserved canonical object");
  }
  if (!plan.ready) {
    active(); plan.ready = true; await writeJson(manifestPath(contentRoot, sourceRoot), plan);
  }
  return plan;
}

/** Return the only allowed real file behind a client path. Legacy regular layouts remain readable. */
export async function resolveDownloadFile(contentRoot: string, sourceRoot: string, sourcePath: string, expectedSize?: number) {
  const plan = await readCanonicalDownload(contentRoot, sourceRoot);
  return resolveMember(contentRoot, sourceRoot, plan, relativePath => plan?.files.find(file => file.path === relativePath), sourcePath, expectedSize);
}

/** Use one validated manifest per scan; each member's path and inode are still checked. */
export function resolveDownloadMembers(contentRoot: string, sourceRoot: string, plan: CanonicalDownload | null) {
  const index = new Map(plan?.files.map(file => [file.path, file]));
  return (sourcePath: string, expectedSize?: number) =>
    resolveMember(contentRoot, sourceRoot, plan, key => index.get(key), sourcePath, expectedSize);
}

async function resolveMember(contentRoot: string, sourceRoot: string, plan: CanonicalDownload | null,
  lookup: (path: string) => CanonicalDownload["files"][number] | undefined, sourcePath: string, expectedSize?: number) {
  const relativePath = path.relative(sourceRoot, path.resolve(sourcePath));
  relative.parse(relativePath); await directory(sourceRoot, path.dirname(relativePath) === "." ? "" : path.dirname(relativePath));
  const file = lookup(relativePath);
  let canonical = sourcePath;
  if (plan) {
    if (!plan.ready || !file || (expectedSize !== undefined && expectedSize !== file.size)) throw new Error("File is not declared by the canonical download");
    if (file.relativePath) {
      canonical = path.join(contentRoot, file.relativePath);
      await directory(contentRoot, path.dirname(file.relativePath));
      if (!(await fs.lstat(sourcePath)).isSymbolicLink() || await fs.readlink(sourcePath) !== canonical)
        throw new Error("Canonical download link was replaced");
    }
  }
  const found = await fs.lstat(canonical, { bigint: true });
  if (!found.isFile() || found.isSymbolicLink() || await fs.realpath(path.dirname(canonical)) !== path.dirname(canonical) ||
    (expectedSize !== undefined && found.size !== BigInt(expectedSize)) ||
    (file?.relativePath && (file.device !== String(found.dev) || file.inode !== String(found.ino))))
    throw new Error("Download file escaped its declared canonical regular object");
  return { path: canonical, file: file?.relativePath ? file : null, plan, stat: found };
}

/** Completion ends writer authority before these objects can be published or shared. */
export function sealCanonicalDownload(contentRoot: string, sourceRoot: string, client: TorrentClient, ref: TorrentRef, active: () => void = () => {}) {
  return serialized(sourceRoot, () => seal(contentRoot, sourceRoot, client, ref, active));
}
async function seal(contentRoot: string, sourceRoot: string, client: TorrentClient, ref: TorrentRef, active: () => void) {
  const plan = await readCanonicalDownload(contentRoot, sourceRoot);
  if (!plan) return;
  if (plan.clientId !== ref.clientId || plan.infoHash !== ref.nativeId || client.id !== ref.clientId || !client.sealDownload)
    throw new Error("Cannot surrender canonical download writer authority");
  const resolve = resolveDownloadMembers(contentRoot, sourceRoot, plan);
  for (const file of plan.files) {
    active();
    if (file.relativePath) await resolve(path.join(sourceRoot, file.path));
    else {
      const existing = await stat(path.join(sourceRoot, file.path));
      if (existing && (!existing.isFile() || existing.isSymbolicLink())) throw new Error("Invalid torrent-specific sidecar");
    }
  }
  active(); await client.sealDownload(ref); active();
  // stop flushes the daemon's verified-but-buffered pieces into the reserved objects.
  for (const file of plan.files) { active(); await resolve(path.join(sourceRoot, file.path), file.size); }
  if (JSON.stringify(await readCanonicalDownload(contentRoot, sourceRoot)) !== JSON.stringify(plan))
    throw new Error("Canonical download reservation changed during sealing");
  plan.sealed = true; await writeJson(manifestPath(contentRoot, sourceRoot), plan);
}

const finalizedSchema = z.object({ sourceRoot: z.string(), relativePath: relative, id: z.string().uuid(), sizeBytes: count,
  sha256: z.string().regex(/^[a-f0-9]{64}$/), device: z.string(), inode: z.string(), mtimeNs: z.string(), ctimeNs: z.string() });
const proofPath = (canonical: string) => canonical + ".podible-finalized.json";
export async function finalizedCanonicalObject(contentRoot: string, canonical: string) {
  if (!canonical.startsWith(contentRoot + "/files/") || !await stat(proofPath(canonical))) return null;
  await directory(contentRoot, path.relative(contentRoot, path.dirname(canonical)));
  const proof = finalizedSchema.parse(await readJson(proofPath(canonical)));
  const plan = await readCanonicalDownload(contentRoot, proof.sourceRoot);
  const file = plan?.files.find(file => file.id === proof.id);
  if (!plan?.sealed || !file?.relativePath || path.join(contentRoot, file.relativePath) !== canonical || proof.relativePath !== file.relativePath || proof.sizeBytes !== file.size)
    throw new Error("Canonical finalization proof differs from its reservation");
  const current = await fs.lstat(canonical, { bigint: true });
  if (!current.isFile() || current.isSymbolicLink() || String(current.dev) !== proof.device || String(current.ino) !== proof.inode ||
    String(current.mtimeNs) !== proof.mtimeNs || String(current.ctimeNs) !== proof.ctimeNs || current.size !== BigInt(proof.sizeBytes))
    throw new Error("Finalized canonical object changed");
  return { id: proof.id, relativePath: proof.relativePath, sizeBytes: proof.sizeBytes, sha256: proof.sha256 };
}

/** Hash/finalize in place. This does not allocate another payload file. */
export async function finalizeCanonicalDownloadFile(contentRoot: string, sourceRoot: string, sourcePath: string, expectedSize: number, active: () => void = () => {}) {
  const plan = await readCanonicalDownload(contentRoot, sourceRoot);
  if (plan && !plan.sealed) throw new Error("Canonical download still has an active writer");
  const resolved = await resolveDownloadFile(contentRoot, sourceRoot, sourcePath, expectedSize);
  if (!resolved.file) return null;
  if (!resolved.plan?.sealed) throw new Error("Canonical download still has an active writer");
  const previous = await finalizedCanonicalObject(contentRoot, resolved.path);
  active();
  if (previous) return previous;
  const handle = await fs.open(resolved.path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const before = await handle.stat({ bigint: true }), hash = createHash("sha256"), buffer = Buffer.alloc(1024 * 1024);
    let fenceAfter = performance.now() + 250;
    for (;;) {
      effectCheckpoint();
      // This loop only reads bytes. Keep cheap cancellation per chunk and
      // recheck durable ownership periodically and before writing the proof.
      if (performance.now() >= fenceAfter) {
        active();
        fenceAfter = performance.now() + 250;
      }
      const { bytesRead } = await handle.read(buffer);
      if (!bytesRead) break; hash.update(buffer.subarray(0, bytesRead));
    }
    const after = await handle.stat({ bigint: true });
    if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs)
      throw new Error("Canonical download changed during finalization");
    await resolveDownloadFile(contentRoot, sourceRoot, sourcePath, expectedSize); active();
    const result = { id: resolved.file.id, relativePath: resolved.file.relativePath!, sizeBytes: expectedSize, sha256: hash.digest("hex") };
    await writeJson(proofPath(resolved.path), { ...result, sourceRoot,
      device: String(after.dev), inode: String(after.ino), mtimeNs: String(after.mtimeNs), ctimeNs: String(after.ctimeNs) });
    return result;
  } finally { await handle.close(); }
}
