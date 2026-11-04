import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import type { Database } from "bun:sqlite";
import { z } from "zod";
import { BookTorrentsRepo, type BookTorrentBinding } from "./book-torrents";
import { initializeManagedContent } from "./library-view";
import { inspectMetainfo } from "./metainfo";
import type { RtorrentDownloadState, RtorrentImportSource } from "../rtorrent";
import type { TorrentClient, TorrentRef, TorrentSnapshot } from "./torrent-client";

export type MigrationRelease = {
  id: number;
  bookId: number;
  infoHash: string;
  status: string;
};
type Metadata = ReturnType<typeof inspectMetainfo>;
type Target = { rootPath: string; rootToken: string; clientId: string };
export type LegacyMigrationState = {
  tag: string | null;
  hashing: number | null;
  state: RtorrentDownloadState;
  source: RtorrentImportSource;
};
export type MigrationFileMapping = { source: string; destination: string };
export interface MigrationRepository {
  releases(): MigrationRelease[];
  cachedMetainfo(release: MigrationRelease): Uint8Array[];
  binding(hash: string): BookTorrentBinding | null;
  commit(release: MigrationRelease, binding: BookTorrentBinding, files: MigrationFileMapping[]): void;
}
export type MigrationDependencies = {
  repository: MigrationRepository;
  legacy: {
    inspect(hash: string): Promise<LegacyMigrationState>;
    stop(hash: string): Promise<void>;
  };
  client: TorrentClient;
  prepareTarget(): Promise<Target>;
  // Must validate all bytes, not just the subset of files marked wanted.
  assertVerified(ref: TorrentRef, metadata: Metadata): Promise<void>;
  journal: MigrationJournal;
  now?: () => number;
  sleep?: (ms: number) => Promise<unknown>;
};

const hashSchema = z.string().regex(/^[a-f0-9]{40}$/);
const bindingSchema = z.object({
  info_hash: hashSchema,
  book_id: z.number().int().positive(),
  release_id: z.number().int().positive().nullable(),
  backend: z.literal("transmission"),
  client_id: z.string().min(1),
  root_path: z.string(),
  root_token: z.string().uuid(),
  root_id: z.literal("books"),
  relative_path: hashSchema,
  created_at: z.string(),
}).strict();
const entrySchema = z.object({
  bookId: z.number().int().positive(),
  releaseId: z.number().int().positive(),
  infoHash: hashSchema,
  phase: z.enum([
    "planned", "skipped", "stopping-source", "adding-paused", "linking",
    "checking", "binding", "resuming", "complete", "failed",
  ]),
  reason: z.string().optional(),
  failedAt: z.string().optional(),
  binding: bindingSchema.optional(),
  metainfoDigest: z.string().regex(/^[a-f0-9]{64}$/).optional(),
  metainfoSource: z.literal("cache").optional(),
  oldRecord: z.enum(["retained", "stopped-retained"]).default("retained"),
  pauseConfirmed: z.boolean().optional(),
  updatedAt: z.string(),
}).strict();
const reportSchema = z.object({
  version: z.literal(1),
  runId: z.string().uuid(),
  entries: z.record(z.string(), entrySchema),
}).strict();
export type MigrationEntry = z.infer<typeof entrySchema>;
export type MigrationReport = z.infer<typeof reportSchema>;
export interface MigrationJournal {
  report: MigrationReport;
  save(): Promise<void>;
}

class MigrationFailure extends Error {
  constructor(readonly code: string) { super(code); }
}
function requireSafe(condition: unknown, code: string): asserts condition {
  if (!condition) throw new MigrationFailure(code);
}
function missing(error: unknown) {
  return (error as NodeJS.ErrnoException).code === "ENOENT";
}
function sameBinding(a: BookTorrentBinding, b: BookTorrentBinding) {
  return a.info_hash === b.info_hash && a.book_id === b.book_id &&
    a.release_id === b.release_id && a.client_id === b.client_id &&
    a.root_path === b.root_path && a.root_token === b.root_token &&
    a.root_id === b.root_id && a.relative_path === b.relative_path;
}

/** The only database write is the post-verification, atomic ownership cutover. */
export function migrationRepository(db: Database): MigrationRepository {
  const bindings = new BookTorrentsRepo(db);
  return {
    releases() {
      return db.query<MigrationRelease, []>(
        "SELECT id, book_id AS bookId, lower(info_hash) AS infoHash, status FROM releases ORDER BY id",
      ).all();
    },
    cachedMetainfo(release) {
      // Older cache rows can lack an indexed hash; release identity is an additional
      // lookup hint, never a substitute for hashing the actual info dictionary.
      return db.query<{ torrent_bytes: Uint8Array }, [string, number]>(`
        SELECT c.torrent_bytes FROM torrent_cache c JOIN releases r ON r.id = ?2
        WHERE lower(c.info_hash) = ?1 OR c.url = r.url
          OR (c.provider = r.provider AND c.provider_guid = r.provider_guid)
      `).all(release.infoHash, release.id).map((row) => row.torrent_bytes);
    },
    binding: (hash) => bindings.get(hash),
    commit(release, binding, files) {
      db.transaction(() => {
        const current = db.query<MigrationRelease, [number]>(
          "SELECT id, book_id AS bookId, lower(info_hash) AS infoHash, status FROM releases WHERE id = ?",
        ).get(release.id);
        requireSafe(current && JSON.stringify(current) === JSON.stringify(release), "release-changed");
        requireSafe(db.query("SELECT id FROM books WHERE id = ?").get(release.bookId), "book-missing");
        const previous = bindings.get(release.infoHash);
        requireSafe(!previous || sameBinding(previous, binding), "binding-conflict");
        if (!previous) bindings.reserve({
          infoHash: release.infoHash, bookId: release.bookId,
          clientId: binding.client_id, rootPath: binding.root_path,
          rootToken: binding.root_token,
        });
        bindings.attach(release.infoHash, release.id);
        requireSafe(sameBinding(bindings.get(release.infoHash)!, binding), "binding-conflict");
        for (const file of files) {
          requireSafe(path.isAbsolute(file.source) && file.destination.startsWith(
            path.join(binding.root_path, binding.relative_path) + path.sep,
          ), "invalid-source-mapping");
          db.query(`UPDATE asset_files SET source_path = ?
            WHERE source_path = ? AND asset_id IN
              (SELECT id FROM assets WHERE source_release_id = ?)`)
            .run(file.destination, file.source, release.id);
        }
      }).immediate();
    },
  };
}

export function selectCachedMetainfo(release: MigrationRelease, candidates: Uint8Array[]) {
  for (const bytes of candidates) {
    try {
      const metadata = inspectMetainfo(bytes);
      if (metadata.infoHash !== release.infoHash) continue;
      requireSafe(metadata.size > 0 && !metadata.files.some((file) =>
        file.path.split("/")[0]!.normalize("NFC").toLowerCase() === ".podible-book-torrent.json",
      ), "unsafe-metainfo");
      return { bytes, metadata, digest: createHash("sha256").update(bytes).digest("hex") };
    } catch { /* Reject malformed or unrelated cache entries without logging metainfo. */ }
  }
  throw new MigrationFailure("verified-metainfo-unavailable");
}

async function realDirectory(directory: string, create = false) {
  requireSafe(path.isAbsolute(directory) && path.resolve(directory) === directory, "unsafe-directory");
  const parts = directory.split(path.sep).filter(Boolean);
  let current = path.parse(directory).root;
  for (const part of parts) {
    current = path.join(current, part);
    if (create) {
      try { await fs.mkdir(current); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
    }
    const stat = await fs.lstat(current);
    requireSafe(stat.isDirectory() && !stat.isSymbolicLink(), "unsafe-directory");
  }
  requireSafe(await fs.realpath(directory) === directory, "unsafe-directory");
}

export async function prepareMigrationTarget(libraryRoot: string, clientId: string): Promise<Target> {
  await realDirectory(libraryRoot);
  const rootPath = path.join(libraryRoot, ".podible", "downloads");
  await realDirectory(rootPath, true);
  requireSafe((await fs.stat(libraryRoot)).dev === (await fs.stat(rootPath)).dev, "cross-device-layout");
  return { rootPath, rootToken: await initializeManagedContent(rootPath), clientId };
}

async function readJson(file: string) {
  const stat = await fs.lstat(file);
  requireSafe(stat.isFile() && !stat.isSymbolicLink(), "unsafe-marker");
  return JSON.parse(await fs.readFile(file, "utf8")) as unknown;
}

/** Matches book-torrents' filesystem markers, without reserving a DB binding. */
export async function prepareMigrationDirectory(binding: BookTorrentBinding) {
  bindingSchema.parse(binding);
  requireSafe(binding.relative_path === binding.info_hash, "unsafe-directory");
  await realDirectory(binding.root_path);
  const root = z.object({ id: z.string() }).parse(
    await readJson(path.join(binding.root_path, ".podible-content.json")),
  );
  requireSafe(root.id === binding.root_token, "root-identity-changed");
  const directory = path.join(binding.root_path, binding.relative_path);
  await realDirectory(directory, true);
  const marker = path.join(directory, ".podible-book-torrent.json");
  const expected = {
    rootToken: binding.root_token, clientId: binding.client_id, infoHash: binding.info_hash,
  };
  let owner: unknown;
  try { owner = await readJson(marker); } catch (error) {
    if (!missing(error)) throw error;
    requireSafe((await fs.readdir(directory)).length === 0, "unowned-target-directory");
    const file = await fs.open(marker, "wx", 0o600);
    try { await file.writeFile(JSON.stringify(expected)); await file.sync(); }
    finally { await file.close(); }
    owner = await readJson(marker);
  }
  requireSafe(JSON.stringify(owner) === JSON.stringify(expected), "target-identity-changed");
  return directory;
}

function completedLegacy(legacy: LegacyMigrationState, hash: string, size: number, allowedTags: readonly string[]) {
  const s = legacy.state;
  requireSafe(legacy.tag !== null && allowedTags.includes(legacy.tag), "legacy-tag-excluded");
  requireSafe(s.hash?.toLowerCase() === hash, "legacy-identity-changed");
  requireSafe(s.complete && legacy.hashing === 0 && s.sizeBytes === size &&
    s.bytesDone === size && s.leftBytes === 0 && s.downRate === 0 && !s.message,
  "legacy-not-complete");
}

async function sourceFiles(legacy: LegacyMigrationState, metadata: Metadata) {
  const base = legacy.source.basePath;
  requireSafe(base && path.isAbsolute(base) && base === legacy.state.basePath, "source-path-unavailable");
  const files = metadata.files.map((file) => ({
    ...file,
    source: legacy.state.isMultiFile
      ? path.join(base, ...file.path.split("/").slice(1)) : base,
  }));
  requireSafe(legacy.source.selectedPaths.length === files.length &&
    files.every((file, i) => file.source === legacy.source.selectedPaths[i]), "source-layout-mismatch");
  for (const file of files) {
    await realDirectory(path.dirname(file.source));
    const stat = await fs.lstat(file.source);
    requireSafe(stat.isFile() && !stat.isSymbolicLink() && stat.size === file.size, "source-file-invalid");
  }
  return files;
}

async function validateTargetTree(directory: string, metadata: Metadata) {
  const files = new Set(metadata.files.map((file) => file.path));
  const directories = new Set(metadata.files.flatMap((file) => {
    const parts = file.path.split("/");
    return parts.slice(0, -1).map((_, i) => parts.slice(0, i + 1).join("/"));
  }));
  const walk = async (relative: string) => {
    for (const entry of await fs.readdir(path.join(directory, relative), { withFileTypes: true })) {
      if (!relative && entry.name === ".podible-book-torrent.json") continue;
      const name = relative ? `${relative}/${entry.name}` : entry.name;
      requireSafe(!entry.isSymbolicLink(), "unsafe-target-file");
      if (entry.isDirectory() && directories.has(name)) await walk(name);
      else requireSafe(entry.isFile() && files.has(name), "unexpected-target-file");
    }
  };
  await walk("");
}

export async function linkMigrationFiles(
  directory: string,
  files: Awaited<ReturnType<typeof sourceFiles>>,
) {
  for (const file of files) {
    const destination = path.join(directory, file.path);
    await realDirectory(path.dirname(destination), true);
    await realDirectory(path.dirname(file.source));
    const source = await fs.lstat(file.source);
    requireSafe(source.isFile() && !source.isSymbolicLink() && source.size === file.size, "source-file-invalid");
    requireSafe(source.dev === (await fs.stat(path.dirname(destination))).dev, "cross-device-hardlink");
    try { await fs.link(file.source, destination); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const target = await fs.lstat(destination);
    const after = await fs.lstat(file.source);
    requireSafe(target.isFile() && !target.isSymbolicLink() && target.dev === source.dev &&
      target.ino === source.ino && target.size === file.size && after.dev === source.dev &&
      after.ino === source.ino && after.size === source.size, "hardlink-conflict");
  }
}

function snapshotIdentity(snapshot: TorrentSnapshot, binding: BookTorrentBinding) {
  requireSafe(snapshot.ref.clientId === binding.client_id && snapshot.ref.nativeId === binding.info_hash &&
    snapshot.location.rootId === "books" && snapshot.location.relativePath === binding.relative_path,
  "target-layout-mismatch");
  requireSafe(snapshot.downloadBytesPerSecond === 0 && snapshot.state !== "downloading", "target-unexpected-download");
}
function completeSnapshot(snapshot: TorrentSnapshot, binding: BookTorrentBinding, metadata: Metadata) {
  snapshotIdentity(snapshot, binding);
  requireSafe(snapshot.state === "inactive" && snapshot.complete && snapshot.sizeBytes === metadata.size &&
    snapshot.bytesDone === metadata.size && !snapshot.message, "recheck-incomplete");
}

export type MigrationOptions = {
  apply?: boolean;
  bookIds?: number[];
  hashes?: string[];
  allowedLegacyTags?: readonly string[];
  timeoutMs?: number;
};

/** One-off orchestration only. Reports are hints for reconciliation, never proof of verification. */
export async function migrateBookTorrents(deps: MigrationDependencies, options: MigrationOptions = {}) {
  const { repository, journal, client } = deps;
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? Bun.sleep;
  const allowedTags = options.allowedLegacyTags ?? ["Podible", "LazyLibrarian"];
  const selected = repository.releases().filter((r) =>
    (!options.bookIds?.length || options.bookIds.includes(r.bookId)) &&
    (!options.hashes?.length || options.hashes.includes(r.infoHash)),
  );
  const results: MigrationEntry[] = [];
  for (const release of selected) {
    hashSchema.parse(release.infoHash);
    const previous = journal.report.entries[release.infoHash];
    let entry: MigrationEntry = {
      ...previous,
      bookId: release.bookId, releaseId: release.id, infoHash: release.infoHash,
      phase: "planned", reason: undefined, failedAt: undefined, pauseConfirmed: undefined,
      oldRecord: previous?.oldRecord ?? "retained",
      updatedAt: new Date(now()).toISOString(),
    };
    let targetRef: TorrentRef | undefined;
    let attemptedAdd = false;
    let operation = "validate-release";
    const save = async (phase: MigrationEntry["phase"]) => {
      entry.phase = phase;
      entry.updatedAt = new Date(now()).toISOString();
      journal.report.entries[release.infoHash] = entry;
      if (options.apply) await journal.save();
    };
    try {
      requireSafe(!previous || (previous.bookId === release.bookId && previous.releaseId === release.id), "report-release-conflict");
      if (!["imported", "downloaded", "failed"].includes(release.status)) {
        entry.reason = "release-status-excluded";
        await save("skipped"); results.push(entry); continue;
      }
      const existing = repository.binding(release.infoHash);
      if (existing && !previous?.binding) {
        entry.reason = "already-bound";
        await save("skipped"); results.push(entry); continue;
      }
      operation = "inspect-source";
      const legacy = await deps.legacy.inspect(release.infoHash);
      if (legacy.tag === null || !allowedTags.includes(legacy.tag)) {
        entry.reason = "legacy-tag-excluded";
        await save("skipped"); results.push(entry); continue;
      }
      operation = "load-cached-metainfo";
      const { bytes, metadata, digest } = selectCachedMetainfo(release, repository.cachedMetainfo(release));
      operation = "validate-source-files";
      completedLegacy(legacy, release.infoHash, metadata.size, allowedTags);
      await sourceFiles(legacy, metadata);
      requireSafe(!previous?.metainfoDigest || previous.metainfoDigest === digest, "cached-metainfo-changed");
      entry.metainfoDigest = digest;
      entry.metainfoSource = "cache";
      if (!options.apply) { await save("planned"); results.push(entry); continue; }
      requireSafe(client.capabilities.pausedAdd && client.capabilities.recheck &&
        client.capabilities.metainfoVersions.includes(1), "client-capabilities-missing");
      await save("planned");
      operation = "prepare-target-root";
      const target = await deps.prepareTarget();
      requireSafe(target.clientId === client.id, "client-identity-changed");
      const binding: BookTorrentBinding = {
        info_hash: release.infoHash, book_id: release.bookId, release_id: release.id,
        backend: "transmission", client_id: client.id, root_path: target.rootPath,
        root_token: target.rootToken, root_id: "books", relative_path: release.infoHash,
        created_at: previous?.binding?.created_at ?? new Date(now()).toISOString(),
      };
      requireSafe(!previous?.binding || sameBinding(previous.binding, binding), "report-target-conflict");
      requireSafe(!existing || sameBinding(existing, binding), "binding-conflict");
      entry.binding = binding;
      operation = "stop-source";
      await save("stopping-source");
      await deps.legacy.stop(release.infoHash);
      const stopped = await deps.legacy.inspect(release.infoHash);
      completedLegacy(stopped, release.infoHash, metadata.size, allowedTags);
      requireSafe(!stopped.state.isActive && JSON.stringify(stopped.source) === JSON.stringify(legacy.source), "legacy-not-stopped");
      entry.oldRecord = "stopped-retained";
      const files = await sourceFiles(stopped, metadata);
      operation = "prepare-target-directory";
      const directory = await prepareMigrationDirectory(binding);
      await validateTargetTree(directory, metadata);
      targetRef = { clientId: client.id, nativeId: release.infoHash };
      operation = "add-paused";
      await save("adding-paused");
      attemptedAdd = true;
      const added = await client.ensureAdded({ metainfo: bytes, location: { rootId: "books", relativePath: release.infoHash } });
      requireSafe(added.clientId === targetRef.clientId && added.nativeId === targetRef.nativeId, "target-identity-changed");
      operation = "pause-target";
      await client.quiesce(targetRef);
      const paused = await client.inspect(targetRef);
      snapshotIdentity(paused, binding);
      requireSafe(paused.state === "inactive", "target-not-paused");
      const engineFiles = await client.files(targetRef);
      requireSafe(engineFiles.length === files.length && engineFiles.every((file, i) =>
        file.index === i && file.path === path.join(directory, files[i]!.path)), "target-layout-mismatch");
      operation = "hardlink-source-files";
      await save("linking");
      await linkMigrationFiles(directory, files);
      operation = "recheck-target";
      await save("checking");
      await client.recheck(targetRef);
      const deadline = now() + (options.timeoutMs ?? 30 * 60_000);
      for (;;) {
        const snapshot = await client.inspect(targetRef);
        snapshotIdentity(snapshot, binding);
        requireSafe(!snapshot.message, "recheck-engine-error");
        if (snapshot.state !== "checking") { completeSnapshot(snapshot, binding, metadata); break; }
        requireSafe(now() < deadline, "recheck-timeout");
        await sleep(250);
      }
      await deps.assertVerified(targetRef, metadata);
      operation = "validate-cutover";
      // Reconcile filesystem and source again after a potentially long recheck.
      await prepareMigrationDirectory(binding);
      await validateTargetTree(directory, metadata);
      await linkMigrationFiles(directory, files);
      const finalLegacy = await deps.legacy.inspect(release.infoHash);
      completedLegacy(finalLegacy, release.infoHash, metadata.size, allowedTags);
      requireSafe(!finalLegacy.state.isActive, "legacy-not-stopped");
      operation = "commit-binding-and-source-paths";
      await save("binding");
      await deps.assertVerified(targetRef, metadata);
      completeSnapshot(await client.inspect(targetRef), binding, metadata);
      repository.commit(release, binding, files.map((file) => ({
        source: file.source, destination: path.join(directory, file.path),
      })));
      operation = "resume-verified-target";
      await save("resuming");
      await deps.assertVerified(targetRef, metadata);
      completeSnapshot(await client.inspect(targetRef), binding, metadata);
      await client.resume(targetRef);
      const resumed = await client.inspect(targetRef);
      snapshotIdentity(resumed, binding);
      requireSafe(resumed.complete && resumed.bytesDone === metadata.size && resumed.sizeBytes === metadata.size &&
        ["seeding", "inactive"].includes(resumed.state), "resume-not-confirmed");
      await save("complete");
    } catch (error) {
      // An uncertain add/resume may have succeeded. Stop by stable hash; never
      // remove records, unlink raw files, or automatically fall back to download.
      if (attemptedAdd && targetRef) {
        try { await client.quiesce(targetRef); entry.pauseConfirmed = true; }
        catch { entry.pauseConfirmed = false; }
      }
      entry.reason = error instanceof MigrationFailure ? error.code : "operation-failed";
      entry.failedAt = operation;
      await save("failed");
    }
    results.push(entry);
  }
  return results;
}

/** Atomic, fsynced report writes. A stale lock requires operator review, never auto-stealing. */
export async function openMigrationJournal(file: string, apply: boolean) {
  const absolute = path.resolve(file);
  await realDirectory(path.dirname(absolute));
  let lock: Awaited<ReturnType<typeof fs.open>> | undefined;
  if (apply) lock = await fs.open(`${absolute}.lock`, "wx", 0o600);
  try {
    let report: MigrationReport;
    try { report = reportSchema.parse(await readJson(absolute)); }
    catch (error) {
      if (!missing(error)) throw error;
      report = { version: 1, runId: randomUUID(), entries: {} };
    }
    const journal: MigrationJournal = {
      report,
      async save() {
        requireSafe(apply && lock, "report-is-readonly");
        const temporary = `${absolute}.${randomUUID()}.tmp`;
        const handle = await fs.open(temporary, "wx", 0o600);
        try { await handle.writeFile(JSON.stringify(reportSchema.parse(journal.report), null, 2)); await handle.sync(); }
        finally { await handle.close(); }
        await fs.rename(temporary, absolute);
        const parent = await fs.open(path.dirname(absolute), "r");
        try { await parent.sync(); } finally { await parent.close(); }
      },
    };
    return {
      journal,
      async close() {
        if (lock) { await lock.close(); await fs.unlink(`${absolute}.lock`); lock = undefined; }
      },
    };
  } catch (error) {
    if (lock) { await lock.close(); await fs.unlink(`${absolute}.lock`); }
    throw error;
  }
}
