import { constants, type BigIntStats } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { assertMovieStorage } from "../movies/storage";
import { defaultMovieSettings } from "../movies/settings";
import { readTransmissionIdentity } from "./managed-transmission";
import { TransmissionRpc } from "./transmission-client";
import { retentionDigest } from "./retention-filesystem";
import {
  auditDeletion, recoveryDecision, validateResourceKey,
  type ActualPresence, type DeletionFileEvidence, type ResourceKey,
  type RetentionResource, type SeedObligation,
} from "./retention";

type IntentRepo = Pick<BooksRepo, "retention" | "transaction">;

/** Call only for explicit user request/re-request, inside the request/outbox transaction. */
export function ensurePresentIntent(repo: IntentRepo, key: ResourceKey, reason: string): RetentionResource {
  return repo.transaction(() => repo.retention.request(key, {
    expectedRevision: repo.retention.get(key)?.revision ?? 0, reason,
  }));
}

/** Intent gate only: absence, demand, acquisition deduplication and fencing remain caller duties. */
export function allowRecovery(repo: Pick<BooksRepo, "retention">, key: ResourceKey): boolean {
  return repo.retention.get(key)?.desiredPresence === "present";
}

const count = z.number().int().nonnegative().safe();
const presence = z.enum(["present", "missing", "unknown"]);
const recovery = z.object({ action: z.enum(["recover", "none", "inspect"]), reason: z.string() });
const keySchema = z.object({ namespace: z.literal("movie-target"), id: z.string().regex(/^[1-9]\d*$/)
  .refine((id) => Number.isSafeInteger(Number(id)), "Invalid movie target ID") }).strict();
export const retentionAuditParamsSchema = keySchema;
export const retentionStatusParamsSchema = z.object({
  afterTargetId: count.default(0), limit: z.number().int().min(1).max(100).default(20),
}).strict();
const fileSchema = z.object({
  id: z.string(), relativePath: z.string(), presence,
  ownership: z.enum(["verified-owned", "unknown"]),
  physicalId: z.string().nullable(), linkCount: count.nullable(),
  databaseReferences: count, references: z.enum(["verified-unshared", "shared", "unknown"]),
  allocatedBytes: count.nullable(), verifiedReclaimableBytes: count.nullable(),
  mtimeNs: z.string().nullable(), ctimeNs: z.string().nullable(),
  reasons: z.array(z.string()),
});
export const retentionResourceDetailSchema = z.object({
  key: keySchema, title: z.string(), targetId: count,
  mode: z.literal("audit-only"), scope: z.literal("canonical-movie-objects-only"),
  deletionSupported: z.literal(false), observedAt: z.string(),
  storedIntent: z.object({
    desiredPresence: z.enum(["present", "evicted"]), actualPresence: presence,
    revision: count, updatedAt: z.string(),
  }).nullable(),
  observation: z.object({ presence, reasons: z.array(z.string()) }),
  displayState: z.enum(["present", "missing", "intentional-absence", "eviction-intent-only", "unknown"]),
  desiredRecovery: recovery, recoveryAllowedByIntent: z.boolean(),
  files: z.array(fileSchema), activeAcquisitions: count.nullable(),
  watch: z.object({ state: z.enum(["watched", "unwatched", "unknown"]), coverage: z.enum(["global-all-users", "unknown"]), reason: z.string(),
    playCount: count.nullable(), lastViewedAt: z.string().nullable(), activePlayback: z.enum(["active", "none", "unknown"]) }),
  seeding: z.object({
    state: z.enum(["verified-independent-seed-store", "last-copy-or-unknown"]), reason: z.string(),
    obligations: z.array(z.object({
      id: z.string(), remainingSeedableCopies: count.nullable(), verifiedSecondsSeeded: count.nullable(),
      requiredSecondsSeeded: count.nullable(), otherRequirements: z.literal("unknown"),
    })),
  }),
  verifiedReclaimableBytes: count.nullable(),
  audit: z.object({
    eligible: z.boolean(), summary: z.string(), reclaimableBytes: count,
    blockers: z.array(z.object({ code: z.string(), message: z.string() })), desiredRecovery: recovery,
  }),
});
export const retentionStatusSchema = z.object({
  mode: z.literal("audit-only"), deletionSupported: z.literal(false),
  supportedNamespaces: z.array(z.literal("movie-target")), unsupportedNamespaces: z.array(z.string()),
  resources: z.array(retentionResourceDetailSchema), nextAfterTargetId: count.nullable(),
});
export type RetentionResourceDetail = z.infer<typeof retentionResourceDetailSchema>;
type FileDetail = RetentionResourceDetail["files"][number];
export type PhysicalFile = {
  id?: string;
  canonicalPath: string; device: string; inode: string; sizeBytes: number;
  allocatedBytes: number; linkCount: number; ctimeNs: string; mtimeNs: string; sha256?: string;
};
export type RetentionServiceAdapters = {
  /** Trusted server-side adapter; GET Plex global history using the supplied shared movie credentials. */
  watchHistory?: (input: {
    tmdbId: number; imdbId: string | null; plexUrl: string; plexToken: string; plexSectionId: string;
  }) => Promise<{ state: "watched" | "unwatched" | "unknown"; coverage: "global-all-users" | "owner-only" | "unknown";
    playCount?: number | null; lastViewedAt?: string | null; activePlayback?: "active" | "none" | "unknown" }>;
  /** Must account for clone/snapshot/allocation sharing. stat.blocks alone is NOT this evidence. */
  reclaimableAllocation?: (file: PhysicalFile) => Promise<number | null>;
  /** undefined uses the existing private identity; null explicitly leaves seed telemetry unknown. */
  transmission?: { clientId: string; rpc: Pick<TransmissionRpc, "call"> } | null;
};

const objectRowSchema = z.object({ id: z.string(), relative_path: z.string(), size_bytes: count });
type ObjectRow = z.infer<typeof objectRowSchema>;
type Tables = Map<string, { columns: string[]; rows: Record<string, unknown>[] }>;
const inventoryLimit = 5000;
const coreTables = new Set([
  "managed_content_files", "movie_targets", "movie_versions", "movie_acquisitions",
  "movie_version_files", "movie_publications", "asset_files",
]);

/** Bounded public repository reads, not private connection access or a second SQLite connection. */
function inventory(repo: BooksRepo): Tables {
  const tables: Tables = new Map();
  for (const name of repo.adminDbTableNames()) {
    const first = repo.adminDbTablePage(name, 100, 0);
    if (!coreTables.has(name) && !first.columns.includes("file_id") && !name.endsWith("_acquisitions")) continue;
    if (first.total > inventoryLimit) throw new Error("Retention inventory exceeds bounded read budget");
    const rows = first.rows;
    for (let offset = rows.length; offset < first.total; offset += 100)
      rows.push(...repo.adminDbTablePage(name, 100, offset).rows);
    if (rows.length !== first.total) throw new Error("Retention inventory changed during paging");
    tables.set(name, { columns: first.columns, rows });
  }
  for (const name of coreTables) if (!tables.has(name)) throw new Error("Retention inventory is incomplete");
  return tables;
}
const rows = (tables: Tables, name: string) => tables.get(name)?.rows ?? [];
const fingerprint = (tables: Tables) => JSON.stringify([...tables]);

function movieSettings(repo: BooksRepo) {
  // getSettings() creates a defaults row; audit reads must never initialize configuration.
  return repo.readSettings()?.movies ?? defaultMovieSettings();
}

function contained(root: string, relative: string): string {
  if (!relative || path.isAbsolute(relative) || /[\x00-\x1f\\]/.test(relative) ||
      relative.split("/").some((part) => !part || part === "." || part === ".."))
    throw new Error("Unsafe persisted managed path");
  const file = path.resolve(root, relative);
  if (!file.startsWith(root + path.sep)) throw new Error("Managed path escaped root");
  return file;
}

/** Validate ancestors even when a later component is missing; never follow a symlink. */
async function physicalFile(root: string, relative: string): Promise<{ stat: BigIntStats; canonicalPath: string } | null> {
  const canonicalPath = contained(root, relative);
  const parts = relative.split("/");
  let current = root;
  for (const [index, part] of parts.entries()) {
    current = path.join(current, part);
    let stat: BigIntStats;
    try { stat = await fs.lstat(current, { bigint: true }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
    if (stat.isSymbolicLink() || (index < parts.length - 1 && !stat.isDirectory()))
      throw new Error("Managed path is not canonical");
    if (index === parts.length - 1) {
      if (!stat.isFile()) throw new Error("Managed object is not a regular file");
      const handle = await fs.open(current, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const opened = await handle.stat({ bigint: true });
        if (!opened.isFile() || opened.dev !== stat.dev || opened.ino !== stat.ino ||
            opened.size !== stat.size || opened.ctimeNs !== stat.ctimeNs)
          throw new Error("Managed file changed during inspection");
        return { stat: opened, canonicalPath };
      } finally { await handle.close(); }
    }
  }
  throw new Error("Invalid managed path");
}

async function viewReferences(root: string, contentId: string): Promise<Set<string>> {
  const references = new Set<string>();
  let existing;
  try { existing = await fs.lstat(root); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return references; throw error; }
  if (!existing.isDirectory() || (await fs.realpath(root)) !== root) throw new Error("View root is not canonical");
  let manifests = 0;
  for (const name of [".podible-view.json", ".podible-view-pending.json"]) {
    const file = await physicalFile(root, name);
    if (!file) continue;
    if (file.stat.size > 1024n * 1024n) throw new Error("View inventory exceeds read budget");
    const manifest = z.object({ contentId: z.string().uuid(), files: z.record(z.string(), z.array(z.string())) })
      .parse(JSON.parse(await fs.readFile(file.canonicalPath, "utf8")));
    if (manifest.contentId !== contentId) throw new Error("View mount identity does not match");
    for (const paths of Object.values(manifest.files)) for (const relative of paths) references.add(relative);
    manifests++;
  }
  if (!manifests && (await fs.readdir(root)).length) throw new Error("View has no authoritative ownership inventory");
  return references;
}

function activeAcquisitions(repo: BooksRepo, tables: Tables, targetId?: number): number | null {
  let active = 0;
  for (const [name, table] of tables) {
    if (!name.endsWith("_acquisitions")) continue;
    if (targetId !== undefined && name !== "movie_acquisitions") continue;
    if (!table.columns.includes("finished_at") || !table.columns.includes("job_id")) return null;
    for (const row of table.rows) {
      if (targetId !== undefined && row.target_id !== targetId) continue;
      if (typeof row.job_id !== "number") { if (row.finished_at === null) return null; continue; }
      let job;
      try { job = repo.getJob(row.job_id); } catch { return null; }
      if (!job) { if (row.finished_at === null) return null; continue; }
      if (["queued", "running"].includes(job.status) || (targetId !== undefined && row.finished_at === null)) active++;
    }
  }
  return active;
}

export class RetentionResourceNotFoundError extends Error {}

export class RetentionService {
  constructor(private readonly repo: BooksRepo, private readonly adapters: RetentionServiceAdapters = {}) {}

  async status(input: { afterTargetId?: number; limit?: number } = {}): Promise<z.infer<typeof retentionStatusSchema>> {
    const params = retentionStatusParamsSchema.parse(input);
    const targets = this.repo.movies.targets().filter((target) => target.id > params.afterTargetId);
    const page = targets.slice(0, params.limit);
    const resources: RetentionResourceDetail[] = [];
    for (const target of page) resources.push(await this.audit({ namespace: "movie-target", id: String(target.id) }));
    return {
      mode: "audit-only", deletionSupported: false, supportedNamespaces: ["movie-target"],
      unsupportedNamespaces: ["tv-request", "book-manifestation"], resources,
      nextAfterTargetId: targets.length > page.length ? page.at(-1)!.id : null,
    };
  }

  async audit(rawKey: ResourceKey, options: { wholeResource?: boolean } = {}): Promise<RetentionResourceDetail> {
    validateResourceKey(rawKey);
    const key = retentionAuditParamsSchema.parse(rawKey);
    const targetId = Number(key.id);
    if (!Number.isSafeInteger(targetId)) throw new TypeError("Invalid movie target ID");
    const target = this.repo.movies.target(targetId);
    const movie = target && this.repo.movies.movie(target.movie_id);
    if (!target || !movie) throw new RetentionResourceNotFoundError("Movie target not found");
    const stored = this.repo.retention.get(key);
    const settings = movieSettings(this.repo);
    const reasons: string[] = [];
    let tables: Tables | null = null;
    try { tables = inventory(this.repo); } catch { reasons.push("Database reference inventory is incomplete or exceeds the bounded read budget."); }
    const acquisitions = tables ? rows(tables, "movie_acquisitions").filter((a) => a.target_id === targetId) : [];
    const acquisitionIds = new Set(acquisitions.map((a) => a.id));
    const versionIds = new Set(tables ? rows(tables, "movie_versions").filter((v) => acquisitionIds.has(v.acquisition_id)).map((v) => v.id) : []);
    const ownsSelected = target.active_version_id === null || versionIds.has(target.active_version_id);
    if (target.active_version_id !== null) versionIds.add(target.active_version_id);
    const associations = tables ? rows(tables, "movie_version_files") : [];
    const fileIds = new Set(associations.filter((a) => versionIds.has(a.version_id)).map((a) => a.file_id));
    const objectRows = tables ? rows(tables, "managed_content_files").filter((f) => fileIds.has(f.id)) : [];
    if (fileIds.size !== objectRows.length) reasons.push("Canonical object records are missing from the database inventory.");
    const contentRoot = path.join(settings.managedRoot, "objects");
    let mountVerified = false;
    let contentId: string | null = null;
    let views: Set<string> | null = null;
    try {
      if (!path.isAbsolute(settings.managedRoot) || (await fs.realpath(settings.managedRoot)) !== settings.managedRoot)
        throw new Error("Managed root is not canonical");
      await assertMovieStorage(this.repo, contentRoot);
      mountVerified = true;
      const marker = z.object({ id: z.string().uuid() }).parse(JSON.parse(await fs.readFile(path.join(contentRoot, ".podible-content.json"), "utf8")));
      contentId = marker.id;
      try { views = await viewReferences(path.join(settings.managedRoot, "views", "plex", "movies"), marker.id); }
      catch { reasons.push("Published view references could not be verified."); }
    } catch { reasons.push("Owned movie mount is unavailable, replaced, uninitialized, or noncanonical; this is an inspection error, not verified absence."); }
    const files: FileDetail[] = [];
    const physical = new Map<string, PhysicalFile>();
    for (const raw of objectRows) {
      const parsed = objectRowSchema.safeParse(raw);
      const detail: FileDetail = {
        id: String(raw.id), relativePath: typeof raw.relative_path === "string" ? raw.relative_path : "",
        presence: "unknown", ownership: "unknown", physicalId: null, linkCount: null,
        databaseReferences: tables ? [...tables.values()].reduce((total, table) => total +
          (table.columns.includes("file_id") ? table.rows.filter((row) => row.file_id === raw.id).length : 0), 0) : 0,
        references: "unknown", allocatedBytes: null, verifiedReclaimableBytes: null, mtimeNs: null, ctimeNs: null, reasons: [],
      };
      files.push(detail);
      if (!mountVerified || !parsed.success) { detail.reasons.push("Owned mount or canonical database object is unverified."); continue; }
      const file = parsed.data;
      const extension = path.extname(file.relative_path);
      if (!z.string().uuid().safeParse(file.id).success ||
          ![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(extension) ||
          file.relative_path !== `files/${file.id.slice(0, 2)}/${file.id}${extension}`) {
        detail.reasons.push("Database path does not match the owned canonical object layout; no filesystem lookup was attempted."); continue;
      }
      try {
        const found = await physicalFile(contentRoot, file.relative_path);
        detail.ownership = "verified-owned";
        if (!found) { detail.presence = "missing"; detail.reasons.push("Pinned mount is present but the expected canonical object is missing."); continue; }
        if (found.stat.size !== BigInt(file.size_bytes)) throw new Error("Canonical size changed");
        const actual: PhysicalFile = {
          id: file.id, canonicalPath: found.canonicalPath, device: String(found.stat.dev), inode: String(found.stat.ino),
          sizeBytes: file.size_bytes, allocatedBytes: Number(found.stat.blocks * 512n), linkCount: Number(found.stat.nlink),
          ctimeNs: String(found.stat.ctimeNs), mtimeNs: String(found.stat.mtimeNs),
        };
        if (!count.safeParse(actual.allocatedBytes).success || !count.safeParse(actual.linkCount).success) throw new Error("Unsafe allocation counters");
        physical.set(file.id, actual);
        detail.presence = "present"; detail.physicalId = `${actual.device}:${actual.inode}`;
        detail.allocatedBytes = actual.allocatedBytes; detail.linkCount = actual.linkCount;
        detail.mtimeNs = actual.mtimeNs; detail.ctimeNs = actual.ctimeNs;
        let shared = actual.linkCount !== 1 || (options.wholeResource
          ? associations.some((association) => association.file_id === file.id && !versionIds.has(association.version_id))
          : detail.databaseReferences !== 1 || Boolean(views?.has(file.relative_path)));
        for (const [name, table] of tables!) {
          if (name !== "movie_version_files" && table.columns.includes("file_id") && table.rows.some((row) => row.file_id === file.id)) shared = true;
        }
        if (rows(tables!, "movie_targets").some((row) => row.id !== targetId && versionIds.has(row.active_version_id))) shared = true;
        if (rows(tables!, "movie_publications").some((row) => versionIds.has(row.version_id))) shared = true;
        if (rows(tables!, "asset_files").some((row) => row.path === actual.canonicalPath || row.identity_path === actual.canonicalPath)) shared = true;
        detail.references = shared ? "shared" : views === null ? "unknown" : "verified-unshared";
        if (shared) {
          detail.reasons.push("Database or published view references, or multiple inode links, prevent a verified-unshared removal set.");
          // At most one name per candidate can be removed. More links prove a surviving inode;
          // logical references and symlinks alone say nothing about allocation freed.
          if (actual.linkCount > fileIds.size) {
            detail.verifiedReclaimableBytes = 0;
            detail.reasons.push("An inode link must survive even if every canonical candidate is unlinked; no allocation would be freed.");
          }
        }
        else if (detail.references === "verified-unshared" && this.adapters.reclaimableAllocation) {
          const verified = await this.adapters.reclaimableAllocation({ ...actual });
          if (verified !== null && count.safeParse(verified).success && verified <= actual.allocatedBytes)
            detail.verifiedReclaimableBytes = verified;
        }
        if (detail.verifiedReclaimableBytes === null)
          detail.reasons.push("Allocated blocks are not verified reclaimable bytes; filesystem clone/snapshot sharing is unknown.");
      } catch {
        detail.presence = "unknown"; detail.ownership = "unknown"; detail.references = "unknown";
        detail.verifiedReclaimableBytes = null;
        detail.reasons.push("Canonical object inspection failed or identity/size changed; not verified missing.");
      }
    }
    if (options.wholeResource) {
      const retired = new Set([...this.repo.retention.retiredFileIds(targetId),
        ...this.repo.contentLoss.fileIds({ namespace: "movie-target", id: String(targetId) })]);
      const selected = new Set(associations.filter((association) => association.version_id === target.active_version_id).map((association) => association.file_id));
      for (let index = files.length - 1; index >= 0; index--)
        if (files[index]!.presence === "missing" && retired.has(files[index]!.id) && !selected.has(files[index]!.id)) files.splice(index, 1);
    }
    let actualPresence: ActualPresence = "unknown";
    const requiredIds = new Set(associations.filter((a) => a.version_id === target.active_version_id).map((a) => a.file_id));
    const required = files.filter((file) => requiredIds.has(file.id));
    if (mountVerified && tables && fileIds.size === objectRows.length) {
      if (options.wholeResource && ownsSelected && files.length) {
        actualPresence = files.some((file) => file.presence === "unknown") ? "unknown" : files.some((file) => file.presence === "missing") ? "missing" : "present";
      } else if (target.active_version_id === null) {
        const retired = stored?.desiredPresence === "evicted" && stored.actualPresence === "missing"
          ? new Set(this.repo.retention.retiredFileIds(targetId)) : new Set<string>();
        if (files.length && files.every((file) => file.presence === "missing" && retired.has(file.id))) actualPresence = "missing";
        else reasons.push("No selected canonical version; availability has not been established.");
      }
      else if (required.length && required.length === requiredIds.size) {
        if (required.some((file) => file.presence === "unknown")) reasons.push("At least one required object could not be inspected safely.");
        else actualPresence = required.some((file) => file.presence === "missing") ? "missing" : "present";
      } else reasons.push("Selected version has no complete canonical file manifest.");
    }
    const watch: RetentionResourceDetail["watch"] = { state: "unknown", coverage: "unknown", reason: "Global Plex history across users is unverified; owner viewCount is not all-user evidence.",
      playCount: null, lastViewedAt: null, activePlayback: "unknown" };
    if (this.adapters.watchHistory) {
      try {
        const evidence = await this.adapters.watchHistory({ tmdbId: movie.tmdbId, imdbId: movie.imdbId,
          plexUrl: settings.plexUrl, plexToken: settings.plexToken, plexSectionId: settings.plexSectionId });
        if (evidence.coverage === "global-all-users" && ["watched", "unwatched", "unknown"].includes(evidence.state)) {
          watch.state = evidence.state; watch.coverage = "global-all-users";
          watch.playCount = evidence.playCount !== null && evidence.playCount !== undefined && count.safeParse(evidence.playCount).success ? evidence.playCount : null;
          watch.lastViewedAt = evidence.lastViewedAt && Number.isFinite(Date.parse(evidence.lastViewedAt)) ? evidence.lastViewedAt : null;
          watch.activePlayback = evidence.activePlayback ?? "unknown";
          watch.reason = "Server-side Plex global-history adapter supplied all-user coverage; no owner-only counters were used.";
        }
      } catch { watch.reason = "Global Plex history lookup failed; consumption remains unknown."; }
    }
    if (options.wholeResource && !ownsSelected) { actualPresence = "unknown"; reasons.push("Selected version does not belong to this movie target."); }
    const seeding = await this.seedEvidence(targetId, settings.managedRoot, [...physical.values()]);
    let active = tables ? activeAcquisitions(this.repo, tables, options.wholeResource ? targetId : undefined) : null;
    try {
      if (!mountVerified) throw new Error("Unverified mount");
      await assertMovieStorage(this.repo, contentRoot);
      for (const file of files) {
        if (file.ownership !== "verified-owned") continue;
        const current = await physicalFile(contentRoot, file.relativePath);
        const expected = physical.get(file.id);
        if (file.presence === "missing" ? current !== null : !current || !expected ||
            String(current.stat.dev) !== expected.device || String(current.stat.ino) !== expected.inode ||
            current.stat.size !== BigInt(expected.sizeBytes) || String(current.stat.ctimeNs) !== expected.ctimeNs ||
            current.stat.nlink !== BigInt(expected.linkCount) || current.stat.blocks * 512n !== BigInt(expected.allocatedBytes))
          throw new Error("Canonical file changed across observation");
      }
      if (views !== null && JSON.stringify([...await viewReferences(path.join(settings.managedRoot, "views", "plex", "movies"), contentId!)].sort()) !==
          JSON.stringify([...views].sort())) throw new Error("Published view references changed");
      await assertMovieStorage(this.repo, contentRoot);
      if (JSON.stringify(movieSettings(this.repo)) !== JSON.stringify(settings) ||
          JSON.stringify(this.repo.retention.get(key)) !== JSON.stringify(stored) ||
          !tables || fingerprint(inventory(this.repo)) !== fingerprint(tables)) throw new Error("Inventory changed");
      active = activeAcquisitions(this.repo, tables, options.wholeResource ? targetId : undefined);
    } catch {
      actualPresence = "unknown"; active = null;
      for (const file of files) { file.presence = "unknown"; file.ownership = "unknown"; file.references = "unknown"; file.verifiedReclaimableBytes = null; }
      seeding.state = "last-copy-or-unknown";
      seeding.reason = "Mount/configuration/inventory changed; surviving seed-copy evidence must be collected again.";
      for (const obligation of seeding.obligations) obligation.remainingSeedableCopies = null;
      reasons.push("Mount, configuration, intent, or database inventory was not stable across this observation; retry inspection.");
    }
    const observed = stored ? { ...stored, actualPresence } : null;
    const evidence: DeletionFileEvidence[] = files.map((file) => ({
      fileId: file.physicalId ?? `unverified:${file.id}`, ownership: file.ownership,
      references: file.references, verifiedReclaimableBytes: file.verifiedReclaimableBytes,
    }));
    const projectedIntent = options.wholeResource && observed ? { ...observed, desiredPresence: "evicted" as const } : observed;
    const audit = auditDeletion({ resource: projectedIntent, files: evidence, activeAcquisitions: active, watchState: watch.state,
      seeding: seeding.obligations.length ? { kind: "obligations", obligations: seeding.obligations } : { kind: "unknown" } });
    const verifiedBytes = files.length && files.every((file) => file.verifiedReclaimableBytes !== null)
      ? files.reduce((sum, file) => sum + file.verifiedReclaimableBytes!, 0) : null;
    return {
      key, targetId, title: movie.title, mode: "audit-only", scope: "canonical-movie-objects-only", deletionSupported: false,
      observedAt: new Date().toISOString(),
      storedIntent: stored ? { desiredPresence: stored.desiredPresence, actualPresence: stored.actualPresence, revision: stored.revision, updatedAt: stored.updatedAt } : null,
      observation: { presence: actualPresence, reasons },
      displayState: actualPresence === "unknown" ? "unknown" : stored?.desiredPresence === "evicted"
        ? actualPresence === "missing" ? "intentional-absence" : "eviction-intent-only" : actualPresence,
      desiredRecovery: recoveryDecision(observed), recoveryAllowedByIntent: stored?.desiredPresence === "present",
      files, activeAcquisitions: active, watch, seeding,
      verifiedReclaimableBytes: verifiedBytes !== null && count.safeParse(verifiedBytes).success ? verifiedBytes : null, audit,
    };
  }

  async seedEvidence(targetId: number, root: string, objects: PhysicalFile[]): Promise<RetentionResourceDetail["seeding"]> {
    const obligations: SeedObligation[] = [];
    const objectHashes = new Set<string>(), covered = new Set<string>();
    let reader = this.adapters.transmission;
    try {
      if (reader === undefined) {
        const identity = readTransmissionIdentity();
        reader = { clientId: `transmission:${identity.id}`, rpc: new TransmissionRpc({
          url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password,
        }) };
      }
      if (!reader || !/^transmission:[a-f0-9-]{36}$/.test(reader.clientId)) throw new Error("Private identity unavailable");
      const fileIds = objects.map((object) => object.id ?? path.basename(object.canonicalPath, path.extname(object.canonicalPath)));
      if (fileIds.some((id) => !z.string().uuid().safeParse(id).success)) throw new Error("Canonical object identities are unknown");
      const downloads = this.repo.retention.seedDownloads(targetId, fileIds);
      if (!downloads.length || downloads.length > 100) throw new Error("Seed bindings unknown");
      const downloadsRoot = path.join(root, "downloads");
      if ((await fs.realpath(downloadsRoot)) !== downloadsRoot) throw new Error("Noncanonical seed store");
      for (const object of objects) {
        const hash = object.sha256 ?? await retentionDigest(object.canonicalPath);
        if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error("Canonical byte proof is unknown");
        objectHashes.add(hash);
      }
      for (const download of downloads) obligations.push({
        id: `${download.client_id}:${download.native_id}`, remainingSeedableCopies: null,
        verifiedSecondsSeeded: null, requiredSecondsSeeded: null, otherRequirements: "unknown",
      });
      const ids = downloads.filter((d) => d.client_id === reader!.clientId && /^[a-f0-9]{40}$/.test(d.native_id)).map((d) => d.native_id);
      const result = ids.length ? await reader.rpc.call("torrent-get", { ids, fields: [
        "hashString", "downloadDir", "labels", "files", "leftUntilDone", "secondsSeeding", "error", "status",
      ] }) : { torrents: [] };
      const torrents = z.array(z.object({ hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()),
        files: z.array(z.object({ name: z.string(), length: count, bytesCompleted: count })),
        leftUntilDone: count, secondsSeeding: count.nullish(), error: count, status: count,
      })).parse(result.torrents);
      for (const [index, download] of downloads.entries()) {
        const obligation = obligations[index]!;
        try {
          if (download.client_id !== reader.clientId || !/^[a-f0-9]{40}$/.test(download.native_id)) throw new Error("Wrong client identity");
          const location = z.object({ rootId: z.literal("movies"), relativePath: z.string() }).strict().parse(JSON.parse(download.location_json));
          const directory = contained(downloadsRoot, location.relativePath);
          const matches = torrents.filter((torrent) => torrent.hashString.toLowerCase() === download.native_id);
          if (matches.length !== 1) throw new Error("Ambiguous seed identity");
          const torrent = matches[0]!;
          if (torrent.hashString.toLowerCase() !== download.native_id || !torrent.labels.includes("Podible Movies") ||
              torrent.downloadDir !== directory || (await fs.realpath(directory)) !== directory) throw new Error("Unowned seed store");
          obligation.verifiedSecondsSeeded = torrent.secondsSeeding ?? null;
          if (torrent.leftUntilDone !== 0 || torrent.error !== 0 || ![0, 5, 6].includes(torrent.status) ||
              !torrent.files.length || torrent.files.length > 1000) throw new Error("Seed copy not verified complete");
          let independent = true;
          const byteMatches = new Set<string>();
          for (const file of torrent.files) {
            if (file.bytesCompleted !== file.length) throw new Error("Incomplete seed file");
            const found = await physicalFile(downloadsRoot, `${location.relativePath}/${file.name}`);
            if (!found || found.stat.size !== BigInt(file.length)) { independent = false; break; }
            if (objects.some((object) => object.device === String(found.stat.dev) && object.inode === String(found.stat.ino))) independent = false;
            if (objects.some((object) => object.sizeBytes === file.length)) {
              const hash = await retentionDigest(found.canonicalPath);
              const after = await physicalFile(downloadsRoot, `${location.relativePath}/${file.name}`);
              if (!after || after.stat.dev !== found.stat.dev || after.stat.ino !== found.stat.ino || after.stat.ctimeNs !== found.stat.ctimeNs || after.stat.mtimeNs !== found.stat.mtimeNs)
                throw new Error("Seed copy changed during byte verification");
              if (objectHashes.has(hash)) byteMatches.add(hash);
            }
          }
          obligation.remainingSeedableCopies = independent && byteMatches.size > 0 ? 1 : 0;
          if (obligation.remainingSeedableCopies === 1) for (const hash of byteMatches) covered.add(hash);
        } catch { /* Unknown ownership, layout, history or files must not become a surviving-copy claim. */ }
      }
    } catch { /* Unavailable private identity/store leaves last-copy safety unverified. */ }
    if (!objectHashes.size || [...objectHashes].some((hash) => !covered.has(hash)))
      for (const obligation of obligations) if (obligation.remainingSeedableCopies === 1) obligation.remainingSeedableCopies = null;
    const independent = obligations.length > 0 && obligations.every((obligation) => obligation.remainingSeedableCopies === 1);
    return {
      state: independent ? "verified-independent-seed-store" : "last-copy-or-unknown",
      reason: independent
        ? "Every canonical byte proof matches a fresh independently stored file in exact owned complete Transmission bindings. No seed-store files or registrations would be removed."
        : "Seed-store ownership, surviving independent copies, or history is unknown. Last-copy deletion remains blocked; tracker requirements are not inferred.",
      obligations: obligations.map((o) => ({ ...o, otherRequirements: "unknown" })),
    };
  }
}
