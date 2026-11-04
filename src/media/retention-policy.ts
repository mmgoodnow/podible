import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import type { BooksRepo } from "../repo";
import { createPlexWatchHistoryReader, readPlexWatchHistory } from "./plex-watch-history";
import { RetentionService, type RetentionServiceAdapters, type RetentionResourceDetail } from "./retention-service";
import { ext4ReclaimableAllocation, inspectRetentionFile, observeRetentionDisk } from "./retention-filesystem";
import { DEFAULT_RETENTION_POLICY, RETENTION_REQUEST_GRACE_MS, retentionEvictionPlanSchema, retentionPolicySchema,
  type RetentionDisk, type RetentionEvictionPlan, type RetentionPolicy } from "./retention-policy-types";

export type RetentionAdapters = RetentionServiceAdapters & {
  observeDisk?: typeof observeRetentionDisk;
  now?: () => number;
  afterCheckpoint?: (name: string) => void | Promise<void>;
};
export type RetentionCandidate = {
  targetId: number; title: string; eligible: boolean; blockers: string[];
  expectedRevision: number | null; expectedTargetRevision: number;
  reclaimableBytes: number | null; playCount: number | null; lastViewedAt: string | null;
};
export type RetentionPreview = {
  mode: "pressure-preview"; disk: RetentionDisk; pressure: boolean | null;
  candidates: RetentionCandidate[]; nextAfterTargetId: number | null;
};
const freshWatch = createPlexWatchHistoryReader({ cacheTtlMs: 0 });

export function retentionConfiguration(repo: BooksRepo) {
  const settings = repo.readSettings();
  if (!settings) throw new Error("Retention requires existing server configuration");
  const movies = settings.movies;
  if (!movies || typeof movies.managedRoot !== "string" || !path.isAbsolute(movies.managedRoot) ||
      typeof movies.plexViewPath !== "string" || !path.isAbsolute(movies.plexViewPath)) throw new Error("Owned movie storage is not configured");
  return { movies, hash: createHash("sha256").update(JSON.stringify(settings)).digest("hex"), policy: settings.retention };
}

export function retentionAdapters(adapters: RetentionAdapters): RetentionServiceAdapters {
  return { watchHistory: readPlexWatchHistory, reclaimableAllocation: ext4ReclaimableAllocation, ...adapters };
}

export async function inspectRetentionCandidate(repo: BooksRepo, targetId: number, rawPolicy: RetentionPolicy, adapters: RetentionAdapters = {}) {
  const policy = retentionPolicySchema.parse(rawPolicy), target = repo.movies.target(targetId);
  if (!target) throw new Error("Unknown movie target");
  const key = { namespace: "movie-target", id: String(targetId) }, intent = repo.retention.get(key);
  const blockers: string[] = [];
  if (!intent) blockers.push("untracked-resource");
  if (policy.protectedMovieTargetIds.includes(targetId)) blockers.push("protected-resource");
  const requested = repo.retention.lastRequestAt(targetId);
  const now = adapters.now?.() ?? Date.now(), requestedAt = requested ? Date.parse(requested) : NaN;
  if (!Number.isFinite(requestedAt) || now - requestedAt < RETENTION_REQUEST_GRACE_MS) blockers.push("fresh-or-unknown-request");
  if (target.active_version_id === null) blockers.push("no-selected-version");
  const title = repo.movies.movie(target.movie_id)?.title ?? `Movie target ${targetId}`;
  const base: RetentionCandidate = { targetId, title, eligible: false, blockers, expectedRevision: intent?.revision ?? null,
    expectedTargetRevision: target.revision, reclaimableBytes: null, playCount: null, lastViewedAt: null };
  if (blockers.length) return { candidate: base, detail: null as RetentionResourceDetail | null };
  const detail = await new RetentionService(repo, retentionAdapters(adapters)).audit(key, { wholeResource: true });
  blockers.push(...detail.audit.blockers.map((blocker) => blocker.code));
  if (detail.watch.coverage !== "global-all-users" || detail.watch.state !== "watched" ||
      detail.watch.playCount === null || detail.watch.playCount < 1 || detail.watch.lastViewedAt === null) blockers.push("watch-history-unknown-or-unwatched");
  if (detail.watch.activePlayback !== "none") blockers.push("playback-active-or-unknown");
  for (const type of ["full_library_refresh", "reconcile", "import", "movie_reconcile"] as const) {
    try { if (repo.listJobsByType(type).some((job) => job.status === "queued" || job.status === "running")) blockers.push("active-library-work"); }
    catch { blockers.push("execution-inventory-unknown"); }
  }
  return { candidate: { ...base, blockers: [...new Set(blockers)], eligible: blockers.length === 0,
    reclaimableBytes: detail.verifiedReclaimableBytes, playCount: detail.watch.playCount, lastViewedAt: detail.watch.lastViewedAt }, detail };
}

export async function previewRetention(repo: BooksRepo, policy = retentionConfiguration(repo).policy, adapters: RetentionAdapters = {},
  input: { targetId?: number; afterTargetId?: number; limit?: number } = {}): Promise<RetentionPreview> {
  const configuration = retentionConfiguration(repo);
  const disk = await (adapters.observeDisk ?? observeRetentionDisk)(configuration.movies.managedRoot);
  const pressure = disk.state === "known" && disk.availableBytes !== null ? disk.availableBytes < policy.minimumFreeBytes : null;
  if (input.targetId === undefined && pressure !== true) return { mode: "pressure-preview", disk, pressure, candidates: [], nextAfterTargetId: null };
  const limit = Math.max(1, Math.min(100, input.limit ?? 20));
  const all = repo.movies.targets().filter((target) => target.active_version_id !== null).sort((a, b) => a.id - b.id);
  const after = all.filter((target) => target.id > (input.afterTargetId ?? 0));
  const targets = input.targetId === undefined ? (after.length ? after : all).slice(0, limit) : all.filter((target) => target.id === input.targetId);
  const candidates: RetentionCandidate[] = [];
  for (const target of targets) candidates.push((await inspectRetentionCandidate(repo, target.id, policy, adapters)).candidate);
  candidates.sort((a, b) => Number(b.eligible) - Number(a.eligible) || (a.playCount ?? Infinity) - (b.playCount ?? Infinity) ||
    Date.parse(a.lastViewedAt ?? "9999-01-01") - Date.parse(b.lastViewedAt ?? "9999-01-01") || a.targetId - b.targetId);
  return { mode: "pressure-preview", disk, pressure, candidates, nextAfterTargetId: targets.at(-1)?.id ?? null };
}

export async function prepareMovieEviction(repo: BooksRepo, input: { targetId: number; expectedRevision: number; reason: string; mode: "operator" | "pressure" },
  policy: RetentionPolicy, adapters: RetentionAdapters = {}): Promise<RetentionEvictionPlan> {
  const configuration = retentionConfiguration(repo);
  const { candidate, detail } = await inspectRetentionCandidate(repo, input.targetId, policy, adapters);
  if (!candidate.eligible || !detail) throw new Error(`Movie eviction blocked: ${candidate.blockers.join(", ")}`);
  if (candidate.expectedRevision !== input.expectedRevision) throw new Error("Retention revision changed; refresh the preview");
  const root = path.join(configuration.movies.managedRoot, "objects");
  const marker = JSON.parse(await fs.readFile(path.join(root, ".podible-content.json"), "utf8"));
  const disk = await (adapters.observeDisk ?? observeRetentionDisk)(configuration.movies.managedRoot);
  if (disk.state !== "known" || !disk.device) throw new Error("Movie filesystem is unknown");
  const files: RetentionEvictionPlan["files"] = [];
  for (const file of detail.files) {
    const current = await inspectRetentionFile(root, file);
    if (!current || `${current.stat.dev}:${current.stat.ino}` !== file.physicalId || current.stat.nlink !== 1n ||
        String(current.stat.mtimeNs) !== file.mtimeNs || String(current.stat.ctimeNs) !== file.ctimeNs ||
        current.stat.blocks * 512n !== BigInt(file.allocatedBytes ?? -1) || String(current.stat.dev) !== disk.device)
      throw new Error("Canonical movie object changed after preview");
    files.push({ id: file.id, relativePath: file.relativePath, sizeBytes: Number(current.stat.size), device: String(current.stat.dev), inode: String(current.stat.ino),
      mtimeNs: String(current.stat.mtimeNs), ctimeNs: String(current.stat.ctimeNs), allocatedBytes: file.allocatedBytes!, reclaimableBytes: file.verifiedReclaimableBytes! });
  }
  if (retentionConfiguration(repo).hash !== configuration.hash) throw new Error("Retention configuration changed during preview");
  const target = repo.movies.target(input.targetId)!;
  return retentionEvictionPlanSchema.parse({ id: randomUUID(), targetId: input.targetId, mode: input.mode, reason: input.reason,
    expectedRevision: input.expectedRevision, expectedTargetRevision: target.revision, activeVersionId: target.active_version_id,
    intentRevision: input.expectedRevision + 1, targetRevision: target.revision + 1, settingsHash: configuration.hash,
    contentId: marker.id, device: disk.device, files, createdAt: new Date(adapters.now?.() ?? Date.now()).toISOString() });
}

export async function queueMovieEviction(repo: BooksRepo, input: { targetId: number; expectedRevision: number; reason: string },
  policy = retentionConfiguration(repo).policy, adapters: RetentionAdapters = {}) {
  const plan = await prepareMovieEviction(repo, { ...input, mode: "operator" }, policy, { ...adapters, watchHistory: adapters.watchHistory ?? freshWatch });
  const eviction = repo.retention.queueEviction(plan, (evictionId) => repo.createJob({
    type: "retention_cleanup", payload: { evictionId }, idempotencyKey: `retention-eviction:${evictionId}`, maxAttempts: 5,
  }).id);
  return { evictionId: eviction.id, jobId: eviction.jobId };
}
