import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { AppSettings, JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { fetchPlexServerDevices, decodePlexTokenExpiry, type PlexServerDevice } from "../plex";
import { defaultSettings, parseSettings } from "../settings";
import { assertMovieStorage } from "../movies/storage";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { adminTorrentKey, readAdminTorrents, type AdminTorrentBinding, type AdminTorrentSource, type AdminTransmissionReader } from "./admin-torrents";

const empty = z.object({}).strict();
const id = z.number().int().positive().safe();
const count = z.number().int().nonnegative().safe();
const machineId = z.string().min(1).max(256).regex(/^[a-zA-Z0-9._:-]+$/);
const safeErrorSchema = z.object({ code: z.string(), message: z.string() });
const serverSchema = z.object({ machineId, name: z.string(), product: z.string(), owned: z.boolean(), sourceTitle: z.string().nullable() });
export const adminWebUsersSchema = z.object({ users: z.array(z.object({
  id, provider: z.string(), username: z.string(), displayName: z.string().nullable(), isAdmin: z.boolean(),
  createdAt: z.string(), updatedAt: z.string(),
})) });
export const adminWebPlexServersSchema = z.object({
  authMode: z.literal("plex"), ownerTokenConfigured: z.boolean(), ownerTokenExpired: z.boolean().nullable(),
  selectedMachineId: z.string().nullable(), servers: z.array(serverSchema),
  source: z.enum(["fresh", "cache", "stale-cache", "not-configured", "unavailable"]),
  fetchedAt: z.string().nullable(), error: z.string().nullable(),
});
export const adminWebJobSchema = z.object({
  id, type: z.string(), status: z.enum(["queued", "running", "succeeded", "failed", "cancelled"]),
  bookId: id.nullable(), releaseId: id.nullable(), attempts: count, maxAttempts: count,
  nextWakeAt: z.string().nullable(), error: safeErrorSchema.nullable(),
  recovery: z.enum(["running", "automatic-retry", "scheduled-wake", "queued", "manual-retry", "cancelled", "complete", "unknown"]),
});
const workSchema = z.object({
  key: z.string(), title: z.string(), media: z.enum(["book", "movie", "tv", "system"]),
  targetId: id.nullable(), href: z.string().nullable(), stage: z.string().nullable(), job: adminWebJobSchema,
});
const incidentSchema = z.object({
  key: z.string(), title: z.string(), detail: z.string(), code: z.string(), jobId: id.nullable(), href: z.string().nullable(),
  recovery: adminWebJobSchema.shape.recovery, nextWakeAt: z.string().nullable(),
});
export const adminWebOpsSchema = z.object({
  observedAt: z.string(), summary: z.object({ activeAcquisitions: count, downloads: count, incidents: count, waiting: count, nextWakeAt: z.string().nullable() }),
  activeAcquisitions: z.array(workSchema),
  downloads: z.array(workSchema.extend({
    bytesDone: count.nullable(), sizeBytes: count.nullable(), bytesPerSecond: count.nullable(),
    observedAt: z.string().nullable(), telemetry: z.enum(["live", "recorded", "unknown"]), problem: safeErrorSchema.nullable(),
  })),
  incidents: z.array(incidentSchema),
  feeds: z.array(z.object({
    id: z.string(), provider: z.string(), mediaType: z.string(), state: z.enum(["healthy", "cooldown", "unavailable", "unknown"]),
    lastSuccessAt: z.string().nullable(), lastPolledAt: z.string().nullable(), retryAt: z.string().nullable(), nextPollAt: z.string().nullable(),
    error: safeErrorSchema.nullable(), gap: z.boolean(),
  })),
  storage: z.array(z.object({
    media: z.enum(["book", "movie", "tv"]), root: z.string().nullable(),
    capacityState: z.enum(["known", "unknown", "not-configured"]), identityState: z.enum(["verified", "unknown", "mismatch", "not-configured"]),
    totalBytes: count.nullable(), availableBytes: count.nullable(), reason: z.string(),
  })),
  diagnostics: z.object({ recentJobs: z.array(adminWebJobSchema), warnings: z.array(z.string()), truncated: z.boolean() }),
});
export type AdminWebOps = z.infer<typeof adminWebOpsSchema>;
type PublicJob = z.infer<typeof adminWebJobSchema>;
type PublicServer = z.infer<typeof serverSchema>;
type Work = z.infer<typeof workSchema>;
type Context = Omit<Work, "job"> & { suppressIncident: boolean };
export type AdminWebDependencies = {
  now?: () => number;
  fetchPlexServers?: (settings: AppSettings) => Promise<PlexServerDevice[]>;
  transmission?: AdminTransmissionReader | null;
};

const CACHE_KEY = "plex_server_devices";
const CACHE_TTL = 5 * 60_000;
const cacheSchema = z.object({ ownerTokenHash: z.string(), fetchedAtMs: count, servers: z.array(serverSchema) });
const tokenHash = (token: string) => createHash("sha256").update(token).digest("hex");
const iso = (value: string | null | undefined) => value && Number.isFinite(Date.parse(value)) ? new Date(value).toISOString() : null;
const active = (job: JobRow) => job.status === "queued" || job.status === "running";

function settingsForRead(repo: BooksRepo): AppSettings {
  const row = repo.adminDbTablePage("settings", 100, 0).rows.find((row) => row.id === 1);
  return row ? parseSettings(z.string().parse(row.value_json)) : defaultSettings();
}

function labelSanitizer(settings: AppSettings) {
  const secrets: string[] = [];
  const visit = (value: unknown, key = "") => {
    if (typeof value === "string" && value && /token|password|secret|api.?key|private.?key/i.test(key)) secrets.push(value);
    else if (value && typeof value === "object") for (const [name, child] of Object.entries(value)) visit(child, name);
  };
  visit(settings);
  return (value: string) => {
    let result = value.replace(/https?:\/\/\S+/gi, "[redacted URL]")
      .replace(/(?:bearer|basic)\s+\S+/gi, "[redacted credential]")
      .replace(/((?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+/gi, "$1[redacted]")
      .replace(/[\x00-\x1f\x7f]/g, " ");
    for (const secret of secrets) result = result.split(secret).join("[redacted]");
    return result.slice(0, 300);
  };
}

/** Categorize errors without returning upstream text, URLs, payloads, or credentials. */
export function normalizedAdminError(raw: string | null): z.infer<typeof safeErrorSchema> | null {
  if (!raw) return null;
  const signal = raw.replace(/https?:\/\/\S+/gi, "[URL]");
  if (/(?:client|version).*(?:not (?:on (?:the )?)?(?:white|allow)[ -]?list|not allowed|not permitted|banned|unsupported|unapproved)|(?:white|allow)[ -]?list.*(?:client|version)/i.test(signal))
    return { code: "tracker-client-not-allowed", message: "A tracker rejected the torrent client or version. Check that tracker's allowed client/version list and the managed client's tracker status before retrying." };
  if (/\b(?:401|403)\b|HTTP (?:400|404|405|422)\b|authentication.*(?:fail|reject)|invalid (?:provider )?(?:credential|api.?key|token)|unauthori[sz]ed|forbidden/i.test(signal))
    return { code: "provider-configuration", message: "A provider rejected access; check its configured credentials and permissions." };
  if (/\b429\b|cooldown|rate.?limit/i.test(signal))
    return { code: "provider-cooldown", message: "A provider is in cooldown; upstream health or credentials may need attention." };
  if (/ENOSPC|insufficient.*(?:disk|space)|no space/i.test(signal))
    return { code: "storage-capacity", message: "Storage capacity prevented the operation; inspect free space." };
  if (/mount|symlink|ownership|root.*identity/i.test(signal))
    return { code: "storage-identity", message: "Storage ownership or mount identity could not be verified." };
  if (/timeout|timed out|unavailable|ECONN|HTTP 5\d\d|network|fetch failed/i.test(signal))
    return { code: "provider-unavailable", message: "An upstream service is unavailable; inspect its health and the recorded retry state." };
  if (/plex|publication|publishing|indexing/i.test(signal))
    return { code: "publication-failed", message: "Publication or library verification failed; existing content is retained." };
  return { code: "operation-failed", message: "The operation failed; inspect its diagnostics or retry after resolving the cause." };
}

function publicJob(job: JobRow, now: number, retryPending = false): PublicJob {
  const nextWakeAt = iso(job.next_run_at);
  const error = normalizedAdminError(job.error);
  return {
    id: job.id, type: job.type, status: job.status, bookId: job.book_id, releaseId: job.release_id,
    attempts: job.attempt_count, maxAttempts: job.max_attempts, nextWakeAt, error,
    recovery: retryPending && ["failed", "cancelled"].includes(job.status) ? "queued"
      : job.status === "running" ? "running" : job.status === "failed" ? "manual-retry"
      : job.status === "cancelled" ? "cancelled" : job.status === "succeeded" ? "complete"
      : error && nextWakeAt && Date.parse(nextWakeAt) > now ? "automatic-retry"
      : nextWakeAt ? "scheduled-wake" : "queued",
  };
}

function publicServers(devices: readonly PublicServer[], settings: AppSettings): PublicServer[] {
  const label = labelSanitizer(settings);
  const seen = new Set<string>();
  return devices.filter((device) => machineId.safeParse(device.machineId).success &&
    label(device.machineId) === device.machineId && !seen.has(device.machineId) && Boolean(seen.add(device.machineId)))
    .map((device) => ({ machineId: device.machineId, name: label(device.name), product: label(device.product),
      owned: device.owned, sourceTitle: device.sourceTitle === null ? null : label(device.sourceTitle) }));
}

function cachedServers(repo: BooksRepo, settings: AppSettings, now: number) {
  const parsed = cacheSchema.safeParse(repo.getJsonState(CACHE_KEY));
  if (!parsed.success || parsed.data.ownerTokenHash !== tokenHash(settings.auth.plex.ownerToken) || parsed.data.fetchedAtMs > now) return null;
  return { ...parsed.data, servers: publicServers(parsed.data.servers, settings) };
}
function storeServers(repo: BooksRepo, settings: AppSettings, servers: PublicServer[], now: number) {
  repo.setJsonState(CACHE_KEY, { ownerTokenHash: tokenHash(settings.auth.plex.ownerToken), fetchedAtMs: now, servers });
}

async function storageView(repo: BooksRepo, settings: AppSettings): Promise<AdminWebOps["storage"]> {
  const label = labelSanitizer(settings);
  const roots = [{ media: "book" as const, root: settings.libraryRoot },
    { media: "movie" as const, root: settings.movies.managedRoot }, { media: "tv" as const, root: settings.tv.managedRoot }];
  const result: AdminWebOps["storage"] = [];
  for (const { media, root } of roots) {
    const row: AdminWebOps["storage"][number] = { media, root: root ? label(root) : null,
      capacityState: root ? "unknown" : "not-configured", identityState: root ? "unknown" : "not-configured",
      totalBytes: null, availableBytes: null, reason: root ? "Storage capacity and identity have not been verified." : "No managed root is configured." };
    result.push(row);
    if (!root) continue;
    try {
      if (!path.isAbsolute(root)) throw new Error("Nonabsolute storage root");
      const canonical = await fs.realpath(root);
      if (media !== "book" && canonical !== root) { row.identityState = "mismatch"; throw new Error("Noncanonical root"); }
      const space = await fs.statfs(canonical);
      const total = space.blocks * space.bsize, available = space.bavail * space.bsize;
      if (!count.safeParse(total).success || !count.safeParse(available).success) throw new Error("Unsafe capacity counters");
      row.capacityState = "known"; row.totalBytes = total; row.availableBytes = available;
      if (media === "book") {
        row.reason = "Capacity observed; no durable book-content mount pin is exposed for identity verification. Marker existence alone is not success.";
      } else {
        const contentRoot = path.join(root, "objects");
        try { await assertMovieStorage(repo, contentRoot); row.identityState = "verified"; row.reason = "Capacity observed and the pinned canonical content mount identity verified."; }
        catch {
          row.reason = "Capacity observed, but the canonical content mount identity is unknown or unavailable.";
          const key = "movie-content-mount:" + createHash("sha256").update(path.resolve(contentRoot)).digest("hex");
          const expected = repo.getJsonState<string>(key);
          if (expected) {
            try {
              const marker = path.join(contentRoot, ".podible-content.json");
              const stat = await fs.lstat(marker);
              if (stat.isSymbolicLink() || !stat.isFile() || (await fs.realpath(contentRoot)) !== contentRoot) row.identityState = "mismatch";
              else if (JSON.parse(await fs.readFile(marker, "utf8")).id !== expected) row.identityState = "mismatch";
            } catch { /* Missing or unreadable mounts stay unknown, never healthy. */ }
            if (row.identityState === "mismatch") row.reason = "Canonical content mount identity mismatched; filesystem effects must remain fenced.";
          }
        }
      }
    } catch { row.reason = "Storage path or capacity inspection failed; unknown capacity is not zero or success."; }
  }
  return result;
}

function associatedRows(repo: BooksRepo, table: string): Record<string, unknown>[] {
  const first = repo.adminDbTablePage(table, 100, 0);
  if (first.total > 10_000) throw new Error("Bounded association inventory exceeded");
  const rows = first.rows;
  for (let offset = rows.length; offset < first.total; offset += 100) rows.push(...repo.adminDbTablePage(table, 100, offset).rows);
  if (rows.length !== first.total) throw new Error("Association inventory changed");
  return rows;
}

async function operationalView(repo: BooksRepo, settings: AppSettings, includeJobs: boolean, now: number, transmission?: AdminTransmissionReader | null): Promise<AdminWebOps> {
  const label = labelSanitizer(settings);
  const warnings: string[] = [];
  const incidents: AdminWebOps["incidents"] = [];
  let jobs: JobRow[] = [];
  try { jobs = repo.listExecutionJobs(); }
  catch {
    warnings.push("Execution journal could not be read; active work and failure counts are incomplete.");
    incidents.push({ key: "execution-authority", title: "Execution state unavailable", detail: "The execution journal cannot be read; do not infer an empty queue or successful execution.",
      code: "execution-state-unknown", jobId: null, href: null, recovery: "unknown", nextWakeAt: null });
  }
  const contexts = new Map<number, Context>();
  const torrents = new Map<number, AdminTorrentBinding[]>();
  const byId = new Map(jobs.map((job) => [job.id, job]));
  const getJob = (jobId: number) => byId.get(jobId) ?? repo.getJob(jobId);
  const retryPending = new Set(repo.listJobCommands().filter((command) => command.kind === "retry" &&
    command.generation === repo.jobExecutionGeneration(command.job_id)).map((command) => command.job_id));
  let waiting = 0;
  try {
    const bindings = associatedRows(repo, "movie_acquisition_downloads");
    for (const target of repo.movies.targets()) {
      const movie = repo.movies.movie(target.movie_id);
      const acquisitions = repo.movies.acquisitions(target.id);
      const downloads = repo.movies.downloads(target.id);
      const latest = acquisitions[0];
      if (latest?.finished_at && latest.job_id && getJob(latest.job_id)?.status === "succeeded" &&
          target.active_version_id === null && repo.movies.activeRequestCount(target.id) > 0 &&
          repo.movies.events(target.id).at(-1)?.stage === "waiting" &&
          repo.retention.get({ namespace: "movie-target", id: String(target.id) })?.desiredPresence !== "evicted") waiting++;
      for (const [index, acquisition] of acquisitions.entries()) if (acquisition.job_id) {
        contexts.set(acquisition.job_id, { key: `movie:${acquisition.id}`, title: label(movie?.title ?? `Movie target ${target.id}`), media: "movie", targetId: target.id,
          href: movie ? `/movies/${movie.tmdbId}` : null, stage: repo.movies.events(target.id).at(-1)?.stage ?? null,
          suppressIncident: index > 0 || target.active_version_id !== null || !repo.movies.activeRequestCount(target.id) ||
            repo.retention.get({ namespace: "movie-target", id: String(target.id) })?.desiredPresence === "evicted" });
        const ids = new Set(bindings.filter((row) => row.acquisition_id === acquisition.id).map((row) => row.torrent_id));
        torrents.set(acquisition.job_id, downloads.filter((download) => ids.has(download.id)));
      }
    }
    const requests = repo.tv.requests();
    const tvBindings = associatedRows(repo, "tv_acquisition_downloads");
    for (const targetId of new Set(requests.map((request) => request.target_id))) {
      const target = repo.tv.target(targetId)!;
      const entry = repo.tv.entry(target.entry_id);
      const liveRequests = requests.filter((request) => request.target_id === targetId && !request.withdrawn_at);
      const pending = liveRequests.flatMap((request) => repo.tv.missingEpisodes(request.id));
      const nothingAiredMissing = pending.every((episode) => !episode.airDate || Date.parse(episode.airDate) > now);
      const downloads = repo.tv.downloads(targetId);
      const acquisitions = repo.tv.acquisitions(targetId);
      const latest = acquisitions[0];
      if (latest?.finished_at && latest.job_id && getJob(latest.job_id)?.status === "succeeded" && liveRequests.length && pending.length &&
          repo.retention.get({ namespace: "tv-target", id: String(targetId) })?.desiredPresence !== "evicted") {
        let noOffer = false;
        try { noOffer = JSON.parse(latest.decision_json ?? "{}").selection?.title === null; } catch { /* No inferred waiting from malformed evidence. */ }
        if (nothingAiredMissing || noOffer) waiting++;
      }
      for (const [index, acquisition] of acquisitions.entries()) if (acquisition.job_id) {
        contexts.set(acquisition.job_id, { key: `tv:${acquisition.id}`, title: label(entry?.title ?? `TV target ${targetId}`), media: "tv", targetId,
          href: "/tv-sync", stage: repo.tv.events(targetId).at(-1)?.stage ?? null,
          suppressIncident: index > 0 || !liveRequests.length || nothingAiredMissing ||
            repo.retention.get({ namespace: "tv-target", id: String(targetId) })?.desiredPresence === "evicted" });
        const ids = new Set(tvBindings.filter((row) => row.acquisition_id === acquisition.id).map((row) => row.torrent_id));
        torrents.set(acquisition.job_id, downloads.filter((download) => ids.has(download.id)));
      }
    }
  } catch { warnings.push("Some movie/TV acquisition associations could not be read; those operational counts may be incomplete."); }

  const sources: AdminTorrentSource[] = [];
  for (const job of jobs) {
    const context = contexts.get(job.id);
    if (!active(job) || repo.isJobCancellationRequested(job.id) || (context?.media !== "movie" && context?.media !== "tv")) continue;
    for (const binding of torrents.get(job.id) ?? []) sources.push({ media: context.media, binding });
  }
  const inventory = await readAdminTorrents(sources, settings, now, transmission);
  if (inventory.unknown)
    warnings.push(`Current managed torrent activity could not be verified for ${inventory.unknown} bound source(s); recorded observations are not counted as active downloads or actionable torrent errors.`);
  if (inventory.truncated) warnings.push("Managed torrent inventory exceeded the bounded read budget; current download counts are incomplete.");

  const latestByType = new Map<string, JobRow[]>();
  function contextFor(job: JobRow): Context {
    const associated = contexts.get(job.id);
    if (associated) return associated;
    if (job.book_id) {
      const book = repo.getBook(job.book_id);
      const release = job.release_id ? repo.getRelease(job.release_id) : null;
      let media: string[] = release ? [release.media_type] : ["audio", "ebook"];
      try { const payload = JSON.parse(job.payload_json ?? "{}"); if (!release && Array.isArray(payload.media) && payload.media.length) media = payload.media; } catch { /* Unknown payload is not returned. */ }
      const available = !!book && media.every((kind) => kind === "audio" ? book.audioStatus === "imported" : kind === "ebook" && book.ebookStatus === "imported");
      return { key: `job:${job.id}`, title: label(book?.title ?? `Book ${job.book_id}`), media: "book", targetId: job.book_id,
        href: `/book/${job.book_id}`, stage: release?.status ?? job.type,
        suppressIncident: ["acquire", "download", "import"].includes(job.type) && available };
    }
    return { key: `job:${job.id}`, title: job.type.replaceAll("_", " "), media: "system", targetId: null, href: null, stage: job.type, suppressIncident: false };
  }
  const acquisitions: Work[] = [], downloads: AdminWebOps["downloads"] = [];
  for (const job of jobs) {
    if (repo.isJobCancellationRequested(job.id)) continue;
    let context: Context;
    try { context = contextFor(job); } catch { warnings.push("An operation's domain metadata is unavailable."); continue; }
    const { suppressIncident, ...work } = context;
    const view = publicJob(job, now, retryPending.has(job.id));
    if (active(job) && ["acquire", "import", "movie_acquire", "tv_acquire"].includes(job.type)) acquisitions.push({ ...work, job: view });
    if (active(job) && job.type === "download") downloads.push({ ...work, job: view, bytesDone: null, sizeBytes: null,
      bytesPerSecond: null, observedAt: null, telemetry: "unknown", problem: null });
    if (active(job)) for (const torrent of torrents.get(job.id) ?? []) {
      if (work.media !== "movie" && work.media !== "tv") continue;
      const observed = inventory.observations.get(adminTorrentKey({ media: work.media, binding: torrent }));
      if (!observed?.active) continue;
      const classified = normalizedAdminError(observed.error);
      const problem = classified?.code === "tracker-client-not-allowed" ? classified : observed.error
        ? { code: "torrent-tracker-or-storage", message: "The private managed client currently reports a tracker or storage problem. Exact cause is unknown from its status; inspect tracker requirements and local storage." }
        : null;
      downloads.push({ ...work, key: `${work.key}:torrent:${torrent.id}`, job: view,
        bytesDone: observed.bytesDone, sizeBytes: observed.sizeBytes, bytesPerSecond: observed.bytesPerSecond,
        observedAt: observed.observedAt, telemetry: "live", problem });
      if (problem && !suppressIncident && view.error?.code !== problem.code)
        incidents.push({ key: `torrent:${torrent.id}`, title: work.title, detail: problem.message, code: problem.code,
          jobId: job.id, href: work.href, recovery: view.recovery, nextWakeAt: view.nextWakeAt });
    }
    if ((job.status !== "failed" && !(active(job) && job.error)) || suppressIncident) continue;
    if (!contexts.has(job.id)) {
      if (!latestByType.has(job.type)) latestByType.set(job.type, repo.listJobsByType(job.type));
      const newer = latestByType.get(job.type)!.some((other) => other.id > job.id && other.book_id === job.book_id && other.release_id === job.release_id);
      if (newer) continue;
    }
    const error = view.error ?? normalizedAdminError("Operation failed")!;
    incidents.push({ key: `job:${job.id}`, title: work.title, detail: error.message, code: error.code,
      jobId: job.id, href: work.href, recovery: view.recovery, nextWakeAt: view.nextWakeAt });
  }
  let feeds: AdminWebOps["feeds"] = [];
  try {
    const feedJob = repo.latestJobByType("indexer_feed_sync");
    const nextPollAt = feedJob && active(feedJob) ? iso(feedJob.next_run_at) : null;
    feeds = repo.releaseInbox.feeds().map((feed) => {
      const retryAt = iso(feed.retry_at), error = normalizedAdminError(feed.last_error);
      const state = retryAt && Date.parse(retryAt) > now ? "cooldown" as const : error ? "unavailable" as const : feed.last_success_at ? "healthy" as const : "unknown" as const;
      if (error) incidents.push({ key: `feed:${feed.id}`, title: label(feed.provider), detail: error.message, code: error.code,
        jobId: null, href: "/movie-dual-write", recovery: nextPollAt ? "scheduled-wake" : "unknown", nextWakeAt: nextPollAt });
      return { id: label(feed.id), provider: label(feed.provider), mediaType: label(feed.media_type), state,
        lastSuccessAt: iso(feed.last_success_at), lastPolledAt: iso(feed.last_polled_at), retryAt, nextPollAt, error, gap: Boolean(feed.gap_since) };
    });
  } catch { warnings.push("Provider feed observations are unavailable; provider health is unknown."); }
  const storage = await storageView(repo, settings);
  for (const store of storage) if (store.identityState === "mismatch" || store.capacityState === "known" && store.availableBytes === 0)
    incidents.push({ key: `storage:${store.media}`, title: `${store.media} storage`, detail: store.reason,
      code: store.identityState === "mismatch" ? "storage-identity" : "storage-capacity", jobId: null, href: "/retention", recovery: "manual-retry", nextWakeAt: null });
  let recentJobs: PublicJob[] = [];
  if (includeJobs) {
    try { recentJobs = repo.listRecentJobs(30).map((job) => publicJob(job, now, retryPending.has(job.id))); }
    catch { warnings.push("Recent execution diagnostics could not be read."); }
  }
  const wakes = jobs.filter((job) => active(job) && !repo.isJobCancellationRequested(job.id))
    .flatMap((job) => iso(job.next_run_at) ? [iso(job.next_run_at)!] : []).sort();
  const truncated = inventory.truncated || acquisitions.length > 100 || downloads.length > 100 || incidents.length > 100 || feeds.length > 100;
  return { observedAt: new Date(now).toISOString(), summary: { activeAcquisitions: acquisitions.length, downloads: downloads.length, incidents: incidents.length, waiting, nextWakeAt: wakes[0] ?? null },
    activeAcquisitions: acquisitions.slice(0, 100), downloads: downloads.slice(0, 100), incidents: incidents.slice(0, 100), feeds: feeds.slice(0, 100), storage,
    diagnostics: { recentJobs, warnings: [...new Set(warnings)], truncated } };
}

export function createAdminWebRouter(deps: AdminWebDependencies = {}) {
  const now = deps.now ?? Date.now;
  const fetchServers = deps.fetchPlexServers ?? fetchPlexServerDevices;
  return defineRouter({
    users: defineMethod({ auth: "admin", readOnly: true, summary: "Read user account metadata, never sessions or credentials.", paramsSchema: empty, resultSchema: adminWebUsersSchema,
      handler(ctx) {
        const label = labelSanitizer(settingsForRead(ctx.repo));
        return { users: ctx.repo.listUsers().map((user) => ({ id: user.id, provider: user.provider, username: label(user.username),
          displayName: user.display_name === null ? null : label(user.display_name), isAdmin: user.is_admin === 1, createdAt: user.created_at, updatedAt: user.updated_at })) };
      } }),
    plexServers: defineMethod({ auth: "admin", readOnly: true, summary: "Read Plex access-control server metadata with token-scoped cache freshness.", paramsSchema: empty, resultSchema: adminWebPlexServersSchema,
      async handler(ctx) {
        const settings = settingsForRead(ctx.repo), token = settings.auth.plex.ownerToken;
        const expiry = decodePlexTokenExpiry(token);
        const result: z.infer<typeof adminWebPlexServersSchema> = { authMode: "plex", ownerTokenConfigured: Boolean(token),
          ownerTokenExpired: token && expiry.expSeconds !== null ? expiry.expired : null,
          selectedMachineId: machineId.safeParse(settings.auth.plex.machineId).success && labelSanitizer(settings)(settings.auth.plex.machineId) === settings.auth.plex.machineId ? settings.auth.plex.machineId : null,
          servers: [], source: token ? "unavailable" : "not-configured", fetchedAt: null, error: null };
        if (!token) return result;
        const cached = cachedServers(ctx.repo, settings, now());
        if (cached && now() - cached.fetchedAtMs < CACHE_TTL) return { ...result, servers: cached.servers, source: "cache", fetchedAt: new Date(cached.fetchedAtMs).toISOString() };
        try {
          const fetched = await fetchServers(settings);
          if (settingsForRead(ctx.repo).auth.plex.ownerToken !== token) throw new Error("Owner identity changed");
          result.servers = publicServers(fetched.filter((server) => server.provides.includes("server")), settings);
          storeServers(ctx.repo, settings, result.servers, now());
          result.source = "fresh"; result.fetchedAt = new Date(now()).toISOString();
        } catch {
          if (settingsForRead(ctx.repo).auth.plex.ownerToken !== token) return { ...result, selectedMachineId: null, error: "Plex owner identity changed; reload server discovery." };
          result.error = "Plex server discovery failed; check the owner account connection and try again.";
          if (cached) { result.servers = cached.servers; result.source = "stale-cache"; result.fetchedAt = new Date(cached.fetchedAtMs).toISOString(); }
        }
        return result;
      } }),
    selectPlexServer: defineMethod({ auth: "admin", summary: "Select an exact freshly verified Plex server without replacing other settings.",
      paramsSchema: z.object({ machineId }).strict(), resultSchema: z.object({ selectedMachineId: z.string() }),
      async handler(ctx, params) {
        const settings = settingsForRead(ctx.repo), token = settings.auth.plex.ownerToken;
        if (!token) throw new RpcError(-32602, "Link a Plex owner account before selecting a server");
        let devices: PlexServerDevice[];
        try { devices = await fetchServers(settings); } catch { throw new RpcError(-32000, "Plex server discovery failed; selection was not changed"); }
        const matches = devices.filter((server) => server.machineId === params.machineId && server.provides.includes("server"));
        if (matches.length !== 1 || !publicServers(matches, settings).some((server) => server.machineId === params.machineId))
          throw new RpcError(-32602, "Select a server from the freshly verified Plex server list");
        return ctx.repo.transaction(() => {
          const current = settingsForRead(ctx.repo);
          if (current.auth.plex.ownerToken !== token) throw new RpcError(-32000, "Plex owner identity changed; reload server discovery before selecting");
          ctx.repo.updateSettings({ ...current, auth: { ...current.auth, plex: { ...current.auth.plex, machineId: params.machineId } } });
          storeServers(ctx.repo, current, publicServers(devices.filter((server) => server.provides.includes("server")), current), now());
          return { selectedMachineId: params.machineId };
        });
      } }),
    ops: defineMethod({ auth: "admin", readOnly: true, summary: "Read active media work, genuine failures, provider cooldowns, and verified/unknown storage facts.",
      paramsSchema: z.object({ includeJobs: z.boolean().default(false) }).strict(), resultSchema: adminWebOpsSchema,
      handler: (ctx, params) => operationalView(ctx.repo, settingsForRead(ctx.repo), params.includeJobs, now(), deps.transmission) }),
    refresh: defineMethod({ auth: "admin", summary: "Queue one bounded full-library refresh through the existing transactional outbox.", paramsSchema: empty,
      resultSchema: z.object({ jobId: id, shared: z.boolean() }),
      handler(ctx) {
        return ctx.repo.transaction(() => {
          const pendingRetries = ctx.repo.listJobCommands().filter((command) => command.kind === "retry" && command.generation === ctx.repo.jobExecutionGeneration(command.job_id));
          const existing = ctx.repo.listJobsByType("full_library_refresh").find((job) =>
            (active(job) || pendingRetries.some((command) => command.job_id === job.id)) && !ctx.repo.isJobCancellationRequested(job.id));
          if (existing) return { jobId: existing.id, shared: true };
          const job = ctx.repo.createJob({ type: "full_library_refresh", maxAttempts: 2, idempotencyKey: `admin-refresh:${randomUUID()}` });
          return { jobId: job.id, shared: false };
        });
      } }),
  });
}

export const adminWebRouter = createAdminWebRouter();
