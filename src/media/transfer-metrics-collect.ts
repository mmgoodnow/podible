import path from "node:path";
import { promises as fs } from "node:fs";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { WorkerContext } from "../worker/context";
import type { JobRow } from "../app-types";
import { durableStep } from "../worker/context";
import { stewardshipConnection, type StewardshipConnection } from "./torrent-stewardship";
import { verifyStewardshipDirectory, stewardshipBinding } from "./torrent-stewardship";
import { captureReuseRoot } from "./reuse-layout";
import { advanceTransferCounter, metricDigest, normalizeTrackerHost, type MetricInventory, type MetricRecord } from "./transfer-metrics";

const count = z.number().int().nonnegative().safe();
const nullableCount = count.nullish().transform(value => value ?? null);
const rowSchema = z.object({ hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()).max(100),
  status: count.max(6), totalSize: nullableCount, haveValid: nullableCount, haveUnchecked: nullableCount,
  uploadedEver: nullableCount, downloadedEver: nullableCount, rateUpload: nullableCount, rateDownload: nullableCount,
  secondsSeeding: nullableCount, error: count, trackerStats: z.unknown().optional() });
const fields = ["hashString", "downloadDir", "labels", "status", "totalSize", "haveValid", "haveUnchecked",
  "uploadedEver", "downloadedEver", "rateUpload", "rateDownload", "secondsSeeding", "error", "trackerStats"];
export type TransferMetricsAdapters = { connection?: StewardshipConnection | null; now?: () => number; assertActive?: () => void };

/** Hostnames and enumerated state only: tracker paths, passkeys and messages never leave this function. */
export function sanitizedMetricTrackers(raw: unknown, now: number): MetricRecord["trackers"] {
  if (!Array.isArray(raw) || raw.length > 100) return [];
  const trackers = new Map<string, "healthy" | "error" | "unknown">();
  for (const entry of raw) {
    if (!entry || typeof entry.announce !== "string") continue;
    try {
      const url = new URL(entry.announce);
      if (!["http:", "https:", "udp:"].includes(url.protocol)) continue;
      const host = normalizeTrackerHost(url.hostname);
      const recent = count.safeParse(entry.lastAnnounceTime).success && entry.lastAnnounceTime > 0 &&
        now / 1000 >= entry.lastAnnounceTime && now / 1000 - entry.lastAnnounceTime < 24 * 3600;
      const state = recent && entry.hasAnnounced === true && entry.lastAnnounceTimedOut === false
        ? entry.lastAnnounceSucceeded === true ? "healthy" : entry.lastAnnounceSucceeded === false ? "error" : "unknown" : "unknown";
      const previous = trackers.get(host);
      trackers.set(host, previous === "healthy" || state === "healthy" ? "healthy" : previous === "error" || state === "error" ? "error" : "unknown");
    } catch { /* Invalid URLs cannot become tracker identity. */ }
  }
  return [...trackers].map(([host, state]) => ({ host, state }));
}
async function directory(value: string) {
  const stat = await fs.lstat(value);
  if (!stat.isDirectory() || stat.isSymbolicLink() || await fs.realpath(value) !== value) throw new Error("Unverified torrent directory");
}
async function ownedDirectory(repo: BooksRepo, record: MetricInventory, clientId: string, actual: z.infer<typeof rowSchema>) {
  if (record.client_id !== clientId || actual.hashString.toLowerCase() !== record.native_id || !/^[a-f0-9]{40}$/.test(record.native_id))
    throw new Error("Torrent identity mismatch");
  const location = z.object({ rootId: z.enum(["books", "movies", "tv"]), relativePath: z.string().min(1) }).strict().parse(JSON.parse(record.location_json));
  if (path.isAbsolute(location.relativePath) || /[\\\x00-\x1f]/.test(location.relativePath) ||
    location.relativePath.split("/").some(part => !part || part === "." || part === "..")) throw new Error("Torrent location mismatch");
  const tag = location.rootId === "books" ? "Podible Books" : location.rootId === "movies" ? "Podible Movies" : "Podible TV";
  if (record.ownership_tag !== tag || !actual.labels.includes(tag)) throw new Error("Torrent ownership mismatch");
  const settings = repo.readSettings();
  if (!settings) throw new Error("Missing settings");
  const managed = location.rootId === "books" ? path.join(await fs.realpath(settings.libraryRoot), ".podible", "downloads")
    : path.join(location.rootId === "movies" ? settings.movies.managedRoot : settings.tv.managedRoot, "downloads");
  const expected = path.join(managed, location.relativePath);
  if (actual.downloadDir !== expected) throw new Error("Torrent layout mismatch");
  await directory(managed); await directory(expected);
  if (record.book_id !== null) {
    const binding = stewardshipBinding(repo, "books", { ...record, info_hash: record.native_id, backend: "transmission",
      root_id: location.rootId, relative_path: location.relativePath }, clientId);
    if (!binding) throw new Error("Book binding mismatch");
    await verifyStewardshipDirectory(repo, binding);
  } else if (location.rootId === "books") {
    const owner = repo.crossSeed.registrationOwner({ clientId, nativeId: record.native_id });
    const receipt = owner ? repo.crossSeed.receipt(owner) : null;
    if (!receipt || !["audio", "ebook"].includes(receipt.command.scope.media)) throw new Error("Unknown book reuse owner");
    const ownedRoot = await captureReuseRoot({ rootPath: path.join(managed, "reuse"), markerName: ".podible-reuse-root.json", ownerId: clientId });
    if (!expected.startsWith(ownedRoot.path + path.sep)) throw new Error("Book reuse escaped its root");
  }
}

/** Bounded batches of every registered identity, independent of domain acquisition tables. No controls. */
export async function collectTransferMetrics(repo: BooksRepo, adapters: TransferMetricsAdapters = {}) {
  const now = (adapters.now ?? Date.now)(), at = new Date(now).toISOString(), metrics = repo.transferMetrics;
  adapters.assertActive?.();
  repo.transaction(() => metrics.learnCachedTrackers(at));
  const connection = stewardshipConnection({ connection: adapters.connection });
  const receipts = metrics.receipts(), byOwner = new Map(receipts.map(receipt => [receipt.command.operationId, receipt]));
  let after = 0, scanned = 0, unavailableReason: string | null = connection ? null : "Private Transmission telemetry is unavailable.";
  for (;;) {
    adapters.assertActive?.();
    const records = metrics.inventory(after, 100);
    if (!records.length) break;
    if (scanned >= 10000) { unavailableReason = "The collection bound was reached; remaining records are not reported as zero."; break; }
    let rows: z.infer<typeof rowSchema>[] = [];
    if (connection) {
      try {
        const own = records.filter(record => record.client_id === connection.clientId);
        if (own.length) {
          const response = await connection.get(own.map(record => record.native_id), fields);
          if (!Array.isArray(response.torrents) || response.torrents.length > 100) throw new Error("Invalid telemetry batch");
          rows = response.torrents.flatMap(value => { const parsed = rowSchema.safeParse(value); return parsed.success ? [parsed.data] : []; });
        }
      } catch { unavailableReason = "Some private Transmission telemetry could not be collected."; }
    }
    adapters.assertActive?.();
    for (const record of records) {
      const previous = metrics.record(record.id);
      const owner = repo.crossSeed.registrationOwner({ clientId: record.client_id, nativeId: record.native_id });
      const receipt = owner ? byOwner.get(owner) : undefined;
      let metric: MetricRecord = { id: record.id, observedAt: at, verified: false, state: "unknown", complete: false,
        sizeBytes: null, bytesDone: null, uploadedEver: previous?.uploadedEver ?? null, downloadedEver: previous?.downloadedEver ?? null,
        uploadedBytes: previous?.uploadedBytes ?? null, downloadedBytes: previous?.downloadedBytes ?? null,
        uploadBytesPerSecond: null, downloadBytesPerSecond: null, seedSeconds: previous?.seedSeconds ?? null,
        seedSecondsRaw: previous?.seedSecondsRaw ?? null, resets: previous?.resets ?? 0,
        crossSeed: !!receipt, contents: metrics.contents(record, receipt), trackers: previous?.trackers ?? [], attributionStable: previous?.attributionStable ?? true };
      try {
        const matches = rows.filter(row => row.hashString.toLowerCase() === record.native_id);
        if (!connection || matches.length !== 1) throw new Error("Missing exact owned telemetry");
        const row = matches[0]!;
        await ownedDirectory(repo, record, connection.clientId, row);
        const complete = row.totalSize !== null && row.haveValid === row.totalSize && row.haveUnchecked === 0 && row.error !== 3;
        const trackers = sanitizedMetricTrackers(row.trackerStats, now);
        metric = { ...metric, verified: true, state: ["paused", "checking", "checking", "download-queued", "downloading", "seed-queued", "seeding"][row.status]!,
          complete, sizeBytes: row.totalSize, bytesDone: row.haveValid, uploadBytesPerSecond: row.rateUpload, downloadBytesPerSecond: row.rateDownload,
          uploadedBytes: advanceTransferCounter(previous?.uploadedEver ?? null, previous?.uploadedBytes ?? null, row.uploadedEver),
          downloadedBytes: advanceTransferCounter(previous?.downloadedEver ?? null, previous?.downloadedBytes ?? null, row.downloadedEver),
          uploadedEver: row.uploadedEver ?? previous?.uploadedEver ?? null, downloadedEver: row.downloadedEver ?? previous?.downloadedEver ?? null,
          seedSeconds: advanceTransferCounter(previous?.seedSecondsRaw ?? null, previous?.seedSeconds ?? null, row.secondsSeeding),
          seedSecondsRaw: row.secondsSeeding ?? previous?.seedSecondsRaw ?? null, trackers,
          attributionStable: metric.attributionStable && (!previous?.trackers.length ||
            metricDigest(previous.trackers.map(t => t.host).sort()) === metricDigest(trackers.map(t => t.host).sort())),
          resets: metric.resets + Number(row.uploadedEver !== null && previous?.uploadedEver != null && row.uploadedEver < previous.uploadedEver ||
            row.downloadedEver !== null && previous?.downloadedEver != null && row.downloadedEver < previous.downloadedEver) };
      } catch { /* Unverified observations retain historical counters, never current rates/seed size. */ }
      adapters.assertActive?.();
      repo.transaction(() => {
        metrics.writeRecord(record, metric);
        if (metric.verified) {
          if (metric.trackers.length === 1) metrics.inferTracker(metric.trackers[0]!.host, metrics.provider(record, receipt), at);
          for (const tracker of metric.trackers) metrics.inferTracker(tracker.host, null, at);
        }
      });
    }
    scanned += records.length; after = records.at(-1)!.id;
  }
  adapters.assertActive?.();
  repo.transaction(() => metrics.finishCollection(at, unavailableReason));
  return { observedAt: at, scanned, unavailableReason };
}
export async function processTransferMetrics(ctx: WorkerContext, _job: JobRow): Promise<"done"> {
  await durableStep(ctx, "metrics:collect", () => collectTransferMetrics(ctx.repo, {
    ...ctx.transferMetricsAdapters, assertActive: () => ctx.execution?.assertActive(),
  }));
  return "done";
}
