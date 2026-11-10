import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { z } from "zod";
import { transferMetricsStatusSchema, transferMetricsStatusParamsSchema, type TransferMetricsStatus } from "./transfer-metrics-types";
import { metainfoTrackerIdentity, normalizeTrackerHost } from "./tracker-identity";
export { normalizeTrackerHost } from "./tracker-identity";
import type { CrossSeedReceipt } from "./cross-seed-state";

export type MetricContent = { media: "audio" | "ebook" | "movie" | "tv"; targetId: number; title: string; href: string };
export type MetricRecord = {
  id: number; observedAt: string; verified: boolean; state: string; sizeBytes: number | null; bytesDone: number | null;
  uploadedEver: number | null; downloadedEver: number | null; uploadedBytes: number | null; downloadedBytes: number | null;
  uploadBytesPerSecond: number | null; downloadBytesPerSecond: number | null; seedSeconds: number | null;
  complete: boolean; crossSeed: boolean; resets: number; attributionStable: boolean; contents: MetricContent[];
  seedSecondsRaw?: number | null;
  trackers: { host: string; state: "healthy" | "error" | "unknown" }[];
};
export type MetricInventory = { id: number; client_id: string; native_id: string; location_json: string;
  ownership_tag: string | null; root_path: string | null; root_token: string | null;
  book_id: number | null; release_id: number | null; offer_key: string };
export const metricDigest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
export function advanceTransferCounter(previousRaw: number | null, previousTotal: number | null, raw: number | null) {
  if (raw === null) return previousTotal;
  if (previousRaw === null || previousTotal === null) return raw;
  return previousTotal + (raw >= previousRaw ? raw - previousRaw : raw);
}
const sum = (values: (number | null)[]) => values.some(n => n === null) ? null : values.reduce<number>((a, b) => a + b!, 0);
const trackerCacheCursor = "transfer-metrics:tracker-cache-source-cursor";
function safeTrackerName(name: string) {
  return name.trim() && name.length <= 100 && !/[\x00-\x1f]|:\/\//.test(name);
}

function trackerSource(provider: string, downloadUrl?: string, prowlarrUrl?: string | null) {
  const base = prowlarrUrl ? URL.parse(prowlarrUrl) : null;
  const download = downloadUrl ? URL.parse(downloadUrl) : null;
  if (base && download && /^https?:$/.test(base.protocol) && base.origin === download.origin) {
    const prefix = base.pathname.replace(/\/+$/, "") + "/";
    const match = download.pathname.startsWith(prefix) && /^(\d+)\/download\/?$/.exec(download.pathname.slice(prefix.length));
    if (match) return metricDigest(["prowlarr", base.origin, prefix, match[1]]);
  }
  return metricDigest(["provider", provider.toLowerCase()]);
}

/** Sanitized domain telemetry only; no execution state, torrent URLs or daemon credentials. */
export class TransferMetricsRepo {
  constructor(private db: Database, private beforeWrite: () => void = () => {},
    private jobState: (id: number) => { status: string } | null = () => null,
    private prowlarrUrl: () => string | null = () => null) {}
  inventory(after = 0, limit = 100): MetricInventory[] {
    return this.db.query(`SELECT t.id,t.client_id,t.native_id,t.location_json,t.ownership_tag,t.root_path,t.root_token,t.offer_key,
      b.book_id,b.release_id FROM media_torrents t LEFT JOIN book_torrent_bindings b ON b.torrent_id=t.id
      WHERE t.id>? ORDER BY t.id LIMIT ?`).all(after, limit) as MetricInventory[];
  }
  inventoryCount() { return (this.db.query("SELECT COUNT(*) AS n FROM media_torrents").get() as { n: number }).n; }
  record(id: number): MetricRecord | null {
    const row = this.db.query("SELECT snapshot_json FROM transfer_metric_records WHERE torrent_id=?").get(id) as { snapshot_json: string } | null;
    return row ? JSON.parse(row.snapshot_json) : null;
  }
  writeRecord(expected: MetricInventory, record: MetricRecord) {
    this.beforeWrite();
    const current = this.inventory(expected.id - 1, 1)[0];
    if (!current || metricDigest(current) !== metricDigest(expected)) return;
    const previous = this.record(record.id);
    if (previous && previous.observedAt >= record.observedAt) return;
    this.db.query(`INSERT INTO transfer_metric_records VALUES(?,?,?,?) ON CONFLICT(torrent_id) DO UPDATE SET
      binding_digest=excluded.binding_digest,observed_at=excluded.observed_at,snapshot_json=excluded.snapshot_json`)
      .run(record.id, metricDigest(expected), record.observedAt, JSON.stringify(record));
  }
  receipts(): CrossSeedReceipt[] {
    return (this.db.query("SELECT value_json FROM app_state WHERE key>='cross-seed:operation:' AND key<'cross-seed:operation:~'")
      .all() as { value_json: string }[]).map(row => JSON.parse(row.value_json));
  }
  contents(row: MetricInventory, receipt?: CrossSeedReceipt): MetricContent[] {
    if (receipt) {
      const { media, targetId } = receipt.command.scope;
      if (media === "audio" || media === "ebook") {
        const book = this.db.query("SELECT title FROM books WHERE id=?").get(targetId) as { title: string } | null;
        return book ? [{ media, targetId, title: book.title, href: `/book/${targetId}` }] : [];
      }
      if (media === "movie") {
        const movie = this.db.query(`SELECT m.tmdb_id,m.metadata_json FROM movie_targets t JOIN movies m ON m.id=t.movie_id WHERE t.id=?`).get(targetId) as { tmdb_id: number; metadata_json: string } | null;
        return movie ? [{ media, targetId, title: JSON.parse(movie.metadata_json).title, href: `/movies/${movie.tmdb_id}` }] : [];
      }
      const entry = this.db.query(`SELECT e.id,e.metadata_json FROM tv_targets t JOIN tv_entries e ON e.id=t.entry_id WHERE t.id=?`).get(targetId) as { id: number; metadata_json: string } | null;
      return entry ? [{ media, targetId, title: JSON.parse(entry.metadata_json).title, href: `/tv/library/${entry.id}` }] : [];
    }
    if (row.book_id !== null) {
      const book = this.db.query("SELECT b.title,r.media_type FROM books b LEFT JOIN releases r ON r.id=? WHERE b.id=?").get(row.release_id, row.book_id) as { title: string; media_type: string | null } | null;
      return book && (book.media_type === "audio" || book.media_type === "ebook")
        ? [{ media: book.media_type, targetId: row.book_id, title: book.title, href: `/book/${row.book_id}` }] : [];
    }
    const refs = this.db.query(`SELECT a.target_id,'movie' AS media FROM movie_acquisition_downloads d JOIN movie_acquisitions a ON a.id=d.acquisition_id WHERE d.torrent_id=?
      UNION SELECT a.target_id,'tv' FROM tv_acquisition_downloads d JOIN tv_acquisitions a ON a.id=d.acquisition_id WHERE d.torrent_id=?`).all(row.id, row.id) as { target_id: number; media: "movie" | "tv" }[];
    return refs.flatMap(ref => this.contents(row, { command: { scope: { media: ref.media, targetId: ref.target_id } } } as CrossSeedReceipt));
  }
  learnMetainfo(bytes: Uint8Array, provider: string | null, now = new Date().toISOString(), downloadUrl?: string) {
    this.learnTrackerEvidence(bytes, provider, now, downloadUrl, downloadUrl ? this.prowlarrUrl() : null);
  }
  private learnTrackerEvidence(bytes: Uint8Array, provider: string | null, now: string, downloadUrl?: string, prowlarrUrl?: string | null) {
    if (!provider || !safeTrackerName(provider)) return;
    const { hosts, privateTorrent } = metainfoTrackerIdentity(bytes);
    const source = trackerSource(provider.trim(), downloadUrl, prowlarrUrl);
    // Private providers can publish backup announce aliases. Public multi-tracker lists are not identity evidence.
    for (const host of hosts) this.inferTracker(host, privateTorrent || hosts.length === 1 ? provider : null, now, source);
  }
  /** Gradual local backfill; never downloads metainfo or scans the filesystem on a dashboard read. */
  learnCachedTrackers(now: string) {
    const state = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(trackerCacheCursor) as { value_json: string } | null;
    const after = state ? JSON.parse(state.value_json) as string | null : "";
    if (after === null) return;
    const prowlarrUrl = this.prowlarrUrl();
    const rows = this.db.query("SELECT key,provider,length(torrent_bytes) AS size FROM torrent_cache WHERE key>? ORDER BY key LIMIT 100").all(after) as { key: string; provider: string | null; size: number }[];
    let processed = 0, budget = 0, last = after;
    for (const row of rows) {
      if (processed && budget + row.size > 16 * 1024 * 1024) break;
      processed++; last = row.key;
      if (!row.provider || row.size > 8 * 1024 * 1024) continue;
      const cached = this.db.query("SELECT torrent_bytes,url,updated_at FROM torrent_cache WHERE key=?").get(row.key) as
        { torrent_bytes: Uint8Array; url: string; updated_at: string };
      this.learnTrackerEvidence(cached.torrent_bytes, row.provider, cached.updated_at, cached.url, prowlarrUrl);
      budget += row.size;
    }
    const next = processed < rows.length || rows.length === 100 ? last : null;
    this.beforeWrite();
    this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
      .run(trackerCacheCursor, JSON.stringify(next), now);
  }
  inferTracker(host: string, name: string | null, now: string, source?: string) {
    if (name !== null && !safeTrackerName(name)) return;
    this.beforeWrite();
    host = normalizeTrackerHost(host);
    if (name === null) {
      this.db.run("INSERT OR IGNORE INTO transfer_tracker_mappings(hostname,inferred_name,updated_at) VALUES(?,NULL,?)", [host, now]);
      return;
    }
    const label = name.trim(), id = source ?? trackerSource(label);
    const key = `transfer-metrics:tracker-sources:${host}`;
    this.db.transaction(() => {
      const saved = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(key) as { value_json: string } | null;
      const evidence = saved ? JSON.parse(saved.value_json) as Record<string, { label: string; at: string }> : {};
      const previous = evidence[id];
      // Replaying older cache entries cannot undo a provider rename.
      if (!previous || now > previous.at || (now === previous.at && label < previous.label)) {
        evidence[id] = { label, at: now };
        this.db.run(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
          ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`, [key, JSON.stringify(evidence), now]);
      }
      const names = Object.values(evidence).sort((a, b) => b.at.localeCompare(a.at) || a.label.localeCompare(b.label));
      const preferred = names[0]!.label;
      const inferred = names.every(entry => entry.label.toLowerCase() === preferred.toLowerCase()) ? preferred : "";
      this.db.run(`INSERT INTO transfer_tracker_mappings(hostname,inferred_name,updated_at) VALUES(?,?,?)
        ON CONFLICT(hostname) DO UPDATE SET inferred_name=excluded.inferred_name,updated_at=excluded.updated_at
        WHERE inferred_name IS NOT excluded.inferred_name`, [host, inferred, names[0]!.at]);
    })();
  }
  mappings() {
    return (this.db.query("SELECT hostname,inferred_name FROM transfer_tracker_mappings ORDER BY hostname").all() as { hostname: string; inferred_name: string | null }[])
      .map(row => ({ host: row.hostname, name: row.inferred_name || "Unknown tracker",
        source: row.inferred_name ? "inferred" as const : "unknown" as const }));
  }
  finishCollection(now: string, unavailableReason: string | null) {
    this.beforeWrite();
    const previous = this.db.query("SELECT value_json FROM app_state WHERE key='transfer-metrics:collection'").get() as { value_json: string } | null;
    if (previous && JSON.parse(previous.value_json).at > now) return;
    const since = previous ? JSON.parse(previous.value_json).since ?? JSON.parse(previous.value_json).at : now;
    this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES('transfer-metrics:collection',?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
      .run(JSON.stringify({ at: now, since, unavailableReason }), now);
    const snapshot = this.status({ hours: 24 }, Date.parse(now));
    for (const resolution of ["raw", "hourly"]) {
      const bucket = now.slice(0, resolution === "raw" ? 16 : 13);
      this.db.query(`INSERT INTO transfer_metric_samples VALUES(?,?,?,?) ON CONFLICT(resolution,bucket) DO UPDATE SET
        observed_at=excluded.observed_at,snapshot_json=excluded.snapshot_json`).run(resolution, bucket, now, JSON.stringify({ ...snapshot, history: [] }));
    }
    this.db.query("DELETE FROM transfer_metric_samples WHERE resolution='raw' AND observed_at<?").run(new Date(Date.parse(now) - 24 * 3600_000).toISOString());
    this.db.query("DELETE FROM transfer_metric_samples WHERE resolution='hourly' AND observed_at<?").run(new Date(Date.parse(now) - 90 * 24 * 3600_000).toISOString());
  }
  scrapeSnapshot(now = Date.now()): TransferMetricsStatus | null {
    const row = this.db.query(`SELECT snapshot_json FROM transfer_metric_samples
      WHERE resolution='raw' ORDER BY bucket DESC LIMIT 1`).get() as { snapshot_json: string } | null;
    if (!row) return null;
    const sample = JSON.parse(row.snapshot_json) as TransferMetricsStatus;
    const age = sample.observedAt ? now - Date.parse(sample.observedAt) : NaN;
    return { ...sample, stale: sample.stale || !(age >= 0 && age <= 180_000) };
  }
  publishedCounts() {
    return this.db.query(`WITH published_books AS (
      SELECT m.book_id,m.kind FROM manifestations m JOIN assets a ON a.manifestation_id=m.id
      JOIN asset_files f ON f.asset_id=a.id WHERE m.preferred_score>=0 GROUP BY m.book_id,m.kind
    )
    SELECT (SELECT count(DISTINCT book_id) FROM published_books) AS books,
      (SELECT count(*) FROM published_books WHERE kind='ebook') AS ebooks,
      (SELECT count(*) FROM published_books WHERE kind='audio') AS audiobooks,
      (SELECT count(DISTINCT movie_id) FROM movie_targets WHERE active_version_id IS NOT NULL) AS movies,
      (SELECT count(DISTINCT entry_id) FROM tv_active_episodes) AS shows,
      (SELECT count(*) FROM (SELECT DISTINCT a.entry_id,n.season FROM tv_active_episodes a
        JOIN tv_episode_numbering n ON n.entry_id=a.entry_id AND n.episode_id=a.episode_id
        WHERE n.scheme='tvdb:aired' AND n.season IS NOT NULL)) AS seasons,
      (SELECT count(DISTINCT episode_id) FROM tv_active_episodes) AS episodes`).get() as Record<string, number>;
  }
  status(input: { hours?: 24 | 168 | 720 | 2160 } = {}, now = Date.now()): TransferMetricsStatus {
    const { hours } = transferMetricsStatusParamsSchema.parse(input);
    const saved = this.db.query("SELECT value_json FROM app_state WHERE key='transfer-metrics:collection'").get() as { value_json: string } | null;
    const collection = saved ? JSON.parse(saved.value_json) as { at: string; since: string; unavailableReason: string | null } : null;
    const all = (this.db.query(`SELECT r.snapshot_json,t.id AS live_id,r.binding_digest,t.client_id,t.native_id,t.location_json,t.ownership_tag,
      t.root_path,t.root_token,t.offer_key,b.book_id,b.release_id FROM transfer_metric_records r LEFT JOIN media_torrents t ON t.id=r.torrent_id
      LEFT JOIN book_torrent_bindings b ON b.torrent_id=t.id`).all() as (MetricInventory & { snapshot_json: string; live_id: number | null; binding_digest: string })[]);
    const current = all.filter(row => row.live_id !== null).map(row => ({ record: JSON.parse(row.snapshot_json) as MetricRecord,
      matches: row.binding_digest === metricDigest({ id: row.live_id, client_id: row.client_id, native_id: row.native_id,
        location_json: row.location_json, ownership_tag: row.ownership_tag, root_path: row.root_path, root_token: row.root_token,
        offer_key: row.offer_key, book_id: row.book_id, release_id: row.release_id }) }));
    const fresh = current.filter(({ record, matches }) => matches && record.verified && now >= Date.parse(record.observedAt) && now - Date.parse(record.observedAt) <= 180_000).map(row => row.record);
    const total = this.inventoryCount(), stale = !collection || !!collection.unavailableReason || fresh.length !== total || now - Date.parse(collection.at) > 180_000;
    const completeSample = !stale;
    const seeding = fresh.filter(row => row.state === "seeding" && row.complete);
    const totals = all.map(row => JSON.parse(row.snapshot_json) as MetricRecord);
    const freshIds = new Set(fresh.map(row => row.id));
    const liveIds = new Set(current.map(row => row.record.id));
    const mappings = this.mappings(), mapped = new Map(mappings.map(row => [row.host, row]));
    const trackerGroups = new Map<string, { hosts: Set<string>; name: string; rows: MetricRecord[] }>();
    const identity = (host: string) => mapped.get(host)?.source !== "unknown" && mapped.get(host) ? `name:${mapped.get(host)!.name.toLowerCase()}` : `host:${host}`;
    for (const row of totals) for (const key of new Set(row.trackers.map(tracker => identity(tracker.host)))) {
      const hosts = row.trackers.filter(tracker => identity(tracker.host) === key).map(tracker => tracker.host);
      const group = trackerGroups.get(key) ?? { hosts: new Set<string>(), name: mapped.get(hosts[0]!)?.name ?? "Unknown tracker", rows: [] };
      hosts.forEach(host => group.hosts.add(host)); group.rows.push(row); trackerGroups.set(key, group);
    }
    const trackers = [...trackerGroups.entries()].map(([key, group]) => {
      const seeds = group.rows.filter(row => freshIds.has(row.id) && row.state === "seeding" && row.complete);
      const completeGroup = group.rows.every(row => !liveIds.has(row.id) || freshIds.has(row.id));
      const unambiguous = group.rows.every(row => row.attributionStable && new Set(row.trackers.map(t => t.host)).size === 1);
      const uploadedBytes = unambiguous ? sum(group.rows.map(row => row.uploadedBytes)) : null;
      const downloadedBytes = unambiguous ? sum(group.rows.map(row => row.downloadedBytes)) : null;
      return { host: [...group.hosts].sort()[0]!, name: group.name, seedBytes: completeGroup ? seeds.reduce((n, row) => n + row.sizeBytes!, 0) : null,
        seedingTorrents: completeGroup ? seeds.length : null, uploadedBytes, downloadedBytes, seedSeconds: sum(group.rows.map(row => row.seedSeconds)),
        uploadBytesPerSecond: completeGroup && unambiguous ? sum(group.rows.filter(row => freshIds.has(row.id)).map(row => row.uploadBytesPerSecond)) : null,
        downloadBytesPerSecond: completeGroup && unambiguous ? sum(group.rows.filter(row => freshIds.has(row.id)).map(row => row.downloadBytesPerSecond)) : null,
        ratio: uploadedBytes !== null && downloadedBytes !== null && downloadedBytes > 0 ? uploadedBytes / downloadedBytes : null };
    });
    const units = new Set(fresh.flatMap(row => row.contents.map(content => `${content.media}:${content.targetId}`)));
    const receipts = this.receipts();
    const outcomeCounts = { seeding: 0, pending: 0, skipped: 0, failed: 0, stalled: 0, cancelled: 0, unsupported: 0 };
    for (const receipt of receipts) {
      // Durable validated outcomes take precedence over a later execution-state change.
      if (receipt.outcome) outcomeCounts[receipt.outcome.kind === "incompatible" ? "failed" : receipt.outcome.kind]++;
      else {
        const status = this.jobState(receipt.jobId)?.status;
        outcomeCounts[status === "failed" || status === "cancelled" ? status : "pending"]++;
      }
    }
    const terminalCount = receipts.length - outcomeCounts.pending;
    const successes = receipts.filter(row => row.outcome?.kind === "seeding");
    const accounted = successes.filter(row => row.outcome?.accountingVersion === 1 &&
      row.outcome.sidecarDownloadBytes !== undefined && row.outcome.contentRepairBytes !== undefined && row.outcome.contentFileCount !== undefined);
    const points = collection ? this.db.query(`SELECT observed_at,snapshot_json FROM transfer_metric_samples WHERE resolution=? AND observed_at>=? ORDER BY observed_at LIMIT 2160`)
      .all(hours === 24 ? "raw" : "hourly", new Date(now - hours * 3600_000).toISOString()) as { observed_at: string; snapshot_json: string }[] : [];
    return transferMetricsStatusSchema.parse({ trackingSince: collection?.since ?? null, observedAt: collection?.at ?? null, stale,
      unavailableReason: collection?.unavailableReason ?? (!collection ? "Transfer metrics have not been collected yet." : stale ? "Some registered torrents lack a current verified observation." : null),
      summary: collection ? { seedBytes: completeSample ? seeding.reduce((n, row) => n + row.sizeBytes!, 0) : null,
        seedingTorrents: completeSample ? seeding.length : null, uploadedBytes: sum(totals.map(row => row.uploadedBytes)), downloadedBytes: sum(totals.map(row => row.downloadedBytes)),
        uploadBytesPerSecond: completeSample ? sum(fresh.map(row => row.uploadBytesPerSecond)) : null,
        downloadBytesPerSecond: completeSample ? sum(fresh.map(row => row.downloadBytesPerSecond)) : null,
        crossSeedUploadedBytes: sum(totals.filter(row => row.crossSeed).map(row => row.uploadedBytes)), seedSeconds: sum(totals.map(row => row.seedSeconds)),
        contentUnits: units.size, averageCopies: units.size && completeSample ? seeding.reduce((n, row) => n + row.contents.length, 0) / units.size : null } : null,
      trackers, mappings, history: points.map(row => { const previous = JSON.parse(row.snapshot_json) as TransferMetricsStatus;
        return { at: row.observed_at, seedBytes: previous.summary?.seedBytes ?? null, seedingTorrents: previous.summary?.seedingTorrents ?? null,
          uploadedBytes: previous.summary?.uploadedBytes ?? null, downloadedBytes: previous.summary?.downloadedBytes ?? null,
          uploadBytesPerSecond: previous.summary?.uploadBytesPerSecond ?? null, downloadBytesPerSecond: previous.summary?.downloadBytesPerSecond ?? null,
          crossSeedUploadedBytes: previous.summary?.crossSeedUploadedBytes ?? null, seedSeconds: previous.summary?.seedSeconds ?? null,
          averageCopies: previous.summary?.averageCopies ?? null, reusedBytes: previous.reuse.verifiedReusedBytes,
          trackers: previous.stale ? [] : previous.trackers }; }),
      reuse: { attempts: receipts.length, successful: successes.length, pending: outcomeCounts.pending,
        skipped: outcomeCounts.skipped, failed: outcomeCounts.failed, stalled: outcomeCounts.stalled,
        cancelled: outcomeCounts.cancelled, unsupported: outcomeCounts.unsupported,
        matchRate: terminalCount ? successes.length / terminalCount : null,
        verifiedReusedBytes: successes.filter(row => row.outcome?.verification !== "assumed")
          .reduce((n, row) => n + (row.outcome?.reusedBytes ?? 0), 0),
        sidecarDownloadBytes: accounted.length === successes.length ? accounted.reduce((n, row) => n + (row.outcome?.sidecarDownloadBytes ?? 0), 0) : null,
        contentRepairBytes: accounted.length === successes.length ? accounted.reduce((n, row) => n + (row.outcome?.contentRepairBytes ?? 0), 0) : null,
        tvMultiFileReusedBytes: accounted.filter(row => row.command.scope.media === "tv" && (row.outcome?.contentFileCount ?? 0) > 1).reduce((n, row) => n + (row.outcome?.reusedBytes ?? 0), 0) },
      downloads: fresh.filter(row => ["downloading", "checking", "download-queued"].includes(row.state)).map(row => ({ id: row.id,
        title: row.contents[0]?.title ?? "Owned torrent", state: row.state, bytesDone: row.bytesDone, sizeBytes: row.sizeBytes,
        downloadBytesPerSecond: row.downloadBytesPerSecond, href: row.contents[0]?.href ?? null })),
      warnings: ["Seed size is logical torrent size, not physical disk usage. Coverage counts owned content targets, not files or episodes.",
        "Tracker traffic is attributable only when a torrent maps to exactly one tracker; it is not tracker account credit.",
        ...(stale ? ["Missing or stale telemetry is not zero activity."] : []),
        ...(totals.some(row => row.resets) ? ["Daemon transfer counters reset; persisted totals preserve observed traffic across resets."] : [])],
    });
  }
}
