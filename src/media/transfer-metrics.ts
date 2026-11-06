import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { z } from "zod";
import { transferMetricsStatusSchema, transferMetricsStatusParamsSchema, mapTrackerSchema, type TransferMetricsStatus } from "./transfer-metrics-types";
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
export function normalizeTrackerHost(raw: string): string {
  const host = raw.trim().toLowerCase().replace(/\.$/, "");
  if (host.length > 253 || !host.includes(".") || !host.split(".").every(part => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(part)))
    throw new Error("Provide a tracker hostname, not an announce URL or credential");
  return host;
}
export function advanceTransferCounter(previousRaw: number | null, previousTotal: number | null, raw: number | null) {
  if (raw === null) return previousTotal;
  if (previousRaw === null || previousTotal === null) return raw;
  return previousTotal + (raw >= previousRaw ? raw - previousRaw : raw);
}
const sum = (values: (number | null)[]) => values.some(n => n === null) ? null : values.reduce<number>((a, b) => a + b!, 0);

/** Sanitized domain telemetry only; no execution state, torrent URLs or daemon credentials. */
export class TransferMetricsRepo {
  constructor(private db: Database, private beforeWrite: () => void = () => {},
    private jobState: (id: number) => { status: string } | null = () => null) {}
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
  provider(row: MetricInventory, receipt?: CrossSeedReceipt): string | null {
    if (receipt?.command.releaseId) {
      const found = this.db.query("SELECT json_extract(payload_json,'$.provider') AS provider FROM release_inbox WHERE id=?").get(receipt.command.releaseId) as { provider: string } | null;
      if (found) return found.provider;
    }
    const release = this.db.query("SELECT DISTINCT provider FROM releases WHERE info_hash=? LIMIT 2").all(row.native_id) as { provider: string }[];
    if (release.length === 1) return release[0]!.provider;
    const evidence = this.db.query(`SELECT value_json FROM app_state WHERE key LIKE 'cross-seed:selected-copy:%' AND
      (json_extract(value_json,'$.offer.key')=? OR json_extract(value_json,'$.selectedOfferKey')=?) LIMIT 2`).all(row.offer_key, row.offer_key) as { value_json: string }[];
    const names = [...new Set(evidence.map(item => { const copy = JSON.parse(item.value_json); return copy.offer.key === row.offer_key ? copy.offer.provider : null; }).filter(Boolean))];
    if (!names.length) {
      const selected = this.db.query(`SELECT json_extract(value_json,'$.selected.provider') AS provider FROM app_state
        WHERE key LIKE 'release-selection:%' AND json_extract(value_json,'$.selected.key')=? LIMIT 2`).all(row.offer_key) as { provider: string }[];
      names.push(...new Set(selected.map(item => item.provider).filter(Boolean)));
    }
    return names.length === 1 ? names[0] : null;
  }
  inferTracker(host: string, name: string | null, now: string) {
    if (name !== null && (!name.trim() || name.length > 100 || /[\x00-\x1f]|:\/\//.test(name))) return;
    this.beforeWrite();
    this.db.query(`INSERT INTO transfer_tracker_mappings(hostname,inferred_name,updated_at) VALUES(?,?,?)
      ON CONFLICT(hostname) DO UPDATE SET inferred_name=CASE WHEN excluded.inferred_name IS NULL THEN inferred_name
        WHEN inferred_name=excluded.inferred_name OR inferred_name IS NULL THEN excluded.inferred_name ELSE '' END,updated_at=excluded.updated_at`)
      .run(normalizeTrackerHost(host), name?.trim() ?? null, now);
  }
  mapTracker(input: { host: string; name: string | null }) {
    const parsed = mapTrackerSchema.parse(input), host = normalizeTrackerHost(parsed.host);
    if (parsed.name && /[\x00-\x1f]|:\/\//.test(parsed.name)) throw new Error("Tracker name must not contain a URL or control characters");
    this.beforeWrite();
    this.db.query(`INSERT INTO transfer_tracker_mappings(hostname,manual_name,updated_at) VALUES(?,?,?)
      ON CONFLICT(hostname) DO UPDATE SET manual_name=excluded.manual_name,updated_at=excluded.updated_at`)
      .run(host, parsed.name, new Date().toISOString());
    return this.mappings();
  }
  mappings() {
    return (this.db.query("SELECT * FROM transfer_tracker_mappings ORDER BY hostname").all() as { hostname: string; inferred_name: string | null; manual_name: string | null }[])
      .map(row => ({ host: row.hostname, name: row.manual_name || row.inferred_name || "Unknown tracker",
        source: row.manual_name ? "manual" as const : row.inferred_name ? "inferred" as const : "unknown" as const }));
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
      const unambiguous = group.rows.every(row => row.attributionStable && new Set(row.trackers.map(t => identity(t.host))).size === 1);
      const uploadedBytes = unambiguous ? sum(group.rows.map(row => row.uploadedBytes)) : null;
      const downloadedBytes = unambiguous ? sum(group.rows.map(row => row.downloadedBytes)) : null;
      return { host: [...group.hosts].sort()[0]!, name: group.name, seedBytes: seeds.reduce((n, row) => n + row.sizeBytes!, 0),
        seedingTorrents: seeds.length, uploadedBytes, downloadedBytes, seedSeconds: sum(group.rows.map(row => row.seedSeconds)),
        ratio: uploadedBytes !== null && downloadedBytes !== null && downloadedBytes > 0 ? uploadedBytes / downloadedBytes : null };
    });
    const units = new Set(fresh.flatMap(row => row.contents.map(content => `${content.media}:${content.targetId}`)));
    const receipts = this.receipts(), terminal = receipts.filter(row => row.outcome || ["failed", "cancelled"].includes(this.jobState(row.jobId)?.status ?? "")), successes = terminal.filter(row => row.outcome?.kind === "seeding");
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
      reuse: { attempts: receipts.length, successful: successes.length, pending: receipts.length - terminal.length,
        unsupported: terminal.filter(row => row.outcome?.kind === "unsupported").length, matchRate: terminal.length ? successes.length / terminal.length : null,
        verifiedReusedBytes: successes.reduce((n, row) => n + (row.outcome?.reusedBytes ?? 0), 0),
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
