import type { TransferMetricsStatus } from "./transfer-metrics-types";

const quoted = (text: string) => `"${text.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"')}"`;
/** Saved low-cardinality telemetry only. Scraping must never collect or control torrents. */
export function transferMetricsPrometheus(status: TransferMetricsStatus | null, publishedCounts: Record<string, number> = {}): string {
  const lines: string[] = [];
  const metric = (name: string, description: string, type: "gauge" | "counter", rows: { value: number | null; labels?: Record<string, string> }[]) => {
    lines.push(`# HELP podible_${name} ${description}`, `# TYPE podible_${name} ${type}`);
    for (const { value, labels } of rows) if (value !== null && Number.isFinite(value))
      lines.push(`podible_${name}${labels ? `{${Object.entries(labels).map(([key, text]) => `${key}=${quoted(text)}`).join(",")}}` : ""} ${value}`);
  };
  const one = (value: number | null | undefined) => [{ value: value ?? null }];
  metric("published_library_items", "Recorded published library entries by kind; not a live filesystem check. Kinds overlap.", "gauge",
    Object.entries(publishedCounts).map(([kind, value]) => ({ value, labels: { kind } })));
  metric("transfer_metrics_fresh", "Whether every registered torrent has current verified telemetry.", "gauge", one(Number(status !== null && !status.stale)));
  if (!status) return lines.join("\n") + "\n";
  metric("transfer_metrics_observed_timestamp_seconds", "Last saved collection timestamp; not scrape time.", "gauge", one(status.observedAt ? Date.parse(status.observedAt) / 1000 : null));
  const current = status.stale ? null : status.summary;
  metric("seed_bytes", "Logical complete torrent bytes actively seeding, not physical disk usage.", "gauge", one(current?.seedBytes));
  metric("seeding_torrents", "Complete torrent registrations actively seeding.", "gauge", one(current?.seedingTorrents));
  metric("seeding_copies_per_content_target", "Actively seeding associations per observed owned content target, not episodes/files.", "gauge", one(current?.averageCopies));
  metric("active_downloads", "Observed owned registrations by download stage, excluding seeding.", "gauge",
    status.stale ? [] : ["downloading", "checking", "download-queued"].map(state => ({
      labels: { state }, value: status.downloads.filter(row => row.state === state).length,
    })));
  for (const [direction, bytes, rate] of [
    ["upload", status.summary?.uploadedBytes, current?.uploadBytesPerSecond],
    ["download", status.summary?.downloadedBytes, current?.downloadBytesPerSecond],
  ] as const) {
    metric(`${direction}ed_bytes_total`, "Observed client traffic, accumulated across counter resets and removed registrations.", "counter", one(bytes));
    metric(`${direction}_bytes_per_second`, "Latest sampled client transfer speed.", "gauge", one(rate));
  }
  metric("cross_seed_uploaded_bytes_total", "Client-reported upload from cross-seeded registrations; not counterfactual extra upload or account credit.", "counter", one(status.summary?.crossSeedUploadedBytes));
  metric("seed_seconds_total", "Observed cumulative torrent seed time, summed across registrations.", "counter", one(status.summary?.seedSeconds));
  metric("reuse_attempts_total", "Persisted reuse attempts including pending and unsupported cases.", "counter", one(status.reuse.attempts));
  metric("reuse_completed_total", "Reuse attempts that independently verified complete and started seeding.", "counter", one(status.reuse.successful));
  metric("reuse_pending", "Reuse attempts that have not reached a recorded outcome.", "gauge", one(status.reuse.pending));
  metric("reuse_tv_multifile_reused_bytes_total", "Reusable payload recorded in completed multi-file TV matches, including assumed matches.", "counter", one(status.reuse.tvMultiFileReusedBytes));
  metric("reuse_verified_bytes_total", "Verified payload reused by completed registrations; not physical bytes saved.", "counter", one(status.reuse.verifiedReusedBytes));
  metric("reuse_sidecar_payload_bytes_total", "Missing sidecar payload required by completed reuse; excludes protocol/retry overhead.", "counter", one(status.reuse.sidecarDownloadBytes));
  metric("reuse_repair_payload_bytes_total", "Missing content payload required by completed reuse; excludes protocol/retry overhead.", "counter", one(status.reuse.contentRepairBytes));
  for (const [name, description, field, type] of [
    ["seed_bytes", "Logical seed bytes per tracker; totals may overlap.", "seedBytes", "gauge"],
    ["seeding_torrents", "Complete active registrations per tracker; totals may overlap.", "seedingTorrents", "gauge"],
    ["uploaded_bytes_total", "Client upload attributable to one tracker only, not account credit.", "uploadedBytes", "counter"],
    ["downloaded_bytes_total", "Client download attributable to one tracker only, not account credit.", "downloadedBytes", "counter"],
    ["seed_seconds_total", "Torrent seed time associated with each tracker; totals may overlap.", "seedSeconds", "counter"],
    ["upload_bytes_per_second", "Sampled upload attributable to one tracker; unknown observations omitted.", "uploadBytesPerSecond", "gauge"],
    ["download_bytes_per_second", "Sampled download attributable to one tracker; unknown observations omitted.", "downloadBytesPerSecond", "gauge"],
  ] as const) metric(`tracker_${name}`, description, type, status.trackers.map(tracker => ({
    labels: { tracker: tracker.name, host: tracker.host }, value: status.stale && type === "gauge" ? null : tracker[field],
  })));
  return lines.join("\n") + "\n";
}
