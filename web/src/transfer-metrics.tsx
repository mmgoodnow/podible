import { useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { TransferMetricsStatus } from "../../src/media/transfer-metrics-types";
import { useTRPC } from "./rpc";
import { Link } from "./router";
import { metricBytes, metricDays } from "./transfer-metrics-model";
import { TransferHistory } from "./transfer-history";
import "./transfer-metrics.css";

const count = (value: number | null) => value === null ? "Unknown" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
const date = (value: string) => new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
const speed = (value: number | null) => value === null ? "Unknown" : `${metricBytes(value)}/s`;

function TrackerTable({ data }: { data: TransferMetricsStatus }) {
  const maximum = Math.max(1, ...data.trackers.map(row => row.seedBytes ?? 0));
  return <section className="transfer-trackers" aria-label="Seeding by tracker">
    <h3>By tracker</h3>
    {!data.trackers.length ? <p className="muted">Trackers appear after the first verified torrent observation.</p> : <div className="transfer-table-scroll"><table>
      <thead><tr><th>Tracker</th><th>Seed size{data.stale ? " (partial)" : ""}</th><th>Torrents</th><th>Uploaded</th><th>Downloaded</th><th>Ratio</th><th>Seed time</th></tr></thead>
      <tbody>{[...data.trackers].sort((a, b) => (b.seedBytes ?? -1) - (a.seedBytes ?? -1)).map(row => <tr key={row.host}>
        <th scope="row"><span>{row.name === "Unknown tracker" ? row.host : row.name}</span>{row.seedBytes !== null && <span className="transfer-tracker-bar" style={{ width: `${row.seedBytes / maximum * 100}%` }} />}</th>
        <td>{metricBytes(row.seedBytes)}</td><td>{count(row.seedingTorrents)}</td><td>{metricBytes(row.uploadedBytes)}</td>
        <td>{metricBytes(row.downloadedBytes)}</td><td>{row.ratio === null ? "—" : `${row.ratio.toFixed(2)}×`}</td><td>{metricDays(row.seedSeconds)}</td>
      </tr>)}</tbody>
    </table></div>}
    <p className="transfer-footnote">Names are learned from indexer metainfo. Traffic is client-reported, not tracker-account credit; seed time adds up across torrents.</p>
  </section>;
}

export function CrossSeedMetrics({ reuse }: { reuse: TransferMetricsStatus["reuse"] }) {
  return <section className="transfer-reuse" aria-label="Cross-seeding"><h3>Cross-seeding</h3>
    <dl>
      <div><dt>Seeds created</dt><dd>{count(reuse.successful)}</dd></div>
      <div><dt>Pending</dt><dd>{count(reuse.pending)}</dd></div>
      <div><dt>Skipped</dt><dd>{count(reuse.skipped)}</dd></div>
      {reuse.failed > 0 && <div><dt>Failed</dt><dd>{count(reuse.failed)}</dd></div>}
      {reuse.stalled > 0 && <div><dt>Stalled</dt><dd>{count(reuse.stalled)}</dd></div>}
      {reuse.unsupported > 0 && <div><dt>Unsupported</dt><dd>{count(reuse.unsupported)}</dd></div>}
      {reuse.cancelled > 0 && <div><dt>Cancelled</dt><dd>{count(reuse.cancelled)}</dd></div>}
      <div><dt>Side files downloaded</dt><dd>{metricBytes(reuse.sidecarDownloadBytes)}</dd></div>
      <div><dt>Content repairs</dt><dd>{metricBytes(reuse.contentRepairBytes)}</dd></div>
    </dl>
    {reuse.skipped > 0 && <p className="transfer-footnote">Skipped candidates are not failed cross-seeds.</p>}
  </section>;
}

export function useSharingMetrics() {
  const trpc = useTRPC();
  const [hours, setHours] = useState<24 | 168 | 720 | 2160>(24);
  const query = useQuery(trpc.transferMetrics.status.queryOptions({ hours }, { refetchInterval: 60_000, placeholderData: keepPreviousData }));
  return { query, hours, setHours };
}

export function SharingOverview({ metrics }: { metrics: ReturnType<typeof useSharingMetrics> }) {
  const { query } = metrics;
  const data = query.data, summary = data?.summary;
  return <section id="activity-sharing" className="activity-section transfer-dashboard" aria-label="Sharing and transfers">
    <div className="transfer-heading"><h2>Sharing</h2>{data &&
      <div className="activity-heading-meta">
        {data.observedAt && <span className="transfer-live">{data.stale ? "Live readings incomplete · Last sample" : "Sampled"} {date(data.observedAt)}</span>}
        <a href="#activity-sharing-history">View history</a>
      </div>}</div>
    {data ? <div className="transfer-overview">
      <article><h3>Seeding</h3><strong>{metricBytes(summary?.seedBytes ?? null)}</strong>
        <small>{summary?.seedingTorrents != null ? `${count(summary.seedingTorrents)} active torrents` : "Current seed size not verified"}
          {summary?.averageCopies != null ? ` · ${count(summary.averageCopies)} copies per target` : ""}</small></article>
      <article><h3>Cross-seeds started</h3><strong>{count(data.reuse.successful)}</strong><small>{metricBytes(data.reuse.verifiedReusedBytes)} of verified payload reused</small></article>
      <article><h3>Uploaded</h3><strong>{metricBytes(summary?.uploadedBytes ?? null)}</strong><small>{summary?.crossSeedUploadedBytes != null
        ? `${metricBytes(summary.crossSeedUploadedBytes)} from cross-seeded torrents` : "Cross-seed traffic not observed"}</small></article>
      <article><h3>Download speed</h3><strong>{speed(summary?.downloadBytesPerSecond ?? null)}</strong><small>{summary?.uploadBytesPerSecond != null
        ? `${speed(summary.uploadBytesPerSecond)} uploading at last sample` : "Upload speed not observed"}</small></article>
    </div> : query.isError ? <p role="alert">Sharing readings unavailable. <button className="secondary" onClick={() => query.refetch()}>Try again</button></p>
      : <p role="status">Loading sharing readings...</p>}
    {query.isRefetchError && <p className="muted">Could not refresh sharing readings. Showing the last response.</p>}
  </section>;
}

export function SharingHistory({ metrics }: { metrics: ReturnType<typeof useSharingMetrics> }) {
  const { query, hours, setHours } = metrics;
  const data = query.data;
  if (!data) return null;
  return <section id="activity-sharing-history" className="activity-section transfer-dashboard" aria-label="Sharing history">
    <h2>Sharing over time</h2>
    <TransferHistory data={data} hours={hours} setHours={setHours} />
    <p className="transfer-footnote">History since {data.trackingSince ? date(data.trackingSince) : "collection began"} · {hours === 24 ? "Minute samples" : "Hourly samples"}</p>
    <details className="activity-disclosure"><summary>Tracker totals and cross-seed outcomes</summary>
      <TrackerTable data={data} />
      <CrossSeedMetrics reuse={data.reuse} />
      <p className="transfer-footnote">{metricDays(data.summary?.seedSeconds ?? null)} combined seed time across all managed torrents.
        Seed size counts shared bytes once per torrent, not physical disk space. Transfer totals can include activity from before tracking began.</p>
    </details>
  </section>;
}

export function RecordedDownloads({ downloads }: { downloads: TransferMetricsStatus["downloads"] }) {
  if (!downloads.length) return null;
  return <section className="transfer-downloads" aria-label="Other transfers">
    <h3>Other transfers <small>{downloads.length}</small></h3>
    {downloads.map(download => <article key={download.id}>
      <div>{download.href ? <Link href={download.href}>{download.title}</Link> : <strong>{download.title}</strong>}
        <span>{download.state === "checking" ? "Checking files" : download.state === "download-queued" ? "Queued" : speed(download.downloadBytesPerSecond)}</span></div>
      {download.sizeBytes !== null && download.sizeBytes > 0 && download.bytesDone !== null &&
        <progress aria-label={`${download.title} download`} max={download.sizeBytes} value={download.bytesDone} />}
      <small>{metricBytes(download.bytesDone)} / {metricBytes(download.sizeBytes)} · Last sample</small>
    </article>)}
  </section>;
}
