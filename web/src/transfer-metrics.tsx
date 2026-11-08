import { useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { TransferMetricsStatus } from "../../src/media/transfer-metrics-types";
import { useTRPC } from "./rpc";
import { Link } from "./router";
import { metricBytes, metricDays } from "./transfer-metrics-model";
import { TransferHistory } from "./transfer-history";
import { LibraryInventoryStats } from "./library-inventory";
import { CrossSeedBackfill } from "./cross-seed-backfill";
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

export function TransferMetrics() {
  const trpc = useTRPC();
  const [hours, setHours] = useState<24 | 168 | 720 | 2160>(24);
  const metrics = useQuery(trpc.transferMetrics.status.queryOptions({ hours }, { refetchInterval: 60_000, placeholderData: keepPreviousData }));
  const data = metrics.data, summary = data?.summary;
  return <section className="activity-section transfer-dashboard" aria-label="Sharing and transfers" aria-busy={metrics.isFetching}>
    <div className="transfer-heading"><h2>Sharing &amp; transfers</h2>{data?.observedAt && <span className={`transfer-live ${data.stale ? "transfer-stale" : ""}`}>
      {data.stale ? "Some readings unavailable" : "Updated"} {date(data.observedAt)}</span>}</div>
    <LibraryInventoryStats />
    {metrics.isPending ? <p role="status">Loading transfer metrics...</p> : metrics.isError ? <p role="alert">Transfer metrics unavailable. <button onClick={() => metrics.refetch()}>Try again</button></p> : data && <>
      {!summary ? <p className="muted">Waiting for the first transfer sample. Collection runs every minute.</p> : <>
        <div className="transfer-overview">
          <article><h3>Seeding</h3><strong>{metricBytes(summary.seedBytes)}</strong><small>{count(summary.seedingTorrents)} torrents · {count(summary.averageCopies)} copies on average</small>
            <small>{metricDays(summary.seedSeconds)} combined seed time across all managed torrents</small></article>
          <article><h3>Download avoided</h3><strong>{metricBytes(data.reuse.verifiedReusedBytes)}</strong><small>Verified existing bytes reused</small></article>
          <article><h3>Uploaded</h3><strong>{metricBytes(summary.uploadedBytes)}</strong><small>{speed(summary.uploadBytesPerSecond)} now · {metricBytes(summary.crossSeedUploadedBytes)} from cross-seeds</small></article>
          <article><h3>Downloaded</h3><strong>{metricBytes(summary.downloadedBytes)}</strong><small>{speed(summary.downloadBytesPerSecond)} now</small></article>
        </div>
        <p className="transfer-footnote">Seeding counts shared bytes once per torrent, not disk space. Transfer totals include client activity before tracking began.</p>
        {data.downloads.length > 0 && <section className="transfer-downloads" aria-label="Current transfers"><h3>Downloading <span className="muted">{data.downloads.length}</span></h3>{data.downloads.map(download => <article key={download.id}>
          <div>{download.href ? <Link href={download.href}>{download.title}</Link> : <strong>{download.title}</strong>}<span>{download.state === "checking" ? "Checking existing files" : download.state === "download-queued" ? "Queued" : speed(download.downloadBytesPerSecond)}</span></div>
          {download.sizeBytes !== null && download.sizeBytes > 0 && download.bytesDone !== null && <progress aria-label={`${download.title} download`} max={download.sizeBytes} value={download.bytesDone} />}
          <small>{metricBytes(download.bytesDone)} / {metricBytes(download.sizeBytes)}</small>
        </article>)}</section>}
        <TransferHistory data={data} hours={hours} setHours={setHours} />
        <CrossSeedMetrics reuse={data.reuse} />
        <CrossSeedBackfill />
        <TrackerTable data={data} />
        <p className="transfer-footnote">History since {data.trackingSince ? date(data.trackingSince) : "collection began"} · {hours === 24 ? "Minute samples" : "Hourly samples"} · Missing observations remain gaps</p>
      </>}
    </>}
  </section>;
}
