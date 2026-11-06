import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { TransferMetricsStatus } from "../../src/media/transfer-metrics-types";
import { useTRPC } from "./rpc";
import { Link } from "./router";
import { chartSegments, metricBytes, metricDays } from "./transfer-metrics-model";
import "./transfer-metrics.css";

type History = TransferMetricsStatus["history"][number];
const count = (value: number | null) => value === null ? "Unknown" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
const date = (value: string) => new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
const chartModes = {
  speed: { label: "Transfer speed", keys: ["uploadBytesPerSecond", "downloadBytesPerSecond"], legends: ["Upload", "Download"], format: (n: number | null) => `${metricBytes(n)}/s` },
  volume: { label: "Seed size", keys: ["seedBytes"], legends: ["Logical seed size"], format: metricBytes },
  torrents: { label: "Seeding torrents", keys: ["seedingTorrents"], legends: ["Seeding torrents"], format: count },
  copies: { label: "Seeding copies", keys: ["averageCopies"], legends: ["Copies per content target"], format: count },
  traffic: { label: "Transferred", keys: ["uploadedBytes", "downloadedBytes"], legends: ["Upload", "Download"], format: metricBytes },
  reuse: { label: "Reused payload", keys: ["reusedBytes"], legends: ["Verified payload reused"], format: metricBytes },
  crossUpload: { label: "Cross-seed upload", keys: ["crossSeedUploadedBytes"], legends: ["Upload from cross-seeded torrents"], format: metricBytes },
  seedTime: { label: "Seed time", keys: ["seedSeconds"], legends: ["Combined seed time"], format: metricDays },
} as const;
type Mode = keyof typeof chartModes;

function HistoryChart({ history, hours, trackers }: { history: History[]; hours: number; trackers: TransferMetricsStatus["trackers"] }) {
  const [mode, setMode] = useState<Mode>("speed");
  const [trackerHost, setTrackerHost] = useState("");
  const config = chartModes[mode];
  const trackerFields = { seedBytes: "seedBytes", seedingTorrents: "seedingTorrents", uploadedBytes: "uploadedBytes", downloadedBytes: "downloadedBytes", seedSeconds: "seedSeconds" } as const;
  const trackerSupported = mode === "volume" || mode === "torrents" || mode === "traffic" || mode === "seedTime";
  const series = config.keys.map(key => chartSegments(history.map(point => ({ at: point.at,
    value: trackerHost && trackerSupported && key in trackerFields
      ? point.trackers.find(tracker => tracker.host === trackerHost)?.[key as keyof typeof trackerFields] ?? null : point[key],
  })), hours === 24 ? 60_000 : 3600_000));
  const maximum = Math.max(0, ...series.map(item => item.maximum));
  // All series share the same vertical scale; a zero-only series stays on the baseline.
  return <div className="transfer-chart">
    <div className="transfer-chart-heading"><h3>Over time</h3><label>Measure <select aria-label="Chart measure" value={mode} onChange={event => setMode(event.target.value as Mode)}>
      {Object.entries(chartModes).map(([key, item]) => <option key={key} value={key}>{item.label}</option>)}
    </select></label>
      {trackerSupported && <label>Tracker <select aria-label="Chart tracker" value={trackerHost} onChange={event => setTrackerHost(event.target.value)}>
        <option value="">All trackers</option>{trackers.map(tracker => <option key={tracker.host} value={tracker.host}>{tracker.name === "Unknown tracker" ? tracker.host : tracker.name}</option>)}
      </select></label>}
    </div>
    {history.length < 2 ? <p className="transfer-chart-empty">History builds with each minute of observation.</p> : <>
      <div className="transfer-chart-legend">{config.legends.map((legend, index) => <span key={legend} className={`transfer-series-${index}`}>{legend}</span>)}<small>{config.format(maximum)} peak sample</small></div>
      <svg viewBox="-2 -10 804 204" role="img" aria-label={`${config.label} history; gaps indicate missing observations`} preserveAspectRatio="none">
        {[0, 90, 180].map(y => <line key={y} x1="0" x2="800" y1={y} y2={y} className="transfer-grid-line" />)}
        {series.map((item, index) => <g key={index} className={`transfer-series-${index}`} transform={`translate(0 ${item.maximum ? 180 * (1 - item.maximum / (maximum || 1)) : 0}) scale(1 ${item.maximum ? item.maximum / (maximum || 1) : 1})`}>
          {item.paths.map((path, i) => <path key={i} d={path} fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" vectorEffect="non-scaling-stroke" />)}
        </g>)}
      </svg>
      <div className="transfer-chart-axis"><span>{date(history[0]!.at)}</span><span>{date(history.at(-1)!.at)}</span></div>
    </>}
  </div>;
}

function TrackerTable({ data }: { data: TransferMetricsStatus }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const [editing, setEditing] = useState(false), [notice, setNotice] = useState<string | null>(null);
  const save = useMutation(trpc.transferMetrics.mapTracker.mutationOptions({ onSuccess: () => {
    setNotice("Tracker name saved."); void cache.invalidateQueries({ queryKey: trpc.transferMetrics.status.queryKey() });
  } }));
  const maximum = Math.max(1, ...data.trackers.map(row => row.seedBytes));
  return <section className="transfer-trackers" aria-label="Seeding by tracker">
    <div className="transfer-chart-heading"><h3>By tracker</h3><button className="transfer-text-button" onClick={() => setEditing(!editing)}>{editing ? "Done" : "Edit tracker names"}</button></div>
    {!data.trackers.length ? <p className="muted">Trackers appear after the first verified torrent observation.</p> : <div className="transfer-table-scroll"><table>
      <thead><tr><th>Tracker</th><th>Seed size{data.stale ? " (known subset)" : ""}</th><th>Torrents</th><th>Uploaded / downloaded</th><th>Ratio</th><th>Seed time</th></tr></thead>
      <tbody>{data.trackers.map(row => <tr key={row.host}>
        <th scope="row"><span>{row.name === "Unknown tracker" ? row.host : row.name}</span><span className="transfer-tracker-bar" style={{ width: `${Math.max(1, row.seedBytes / maximum * 100)}%` }} /></th>
        <td>{metricBytes(row.seedBytes)}</td><td>{count(row.seedingTorrents)}</td>
        <td>{metricBytes(row.uploadedBytes)} / {metricBytes(row.downloadedBytes)}</td><td>{row.ratio === null ? "Unknown" : `${row.ratio.toFixed(2)}×`}</td><td>{metricDays(row.seedSeconds)}</td>
      </tr>)}</tbody>
    </table></div>}
    {editing && <div className="transfer-name-editor"><p className="muted">Only hostnames are stored, never announce URLs or passkeys. Hosts with the same name are grouped.</p>
      {data.mappings.map(mapping => <form key={`${mapping.host}:${mapping.name}`} onSubmit={event => {
        event.preventDefault(); const fields = new FormData(event.currentTarget);
        save.mutate({ host: mapping.host, name: String(fields.get("name") ?? "").trim() || null });
      }}><label>{mapping.host}<input name="name" aria-label={`Name for ${mapping.host}`} maxLength={100} defaultValue={mapping.source === "unknown" ? "" : mapping.name} placeholder="Tracker name" /></label>
        <button className="secondary" disabled={save.isPending}>Save</button></form>)}
      {notice && <p role="status">{notice}</p>}{save.error && <p role="alert">{save.error.message}</p>}
    </div>}
    <p className="transfer-footnote">Traffic is attributed only to unambiguous tracker registrations, not tracker-account credit. Seed time is combined torrent time; tracker totals can overlap.</p>
  </section>;
}

export function TransferMetrics() {
  const trpc = useTRPC();
  const [hours, setHours] = useState<24 | 168 | 720 | 2160>(24);
  const metrics = useQuery(trpc.transferMetrics.status.queryOptions({ hours }, { refetchInterval: 60_000 }));
  const data = metrics.data, summary = data?.summary;
  return <section className="activity-section transfer-dashboard" aria-label="Sharing and transfers">
    <div className="transfer-heading"><h2>Sharing &amp; transfers</h2><div className="transfer-ranges" aria-label="History range">
      {([[24, "24h"], [168, "7d"], [720, "30d"], [2160, "90d"]] as const).map(([value, label]) => <button key={value} aria-pressed={hours === value} onClick={() => setHours(value)}>{label}</button>)}
    </div></div>
    {metrics.isPending ? <p role="status">Loading transfer metrics...</p> : metrics.isError ? <p role="alert">Transfer metrics unavailable. <button onClick={() => metrics.refetch()}>Try again</button></p> : data && <>
      {!summary ? <p className="muted">Waiting for the first transfer sample. Collection runs every minute.</p> : <>
        <p className={`transfer-live ${data.stale ? "transfer-stale" : ""}`}>{data.stale ? "Some current readings are unavailable" : "Updated every minute"}{data.observedAt && ` · Last sample ${date(data.observedAt)}`}</p>
        <div className="transfer-stats">
          <article><h3>Logical seed size</h3><strong>{metricBytes(summary.seedBytes)}</strong><small>Shared bytes count for each seeding torrent, not disk usage.</small></article>
          <article><h3>Seeding torrents</h3><strong>{count(summary.seedingTorrents)}</strong><small>{count(summary.averageCopies)} copies per owned content target</small></article>
          <article><h3>Verified payload reused</h3><strong>{metricBytes(data.reuse.verifiedReusedBytes)}</strong><small>Payload not needed from the network</small></article>
          <article><h3>Cross-seed upload</h3><strong>{metricBytes(summary.crossSeedUploadedBytes)}</strong><small>Upload from cross-seeded torrents</small></article>
        </div>
        <div className="transfer-traffic">
          <div><span>Upload now</span><strong>{summary.uploadBytesPerSecond === null ? "Unknown" : `${metricBytes(summary.uploadBytesPerSecond)}/s`}</strong><small>{metricBytes(summary.uploadedBytes)} transferred</small></div>
          <div><span>Download now</span><strong>{summary.downloadBytesPerSecond === null ? "Unknown" : `${metricBytes(summary.downloadBytesPerSecond)}/s`}</strong><small>{metricBytes(summary.downloadedBytes)} transferred</small></div>
          <div><span>Combined seed time</span><strong>{metricDays(summary.seedSeconds)}</strong><small>Across all observed registrations</small></div>
        </div>
        <HistoryChart history={data.history} hours={hours} trackers={data.trackers} />
        <div className="transfer-reuse"><h3>Reuse outcomes</h3><div className="transfer-reuse-values">
          <span>{data.reuse.matchRate === null ? "No completed reuse attempts yet" : <><strong>{(data.reuse.matchRate * 100).toFixed(1)}%</strong> completed successfully</>}</span>
          <span>{data.reuse.successful} completed / {data.reuse.attempts} attempts</span><span>{data.reuse.pending} in progress</span><span>{data.reuse.unsupported} unsupported</span>
        </div><p className="transfer-footnote">Sidecar payload needed: {metricBytes(data.reuse.sidecarDownloadBytes)} · Content repair needed: {metricBytes(data.reuse.contentRepairBytes)} · Multi-file TV payload reused: {metricBytes(data.reuse.tvMultiFileReusedBytes)}</p></div>
        <TrackerTable data={data} />
        {data.downloads.length > 0 && <section className="transfer-downloads" aria-label="Current transfers"><h3>Downloading</h3>{data.downloads.map(download => <article key={download.id}>
          <div>{download.href ? <Link href={download.href}>{download.title}</Link> : <strong>{download.title}</strong>}<span>{download.state === "checking" ? "Checking existing files" : download.state === "download-queued" ? "Queued" : "Downloading"}</span></div>
          {download.sizeBytes !== null && download.sizeBytes > 0 && download.bytesDone !== null && <progress aria-label={`${download.title} download`} max={download.sizeBytes} value={download.bytesDone} />}
          <small>{metricBytes(download.bytesDone)} / {metricBytes(download.sizeBytes)}{download.downloadBytesPerSecond !== null && ` · ${metricBytes(download.downloadBytesPerSecond)}/s`}</small>
        </article>)}</section>}
        <p className="transfer-footnote">History since {data.trackingSince ? date(data.trackingSince) : "collection began"}. Initial transfer counters include prior client activity; gaps mean unavailable observations. {hours === 24 ? "Minute samples." : "Hourly latest samples, not hourly averages."}</p>
      </>}
    </>}
  </section>;
}
