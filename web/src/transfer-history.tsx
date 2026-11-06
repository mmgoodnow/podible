import { useState, type CSSProperties } from "react";
import type { TransferMetricsStatus } from "../../src/media/transfer-metrics-types";
import { chartDomain, chartSegments, metricBytes, metricDays, seriesChange } from "./transfer-metrics-model";

const count = (n: number | null) => n === null ? "Unknown" : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
const modes = {
  seedBytes: { label: "Seed size", format: metricBytes, tracker: true },
  seedingTorrents: { label: "Seeding torrents", format: count, tracker: true },
  uploadedBytes: { label: "Uploaded", format: metricBytes, tracker: true },
  downloadedBytes: { label: "Downloaded", format: metricBytes, tracker: true },
  uploadBytesPerSecond: { label: "Upload speed", format: (n: number | null) => n === null ? "Unknown" : `${metricBytes(n)}/s`, tracker: true },
  downloadBytesPerSecond: { label: "Download speed", format: (n: number | null) => n === null ? "Unknown" : `${metricBytes(n)}/s`, tracker: true },
  seedSeconds: { label: "Seed time", format: metricDays, tracker: true },
  averageCopies: { label: "Average seeding copies", format: count, tracker: false },
  reusedBytes: { label: "Download avoided", format: metricBytes, tracker: false },
  crossSeedUploadedBytes: { label: "Cross-seed upload", format: metricBytes, tracker: false },
} as const;
type Mode = keyof typeof modes;
const colors = ["#326853", "#bd5834", "#386b96", "#986c20", "#8a5270", "#467e7b", "#696737", "#644e99"];
const shortDate = (at: string) => new Date(at).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

export function TransferHistory({ data, hours, setHours }: { data: TransferMetricsStatus; hours: 24 | 168 | 720 | 2160; setHours: (hours: 24 | 168 | 720 | 2160) => void }) {
  const [mode, setMode] = useState<Mode>("seedBytes"), [hidden, setHidden] = useState<Set<string>>(new Set());
  const [zoom, setZoom] = useState(false), [focused, setFocused] = useState<number | null>(null);
  const config = modes[mode], history = data.history;
  // Include historical-only trackers, rather than dropping retired registrations from the graph.
  const hosts = [...new Set([...data.trackers.map(row => row.host), ...history.flatMap(point => point.trackers.map(row => row.host))])].sort();
  const series = config.tracker && hosts.length ? hosts.map((host, index) => {
    const name = data.mappings.find(row => row.host === host)?.name ?? data.trackers.find(row => row.host === host)?.name;
    return { id: host, label: name && name !== "Unknown tracker" ? name : host, color: colors[index % colors.length]!, dashed: index >= colors.length,
      points: history.map(point => ({ at: point.at, value: point.trackers.find(row => row.host === host)?.[mode as Extract<Mode, keyof TransferMetricsStatus["trackers"][number]>] ?? null })) };
  }) : [{ id: "all", label: config.label, color: colors[0]!, dashed: false, points: history.map(point => ({ at: point.at, value: point[mode] })) }];
  const visible = series.filter(row => !hidden.has(row.id));
  const domain = chartDomain(visible.map(row => row.points), zoom);
  const hasReadings = visible.some(row => row.points.some(point => point.value !== null));
  const noChange = hasReadings && visible.every(row => { const values = row.points.flatMap(point => point.value === null ? [] : [point.value]); return values.length < 2 || Math.max(...values) === Math.min(...values); });
  const selected = focused === null ? null : history[focused];
  const label = selected ? shortDate(selected.at) : "Latest sample";
  const toggle = (id: string) => setHidden(previous => { const next = new Set(previous); next.has(id) ? next.delete(id) : next.add(id); return next; });
  return <section className="transfer-chart" aria-label="Tracker history">
    <div className="transfer-chart-heading"><h3>Tracker history</h3><div className="transfer-chart-controls">
      <select aria-label="Chart measure" value={mode} onChange={event => { setMode(event.target.value as Mode); setFocused(null); }}>
        {Object.entries(modes).map(([key, item]) => <option key={key} value={key}>{item.label}</option>)}
      </select>
      <div className="transfer-ranges" aria-label="History range">{([[24, "24h"], [168, "7d"], [720, "30d"], [2160, "90d"]] as const).map(([value, title]) =>
        <button key={value} aria-pressed={hours === value} onClick={() => { setHours(value); setFocused(null); }}>{title}</button>)}</div>
    </div></div>
    <div className="transfer-chart-caption"><span>{!hasReadings ? "No readings for this measure yet" : noChange ? "No change in this window" : "Select a point to inspect values"}</span>
      <label><input type="checkbox" checked={zoom} onChange={event => setZoom(event.target.checked)} /> Zoom vertical scale</label></div>
    {history.length < 2 ? <p className="transfer-chart-empty">History builds with each minute of observation.</p> : !hasReadings ? <p className="transfer-chart-empty">{visible.length ? "Unavailable readings are gaps, not zero activity." : "Select a tracker below to show its history."}</p> : <>
      <div className="transfer-chart-plot">
        <div className="transfer-chart-yaxis">{[domain.maximum, (domain.maximum + domain.minimum) / 2, domain.minimum].map((value, index) => <span key={index}>{config.format(value)}</span>)}</div>
        <svg viewBox="-2 -8 804 196" role="img" aria-label={`${config.label} by tracker; missing readings appear as gaps`} preserveAspectRatio="none"
          onPointerMove={event => { const bounds = event.currentTarget.getBoundingClientRect(), fraction = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
            const time = Date.parse(history[0]!.at) + fraction * (Date.parse(history.at(-1)!.at) - Date.parse(history[0]!.at));
            let nearest = 0; for (let i = 1; i < history.length; i++) if (Math.abs(Date.parse(history[i]!.at) - time) < Math.abs(Date.parse(history[nearest]!.at) - time)) nearest = i; setFocused(nearest); }}>
          {[0, 90, 180].map(y => <line key={y} x1="0" x2="800" y1={y} y2={y} className="transfer-grid-line" />)}
          {visible.map(row => <g key={row.id} style={{ color: row.color }}>{chartSegments(row.points, hours === 24 ? 60_000 : 3600_000, 800, 180, domain).paths.map((path, i) =>
            <path key={i} d={path} fill="none" stroke="currentColor" strokeWidth="2.5" strokeDasharray={row.dashed ? "5 3" : undefined} strokeLinecap="round" vectorEffect="non-scaling-stroke" />)}</g>)}
          {selected && <line x1={(Date.parse(selected.at) - Date.parse(history[0]!.at)) / (Date.parse(history.at(-1)!.at) - Date.parse(history[0]!.at) || 1) * 800} x2={(Date.parse(selected.at) - Date.parse(history[0]!.at)) / (Date.parse(history.at(-1)!.at) - Date.parse(history[0]!.at) || 1) * 800} y1="0" y2="180" className="transfer-crosshair" />}
        </svg>
      </div>
      <div className="transfer-chart-axis"><span>{shortDate(history[0]!.at)}</span><span>{shortDate(history.at(-1)!.at)}</span></div>
      <label className="transfer-chart-scrubber">{label}<input aria-label="History sample" type="range" min="0" max={history.length - 1} value={focused ?? history.length - 1} onChange={event => setFocused(Number(event.target.value))} /></label>
    </>}
    <div className="transfer-chart-legend" aria-label="Chart series">{series.map(row => {
      const value = row.points[focused ?? row.points.length - 1]?.value ?? null, change = seriesChange(row.points);
      return <button key={row.id} aria-pressed={!hidden.has(row.id)} onClick={() => toggle(row.id)} style={{ "--series-color": row.color } as CSSProperties}>
        <span className="transfer-series-swatch" /><span className="transfer-series-name">{row.label}</span><strong>{config.format(value)}</strong>
        <small>{change === null ? "Not enough samples" : change === 0 ? "No change" : `${change > 0 ? "+" : "−"}${config.format(Math.abs(change))} in window`}</small>
      </button>;
    })}</div>
  </section>;
}
