// bun run scripts/grafana-dashboard.ts <prometheus-datasource-uid>
const uid = process.argv[2];
if (!uid || !/^[\w-]+$/.test(uid)) throw new Error("Supply a Prometheus datasource UID");
const datasource = { type: "prometheus", uid };
const panels: object[] = [];
let y = 0;
const grid = (x: number, w: number, h: number) => ({ x, y, w, h });
const series = (metric: string) => `podible_${metric}{job="podible"}`;
const delta = (metric: string) => `increase(${series(metric)}[$__range])`;
const rolling = (metric: string) => `increase(${series(metric)}[1h])`;
type Query = [expression: string, legend?: string];
function panel(type: "stat" | "timeseries" | "table", title: string, description: string, unit: string,
  position: ReturnType<typeof grid>, queries: Query[], decimals?: number, statusLabels?: readonly [string, string]) {
  const instant = type !== "timeseries";
  panels.push({
    id: panels.length + 1, type, title, description, gridPos: position, datasource,
    targets: queries.map(([expr, legendFormat], i) => ({
      datasource, refId: String.fromCharCode(65 + i), expr, legendFormat, instant, range: !instant,
      ...(type === "table" ? { format: "table" } : {}),
    })),
    fieldConfig: { defaults: {
      unit, decimals, noValue: "Not observed",
      color: type === "stat" ? { mode: "fixed", fixedColor: "gray" } : { mode: "palette-classic" },
      ...(statusLabels ? { mappings: [{ type: "value", options: {
        0: { text: statusLabels[0], color: "red", index: 0 },
        1: { text: statusLabels[1], color: "green", index: 1 },
      } }] } : {}),
      ...(type === "timeseries" ? { min: 0, custom: { lineWidth: 2, fillOpacity: 8, spanNulls: false } } : {}),
    }, overrides: [] },
    options: type === "stat" ? { colorMode: statusLabels ? "value" : "none", graphMode: "none", justifyMode: "auto", textMode: "value",
      reduceOptions: { calcs: ["lastNotNull"], fields: "", values: false } } : type === "timeseries" ? {
      legend: { showLegend: true, placement: "bottom", displayMode: "table", calcs: ["lastNotNull"] },
      tooltip: { mode: "multi", sort: "desc" },
    } : { showHeader: true },
  });
}
function row(title: string) {
  panels.push({ id: panels.length + 1, type: "row", title, gridPos: grid(0, 24, 1), collapsed: false, panels: [] });
  y++;
}
function note(text: string) {
  panels.push({ id: panels.length + 1, type: "text", transparent: true, gridPos: grid(0, 24, 3), options: { mode: "markdown", content: text } });
  y += 3;
}
const stat = (x: number, title: string, description: string, unit: string, expr: string, decimals?: number) =>
  panel("stat", title, description, unit, grid(x, 6, 4), [[expr]], decimals);
const status = (x: number, title: string, description: string, expr: string, labels: readonly [string, string]) =>
  panel("stat", title, description, "none", grid(x, 6, 4), [[expr]], 0, labels);
const chart = (x: number, title: string, description: string, unit: string, queries: Query[]) =>
  panel("timeseries", title, description, unit, grid(x, 12, 9), queries);
const items = (kind: string) => `podible_published_library_items{job="podible",kind="${kind}"}`;

row("Library");
const inventory = "Published in Podible's records. Not a live filesystem verification.";
stat(0, "Movies", inventory, "none", items("movies"));
stat(6, "TV shows", inventory, "none", items("shows"));
stat(12, "Episodes", inventory, "none", items("episodes"));
stat(18, "Books", inventory, "none", items("books"));
y += 4;
chart(0, "Movies and TV", "A season counts once it has a published episode. Episodes are charted separately to keep the other series readable.", "none",
  [[items("movies"), "Movies"], [items("shows"), "Shows"], [items("seasons"), "Seasons"]]);
chart(12, "Episodes and book formats", "A book can count as both an ebook and an audiobook.", "none",
  [[items("episodes"), "Episodes"], [items("ebooks"), "Ebooks"], [items("audiobooks"), "Audiobooks"]]);
y += 9;

row("Seeding coverage");
stat(0, "Logical seed size", "Counts a byte once per active torrent, not once on disk.", "bytes", series("seed_bytes"));
stat(6, "Torrents seeding", "Complete torrent registrations actively seeding.", "none", series("seeding_torrents"));
stat(12, "Copies per title or target", "Active seeding associations per observed owned target, not per episode or file.", "none", series("seeding_copies_per_content_target"), 2);
stat(18, "Combined seed time", "Observed cumulative time summed across torrent registrations.", "dtdurations", series("seed_seconds_total"));
y += 4;
const tracker = (metric: string) => `sum by (tracker, host) (${series("tracker_" + metric)})`;
const label = "{{tracker}} ({{host}})";
chart(0, "Seed size by tracker", "Logical bytes. Tracker totals can overlap; these lines are not stacked.", "bytes", [[tracker("seed_bytes"), label]]);
chart(12, "Torrents by tracker", "Tracker totals can overlap.", "none", [[tracker("seeding_torrents"), label]]);
y += 9;
chart(0, "Seed time by tracker", "Observed cumulative torrent time. Tracker totals can overlap.", "dtdurations", [[tracker("seed_seconds_total"), label]]);
chart(12, "Attributed transfer ratio", "Client upload divided by nonzero download, when uniquely attributable. Not tracker account ratio.", "none",
  [[`${tracker("uploaded_bytes_total")} / (${tracker("downloaded_bytes_total")} > 0)`, label]]);
y += 9;

row("Cross-seeding and transfer efficiency");
stat(0, "Completed in range", "Registrations started seeding after reuse. Prometheus counter increase over the selected range.", "none", delta("reuse_completed_total"), 0);
stat(6, "Verified reuse in range", "Counts verified reuse only, not assumed matches or physical disk savings.", "bytes", delta("reuse_verified_bytes_total"));
stat(12, "Cross-seed upload in range", "Client-observed upload from cross-seeded registrations, not tracker credit or additional upload versus a baseline.", "bytes", delta("cross_seed_uploaded_bytes_total"));
stat(18, "Sidecar payload in range", "Sidecar bytes required by completed reuse. Excludes protocol and retry overhead.", "bytes", delta("reuse_sidecar_payload_bytes_total"));
y += 4;
chart(0, "Reuse outcomes and overhead", "Trailing-hour changes. Missing accounting stays blank.", "bytes", [
  [rolling("reuse_verified_bytes_total"), "Verified reuse"], [rolling("reuse_sidecar_payload_bytes_total"), "Side files downloaded"],
  [rolling("reuse_repair_payload_bytes_total"), "Content repair"], [rolling("reuse_tv_multifile_reused_bytes_total"), "Multi-file TV reuse (includes assumed matches)"],
]);
chart(12, "Indexer work", "Trailing-hour requests by kind and purpose. Cache hits and coalesced requests did not reach an indexer.", "none",
  [[`increase(${series("indexer_traffic_total")}[1h])`, "{{event}} / {{purpose}}"]]);
y += 9;

row("Traffic and progress");
chart(0, "Transfer speed", "Sampled client speeds. Gaps mean telemetry was unavailable, not zero throughput.", "Bps",
  [[series("upload_bytes_per_second"), "Upload"], [series("download_bytes_per_second"), "Download"]]);
chart(12, "Bytes transferred per hour", "Trailing-hour client bytes. Cross-seed upload is a subset of upload.", "bytes",
  [[rolling("uploaded_bytes_total"), "Uploaded"], [rolling("downloaded_bytes_total"), "Downloaded"], [rolling("cross_seed_uploaded_bytes_total"), "Cross-seed upload"]]);
y += 9;
chart(0, "Downloads by stage", "Owned registrations by stage. Open Activity for titles and progress per download.", "none", [[series("active_downloads"), "{{state}}"]]);
chart(12, "Transfer speed by tracker", "Only traffic uniquely attributable to one tracker is included.", "Bps",
  [[tracker("upload_bytes_per_second"), "{{tracker}} upload"], [tracker("download_bytes_per_second"), "{{tracker}} download"]]);
y += 9;

row("Telemetry");
status(0, "Prometheus scrape", "Whether the latest scrape succeeded.", 'up{job="podible"}', ["Scrape failed", "Scraping"]);
stat(6, "Transfer snapshot age", "Age of the observation, not the age of the HTTP response.", "dtdurations",
  `clamp_min(time() - ${series("transfer_metrics_observed_timestamp_seconds")}, 0)`);
status(12, "Transfer telemetry", "Complete means all registered torrents were observed and verified recently.", series("transfer_metrics_fresh"), ["Incomplete", "Complete"]);
stat(18, "Reuse attempts pending", "Attempts without a recorded outcome.", "none", series("reuse_pending"));
y += 4;
note("History starts with the first successful Prometheus scrape. Missing series are not zero. Publication counts reflect Podible records; they do not prove files still exist. Tracker traffic does not represent account credit.");

console.log(JSON.stringify({
  uid: "podible-operations", title: "Podible", tags: ["media", "podible"], schemaVersion: 41, version: 1,
  description: "Library delivery, seeding coverage, reuse and network activity.",
  editable: false, time: { from: "now-24h", to: "now" }, refresh: "1m", panels,
  links: [{ type: "link", title: "Podible Activity", url: "https://podible.cyplex.dev/ops", targetBlank: true }],
}, null, 2));
