#!/usr/bin/env bun
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

type JsonRecord = Record<string, any>;

type SpanSummary = {
  path: string;
  nodeCount: number | null;
  depth: number | null;
  startTime: number | null;
  endTime: number | null;
  startedAt: string | null;
  lastAt: string | null;
  lastEvent: string;
  lastTool: string | null;
  toolCalls: number;
  judgeRejected: number;
  terminal: string | null;
};

type BoundaryMarker = {
  spanPath: string | null;
  epubNodeId: string | null;
  title: string;
  startTime: number;
  source: "split" | "leaf";
};

type RunSummary = {
  id: string;
  eventLog: string;
  resultPath: string | null;
  traceDir: string | null;
  startedAt: string | null;
  updatedAt: string | null;
  status: "running" | "accepted" | "failed" | "interrupted" | "partial";
  durationSeconds: number | null;
  audiobookDurationSeconds: number | null;
  model: string | null;
  epubEntries: number | null;
  originalEpubEntries: number | null;
  audioOnlyIntervals: number;
  audibleExcludedNodes: number;
  metrics: {
    splits: number;
    leaves: number;
    spanErrors: number;
    judgeAccepted: number;
    judgeRejected: number;
    toolCalls: number;
  };
  activeSpans: SpanSummary[];
  churnSpans: SpanSummary[];
  failedSpans: Array<{ path: string; nodeCount: number | null; depth: number | null; error: string; elapsedMs: number | null }>;
  rejectedNodes: Array<{ epubNodeId: string; count: number }>;
  excludedNodes: Array<{ epubNodeId: string; reason: string; notes: string }>;
  audioOnlyIntervalDetails: Array<{ startTime: number; endTime: number; kind: string; notes: string }>;
  eventTail: Array<{
    ts: string | null;
    type: string;
    spanPath: string | null;
    toolName: string | null;
    message: string;
  }>;
  resultSummary: unknown;
  traceFiles: string[];
  judgeDecisions: Array<{
    ts: string | null;
    spanPath: string | null;
    epubNodeId: string | null;
    startTime: number | null;
    accepted: boolean | null;
    confidence: string | null;
    finding: string | null;
    reason: string;
    concerns: string[];
  }>;
  treeSpans: SpanSummary[];
  treeEdges: Array<{ parent: string; left: string; right: string; epubNodeId: string | null; startTime: number | null }>;
  boundaryMarkers: BoundaryMarker[];
  traceSummaries: Array<{
    file: string;
    kind: string;
    spanPath: string | null;
    title: string;
    reasoningSummaries: string[];
    finalOutput: unknown;
  }>;
};

const repoRoot = path.resolve(import.meta.dir, "..");
const caseDir = path.join(repoRoot, "tmp/chapter-cases/red-rising/prod");
const port = Number(process.env.PORT ?? 7331);

function eventLogFiles(): string[] {
  if (!existsSync(caseDir)) return [];
  return readdirSync(caseDir)
    .filter((name) => /^agent-events-m59-.*\.jsonl$/.test(name))
    .map((name) => path.join(caseDir, name))
    .sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
}

function readJsonl(filePath: string): JsonRecord[] {
  if (!existsSync(filePath)) return [];
  return readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line) as JsonRecord];
      } catch {
        return [];
      }
    });
}

function runIdFromEventLog(filePath: string): string {
  return path.basename(filePath).replace(/^agent-events-m59-/, "").replace(/\.jsonl$/, "");
}

function artifactPathFor(eventLog: string, kind: "result" | "trace"): string | null {
  const id = runIdFromEventLog(eventLog);
  const candidate =
    kind === "result" ? path.join(caseDir, `agent-result-m59-${id}.json`) : path.join(caseDir, `agent-traces-m59-${id}`);
  return existsSync(candidate) ? candidate : null;
}

function readJsonFile(filePath: string | null): unknown {
  if (!filePath || !existsSync(filePath)) return null;
  try {
    return JSON.parse(readFileSync(filePath, "utf8")) as unknown;
  } catch {
    return null;
  }
}

function traceFilesFor(traceDir: string | null): string[] {
  if (!traceDir || !existsSync(traceDir) || !statSync(traceDir).isDirectory()) return [];
  return readdirSync(traceDir)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .slice(-80);
}

function traceSummariesFor(traceDir: string | null): RunSummary["traceSummaries"] {
  if (!traceDir || !existsSync(traceDir) || !statSync(traceDir).isDirectory()) return [];
  return readdirSync(traceDir)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .slice(-30)
    .flatMap((file) => {
      const fullPath = path.join(traceDir, file);
      const payload = readJsonFile(fullPath) as JsonRecord | null;
      if (!payload) return [];
      const rawResponses = Array.isArray(payload.rawResponses) ? payload.rawResponses : [];
      const reasoningSummaries = rawResponses.flatMap((response: JsonRecord) =>
        Array.isArray(response.output)
          ? response.output.flatMap((item: JsonRecord) =>
              item?.type === "reasoning" && Array.isArray(item.content)
                ? item.content.map((content: JsonRecord) => String(content.text ?? "")).filter(Boolean)
                : []
            )
          : []
      );
      const spanPath =
        typeof payload.span?.path === "string"
          ? payload.span.path
          : typeof payload.split?.spanPath === "string"
            ? payload.split.spanPath
            : null;
      const kind = file.includes("fulcrum-judge") ? "judge" : file.includes("span-") ? "curator" : "classifier";
      const title =
        kind === "judge"
          ? `${payload.split?.epubNodeId ?? "judge"} @ ${payload.split?.startTime ?? ""}`
          : kind === "curator"
            ? `${spanPath ?? "span"}`
            : "audible EPUB classifier";
      return [
        {
          file,
          kind,
          spanPath,
          title,
          reasoningSummaries,
          finalOutput: payload.finalOutput ?? payload.judgment ?? payload.selection ?? null,
        },
      ];
    })
    .reverse();
}

function secondsBetween(start: string | null, end: string | null): number | null {
  if (!start || !end) return null;
  const delta = new Date(end).getTime() - new Date(start).getTime();
  return Number.isFinite(delta) && delta >= 0 ? delta / 1000 : null;
}

function spanNodeCount(span: JsonRecord | undefined): number | null {
  if (!span) return null;
  if (typeof span.epubStartIndex !== "number" || typeof span.epubEndIndex !== "number") return null;
  return span.epubEndIndex - span.epubStartIndex + 1;
}

function errorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (error && typeof error === "object" && "message" in error) return String((error as { message: unknown }).message);
  return error ? JSON.stringify(error) : "";
}

function summarizeRun(eventLog: string): RunSummary {
  const events = readJsonl(eventLog);
  const eventLogMtimeMs = existsSync(eventLog) ? statSync(eventLog).mtimeMs : 0;
  const resultPath = artifactPathFor(eventLog, "result");
  const traceDir = artifactPathFor(eventLog, "trace");
  const spans = new Map<string, SpanSummary>();
  const rejectedNodes = new Map<string, number>();
  const failedSpans: RunSummary["failedSpans"] = [];
  const metrics = { splits: 0, leaves: 0, spanErrors: 0, judgeAccepted: 0, judgeRejected: 0, toolCalls: 0 };
  let status: RunSummary["status"] = events.length > 0 ? "running" : "partial";
  let model: string | null = null;
  let epubEntries: number | null = null;
  let originalEpubEntries: number | null = null;
  let audiobookDurationSeconds: number | null = null;
  let audioOnlyIntervals = 0;
  let audibleExcludedNodes = 0;
  let excludedNodes: RunSummary["excludedNodes"] = [];
  let audioOnlyIntervalDetails: RunSummary["audioOnlyIntervalDetails"] = [];
  const judgeDecisions: RunSummary["judgeDecisions"] = [];
  const treeEdges: RunSummary["treeEdges"] = [];
  const boundaryMarkers: BoundaryMarker[] = [];

  const ensureSpan = (event: JsonRecord): SpanSummary | null => {
    const span = event.span as JsonRecord | undefined;
    const pathValue = span?.path;
    if (typeof pathValue !== "string") return null;
    const existing = spans.get(pathValue);
    if (existing) return existing;
    const summary: SpanSummary = {
      path: pathValue,
      nodeCount: spanNodeCount(span),
      depth: typeof span?.depth === "number" ? span.depth : null,
      startTime: typeof span?.startTime === "number" ? span.startTime : null,
      endTime: typeof span?.endTime === "number" ? span.endTime : null,
      startedAt: null,
      lastAt: null,
      lastEvent: "",
      lastTool: null,
      toolCalls: 0,
      judgeRejected: 0,
      terminal: null,
    };
    spans.set(pathValue, summary);
    return summary;
  };

  for (const event of events) {
    const type = String(event.type ?? "");
    if (type === "recursive-run-start") {
      model = typeof event.model === "string" ? event.model : model;
      epubEntries = typeof event.epubEntries === "number" ? event.epubEntries : epubEntries;
      originalEpubEntries = typeof event.originalEpubEntries === "number" ? event.originalEpubEntries : originalEpubEntries;
      audioOnlyIntervals = typeof event.audioOnlyIntervals === "number" ? event.audioOnlyIntervals : audioOnlyIntervals;
      audiobookDurationSeconds = typeof event.durationSeconds === "number" ? event.durationSeconds : audiobookDurationSeconds;
    }
    if (type === "audible-epub-node-selection" && event.selection) {
      const selection = event.selection as JsonRecord;
      excludedNodes = Array.isArray(selection.excludedNodes) ? selection.excludedNodes : excludedNodes;
      audioOnlyIntervalDetails = Array.isArray(selection.audioOnlyIntervals) ? selection.audioOnlyIntervals : audioOnlyIntervalDetails;
      audibleExcludedNodes = excludedNodes.length;
      audioOnlyIntervals = audioOnlyIntervalDetails.length;
    }
    if (type === "audio-only-intervals-applied" && Array.isArray(event.audioOnlyIntervals)) {
      audioOnlyIntervalDetails = event.audioOnlyIntervals;
      audioOnlyIntervals = audioOnlyIntervalDetails.length;
    }
    if (type === "recursive-merge-accepted") status = "accepted";
    if (type === "recursive-result-null" || type === "recursive-merge-rejected") status = "failed";
    if (type === "span-split-accepted") metrics.splits++;
    if (type === "span-leaf-accepted") metrics.leaves++;
    if (type === "span-error") metrics.spanErrors++;
    if (type === "span-tool-call") metrics.toolCalls++;
    if (type === "fulcrum-judge-result") {
      judgeDecisions.push({
        ts: typeof event.ts === "string" ? event.ts : null,
        spanPath: typeof event.span?.path === "string" ? event.span.path : null,
        epubNodeId: typeof event.split?.epubNodeId === "string" ? event.split.epubNodeId : null,
        startTime: typeof event.split?.startTime === "number" ? event.split.startTime : null,
        accepted: typeof event.judgment?.accepted === "boolean" ? event.judgment.accepted : null,
        confidence: typeof event.judgment?.confidence === "string" ? event.judgment.confidence : null,
        finding: typeof event.judgment?.finding === "string" ? event.judgment.finding : null,
        reason: typeof event.judgment?.reason === "string" ? event.judgment.reason : "",
        concerns: Array.isArray(event.judgment?.concerns) ? event.judgment.concerns.map(String) : [],
      });
      if (event.judgment?.accepted === true) metrics.judgeAccepted++;
      if (event.judgment?.accepted === false) {
        metrics.judgeRejected++;
        const nodeId = event.split?.epubNodeId;
        if (typeof nodeId === "string") rejectedNodes.set(nodeId, (rejectedNodes.get(nodeId) ?? 0) + 1);
      }
    }

    const span = ensureSpan(event);
    if (span) {
      span.lastAt = typeof event.ts === "string" ? event.ts : span.lastAt;
      span.lastEvent = type || span.lastEvent;
      if (type === "span-start") span.startedAt = typeof event.ts === "string" ? event.ts : span.startedAt;
      if (type === "span-tool-call") {
        span.toolCalls++;
        span.lastTool = typeof event.toolName === "string" ? event.toolName : span.lastTool;
      }
      if (type === "fulcrum-judge-result" && event.judgment?.accepted === false) span.judgeRejected++;
      if (type === "span-split-accepted") span.terminal = "split";
      if (type === "span-leaf-accepted") span.terminal = "leaf";
      if (type === "span-error") {
        span.terminal = "error";
        failedSpans.push({
          path: span.path,
          nodeCount: span.nodeCount,
          depth: span.depth,
          error: errorMessage(event.error),
          elapsedMs: typeof event.elapsedMs === "number" ? event.elapsedMs : null,
        });
      }
    }
    if (type === "span-split-accepted") {
      if (typeof event.span?.path === "string" && typeof event.left?.path === "string" && typeof event.right?.path === "string") {
        treeEdges.push({
          parent: event.span.path,
          left: event.left.path,
          right: event.right.path,
          epubNodeId: typeof event.split?.epubNodeId === "string" ? event.split.epubNodeId : null,
          startTime: typeof event.split?.startTime === "number" ? event.split.startTime : null,
        });
      }
      if (typeof event.split?.startTime === "number") {
        boundaryMarkers.push({
          spanPath: typeof event.span?.path === "string" ? event.span.path : null,
          epubNodeId: typeof event.split?.epubNodeId === "string" ? event.split.epubNodeId : null,
          title: String(event.split?.title ?? event.split?.epubNodeId ?? "split"),
          startTime: event.split.startTime,
          source: "split",
        });
      }
    }
    if (type === "span-leaf-accepted" && Array.isArray(event.result?.chapters)) {
      for (const chapter of event.result.chapters) {
        if (typeof chapter.startTime !== "number") continue;
        boundaryMarkers.push({
          spanPath: typeof event.span?.path === "string" ? event.span.path : null,
          epubNodeId: typeof chapter.epubNodeId === "string" ? chapter.epubNodeId : null,
          title: String(chapter.title ?? chapter.epubNodeId ?? "chapter"),
          startTime: chapter.startTime,
          source: "leaf",
        });
      }
    }
  }

  const startedAt = typeof events[0]?.ts === "string" ? events[0].ts : null;
  const updatedAt = typeof events[events.length - 1]?.ts === "string" ? events[events.length - 1].ts : null;
  if (status === "running" && eventLogMtimeMs > 0 && Date.now() - eventLogMtimeMs > 180_000) {
    status = "interrupted";
  }
  const allSpans = [...spans.values()];
  const activeSpans = allSpans
    .filter((span) => !span.terminal)
    .sort((a, b) => (b.toolCalls - a.toolCalls) || String(b.lastAt ?? "").localeCompare(String(a.lastAt ?? "")))
    .slice(0, 12);
  const churnSpans = allSpans
    .sort((a, b) => (b.toolCalls - a.toolCalls) || (b.judgeRejected - a.judgeRejected))
    .slice(0, 12);

  return {
    id: runIdFromEventLog(eventLog),
    eventLog,
    resultPath,
    traceDir,
    startedAt,
    updatedAt,
    status,
    durationSeconds: secondsBetween(startedAt, updatedAt),
    audiobookDurationSeconds,
    model,
    epubEntries,
    originalEpubEntries,
    audioOnlyIntervals,
    audibleExcludedNodes,
    metrics,
    activeSpans,
    churnSpans,
    failedSpans,
    rejectedNodes: [...rejectedNodes.entries()].map(([epubNodeId, count]) => ({ epubNodeId, count })).sort((a, b) => b.count - a.count).slice(0, 12),
    excludedNodes,
    audioOnlyIntervalDetails,
    eventTail: events.slice(-80).map((event) => ({
      ts: typeof event.ts === "string" ? event.ts : null,
      type: String(event.type ?? ""),
      spanPath: typeof event.span?.path === "string" ? event.span.path : null,
      toolName: typeof event.toolName === "string" ? event.toolName : null,
      message: typeof event.message === "string" ? event.message : "",
    })),
    resultSummary: readJsonFile(resultPath),
    traceFiles: traceFilesFor(traceDir),
    judgeDecisions: judgeDecisions.slice(-80).reverse(),
    treeSpans: allSpans.sort((a, b) => (a.depth ?? 0) - (b.depth ?? 0) || a.path.localeCompare(b.path)),
    treeEdges,
    boundaryMarkers: [...boundaryMarkers]
      .sort((a, b) => a.startTime - b.startTime || a.title.localeCompare(b.title))
      .filter((marker, index, markers) => index === 0 || marker.startTime !== markers[index - 1]!.startTime || marker.epubNodeId !== markers[index - 1]!.epubNodeId),
    traceSummaries: traceSummariesFor(traceDir),
  };
}

function html(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Red Rising Curation Runs</title>
  <style>
    :root { color-scheme: dark; --bg: #111315; --panel: #191c1f; --line: #30363d; --text: #eceff3; --muted: #a9b0ba; --bad: #ff8b8b; --ok: #8bd693; --warn: #ffd166; --accent: #79b8ff; }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header { position: sticky; top: 0; z-index: 2; background: rgba(17,19,21,0.94); border-bottom: 1px solid var(--line); padding: 14px 20px; display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }
    h1 { margin: 0; font-size: 18px; font-weight: 650; }
    h2 { margin: 0 0 10px; font-size: 15px; font-weight: 650; }
    main { padding: 18px 20px 40px; max-width: 1600px; margin: 0 auto; }
    .muted { color: var(--muted); }
    .grid { display: grid; grid-template-columns: repeat(6, minmax(120px, 1fr)); gap: 10px; }
    .card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 12px; }
    .metric { min-height: 72px; }
    .metric .value { display: block; font-size: 24px; font-weight: 700; margin-top: 4px; }
    .status { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: 2px 8px; font-size: 12px; }
    .accepted { color: var(--ok); }
    .failed { color: var(--bad); }
    .running { color: var(--warn); }
    .interrupted { color: var(--warn); }
    .partial { color: var(--muted); }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
    th { color: var(--muted); font-size: 12px; font-weight: 600; }
    td code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 360px; overflow: auto; margin: 0; }
    .section { margin-top: 16px; }
    .two { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; }
    .run-row { cursor: pointer; }
    .run-row:hover { background: #20252a; }
    .small { font-size: 12px; }
    .viz-scroll { overflow: auto; }
    .tree { min-width: 980px; padding-left: 72px; }
    .tree-row { position: relative; height: 64px; margin: 7px 0; }
    .tree-depth { position: absolute; left: -72px; top: 8px; width: 64px; color: var(--muted); font-size: 12px; }
    .tree-node { position: absolute; top: 0; bottom: 0; border: 1px solid var(--line); background: #14171a; border-radius: 6px; padding: 4px 5px; min-width: 0; overflow: hidden; font-size: 9px; line-height: 1.18; }
    .tree-node strong { display: block; font-size: 10px; line-height: 1.05; margin-bottom: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tree-node span { display: block; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tree-node .small { font-size: 9px; }
    .tree-node.leaf { border-color: rgba(139,214,147,0.55); }
    .tree-node.split { border-color: rgba(121,184,255,0.65); }
    .tree-node.error { border-color: rgba(255,139,139,0.7); }
    .tree-node.active { border-color: rgba(255,209,102,0.65); }
    .ruler { position: relative; height: 132px; border: 1px solid var(--line); border-radius: 8px; background: linear-gradient(90deg, #15191d, #111315); overflow: hidden; }
    .ruler-line { position: absolute; left: 10px; right: 10px; top: 62px; height: 2px; background: var(--line); }
    .tick { position: absolute; top: 32px; width: 1px; height: 58px; background: var(--accent); }
    .tick.leaf { background: var(--ok); }
    .tick.audio-only { background: var(--warn); width: auto; opacity: 0.18; top: 0; bottom: 0; height: auto; }
    .tick.failed-span { background: var(--bad); width: auto; opacity: 0.14; top: 0; bottom: 0; height: auto; }
    .tick-label { position: absolute; top: 8px; transform: translateX(-50%); max-width: 130px; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tick-time { position: absolute; top: 94px; transform: translateX(-50%); font-size: 10px; color: var(--muted); }
    details { border-top: 1px solid var(--line); padding: 8px 0; }
    summary { cursor: pointer; }
    @media (max-width: 1000px) { .grid, .two { grid-template-columns: 1fr; } header { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <header>
    <h1>Red Rising Curation Runs</h1>
    <div class="muted small">Auto-refreshes every 2s. Reading <span class="mono">${caseDir}</span>.</div>
  </header>
  <main>
    <section id="current"></section>
    <section class="section card">
      <h2>Previous Runs</h2>
      <div id="runs"></div>
    </section>
  </main>
  <script>
    let selectedRunId = null;
    let latestRuns = [];
    let openTraceDetails = new Set();
    const fmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", month: "short", day: "numeric" });
    const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
    const num = (value) => value == null ? "" : Number(value).toLocaleString();
    const time = (value) => value ? fmt.format(new Date(value)) : "";
    const dur = (seconds) => seconds == null ? "" : seconds < 90 ? Math.round(seconds) + "s" : Math.round(seconds / 60) + "m";
    const status = (s) => '<span class="status ' + esc(s) + '">' + esc(s) + '</span>';

    function metric(label, value, cls = "") {
      return '<div class="card metric"><span class="muted small">' + esc(label) + '</span><span class="value ' + cls + '">' + esc(value) + '</span></div>';
    }

    function spanTable(spans) {
      if (!spans.length) return '<div class="muted">None</div>';
      return '<table><thead><tr><th>Span</th><th>Nodes</th><th>Depth</th><th>Tools</th><th>Rejected</th><th>Last</th><th>Terminal</th></tr></thead><tbody>' +
        spans.map((s) => '<tr><td><code>' + esc(s.path) + '</code></td><td>' + esc(s.nodeCount ?? "") + '</td><td>' + esc(s.depth ?? "") + '</td><td>' + esc(s.toolCalls) + '</td><td>' + esc(s.judgeRejected) + '</td><td><code>' + esc(s.lastTool || s.lastEvent) + '</code><br><span class="muted small">' + esc(time(s.lastAt)) + '</span></td><td>' + esc(s.terminal ?? "") + '</td></tr>').join("") +
        '</tbody></table>';
    }

    function rejectedTable(nodes) {
      if (!nodes.length) return '<div class="muted">None</div>';
      return '<table><thead><tr><th>EPUB node</th><th>Rejects</th></tr></thead><tbody>' +
        nodes.map((n) => '<tr><td><code>' + esc(n.epubNodeId) + '</code></td><td>' + esc(n.count) + '</td></tr>').join("") +
        '</tbody></table>';
    }

    function failedTable(failed) {
      if (!failed.length) return '<div class="muted">None</div>';
      return '<table><thead><tr><th>Span</th><th>Nodes</th><th>Depth</th><th>Error</th><th>Elapsed</th></tr></thead><tbody>' +
        failed.map((f) => '<tr><td><code>' + esc(f.path) + '</code></td><td>' + esc(f.nodeCount ?? "") + '</td><td>' + esc(f.depth ?? "") + '</td><td>' + esc(f.error) + '</td><td>' + esc(dur((f.elapsedMs ?? 0) / 1000)) + '</td></tr>').join("") +
        '</tbody></table>';
    }

    function eventTailTable(events) {
      if (!events.length) return '<div class="muted">No events</div>';
      return '<table><thead><tr><th>Time</th><th>Type</th><th>Span</th><th>Tool</th><th>Message</th></tr></thead><tbody>' +
        events.map((e) => '<tr><td class="small">' + esc(time(e.ts)) + '</td><td><code>' + esc(e.type) + '</code></td><td><code>' + esc(e.spanPath ?? "") + '</code></td><td><code>' + esc(e.toolName ?? "") + '</code></td><td>' + esc(e.message) + '</td></tr>').join("") +
        '</tbody></table>';
    }

    function judgeTable(judges) {
      if (!judges.length) return '<div class="muted">No judge decisions</div>';
      return '<table><thead><tr><th>Time</th><th>Span</th><th>Node</th><th>At</th><th>Decision</th><th>Finding</th><th>Reason</th></tr></thead><tbody>' +
        judges.slice(0, 30).map((j) => '<tr><td class="small">' + esc(time(j.ts)) + '</td><td><code>' + esc(j.spanPath ?? "") + '</code></td><td><code>' + esc(j.epubNodeId ?? "") + '</code></td><td>' + esc(j.startTime ?? "") + '</td><td>' + (j.accepted ? '<span class="accepted">accepted</span>' : '<span class="failed">rejected</span>') + '</td><td><code>' + esc(j.finding ?? "") + '</code></td><td>' + esc(j.reason) + (j.concerns?.length ? '<br><span class="muted small">' + esc(j.concerns.join(" | ")) + '</span>' : '') + '</td></tr>').join("") +
        '</tbody></table>';
    }

    function traceInventory(run) {
      return '<div class="card"><h2>Trace Inventory</h2><div class="muted small"><code>' + esc(run.traceDir ?? "") + '</code></div>' +
        (run.traceFiles.length ? '<pre class="small">' + esc(run.traceFiles.join("\\n")) + '</pre>' : '<div class="muted">No trace files yet</div>') +
        '</div>';
    }

    function resultPanel(run) {
      const summary = run.resultSummary == null ? "No result JSON yet." : JSON.stringify(run.resultSummary, null, 2);
      return '<div class="card"><h2>Result JSON</h2><div class="muted small"><code>' + esc(run.resultPath ?? "") + '</code></div><pre class="small">' + esc(summary) + '</pre></div>';
    }

    function renderTree(run) {
      const byDepth = new Map();
      for (const span of run.treeSpans || []) {
        const depth = span.depth ?? 0;
        if (!byDepth.has(depth)) byDepth.set(depth, []);
        byDepth.get(depth).push(span);
      }
      const rows = [...byDepth.entries()].sort((a, b) => a[0] - b[0]).map(([depth, spans]) => {
        spans.sort((a, b) => a.path.localeCompare(b.path));
        return '<div class="tree-row"><div class="tree-depth">depth ' + esc(depth) + '</div>' +
          spans.map((s) => {
            const bits = s.path === "root" ? "" : s.path;
            let slot = 0;
            for (const bit of bits) slot = slot * 2 + (bit === "R" ? 1 : 0);
            const slots = Math.pow(2, depth);
            const left = depth === 0 ? 0 : slot / slots * 100;
            const width = depth === 0 ? 100 : 100 / slots;
            const label = (s.path ?? "") + ": " + (s.nodeCount ?? "") + " nodes, " + Math.round(s.startTime ?? 0) + "-" + Math.round(s.endTime ?? 0) + "s, tools " + (s.toolCalls ?? 0) + ", rejects " + (s.judgeRejected ?? 0) + ", " + (s.terminal ?? "active");
            return '<div class="tree-node ' + esc(s.terminal || "active") + '" title="' + esc(label) + '" style="left:calc(' + left + '% + 4px);width:calc(' + width + '% - 8px)"><strong><code>' + esc(s.path) + '</code></strong><span class="small muted">n ' + esc(s.nodeCount ?? "") + ' · ' + esc(Math.round(s.startTime ?? 0)) + '-' + esc(Math.round(s.endTime ?? 0)) + 's</span><span class="small">t ' + esc(s.toolCalls) + ' · r ' + esc(s.judgeRejected) + '</span><span class="small ' + (s.terminal === "error" ? "failed" : "") + '">' + esc(s.terminal ?? "active") + '</span></div>';
          }).join("") +
          '</div>';
      }).join("");
      return '<div class="card section"><h2>Binary Tree Progress</h2><div class="viz-scroll"><div class="tree">' + (rows || '<div class="muted">No spans yet</div>') + '</div></div></div>';
    }

    function pct(value, total) {
      if (!total || !Number.isFinite(value)) return 0;
      return Math.max(0, Math.min(100, value / total * 100));
    }

    function renderRuler(run) {
      const total = run.audiobookDurationSeconds || 1;
      const failed = (run.failedSpans || []).map((f) => {
        const span = (run.treeSpans || []).find((s) => s.path === f.path);
        if (!span || span.startTime == null || span.endTime == null) return "";
        return '<div class="tick failed-span" title="failed span ' + esc(f.path) + '" style="left:' + pct(span.startTime, total) + '%;width:' + Math.max(0.3, pct(span.endTime - span.startTime, total)) + '%"></div>';
      }).join("");
      const intervals = (run.audioOnlyIntervalDetails || []).map((i) => '<div class="tick audio-only" title="' + esc(i.kind + ': ' + i.notes) + '" style="left:' + pct(i.startTime, total) + '%;width:' + Math.max(0.3, pct(i.endTime - i.startTime, total)) + '%"></div>').join("");
      const labelPositions = [];
      const markers = (run.boundaryMarkers || []).map((m) => {
        const left = pct(m.startTime, total);
        const showLabel = m.source === "split" && labelPositions.every((position) => Math.abs(position - left) > 8);
        if (showLabel) labelPositions.push(left);
        return '<div class="tick ' + esc(m.source) + '" title="' + esc(m.startTime + 's ' + m.title + ' (' + m.source + ')') + '" style="left:' + left + '%"></div>' +
          (showLabel ? '<div class="tick-label" style="left:' + left + '%">' + esc(m.title) + '</div><div class="tick-time" style="left:' + left + '%">' + esc(Math.round(m.startTime)) + 's</div>' : '');
      }).join("");
      return '<div class="card section"><h2>EPUB/Audio Boundary Ruler</h2><div class="ruler"><div class="ruler-line"></div>' + failed + intervals + markers + '</div><div class="muted small">Blue ticks are accepted fulcrum splits, green ticks are accepted leaf chapter starts, yellow bands are audio-only intervals, red bands are failed spans.</div></div>';
    }

    function renderTraceSummaries(run) {
      if (!run.traceSummaries?.length) return '<div class="card section"><h2>Trace Summaries</h2><div class="muted">No trace summaries yet. Hidden chain-of-thought is not available; this panel shows stored reasoning summaries and outputs when present.</div></div>';
      return '<div class="card section"><h2>Trace Summaries</h2><div class="muted small">Shows stored model reasoning summaries and final/tool outputs where present, not hidden chain-of-thought.</div>' +
        run.traceSummaries.slice(0, 20).map((t) => '<details data-trace-file="' + esc(t.file) + '"' + (openTraceDetails.has(t.file) ? ' open' : '') + '><summary><code>' + esc(t.file) + '</code> <span class="muted">' + esc(t.kind) + ' ' + esc(t.title) + '</span></summary>' +
          (t.reasoningSummaries.length ? '<h2>Reasoning Summary</h2><pre class="small">' + esc(t.reasoningSummaries.join("\\n\\n")) + '</pre>' : '<div class="muted small">No stored reasoning summary in this trace.</div>') +
          '<h2>Final Output</h2><pre class="small">' + esc(JSON.stringify(t.finalOutput, null, 2)) + '</pre></details>').join("") +
        '</div>';
    }

    function rememberOpenDetails() {
      document.querySelectorAll("details[data-trace-file]").forEach((detail) => {
        const file = detail.getAttribute("data-trace-file");
        if (!file) return;
        if (detail.open) openTraceDetails.add(file);
        else openTraceDetails.delete(file);
      });
    }

    function attachPersistentDetails() {
      document.querySelectorAll("details[data-trace-file]").forEach((detail) => {
        detail.addEventListener("toggle", () => {
          const file = detail.getAttribute("data-trace-file");
          if (!file) return;
          if (detail.open) openTraceDetails.add(file);
          else openTraceDetails.delete(file);
        });
      });
    }

    function intervalTable(run) {
      const intervals = run.audioOnlyIntervalDetails || [];
      if (!intervals.length && !run.excludedNodes.length) return "";
      return '<div class="two section"><div class="card"><h2>Audio-Only Intervals</h2>' +
        (intervals.length ? '<table><thead><tr><th>Time</th><th>Kind</th><th>Notes</th></tr></thead><tbody>' + intervals.map((i) => '<tr><td><code>' + esc(i.startTime) + '-' + esc(i.endTime) + 's</code></td><td>' + esc(i.kind) + '</td><td>' + esc(i.notes) + '</td></tr>').join("") + '</tbody></table>' : '<div class="muted">None</div>') +
        '</div><div class="card"><h2>Excluded EPUB Nodes</h2>' +
        (run.excludedNodes.length ? '<table><thead><tr><th>Node</th><th>Reason</th><th>Notes</th></tr></thead><tbody>' + run.excludedNodes.map((n) => '<tr><td><code>' + esc(n.epubNodeId) + '</code></td><td>' + esc(n.reason) + '</td><td>' + esc(n.notes) + '</td></tr>').join("") + '</tbody></table>' : '<div class="muted">None</div>') +
        '</div></div>';
    }

    function renderCurrent(run) {
      if (!run) return '<div class="card">No runs found.</div>';
      return '<div class="card"><h2>Current Run ' + status(run.status) + '</h2><div class="muted small"><code>' + esc(run.id) + '</code> started ' + esc(time(run.startedAt)) + ', updated ' + esc(time(run.updatedAt)) + '<br>event log <code>' + esc(run.eventLog) + '</code></div></div>' +
        '<div class="grid section">' +
        metric("Splits", run.metrics.splits) +
        metric("Leaves", run.metrics.leaves) +
        metric("Span Errors", run.metrics.spanErrors, run.metrics.spanErrors ? "failed" : "") +
        metric("Judge Rejects", run.metrics.judgeRejected, run.metrics.judgeRejected ? "failed" : "") +
        metric("Tool Calls", run.metrics.toolCalls) +
        metric("Elapsed", dur(run.durationSeconds)) +
        '</div>' +
        '<div class="two section"><div class="card"><h2>Active Spans</h2>' + spanTable(run.activeSpans) + '</div><div class="card"><h2>Churn Spans</h2>' + spanTable(run.churnSpans) + '</div></div>' +
        renderTree(run) +
        renderRuler(run) +
        '<div class="two section"><div class="card"><h2>Failed Spans</h2>' + failedTable(run.failedSpans) + '</div><div class="card"><h2>Rejected Nodes</h2>' + rejectedTable(run.rejectedNodes) + '</div></div>' +
        intervalTable(run) +
        '<div class="section card"><h2>Recent Judge Decisions</h2>' + judgeTable(run.judgeDecisions) + '</div>' +
        renderTraceSummaries(run) +
        '<div class="section card"><h2>Event Tail</h2>' + eventTailTable(run.eventTail) + '</div>' +
        '<div class="two section">' + resultPanel(run) + traceInventory(run) + '</div>';
    }

    function renderRuns(runs) {
      return '<table><thead><tr><th>Run</th><th>Status</th><th>Started</th><th>Elapsed</th><th>Splits</th><th>Leaves</th><th>Errors</th><th>Judge +/-</th><th>Tools</th><th>EPUB</th><th>Files</th></tr></thead><tbody>' +
        runs.map((r) => '<tr class="run-row" data-run-id="' + esc(r.id) + '"><td><code>' + esc(r.id) + '</code>' + (r.id === selectedRunId ? ' <span class="muted small">selected</span>' : '') + '</td><td>' + status(r.status) + '</td><td>' + esc(time(r.startedAt)) + '</td><td>' + esc(dur(r.durationSeconds)) + '</td><td>' + esc(r.metrics.splits) + '</td><td>' + esc(r.metrics.leaves) + '</td><td>' + esc(r.metrics.spanErrors) + '</td><td><span class="accepted">' + esc(r.metrics.judgeAccepted) + '</span> / <span class="failed">' + esc(r.metrics.judgeRejected) + '</span></td><td>' + esc(r.metrics.toolCalls) + '</td><td>' + esc(r.epubEntries ?? "") + (r.originalEpubEntries ? ' / ' + esc(r.originalEpubEntries) : '') + '</td><td><code>' + esc(r.eventLog.split("/").pop()) + '</code></td></tr>').join("") +
        '</tbody></table>';
    }

    async function refresh() {
      rememberOpenDetails();
      const response = await fetch("/api/runs", { cache: "no-store" });
      const data = await response.json();
      latestRuns = data.runs;
      if (!selectedRunId && latestRuns[0]) selectedRunId = latestRuns[0].id;
      const selected = latestRuns.find((run) => run.id === selectedRunId) || latestRuns[0];
      document.querySelector("#current").innerHTML = renderCurrent(selected);
      document.querySelector("#runs").innerHTML = renderRuns(latestRuns);
      attachPersistentDetails();
      document.querySelectorAll(".run-row").forEach((row) => {
        row.addEventListener("click", () => {
          rememberOpenDetails();
          selectedRunId = row.getAttribute("data-run-id");
          const selected = latestRuns.find((run) => run.id === selectedRunId) || latestRuns[0];
          document.querySelector("#current").innerHTML = renderCurrent(selected);
          document.querySelector("#runs").innerHTML = renderRuns(latestRuns);
          attachPersistentDetails();
        });
      });
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>`;
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), { headers: { "content-type": "application/json; charset=utf-8" } });
}

Bun.serve({
  port,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/red-rising") {
      return new Response(html(), { headers: { "content-type": "text/html; charset=utf-8" } });
    }
    if (url.pathname === "/api/runs") {
      const runs = eventLogFiles().slice(0, 40).map(summarizeRun);
      return jsonResponse({ caseDir, runs });
    }
    return new Response("not found", { status: 404 });
  },
});

console.log(`Red Rising curation dashboard: http://localhost:${port}/red-rising`);
