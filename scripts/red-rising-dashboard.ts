#!/usr/bin/env bun
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

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

type CaseConfig = {
  slug: string;
  label: string;
  dir: string;
  eventPrefix: string;
  resultPrefix: string;
  tracePrefix: string;
};

type EventLogRef = {
  caseConfig: CaseConfig;
  eventLog: string;
};

type RunSummary = {
  id: string;
  runId: string;
  caseSlug: string;
  caseLabel: string;
  caseDir: string;
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
const redRisingCaseDir = path.join(repoRoot, "tmp/chapter-cases/red-rising/prod");
const corpusRoot = path.join(repoRoot, "tmp/chapter-cases/corpus");
const port = Number(process.env.PORT ?? 7331);

function titleCaseSlug(slug: string): string {
  return slug.split("-").filter(Boolean).map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`).join(" ");
}

function caseConfigs(): CaseConfig[] {
  const configs: CaseConfig[] = [
    {
      slug: "red-rising",
      label: "Red Rising",
      dir: redRisingCaseDir,
      eventPrefix: "agent-events-m59-",
      resultPrefix: "agent-result-m59-",
      tracePrefix: "agent-traces-m59-",
    },
  ];
  if (existsSync(corpusRoot)) {
    for (const name of readdirSync(corpusRoot).sort()) {
      const dir = path.join(corpusRoot, name);
      if (!statSync(dir).isDirectory()) continue;
      configs.push({
        slug: name,
        label: titleCaseSlug(name),
        dir,
        eventPrefix: "agent-events-",
        resultPrefix: "agent-result-",
        tracePrefix: "agent-traces-",
      });
    }
  }
  return configs;
}

function eventLogRefs(): EventLogRef[] {
  return caseConfigs()
    .flatMap((caseConfig) => {
      if (!existsSync(caseConfig.dir)) return [];
      return readdirSync(caseConfig.dir)
        .filter((name) => name.startsWith(caseConfig.eventPrefix) && name.endsWith(".jsonl"))
        .map((name) => ({ caseConfig, eventLog: path.join(caseConfig.dir, name) }));
    })
    .sort((a, b) => statSync(b.eventLog).mtimeMs - statSync(a.eventLog).mtimeMs);
}

function hasLiveCurationRun(): boolean {
  try {
    const output = execFileSync("ps", ["-axo", "command="], { encoding: "utf8" });
    return output
      .split(/\n/)
      .some(
        (command) =>
          command.includes("bun tmp/run-red-rising-curation.ts") ||
          command.includes("bun run tmp/run-red-rising-curation.ts") ||
          command.includes("bun scripts/run-red-rising-curation.ts") ||
          command.includes("bun run scripts/run-red-rising-curation.ts") ||
          command.includes("bun tmp/run-corpus-curation.ts") ||
          command.includes("bun run tmp/run-corpus-curation.ts") ||
          command.includes("bun scripts/run-corpus-curation.ts") ||
          command.includes("bun run scripts/run-corpus-curation.ts")
      );
  } catch {
    return false;
  }
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

function runIdFromEventLog(ref: EventLogRef): string {
  return path.basename(ref.eventLog).replace(ref.caseConfig.eventPrefix, "").replace(/\.jsonl$/, "");
}

function dashboardRunId(ref: EventLogRef): string {
  return `${ref.caseConfig.slug}::${runIdFromEventLog(ref)}`;
}

function artifactPathFor(ref: EventLogRef, kind: "result" | "trace"): string | null {
  const id = runIdFromEventLog(ref);
  const candidate =
    kind === "result"
      ? path.join(ref.caseConfig.dir, `${ref.caseConfig.resultPrefix}${id}.json`)
      : path.join(ref.caseConfig.dir, `${ref.caseConfig.tracePrefix}${id}`);
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

function summarizeResultFile(filePath: string | null): unknown {
  if (!filePath || !existsSync(filePath)) return null;
  const stat = statSync(filePath);
  const value = readJsonFile(filePath) as JsonRecord | null;
  if (!value || typeof value !== "object") return { fileBytes: stat.size };
  const result = value.result && typeof value.result === "object" ? (value.result as JsonRecord) : null;
  return {
    fileBytes: stat.size,
    accepted: typeof result?.accepted === "boolean" ? result.accepted : null,
    chapters: Array.isArray(result?.chapters) ? result.chapters.length : null,
    strategy: typeof result?.strategy === "string" ? result.strategy : null,
    errors: Array.isArray(result?.errors) ? result.errors.slice(0, 8) : [],
    recursiveReports: Array.isArray(value.recursiveReports) ? value.recursiveReports.length : null,
    recursiveSpanTraces: Array.isArray(value.recursiveSpanTraces) ? value.recursiveSpanTraces.length : null,
  };
}

function compactForDisplay(value: unknown, depth = 0): unknown {
  if (typeof value === "string") return value.length > 8_000 ? `${value.slice(0, 8_000)}\n... [truncated ${value.length - 8_000} chars]` : value;
  if (typeof value !== "object" || value === null) return value;
  if (depth >= 4) return Array.isArray(value) ? `[array length ${(value as unknown[]).length}]` : "[object truncated]";
  if (Array.isArray(value)) {
    const shown = value.slice(0, 20).map((item) => compactForDisplay(item, depth + 1));
    return value.length > shown.length ? [...shown, `... [${value.length - shown.length} more items]`] : shown;
  }
  const entries = Object.entries(value as JsonRecord);
  const compact: JsonRecord = {};
  for (const [key, entryValue] of entries.slice(0, 40)) compact[key] = compactForDisplay(entryValue, depth + 1);
  if (entries.length > 40) compact.__truncatedKeys = entries.length - 40;
  return compact;
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
    .slice(-160)
    .map((file) => {
      const spanPath = file.match(/-span-(.+?)-attempt-/)?.[1] ?? file.match(/fulcrum-judge-(.+?)-/)?.[1] ?? null;
      const kind = file.includes("fulcrum-judge") ? "judge" : file.includes("-span-") ? "curator" : "classifier";
      const title = kind === "classifier" ? "audible EPUB classifier" : (spanPath ?? kind);
      return {
        file,
        kind,
        spanPath,
        title,
        reasoningSummaries: [],
        finalOutput: null,
      };
    })
    .reverse();
}

function traceDetailFor(traceDir: string | null, file: string | null): unknown {
  if (!traceDir || !file || path.basename(file) !== file) return null;
  const fullPath = path.join(traceDir, file);
  if (!fullPath.startsWith(traceDir + path.sep)) return null;
  const payload = readJsonFile(fullPath) as JsonRecord | null;
  if (!payload) return null;
  const rawResponses = Array.isArray(payload.rawResponses)
    ? payload.rawResponses
    : Array.isArray(payload.error?.state?.modelResponses)
      ? payload.error.state.modelResponses
      : [];
  const reasoningSummaries = rawResponses.flatMap((response: JsonRecord) =>
    Array.isArray(response.output)
      ? response.output.flatMap((item: JsonRecord) =>
          item?.type === "reasoning" && Array.isArray(item.content)
            ? item.content.map((content: JsonRecord) => String(content.text ?? "")).filter(Boolean)
            : []
        )
      : []
  );
  return {
    file,
    reasoningSummaries: reasoningSummaries.map((summary) => summary.length > 12_000 ? `${summary.slice(0, 12_000)}\n... [truncated ${summary.length - 12_000} chars]` : summary),
    finalOutput: compactForDisplay(payload.finalOutput ?? payload.judgment ?? payload.selection ?? payload.error ?? null),
  };
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

function summarizeRun(ref: EventLogRef, includeDetails = true, liveCurationRun = false): RunSummary {
  const events = readJsonl(ref.eventLog);
  const eventLogMtimeMs = existsSync(ref.eventLog) ? statSync(ref.eventLog).mtimeMs : 0;
  const resultPath = artifactPathFor(ref, "result");
  const traceDir = artifactPathFor(ref, "trace");
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
    if (type === "span-leaf-accepted" || type === "span-auto-leaf") metrics.leaves++;
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
      if (type === "span-leaf-accepted" || type === "span-auto-leaf") span.terminal = "leaf";
      if (type === "span-partial-leaf") span.terminal = "partial";
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
    if (type === "span-auto-leaf" && Array.isArray(event.chapterPlan)) {
      for (const chapter of event.chapterPlan) {
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
  if (status === "running" && !liveCurationRun) {
    status = "interrupted";
  }
  if (status === "running" && eventLogMtimeMs > 0 && Date.now() - eventLogMtimeMs > 180_000) {
    status = "interrupted";
  }
  const allSpans = [...spans.values()];
  const treeSpans = allSpans.filter((span) => span.lastEvent !== "span-auto-leaf" || span.toolCalls > 0 || span.judgeRejected > 0);
  const activeSpans = allSpans
    .filter((span) => !span.terminal)
    .sort((a, b) => (b.toolCalls - a.toolCalls) || String(b.lastAt ?? "").localeCompare(String(a.lastAt ?? "")))
    .slice(0, 12);
  const churnSpans = allSpans
    .sort((a, b) => (b.toolCalls - a.toolCalls) || (b.judgeRejected - a.judgeRejected))
    .slice(0, 12);

  return {
    id: dashboardRunId(ref),
    runId: runIdFromEventLog(ref),
    caseSlug: ref.caseConfig.slug,
    caseLabel: ref.caseConfig.label,
    caseDir: ref.caseConfig.dir,
    eventLog: ref.eventLog,
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
    eventTail: includeDetails ? events.slice(-80).map((event) => ({
      ts: typeof event.ts === "string" ? event.ts : null,
      type: String(event.type ?? ""),
      spanPath: typeof event.span?.path === "string" ? event.span.path : null,
      toolName: typeof event.toolName === "string" ? event.toolName : null,
      message: typeof event.message === "string" ? event.message : "",
    })) : [],
    resultSummary: includeDetails ? summarizeResultFile(resultPath) : null,
    traceFiles: includeDetails ? traceFilesFor(traceDir) : [],
    judgeDecisions: includeDetails ? judgeDecisions.slice(-80).reverse() : [],
    treeSpans: includeDetails ? treeSpans.sort((a, b) => (a.depth ?? 0) - (b.depth ?? 0) || a.path.localeCompare(b.path)) : [],
    treeEdges: includeDetails ? treeEdges : [],
    boundaryMarkers: includeDetails ? [...boundaryMarkers]
      .sort((a, b) => a.startTime - b.startTime || a.title.localeCompare(b.title))
      .filter((marker, index, markers) => index === 0 || marker.startTime !== markers[index - 1]!.startTime || marker.epubNodeId !== markers[index - 1]!.epubNodeId) : [],
    traceSummaries: includeDetails ? traceSummariesFor(traceDir) : [],
  };
}

function html(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Audiobook Curation Runs</title>
  <style>
    :root { color-scheme: dark; --bg: #111315; --panel: #191c1f; --line: #30363d; --text: #eceff3; --muted: #a9b0ba; --bad: #ff8b8b; --ok: #8bd693; --warn: #ffd166; --accent: #79b8ff; }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; overflow: hidden; }
    .app { display: grid; grid-template-columns: 340px minmax(0, 1fr); height: 100vh; }
    aside { border-right: 1px solid var(--line); background: #15181b; min-height: 0; overflow: auto; }
    .sidebar-head { position: sticky; top: 0; z-index: 2; background: rgba(21,24,27,0.96); border-bottom: 1px solid var(--line); padding: 14px; }
    .sidebar-section { padding: 14px; border-bottom: 1px solid var(--line); }
    .content { min-width: 0; min-height: 0; overflow: auto; }
    header { position: sticky; top: 0; z-index: 2; background: rgba(17,19,21,0.94); border-bottom: 1px solid var(--line); padding: 14px 18px; display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }
    h1 { margin: 0; font-size: 18px; font-weight: 650; }
    h2 { margin: 0 0 10px; font-size: 15px; font-weight: 650; }
    main { padding: 16px 18px 36px; max-width: none; margin: 0; }
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
    .run-list { display: grid; gap: 8px; }
    .run-button { width: 100%; text-align: left; background: #111417; border: 1px solid var(--line); border-radius: 8px; padding: 9px; color: var(--text); cursor: pointer; }
    .run-button:hover, .run-button.selected { border-color: var(--accent); background: #171c21; }
    .run-button strong { display: block; font-size: 12px; margin-bottom: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .run-meta { display: flex; gap: 8px; flex-wrap: wrap; color: var(--muted); font-size: 11px; }
    .param-list { display: grid; gap: 7px; }
    .param { display: grid; gap: 2px; min-width: 0; }
    .param .label { color: var(--muted); font-size: 11px; }
    .param .value { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; overflow-wrap: anywhere; }
    .small { font-size: 12px; }
    .header-controls { display: flex; align-items: center; justify-content: flex-end; gap: 12px; flex-wrap: wrap; }
    .toggle { display: inline-flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; user-select: none; }
    .toggle input { margin: 0; }
    button { background: #20252a; color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 4px 8px; font: inherit; font-size: 12px; cursor: pointer; }
    button:hover { border-color: var(--accent); }
    .viz-scroll { overflow: auto; }
    .tree { position: relative; min-width: 0; min-height: 720px; }
    .tree-depth-label { position: absolute; top: 0; color: var(--muted); font-size: 11px; transform: translateY(-100%); padding-bottom: 5px; }
    .tree-node { position: absolute; border: 1px solid var(--line); background: #14171a; border-radius: 6px; padding: 6px 7px; min-width: 0; overflow: hidden; font-size: 12px; line-height: 1.2; display: block; }
    .tree-node strong { display: block; font-size: 12px; line-height: 1.1; margin-bottom: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tree-node span { display: block; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tree-node .small { font-size: 11px; }
    .tree-node.trace-link { cursor: pointer; }
    .tree-node.trace-link:hover { filter: brightness(1.18); }
    .tree-node.selected { outline: 2px solid var(--accent); outline-offset: -2px; }
    .tree-node.leaf { border-color: rgba(139,214,147,0.55); }
    .tree-node.split { border-color: rgba(121,184,255,0.65); }
    .tree-node.error { border-color: rgba(255,139,139,0.7); }
    .tree-node.partial { border-color: rgba(169,176,186,0.65); }
    .tree-node.active { border-color: rgba(255,209,102,0.65); }
    .outcome { display: inline-flex; justify-content: center; border: 1px solid var(--line); border-radius: 999px; padding: 2px 7px; font-size: 11px; width: max-content; }
    .outcome.split { border-color: rgba(121,184,255,0.65); color: var(--accent); }
    .outcome.leaf { border-color: rgba(139,214,147,0.55); color: var(--ok); }
    .outcome.partial { border-color: rgba(169,176,186,0.65); color: var(--muted); }
    .outcome.error { border-color: rgba(255,139,139,0.7); color: var(--bad); }
    .outcome.active { border-color: rgba(255,209,102,0.65); color: var(--warn); }
    .ruler { position: relative; height: 132px; border: 1px solid var(--line); border-radius: 8px; background: linear-gradient(90deg, #15191d, #111315); overflow: hidden; }
    .ruler-line { position: absolute; left: 10px; right: 10px; top: 62px; height: 2px; background: var(--line); }
    .tick { position: absolute; top: 32px; width: 1px; height: 58px; background: var(--accent); }
    .tick.leaf { background: var(--ok); }
    .tick.audio-only { background: var(--warn); width: auto; opacity: 0.18; top: 0; bottom: 0; height: auto; }
    .tick.failed-span { background: var(--bad); width: auto; opacity: 0.14; top: 0; bottom: 0; height: auto; }
    .tick-label { position: absolute; top: 8px; transform: translateX(-50%); max-width: 130px; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tick-time { position: absolute; top: 94px; transform: translateX(-50%); font-size: 10px; color: var(--muted); }
    details { border-top: 1px solid var(--line); padding: 8px 0; }
    details.section-details { border-top: 0; padding: 0; }
    details.section-details > summary { list-style: none; }
    details.section-details > summary::-webkit-details-marker { display: none; }
    details.section-details > summary h2 { display: inline; }
    summary { cursor: pointer; }
    .trace-panel { min-height: 128px; }
    .trace-panel pre { max-height: 520px; }
    .trace-output { background: #101316; border: 1px solid var(--line); border-radius: 6px; padding: 10px; font-size: 12px; line-height: 1.38; }
    @media (max-width: 1000px) { body { overflow: auto; } .app { display: block; height: auto; } aside { max-height: none; } .grid, .two { grid-template-columns: 1fr; } header { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <div class="sidebar-head">
        <h1>Audiobook Curation</h1>
        <div class="header-controls section">
          <label class="toggle"><input id="auto-refresh-toggle" type="checkbox"> Auto-refresh 5s</label>
          <button id="manual-refresh" type="button">Refresh</button>
        </div>
      </div>
      <section class="sidebar-section">
        <h2>Current Run</h2>
        <div id="run-details"></div>
      </section>
      <section class="sidebar-section">
        <h2>Previous Runs</h2>
        <div id="runs"></div>
      </section>
    </aside>
    <div class="content">
      <header>
        <h1 id="main-title">Run</h1>
        <div class="muted small">Tree blocks with traces are clickable.</div>
      </header>
      <main id="current"></main>
    </div>
  </div>
  <script>
    let selectedRunId = null;
    let selectedTraceFile = null;
    let selectedTraceSpanPath = null;
    let latestRuns = [];
    let openTraceDetails = new Set();
    let openSectionDetails = new Set();
    let refreshTimer = null;
    const autoRefreshStorageKey = "red-rising-dashboard:auto-refresh";
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
      const traceSpanPaths = new Set((run.traceSummaries || []).map((trace) => trace.spanPath).filter(Boolean));
      const spans = (run.treeSpans || [])
        .filter((span) => span.lastEvent !== "span-auto-leaf" || span.toolCalls || span.judgeRejected)
        .sort((a, b) => (a.path === "root" ? "" : a.path).localeCompare(b.path === "root" ? "" : b.path));
      const maxDepth = Math.max(0, ...spans.map((span) => span.depth ?? 0));
      const columnWidth = 144;
      const columnGap = 12;
      const treeWidth = (maxDepth + 1) * columnWidth + maxDepth * columnGap;
      const deepestSlots = Math.pow(2, Math.min(maxDepth, 7));
      const treeHeight = Math.max(720, deepestSlots * 38);
      const labels = Array.from({ length: maxDepth + 1 }, (_, depth) => '<div class="tree-depth-label" style="left:' + (depth * (columnWidth + columnGap)) + 'px;width:' + columnWidth + 'px">depth ' + depth + '</div>').join("");
      const rows = spans.map((s) => {
        const depth = s.depth ?? 0;
        const label = (s.path ?? "") + ": " + (s.nodeCount ?? "") + " nodes, tools " + (s.toolCalls ?? 0) + ", rejects " + (s.judgeRejected ?? 0) + ", " + (s.terminal ?? "active");
        const trace = (run.traceSummaries || []).find((candidate) => candidate.spanPath === s.path);
        const hasTrace = traceSpanPaths.has(s.path);
        const outcome = s.terminal || "active";
        const bits = s.path === "root" ? "" : s.path;
        let slot = 0;
        for (const bit of bits) slot = slot * 2 + (bit === "R" ? 1 : 0);
        const slots = Math.pow(2, depth);
        const top = depth === 0 ? 0 : slot / slots * 100;
        const height = depth === 0 ? 100 : 100 / slots;
        const left = depth * (columnWidth + columnGap);
        const pixelHeight = Math.max(30, treeHeight * height / 100 - 8);
        const compact = pixelHeight < 50;
        return '<div class="tree-node ' + esc(outcome) + (hasTrace ? ' trace-link' : '') + (selectedTraceSpanPath === s.path ? ' selected' : '') + '" data-span-path="' + esc(s.path) + '" data-trace-file="' + esc(trace?.file ?? "") + '" title="' + esc(label + (hasTrace ? " - click to open trace" : "")) + '" style="left:' + left + 'px;top:calc(' + top + '% + 4px);height:' + pixelHeight + 'px;width:' + columnWidth + 'px">' +
          '<strong><code>' + esc(s.path) + '</code></strong>' +
          '<span class="small">n ' + esc(s.nodeCount ?? "") + ' · t ' + esc(s.toolCalls) + ' · r ' + esc(s.judgeRejected) + '</span>' +
          '</div>';
      }).join("");
      return '<div class="card section" id="tree"><h2>Binary Tree Progress</h2><div class="viz-scroll"><div class="tree" style="width:' + treeWidth + 'px;height:' + treeHeight + 'px;margin-top:18px">' + labels + (rows || '<div class="muted">No spans yet</div>') + '</div></div></div>';
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
      return '<div class="card section" id="ruler"><h2>EPUB/Audio Boundary Ruler</h2><div class="ruler"><div class="ruler-line"></div>' + failed + intervals + markers + '</div><div class="muted small">Blue ticks are accepted fulcrum splits, green ticks are accepted leaf chapter starts, yellow bands are audio-only intervals, red bands are failed spans.</div></div>';
    }

    function renderTraceSummaries(run) {
      if (!run.traceSummaries?.length) return '<div class="card section"><details class="section-details" data-section-details="trace-summaries"' + (openSectionDetails.has("trace-summaries") ? ' open' : '') + '><summary><h2>Trace Summaries</h2></summary><div class="muted">No trace summaries yet. Hidden chain-of-thought is not available; this panel shows stored reasoning summaries and outputs when present.</div></details></div>';
      return '<div class="card section"><details class="section-details" data-section-details="trace-summaries"' + (openSectionDetails.has("trace-summaries") ? ' open' : '') + '><summary><h2>Trace Summaries</h2></summary><div class="muted small">Shows stored model reasoning summaries and final/tool outputs where present, not hidden chain-of-thought.</div>' +
        run.traceSummaries.map((t) => '<details data-trace-file="' + esc(t.file) + '" data-trace-span-path="' + esc(t.spanPath ?? "") + '"' + (openTraceDetails.has(t.file) ? ' open' : '') + '><summary><code>' + esc(t.file) + '</code> <span class="muted">' + esc(t.kind) + ' ' + esc(t.title) + '</span></summary><div class="trace-detail-body muted small">Open to load trace detail.</div></details>').join("") +
        '</details></div>';
    }

    function renderTracePanel(run) {
      const traces = run.traceSummaries || [];
      const trace = selectedTraceFile ? traces.find((candidate) => candidate.file === selectedTraceFile) : null;
      if (!trace) {
        return '<div class="card section trace-panel" id="trace-panel"><h2>Thinking Trace</h2><div class="muted">Click a binary-tree span with recorded trace data.</div></div>';
      }
      return '<div class="card section trace-panel" id="trace-panel" data-trace-file="' + esc(trace.file) + '"><h2>Thinking Trace <code>' + esc(trace.spanPath ?? "") + '</code></h2><div class="muted small"><code>' + esc(trace.file) + '</code></div><div id="trace-detail-body" class="muted small">Loading trace detail...</div></div>';
    }

    function rememberOpenDetails() {
      document.querySelectorAll("details[data-trace-file]").forEach((detail) => {
        const file = detail.getAttribute("data-trace-file");
        if (!file) return;
        if (detail.open) openTraceDetails.add(file);
        else openTraceDetails.delete(file);
      });
      document.querySelectorAll("details[data-section-details]").forEach((detail) => {
        const section = detail.getAttribute("data-section-details");
        if (!section) return;
        if (detail.open) openSectionDetails.add(section);
        else openSectionDetails.delete(section);
      });
    }

    function attachPersistentDetails() {
      document.querySelectorAll("details[data-section-details]").forEach((detail) => {
        detail.addEventListener("toggle", () => {
          const section = detail.getAttribute("data-section-details");
          if (!section) return;
          if (detail.open) openSectionDetails.add(section);
          else openSectionDetails.delete(section);
        });
      });
      document.querySelectorAll("details[data-trace-file]").forEach((detail) => {
        detail.addEventListener("toggle", () => {
          const file = detail.getAttribute("data-trace-file");
          if (!file) return;
          if (detail.open) {
            openTraceDetails.add(file);
            loadTraceDetail(detail);
          } else openTraceDetails.delete(file);
        });
      });
      document.querySelectorAll("details[data-trace-file][open]").forEach((detail) => loadTraceDetail(detail));
    }

    async function loadTraceDetail(detail) {
      const file = detail.getAttribute("data-trace-file");
      const body = detail.querySelector(".trace-detail-body");
      if (!file || !body || body.getAttribute("data-loaded") === "1") return;
      body.textContent = "Loading trace detail...";
      const response = await fetch("/api/trace?runId=" + encodeURIComponent(selectedRunId || "") + "&file=" + encodeURIComponent(file), { cache: "no-store" });
      const data = await response.json();
      const reasoning = data.reasoningSummaries?.length ? '<h2>Reasoning Summary</h2><pre class="small">' + esc(data.reasoningSummaries.join("\\n\\n")) + '</pre>' : '<div class="muted small">No stored reasoning summary in this trace.</div>';
      body.className = "trace-detail-body";
      body.setAttribute("data-loaded", "1");
      body.innerHTML = reasoning + renderFinalOutput(data.finalOutput);
    }

    function normalizeTraceOutput(value) {
      let current = value;
      for (let i = 0; i < 3; i++) {
        if (typeof current !== "string") break;
        const trimmed = current.trim();
        if (!trimmed || !/^[\\[{"]/.test(trimmed)) break;
        try {
          current = JSON.parse(trimmed);
        } catch {
          break;
        }
      }
      return current;
    }

    function renderFinalOutput(value) {
      const normalized = normalizeTraceOutput(value);
      const text = typeof normalized === "string" ? normalized : JSON.stringify(normalized ?? null, null, 2);
      return '<h2>Final Output</h2><pre class="trace-output">' + esc(text) + '</pre>';
    }

    async function loadSelectedTracePanel() {
      const panel = document.querySelector("#trace-panel[data-trace-file]");
      const body = document.querySelector("#trace-detail-body");
      if (!panel || !body) return;
      const file = panel.getAttribute("data-trace-file");
      if (!file) return;
      body.textContent = "Loading trace detail...";
      const response = await fetch("/api/trace?runId=" + encodeURIComponent(selectedRunId || "") + "&file=" + encodeURIComponent(file), { cache: "no-store" });
      const data = await response.json();
      const reasoning = data.reasoningSummaries?.length ? '<h2>Reasoning Summary</h2><pre class="small">' + esc(data.reasoningSummaries.join("\\n\\n")) + '</pre>' : '<div class="muted small">No stored reasoning summary in this trace.</div>';
      body.className = "";
      body.innerHTML = reasoning + renderFinalOutput(data.finalOutput);
    }

    function attachTreeTraceLinks() {
      document.querySelectorAll(".tree-node.trace-link[data-span-path]").forEach((node) => {
        node.addEventListener("click", () => {
          const spanPath = node.getAttribute("data-span-path");
          const traceFile = node.getAttribute("data-trace-file");
          if (!spanPath) return;
          if (!traceFile) return;
          selectedTraceSpanPath = spanPath;
          selectedTraceFile = traceFile;
          const selected = latestRuns.find((run) => run.id === selectedRunId) || latestRuns[0];
          document.querySelector("#current").innerHTML = renderCurrent(selected);
          attachTreeTraceLinks();
          loadSelectedTracePanel();
          document.querySelector("#trace-panel")?.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      });
    }

    function autoRefreshEnabled() {
      try {
        return window.localStorage.getItem(autoRefreshStorageKey) !== "0";
      } catch {
        return true;
      }
    }

    function setAutoRefreshEnabled(enabled) {
      try {
        window.localStorage.setItem(autoRefreshStorageKey, enabled ? "1" : "0");
      } catch {
      }
      const toggle = document.querySelector("#auto-refresh-toggle");
      if (toggle) toggle.checked = enabled;
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
      if (enabled) refreshTimer = setInterval(refresh, 5000);
    }

    function intervalTable(run) {
      const intervals = run.audioOnlyIntervalDetails || [];
      if (!intervals.length && !run.excludedNodes.length) return "";
      return '<div class="two section" id="filters"><div class="card"><h2>Audio-Only Intervals</h2>' +
        (intervals.length ? '<table><thead><tr><th>Time</th><th>Kind</th><th>Notes</th></tr></thead><tbody>' + intervals.map((i) => '<tr><td><code>' + esc(i.startTime) + '-' + esc(i.endTime) + 's</code></td><td>' + esc(i.kind) + '</td><td>' + esc(i.notes) + '</td></tr>').join("") + '</tbody></table>' : '<div class="muted">None</div>') +
        '</div><div class="card"><h2>Excluded EPUB Nodes</h2>' +
        (run.excludedNodes.length ? '<table><thead><tr><th>Node</th><th>Reason</th><th>Notes</th></tr></thead><tbody>' + run.excludedNodes.map((n) => '<tr><td><code>' + esc(n.epubNodeId) + '</code></td><td>' + esc(n.reason) + '</td><td>' + esc(n.notes) + '</td></tr>').join("") + '</tbody></table>' : '<div class="muted">None</div>') +
        '</div></div>';
    }

    function renderCurrent(run) {
      if (!run) return '<div class="card">No runs found.</div>';
      return renderTree(run) +
        renderTracePanel(run) +
        renderRuler(run) +
        '<div class="grid section">' +
        metric("Tool Calls", num(run.metrics.toolCalls)) +
        metric("Elapsed", dur(run.durationSeconds)) +
        metric("Span Errors", num(run.metrics.spanErrors), run.metrics.spanErrors ? "failed" : "") +
        metric("Judge Rejects", num(run.metrics.judgeRejected), run.metrics.judgeRejected ? "failed" : "") +
        '</div>' +
        '<div class="section card" id="active-spans"><h2>Active Spans</h2>' + spanTable(run.activeSpans) + '</div>' +
        intervalTable(run) +
        '<div class="section card" id="event-tail"><details class="section-details" data-section-details="event-tail"' + (openSectionDetails.has("event-tail") ? ' open' : '') + '><summary><h2>Event Tail</h2></summary>' + eventTailTable(run.eventTail) + '</details></div>';
    }

    function renderRuns(runs) {
      return '<div class="run-list">' +
        runs.map((r) => '<button class="run-button' + (r.id === selectedRunId ? ' selected' : '') + '" type="button" data-run-id="' + esc(r.id) + '"><strong>' + esc(r.caseLabel) + '</strong><div class="run-meta"><span>' + status(r.status) + '</span><span>' + esc(time(r.startedAt)) + '</span><span>' + esc(dur(r.durationSeconds)) + '</span></div><div class="run-meta"><span>s ' + esc(r.metrics.splits) + '</span><span>l ' + esc(r.metrics.leaves) + '</span><span>t ' + esc(num(r.metrics.toolCalls)) + '</span></div></button>').join("") +
        '</div>';
    }

    function renderRunDetails(run) {
      if (!run) return '<div class="muted">No run selected.</div>';
      return '<div class="param-list">' +
        '<div class="param"><span class="label">Book</span><span class="value">' + esc(run.caseLabel) + '</span></div>' +
        '<div class="param"><span class="label">Status</span><span class="value">' + status(run.status) + '</span></div>' +
        '<div class="param"><span class="label">Run</span><span class="value">' + esc(run.runId) + '</span></div>' +
        '<div class="param"><span class="label">Started</span><span class="value">' + esc(time(run.startedAt)) + '</span></div>' +
        '<div class="param"><span class="label">Updated</span><span class="value">' + esc(time(run.updatedAt)) + '</span></div>' +
        '<div class="param"><span class="label">Model</span><span class="value">' + esc(run.model ?? "") + '</span></div>' +
        '<div class="param"><span class="label">EPUB nodes</span><span class="value">' + esc(run.epubEntries ?? "") + (run.originalEpubEntries ? ' / ' + esc(run.originalEpubEntries) : '') + '</span></div>' +
        '<div class="param"><span class="label">Splits / Leaves</span><span class="value">' + esc(run.metrics.splits) + ' / ' + esc(run.metrics.leaves) + '</span></div>' +
        '<details><summary class="small muted">Files</summary>' +
        '<div class="param"><span class="label">Case directory</span><span class="value">' + esc(run.caseDir) + '</span></div>' +
        '<div class="param"><span class="label">Event log</span><span class="value">' + esc(run.eventLog) + '</span></div>' +
        '</details>' +
        '</div>';
    }

    function attachRunButtons() {
      document.querySelectorAll(".run-button").forEach((row) => {
        row.addEventListener("click", () => {
          const nextRunId = row.getAttribute("data-run-id");
          if (!nextRunId || nextRunId === selectedRunId) return;
          rememberOpenDetails();
          selectedRunId = nextRunId;
          selectedTraceFile = null;
          selectedTraceSpanPath = null;
          refresh();
        });
      });
    }

    async function refresh() {
      rememberOpenDetails();
      const response = await fetch("/api/runs?selectedRunId=" + encodeURIComponent(selectedRunId || ""), { cache: "no-store" });
      const data = await response.json();
      latestRuns = data.runs;
      if (!selectedRunId && latestRuns[0]) selectedRunId = latestRuns[0].id;
      const selected = latestRuns.find((run) => run.id === selectedRunId) || latestRuns[0];
      if (selectedTraceFile && selected && !(selected.traceSummaries || []).some((trace) => trace.file === selectedTraceFile)) {
        selectedTraceFile = null;
        selectedTraceSpanPath = null;
      }
      document.querySelector("#main-title").innerHTML = selected ? esc(selected.caseLabel) + " " + status(selected.status) : "Run";
      document.querySelector("#run-details").innerHTML = renderRunDetails(selected);
      document.querySelector("#current").innerHTML = renderCurrent(selected);
      document.querySelector("#runs").innerHTML = renderRuns(latestRuns);
      attachPersistentDetails();
      attachTreeTraceLinks();
      attachRunButtons();
      loadSelectedTracePanel();
    }
    document.querySelector("#auto-refresh-toggle").checked = autoRefreshEnabled();
    document.querySelector("#auto-refresh-toggle").addEventListener("change", (event) => {
      setAutoRefreshEnabled(event.target.checked);
    });
    document.querySelector("#manual-refresh").addEventListener("click", () => refresh());
    refresh();
    setAutoRefreshEnabled(autoRefreshEnabled());
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
      const selectedRunId = url.searchParams.get("selectedRunId");
      const logs = eventLogRefs().slice(0, 80);
      const detailedRunId = selectedRunId && logs.some((ref) => dashboardRunId(ref) === selectedRunId) ? selectedRunId : logs[0] ? dashboardRunId(logs[0]) : "";
      const liveCurationRun = hasLiveCurationRun();
      const runs = logs.map((ref) => summarizeRun(ref, dashboardRunId(ref) === detailedRunId, liveCurationRun));
      return jsonResponse({ caseDirs: caseConfigs().map((config) => config.dir), runs });
    }
    if (url.pathname === "/api/trace") {
      const runId = url.searchParams.get("runId");
      const file = url.searchParams.get("file");
      const ref = eventLogRefs().find((candidate) => dashboardRunId(candidate) === runId);
      if (!ref) return new Response("run not found", { status: 404 });
      return jsonResponse(traceDetailFor(artifactPathFor(ref, "trace"), file));
    }
    return new Response("not found", { status: 404 });
  },
});

console.log(`Audiobook curation dashboard: http://localhost:${port}/red-rising`);
