import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

type JsonRecord = Record<string, any>;

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

type SpanSummary = {
  path: string;
  nodeCount: number | null;
  depth: number | null;
  startTime: number | null;
  endTime: number | null;
  lastAt: string | null;
  lastEvent: string;
  lastTool: string | null;
  toolCalls: number;
  judgeRejected: number;
  terminal: string | null;
};

const repoRoot = path.resolve(import.meta.dir, "../..");
const redRisingCaseDir = path.join(repoRoot, "tmp/chapter-cases/red-rising/prod");
const corpusRoot = path.join(repoRoot, "tmp/chapter-cases/corpus");

function titleCaseSlug(slug: string): string {
  return slug
    .split("-")
    .filter(Boolean)
    .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`)
    .join(" ");
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

function spanNodeCount(span: JsonRecord | undefined): number | null {
  if (!span) return null;
  if (typeof span.epubStartIndex !== "number" || typeof span.epubEndIndex !== "number") return null;
  return span.epubEndIndex - span.epubStartIndex + 1;
}

function ensureSpan(spans: Map<string, SpanSummary>, event: JsonRecord): SpanSummary | null {
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
    lastAt: null,
    lastEvent: "",
    lastTool: null,
    toolCalls: 0,
    judgeRejected: 0,
    terminal: null,
  };
  spans.set(pathValue, summary);
  return summary;
}

function secondsBetween(start: string | null, end: string | null): number | null {
  if (!start || !end) return null;
  const delta = new Date(end).getTime() - new Date(start).getTime();
  return Number.isFinite(delta) && delta >= 0 ? delta / 1000 : null;
}

function traceSummariesFor(traceDir: string | null): Array<{ file: string; kind: string; spanPath: string | null; title: string }> {
  if (!traceDir || !existsSync(traceDir) || !statSync(traceDir).isDirectory()) return [];
  return readdirSync(traceDir)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .slice(-160)
    .map((file) => {
      const spanPath = file.match(/-span-(.+?)-attempt-/)?.[1] ?? file.match(/fulcrum-judge-(.+?)-/)?.[1] ?? null;
      const kind = file.includes("fulcrum-judge") ? "judge" : file.includes("-span-") ? "curator" : "classifier";
      return { file, kind, spanPath, title: kind === "classifier" ? "audible EPUB classifier" : (spanPath ?? kind) };
    })
    .reverse();
}

function compactForDisplay(value: unknown, depth = 0): unknown {
  if (typeof value === "string") return value.length > 8_000 ? `${value.slice(0, 8_000)}\n... [truncated ${value.length - 8_000} chars]` : value;
  if (typeof value !== "object" || value === null) return value;
  if (depth >= 4) return Array.isArray(value) ? `[array length ${(value as unknown[]).length}]` : "[object truncated]";
  if (Array.isArray(value)) return value.slice(0, 20).map((item) => compactForDisplay(item, depth + 1));
  const compact: JsonRecord = {};
  for (const [key, entryValue] of Object.entries(value as JsonRecord).slice(0, 40)) compact[key] = compactForDisplay(entryValue, depth + 1);
  return compact;
}

function summarizeRun(ref: EventLogRef, includeDetails: boolean) {
  const events = readJsonl(ref.eventLog);
  const resultPath = artifactPathFor(ref, "result");
  const traceDir = artifactPathFor(ref, "trace");
  const spans = new Map<string, SpanSummary>();
  const metrics = { splits: 0, leaves: 0, spanErrors: 0, judgeAccepted: 0, judgeRejected: 0, toolCalls: 0 };
  const failedSpans: Array<{ path: string; startTime: number | null; endTime: number | null; error: string }> = [];
  const boundaryMarkers: Array<{ spanPath: string | null; epubNodeId: string | null; title: string; startTime: number; source: "split" | "leaf" }> = [];
  let status: "running" | "accepted" | "failed" | "interrupted" | "partial" = events.length > 0 ? "running" : "partial";
  let model: string | null = null;
  let epubEntries: number | null = null;
  let originalEpubEntries: number | null = null;
  let audiobookDurationSeconds: number | null = null;

  for (const event of events) {
    const type = String(event.type ?? "");
    if (type === "recursive-run-start") {
      model = typeof event.model === "string" ? event.model : model;
      epubEntries = typeof event.epubEntries === "number" ? event.epubEntries : epubEntries;
      originalEpubEntries = typeof event.originalEpubEntries === "number" ? event.originalEpubEntries : originalEpubEntries;
      audiobookDurationSeconds = typeof event.durationSeconds === "number" ? event.durationSeconds : audiobookDurationSeconds;
    }
    if (type === "recursive-merge-accepted") status = "accepted";
    if (type === "recursive-result-null" || type === "recursive-merge-rejected") status = "failed";
    if (type === "span-split-accepted") metrics.splits++;
    if (type === "span-leaf-accepted" || type === "span-auto-leaf") metrics.leaves++;
    if (type === "span-error") metrics.spanErrors++;
    if (type === "span-tool-call") metrics.toolCalls++;
    if (type === "fulcrum-judge-result") {
      if (event.judgment?.accepted === true) metrics.judgeAccepted++;
      if (event.judgment?.accepted === false) metrics.judgeRejected++;
    }
    const span = ensureSpan(spans, event);
    if (span) {
      span.lastAt = typeof event.ts === "string" ? event.ts : span.lastAt;
      span.lastEvent = type || span.lastEvent;
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
          startTime: span.startTime,
          endTime: span.endTime,
          error: typeof event.error === "string" ? event.error : JSON.stringify(event.error ?? ""),
        });
      }
    }
    if (type === "span-split-accepted" && typeof event.split?.startTime === "number") {
      boundaryMarkers.push({
        spanPath: typeof event.span?.path === "string" ? event.span.path : null,
        epubNodeId: typeof event.split?.epubNodeId === "string" ? event.split.epubNodeId : null,
        title: String(event.split?.title ?? event.split?.epubNodeId ?? "split"),
        startTime: event.split.startTime,
        source: "split",
      });
    }
    if ((type === "span-leaf-accepted" || type === "span-auto-leaf") && Array.isArray(event.result?.chapters ?? event.chapterPlan)) {
      for (const chapter of (event.result?.chapters ?? event.chapterPlan) as JsonRecord[]) {
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
  if (status === "running") status = "interrupted";
  const treeSpans = [...spans.values()].sort((a, b) => (a.depth ?? 0) - (b.depth ?? 0) || a.path.localeCompare(b.path));
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
    metrics,
    failedSpans,
    eventTail: includeDetails
      ? events.slice(-40).map((event) => ({
          ts: typeof event.ts === "string" ? event.ts : null,
          type: String(event.type ?? ""),
          message: typeof event.message === "string" ? event.message : "",
        }))
      : [],
    treeSpans: includeDetails ? treeSpans : [],
    boundaryMarkers: includeDetails ? boundaryMarkers.sort((a, b) => a.startTime - b.startTime).slice(0, 300) : [],
    traceSummaries: includeDetails ? traceSummariesFor(traceDir) : [],
  };
}

export function curationRunsResponse(selectedRunId: string | null): { caseDirs: string[]; runs: ReturnType<typeof summarizeRun>[] } {
  const logs = eventLogRefs().slice(0, 80);
  const detailedRunId = selectedRunId && logs.some((ref) => dashboardRunId(ref) === selectedRunId) ? selectedRunId : logs[0] ? dashboardRunId(logs[0]) : "";
  return {
    caseDirs: caseConfigs().map((config) => config.dir),
    runs: logs.map((ref) => summarizeRun(ref, dashboardRunId(ref) === detailedRunId)),
  };
}

export function curationTraceResponse(runId: string | null, file: string | null): unknown {
  const ref = eventLogRefs().find((candidate) => dashboardRunId(candidate) === runId);
  const traceDir = ref ? artifactPathFor(ref, "trace") : null;
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
    reasoningSummaries: (reasoningSummaries as string[]).map((summary: string) =>
      summary.length > 12_000 ? `${summary.slice(0, 12_000)}\n... [truncated ${summary.length - 12_000} chars]` : summary
    ),
    finalOutput: compactForDisplay(payload.finalOutput ?? payload.judgment ?? payload.selection ?? payload.error ?? null),
  };
}
