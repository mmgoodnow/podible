#!/usr/bin/env bun
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { loadEpubEntries } from "../src/library/chapter-analysis";
import {
  resolveNodeBoundaryChapters,
  type NodeBoundaryCurationReport,
  type NodeBoundaryDecision,
  type SubmittedChapter,
} from "../src/library/chapter-curation";

type JsonRecord = Record<string, any>;

const boundaryDiagnosticStopTokens = new Set(
  "about after again among before being between because cannot could does done from have into just like more most much only over should some than that their them then there they this through under very were what when where which while will with within without would your".split(" ")
);

type EventSummary = {
  eventLog: string | null;
  wallSeconds: number | null;
  usage: {
    requests: number;
    tokens: number;
    costUsd: number;
    byRole: Record<string, { requests: number; tokens: number; costUsd: number }>;
  };
  tools: Record<string, number>;
  nodeBoundaries: {
    started: number;
    deterministicAccepted: number;
    agentAccepted: number;
    failed: number;
  };
};

type ModeSummary = {
  mode: "recursive" | "node";
  resultPath: string | null;
  eventLogPath: string | null;
  accepted: boolean | null;
  chapters: number | null;
  elapsedMs: number | null;
  wallSeconds: number | null;
  requests: number;
  tokens: number;
  costUsd: number;
  tools: Record<string, number>;
  accuracy: "not_scored";
  nodeBoundarySources?: {
    started: number;
    deterministicAccepted: number;
    agentAccepted: number;
    failed: number;
  };
  nodeReports?: {
    total: number;
    accepted: number;
    failed: number;
    dropped: number;
  };
  nodeReplay?: {
    chapters: number;
    acceptedReports: number;
    failedReports: number;
    droppedReports: number;
    coverage: number;
    monotonicErrors: number;
    acceptedAfterReplay: boolean;
  };
  nodeFailureDiagnostic?: {
    kind: string;
    message: string;
    failedExpectedWindowOverlap: number | null;
    firstCuratedNodeOpeningOverlap: number | null;
  };
};

type CaseSummary = {
  slug: string;
  title: string | null;
  author: string | null;
  runnable: boolean;
  recursive: ModeSummary | null;
  node: ModeSummary | null;
  comparison: {
    nodeHasRun: boolean;
    recursiveHasRun: boolean;
    nodeAcceptedMinusRecursive: number | null;
    nodeChaptersMinusRecursive: number | null;
    nodeWallSecondsMinusRecursive: number | null;
    nodeRequestsMinusRecursive: number | null;
    nodeTokensMinusRecursive: number | null;
    nodeCostUsdMinusRecursive: number | null;
  };
};

function usage(): never {
  console.error("Usage: bun run scripts/compare-curation-modes.ts [tmp/chapter-cases/corpus]");
  process.exit(1);
}

function readJson(filePath: string): JsonRecord | null {
  try {
    return JSON.parse(readFileSync(filePath, "utf8")) as JsonRecord;
  } catch {
    return null;
  }
}

function readEvents(filePath: string | null): JsonRecord[] {
  if (!filePath || !existsSync(filePath)) return [];
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

function round(value: number, digits = 3): number {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function roundMoney(value: number): number {
  return round(value, 6);
}

function diagnosticTokens(value: string): string[] {
  return (
    value
      .toLowerCase()
      .match(/[a-z0-9]+(?:'[a-z0-9]+)?/g)
      ?.map((token) => token.replace(/[^a-z0-9]+/g, ""))
      .filter((token) => token.length >= 4 && !boundaryDiagnosticStopTokens.has(token)) ?? []
  );
}

function overlapRatio(sampleTokens: string[], candidateTokens: Set<string>): number {
  if (sampleTokens.length === 0) return 0;
  const hits = sampleTokens.filter((token) => candidateTokens.has(token)).length;
  return round(hits / sampleTokens.length, 3);
}

function timestampSeconds(start: string | null, end: string | null): number | null {
  if (!start || !end) return null;
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  return round(Math.max(0, (endMs - startMs) / 1000), 1);
}

function summarizeEvents(eventLogPath: string | null): EventSummary {
  const events = readEvents(eventLogPath);
  const firstTs = events.find((event) => typeof event.ts === "string")?.ts ?? null;
  const lastTs = [...events].reverse().find((event) => typeof event.ts === "string")?.ts ?? null;
  const summary: EventSummary = {
    eventLog: eventLogPath,
    wallSeconds: timestampSeconds(firstTs, lastTs),
    usage: { requests: 0, tokens: 0, costUsd: 0, byRole: {} },
    tools: {},
    nodeBoundaries: { started: 0, deterministicAccepted: 0, agentAccepted: 0, failed: 0 },
  };

  for (const event of events) {
    if (event.type === "agent-usage") {
      const role = typeof event.role === "string" ? event.role : "unknown";
      const requests = typeof event.usage?.requests === "number" ? event.usage.requests : 0;
      const tokens = typeof event.usage?.totalTokens === "number" ? event.usage.totalTokens : 0;
      const costUsd = typeof event.cost?.amountUsd === "number" ? event.cost.amountUsd : typeof event.costUsd === "number" ? event.costUsd : 0;
      summary.usage.requests += requests;
      summary.usage.tokens += tokens;
      summary.usage.costUsd += costUsd;
      const roleSummary = summary.usage.byRole[role] ?? { requests: 0, tokens: 0, costUsd: 0 };
      roleSummary.requests += requests;
      roleSummary.tokens += tokens;
      roleSummary.costUsd += costUsd;
      summary.usage.byRole[role] = roleSummary;
    }

    if (event.type === "span-tool-call" || event.type === "agent-tool-call") {
      const toolName = typeof event.toolName === "string" ? event.toolName : "unknown";
      summary.tools[toolName] = (summary.tools[toolName] ?? 0) + 1;
    }

    if (event.type === "node-boundary-start") summary.nodeBoundaries.started += 1;
    if (event.type === "node-boundary-result") {
      const message = typeof event.message === "string" ? event.message : "";
      const accepted = event.decision?.accepted === true || message.includes("accepted=1");
      const deterministic = message.includes("deterministic=1");
      if (accepted && deterministic) summary.nodeBoundaries.deterministicAccepted += 1;
      else if (accepted) summary.nodeBoundaries.agentAccepted += 1;
      else summary.nodeBoundaries.failed += 1;
    }
  }

  summary.usage.costUsd = roundMoney(summary.usage.costUsd);
  for (const roleSummary of Object.values(summary.usage.byRole)) roleSummary.costUsd = roundMoney(roleSummary.costUsd);
  return summary;
}

function latestMatchingFile(caseDir: string, pattern: RegExp): string | null {
  const matches = readdirSync(caseDir)
    .filter((name) => pattern.test(name))
    .map((name) => path.join(caseDir, name))
    .sort((a, b) => a.localeCompare(b));
  return matches.at(-1) ?? null;
}

function eventLogForResult(caseDir: string, mode: "recursive" | "node", resultPath: string | null): string | null {
  if (!resultPath) return latestEventLog(caseDir, mode);
  const resultName = path.basename(resultPath);
  const prefix = mode === "node" ? "node-agent-result-" : resultName.startsWith("recursive-agent-result-") ? "recursive-agent-result-" : "agent-result-";
  const suffix = resultName.slice(prefix.length).replace(/\.json$/, "");
  const eventName =
    mode === "node"
      ? `node-agent-events-${suffix}.jsonl`
      : prefix === "recursive-agent-result-"
        ? `recursive-agent-events-${suffix}.jsonl`
        : `agent-events-${suffix}.jsonl`;
  const candidate = path.join(caseDir, eventName);
  return existsSync(candidate) ? candidate : latestEventLog(caseDir, mode);
}

function latestEventLog(caseDir: string, mode: "recursive" | "node"): string | null {
  return mode === "node"
    ? latestMatchingFile(caseDir, /^node-agent-events-.*\.jsonl$/)
    : latestMatchingFile(caseDir, /^(?:recursive-)?agent-events-.*\.jsonl$/);
}

function latestResult(caseDir: string, mode: "recursive" | "node"): string | null {
  return mode === "node"
    ? latestMatchingFile(caseDir, /^node-agent-result-.*\.json$/)
    : latestMatchingFile(caseDir, /^(?:recursive-)?agent-result-.*\.json$/);
}

function monotonicErrors(chapters: SubmittedChapter[]): number {
  let errors = 0;
  for (let index = 1; index < chapters.length; index++) {
    if (chapters[index]!.startTime <= chapters[index - 1]!.startTime) errors += 1;
  }
  return errors;
}

async function replayNodeReports(caseDir: string, result: JsonRecord): Promise<ModeSummary["nodeReplay"]> {
  const originalReports = Array.isArray(result.nodeBoundaryReports) ? (result.nodeBoundaryReports as NodeBoundaryCurationReport[]) : [];
  if (originalReports.length === 0) return undefined;
  const acceptedById = new Map(
    originalReports
      .filter((report) => (report.outcome === "accepted" || report.outcome === "dropped") && typeof report.startTime === "number")
      .map((report) => [report.epubNodeId, report])
  );
  const reportIds = new Set(originalReports.map((report) => report.epubNodeId));
  const epubEntries = (await loadEpubEntries(path.join(caseDir, "book.epub"))).filter((entry) => reportIds.has(entry.id));
  const replayReports: NodeBoundaryCurationReport[] = [];
  const chapters = await resolveNodeBoundaryChapters(
    { durationMs: Number(result.durationMs ?? 0) || Number(readJson(path.join(caseDir, "metadata.json"))?.audio_duration_ms ?? 0), epubEntries },
    async (target): Promise<NodeBoundaryDecision | null> => {
      const report = acceptedById.get(target.epubNodeId);
      if (!report || typeof report.startTime !== "number") return null;
      return {
        accepted: true,
        kind: "node_boundary",
        spanPath: "root",
        epubNodeId: target.epubNodeId,
        epubIndex: target.epubIndex,
        title: target.title,
        startTime: report.startTime,
        notes: null,
        audit: {
          epubNodeId: target.epubNodeId,
          title: target.title,
          startTime: report.startTime,
          boundaryComparison: {
            transcriptPrecision: "utterance",
            transcriptPrecisionNote: null,
            previousEpub: { epubNodeId: null, title: null, tailText: "" },
            targetEpub: { epubNodeId: target.epubNodeId, title: target.title, headText: "" },
            transcriptBefore: "",
            transcriptAfter: "",
          },
          transcriptWindow: "",
          candidates: [],
        },
      };
    },
    { maxConcurrency: 1, reports: replayReports }
  );
  const replayed = chapters ?? [];
  const acceptedReports = replayReports.filter((report) => report.outcome === "accepted").length;
  const failedReports = replayReports.filter((report) => report.outcome === "failed").length;
  const droppedReports = replayReports.filter((report) => report.outcome === "dropped").length;
  const coverage = replayReports.length === 0 ? 0 : (acceptedReports + droppedReports) / replayReports.length;
  return {
    chapters: replayed.length,
    acceptedReports,
    failedReports,
    droppedReports,
    coverage: round(coverage, 3),
    monotonicErrors: monotonicErrors(replayed),
    acceptedAfterReplay: replayed.length > 0 && monotonicErrors(replayed) === 0 && coverage >= 0.9,
  };
}

async function buildNodeFailureDiagnostic(caseDir: string, reports: NodeBoundaryCurationReport[]): Promise<ModeSummary["nodeFailureDiagnostic"]> {
  const failedReports = reports.filter((report) => report.outcome === "failed");
  const acceptedReports = reports.filter((report) => report.outcome === "accepted" || report.outcome === "dropped");
  if (reports.length === 0 || failedReports.length === 0) return undefined;
  const transcript = readJson(path.join(caseDir, "transcript.json"));
  const utterances = Array.isArray(transcript?.utterances) ? (transcript.utterances as Array<{ startMs?: unknown; endMs?: unknown; text?: unknown }>) : [];
  if (utterances.length === 0) return undefined;
  const entries = await loadEpubEntries(path.join(caseDir, "book.epub"));
  const failedOverlaps = failedReports.flatMap((report) => {
    const entry = entries.find((candidate) => candidate.id === report.epubNodeId);
    if (!entry) return [];
    const sampleTokens = diagnosticTokens(entry.words.slice(0, 160).map((word) => word.text).join(" "));
    const startMs = Math.max(0, Math.round((report.expectedStartTime - 900) * 1000));
    const endMs = Math.round((report.expectedStartTime + 900) * 1000);
    const windowTokens = new Set(
      diagnosticTokens(
        utterances
          .filter((utterance) => typeof utterance.startMs === "number" && typeof utterance.endMs === "number" && utterance.endMs >= startMs && utterance.startMs <= endMs)
          .map((utterance) => String(utterance.text ?? ""))
          .join(" ")
      )
    );
    return [{ report, overlap: overlapRatio(sampleTokens, windowTokens) }];
  });
  if (failedOverlaps.length === 0) return undefined;
  const failedExpectedWindowOverlap = round(failedOverlaps.reduce((sum, item) => sum + item.overlap, 0) / failedOverlaps.length, 3);
  const firstEntry = entries.find((entry) => reports.some((report) => report.epubNodeId === entry.id));
  const firstCuratedNodeOpeningOverlap = firstEntry
    ? overlapRatio(
        diagnosticTokens(firstEntry.words.slice(0, 160).map((word) => word.text).join(" ")),
        new Set(
          diagnosticTokens(
            utterances
              .slice(0, 80)
              .map((utterance) => String(utterance.text ?? ""))
              .join(" ")
          )
        )
      )
    : null;
  const allBoundariesFailed = reports.length > 0 && acceptedReports.length === 0;
  const lowExpectedWindowOverlap = failedExpectedWindowOverlap < 0.45;
  const kind = allBoundariesFailed ? "all_boundaries_failed" : lowExpectedWindowOverlap ? "low_expected_window_overlap" : "none";
  if (kind === "none") return undefined;
  return {
    kind,
    message:
      kind === "all_boundaries_failed"
        ? "No curated EPUB node could be aligned. Check for a wrong EPUB/audio pairing before tuning the chapter algorithm."
        : "Failed EPUB nodes have weak opener overlap near their expected transcript windows. Check for translated audio, wrong edition, or wrong EPUB/audio pairing.",
    failedExpectedWindowOverlap,
    firstCuratedNodeOpeningOverlap,
  };
}

async function summarizeMode(caseDir: string, mode: "recursive" | "node"): Promise<ModeSummary | null> {
  const resultPath = latestResult(caseDir, mode);
  const result = resultPath ? readJson(resultPath) : null;
  const eventLogPath = eventLogForResult(caseDir, mode, resultPath);
  if (!result && !eventLogPath) return null;
  const events = summarizeEvents(eventLogPath);
  const accepted = typeof result?.result?.accepted === "boolean" ? result.result.accepted : result?.result === null ? false : null;
  const chapters = Array.isArray(result?.result?.chapters) ? result.result.chapters.length : null;
  const summary: ModeSummary = {
    mode,
    resultPath,
    eventLogPath,
    accepted,
    chapters,
    elapsedMs: typeof result?.elapsedMs === "number" ? result.elapsedMs : null,
    wallSeconds: events.wallSeconds,
    requests: events.usage.requests,
    tokens: events.usage.tokens,
    costUsd: events.usage.costUsd,
    tools: events.tools,
    accuracy: "not_scored",
  };

  if (mode === "node" && result) {
    summary.nodeBoundarySources = events.nodeBoundaries;
    const reports = Array.isArray(result.nodeBoundaryReports) ? (result.nodeBoundaryReports as NodeBoundaryCurationReport[]) : [];
    summary.nodeReports = {
      total: reports.length,
      accepted: reports.filter((report) => report.outcome === "accepted").length,
      failed: reports.filter((report) => report.outcome === "failed").length,
      dropped: reports.filter((report) => report.outcome === "dropped").length,
    };
    summary.nodeReplay = await replayNodeReports(caseDir, result);
    if (result.nodeBoundaryFailureDiagnostic && typeof result.nodeBoundaryFailureDiagnostic === "object") {
      const diagnostic = result.nodeBoundaryFailureDiagnostic as JsonRecord;
      summary.nodeFailureDiagnostic = {
        kind: typeof diagnostic.kind === "string" ? diagnostic.kind : "unknown",
        message: typeof diagnostic.message === "string" ? diagnostic.message : "",
        failedExpectedWindowOverlap: typeof diagnostic.failedExpectedWindowOverlap === "number" ? diagnostic.failedExpectedWindowOverlap : null,
        firstCuratedNodeOpeningOverlap: typeof diagnostic.firstCuratedNodeOpeningOverlap === "number" ? diagnostic.firstCuratedNodeOpeningOverlap : null,
      };
    } else if (!(summary.nodeReplay?.acceptedAfterReplay ?? summary.accepted)) {
      summary.nodeFailureDiagnostic = await buildNodeFailureDiagnostic(caseDir, reports);
    }
  }
  return summary;
}

function nullableDelta(a: number | null | undefined, b: number | null | undefined): number | null {
  return typeof a === "number" && typeof b === "number" ? round(a - b, 6) : null;
}

function boolDelta(a: boolean | null | undefined, b: boolean | null | undefined): number | null {
  return typeof a === "boolean" && typeof b === "boolean" ? Number(a) - Number(b) : null;
}

async function summarizeCase(caseDir: string): Promise<CaseSummary> {
  const metadata = readJson(path.join(caseDir, "metadata.json"));
  const runnable = existsSync(path.join(caseDir, "book.epub")) && existsSync(path.join(caseDir, "transcript.json"));
  const recursive = await summarizeMode(caseDir, "recursive");
  const node = await summarizeMode(caseDir, "node");
  return {
    slug: path.basename(caseDir),
    title: typeof metadata?.title === "string" ? metadata.title : null,
    author: typeof metadata?.author === "string" ? metadata.author : null,
    runnable,
    recursive,
    node,
    comparison: {
      nodeHasRun: Boolean(node),
      recursiveHasRun: Boolean(recursive),
      nodeAcceptedMinusRecursive: boolDelta(node?.nodeReplay?.acceptedAfterReplay ?? node?.accepted, recursive?.accepted),
      nodeChaptersMinusRecursive: nullableDelta(node?.nodeReplay?.chapters ?? node?.chapters, recursive?.chapters),
      nodeWallSecondsMinusRecursive: nullableDelta(node?.wallSeconds ?? (node?.elapsedMs ? node.elapsedMs / 1000 : null), recursive?.wallSeconds ?? (recursive?.elapsedMs ? recursive.elapsedMs / 1000 : null)),
      nodeRequestsMinusRecursive: nullableDelta(node?.requests, recursive?.requests),
      nodeTokensMinusRecursive: nullableDelta(node?.tokens, recursive?.tokens),
      nodeCostUsdMinusRecursive: nullableDelta(node?.costUsd, recursive?.costUsd),
    },
  };
}

function fmt(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : String(round(value, 3));
  return String(value);
}

function modeStatus(summary: ModeSummary | null): string {
  if (!summary) return "missing";
  const accepted = summary.nodeReplay?.acceptedAfterReplay ?? summary.accepted;
  const chapters = summary.nodeReplay?.chapters ?? summary.chapters;
  return `${accepted ? "accepted" : "failed"} / ${fmt(chapters)} ch / ${fmt(summary.wallSeconds ?? (summary.elapsedMs ? summary.elapsedMs / 1000 : null))}s`;
}

function nodeReportStatus(summary: ModeSummary | null): string {
  const reports = summary?.nodeReports;
  if (!reports) return "-";
  return `${reports.accepted}/${reports.total} accepted, ${reports.failed} failed, ${reports.dropped} dropped`;
}

function nodeSourceStatus(summary: ModeSummary | null): string {
  const sources = summary?.nodeBoundarySources;
  if (!sources) return "-";
  return `${sources.deterministicAccepted} det, ${sources.agentAccepted} agent, ${sources.failed} failed`;
}

function nodeDiagnosticStatus(summary: ModeSummary | null): string {
  const diagnostic = summary?.nodeFailureDiagnostic;
  if (!diagnostic || diagnostic.kind === "none") return "-";
  const overlap = diagnostic.failedExpectedWindowOverlap === null ? "?" : `${Math.round(diagnostic.failedExpectedWindowOverlap * 100)}%`;
  const first = diagnostic.firstCuratedNodeOpeningOverlap === null ? "?" : `${Math.round(diagnostic.firstCuratedNodeOpeningOverlap * 100)}%`;
  return `${diagnostic.kind} (failed window overlap ${overlap}, first opener ${first})`;
}

function renderMarkdown(cases: CaseSummary[], generatedAt: string): string {
  const runnable = cases.filter((item) => item.runnable).length;
  const paired = cases.filter((item) => item.recursive && item.node).length;
  const nodeAccepted = cases.filter((item) => item.node && (item.node.nodeReplay?.acceptedAfterReplay ?? item.node.accepted)).length;
  const recursiveAccepted = cases.filter((item) => item.recursive?.accepted).length;
  const lines = [
    "# Chapter Curation Mode Comparison",
    "",
    `Generated: ${generatedAt}`,
    "",
    "Accuracy is not scored here because no committed answer keys are present. This report compares repeatable operational signals only: acceptance, monotonic validity, chapter count, node failures/drops, latency, requests, tokens, and cost.",
    "",
    `Cases: ${cases.length} total, ${runnable} runnable with local EPUB+transcript, ${paired} have both recursive and node artifacts.`,
    `Accepted artifacts: recursive ${recursiveAccepted}/${cases.filter((item) => item.recursive).length}, node ${nodeAccepted}/${cases.filter((item) => item.node).length} after current-code node replay.`,
    "",
    "| Case | Recursive | Node | Node reports | Node source | Node replay | Node diagnostic | Δ seconds | Δ requests | Δ tokens | Δ cost |",
    "| --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
  ];
  for (const item of cases) {
    const replay = item.node?.nodeReplay
    ? `${item.node.nodeReplay.acceptedAfterReplay ? "valid" : "invalid"} / ${item.node.nodeReplay.chapters} ch / ${item.node.nodeReplay.failedReports} failed / ${item.node.nodeReplay.droppedReports} dropped / ${Math.round(item.node.nodeReplay.coverage * 100)}% coverage`
      : "-";
    lines.push(
      `| ${item.slug} | ${modeStatus(item.recursive)} | ${modeStatus(item.node)} | ${nodeReportStatus(item.node)} | ${nodeSourceStatus(item.node)} | ${replay} | ${nodeDiagnosticStatus(item.node)} | ${fmt(item.comparison.nodeWallSecondsMinusRecursive)} | ${fmt(item.comparison.nodeRequestsMinusRecursive)} | ${fmt(item.comparison.nodeTokensMinusRecursive)} | ${fmt(item.comparison.nodeCostUsdMinusRecursive)} |`
    );
  }
  lines.push("");
  lines.push("## Interpretation");
  lines.push("");
  lines.push("- `Node replay` re-merges saved node boundary reports through the current merge implementation, so it can validate fixes without spending more API calls.");
  lines.push("- `Node reports` is the original run output before current-code replay; failed/dropped rows are unresolved evidence gaps even when the merged plan is structurally valid.");
  lines.push("- `Node source` counts boundary decisions accepted by deterministic pre-agent research versus accepted by the node agent fallback.");
  lines.push("- `Node diagnostic` is emitted only by failed node runs and is a hint to inspect corpus/data quality; it does not accept chapters or relax validation.");
  lines.push("- Positive Δ seconds/requests/tokens/cost means node-parallel used more than recursive for that saved run; negative means it used less.");
  lines.push("- This is not sufficient to prove chapter quality. Add committed answer keys or a judge-scored audit before making accuracy claims.");
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main(): Promise<void> {
  const corpusRoot = path.resolve(process.argv[2] ?? "tmp/chapter-cases/corpus");
  if (!existsSync(corpusRoot)) usage();
  const caseDirs = readdirSync(corpusRoot)
    .map((name) => path.join(corpusRoot, name))
    .filter((item) => existsSync(path.join(item, "metadata.json")))
    .sort((a, b) => path.basename(a).localeCompare(path.basename(b)));
  const generatedAt = new Date().toISOString();
  const cases = await Promise.all(caseDirs.map((caseDir) => summarizeCase(caseDir)));
  const output = {
    generatedAt,
    corpusRoot,
    cases,
  };
  const runId = generatedAt.replace(/[:.]/g, "-");
  const jsonPath = path.join(corpusRoot, `curation-comparison-${runId}.json`);
  const markdownPath = path.join(corpusRoot, `curation-comparison-${runId}.md`);
  await mkdir(corpusRoot, { recursive: true });
  await writeFile(jsonPath, `${JSON.stringify(output, null, 2)}\n`, "utf8");
  await writeFile(markdownPath, renderMarkdown(cases, generatedAt), "utf8");
  console.log(JSON.stringify({ ok: true, cases: cases.length, jsonPath, markdownPath }, null, 2));
}

await main();
