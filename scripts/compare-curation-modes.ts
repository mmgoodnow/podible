#!/usr/bin/env bun
import { execFileSync } from "node:child_process";
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
  nodeBoundaryJudgments: {
    accepted: number;
    rejected: number;
    acceptedNodeIds: string[];
  };
};

type ModeSummary = {
  mode: "recursive" | "node";
  resultPath: string | null;
  eventLogPath: string | null;
  artifactSelection: {
    currentCommit: string | null;
    artifactCommit: string | null;
    artifactDirty: boolean | null;
    currentCleanArtifact: boolean;
  };
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
    skipped: number;
  };
  nodeJudgedAudit?: {
    acceptedReports: number;
    judgeAccepted: number;
    judgeRejected: number;
    missingJudgeAcceptance: number;
    coverage: number;
    acceptedNodeIdsWithoutJudge: string[];
  };
  nodeReplay?: {
    chapters: number;
    replayedChapters?: SubmittedChapter[];
    acceptedReports: number;
    failedReports: number;
    droppedReports: number;
    skippedReports: number;
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

type AnswerKeyChapter = {
  title: string;
  startTime: number | null;
};

type AnswerKeySummary = {
  path: string;
  expectedCount: number | null;
  chapters: AnswerKeyChapter[];
};

type AnswerKeyScore = {
  keyPath: string;
  toleranceSeconds: number;
  expectedCount: number | null;
  expectedDetailedCount: number;
  actualCount: number | null;
  countMatches: boolean | null;
  detailedMatches: number | null;
  titleMismatches: number | null;
  timeMismatches: number | null;
  missingChapters: number | null;
  extraChapters: number | null;
  maxTimeDeltaSeconds: number | null;
  meanTimeDeltaSeconds: number | null;
  passed: boolean | null;
};

type CaseSummary = {
  slug: string;
  title: string | null;
  author: string | null;
  runnable: boolean;
  answerKey: AnswerKeySummary | null;
  answerKeyScore: AnswerKeyScore | null;
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

type AggregateSummary = {
  cases: {
    total: number;
    runnable: number;
    paired: number;
  };
  artifacts: {
    recursive: number;
    node: number;
    currentCleanRecursive: number;
    currentCleanNode: number;
    staleOrUnversionedRecursive: string[];
    staleOrUnversionedNode: string[];
  };
  acceptance: {
    recursiveAccepted: number;
    nodeAcceptedAfterReplay: number;
    nodeReplayValid: number;
    nodeReplayInvalid: number;
  };
  nodeJudgedAudit: {
    casesWithNodeReports: number;
    casesFullyJudgeBacked: number;
    casesMissingJudgeAcceptance: number;
    acceptedReports: number;
    judgeBackedAcceptedReports: number;
    coverage: number | null;
  };
  answerKeys: {
    casesWithKeys: number;
    casesWithDetailedKeys: number;
    casesPassingKeys: number;
    casesFailingKeys: number;
    casesCountOnly: number;
  };
  pairedDeltas: {
    cases: number;
    nodeAcceptedMinusRecursive: number;
    nodeChaptersMinusRecursive: number;
    nodeWallSecondsSaved: number;
    nodeRequestsSaved: number;
    nodeTokensSaved: number;
    nodeCostUsdSaved: number;
    recursiveWallSeconds: number;
    nodeWallSeconds: number;
    wallSpeedup: number | null;
    recursiveRequests: number;
    nodeRequests: number;
    requestReductionRatio: number | null;
    recursiveTokens: number;
    nodeTokens: number;
    tokenReductionRatio: number | null;
    recursiveCostUsd: number;
    nodeCostUsd: number;
    costReductionRatio: number | null;
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

function gitString(args: string[]): string | null {
  try {
    return execFileSync("git", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim() || null;
  } catch {
    return null;
  }
}

const currentGitCommit = gitString(["rev-parse", "HEAD"]);

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
    nodeBoundaryJudgments: { accepted: 0, rejected: 0, acceptedNodeIds: [] },
  };
  const acceptedNodeJudgmentIds = new Set<string>();

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
    if (event.type === "fulcrum-judge-result" && event.proposalKind === "node_boundary") {
      if (event.judgment?.accepted === true) {
        summary.nodeBoundaryJudgments.accepted += 1;
        if (typeof event.split?.epubNodeId === "string") acceptedNodeJudgmentIds.add(event.split.epubNodeId);
      } else if (event.judgment?.accepted === false) {
        summary.nodeBoundaryJudgments.rejected += 1;
      }
    }
  }

  summary.usage.costUsd = roundMoney(summary.usage.costUsd);
  for (const roleSummary of Object.values(summary.usage.byRole)) roleSummary.costUsd = roundMoney(roleSummary.costUsd);
  summary.nodeBoundaryJudgments.acceptedNodeIds = Array.from(acceptedNodeJudgmentIds).sort();
  return summary;
}

function matchingFiles(caseDir: string, pattern: RegExp): string[] {
  return readdirSync(caseDir)
    .filter((name) => pattern.test(name))
    .map((name) => path.join(caseDir, name))
    .sort((a, b) => a.localeCompare(b));
}

function latestMatchingFile(caseDir: string, pattern: RegExp): string | null {
  const matches = matchingFiles(caseDir, pattern);
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
  const matches =
    mode === "node"
      ? matchingFiles(caseDir, /^node-agent-result-.*\.json$/)
      : matchingFiles(caseDir, /^(?:recursive-)?agent-result-.*\.json$/);
  if (matches.length === 0) return null;
  if (currentGitCommit) {
    const currentClean = matches.filter((filePath) => {
      const result = readJson(filePath);
      return result?.git?.commit === currentGitCommit && result.git.dirty === false;
    });
    if (currentClean.length > 0) return currentClean.at(-1)!;
  }
  return matches.at(-1)!;
}

function normalizeChapterTitle(value: string): string {
  return value
    .toLowerCase()
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAnswerKeyJson(filePath: string): AnswerKeySummary | null {
  const data = readJson(filePath);
  if (!data) return null;
  const rawChapters = Array.isArray(data.expectedChapters)
    ? data.expectedChapters
    : Array.isArray(data.chapters)
      ? data.chapters
      : [];
  const chapters: AnswerKeyChapter[] = rawChapters.flatMap((chapter: JsonRecord) => {
    const title = typeof chapter.title === "string" ? chapter.title : typeof chapter.name === "string" ? chapter.name : null;
    if (!title) return [];
    const startTime =
      typeof chapter.startTime === "number"
        ? chapter.startTime
        : typeof chapter.start === "number"
          ? chapter.start
          : typeof chapter.startSeconds === "number"
            ? chapter.startSeconds
            : null;
    return [{ title, startTime }];
  });
  const expectedCount =
    typeof data.expectedChapterCount === "number"
      ? data.expectedChapterCount
      : typeof data.expectedCount === "number"
        ? data.expectedCount
        : chapters.length > 0
          ? chapters.length
          : null;
  return { path: filePath, expectedCount, chapters };
}

function parseAnswerKeyMarkdown(filePath: string): AnswerKeySummary | null {
  const text = readFileSync(filePath, "utf8");
  const expectedCountMatch = text.match(/Expected chapter count:\s*([0-9]+)/i);
  const chapters: AnswerKeyChapter[] = [];
  for (const line of text.split(/\r?\n/)) {
    const match = line.match(/^\s*-\s+`([^`]+)`\s+@\s+`?([0-9]+(?:\.[0-9]+)?)`?/);
    if (!match) continue;
    chapters.push({ title: match[1]!, startTime: Number(match[2]) });
  }
  const expectedCount = expectedCountMatch ? Number(expectedCountMatch[1]) : chapters.length > 0 ? chapters.length : null;
  return expectedCount === null && chapters.length === 0 ? null : { path: filePath, expectedCount, chapters };
}

function loadAnswerKey(caseDir: string): AnswerKeySummary | null {
  const candidates = ["expected-chapters.json", "answer-key.json", "answer-key.md"].map((name) => path.join(caseDir, name));
  for (const candidate of candidates) {
    if (!existsSync(candidate)) continue;
    const parsed = candidate.endsWith(".md") ? parseAnswerKeyMarkdown(candidate) : parseAnswerKeyJson(candidate);
    if (parsed) return parsed;
  }
  return null;
}

function chaptersFromResult(result: JsonRecord | null, replay: ModeSummary["nodeReplay"]): SubmittedChapter[] | null {
  if (Array.isArray(replay?.replayedChapters)) {
    return replay.replayedChapters;
  }
  if (Array.isArray(result?.result?.chapters)) {
    return result.result.chapters.flatMap((chapter: JsonRecord) => {
      if (typeof chapter.title !== "string" || typeof chapter.startTime !== "number") return [];
      return [{ title: chapter.title, startTime: chapter.startTime, epubNodeId: typeof chapter.epubNodeId === "string" ? chapter.epubNodeId : undefined }];
    });
  }
  if (replay?.chapters === 0) return [];
  return null;
}

function scoreAnswerKey(answerKey: AnswerKeySummary | null, actualChapters: SubmittedChapter[] | null): AnswerKeyScore | null {
  if (!answerKey) return null;
  const toleranceSeconds = 10;
  const actualCount = actualChapters?.length ?? null;
  const countMatches = answerKey.expectedCount === null || actualCount === null ? null : answerKey.expectedCount === actualCount;
  if (answerKey.chapters.length === 0 || !actualChapters) {
    return {
      keyPath: answerKey.path,
      toleranceSeconds,
      expectedCount: answerKey.expectedCount,
      expectedDetailedCount: answerKey.chapters.length,
      actualCount,
      countMatches,
      detailedMatches: null,
      titleMismatches: null,
      timeMismatches: null,
      missingChapters: null,
      extraChapters: null,
      maxTimeDeltaSeconds: null,
      meanTimeDeltaSeconds: null,
      passed: countMatches,
    };
  }

  const compareLength = Math.min(answerKey.chapters.length, actualChapters.length);
  let detailedMatches = 0;
  let titleMismatches = 0;
  let timeMismatches = 0;
  const deltas: number[] = [];
  for (let index = 0; index < compareLength; index++) {
    const expected = answerKey.chapters[index]!;
    const actual = actualChapters[index]!;
    const titleMatches = normalizeChapterTitle(expected.title) === normalizeChapterTitle(actual.title);
    const delta = expected.startTime === null ? null : Math.abs(actual.startTime - expected.startTime);
    const timeMatches = delta === null || delta <= toleranceSeconds;
    if (delta !== null) deltas.push(delta);
    if (titleMatches && timeMatches) detailedMatches++;
    if (!titleMatches) titleMismatches++;
    if (!timeMatches) timeMismatches++;
  }
  const missingChapters = Math.max(0, answerKey.chapters.length - actualChapters.length);
  const extraChapters = Math.max(0, actualChapters.length - answerKey.chapters.length);
  const passed = Boolean(countMatches && detailedMatches === answerKey.chapters.length && missingChapters === 0 && extraChapters === 0);
  return {
    keyPath: answerKey.path,
    toleranceSeconds,
    expectedCount: answerKey.expectedCount,
    expectedDetailedCount: answerKey.chapters.length,
    actualCount,
    countMatches,
    detailedMatches,
    titleMismatches,
    timeMismatches,
    missingChapters,
    extraChapters,
    maxTimeDeltaSeconds: deltas.length > 0 ? round(Math.max(...deltas), 3) : null,
    meanTimeDeltaSeconds: deltas.length > 0 ? round(deltas.reduce((sum, delta) => sum + delta, 0) / deltas.length, 3) : null,
    passed,
  };
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
  const metadata = readJson(path.join(caseDir, "metadata.json"));
  const transcript = readJson(path.join(caseDir, "transcript.json"));
  const acceptedById = new Map(
    originalReports
      .filter((report) => (report.outcome === "accepted" || report.outcome === "dropped") && typeof report.startTime === "number")
      .map((report) => [report.epubNodeId, report])
  );
  const reportIds = new Set(originalReports.map((report) => report.epubNodeId));
  const epubEntries = (await loadEpubEntries(path.join(caseDir, "book.epub"))).filter((entry) => reportIds.has(entry.id));
  const replayReports: NodeBoundaryCurationReport[] = [];
  const durationMs = Number(result.durationMs ?? 0) || Number(metadata?.audio_duration_ms ?? 0);
  const chapters = await resolveNodeBoundaryChapters(
    {
      durationMs,
      epubEntries,
      transcript:
        transcript && typeof transcript === "object"
          ? (transcript as any)
          : {
              text: "",
              words: [],
              utterances: [],
              chunks: [],
            },
    },
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
  const skippedReports = replayReports.filter((report) => report.outcome === "skipped").length;
  const coverage = replayReports.length === 0 ? 0 : (acceptedReports + droppedReports + skippedReports) / replayReports.length;
  return {
    chapters: replayed.length,
    replayedChapters: replayed,
    acceptedReports,
    failedReports,
    droppedReports,
    skippedReports,
    coverage: round(coverage, 3),
    monotonicErrors: monotonicErrors(replayed),
    acceptedAfterReplay: replayed.length > 0 && monotonicErrors(replayed) === 0 && coverage >= 0.9,
  };
}

async function buildNodeFailureDiagnostic(caseDir: string, reports: NodeBoundaryCurationReport[]): Promise<ModeSummary["nodeFailureDiagnostic"]> {
  const failedReports = reports.filter((report) => report.outcome === "failed");
  const resolvedReports = reports.filter((report) => report.outcome === "accepted" || report.outcome === "dropped" || report.outcome === "skipped");
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
  const allBoundariesFailed = reports.length > 0 && resolvedReports.length === 0;
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
    artifactSelection: {
      currentCommit: currentGitCommit,
      artifactCommit: typeof result?.git?.commit === "string" ? result.git.commit : null,
      artifactDirty: typeof result?.git?.dirty === "boolean" ? result.git.dirty : null,
      currentCleanArtifact: Boolean(currentGitCommit && result?.git?.commit === currentGitCommit && result.git.dirty === false),
    },
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
      skipped: reports.filter((report) => report.outcome === "skipped").length,
    };
    const acceptedReportIds = reports.filter((report) => report.outcome === "accepted").map((report) => report.epubNodeId);
    const judgeAcceptedIds = new Set(events.nodeBoundaryJudgments.acceptedNodeIds);
    const acceptedNodeIdsWithoutJudge = acceptedReportIds.filter((id) => !judgeAcceptedIds.has(id));
    summary.nodeJudgedAudit = {
      acceptedReports: acceptedReportIds.length,
      judgeAccepted: acceptedReportIds.length - acceptedNodeIdsWithoutJudge.length,
      judgeRejected: events.nodeBoundaryJudgments.rejected,
      missingJudgeAcceptance: acceptedNodeIdsWithoutJudge.length,
      coverage: acceptedReportIds.length === 0 ? 1 : round((acceptedReportIds.length - acceptedNodeIdsWithoutJudge.length) / acceptedReportIds.length, 3),
      acceptedNodeIdsWithoutJudge,
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
  const answerKey = loadAnswerKey(caseDir);
  const nodeResult = node?.resultPath ? readJson(node.resultPath) : null;
  const answerKeyScore = scoreAnswerKey(answerKey, chaptersFromResult(nodeResult, node?.nodeReplay));
  return {
    slug: path.basename(caseDir),
    title: typeof metadata?.title === "string" ? metadata.title : null,
    author: typeof metadata?.author === "string" ? metadata.author : null,
    runnable,
    answerKey,
    answerKeyScore,
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

function modeSeconds(summary: ModeSummary | null): number | null {
  if (!summary) return null;
  return summary.wallSeconds ?? (typeof summary.elapsedMs === "number" ? summary.elapsedMs / 1000 : null);
}

function ratio(numerator: number, denominator: number): number | null {
  return denominator > 0 ? round(numerator / denominator, 3) : null;
}

function aggregateCases(cases: CaseSummary[]): AggregateSummary {
  const pairedCases = cases.filter((item) => item.node && item.recursive);
  const comparablePairs = pairedCases.filter((item) => modeSeconds(item.node) !== null && modeSeconds(item.recursive) !== null);
  const recursiveWallSeconds = comparablePairs.reduce((sum, item) => sum + (modeSeconds(item.recursive) ?? 0), 0);
  const nodeWallSeconds = comparablePairs.reduce((sum, item) => sum + (modeSeconds(item.node) ?? 0), 0);
  const recursiveRequests = comparablePairs.reduce((sum, item) => sum + (item.recursive?.requests ?? 0), 0);
  const nodeRequests = comparablePairs.reduce((sum, item) => sum + (item.node?.requests ?? 0), 0);
  const recursiveTokens = comparablePairs.reduce((sum, item) => sum + (item.recursive?.tokens ?? 0), 0);
  const nodeTokens = comparablePairs.reduce((sum, item) => sum + (item.node?.tokens ?? 0), 0);
  const recursiveCostUsd = comparablePairs.reduce((sum, item) => sum + (item.recursive?.costUsd ?? 0), 0);
  const nodeCostUsd = comparablePairs.reduce((sum, item) => sum + (item.node?.costUsd ?? 0), 0);
  const nodeAuditCases = cases.filter((item) => item.node?.nodeJudgedAudit);
  const acceptedReports = nodeAuditCases.reduce((sum, item) => sum + (item.node?.nodeJudgedAudit?.acceptedReports ?? 0), 0);
  const judgeBackedAcceptedReports = nodeAuditCases.reduce((sum, item) => sum + (item.node?.nodeJudgedAudit?.judgeAccepted ?? 0), 0);
  const answerKeyCases = cases.filter((item) => item.answerKeyScore);
  const detailedAnswerKeyCases = answerKeyCases.filter((item) => (item.answerKeyScore?.expectedDetailedCount ?? 0) > 0);
  return {
    cases: {
      total: cases.length,
      runnable: cases.filter((item) => item.runnable).length,
      paired: pairedCases.length,
    },
    artifacts: {
      recursive: cases.filter((item) => item.recursive).length,
      node: cases.filter((item) => item.node).length,
      currentCleanRecursive: cases.filter((item) => item.recursive?.artifactSelection.currentCleanArtifact).length,
      currentCleanNode: cases.filter((item) => item.node?.artifactSelection.currentCleanArtifact).length,
      staleOrUnversionedRecursive: cases.filter((item) => item.recursive && !item.recursive.artifactSelection.currentCleanArtifact).map((item) => item.slug),
      staleOrUnversionedNode: cases.filter((item) => item.node && !item.node.artifactSelection.currentCleanArtifact).map((item) => item.slug),
    },
    acceptance: {
      recursiveAccepted: cases.filter((item) => item.recursive?.accepted).length,
      nodeAcceptedAfterReplay: cases.filter((item) => item.node && (item.node.nodeReplay?.acceptedAfterReplay ?? item.node.accepted)).length,
      nodeReplayValid: cases.filter((item) => item.node?.nodeReplay?.acceptedAfterReplay).length,
      nodeReplayInvalid: cases.filter((item) => item.node?.nodeReplay && !item.node.nodeReplay.acceptedAfterReplay).length,
    },
    nodeJudgedAudit: {
      casesWithNodeReports: nodeAuditCases.length,
      casesFullyJudgeBacked: nodeAuditCases.filter((item) => (item.node?.nodeJudgedAudit?.missingJudgeAcceptance ?? 0) === 0).length,
      casesMissingJudgeAcceptance: nodeAuditCases.filter((item) => (item.node?.nodeJudgedAudit?.missingJudgeAcceptance ?? 0) > 0).length,
      acceptedReports,
      judgeBackedAcceptedReports,
      coverage: acceptedReports === 0 ? null : round(judgeBackedAcceptedReports / acceptedReports, 3),
    },
    answerKeys: {
      casesWithKeys: answerKeyCases.length,
      casesWithDetailedKeys: detailedAnswerKeyCases.length,
      casesPassingKeys: answerKeyCases.filter((item) => item.answerKeyScore?.passed === true).length,
      casesFailingKeys: answerKeyCases.filter((item) => item.answerKeyScore?.passed === false).length,
      casesCountOnly: answerKeyCases.filter((item) => (item.answerKeyScore?.expectedDetailedCount ?? 0) === 0).length,
    },
    pairedDeltas: {
      cases: comparablePairs.length,
      nodeAcceptedMinusRecursive: comparablePairs.reduce((sum, item) => sum + (item.comparison.nodeAcceptedMinusRecursive ?? 0), 0),
      nodeChaptersMinusRecursive: comparablePairs.reduce((sum, item) => sum + (item.comparison.nodeChaptersMinusRecursive ?? 0), 0),
      nodeWallSecondsSaved: round(recursiveWallSeconds - nodeWallSeconds, 3),
      nodeRequestsSaved: recursiveRequests - nodeRequests,
      nodeTokensSaved: recursiveTokens - nodeTokens,
      nodeCostUsdSaved: roundMoney(recursiveCostUsd - nodeCostUsd),
      recursiveWallSeconds: round(recursiveWallSeconds, 3),
      nodeWallSeconds: round(nodeWallSeconds, 3),
      wallSpeedup: ratio(recursiveWallSeconds, nodeWallSeconds),
      recursiveRequests,
      nodeRequests,
      requestReductionRatio: ratio(recursiveRequests - nodeRequests, recursiveRequests),
      recursiveTokens,
      nodeTokens,
      tokenReductionRatio: ratio(recursiveTokens - nodeTokens, recursiveTokens),
      recursiveCostUsd: roundMoney(recursiveCostUsd),
      nodeCostUsd: roundMoney(nodeCostUsd),
      costReductionRatio: ratio(recursiveCostUsd - nodeCostUsd, recursiveCostUsd),
    },
  };
}

function modeStatus(summary: ModeSummary | null): string {
  if (!summary) return "missing";
  const accepted = summary.nodeReplay?.acceptedAfterReplay ?? summary.accepted;
  const chapters = summary.nodeReplay?.chapters ?? summary.chapters;
  const provenance = summary.artifactSelection.currentCleanArtifact
    ? ""
    : summary.artifactSelection.artifactCommit
      ? " / stale-or-dirty"
      : " / unversioned";
  return `${accepted ? "accepted" : "failed"} / ${fmt(chapters)} ch / ${fmt(summary.wallSeconds ?? (summary.elapsedMs ? summary.elapsedMs / 1000 : null))}s${provenance}`;
}

function nodeReportStatus(summary: ModeSummary | null): string {
  const reports = summary?.nodeReports;
  if (!reports) return "-";
  return `${reports.accepted}/${reports.total} accepted, ${reports.failed} failed, ${reports.dropped} dropped, ${reports.skipped} skipped`;
}

function nodeJudgedAuditStatus(summary: ModeSummary | null): string {
  const audit = summary?.nodeJudgedAudit;
  if (!audit) return "-";
  const missing = audit.acceptedNodeIdsWithoutJudge.length > 0 ? `, missing ${audit.acceptedNodeIdsWithoutJudge.join(" ")}` : "";
  return `${audit.judgeAccepted}/${audit.acceptedReports} judge-backed, ${audit.judgeRejected} rejected, ${Math.round(audit.coverage * 100)}%${missing}`;
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

function answerKeyStatus(score: AnswerKeyScore | null): string {
  if (!score) return "-";
  const count = score.countMatches === null ? "count ?" : score.countMatches ? `count ${score.actualCount}/${score.expectedCount}` : `count ${score.actualCount} != ${score.expectedCount}`;
  if (score.expectedDetailedCount === 0) {
    return `${score.passed ? "pass" : "fail"} / ${count} / count-only`;
  }
  if (score.detailedMatches === null) {
    return `${score.passed ? "pass" : "fail"} / ${count} / detailed key unavailable for selected artifact`;
  }
  const detail = `${score.detailedMatches}/${score.expectedDetailedCount} exact`;
  const time =
    score.maxTimeDeltaSeconds === null
      ? "time ?"
      : `max Δ ${fmt(score.maxTimeDeltaSeconds)}s, mean Δ ${fmt(score.meanTimeDeltaSeconds)}s, tol ${fmt(score.toleranceSeconds)}s`;
  const misses = [score.titleMismatches ? `${score.titleMismatches} title` : null, score.timeMismatches ? `${score.timeMismatches} time` : null, score.missingChapters ? `${score.missingChapters} missing` : null, score.extraChapters ? `${score.extraChapters} extra` : null]
    .filter(Boolean)
    .join(", ");
  return `${score.passed ? "pass" : "fail"} / ${count} / ${detail} / ${time}${misses ? ` / ${misses}` : ""}`;
}

function renderMarkdown(cases: CaseSummary[], aggregate: AggregateSummary, generatedAt: string): string {
  const runnable = cases.filter((item) => item.runnable).length;
  const paired = cases.filter((item) => item.recursive && item.node).length;
  const nodeAccepted = cases.filter((item) => item.node && (item.node.nodeReplay?.acceptedAfterReplay ?? item.node.accepted)).length;
  const recursiveAccepted = cases.filter((item) => item.recursive?.accepted).length;
  const staleNodeList = aggregate.artifacts.staleOrUnversionedNode.length === 0 ? "none" : aggregate.artifacts.staleOrUnversionedNode.join(", ");
  const staleRecursiveList = aggregate.artifacts.staleOrUnversionedRecursive.length === 0 ? "none" : aggregate.artifacts.staleOrUnversionedRecursive.join(", ");
  const lines = [
    "# Chapter Curation Mode Comparison",
    "",
    `Generated: ${generatedAt}`,
    "",
    "This report compares node-parallel curation against recursive divide-and-conquer using operational signals, judge-backed boundary evidence, and any committed answer keys available for a case.",
    "",
    `Cases: ${cases.length} total, ${runnable} runnable with local EPUB+transcript, ${paired} have both recursive and node artifacts.`,
    `Accepted artifacts: recursive ${recursiveAccepted}/${cases.filter((item) => item.recursive).length}, node ${nodeAccepted}/${cases.filter((item) => item.node).length} after current-code node replay.`,
    "",
    "## Aggregate",
    "",
    `- Current-clean node artifacts: ${aggregate.artifacts.currentCleanNode}/${aggregate.artifacts.node}. Stale or unversioned node artifacts: ${staleNodeList}.`,
    `- Current-clean recursive artifacts: ${aggregate.artifacts.currentCleanRecursive}/${aggregate.artifacts.recursive}. Stale or unversioned recursive artifacts: ${staleRecursiveList}.`,
    `- Node replay validity: ${aggregate.acceptance.nodeReplayValid} valid, ${aggregate.acceptance.nodeReplayInvalid} invalid.`,
    `- Node judged boundary coverage: ${aggregate.nodeJudgedAudit.judgeBackedAcceptedReports}/${aggregate.nodeJudgedAudit.acceptedReports} accepted node reports judge-backed (${fmt(aggregate.nodeJudgedAudit.coverage === null ? null : aggregate.nodeJudgedAudit.coverage * 100)}%); ${aggregate.nodeJudgedAudit.casesFullyJudgeBacked}/${aggregate.nodeJudgedAudit.casesWithNodeReports} node cases fully judge-backed.`,
    `- Answer-key coverage: ${aggregate.answerKeys.casesWithKeys}/${cases.length} cases have answer keys (${aggregate.answerKeys.casesWithDetailedKeys} detailed, ${aggregate.answerKeys.casesCountOnly} count-only); node passes ${aggregate.answerKeys.casesPassingKeys}/${aggregate.answerKeys.casesWithKeys} scored keys and fails ${aggregate.answerKeys.casesFailingKeys}.`,
    `- Paired operational comparison (${aggregate.pairedDeltas.cases} cases): node saved ${fmt(aggregate.pairedDeltas.nodeWallSecondsSaved)} seconds, ${fmt(aggregate.pairedDeltas.nodeRequestsSaved)} requests, ${fmt(aggregate.pairedDeltas.nodeTokensSaved)} tokens, and $${fmt(aggregate.pairedDeltas.nodeCostUsdSaved)}.`,
    `- Paired speedup: ${fmt(aggregate.pairedDeltas.wallSpeedup)}x wall-clock (${fmt(aggregate.pairedDeltas.recursiveWallSeconds)}s recursive vs ${fmt(aggregate.pairedDeltas.nodeWallSeconds)}s node).`,
    `- Paired reductions: ${fmt(aggregate.pairedDeltas.requestReductionRatio === null ? null : aggregate.pairedDeltas.requestReductionRatio * 100)}% requests, ${fmt(aggregate.pairedDeltas.tokenReductionRatio === null ? null : aggregate.pairedDeltas.tokenReductionRatio * 100)}% tokens, ${fmt(aggregate.pairedDeltas.costReductionRatio === null ? null : aggregate.pairedDeltas.costReductionRatio * 100)}% cost.`,
    "",
    "| Case | Recursive | Node | Answer key | Node reports | Judge audit | Node source | Node replay | Node diagnostic | Δ seconds | Δ requests | Δ tokens | Δ cost |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
  ];
  for (const item of cases) {
    const replay = item.node?.nodeReplay
    ? `${item.node.nodeReplay.acceptedAfterReplay ? "valid" : "invalid"} / ${item.node.nodeReplay.chapters} ch / ${item.node.nodeReplay.failedReports} failed / ${item.node.nodeReplay.droppedReports} dropped / ${item.node.nodeReplay.skippedReports} skipped / ${Math.round(item.node.nodeReplay.coverage * 100)}% coverage`
      : "-";
    lines.push(
      `| ${item.slug} | ${modeStatus(item.recursive)} | ${modeStatus(item.node)} | ${answerKeyStatus(item.answerKeyScore)} | ${nodeReportStatus(item.node)} | ${nodeJudgedAuditStatus(item.node)} | ${nodeSourceStatus(item.node)} | ${replay} | ${nodeDiagnosticStatus(item.node)} | ${fmt(item.comparison.nodeWallSecondsMinusRecursive)} | ${fmt(item.comparison.nodeRequestsMinusRecursive)} | ${fmt(item.comparison.nodeTokensMinusRecursive)} | ${fmt(item.comparison.nodeCostUsdMinusRecursive)} |`
    );
  }
  lines.push("");
  lines.push("## Interpretation");
  lines.push("");
  lines.push("- `Node replay` re-merges saved node boundary reports through the current merge implementation, so it can validate fixes without spending more API calls.");
  lines.push("- `Judge audit` counts accepted node-boundary reports that have a matching node-boundary judge acceptance in the event log. It proves the report is judge-backed, not that a human answer key agrees.");
  lines.push("- `Node reports` is the original run output before current-code replay; failed/dropped rows are unresolved evidence gaps even when the merged plan is structurally valid.");
  lines.push("- `Node source` counts boundary decisions accepted by deterministic pre-agent research versus accepted by the node agent fallback.");
  lines.push("- `Answer key` compares the selected node artifact against committed case metadata. Detailed keys require exact normalized titles and timestamps within the reported tolerance; count-only keys only prove expected chapter count.");
  lines.push("- `Node diagnostic` is emitted only by failed node runs and is a hint to inspect corpus/data quality; it does not accept chapters or relax validation.");
  lines.push("- `stale-or-dirty` means the selected artifact was not produced cleanly from the current git commit; rerun that corpus case before treating it as current proof.");
  lines.push("- `unversioned` means the artifact predates git provenance tracking; rerun that corpus case before treating it as current proof.");
  lines.push("- Positive Δ seconds/requests/tokens/cost means node-parallel used more than recursive for that saved run; negative means it used less.");
  lines.push("- Judge-backed coverage is stronger than operational acceptance, but detailed answer-key pass/fail is the stronger correctness signal when a detailed key exists.");
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
  const aggregate = aggregateCases(cases);
  const output = {
    generatedAt,
    corpusRoot,
    aggregate,
    cases,
  };
  const runId = generatedAt.replace(/[:.]/g, "-");
  const jsonPath = path.join(corpusRoot, `curation-comparison-${runId}.json`);
  const markdownPath = path.join(corpusRoot, `curation-comparison-${runId}.md`);
  await mkdir(corpusRoot, { recursive: true });
  await writeFile(jsonPath, `${JSON.stringify(output, null, 2)}\n`, "utf8");
  await writeFile(markdownPath, renderMarkdown(cases, aggregate, generatedAt), "utf8");
  console.log(JSON.stringify({ ok: true, cases: cases.length, jsonPath, markdownPath }, null, 2));
}

await main();
