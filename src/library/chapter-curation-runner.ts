import { Agent, OpenAIProvider, Runner, tool } from "@openai/agents";
import { BatchTraceProcessor, setTraceProcessors } from "@openai/agents-core";
import { OpenAITracingExporter } from "@openai/agents-openai";

let tracingInitializedForKey: string | null = null;
function ensureTracingInitialized(apiKey: string): void {
  if (tracingInitializedForKey === apiKey) return;
  tracingInitializedForKey = apiKey;
  setTraceProcessors([new BatchTraceProcessor(new OpenAITracingExporter({ apiKey }))]);
}
import { z } from "zod";

import type { EpubChapterEntry } from "./chapter-analysis";
import {
  type ChapterCurationContext,
  type ChapterCurationTiming,
  type ChapterCurationSpan,
  type ChapterCurationTargetBoundary,
  type EpubBoundaryResearchHit,
  type ResearchEpubBoundaryResult,
  assessEmbeddedAudioChaptersForCuration,
  estimateTimestampFromEpubPosition,
  fuzzySearchTranscript,
  getEmbeddedAudioChapters,
  getEpubNodeText,
  getTranscriptWindow,
  getTranscriptWindowFromContext,
  inferEntryEndRatio,
  inferEntryStartRatio,
  msToSeconds,
  normalizeToolText,
  researchEpubBoundary,
  rgSearchTranscript,
  searchEpubText,
  secondsToMs,
  summarizeFirstWords,
  transcriptUtterances,
} from "./chapter-curation-tools";
import {
  type AudibleEpubNodeSelection,
  type ChapterBoundaryJudgeProposal,
  type NodeBoundaryCurationReport,
  type NodeBoundaryDecision,
  type ChapterCurationAgentTrace,
  type SubmitFulcrumJudgmentResult,
  type SubmitFulcrumSplitResult,
  type SubmitChapterPlanResult,
  type SubmitNodeBoundaryResult,
  type SubmittedChapter,
  applyAudibleEpubNodeSelection,
  applyEmbeddedAudioChapterNodeScope,
  applyTranscriptEndpointEpubNodeScope,
  audibleEpubNodeSelectionToolUseBehavior,
  createRootCurationSpan,
  fulcrumJudgeToolUseBehavior,
  nodeBoundaryToolUseBehavior,
  resolveNodeBoundaryChapters,
  isShortHeadingOnlyEntry,
  spanDurationSeconds,
  submitChapterPlan,
  submitFulcrumJudgmentSchema,
  audibleEpubNodeSelectionSchema,
  summarizeSubmittedChapterObjects,
  validateFulcrumSplit,
  validateNodeBoundary,
  parseNodeBoundaryOutput,
  parseFulcrumJudgmentOutput,
  parseAudibleEpubNodeSelectionOutput,
} from "./chapter-curation-validation";
import {
  logAgentUsageEvent,
  logChapterCurationEvent,
  logSpanToolCall,
  logSpanToolError,
  logSpanToolResult,
  writeChapterCurationTrace,
} from "./chapter-curation-debug";

export type ChapterCurationDetailedResult = {
  result: SubmitChapterPlanResult | null;
  finalOutput: unknown;
  newItems: unknown[];
  rawResponses: unknown[];
  nodeBoundaryReports?: NodeBoundaryCurationReport[];
  nodeBoundaryTraces?: ChapterCurationAgentTrace[];
  nodeBoundaryFailureDiagnostic?: NodeBoundaryFailureDiagnostic;
};

export type NodeBoundaryFailureDiagnostic = {
  kind: "preflight_mismatch" | "low_expected_window_overlap" | "all_boundaries_failed" | "none";
  message: string;
  totalReports: number;
  failedReports: number;
  acceptedReports: number;
  skippedReports: number;
  failedExpectedWindowOverlap: number | null;
  firstCuratedNodeOpeningOverlap: number | null;
  bestEarlyCuratedNodeOpeningOverlap?: number | null;
  earlyCuratedNodeOpeningOverlaps?: Array<{
    epubNodeId: string;
    title: string;
    overlap: number;
  }>;
  worstFailedNodes: Array<{
    epubNodeId: string;
    title: string;
    expectedStartTime: number;
    overlap: number;
  }>;
};

function chapterCurationModelSettings(
  ctx: ChapterCurationContext,
  base: {
    toolChoice: "required";
    parallelToolCalls: false;
  },
): {
  toolChoice: "required";
  parallelToolCalls: false;
  reasoning?: {
    summary?: "auto" | "concise" | "detailed";
    effort?: "none" | "minimal" | "low" | "medium" | "high" | "xhigh";
  };
} {
  if (!ctx.debugReasoningSummary && !ctx.debugReasoningEffort) return base;
  return {
    ...base,
    reasoning: {
      ...(ctx.debugReasoningSummary ? { summary: ctx.debugReasoningSummary } : {}),
      ...(ctx.debugReasoningEffort ? { effort: ctx.debugReasoningEffort } : {}),
    },
  };
}

function serializeAgentError(error: unknown): unknown {
  const err = error as Error & { state?: { toJSON?: () => unknown } };
  return {
    name: err?.name,
    message: err?.message ?? String(error),
    state: typeof err?.state?.toJSON === "function" ? err.state.toJSON() : null,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryableAgentError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /\b(429|rate limit|timeout|temporarily|aborted|network|ECONNRESET|ETIMEDOUT)\b/i.test(message);
}

function maxTurnsAgentError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /\bmax(?:imum)? turns\b/i.test(message);
}

class NodeBoundaryRejectedLimitError extends Error {
  constructor(
    readonly epubNodeId: string,
    readonly rejectedCount: number,
    readonly lastReason: string
  ) {
    super(`Node boundary ${epubNodeId} stopped after ${rejectedCount} rejected submissions. Last rejection: ${lastReason}`);
    this.name = "NodeBoundaryRejectedLimitError";
  }
}

function nodeBoundaryRejectedLimitError(error: unknown): boolean {
  return error instanceof NodeBoundaryRejectedLimitError;
}

function normalizedWordTokens(value: string): string[] {
  return value
    .toLowerCase()
    .match(/[a-z0-9]+(?:'[a-z0-9]+)?/g)
    ?.map((token) => token.replace(/[^a-z0-9]+/g, ""))
    .filter(Boolean) ?? [];
}

function textTokens(value: string): string[] {
  return normalizedWordTokens(value).filter((token) => token.length >= 4);
}

const boundaryDiagnosticStopTokens = new Set(
  [
    "about",
    "after",
    "again",
    "among",
    "before",
    "being",
    "between",
    "because",
    "cannot",
    "could",
    "does",
    "done",
    "from",
    "have",
    "into",
    "just",
    "like",
    "more",
    "most",
    "much",
    "only",
    "over",
    "should",
    "some",
    "than",
    "that",
    "their",
    "them",
    "then",
    "there",
    "they",
    "this",
    "through",
    "under",
    "very",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "will",
    "with",
    "within",
    "without",
    "would",
    "your",
  ]
);

function diagnosticTokens(value: string): string[] {
  return textTokens(value).filter((token) => !boundaryDiagnosticStopTokens.has(token));
}

function entryOpeningDiagnosticTokens(entry: EpubChapterEntry, wordCount = 160): string[] {
  return diagnosticTokens(entry.words.slice(0, wordCount).map((word) => word.text).join(" "));
}

function transcriptExpectedWindowTokens(ctx: ChapterCurationContext, expectedStartTime: number): Set<string> {
  const startMs = secondsToMs(expectedStartTime - 900);
  const endMs = secondsToMs(expectedStartTime + 900);
  return new Set(
    diagnosticTokens(
      transcriptUtterances(ctx)
        .filter((utterance) => utterance.endMs >= startMs && utterance.startMs <= endMs)
        .map((utterance) => utterance.text)
        .join(" ")
    )
  );
}

function overlapRatio(sampleTokens: string[], candidateTokens: Set<string>): number {
  if (sampleTokens.length === 0) return 0;
  const hits = sampleTokens.filter((token) => candidateTokens.has(token)).length;
  return Math.round((hits / sampleTokens.length) * 1_000) / 1_000;
}

export function buildNodeBoundaryPreflightDiagnostic(ctx: ChapterCurationContext): NodeBoundaryFailureDiagnostic {
  const earlyTranscriptTokens = new Set(
    diagnosticTokens(
      transcriptUtterances(ctx)
        .slice(0, 80)
        .map((utterance) => utterance.text)
        .join(" ")
    )
  );
  const earlyCuratedNodeOpeningOverlaps = ctx.epubEntries.slice(0, Math.min(3, ctx.epubEntries.length)).map((entry) => ({
    epubNodeId: entry.id,
    title: entry.title,
    overlap: overlapRatio(entryOpeningDiagnosticTokens(entry), earlyTranscriptTokens),
  }));
  const firstCuratedNodeOpeningOverlap = earlyCuratedNodeOpeningOverlaps[0]?.overlap ?? null;
  const bestEarlyCuratedNodeOpeningOverlap =
    earlyCuratedNodeOpeningOverlaps.length === 0 ? null : Math.max(...earlyCuratedNodeOpeningOverlaps.map((item) => item.overlap));
  const mismatch = ctx.epubEntries.length >= 3 && bestEarlyCuratedNodeOpeningOverlap !== null && bestEarlyCuratedNodeOpeningOverlap < 0.3;
  return {
    kind: mismatch ? "preflight_mismatch" : "none",
    message: mismatch
      ? "Early EPUB node openings have weak overlap with the transcript opening. Check for wrong EPUB/audio pairing before running chapter curation."
      : "No EPUB/transcript opening mismatch detected.",
    totalReports: 0,
    failedReports: 0,
    acceptedReports: 0,
    skippedReports: 0,
    failedExpectedWindowOverlap: null,
    firstCuratedNodeOpeningOverlap,
    bestEarlyCuratedNodeOpeningOverlap,
    earlyCuratedNodeOpeningOverlaps,
    worstFailedNodes: [],
  };
}

function buildNodeBoundaryFailureDiagnostic(ctx: ChapterCurationContext, reports: NodeBoundaryCurationReport[]): NodeBoundaryFailureDiagnostic {
  const failedReports = reports.filter((report) => report.outcome === "failed");
  const acceptedReports = reports.filter((report) => report.outcome === "accepted" || report.outcome === "dropped");
  const skippedReports = reports.filter((report) => report.outcome === "skipped");
  const failedOverlaps = failedReports.flatMap((report) => {
    const entry = ctx.epubEntries.find((candidate) => candidate.id === report.epubNodeId);
    if (!entry) return [];
    return [{ report, overlap: overlapRatio(entryOpeningDiagnosticTokens(entry), transcriptExpectedWindowTokens(ctx, report.expectedStartTime)) }];
  });
  const failedExpectedWindowOverlap =
    failedOverlaps.length === 0 ? null : Math.round((failedOverlaps.reduce((sum, item) => sum + item.overlap, 0) / failedOverlaps.length) * 1_000) / 1_000;
  const firstEntry = ctx.epubEntries[0];
  const firstCuratedNodeOpeningOverlap = firstEntry
    ? overlapRatio(
        entryOpeningDiagnosticTokens(firstEntry),
        new Set(
          diagnosticTokens(
            transcriptUtterances(ctx)
              .slice(0, 80)
              .map((utterance) => utterance.text)
              .join(" ")
          )
        )
      )
    : null;
  const worstFailedNodes = failedOverlaps
    .sort((a, b) => a.overlap - b.overlap)
    .slice(0, 5)
    .map(({ report, overlap }) => ({
      epubNodeId: report.epubNodeId,
      title: report.title,
      expectedStartTime: report.expectedStartTime,
      overlap,
    }));
  const allBoundariesFailed = reports.length > 0 && acceptedReports.length === 0;
  const lowExpectedWindowOverlap = failedExpectedWindowOverlap !== null && failedExpectedWindowOverlap < 0.45;
  const kind: NodeBoundaryFailureDiagnostic["kind"] = allBoundariesFailed ? "all_boundaries_failed" : lowExpectedWindowOverlap ? "low_expected_window_overlap" : "none";
  const message =
    kind === "all_boundaries_failed"
      ? "No curated EPUB node could be aligned. Check for a wrong EPUB/audio pairing before tuning the chapter algorithm."
      : kind === "low_expected_window_overlap"
        ? "Failed EPUB nodes have weak opener overlap near their expected transcript windows. Check for translated audio, wrong edition, or wrong EPUB/audio pairing."
        : "No corpus-level mismatch signal detected.";
  return {
    kind,
    message,
    totalReports: reports.length,
    failedReports: failedReports.length,
    acceptedReports: acceptedReports.length,
    skippedReports: skippedReports.length,
    failedExpectedWindowOverlap,
    firstCuratedNodeOpeningOverlap,
    worstFailedNodes,
  };
}

function orderedTokenMatchCount(needle: string[], haystack: string[]): number {
  let haystackIndex = 0;
  let count = 0;
  for (const token of needle) {
    const foundIndex = haystack.indexOf(token, haystackIndex);
    if (foundIndex < 0) continue;
    count++;
    haystackIndex = foundIndex + 1;
  }
  return count;
}

function contiguousTokenSequenceIndex(haystack: string[], needle: string[]): number {
  if (needle.length === 0 || haystack.length < needle.length) return -1;
  for (let index = 0; index <= haystack.length - needle.length; index++) {
    let matches = true;
    for (let offset = 0; offset < needle.length; offset++) {
      if (haystack[index + offset] !== needle[offset]) {
        matches = false;
        break;
      }
    }
    if (matches) return index;
  }
  return -1;
}

function immediateBodyMatchAfterHeading(windowText: string, headingText: string, bodyTokens: string[]): number {
  if (bodyTokens.length === 0) return 0;
  const windowTokens = normalizedWordTokens(windowText);
  const headingTokens = normalizedWordTokens(headingText);
  const headingIndex = contiguousTokenSequenceIndex(windowTokens, headingTokens);
  if (headingIndex < 0) return 0;

  const afterHeading = windowTokens
    .slice(headingIndex + headingTokens.length)
    .filter((token) => token.length >= 4)
    .slice(0, Math.max(16, bodyTokens.length * 2));
  const matched = orderedTokenMatchCount(bodyTokens, afterHeading);
  if (matched < Math.min(3, bodyTokens.length)) return 0;
  const firstMatchedIndex = afterHeading.findIndex((token) => bodyTokens.includes(token));
  if (firstMatchedIndex < 0 || firstMatchedIndex > 10) return 0;
  return matched;
}

function titleNumberWord(value: number): string | null {
  const small = [
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
  ];
  if (value >= 0 && value < small.length) return small[value]!;
  const tens = new Map([
    [20, "twenty"],
    [30, "thirty"],
    [40, "forty"],
    [50, "fifty"],
    [60, "sixty"],
    [70, "seventy"],
    [80, "eighty"],
    [90, "ninety"],
  ]);
  if (value >= 20 && value < 100) {
    const ten = Math.floor(value / 10) * 10;
    const one = value % 10;
    const tenWord = tens.get(ten);
    if (!tenWord) return null;
    return one === 0 ? tenWord : `${tenWord} ${small[one]}`;
  }
  return null;
}

function romanNumeralValue(value: string): number | null {
  const numerals = new Map([
    ["i", 1],
    ["v", 5],
    ["x", 10],
    ["l", 50],
  ]);
  const chars = value.toLowerCase();
  if (!/^[ivxl]+$/.test(chars)) return null;
  let total = 0;
  let previous = 0;
  for (const char of [...chars].reverse()) {
    const current = numerals.get(char);
    if (!current) return null;
    if (current < previous) total -= current;
    else total += current;
    previous = current;
  }
  return total > 0 && total < 100 ? total : null;
}

function isStructuralTitleToken(token: string): boolean {
  return (
    token === "chapter" ||
    token === "part" ||
    token === "book" ||
    token === "section" ||
    /^\d+$/.test(token) ||
    /^(?:i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi|xvii|xviii|xix|xx)$/.test(token)
  );
}

function entryTitlePrefixWordCount(entry: EpubChapterEntry): number {
  const wordTokens = entry.words.map((word) => normalizedWordTokens(word.text)[0] ?? "");
  const titleTokens = normalizedWordTokens(entry.title).filter((token) => !isStructuralTitleToken(token));
  const maxPrefix = Math.min(8, wordTokens.length, titleTokens.length);
  for (let length = maxPrefix; length > 0; length--) {
    const titleSuffix = titleTokens.slice(titleTokens.length - length);
    const wordPrefix = wordTokens.slice(0, length);
    if (titleSuffix.every((token, index) => token === wordPrefix[index])) return length;
  }
  return 0;
}

function summarizeOptionalHeadingText(entry: EpubChapterEntry): string {
  const headingWords: string[] = [];
  for (const word of entry.words) {
    if (word.kind !== "heading") break;
    headingWords.push(word.text);
  }
  if (headingWords.length > 0) return headingWords.join(" ").trim();

  const prefixWordCount = entryTitlePrefixWordCount(entry);
  if (prefixWordCount <= 0) return "";
  return entry.words.slice(0, prefixWordCount).map((word) => word.text).join(" ").trim();
}

function summarizeFirstBodyWords(entry: EpubChapterEntry, limit = 40): string {
  const firstBodyIndex = entry.words.findIndex((word) => word.kind === "body");
  const start = firstBodyIndex >= 0 ? firstBodyIndex : entryTitlePrefixWordCount(entry);
  return entry.words.slice(start, start + limit).map((word) => word.text).join(" ").trim();
}

function spokenHeadingVariants(entry: EpubChapterEntry): string[] {
  const variants = new Set<string>();
  const sources = [entry.title, summarizeOptionalHeadingText(entry)].map((value) => normalizeToolText(value)).filter(Boolean);
  for (const source of sources) {
    const tokens = normalizedWordTokens(source);
    if (tokens.length === 0) continue;
    variants.add(tokens.join(" "));
    const structural = tokens.find((token) => token === "chapter" || token === "part" || token === "book" || token === "section");
    const numberToken = tokens.find((token) => /^\d+$/.test(token) || romanNumeralValue(token) !== null);
    if (structural && numberToken) {
      const numeric = /^\d+$/.test(numberToken) ? Number(numberToken) : romanNumeralValue(numberToken);
      const word = numeric === null ? null : titleNumberWord(numeric);
      if (word) variants.add(`${structural} ${word}`);
      if (numeric !== null) variants.add(`${structural} ${numeric}`);
    }

    const leadingNumber = tokens[0];
    const leadingNumeric = leadingNumber && (/^\d+$/.test(leadingNumber) ? Number(leadingNumber) : romanNumeralValue(leadingNumber));
    const leadingNumberWord = typeof leadingNumeric === "number" ? titleNumberWord(leadingNumeric) : null;
    if (leadingNumberWord && tokens.length >= 2 && !isStructuralTitleToken(tokens[1]!)) {
      variants.add([leadingNumberWord, ...tokens.slice(1)].join(" "));
      variants.add([String(leadingNumeric), ...tokens.slice(1)].join(" "));
    }
  }
  return [...variants].filter((variant) => variant.split(/\s+/).length >= 2);
}

function spanScope(span: ChapterCurationSpan, inputScope?: { startTime?: number; endTime?: number }): { startTime?: number; endTime?: number } {
  return {
    startTime: Math.max(span.startTime, inputScope?.startTime ?? span.startTime),
    endTime: Math.min(span.endTime, inputScope?.endTime ?? span.endTime),
  };
}

function structurallyGenericTitle(title: string): boolean {
  const tokens = normalizedWordTokens(title);
  if (tokens.length === 0) return true;
  return tokens.every(
    (token) =>
      token === "chapter" ||
      token === "part" ||
      token === "book" ||
      token === "section" ||
      /^\d+$/.test(token) ||
      romanNumeralValue(token) !== null ||
      Array.from({ length: 21 }, (_, value) => titleNumberWord(value)).includes(token)
  );
}

function repeatedTitleContext(ctx: ChapterCurationContext, targetBoundary: ChapterCurationTargetBoundary): Record<string, unknown> {
  const normalizedTargetTitle = normalizedWordTokens(targetBoundary.title).join(" ");
  if (!normalizedTargetTitle) return { repeatedTitle: false, structuralTitleOnly: true, sameTitleNodes: [] };
  const sameTitleNodes = ctx.epubEntries
    .map((entry, index) => ({ entry, index }))
    .filter(({ entry }) => normalizedWordTokens(entry.title).join(" ") === normalizedTargetTitle)
    .map(({ entry, index }) => ({
      id: entry.id,
      index,
      title: entry.title,
      firstWords: summarizeFirstWords(entry, 18),
    }));
  return {
    repeatedTitle: sameTitleNodes.length > 1,
    structuralTitleOnly: structurallyGenericTitle(targetBoundary.title),
    sameTitleNodeCount: sameTitleNodes.length,
    sameTitleNodes: sameTitleNodes.slice(0, 18),
  };
}

function transcriptWindowSummaryAt(ctx: ChapterCurationContext, startTime: number, radiusSeconds: number): Record<string, unknown> {
  const window = getTranscriptWindowFromContext(ctx, secondsToMs(startTime), secondsToMs(radiusSeconds));
  return {
    startTime: msToSeconds(window.startMs),
    endTime: msToSeconds(window.endMs),
    text: normalizeToolText(window.text).slice(0, 1_200),
  };
}

function targetBoundaryPromptContext(ctx: ChapterCurationContext, span: ChapterCurationSpan, targetBoundary: ChapterCurationTargetBoundary): Record<string, unknown> {
  const targetEntry = ctx.epubEntries[targetBoundary.epubIndex] ?? null;
  const previousEntry = ctx.epubEntries[targetBoundary.epubIndex - 1] ?? null;
  const nextEntry = ctx.epubEntries[targetBoundary.epubIndex + 1] ?? null;
  const targetText = getEpubNodeText(ctx, { epubNodeId: targetBoundary.epubNodeId, startWord: 0, wordCount: 90 });
  return {
    targetBoundary,
    expectedStartTime: targetBoundary.expectedStartTime,
    expectedStartTimeIsEstimate: true,
    span: {
      path: span.path,
      timeSeconds: { start: span.startTime, end: span.endTime },
      epubIndexes: { start: span.epubStartIndex, end: span.epubEndIndex },
    },
    repeatedTitleContext: repeatedTitleContext(ctx, targetBoundary),
    transcriptWindowAroundExpectedTime: transcriptWindowSummaryAt(ctx, targetBoundary.expectedStartTime, 45),
    previousNode: previousEntry
      ? {
          id: previousEntry.id,
          title: previousEntry.title,
          index: targetBoundary.epubIndex - 1,
          tailText: previousEntry.words.slice(Math.max(0, previousEntry.words.length - 48)).map((word) => word.text).join(" "),
        }
      : null,
    targetNode: targetEntry
      ? {
          id: targetEntry.id,
          title: targetEntry.title,
          href: targetEntry.href,
          index: targetBoundary.epubIndex,
          wordCount: targetEntry.wordCount,
          startRatio: inferEntryStartRatio(ctx.epubEntries, targetBoundary.epubIndex),
          endRatio: inferEntryEndRatio(ctx.epubEntries, targetBoundary.epubIndex),
          firstWords: summarizeFirstWords(targetEntry, 80),
          openerText: targetText?.text ?? "",
          phraseVariants: targetText?.phraseVariants ?? [],
        }
      : null,
    nextNode: nextEntry
      ? {
          id: nextEntry.id,
          title: nextEntry.title,
          index: targetBoundary.epubIndex + 1,
          firstWords: summarizeFirstWords(nextEntry, 48),
        }
      : null,
  };
}

function nodeBoundaryPrompt(ctx: ChapterCurationContext, span: ChapterCurationSpan, targetBoundary: ChapterCurationTargetBoundary): string {
  const nearbyAudioOnlyIntervals = (ctx.audioOnlyIntervals ?? []).filter(
    (interval) => Math.abs(interval.startTime - targetBoundary.expectedStartTime) < 1_200 || Math.abs(interval.endTime - targetBoundary.expectedStartTime) < 1_200
  );
  return [
    `Find the audiobook start timestamp for one curated audible EPUB node in "${ctx.book.title}" by ${ctx.book.author}.`,
    `manifestationId: ${ctx.manifestation.id}`,
    `spanPath: ${span.path}`,
    `durationSeconds: ${Math.round(msToSeconds(ctx.durationMs) * 1000) / 1000}`,
    `targetBoundary: ${JSON.stringify(targetBoundary)}`,
    `targetBoundaryContext: ${JSON.stringify(targetBoundaryPromptContext(ctx, span, targetBoundary))}`,
    `audioOnlyIntervalsNearExpectedTime: ${JSON.stringify(nearbyAudioOnlyIntervals)}`,
    "The EPUB list has already been filtered to narrated/audible nodes. Copyright, cover, TOC, and other non-narrated entries should not appear here unless the classifier was uncertain.",
    "Your only goal is this one node. Do not switch to a different node.",
    "Workflow:",
    "1. Call researchEpubBoundary first. It searches this assigned node's opener phrases and reverse-checks transcript hits against the EPUB.",
    "2. If researchEpubBoundary finds an opener/near_opener candidate, inspect it with getTranscriptWindow and submit that first opener word or the silence immediately before it.",
    "3. If no candidate is good enough, call getEpubNodeText for a different target-node word window and search distinctive phrases with rgSearchTranscript or fuzzySearchTranscript.",
    "4. If the transcript appears to be in a different language than the EPUB, translate distinctive target opener phrases yourself and search those translated phrases. Then use the previous/target/next node context to prove the hit belongs to this exact EPUB node.",
    "5. If targetBoundaryContext.repeatedTitleContext says the title is repeated or structural-title-only, ignore the title as evidence. Search the assigned node's body opener and compare against previousNode.tailText and nextNode.firstWords.",
    "6. Use searchEpubText to reject transcript hits that are interior prose rather than the target opener.",
    "7. Do not submit EPUB ratio estimates without transcript evidence.",
    "8. expectedStartTime is only a rough estimate. If transcriptWindowAroundExpectedTime looks like the previous node or another repeated heading, search wider instead of anchoring there.",
    "9. For the first narrated node, do not assume 0s if audioOnlyIntervals indicate credits/preamble. Find where the target opener actually begins.",
    "10. submitNodeBoundary is final; call it only when you can prove this exact EPUB node begins at or immediately after startTime.",
    "All times are seconds.",
  ].join("\n");
}

function createChapterBoundaryJudgeAgent(ctx: ChapterCurationContext, span: ChapterCurationSpan): Agent {
  return new Agent({
    name: "ChapterBoundaryJudge",
    model: ctx.debugJudgeModel?.trim() || ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You review proposed audiobook chapter boundary timestamps. Your only job is to decide whether the proposed timestamp is actually where the assigned EPUB node begins in the audio.",
      "Judge the claim directly: does node X begin at timestamp Y?",
      "Start with audit.boundaryComparison. Compare previousEpub.tailText to transcriptBefore, and targetEpub.bodyHeadText or targetEpub.headText to transcriptAfter.",
      "EPUB chapter headings are often printed but not read aloud. If targetEpub.optionalHeadingText is absent from the transcript but targetEpub.bodyHeadText begins right at the timestamp, that is strong opener evidence.",
      "ASR sometimes drops the printed heading and the first few opener words. If the notes identify a near-opener fallback, accept when transcriptAfter starts with the earliest clear target body phrase and transcriptBefore plausibly matches the previous EPUB tail.",
      "For high-confidence named embedded audio markers, ASR can have a transcript gap after the previous node tail. If notes identify an embedded audio marker, transcriptBefore matches previousEpub.tailText, boundaryWords.after starts tens of seconds later, and those after-words appear inside targetEpub.bodyHeadText/headText rather than previousEpub.tailText, accept the embedded marker as the best chapter boundary. Use finding=embedded_marker_transcript_gap.",
      "For short structural headings such as roman numerals or bare numbers, judge the surrounding prose, not the heading alone. If boundaryWords.after or transcriptAfter contains only the spoken heading cue, call getTranscriptWindow with radiusSeconds around 120 before judging. Accept when the wider window shows previous EPUB tail before the cue and target EPUB prose after the cue. Use finding=spoken_heading_with_surrounding_prose.",
      "For the first EPUB node only, previousEpub is absent. If notes identify an opening interior-start candidate, accept when transcriptBefore is empty or only opening credits/boilerplate and transcriptAfter starts with distinctive prose from inside the target EPUB node. This means the audiobook/ASR omitted earlier printed opener text; it is still the first available narrated boundary.",
      "Check transcriptPrecision. If it is word, the before/after split is exact and should be treated as precise. If it is utterance, a real boundary may fall inside a displayed utterance — accept the utterance start if it contains previous tail then target head.",
      "If boundaryWords.containing is non-empty, the timestamp falls mid-word. Prefer a nearestCleanBoundaryTimes value unless the containing word is itself the first opener word.",
      "Transcript before the timestamp matching the previous EPUB node is positive evidence — not a sign the target opener is interior.",
      "Accept when transcriptBefore plausibly matches the previous EPUB tail and transcriptAfter begins with distinctive target body opener prose, even if the printed heading is absent.",
      "Reject when target body opener evidence is absent, offset, generic, pre-target, or only an interior match.",
      "Do not suggest alternate timestamps or nodes. Do not invent a chapter plan. Judge only this proposed boundary.",
      "Describe problems using evidence terms — opener_evidence_at_timestamp, opener_evidence_offset_in_window, window_starts_before_opener_evidence, embedded_marker_transcript_gap, tool_classified_interior_match, generic_or_weak_overlap, submitted_evidence_insufficient — not narrative terms like 'mid-scene'.",
      "You must call submitBoundaryJudgment.",
    ].join("\n"),
    tools: [
      tool({
        name: "getTranscriptWindow",
        description: "Return wider transcript context around the proposed boundary. Use this before judging spoken heading cues or transcript gaps.",
        parameters: getTranscriptWindowSchema,
        strict: true,
        execute: (input) => {
          const clampedStart = Math.min(span.endTime, Math.max(span.startTime, input.startTime));
          const radiusSeconds = Math.min(input.radiusSeconds ?? 120, 180);
          return getTranscriptWindow(ctx, { startTime: clampedStart, radiusSeconds });
        },
      }),
      tool({
        name: "submitBoundaryJudgment",
        description: "Submit your acceptance or rejection of the proposed chapter boundary.",
        parameters: submitFulcrumJudgmentSchema,
        strict: true,
        execute: (input) => input,
      }),
    ],
    toolUseBehavior: fulcrumJudgeToolUseBehavior,
  });
}

function chapterBoundaryJudgePrompt(ctx: ChapterCurationContext, span: ChapterCurationSpan, proposal: ChapterBoundaryJudgeProposal): string {
  return [
    `Review this proposed chapter boundary for "${ctx.book.title}" by ${ctx.book.author}.`,
    "Return accepted=false if the timestamp is not clearly where the EPUB node begins in the audio. When in doubt, reject — the curator will search again.",
    "Primary evidence: audit.boundaryComparison. transcriptBefore matching previousEpub.tailText and transcriptAfter matching targetEpub.bodyHeadText or targetEpub.headText supports acceptance.",
    "Printed EPUB headings are often absent from ASR. Do not reject merely because optionalHeadingText is missing when bodyHeadText begins at the proposed timestamp.",
    "If notes identify a near-opener fallback, accept the first audible target body phrase when the heading is absent from ASR and transcriptBefore supports the previous EPUB tail.",
    "If notes identify a high-confidence named embedded audio marker, handle transcript gaps differently: accept when transcriptBefore matches previousEpub.tailText, boundaryWords.after starts tens of seconds later due to an ASR/audio gap, and those after-words appear inside targetEpub.bodyHeadText/headText rather than previousEpub.tailText. This is not the same as a random interior title mention; use finding=embedded_marker_transcript_gap.",
    "For short structural headings such as roman numerals or bare numbers, judge surrounding prose instead of the heading alone. If audit.boundaryComparison only shows the spoken heading cue and not target prose after it, call getTranscriptWindow around proposed.startTime with radiusSeconds around 120. Accept when the wider window shows previous-node prose before the cue and target-node prose after the cue; use finding=spoken_heading_with_surrounding_prose.",
    "If this is the first EPUB node and notes identify an opening interior-start candidate, previousEpub will be absent. Accept when transcriptBefore is empty or only opening credits/boilerplate and transcriptAfter starts with distinctive prose from inside targetEpub, even if earlier printed opener words are missing from ASR/audio.",
    "transcriptPrecision=word is exact; boundaryWords.containing non-empty means the timestamp is mid-word — prefer nearestCleanBoundaryTimes unless that word is the first opener. transcriptPrecision=utterance is lower precision; accepting an utterance start is valid when it contains previous tail followed by target head.",
    "transcriptBefore matching the previous EPUB node is positive evidence, not a problem.",
    "Treat finding as an evidence classification, not ground truth about where the boundary truly is.",
    "",
    JSON.stringify(
      {
        span,
        proposed: {
          kind: proposal.kind,
          epubNodeId: proposal.epubNodeId,
          epubIndex: proposal.epubIndex,
          title: proposal.title,
          startTime: proposal.startTime,
          notes: proposal.notes,
        },
        audit: {
          epubNodeId: proposal.audit.epubNodeId,
          title: proposal.audit.title,
          startTime: proposal.audit.startTime,
          boundaryComparison: proposal.audit.boundaryComparison,
          transcriptWindow: proposal.audit.transcriptWindow,
          evidenceCandidates: proposal.audit.candidates.map((candidate) => ({
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            text: candidate.text,
            afterText: candidate.afterText,
            quality: candidate.quality,
          })),
        },
      },
      null,
      2
    ),
  ].join("\n");
}

export async function judgeChapterBoundary(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  proposal: ChapterBoundaryJudgeProposal
): Promise<SubmitFulcrumJudgmentResult | null> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) return null;
  ensureTracingInitialized(apiKey);
  const timeoutMs = Math.min(Math.max(5_000, ctx.settings.agents.timeoutMs), 90_000);
  const abort = new AbortController();
  const timeout = setTimeout(() => abort.abort(), timeoutMs);
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    const result = await runner.run(createChapterBoundaryJudgeAgent(ctx, span), chapterBoundaryJudgePrompt(ctx, span, proposal), {
      maxTurns: 6,
      signal: abort.signal,
      toolExecution: { maxFunctionToolConcurrency: 1 },
    });
    logAgentUsageEvent(ctx, {
      role: "judge",
      model: ctx.debugJudgeModel?.trim() || ctx.settings.agents.model,
      rawResponses: result.rawResponses as unknown[],
      span,
    });
    const judgment = parseFulcrumJudgmentOutput(result.finalOutput);
    const tracePath = writeChapterCurationTrace(ctx, `${proposal.kind}-judge-${span.path}-${proposal.epubNodeId}`, {
      span,
      proposal,
      judgment,
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
    });
    logChapterCurationEvent(ctx, {
      type: "fulcrum-judge-result",
      message: `${proposal.kind} judge span=${span.path} epub=${proposal.epubNodeId} accepted=${judgment?.accepted ?? "none"} confidence=${judgment?.confidence ?? "none"}`,
      span,
      split: {
        epubNodeId: proposal.epubNodeId,
        title: proposal.title,
        startTime: proposal.startTime,
      },
      proposalKind: proposal.kind,
      judgment,
      tracePath,
    });
    return judgment;
  } catch (error) {
    logAgentUsageEvent(ctx, {
      role: "judge",
      model: ctx.debugJudgeModel?.trim() || ctx.settings.agents.model,
      serializedError: serializeAgentError(error),
      span,
    });
    logChapterCurationEvent(ctx, {
      type: "fulcrum-judge-error",
      message: `${proposal.kind} judge span=${span.path} epub=${proposal.epubNodeId} error=${JSON.stringify((error as Error).message)}`,
      span,
      split: {
        epubNodeId: proposal.epubNodeId,
        title: proposal.title,
        startTime: proposal.startTime,
      },
      proposalKind: proposal.kind,
      error: serializeAgentError(error),
    });
    return null;
  } finally {
    clearTimeout(timeout);
    await provider.close().catch(() => undefined);
  }
}

export async function judgeFulcrumSplit(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  split: Extract<SubmitFulcrumSplitResult, { accepted: true }>
): Promise<SubmitFulcrumJudgmentResult | null> {
  return judgeChapterBoundary(ctx, span, {
    kind: "fulcrum",
    spanPath: split.spanPath,
    epubNodeId: split.epubNodeId,
    epubIndex: split.epubIndex,
    title: split.title,
    startTime: split.startTime,
    notes: split.notes,
    audit: split.audit,
  });
}

function createAudibleEpubNodeSelectionAgent(ctx: ChapterCurationContext): Agent {
  return new Agent({
    name: "AudibleEpubNodeClassifier",
    model: ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You classify which EPUB spine nodes are plausible audiobook chapter material before timestamp curation.",
      "Your job is only to include or exclude EPUB nodes. Do not decide timestamps and do not create a chapter plan.",
      "Exclude obvious non-audio front/back matter such as copyright pages, dedications, acknowledgments, tables of contents, cover/nav pages, ads, indexes, and about-the-author pages when they are not plausibly spoken in the audiobook.",
      "Treat edition-specific prefaces, revised/expanded edition notes, bonus essays, and print-only update notes as exclude-by-default unless the transcript beginning/ending excerpts directly show that specific node is narrated.",
      "Keep prologues, part headings, chapter nodes, epilogues, and any uncertain narrative node. False exclusions are worse than false inclusions.",
      "Use the transcript beginning and ending excerpts to identify audiobook credits/preamble and whether EPUB front/back matter is likely represented.",
      "For multi-asset audiobooks, classify audio-only intervals around asset starts, asset ends, and joins, such as publisher intros, end credits, recaps, and part bumpers that have no EPUB node.",
      "Keep audio-only intervals narrow and evidence-based. They are annotations for later tools, not chapter timestamps.",
      "If a provided audio excerpt visibly contains credits, publisher branding, part bumpers, previews, or promotional material that is not an EPUB node, include an audioOnlyIntervals entry with an approximate startTime/endTime from the excerpt range instead of only mentioning it in notes.",
      "You must call submitAudibleEpubNodeSelection.",
    ].join("\n"),
    tools: [
      tool({
        name: "submitAudibleEpubNodeSelection",
        description: "Submit the EPUB node ids that should be used for recursive audiobook chapter curation.",
        parameters: audibleEpubNodeSelectionSchema,
        strict: true,
        execute: (input) => input,
      }),
    ],
    toolUseBehavior: audibleEpubNodeSelectionToolUseBehavior,
  });
}

function orderedCurationContainers(ctx: Pick<ChapterCurationContext, "containers">): ChapterCurationContext["containers"] {
  return [...ctx.containers].sort((a, b) => a.asset.sequence_in_manifestation - b.asset.sequence_in_manifestation || a.asset.id - b.asset.id);
}

function containerDurationMs(container: ChapterCurationContext["containers"][number]): number {
  return container.asset.duration_ms ?? container.files.reduce((sum, file) => sum + file.duration_ms, 0);
}

function audioAssetBoundaryExcerpts(ctx: ChapterCurationContext): Array<Record<string, unknown>> {
  const containers = orderedCurationContainers(ctx);
  const excerpts: Array<Record<string, unknown>> = [];
  let offsetMs = 0;
  for (const [index, container] of containers.entries()) {
    const durationMs = containerDurationMs(container);
    const startTime = msToSeconds(offsetMs);
    const endTime = msToSeconds(offsetMs + durationMs);
    excerpts.push({
      assetId: container.asset.id,
      sequence: container.asset.sequence_in_manifestation,
      position: "asset_start",
      startTime,
      excerptStartTime: msToSeconds(offsetMs),
      excerptEndTime: msToSeconds(Math.min(ctx.durationMs, offsetMs + 90_000)),
      excerpt: getTranscriptWindowFromContext(ctx, offsetMs + 45_000, 45_000).text.slice(0, 1_500),
    });
    excerpts.push({
      assetId: container.asset.id,
      sequence: container.asset.sequence_in_manifestation,
      position: "asset_end",
      endTime,
      excerptStartTime: msToSeconds(Math.max(0, offsetMs + durationMs - 90_000)),
      excerptEndTime: endTime,
      excerpt: getTranscriptWindowFromContext(ctx, Math.max(offsetMs, offsetMs + durationMs - 45_000), 45_000).text.slice(-1_500),
    });
    if (index > 0) {
      excerpts.push({
        assetId: container.asset.id,
        sequence: container.asset.sequence_in_manifestation,
        position: "asset_join",
        joinTime: startTime,
        excerptStartTime: msToSeconds(Math.max(0, offsetMs - 90_000)),
        excerptEndTime: msToSeconds(Math.min(ctx.durationMs, offsetMs + 90_000)),
        excerpt: getTranscriptWindowFromContext(ctx, offsetMs, 90_000).text.slice(0, 2_500),
      });
    }
    offsetMs += durationMs;
  }
  return excerpts;
}

function audibleEpubNodeSelectionPrompt(ctx: ChapterCurationContext): string {
  const startExcerpt = getTranscriptWindowFromContext(ctx, 150_000, 150_000).text.slice(0, 4_000);
  const endCenter = Math.max(0, ctx.durationMs - 150_000);
  const endExcerpt = getTranscriptWindowFromContext(ctx, endCenter, 150_000).text.slice(-4_000);
  return [
    `Classify EPUB nodes for "${ctx.book.title}" by ${ctx.book.author}.`,
    "Return only nodes that are plausible audiobook chapter material for recursive timestamp curation.",
    "Keep uncertain narrative nodes; exclude only nodes that look like non-audio front/back matter or navigation.",
    "Do not exclude prose-like end matter solely because it is titled Postscript, Afterword, Epilogue, Coda, or similar. If the transcript ending contains its opener/title, or if the audio continues with book prose after the prior narrative node, keep it as audiobook chapter material.",
    "It is better to keep a plausible narrated prose node and let boundary validation fail later than to exclude a genuinely narrated chapter-like node during this pre-pass.",
    "",
    JSON.stringify(
      {
        audioTranscriptBeginning: startExcerpt,
        audioTranscriptEnding: endExcerpt,
        audioAssetBoundaryExcerpts: audioAssetBoundaryExcerpts(ctx),
        epubNodes: ctx.epubEntries.map((entry, index) => ({
          index,
          id: entry.id,
          title: entry.title,
          href: entry.href,
          wordCount: entry.wordCount,
          firstWords: summarizeFirstWords(entry, 50),
        })),
      },
      null,
      2
    ),
  ].join("\n");
}

export async function classifyAudibleEpubNodes(ctx: ChapterCurationContext, runner: Runner): Promise<AudibleEpubNodeSelection | null> {
  try {
    const result = await runner.run(createAudibleEpubNodeSelectionAgent(ctx), audibleEpubNodeSelectionPrompt(ctx), {
      maxTurns: 4,
      toolExecution: { maxFunctionToolConcurrency: 1 },
    });
    logAgentUsageEvent(ctx, {
      role: "audible-node-selection",
      model: ctx.settings.agents.model,
      rawResponses: result.rawResponses as unknown[],
    });
    const selection = parseAudibleEpubNodeSelectionOutput(result.finalOutput);
    const tracePath = writeChapterCurationTrace(ctx, "audible-epub-node-selection", {
      selection,
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
    });
    logChapterCurationEvent(ctx, {
      type: "audible-epub-node-selection",
      message: `audible epub nodes selected=${selection?.audibleNodeIds.length ?? 0} excluded=${selection?.excludedNodes.length ?? 0}`,
      selection,
      tracePath,
    });
    return selection;
  } catch (error) {
    logAgentUsageEvent(ctx, {
      role: "audible-node-selection",
      model: ctx.settings.agents.model,
      serializedError: serializeAgentError(error),
    });
    logChapterCurationEvent(ctx, {
      type: "audible-epub-node-selection-error",
      message: `audible epub node selection error=${JSON.stringify((error as Error).message)}`,
      error: serializeAgentError(error),
    });
    return null;
  }
}

function rejectedFulcrumJudgeInstruction(judgment: SubmitFulcrumJudgmentResult): string {
  switch (judgment.finding) {
    case "opener_evidence_offset_in_window":
    case "window_starts_before_opener_evidence":
    case "embedded_marker_transcript_gap":
      return "Check boundaryWords/nearestCleanBoundaryTimes for a cleaner boundary, then inspect and resubmit.";
    case "tool_classified_interior_match":
    case "generic_or_weak_overlap":
      return "Do not resubmit nearby body text. Search a different opener phrase from the same node and verify with searchEpubText.";
    case "submitted_evidence_insufficient":
      return "Gather stronger opener evidence: try researchEpubBoundary or a fresh transcript search followed by searchEpubText.";
    case "opener_evidence_at_timestamp":
      return "Reinspect boundaryWords. Submit only once the target opener starts at or immediately after startTime.";
  }
}

function conciseFulcrumJudgmentMessage(judgment: SubmitFulcrumJudgmentResult): string {
  const concern = judgment.concerns[0] ? ` ${judgment.concerns[0]}` : "";
  return `${judgment.finding}; openerEvidenceAtTimestamp=${judgment.openerEvidenceAtTimestamp}.${concern}`;
}

const rgSearchTranscriptSchema = z.object({
  pattern: z.string(),
  regex: z.boolean().optional(),
  scope: z.object({ startTime: z.number().optional(), endTime: z.number().optional() }).optional(),
  beforeSeconds: z.number().optional(),
  afterSeconds: z.number().optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const fuzzySearchTranscriptSchema = z.object({
  query: z.string(),
  scope: z.object({ startTime: z.number().optional(), endTime: z.number().optional() }).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const assignedResearchEpubBoundarySchema = z.object({});
const assignedGetEpubNodeTextSchema = z.object({
  startWord: z.number().int().nonnegative().optional(),
  wordCount: z.number().int().positive().max(180).optional(),
});
const assignedSearchEpubTextSchema = z.object({
  query: z.string().trim().min(1),
  limit: z.number().int().positive().max(20).optional(),
});
const getTranscriptWindowSchema = z.object({
  startTime: z.number(),
  radiusSeconds: z.number().positive().max(300).optional(),
});
const assignedSubmitFulcrumSplitSchema = z.object({
  startTime: z.number().finite().nonnegative(),
  evidence: z.string().trim().optional(),
  notes: z.string().trim().optional(),
});
const assignedSubmitNodeBoundarySchema = z.object({
  startTime: z.number().finite().nonnegative(),
  evidence: z.string().trim().optional(),
  notes: z.string().trim().optional(),
});

export function createNodeBoundaryCuratorAgent(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary,
  modelOverride?: string
): Agent {
  let rejectedBoundaryRequiresEvidence = false;
  let evidenceCallsSinceRejectedBoundary = 0;
  let transcriptSearchesSinceBoundarySubmit = 0;
  let rejectedBoundaryCount = 0;
  const rejectedBoundaries = new Set<string>();
  const maxRejectedBoundarySubmissions = Math.max(1, Number(process.env.PODIBLE_CHAPTER_NODE_MAX_REJECTIONS ?? 4));

  function markEvidenceToolUsed(): void {
    if (rejectedBoundaryRequiresEvidence) evidenceCallsSinceRejectedBoundary++;
  }

  function markTranscriptSearchToolUsed(): void {
    markEvidenceToolUsed();
    transcriptSearchesSinceBoundarySubmit++;
  }

  function runToolWithEvents<T>(toolName: string, input: unknown, fn: () => T): T {
    logSpanToolCall(ctx, span, toolName, input);
    try {
      const result = fn();
      if (result && typeof ((result as unknown as PromiseLike<unknown>).then) === "function") {
        return (result as unknown as Promise<unknown>).then(
          (value) => {
            logSpanToolResult(ctx, span, toolName, value);
            return value;
          },
          (error) => {
            logSpanToolError(ctx, span, toolName, error);
            throw error;
          }
        ) as T;
      }
      logSpanToolResult(ctx, span, toolName, result);
      return result;
    } catch (error) {
      logSpanToolError(ctx, span, toolName, error);
      throw error;
    }
  }

  function recordRejectedBoundary(reason: string): void {
    rejectedBoundaryCount++;
    if (rejectedBoundaryCount >= maxRejectedBoundarySubmissions) {
      throw new NodeBoundaryRejectedLimitError(targetBoundary.epubNodeId, rejectedBoundaryCount, reason);
    }
  }

  return new Agent({
    name: "NodeChapterCurator",
    model: modelOverride?.trim() || ctx.debugCuratorModel?.trim() || ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You find the start timestamp of one assigned audible EPUB chapter node in an audiobook transcript.",
      `Your only goal is to locate where ${targetBoundary.epubNodeId} (${targetBoundary.title}) begins in the audio. Do not switch to a different node.`,
      "The EPUB nodes have already been filtered to narrated/audible nodes by a separate classifier.",
      "Workflow:",
      "1. Use targetBoundaryContext in the prompt. Do not ask for the full EPUB structure.",
      "2. Call researchEpubBoundary first. It searches only the assigned node.",
      "3. If it finds an opener/near_opener candidate, inspect with getTranscriptWindow and submit the earliest target opener time.",
      "4. If it fails, call getEpubNodeText for a different target-node window and search distinctive phrases with rgSearchTranscript or fuzzySearchTranscript.",
      "5. Use searchEpubText to reject interior transcript hits.",
      "6. Do not submit estimates from EPUB position alone. A transcript search result is required before submitting.",
      "7. submitNodeBoundary is final and must assert the assigned EPUB node begins at startTime.",
      "All times are seconds.",
    ].join("\n"),
    tools: [
      tool({
        name: "getEpubNodeText",
        description: "Return target EPUB text and exact-search phrase variants. Optional startWord/wordCount select a target-node window.",
        parameters: assignedGetEpubNodeTextSchema,
        strict: true,
        execute: (input) => {
          const toolInput = { ...input, epubNodeId: targetBoundary.epubNodeId };
          return runToolWithEvents("getEpubNodeText", toolInput, () => {
            markEvidenceToolUsed();
            return getEpubNodeText(ctx, toolInput);
          });
        },
      }),
      tool({
        name: "researchEpubBoundary",
        description: "Research the assigned target boundary: opener phrases, nearby transcript hits, windows, and reverse EPUB opener/near_opener checks.",
        parameters: assignedResearchEpubBoundarySchema,
        strict: true,
        execute: async () => {
          const toolInput = {
            epubNodeId: targetBoundary.epubNodeId,
            expectedTime: targetBoundary.expectedStartTime,
            scope: { startTime: span.startTime, endTime: span.endTime },
            searchRadiusSeconds: Math.max(600, Math.min(7_200, spanDurationSeconds(span) / 4)),
          };
          return runToolWithEvents("researchEpubBoundary", toolInput, async () => {
            markTranscriptSearchToolUsed();
            return researchEpubBoundary(ctx, toolInput);
          });
        },
      }),
      tool({
        name: "searchEpubText",
        description: "Reverse-search target EPUB text for a transcript phrase and classify it as opener, near_opener, interior, pre_target, or post_target.",
        parameters: assignedSearchEpubTextSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("searchEpubText", input, () => {
            markEvidenceToolUsed();
            return searchEpubText(ctx, { ...input, nodeIds: [targetBoundary.epubNodeId], targetNodeId: targetBoundary.epubNodeId });
          });
        },
      }),
      tool({
        name: "rgSearchTranscript",
        description: "Search transcript utterances with ripgrep inside this task span. Time scopes are seconds.",
        parameters: rgSearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("rgSearchTranscript", input, () => {
            markTranscriptSearchToolUsed();
            return rgSearchTranscript(ctx, { ...input, scope: spanScope(span, input.scope) });
          });
        },
      }),
      tool({
        name: "fuzzySearchTranscript",
        description: "Fuzzy-search transcript utterances inside this task span. Time scopes are seconds.",
        parameters: fuzzySearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("fuzzySearchTranscript", input, () => {
            markTranscriptSearchToolUsed();
            return fuzzySearchTranscript(ctx, { ...input, scope: spanScope(span, input.scope) });
          });
        },
      }),
      tool({
        name: "getTranscriptWindow",
        description: "Return transcript utterances around a timestamp inside this task span. startTime is seconds.",
        parameters: getTranscriptWindowSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("getTranscriptWindow", input, () => {
            markEvidenceToolUsed();
            return getTranscriptWindow(ctx, { ...input, startTime: Math.min(span.endTime, Math.max(span.startTime, input.startTime)) });
          });
        },
      }),
      tool({
        name: "submitNodeBoundary",
        description: "Submit the start timestamp for the assigned audible EPUB node once you have opener evidence.",
        parameters: assignedSubmitNodeBoundarySchema,
        strict: true,
        execute: async (input) => {
          const boundaryInput = {
            spanPath: span.path,
            epubNodeId: targetBoundary.epubNodeId,
            title: targetBoundary.title,
            startTime: input.startTime,
            evidence: input.evidence,
            notes: input.notes,
          };
          return runToolWithEvents("submitNodeBoundary", boundaryInput, async () => {
            if (rejectedBoundaryRequiresEvidence && evidenceCallsSinceRejectedBoundary === 0) {
              return {
                accepted: false,
                kind: "node_boundary",
                errors: ["submitNodeBoundary was called again without gathering new transcript evidence after the previous rejection."],
                warnings: [],
                audit: null,
                instruction: "Search a different opener phrase, inspect the match with getTranscriptWindow, then submit a materially different timestamp.",
              };
            }
            const hasPostRejectionEvidence = rejectedBoundaryRequiresEvidence && evidenceCallsSinceRejectedBoundary > 0;
            if (transcriptSearchesSinceBoundarySubmit === 0 && !hasPostRejectionEvidence) {
              return {
                accepted: false,
                kind: "node_boundary",
                errors: ["A transcript search result is required before submitting a boundary."],
                warnings: [],
                audit: null,
                instruction: "Call researchEpubBoundary or search a distinctive opener phrase before submitting.",
              };
            }
            transcriptSearchesSinceBoundarySubmit = 0;
            const rejectedKey = `${boundaryInput.epubNodeId}:${Math.round(boundaryInput.startTime)}`;
            if (rejectedBoundaries.has(rejectedKey)) {
              rejectedBoundaryRequiresEvidence = true;
              evidenceCallsSinceRejectedBoundary = 0;
              return {
                accepted: false,
                kind: "node_boundary",
                errors: [`${boundaryInput.epubNodeId} near ${Math.round(boundaryInput.startTime)}s was already rejected for this task.`],
                warnings: [],
                audit: null,
                instruction: "Stay on this EPUB node. Search a different opener phrase and submit a materially different timestamp.",
              };
            }
            const result = await validateNodeBoundary(ctx, boundaryInput, { span, targetBoundary });
            if (!result.accepted) {
              rejectedBoundaries.add(rejectedKey);
              rejectedBoundaryRequiresEvidence = true;
              evidenceCallsSinceRejectedBoundary = 0;
              recordRejectedBoundary(result.errors[0] ?? "validation rejected boundary");
              return result;
            }
            const judgment = await judgeChapterBoundary(ctx, span, {
              kind: "node_boundary",
              spanPath: result.spanPath,
              epubNodeId: result.epubNodeId,
              epubIndex: result.epubIndex,
              title: result.title,
              startTime: result.startTime,
              notes: result.notes,
              audit: result.audit,
            });
            if (judgment && !judgment.accepted) {
              rejectedBoundaries.add(rejectedKey);
              rejectedBoundaryRequiresEvidence = true;
              evidenceCallsSinceRejectedBoundary = 0;
              recordRejectedBoundary(conciseFulcrumJudgmentMessage(judgment));
              return {
                accepted: false,
                kind: "node_boundary",
                errors: [conciseFulcrumJudgmentMessage(judgment)],
                warnings: [],
                audit: result.audit,
                instruction: rejectedFulcrumJudgeInstruction(judgment),
              };
            }
            rejectedBoundaryRequiresEvidence = false;
            evidenceCallsSinceRejectedBoundary = 0;
            return result;
          });
        },
      }),
    ],
    toolUseBehavior: nodeBoundaryToolUseBehavior,
  });
}

export type DeterministicBoundaryCandidate = {
  startTime: number;
  source: "research_opener" | "spoken_heading" | "partial_opener" | "near_opener_fallback" | "supporting_context_backtrack" | "opening_interior_start";
  phrase: string;
  phraseStartWord: number | null;
  reverseEpubRelation: EpubBoundaryResearchHit["reverseEpubRelation"];
  transcriptText: string;
  transcriptWindow?: string;
  bodyMatchCount?: number;
};

export async function findSpokenHeadingBoundaryCandidate(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary
): Promise<DeterministicBoundaryCandidate | null> {
  const entry = ctx.epubEntries[targetBoundary.epubIndex];
  if (!entry) return null;
  const variants = spokenHeadingVariants(entry);
  if (variants.length === 0) return null;
  const scope = {
    startTime: span.startTime,
    endTime: span.endTime,
  };
  const searchRadius = Math.max(180, Math.min(1_200, spanDurationSeconds(span) / 2));
  const narrowedScope = {
    startTime: Math.max(scope.startTime, targetBoundary.expectedStartTime - searchRadius),
    endTime: Math.min(scope.endTime, targetBoundary.expectedStartTime + searchRadius),
  };
  const hasBodyLikeWords = entry.words.some((word) => word.kind !== "heading");
  const bodyTokens = hasBodyLikeWords ? textTokens(summarizeFirstBodyWords(entry, 32)).slice(0, 10) : [];
  const candidates: DeterministicBoundaryCandidate[] = [];
  for (const variant of variants.slice(0, 6)) {
    const exact = await rgSearchTranscript(ctx, {
      pattern: variant,
      scope: narrowedScope,
      beforeSeconds: 5,
      afterSeconds: 45,
      limit: 5,
    });
    const matches =
      exact.matches.length > 0
        ? exact.matches
        : (
            await fuzzySearchTranscript(ctx, {
              query: variant,
              scope: narrowedScope,
              limit: 5,
            })
          ).matches;
    for (const match of matches) {
      if (match.startTime <= span.startTime || match.startTime >= span.endTime) continue;
      const window = getTranscriptWindow(ctx, { startTime: match.startTime, radiusSeconds: 60 });
      const matchCount = immediateBodyMatchAfterHeading(window.text, variant, bodyTokens);
      const required = Math.min(3, bodyTokens.length);
      if (required > 0 && matchCount < required) continue;
      candidates.push({
        startTime: match.startTime,
        source: "spoken_heading",
        phrase: variant,
        phraseStartWord: null,
        reverseEpubRelation: "opener",
        transcriptText: normalizeToolText(match.text).slice(0, 500),
        transcriptWindow: normalizeToolText(window.text).slice(0, 900),
        bodyMatchCount: matchCount,
      });
    }
    if (candidates.length > 0) break;
  }
  return candidates.sort((a, b) => Math.abs(a.startTime - targetBoundary.expectedStartTime) - Math.abs(b.startTime - targetBoundary.expectedStartTime))[0] ?? null;
}

export function chooseResearchBoundaryCandidate(
  research: ResearchEpubBoundaryResult | null,
  span: ChapterCurationSpan,
  options: { allowOpeningNearOpenerFallback?: boolean; includeFallback?: boolean } = {}
): DeterministicBoundaryCandidate | null {
  if (!research) return null;
  const includeFallback = options.includeFallback ?? true;
  const strict = research.bestCandidates.find(
    (hit) =>
      hit.boundaryUse === "candidate_start" &&
      (hit.reverseEpubRelation === "opener" || hit.reverseEpubRelation === "near_opener") &&
      hit.phraseStartWord <= 2 &&
      hit.startTime > span.startTime &&
      hit.startTime < span.endTime
  );
  if (strict) {
    return {
      startTime: strict.startTime,
      source: "research_opener",
      phrase: strict.phrase,
      phraseStartWord: strict.phraseStartWord,
      reverseEpubRelation: strict.reverseEpubRelation,
      transcriptText: strict.transcriptText,
      transcriptWindow: strict.transcriptWindow,
    };
  }
  if (!includeFallback) return null;
  const fallback = research.bestCandidates
    .filter(
      (hit) =>
        (hit.boundaryUse === "candidate_start" || (options.allowOpeningNearOpenerFallback && hit.boundaryUse === "supporting_context")) &&
        (hit.reverseEpubRelation === "opener" || hit.reverseEpubRelation === "near_opener") &&
        hit.phraseStartWord <= (options.allowOpeningNearOpenerFallback ? 80 : 32) &&
        hit.startTime > span.startTime &&
        hit.startTime < span.endTime
    )
    .sort((a, b) => a.phraseStartWord - b.phraseStartWord || Math.abs(a.distanceFromExpectedSeconds) - Math.abs(b.distanceFromExpectedSeconds))[0];
  if (!fallback) return null;
  return {
    startTime: fallback.startTime,
    source: "near_opener_fallback",
    phrase: fallback.phrase,
    phraseStartWord: fallback.phraseStartWord,
    reverseEpubRelation: fallback.reverseEpubRelation,
    transcriptText: fallback.transcriptText,
    transcriptWindow: fallback.transcriptWindow,
  };
}

export function chooseSupportingContextBacktrackCandidate(
  ctx: ChapterCurationContext,
  research: ResearchEpubBoundaryResult | null,
  span: ChapterCurationSpan
): DeterministicBoundaryCandidate | null {
  if (!research) return null;
  const entry = ctx.epubEntries[research.epubIndex];
  if (!entry) return null;
  const openerTokens = distinctiveOpenerTokens(entry);
  if (openerTokens.length === 0) return null;
  const supportingHits = research.bestCandidates
    .filter(
      (hit) =>
        (hit.boundaryUse === "supporting_context" || (hit.boundaryUse === "candidate_start" && hit.phraseStartWord > 2)) &&
        (hit.reverseEpubRelation === "opener" || hit.reverseEpubRelation === "near_opener") &&
        hit.phraseStartWord <= 80 &&
        hit.startTime > span.startTime &&
        hit.startTime < span.endTime
    )
    .sort((a, b) => a.phraseStartWord - b.phraseStartWord || Math.abs(a.distanceFromExpectedSeconds) - Math.abs(b.distanceFromExpectedSeconds));

  for (const hit of supportingHits) {
    const windowStartMs = secondsToMs(Math.max(span.startTime, hit.startTime - 90));
    const windowEndMs = secondsToMs(hit.startTime + 5);
    const utterances = transcriptUtterances(ctx).filter((utterance) => utterance.endMs >= windowStartMs && utterance.startMs <= windowEndMs);
    for (const utterance of utterances) {
      const text = normalizeToolText(utterance.text);
      if (normalizedWordTokens(text).length < 4) continue;
      if (!hasOpenerTokenEvidence(text, openerTokens)) continue;
      const reverse = searchEpubText(ctx, {
        query: text,
        nodeIds: [research.epubNodeId],
        targetNodeId: research.epubNodeId,
        limit: 3,
      }).matches.find((candidate) => candidate.epubNodeId === research.epubNodeId);
      if (!reverse || (reverse.relationToTarget !== "opener" && reverse.relationToTarget !== "near_opener")) continue;
      const transcriptWindow = getTranscriptWindowFromContext(ctx, utterance.startMs, secondsToMs(45));
      return {
        startTime: msToSeconds(utterance.startMs),
        source: "supporting_context_backtrack",
        phrase: text,
        phraseStartWord: hit.phraseStartWord,
        reverseEpubRelation: reverse.relationToTarget,
        transcriptText: text.slice(0, 500),
        transcriptWindow: normalizeToolText(transcriptWindow.text).slice(0, 900),
      };
    }
  }

  return null;
}

export function findOpeningInteriorStartCandidate(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary
): DeterministicBoundaryCandidate | null {
  if (targetBoundary.epubIndex !== 0) return null;
  const entry = ctx.epubEntries[targetBoundary.epubIndex];
  if (!entry || entry.id !== targetBoundary.epubNodeId) return null;

  const spanDuration = spanDurationSeconds(span);
  const searchEndTime = Math.min(span.endTime, span.startTime + Math.max(180, Math.min(900, spanDuration * 0.1)));
  const searchStartMs = secondsToMs(span.startTime);
  const searchEndMs = secondsToMs(searchEndTime);

  for (const utterance of transcriptUtterances(ctx)) {
    if (utterance.endMs < searchStartMs || utterance.startMs > searchEndMs) continue;
    const text = normalizeToolText(utterance.text);
    if (textTokens(text).length < 4) continue;

    const reverse = searchEpubText(ctx, {
      query: text,
      targetNodeId: entry.id,
      limit: 5,
    }).matches[0];
    if (!reverse || reverse.epubNodeId !== entry.id) continue;
    if (reverse.targetNodeDistance !== 0 || reverse.targetWordOffset === null) continue;
    if (reverse.relationToTarget !== "opener" && reverse.relationToTarget !== "near_opener" && reverse.relationToTarget !== "interior") continue;

    const phraseStartWord = firstOrderedQueryTokenOffset(entry, text);
    if (phraseStartWord === null || phraseStartWord > 160) continue;
    const reverseEpubRelation = phraseStartWord <= 8 ? "opener" : phraseStartWord <= 50 ? "near_opener" : "interior";

    const transcriptWindow = getTranscriptWindowFromContext(ctx, utterance.startMs, secondsToMs(45));
    return {
      startTime: msToSeconds(utterance.startMs),
      source: "opening_interior_start",
      phrase: text,
      phraseStartWord,
      reverseEpubRelation,
      transcriptText: text.slice(0, 500),
      transcriptWindow: normalizeToolText(transcriptWindow.text).slice(0, 900),
    };
  }

  return null;
}

export function findPartialOpenerBoundaryCandidate(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary
): DeterministicBoundaryCandidate | null {
  const entry = ctx.epubEntries[targetBoundary.epubIndex];
  if (!entry || entry.id !== targetBoundary.epubNodeId) return null;
  const openerTokens = distinctiveOpenerTokens(entry);
  if (openerTokens.length === 0) return null;

  const searchRadiusSeconds = Math.max(180, Math.min(1_200, spanDurationSeconds(span) / 2));
  const searchStartMs = secondsToMs(Math.max(span.startTime, targetBoundary.expectedStartTime - searchRadiusSeconds));
  const searchEndMs = secondsToMs(Math.min(span.endTime, targetBoundary.expectedStartTime + searchRadiusSeconds));
  const candidates: DeterministicBoundaryCandidate[] = [];

  for (const utterance of transcriptUtterances(ctx)) {
    if (utterance.endMs < searchStartMs || utterance.startMs > searchEndMs) continue;
    const text = normalizeToolText(utterance.text);
    if (!hasOpenerTokenEvidence(text, openerTokens)) continue;

    const reverse = searchEpubText(ctx, {
      query: text,
      targetNodeId: entry.id,
      limit: 5,
    }).matches[0];
    if (!reverse || reverse.epubNodeId !== entry.id) continue;
    if (reverse.targetNodeDistance !== 0 || reverse.targetWordOffset === null) continue;
    if (reverse.relationToTarget !== "opener" && reverse.relationToTarget !== "near_opener") continue;

    const phraseStartWord = firstOrderedQueryTokenOffset(entry, text);
    if (phraseStartWord === null || phraseStartWord > 32) continue;
    const transcriptWindow = getTranscriptWindowFromContext(ctx, utterance.startMs, secondsToMs(45));
    candidates.push({
      startTime: msToSeconds(utterance.startMs),
      source: "partial_opener",
      phrase: text,
      phraseStartWord,
      reverseEpubRelation: phraseStartWord <= 8 ? "opener" : "near_opener",
      transcriptText: text.slice(0, 500),
      transcriptWindow: normalizeToolText(transcriptWindow.text).slice(0, 900),
      bodyMatchCount: textTokens(text).filter((token) => openerTokens.includes(token)).length,
    });
  }

  return candidates.sort((a, b) => (a.phraseStartWord ?? 999) - (b.phraseStartWord ?? 999) || Math.abs(a.startTime - targetBoundary.expectedStartTime) - Math.abs(b.startTime - targetBoundary.expectedStartTime))[0] ?? null;
}

function firstOrderedQueryTokenOffset(entry: EpubChapterEntry, query: string): number | null {
  const entryTokens = entry.words.map((word) => normalizedWordTokens(word.token || word.text)[0] || "");
  const queryTokens = normalizedWordTokens(query).filter((token) => token.length > 1);
  if (queryTokens.length === 0) return null;

  for (let offset = 0; offset < entryTokens.length; offset++) {
    if (entryTokens[offset] !== queryTokens[0]) continue;
    let matched = 0;
    let searchFrom = offset;
    let lastFound = offset;
    for (const token of queryTokens) {
      const found = entryTokens.findIndex((entryToken, entryIndex) => entryIndex >= searchFrom && entryToken === token);
      if (found < 0) continue;
      matched++;
      lastFound = found;
      searchFrom = found + 1;
    }
    const spanWords = lastFound - offset + 1;
    const maxPhraseSpan = Math.max(queryTokens.length + 8, queryTokens.length * 2);
    if (matched >= Math.min(4, queryTokens.length) && matched / queryTokens.length >= 0.5 && spanWords <= maxPhraseSpan) return offset;
  }

  return null;
}

function distinctiveOpenerTokens(entry: EpubChapterEntry): string[] {
  const tokens = textTokens(summarizeFirstBodyWords(entry, 40));
  const distinct: string[] = [];
  for (const token of tokens) {
    if (isStructuralTitleToken(token) || distinct.includes(token)) continue;
    distinct.push(token);
    if (distinct.length >= 8) break;
  }
  return distinct;
}

function hasOpenerTokenEvidence(text: string, openerTokens: string[]): boolean {
  const textTokenSet = new Set(textTokens(text));
  const matched = openerTokens.filter((token) => textTokenSet.has(token));
  return matched.length >= Math.min(3, Math.max(2, Math.ceil(openerTokens.length / 2)));
}

function openingAudioOnlyEndTime(ctx: ChapterCurationContext, span: ChapterCurationSpan): number {
  const startEntry = ctx.epubEntries[span.epubStartIndex];
  const hintedStartTime = startEntry ? ctx.chapterStartTimeHints?.[startEntry.id] : undefined;
  if (hintedStartTime !== undefined) return Math.max(span.startTime, hintedStartTime);

  const openingIntervals = (ctx.audioOnlyIntervals ?? [])
    .filter((interval) => interval.startTime <= span.startTime + 5 && interval.endTime > span.startTime)
    .sort((a, b) => b.endTime - a.endTime);
  return openingIntervals[0]?.endTime ?? span.startTime;
}

const ROMAN_NUMERALS: Record<string, number> = {
  i: 1,
  ii: 2,
  iii: 3,
  iv: 4,
  v: 5,
  vi: 6,
  vii: 7,
  viii: 8,
  ix: 9,
  x: 10,
  xi: 11,
  xii: 12,
  xiii: 13,
  xiv: 14,
  xv: 15,
  xvi: 16,
  xvii: 17,
  xviii: 18,
  xix: 19,
  xx: 20,
};

function curationTitleKey(value: string): string {
  return normalizeToolText(value).toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}

function standaloneTitleNumber(value: string): number | null {
  const key = curationTitleKey(value);
  if (/^\d+$/.test(key)) return Number(key);
  return ROMAN_NUMERALS[key] ?? null;
}

function embeddedTitleHasChapterNumber(embeddedTitle: string, chapterNumber: number): boolean {
  const key = curationTitleKey(embeddedTitle);
  return new RegExp(`(?:^|\\b)(?:chapter\\s+)?${chapterNumber}(?:\\b|$)`, "u").test(key);
}

function embeddedTitleMatchesEpubNode(entry: EpubChapterEntry, embeddedTitle: string): boolean {
  const entryKey = curationTitleKey(entry.title);
  const embeddedKey = curationTitleKey(embeddedTitle);
  if (!entryKey || !embeddedKey) return false;
  if (entryKey.length >= 3 && (entryKey === embeddedKey || embeddedKey.includes(entryKey))) return true;

  const structuralKinds = ["interlude", "part"];
  for (const kind of structuralKinds) {
    if (embeddedKey.includes(kind) !== entryKey.includes(kind)) return false;
  }

  const chapterNumber = standaloneTitleNumber(entry.title);
  if (chapterNumber !== null && embeddedTitleHasChapterNumber(embeddedTitle, chapterNumber)) return true;

  return false;
}

const CHAPTER_EVIDENCE_STOPWORDS = new Set([
  "the",
  "and",
  "but",
  "for",
  "with",
  "that",
  "this",
  "was",
  "were",
  "are",
  "you",
  "your",
  "have",
  "had",
  "not",
  "from",
  "into",
  "out",
  "chapter",
  "part",
  "book",
]);

function evidenceTokens(value: string): string[] {
  return curationTitleKey(value)
    .split(/\s+/)
    .filter((token) => token.length >= 3)
    .filter((token) => !CHAPTER_EVIDENCE_STOPWORDS.has(token))
    .filter((token) => !/^\d+$/.test(token))
    .slice(0, 32);
}

function firstDistinctiveEpubTokens(entry: EpubChapterEntry): string[] {
  const tokens: string[] = [];
  for (const word of entry.words) {
    for (const token of evidenceTokens(word.text)) {
      if (!tokens.includes(token)) tokens.push(token);
      if (tokens.length >= 10) return tokens;
    }
  }
  return tokens;
}

export function embeddedNodeBoundaryHasTranscriptEvidence(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "transcript" | "durationMs" | "audioOnlyIntervals">,
  targetBoundary: ChapterCurationTargetBoundary,
  embeddedCandidate: ChapterCurationTiming
): boolean {
  const entry = ctx.epubEntries[targetBoundary.epubIndex];
  if (!entry || entry.id !== targetBoundary.epubNodeId) return false;
  const window = getTranscriptWindowFromContext(ctx, embeddedCandidate.startMs, 60_000);
  const transcriptTokens = new Set(evidenceTokens(window.text));
  const titleTokens = evidenceTokens(embeddedCandidate.title);
  const epubTokens = firstDistinctiveEpubTokens(entry);
  const titleMatches = titleTokens.filter((token) => transcriptTokens.has(token));
  const epubMatches = epubTokens.filter((token) => transcriptTokens.has(token));
  return epubMatches.length >= 3 || (titleMatches.length >= 1 && epubMatches.length >= 2);
}

export function findEmbeddedNodeBoundaryCandidate(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "embeddedChapters">,
  targetBoundary: ChapterCurationTargetBoundary
): ChapterCurationTiming | null {
  const entry = ctx.epubEntries[targetBoundary.epubIndex];
  if (!entry || entry.id !== targetBoundary.epubNodeId) return null;
  const matches = ctx.embeddedChapters
    .filter((chapter) => embeddedTitleMatchesEpubNode(entry, chapter.title))
    .map((chapter) => ({
      chapter,
      distance: Math.abs(msToSeconds(chapter.startMs) - targetBoundary.expectedStartTime),
    }))
    .sort((a, b) => a.distance - b.distance);
  return matches[0]?.chapter ?? null;
}

async function tryDeterministicNodeBoundary(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary
): Promise<NodeBoundaryDecision | null> {
  const embeddedAssessment = assessEmbeddedAudioChaptersForCuration(ctx);
  const embeddedCandidate =
    embeddedAssessment.action === "short_circuit_candidate" || embeddedAssessment.action === "seed_boundaries"
      ? findEmbeddedNodeBoundaryCandidate(ctx, targetBoundary)
      : null;
  if (embeddedCandidate) {
    const startTime = msToSeconds(embeddedCandidate.startMs);
    const hasDeterministicEvidence = embeddedNodeBoundaryHasTranscriptEvidence(ctx, targetBoundary, embeddedCandidate);
    const isHighConfidenceEmbeddedMarker =
      embeddedAssessment.action === "short_circuit_candidate" && embeddedAssessment.confidence === "high";
    const validated = await validateNodeBoundary(
      ctx,
      {
        spanPath: span.path,
        epubNodeId: targetBoundary.epubNodeId,
        title: targetBoundary.title,
        startTime,
        evidence: `Embedded audio marker "${embeddedCandidate.title}" at ${Math.round(startTime)}s matches EPUB node "${targetBoundary.title}" and passed transcript validation.`,
        notes: isHighConfidenceEmbeddedMarker
          ? `High-confidence named embedded audio marker "${embeddedCandidate.title}" matches this EPUB node. If transcriptBefore corroborates the previous EPUB tail and boundaryWords.after resumes later inside the target EPUB node, accept the marker as an embedded-marker transcript-gap boundary.`
          : `Embedded audio marker "${embeddedCandidate.title}" matches this EPUB node.`,
      },
      { span, targetBoundary }
    );
    if (validated.accepted) {
      const judgment = await judgeChapterBoundary(ctx, span, {
        kind: "node_boundary",
        spanPath: validated.spanPath,
        epubNodeId: validated.epubNodeId,
        epubIndex: validated.epubIndex,
        title: validated.title,
        startTime: validated.startTime,
        notes: validated.notes,
        audit: validated.audit,
      });
      if (judgment?.accepted) {
        const accepted: NodeBoundaryDecision = {
          ...validated,
          notes: [
            validated.notes,
            isHighConfidenceEmbeddedMarker
              ? "Accepted by high-confidence named embedded audio marker set after judge validation."
              : hasDeterministicEvidence
                ? "Accepted by embedded audio marker plus deterministic transcript evidence after judge validation."
                : "Accepted by embedded audio marker after judge validation.",
          ]
            .filter(Boolean)
            .join(" "),
        };
        logChapterCurationEvent(ctx, {
          type: "deterministic-node-boundary-accepted",
          message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} embedded=1${
            hasDeterministicEvidence ? " transcript=1" : ""
          } judge=1 accepted=1 time=${Math.round(accepted.startTime)}s`,
          span,
          targetBoundary,
          result: accepted,
          embeddedCandidate,
          embeddedAssessment,
        });
        return accepted;
      }
      logChapterCurationEvent(ctx, {
        type: "deterministic-node-boundary-rejected",
        message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} embedded=1 judge=0`,
        span,
        targetBoundary,
        result: validated,
        judgment,
        embeddedCandidate,
        embeddedAssessment,
      });
    } else {
      logChapterCurationEvent(ctx, {
        type: "deterministic-node-boundary-rejected",
        message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} embedded=1 validation=0`,
        span,
        targetBoundary,
        result: validated,
        embeddedCandidate,
        embeddedAssessment,
      });
    }
  }

  const headingCandidate = await findSpokenHeadingBoundaryCandidate(ctx, span, targetBoundary);
  const isOpeningNode = targetBoundary.epubIndex === 0;
  const hintedOpeningStartTime = isOpeningNode ? ctx.chapterStartTimeHints?.[targetBoundary.epubNodeId] : undefined;
  if (hintedOpeningStartTime !== undefined) {
    const validated = await validateNodeBoundary(
      ctx,
      {
        spanPath: span.path,
        epubNodeId: targetBoundary.epubNodeId,
        title: targetBoundary.title,
        startTime: hintedOpeningStartTime,
        evidence:
          "Deterministic transcript-endpoint hint: endpoint scoping found early non-boilerplate transcript text inside this EPUB node, so this is the first available narrated boundary for the scoped manifestation.",
        notes: "Accepted by transcript endpoint scoping for the first scoped EPUB node.",
      },
      { span, targetBoundary }
    );
      if (validated.accepted) {
      const judgment = await judgeChapterBoundary(ctx, span, {
        kind: "node_boundary",
        spanPath: validated.spanPath,
        epubNodeId: validated.epubNodeId,
        epubIndex: validated.epubIndex,
        title: validated.title,
        startTime: validated.startTime,
        notes: validated.notes,
        audit: validated.audit,
      });
      if (judgment?.accepted) {
        const accepted: NodeBoundaryDecision = {
          ...validated,
          notes: [validated.notes, "Accepted by deterministic pre-agent endpoint hint after judge validation."].filter(Boolean).join(" "),
        };
        logChapterCurationEvent(ctx, {
          type: "deterministic-node-boundary-accepted",
          message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} endpoint_hint=1 judge=1 accepted=1 time=${Math.round(
            accepted.startTime
          )}s`,
          span,
          targetBoundary,
          result: accepted,
        });
        return accepted;
      }
      logChapterCurationEvent(ctx, {
        type: "deterministic-node-boundary-rejected",
        message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} endpoint_hint=1 judge=0`,
        span,
        targetBoundary,
        result: validated,
        judgment,
      });
    }
    logChapterCurationEvent(ctx, {
      type: "deterministic-node-boundary-rejected",
      message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} endpoint_hint=1 validation=0`,
      span,
      targetBoundary,
      result: validated,
    });
  }

  if (headingCandidate) {
    const acceptedHeading = await validateDeterministicNodeBoundaryCandidate(ctx, span, targetBoundary, headingCandidate, {
      eventSuffix: "heading=1",
    });
    if (acceptedHeading) return acceptedHeading;
  }

  const research = await researchEpubBoundary(ctx, {
    epubNodeId: targetBoundary.epubNodeId,
    expectedTime: targetBoundary.expectedStartTime,
    scope: { startTime: span.startTime, endTime: span.endTime },
    searchRadiusSeconds: Math.max(180, Math.min(1_200, spanDurationSeconds(span) / 2)),
    phraseLimit: 8,
    hitLimitPerPhrase: 5,
  });
  const candidate =
    headingCandidate ??
    chooseResearchBoundaryCandidate(research, span, { allowOpeningNearOpenerFallback: isOpeningNode, includeFallback: false }) ??
    findOpeningInteriorStartCandidate(ctx, span, targetBoundary) ??
    findPartialOpenerBoundaryCandidate(ctx, span, targetBoundary) ??
    chooseSupportingContextBacktrackCandidate(ctx, research, span) ??
    chooseResearchBoundaryCandidate(research, span, { allowOpeningNearOpenerFallback: isOpeningNode });
  logChapterCurationEvent(ctx, {
    type: "deterministic-node-boundary-research",
    message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId} candidates=${research?.bestCandidates.length ?? 0}${
      candidate ? ` selected=${Math.round(candidate.startTime)}s` : ""
    }`,
    span,
    targetBoundary,
    candidate,
    research: research
      ? {
          epubNodeId: research.epubNodeId,
          title: research.title,
          expectedStartTime: research.expectedStartTime,
          searchScope: research.searchScope,
          bestCandidates: research.bestCandidates.slice(0, 3),
        }
      : null,
    headingCandidate,
  });
  if (!candidate) return null;

  return validateDeterministicNodeBoundaryCandidate(ctx, span, targetBoundary, candidate, {
    eventSuffix: null,
  });
}

async function validateDeterministicNodeBoundaryCandidate(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary,
  candidate: DeterministicBoundaryCandidate,
  options: { eventSuffix: string | null }
): Promise<NodeBoundaryDecision | null> {
  const openingFallback = targetBoundary.epubIndex === 0 && candidate.source === "near_opener_fallback";
  const startTime = openingFallback ? openingAudioOnlyEndTime(ctx, span) : candidate.startTime;
  const validated = await validateNodeBoundary(
    ctx,
    {
      spanPath: span.path,
      epubNodeId: targetBoundary.epubNodeId,
      title: targetBoundary.title,
      startTime,
      evidence: [
        openingFallback
          ? "Deterministic opening-node fallback: the true opener is absent from transcript evidence, but the earliest clear same-node near-opener proves the first audible EPUB node."
          : candidate.source === "spoken_heading"
          ? "Deterministic spoken-heading candidate: transcript contains the target heading/title cue followed by target body opener text."
          : candidate.source === "partial_opener"
            ? "Deterministic partial-opener candidate: early transcript text reverse-matches the target EPUB opener despite ASR omissions or drift."
          : candidate.source === "supporting_context_backtrack"
            ? "Deterministic supporting-context backtrack: research found target-node prose later in the opener, then backtracked to the first utterance that reverse-matches this EPUB node."
            : candidate.source === "opening_interior_start"
              ? "Deterministic opening interior-start candidate: the first clean early transcript utterance reverse-matches the first EPUB node, so this is the first available narrated boundary."
            : candidate.source === "near_opener_fallback"
              ? "Deterministic near-opener fallback: the printed heading/opening words may be omitted in ASR, but research found the earliest target near-opener body phrase."
              : "Deterministic strong opener candidate from researchEpubBoundary.",
        `Anchor phrase "${candidate.phrase}"${candidate.phraseStartWord === null ? "" : ` starts at word ${candidate.phraseStartWord}`} and reverse EPUB relation is ${candidate.reverseEpubRelation}.`,
        `Transcript text: ${candidate.transcriptText}`,
      ].join(" "),
      notes: openingFallback
        ? `Accepted as the first audible EPUB node at ${Math.round(startTime)}s; supporting near-opener phrase "${candidate.phrase}" starts at ${Math.round(candidate.startTime)}s because earlier opener text is absent from the transcript.`
        : "Accepted by deterministic pre-agent node-boundary research.",
    },
    {
      span,
      targetBoundary,
      candidates: [
        {
          startTime: candidate.startTime,
          endTime: candidate.startTime,
          text: `Deterministic candidate source=${candidate.source}; reverseEpubRelation=${candidate.reverseEpubRelation}; phrase="${candidate.phrase}"; transcript="${candidate.transcriptText}"`,
          afterText: candidate.transcriptWindow ?? "",
          quality: candidate.reverseEpubRelation === "opener" || candidate.source === "opening_interior_start" ? "strong" : "medium",
        },
      ],
    }
  );
  if (!validated.accepted) {
    logChapterCurationEvent(ctx, {
      type: "deterministic-node-boundary-rejected",
      message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId}${
        options.eventSuffix ? ` ${options.eventSuffix}` : ""
      } validation=0`,
      span,
      targetBoundary,
      result: validated,
      candidate,
    });
    return null;
  }

  const judgment = await judgeChapterBoundary(ctx, span, {
    kind: "node_boundary",
    spanPath: validated.spanPath,
    epubNodeId: validated.epubNodeId,
    epubIndex: validated.epubIndex,
    title: validated.title,
    startTime: validated.startTime,
    notes: validated.notes,
    audit: validated.audit,
  });
  if (!judgment?.accepted) {
    logChapterCurationEvent(ctx, {
      type: "deterministic-node-boundary-rejected",
      message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId}${
        options.eventSuffix ? ` ${options.eventSuffix}` : ""
      } judge=0`,
      span,
      targetBoundary,
      result: validated,
      judgment,
      candidate,
    });
    return null;
  }

  const accepted: NodeBoundaryDecision = {
    ...validated,
    notes: [validated.notes, "Accepted by deterministic pre-agent node-boundary candidate after judge validation."].filter(Boolean).join(" "),
  };
  logChapterCurationEvent(ctx, {
    type: "deterministic-node-boundary-accepted",
    message: `deterministic node boundary span=${span.path} target=${targetBoundary.epubNodeId}${
      options.eventSuffix ? ` ${options.eventSuffix}` : ""
    } judge=1 accepted=1 time=${Math.round(accepted.startTime)}s`,
    span,
    targetBoundary,
    result: accepted,
    candidate,
  });
  return accepted;
}

export async function runAgenticChapterCuration(ctx: ChapterCurationContext): Promise<SubmitChapterPlanResult | null> {
  const detailed = await runAgenticChapterCurationDetailed(ctx);
  return detailed.result;
}

export async function runNodeParallelAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) {
    logChapterCurationEvent(ctx, {
      type: "node-parallel-run-skipped",
      message: "node parallel run skipped=no_api_key",
    });
    return { result: null, finalOutput: null, newItems: [], rawResponses: [], nodeBoundaryReports: [], nodeBoundaryTraces: [] };
  }
  ensureTracingInitialized(apiKey);
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    const audibleNodeSelection = await classifyAudibleEpubNodes(ctx, runner);
    const audibleCurationCtx = applyAudibleEpubNodeSelection(ctx, audibleNodeSelection);
    const embeddedCurationCtx = applyEmbeddedAudioChapterNodeScope({
      ...ctx,
      audioOnlyIntervals: audibleCurationCtx.audioOnlyIntervals,
    });
    const endpointCurationCtx =
      embeddedCurationCtx.epubEntries.length !== ctx.epubEntries.length ? embeddedCurationCtx : applyTranscriptEndpointEpubNodeScope(audibleCurationCtx);
    const curationCtx = endpointCurationCtx.epubEntries.length !== ctx.epubEntries.length ? endpointCurationCtx : audibleCurationCtx;
    if (audibleCurationCtx.epubEntries.length !== ctx.epubEntries.length) {
      logChapterCurationEvent(ctx, {
        type: "audible-epub-node-filter-applied",
        message: `audible epub node filter applied original=${ctx.epubEntries.length} curated=${audibleCurationCtx.epubEntries.length}`,
        originalEpubEntries: ctx.epubEntries.length,
        curatedEpubEntries: audibleCurationCtx.epubEntries.length,
        selectedNodeIds: audibleCurationCtx.epubEntries.map((entry) => entry.id),
        excludedNodes: audibleNodeSelection?.excludedNodes ?? [],
      });
    }
    if (curationCtx.epubEntries.length !== ctx.epubEntries.length && curationCtx === embeddedCurationCtx) {
      logChapterCurationEvent(curationCtx, {
        type: "embedded-audio-node-scope-applied",
        message: `embedded audio node scope applied original=${ctx.epubEntries.length} curated=${curationCtx.epubEntries.length}`,
        originalEpubEntries: ctx.epubEntries.length,
        curatedEpubEntries: curationCtx.epubEntries.length,
        selectedNodeIds: curationCtx.epubEntries.map((entry) => entry.id),
        diagnostics: getEmbeddedAudioChapters(ctx).diagnostics,
      });
    }
    if (curationCtx.epubEntries.length !== audibleCurationCtx.epubEntries.length && curationCtx === endpointCurationCtx && curationCtx !== embeddedCurationCtx) {
      logChapterCurationEvent(curationCtx, {
        type: "transcript-endpoint-node-scope-applied",
        message: `transcript endpoint node scope applied original=${audibleCurationCtx.epubEntries.length} curated=${curationCtx.epubEntries.length}`,
        originalEpubEntries: audibleCurationCtx.epubEntries.length,
        curatedEpubEntries: curationCtx.epubEntries.length,
        selectedNodeIds: curationCtx.epubEntries.map((entry) => entry.id),
      });
    }
    if ((curationCtx.audioOnlyIntervals ?? []).length > 0) {
      logChapterCurationEvent(ctx, {
        type: "audio-only-intervals-applied",
        message: `audio only intervals applied count=${curationCtx.audioOnlyIntervals?.length ?? 0}`,
        audioOnlyIntervals: curationCtx.audioOnlyIntervals ?? [],
      });
    }

    const preflightDiagnostic = buildNodeBoundaryPreflightDiagnostic(curationCtx);
    if (preflightDiagnostic.kind === "preflight_mismatch") {
      logChapterCurationEvent(curationCtx, {
        type: "node-parallel-preflight-rejected",
        message: `node parallel preflight accepted=0 reason=epub_transcript_opening_mismatch best_overlap=${preflightDiagnostic.bestEarlyCuratedNodeOpeningOverlap ?? "unknown"}`,
        nodeBoundaryFailureDiagnostic: preflightDiagnostic,
      });
      return {
        result: null,
        finalOutput: null,
        newItems: [],
        rawResponses: [],
        nodeBoundaryReports: [],
        nodeBoundaryTraces: [],
        nodeBoundaryFailureDiagnostic: preflightDiagnostic,
      };
    }

    const embeddedAssessment = assessEmbeddedAudioChaptersForCuration(curationCtx);
    const rootSpan = createRootCurationSpan(curationCtx);
    const nodeBoundaryReports: NodeBoundaryCurationReport[] = [];
    const nodeBoundaryTraces: ChapterCurationAgentTrace[] = [];
    const nodeBoundaryMaxConcurrency = Math.max(1, Number(process.env.PODIBLE_CHAPTER_NODE_CONCURRENCY ?? 12));
    const nodeBoundaryMaxTurns = Math.max(4, Number(process.env.PODIBLE_CHAPTER_NODE_MAX_TURNS ?? 64));
    const minNodeBoundaryCoverage = Math.max(0, Math.min(1, Number(process.env.PODIBLE_CHAPTER_NODE_MIN_COVERAGE ?? 0.9)));
    logChapterCurationEvent(curationCtx, {
      type: "node-parallel-run-start",
      message: `node parallel run start=1 nodes=${curationCtx.epubEntries.length}`,
      model: curationCtx.settings.agents.model,
      curatorModel: curationCtx.debugCuratorModel?.trim() || curationCtx.settings.agents.model,
      judgeModel: curationCtx.debugJudgeModel?.trim() || curationCtx.settings.agents.model,
      maxNodeConcurrency: nodeBoundaryMaxConcurrency,
      maxNodeTurns: nodeBoundaryMaxTurns,
      minNodeBoundaryCoverage,
      durationSeconds: curationCtx.durationMs / 1000,
      epubEntries: curationCtx.epubEntries.length,
      originalEpubEntries: ctx.epubEntries.length,
      audioOnlyIntervals: curationCtx.audioOnlyIntervals?.length ?? 0,
      transcriptUtterances: curationCtx.transcript.utterances?.length ?? 0,
      embeddedChapters: curationCtx.embeddedChapters.length,
      embeddedAssessment,
    });

    const nodeChapters = await resolveNodeBoundaryChapters(
      curationCtx,
      async (targetBoundary): Promise<NodeBoundaryDecision | null> => {
        const startedAt = Date.now();
        logChapterCurationEvent(curationCtx, {
          type: "node-boundary-start",
          message: `node boundary epub=${targetBoundary.epubNodeId} index=${targetBoundary.epubIndex} expected=${Math.round(targetBoundary.expectedStartTime)}s start=1`,
          span: rootSpan,
          targetBoundary,
        });
        const primaryCuratorModel = curationCtx.debugCuratorModel?.trim() || curationCtx.settings.agents.model;
        const configuredCuratorModel = curationCtx.settings.agents.model;
        const deterministicDecision = await tryDeterministicNodeBoundary(curationCtx, rootSpan, targetBoundary);
        if (deterministicDecision) {
          const elapsedMs = Date.now() - startedAt;
          logChapterCurationEvent(curationCtx, {
            type: "node-boundary-result",
            message: `node boundary epub=${targetBoundary.epubNodeId} elapsed_ms=${elapsedMs} deterministic=1 accepted=1 time=${Math.round(deterministicDecision.startTime)}s`,
            span: rootSpan,
            targetBoundary,
            elapsedMs,
            decision: deterministicDecision,
          });
          return deterministicDecision;
        }
        const targetEntry = curationCtx.epubEntries[targetBoundary.epubIndex];
        if (targetEntry && isShortHeadingOnlyEntry(targetEntry)) {
          const elapsedMs = Date.now() - startedAt;
          logChapterCurationEvent(curationCtx, {
            type: "node-boundary-short-heading-deferred",
            message: `node boundary epub=${targetBoundary.epubNodeId} elapsed_ms=${elapsedMs} short_heading=1 deferred_to_recovery_or_skip=1`,
            span: rootSpan,
            targetBoundary,
            elapsedMs,
          });
          return null;
        }
        const attemptModels = Array.from(new Set([primaryCuratorModel, configuredCuratorModel].filter(Boolean)));
        const attempts = attemptModels.map((model, index) => ({ model, delayMs: index === 0 ? 0 : 5_000 }));
        for (let attempt = 0; attempt < attempts.length; attempt++) {
          const attemptConfig = attempts[attempt]!;
          if (attemptConfig.delayMs > 0) await sleep(attemptConfig.delayMs);
          try {
            const result = await runner.run(
              createNodeBoundaryCuratorAgent(curationCtx, rootSpan, targetBoundary, attemptConfig.model),
              nodeBoundaryPrompt(curationCtx, rootSpan, targetBoundary),
              {
                maxTurns: nodeBoundaryMaxTurns,
                toolExecution: { maxFunctionToolConcurrency: 1 },
              }
            );
            logAgentUsageEvent(curationCtx, {
              role: "node-boundary-curator",
              model: attemptConfig.model,
              rawResponses: result.rawResponses as unknown[],
              span: rootSpan,
            });
            const decision = parseNodeBoundaryOutput(result.finalOutput);
            const elapsedMs = Date.now() - startedAt;
            const tracePayload = {
              span: rootSpan,
              targetBoundary,
              attempt: attempt + 1,
              elapsedMs,
              finalOutput: result.finalOutput,
              newItems: result.newItems as unknown[],
              rawResponses: result.rawResponses as unknown[],
            };
            const tracePath = writeChapterCurationTrace(curationCtx, `node-${targetBoundary.epubIndex}-${targetBoundary.epubNodeId}-attempt-${attempt + 1}`, tracePayload);
            nodeBoundaryTraces.push({
              path: rootSpan.path,
              depth: rootSpan.depth,
              targetBoundary,
              finalOutput: result.finalOutput,
              newItems: result.newItems as unknown[],
              rawResponses: result.rawResponses as unknown[],
            });
            logChapterCurationEvent(curationCtx, {
              type: "node-boundary-result",
              message: `node boundary epub=${targetBoundary.epubNodeId} elapsed_ms=${elapsedMs} attempt=${attempt + 1} accepted=${decision ? 1 : 0}${
                decision ? ` time=${Math.round(decision.startTime)}s` : ""
              } model=${attemptConfig.model}`,
              span: rootSpan,
              targetBoundary,
              attempt: attempt + 1,
              elapsedMs,
              model: attemptConfig.model,
              decision,
              tracePath,
            });
            if (decision) return decision;
            if (attempt < attempts.length - 1) continue;
            return null;
          } catch (error) {
            const elapsedMs = Date.now() - startedAt;
            const serializedError = serializeAgentError(error);
            logAgentUsageEvent(curationCtx, {
              role: "node-boundary-curator",
              model: attemptConfig.model,
              serializedError,
              span: rootSpan,
            });
            const tracePath = writeChapterCurationTrace(curationCtx, `node-${targetBoundary.epubIndex}-${targetBoundary.epubNodeId}-attempt-${attempt + 1}-error`, {
              span: rootSpan,
              targetBoundary,
              attempt: attempt + 1,
              elapsedMs,
              error: serializedError,
            });
            nodeBoundaryTraces.push({
              path: rootSpan.path,
              depth: rootSpan.depth,
              targetBoundary,
              finalOutput: null,
              newItems: [],
              rawResponses: [],
              error: serializedError,
            });
            logChapterCurationEvent(curationCtx, {
              type: "node-boundary-error",
              message: `node boundary epub=${targetBoundary.epubNodeId} elapsed_ms=${elapsedMs} attempt=${attempt + 1} model=${attemptConfig.model} error=${JSON.stringify((error as Error).message)}`,
              span: rootSpan,
              targetBoundary,
              attempt: attempt + 1,
              elapsedMs,
              model: attemptConfig.model,
              retryable: retryableAgentError(error) || maxTurnsAgentError(error) || nodeBoundaryRejectedLimitError(error),
              tracePath,
              error: serializedError,
            });
            if (attempt < attempts.length - 1 && (retryableAgentError(error) || maxTurnsAgentError(error) || nodeBoundaryRejectedLimitError(error))) continue;
            return null;
          }
        }
        return null;
      },
      { maxConcurrency: nodeBoundaryMaxConcurrency, reports: nodeBoundaryReports }
    );

    if (nodeChapters && nodeChapters.length > 0) {
      const resolvedReports = nodeBoundaryReports.filter((report) => report.outcome === "accepted" || report.outcome === "dropped" || report.outcome === "skipped").length;
      const failedReports = nodeBoundaryReports.filter((report) => report.outcome === "failed").length;
      const coverage = nodeBoundaryReports.length === 0 ? 0 : resolvedReports / nodeBoundaryReports.length;
      if (coverage < minNodeBoundaryCoverage) {
        const nodeBoundaryFailureDiagnostic = buildNodeBoundaryFailureDiagnostic(curationCtx, nodeBoundaryReports);
        logChapterCurationEvent(curationCtx, {
          type: "node-parallel-merge-rejected",
          message: `node parallel merge accepted=0 reason=low_coverage chapters=${nodeChapters.length} coverage=${coverage.toFixed(3)} failed=${failedReports}`,
          chapters: nodeChapters.length,
          coverage,
          minNodeBoundaryCoverage,
          resolvedReports,
          failedReports,
          nodeBoundaryReports,
          nodeBoundaryFailureDiagnostic,
        });
        return {
          result: null,
          finalOutput: null,
          newItems: [],
          rawResponses: [],
          nodeBoundaryReports,
          nodeBoundaryTraces,
          nodeBoundaryFailureDiagnostic,
        };
      }
      logChapterCurationEvent(curationCtx, {
        type: "node-parallel-merge-start",
        message: `node parallel merge chapters=${nodeChapters.length} validate=structural`,
        chapters: nodeChapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(nodeChapters, 80),
        nodeBoundaryReports,
      });
      const nodeResult = submitChapterPlan(curationCtx, {
        manifestationId: curationCtx.manifestation.id,
        strategy: "Node-parallel chapter boundary curation",
        chapters: nodeChapters,
        notes: "Merged from one independently curated boundary task per audible EPUB node after the non-narrated EPUB-node pre-pass.",
      });
      if (nodeResult.accepted) {
        logChapterCurationEvent(curationCtx, {
          type: "node-parallel-merge-accepted",
          message: `node parallel merge accepted=1 chapters=${nodeResult.chapters.length}`,
          chapters: nodeResult.chapters.length,
          result: nodeResult,
          nodeBoundaryReports,
        });
        return {
          result: nodeResult,
          finalOutput: nodeResult,
          newItems: [],
          rawResponses: [],
          nodeBoundaryReports,
          nodeBoundaryTraces,
        };
      }
      logChapterCurationEvent(curationCtx, {
        type: "node-parallel-merge-rejected",
        message: `node parallel merge accepted=0 chapters=${nodeChapters.length} errors=${JSON.stringify(nodeResult.errors.slice(0, 5))}`,
        chapters: nodeChapters.length,
        errors: nodeResult.errors,
        warnings: nodeResult.warnings,
        audit: nodeResult.audit,
        nodeBoundaryReports,
      });
    } else {
      const nodeBoundaryFailureDiagnostic = buildNodeBoundaryFailureDiagnostic(curationCtx, nodeBoundaryReports);
      logChapterCurationEvent(curationCtx, {
        type: "node-parallel-result-null",
        message: "node parallel result=null",
        nodeBoundaryReports,
        nodeBoundaryFailureDiagnostic,
      });
      return {
        result: null,
        finalOutput: null,
        newItems: [],
        rawResponses: [],
        nodeBoundaryReports,
        nodeBoundaryTraces,
        nodeBoundaryFailureDiagnostic,
      };
    }

    return {
      result: null,
      finalOutput: null,
      newItems: [],
      rawResponses: [],
      nodeBoundaryReports,
      nodeBoundaryTraces,
    };
  } finally {
    await provider.close().catch(() => undefined);
  }
}

export async function runAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  return runNodeParallelAgenticChapterCurationDetailed(ctx);
}
