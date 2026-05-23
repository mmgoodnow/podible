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
} from "./chapter-curation-tools";
import {
  type AudibleEpubNodeSelection,
  type ChapterBoundaryJudgeProposal,
  type RecursiveCurationReport,
  type RecursiveSpanDecision,
  type RecursiveSpanTrace,
  type SubmitFulcrumJudgmentResult,
  type SubmitFulcrumSplitResult,
  type SubmitChapterPlanResult,
  type SubmittedChapter,
  applyAudibleEpubNodeSelection,
  audibleEpubNodeSelectionToolUseBehavior,
  automaticLeafChapters,
  createRootCurationSpan,
  fulcrumJudgeToolUseBehavior,
  rankTargetBoundaries,
  recursiveSpanToolUseBehavior,
  resolveRecursiveChapterSpans,
  spanDurationSeconds,
  spanInternalBoundaryCount,
  submitChapterPlan,
  submitFulcrumJudgmentSchema,
  audibleEpubNodeSelectionSchema,
  summarizeSubmittedChapterObjects,
  validateFulcrumSplit,
  parseSpanDecisionOutput,
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
  recursiveReports?: RecursiveCurationReport[];
  recursiveSpanTraces?: RecursiveSpanTrace[];
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
    if (!structural || !numberToken) continue;
    const numeric = /^\d+$/.test(numberToken) ? Number(numberToken) : romanNumeralValue(numberToken);
    const word = numeric === null ? null : titleNumberWord(numeric);
    if (word) variants.add(`${structural} ${word}`);
    if (numeric !== null) variants.add(`${structural} ${numeric}`);
  }
  return [...variants].filter((variant) => variant.split(/\s+/).length >= 2);
}

function spanScope(span: ChapterCurationSpan, inputScope?: { startTime?: number; endTime?: number }): { startTime?: number; endTime?: number } {
  return {
    startTime: Math.max(span.startTime, inputScope?.startTime ?? span.startTime),
    endTime: Math.min(span.endTime, inputScope?.endTime ?? span.endTime),
  };
}

function targetBoundaryPromptContext(ctx: ChapterCurationContext, span: ChapterCurationSpan, targetBoundary: ChapterCurationTargetBoundary): Record<string, unknown> {
  const targetEntry = ctx.epubEntries[targetBoundary.epubIndex] ?? null;
  const previousEntry = ctx.epubEntries[targetBoundary.epubIndex - 1] ?? null;
  const targetText = getEpubNodeText(ctx, { epubNodeId: targetBoundary.epubNodeId, startWord: 0, wordCount: 90 });
  return {
    targetBoundary,
    expectedStartTime: targetBoundary.expectedStartTime,
    span: {
      path: span.path,
      timeSeconds: { start: span.startTime, end: span.endTime },
      epubIndexes: { start: span.epubStartIndex, end: span.epubEndIndex },
    },
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
  };
}

function spanPrompt(ctx: ChapterCurationContext, span: ChapterCurationSpan, targetBoundary: ChapterCurationTargetBoundary): string {
  const entries = ctx.epubEntries.slice(span.epubStartIndex, span.epubEndIndex + 1);
  const spanAudioOnlyIntervals = (ctx.audioOnlyIntervals ?? []).filter((interval) => interval.endTime >= span.startTime && interval.startTime <= span.endTime);
  const inheritedBoundaryInstructions = span.startBoundary
    ? [
        `This span starts at an already accepted parent split: ${span.startBoundary.title} (${span.startBoundary.epubNodeId}) at ${span.startBoundary.startTime}s.`,
        "Treat that inherited first boundary as already proven; spend research effort only on the assigned target boundary.",
      ]
    : [];
  const boundaryWorkflow = [
        "Workflow:",
        "1. Your only goal is to find where the assigned target EPUB node begins in the audio. Do not switch to a different node, even after rejection.",
        "2. The targetBoundaryContext below already contains opener text for the target node. Do not call getEpubStructure.",
        "3. Call researchEpubBoundary first. It searches for the assigned node's opener phrases in the transcript and reverse-checks candidates against the EPUB.",
        "4. If researchEpubBoundary finds no opener/near_opener candidate, call getEpubNodeText and try shorter or different opener phrases from the same node. Avoid generic words, chapter numbers, standalone names, or repeated formulaic text.",
        "5. Estimate where the node likely falls from its word-position ratio in the span, then search near there with rgSearchTranscript. If a phrase misses, try a different phrase from the same node; do not switch to a guessed timestamp.",
        "6. Inspect the best candidate with getTranscriptWindow. Use radiusSeconds=45 when context is ambiguous or after a rejection. The proposed start must be the first matched opener word or the silence immediately before it — not the start of a broad search window.",
        "7. If the transcript context looks like it precedes the target or is interior prose, call searchEpubText to classify it. Trust relationToTarget: opener/near_opener is valid; interior means do not use that timestamp; pre_target means search later.",
        "8. submitBoundarySplit is a final evidence-backed claim, not a probe. It asserts that startTime is where the assigned EPUB node begins — not an arbitrary sentence or later distinctive phrase inside it.",
        "9. Call submitBoundarySplit only once you can prove the target opener begins at or immediately after the proposed timestamp. Put that proof in the evidence/notes fields.",
        "10. If you do not have that proof yet, keep researching. After a rejection, search a different opener phrase from the same node before trying again.",
      ];
  return [
    `Curate chapter markers for span ${span.path} of "${ctx.book.title}" by ${ctx.book.author}.`,
    `manifestationId: ${ctx.manifestation.id}`,
    `spanPath: ${span.path}`,
    `spanDepth: ${span.depth}`,
    `spanTimeSeconds: ${span.startTime}..${span.endTime}`,
    `spanEpubIndexes: ${span.epubStartIndex}..${span.epubEndIndex}`,
    `spanEpubNodeCount: ${entries.length}`,
    `targetBoundary: ${JSON.stringify(targetBoundary)}`,
    `targetBoundaryContext: ${JSON.stringify(targetBoundaryPromptContext(ctx, span, targetBoundary))}`,
    `Your singular goal is to prove where ${targetBoundary.epubNodeId} (${targetBoundary.title}) begins in the audio. submitBoundarySplit only needs the timestamp and evidence notes.`,
    spanAudioOnlyIntervals.length > 0
      ? `audioOnlyIntervalsInSpan: ${JSON.stringify(spanAudioOnlyIntervals)}`
      : "audioOnlyIntervalsInSpan: []",
    "You must call submitBoundarySplit with a validated start timestamp for the assigned targetBoundary.",
    spanAudioOnlyIntervals.length > 0
      ? "Some transcript time ranges in this span are audio-only with no EPUB node. Do not place an EPUB chapter start inside those intervals unless opener evidence proves the EPUB text actually begins there."
      : "",
    ...inheritedBoundaryInstructions,
    ...boundaryWorkflow,
    "All times are seconds.",
  ].filter(Boolean).join("\n");
}

function createChapterBoundaryJudgeAgent(ctx: ChapterCurationContext): Agent {
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
      "Check transcriptPrecision. If it is word, the before/after split is exact and should be treated as precise. If it is utterance, a real boundary may fall inside a displayed utterance — accept the utterance start if it contains previous tail then target head.",
      "If boundaryWords.containing is non-empty, the timestamp falls mid-word. Prefer a nearestCleanBoundaryTimes value unless the containing word is itself the first opener word.",
      "Transcript before the timestamp matching the previous EPUB node is positive evidence — not a sign the target opener is interior.",
      "Accept when transcriptBefore plausibly matches the previous EPUB tail and transcriptAfter begins with distinctive target body opener prose, even if the printed heading is absent.",
      "Reject when target body opener evidence is absent, offset, generic, pre-target, or only an interior match.",
      "Do not suggest alternate timestamps or nodes. Do not invent a chapter plan. Judge only this proposed boundary.",
      "Describe problems using evidence terms — opener_evidence_at_timestamp, opener_evidence_offset_in_window, window_starts_before_opener_evidence, tool_classified_interior_match, generic_or_weak_overlap, submitted_evidence_insufficient — not narrative terms like 'mid-scene'.",
      "You must call submitBoundaryJudgment.",
    ].join("\n"),
    tools: [
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
    const result = await runner.run(createChapterBoundaryJudgeAgent(ctx), chapterBoundaryJudgePrompt(ctx, span, proposal), {
      maxTurns: 4,
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

export function createRecursiveSpanCuratorAgent(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary,
  modelOverride?: string
): Agent {
  let invalidFulcrums = 0;
  const rejectedFulcrums = new Set<string>();
  let rejectedFulcrumRequiresEvidence = false;
  let evidenceCallsSinceRejectedFulcrum = 0;
  let transcriptSearchesSinceFulcrumSubmit = 0;

  function markEvidenceToolUsed(): void {
    if (rejectedFulcrumRequiresEvidence) evidenceCallsSinceRejectedFulcrum++;
  }

  function markTranscriptSearchToolUsed(): void {
    markEvidenceToolUsed();
    transcriptSearchesSinceFulcrumSubmit++;
  }

  function rejectedFulcrumWithoutEvidence(): SubmitFulcrumSplitResult {
    return {
      accepted: false,
      kind: "split",
      errors: ["submitBoundarySplit was called again without gathering new transcript evidence after the previous rejection."],
      warnings: [],
      audit: null,
      instruction:
        "Search for a different opener phrase with rgSearchTranscript or fuzzySearchTranscript, then inspect the match with getTranscriptWindow before submitting again.",
    };
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

  return new Agent({
    name: "SectionChapterCurator",
    model: modelOverride?.trim() || ctx.debugCuratorModel?.trim() || ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You find the start timestamp of one assigned EPUB chapter node in an audiobook transcript.",
      `Your only goal is to locate where ${targetBoundary.epubNodeId} (${targetBoundary.title}) begins in the audio. Do not switch to a different node.`,
      "Workflow:",
      "1. Use the targetBoundaryContext in the prompt as your starting EPUB opener context. Do not call a different EPUB node.",
      "2. Call researchEpubBoundary. It searches only the assigned node: opener phrases, nearby transcript hits, windows, and opener/near_opener reverse checks.",
      "3. If researchEpubBoundary returns a strong opener/near_opener candidate, use that timestamp (or the silence just before the first opener word). If not, call getEpubNodeText and try different opener phrases from the same node.",
      "4. Estimate where the node likely falls from its position in the span. Search nearby with rgSearchTranscript. If a phrase misses, try a shorter or different phrase from the same node — do not switch to a guessed timestamp.",
      "5. Inspect the best candidate with getTranscriptWindow. Use radiusSeconds=45 when context is ambiguous or after a rejection. The proposed start must be the first matched opener word or the silence just before it — not the beginning of a broad search window.",
      "6. If the transcript context looks like pre-target or interior prose, call searchEpubText. Trust relationToTarget: opener/near_opener is valid evidence; interior means do not use that timestamp; pre_target means search later.",
      "7. submitBoundarySplit is a final evidence-backed claim. It asserts that startTime is where the assigned EPUB node begins — not an arbitrary sentence or a later distinctive phrase inside it.",
      "8. Call submitBoundarySplit only once you can prove the target opener begins at or immediately after the proposed timestamp. Put that proof in the evidence/notes fields.",
      "9. Do not submit guessed timestamps or estimates from EPUB word-position alone. A transcript search result is required before submitting.",
      "10. After any rejection, run a fresh transcript search before trying again. Do not resubmit the same timestamp.",
      "All times are seconds.",
    ]
      .filter(Boolean)
      .join("\n"),
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
            const entryIndex = ctx.epubEntries.findIndex((entry) => entry.id === targetBoundary.epubNodeId);
            if (entryIndex < span.epubStartIndex || entryIndex > span.epubEndIndex) {
              return {
                error: `EPUB node ${targetBoundary.epubNodeId} is not eligible for this span.`,
                spanPath: span.path,
              };
            }
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
            const entryIndex = ctx.epubEntries.findIndex((entry) => entry.id === targetBoundary.epubNodeId);
            if (entryIndex < span.epubStartIndex || entryIndex > span.epubEndIndex) {
              return {
                error: `EPUB node ${targetBoundary.epubNodeId} is not eligible for this span.`,
                spanPath: span.path,
              };
            }
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
        description: "Search transcript utterances with ripgrep inside this span. Time scopes are seconds.",
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
        description: "Fuzzy-search transcript utterances inside this span. Time scopes are seconds.",
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
        description: "Return transcript utterances around a timestamp inside this span. startTime is seconds.",
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
        name: "submitBoundarySplit",
        description: "Submit the start timestamp for the assigned EPUB node once you have opener evidence.",
        parameters: assignedSubmitFulcrumSplitSchema,
        strict: true,
        execute: async (input) => {
          const splitInput = {
            spanPath: span.path,
            epubNodeId: targetBoundary.epubNodeId,
            title: targetBoundary.title,
            startTime: input.startTime,
            evidence: input.evidence,
            notes: input.notes,
          };
          return runToolWithEvents("submitBoundarySplit", splitInput, async () => {
            if (rejectedFulcrumRequiresEvidence && evidenceCallsSinceRejectedFulcrum === 0) return rejectedFulcrumWithoutEvidence();
            if (transcriptSearchesSinceFulcrumSubmit === 0) {
              return {
                accepted: false,
                kind: "split",
                errors: ["A transcript search result is required before submitting a boundary for a large span."],
                warnings: [],
                audit: null,
                instruction:
                  "Call getEpubNodeText, search a distinctive opener phrase with rgSearchTranscript or fuzzySearchTranscript, inspect the match with getTranscriptWindow, then submit.",
              } satisfies SubmitFulcrumSplitResult;
            }
            transcriptSearchesSinceFulcrumSubmit = 0;
            const rejectedKey = `${splitInput.epubNodeId}:${Math.round(splitInput.startTime)}`;
            if (rejectedFulcrums.has(rejectedKey)) {
              rejectedFulcrumRequiresEvidence = true;
              evidenceCallsSinceRejectedFulcrum = 0;
              return {
                accepted: false,
                kind: "split",
                errors: [`${splitInput.epubNodeId} near ${Math.round(splitInput.startTime)}s was already rejected for this span.`],
                warnings: [],
                audit: null,
                instruction: "Stay on this EPUB node. Call getEpubNodeText for an earlier or different word window, search a different opener phrase, and submit a materially different timestamp with stronger evidence.",
              } satisfies SubmitFulcrumSplitResult;
            }
            const result = await validateFulcrumSplit(ctx, span, splitInput, { targetBoundary });
            if (!result.accepted) {
              invalidFulcrums++;
              rejectedFulcrums.add(rejectedKey);
              rejectedFulcrumRequiresEvidence = true;
              evidenceCallsSinceRejectedFulcrum = 0;
            }
            if (result.accepted) {
              const judgment = await judgeFulcrumSplit(ctx, span, result);
              if (judgment && !judgment.accepted) {
                invalidFulcrums++;
                rejectedFulcrums.add(rejectedKey);
                rejectedFulcrumRequiresEvidence = true;
                evidenceCallsSinceRejectedFulcrum = 0;
                return {
                  accepted: false,
                  kind: "split",
                  errors: [conciseFulcrumJudgmentMessage(judgment)],
                  warnings: [],
                  audit: result.audit,
                  instruction: rejectedFulcrumJudgeInstruction(judgment),
                } satisfies SubmitFulcrumSplitResult;
              }
            }
            rejectedFulcrumRequiresEvidence = false;
            evidenceCallsSinceRejectedFulcrum = 0;
            return result;
          });
        },
      }),
    ],
    toolUseBehavior: recursiveSpanToolUseBehavior,
  });
}

type DeterministicBoundaryCandidate = {
  startTime: number;
  source: "research_opener" | "spoken_heading" | "near_opener_fallback";
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
  const bodyTokens = textTokens(summarizeFirstBodyWords(entry, 32)).slice(0, 10);
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
      const matchCount = orderedTokenMatchCount(bodyTokens, textTokens(window.text));
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

export function chooseResearchBoundaryCandidate(research: ResearchEpubBoundaryResult | null, span: ChapterCurationSpan): DeterministicBoundaryCandidate | null {
  if (!research) return null;
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
  const fallback = research.bestCandidates
    .filter(
      (hit) =>
        hit.boundaryUse === "candidate_start" &&
        (hit.reverseEpubRelation === "opener" || hit.reverseEpubRelation === "near_opener") &&
        hit.phraseStartWord <= 32 &&
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

async function tryDeterministicFulcrumSplit(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  targetBoundary: ChapterCurationTargetBoundary
): Promise<RecursiveSpanDecision | null> {
  const headingCandidate = await findSpokenHeadingBoundaryCandidate(ctx, span, targetBoundary);
  const research = await researchEpubBoundary(ctx, {
    epubNodeId: targetBoundary.epubNodeId,
    expectedTime: targetBoundary.expectedStartTime,
    scope: { startTime: span.startTime, endTime: span.endTime },
    searchRadiusSeconds: Math.max(180, Math.min(1_200, spanDurationSeconds(span) / 2)),
    phraseLimit: 8,
    hitLimitPerPhrase: 5,
  });
  const candidate = headingCandidate ?? chooseResearchBoundaryCandidate(research, span);
  logChapterCurationEvent(ctx, {
    type: "deterministic-boundary-research",
    message: `deterministic boundary span=${span.path} target=${targetBoundary.epubNodeId} candidates=${research?.bestCandidates.length ?? 0}${
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

  const validated = await validateFulcrumSplit(
    ctx,
    span,
    {
      spanPath: span.path,
      epubNodeId: targetBoundary.epubNodeId,
      title: targetBoundary.title,
      startTime: candidate.startTime,
      evidence: [
        candidate.source === "spoken_heading"
          ? "Deterministic spoken-heading candidate: transcript contains the target heading/title cue followed by target body opener text."
          : candidate.source === "near_opener_fallback"
            ? "Deterministic near-opener fallback: the printed heading/opening words may be omitted in ASR, but research found the earliest target near-opener body phrase."
            : "Deterministic strong opener candidate from researchEpubBoundary.",
        `Anchor phrase "${candidate.phrase}"${candidate.phraseStartWord === null ? "" : ` starts at word ${candidate.phraseStartWord}`} and reverse EPUB relation is ${candidate.reverseEpubRelation}.`,
        `Transcript text: ${candidate.transcriptText}`,
      ].join(" "),
      notes:
        candidate.source === "near_opener_fallback"
          ? "Deterministic short-circuit used a near-opener fallback because earlier target heading/opener words appear absent from transcript evidence; judge must verify previous-tail/target-body boundary context."
          : "Deterministic short-circuit accepted only because the candidate is a heading/opener/near_opener at the target node start.",
    },
    { targetBoundary }
  );
  if (!validated.accepted) {
    logChapterCurationEvent(ctx, {
      type: "deterministic-boundary-rejected",
      message: `deterministic boundary span=${span.path} target=${targetBoundary.epubNodeId} validation=0`,
      span,
      targetBoundary,
      result: validated,
    });
    return null;
  }

  const judgment = await judgeFulcrumSplit(ctx, span, validated);
  if (!judgment?.accepted) {
    logChapterCurationEvent(ctx, {
      type: "deterministic-boundary-rejected",
      message: `deterministic boundary span=${span.path} target=${targetBoundary.epubNodeId} judge=0`,
      span,
      targetBoundary,
      result: validated,
      judgment,
    });
    return null;
  }

  const accepted: Extract<SubmitFulcrumSplitResult, { accepted: true }> = {
    ...validated,
    notes: [validated.notes, "Accepted by deterministic pre-agent short-circuit."].filter(Boolean).join(" "),
  };
  logChapterCurationEvent(ctx, {
    type: "deterministic-boundary-accepted",
    message: `deterministic boundary span=${span.path} target=${targetBoundary.epubNodeId} accepted=1 time=${Math.round(candidate.startTime)}s`,
    span,
    targetBoundary,
    result: accepted,
    judgment,
  });
  return {
    kind: "split",
    split: accepted,
    result: accepted,
  };
}

export async function runAgenticChapterCuration(ctx: ChapterCurationContext): Promise<SubmitChapterPlanResult | null> {
  const detailed = await runAgenticChapterCurationDetailed(ctx);
  return detailed.result;
}

export async function runRecursiveAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) {
    logChapterCurationEvent(ctx, {
      type: "recursive-run-skipped",
      message: "recursive run skipped=no_api_key",
    });
    return { result: null, finalOutput: null, newItems: [], rawResponses: [], recursiveReports: [], recursiveSpanTraces: [] };
  }
  ensureTracingInitialized(apiKey);
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    // No app-level abort on the multi-turn agent loops. maxTurns plus the OpenAI
    // client's own per-request timeout/retries bound the work; an app timeout
    // here surfaces late (between turns) and turns legitimate slow spans into
    // partial-leaf gaps that fail structural validation.
    const audibleNodeSelection = await classifyAudibleEpubNodes(ctx, runner);
    const curationCtx = applyAudibleEpubNodeSelection(ctx, audibleNodeSelection);
    if (curationCtx.epubEntries.length !== ctx.epubEntries.length) {
      logChapterCurationEvent(ctx, {
        type: "audible-epub-node-filter-applied",
        message: `audible epub node filter applied original=${ctx.epubEntries.length} curated=${curationCtx.epubEntries.length}`,
        originalEpubEntries: ctx.epubEntries.length,
        curatedEpubEntries: curationCtx.epubEntries.length,
        selectedNodeIds: curationCtx.epubEntries.map((entry) => entry.id),
        excludedNodes: audibleNodeSelection?.excludedNodes ?? [],
      });
    }
    if ((curationCtx.audioOnlyIntervals ?? []).length > 0) {
      logChapterCurationEvent(ctx, {
        type: "audio-only-intervals-applied",
        message: `audio only intervals applied count=${curationCtx.audioOnlyIntervals?.length ?? 0}`,
        audioOnlyIntervals: curationCtx.audioOnlyIntervals ?? [],
      });
    }
    const embeddedAssessment = assessEmbeddedAudioChaptersForCuration(curationCtx);
    logChapterCurationEvent(curationCtx, {
      type: "embedded-audio-prepass",
      message: `embedded audio prepass action=${embeddedAssessment.action} confidence=${embeddedAssessment.confidence}`,
      diagnostics: getEmbeddedAudioChapters(curationCtx).diagnostics,
      assessment: embeddedAssessment,
    });
    const recursiveReports: RecursiveCurationReport[] = [];
    const recursiveSpanTraces: RecursiveSpanTrace[] = [];
    const recursiveMaxSpanConcurrency = 24;
    logChapterCurationEvent(curationCtx, {
      type: "recursive-run-start",
      message: "recursive run start=1",
      model: curationCtx.settings.agents.model,
      curatorModel: curationCtx.debugCuratorModel?.trim() || curationCtx.settings.agents.model,
      judgeModel: curationCtx.debugJudgeModel?.trim() || curationCtx.settings.agents.model,
      timeoutMs: curationCtx.settings.agents.timeoutMs,
      maxSpanConcurrency: recursiveMaxSpanConcurrency,
      durationSeconds: curationCtx.durationMs / 1000,
      epubEntries: curationCtx.epubEntries.length,
      originalEpubEntries: ctx.epubEntries.length,
      audioOnlyIntervals: curationCtx.audioOnlyIntervals?.length ?? 0,
      transcriptUtterances: curationCtx.transcript.utterances?.length ?? 0,
      embeddedChapters: curationCtx.embeddedChapters.length,
    });
    const recursiveChapters = await resolveRecursiveChapterSpans(
      curationCtx,
      async (span, targetBoundary) => {
        const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
        logChapterCurationEvent(curationCtx, {
          type: "span-start",
          message: `recursive span=${span.path} depth=${span.depth} epub=${span.epubStartIndex}-${span.epubEndIndex} nodes=${spanNodeCount} time=${Math.round(span.startTime)}-${Math.round(span.endTime)}s target=${targetBoundary.epubNodeId} start=1`,
          span,
          spanNodeCount,
          targetBoundary,
        });
        const startedAt = Date.now();
        const deterministicDecision = await tryDeterministicFulcrumSplit(curationCtx, span, targetBoundary);
        if (deterministicDecision) {
          const elapsedMs = Date.now() - startedAt;
          logChapterCurationEvent(curationCtx, {
            type: "span-agent-result",
            message: `recursive span=${span.path} elapsed_ms=${elapsedMs} attempt=0 decision=split deterministic=1 split_epub=${deterministicDecision.split.epubNodeId} split_time=${Math.round(deterministicDecision.split.startTime)}s`,
            span,
            attempt: 0,
            elapsedMs,
            decisionKind: "split",
            deterministic: true,
            decision: {
              kind: "split",
              epubNodeId: deterministicDecision.split.epubNodeId,
              epubIndex: deterministicDecision.split.epubIndex,
              title: deterministicDecision.split.title,
              startTime: deterministicDecision.split.startTime,
              result: deterministicDecision.result,
            },
          });
          return deterministicDecision;
        }
        const primaryCuratorModel = curationCtx.debugCuratorModel?.trim() || curationCtx.settings.agents.model;
        const configuredCuratorModel = curationCtx.settings.agents.model;
        const attemptModels = Array.from(new Set([primaryCuratorModel, configuredCuratorModel].filter(Boolean)));
        const attempts = attemptModels.map((model, index) => ({ model, delayMs: index === 0 ? 0 : 15_000 }));
        // Pre-rank all candidate target boundaries so we can advance to the next one on max-turns failures.
        const rankedTargets = rankTargetBoundaries(curationCtx, span);
        let targetBoundaryIndex = rankedTargets.findIndex((t) => t.epubNodeId === targetBoundary.epubNodeId);
        if (targetBoundaryIndex < 0) targetBoundaryIndex = 0;
        let currentTargetBoundary = targetBoundary;
        for (let attempt = 0; attempt < attempts.length; attempt++) {
          const attemptConfig = attempts[attempt]!;
          if (attemptConfig.delayMs > 0) {
            logChapterCurationEvent(curationCtx, {
              type: "span-retry-sleep",
              message: `recursive span=${span.path} retry=${attempt} sleep_ms=${attemptConfig.delayMs} model=${attemptConfig.model}`,
              span,
              attempt,
              sleepMs: attemptConfig.delayMs,
              model: attemptConfig.model,
            });
            await sleep(attemptConfig.delayMs);
          }
          if (attempt > 0) {
            logChapterCurationEvent(curationCtx, {
              type: "span-retry-model-upgrade",
              message: `recursive span=${span.path} retry=${attempt} model=${attemptConfig.model} target=${currentTargetBoundary.epubNodeId}`,
              span,
              attempt,
              model: attemptConfig.model,
              targetBoundary: currentTargetBoundary,
            });
          }
          try {
            const spanResult = await runner.run(createRecursiveSpanCuratorAgent(curationCtx, span, currentTargetBoundary, attemptConfig.model), spanPrompt(curationCtx, span, currentTargetBoundary), {
              maxTurns: 64,
              toolExecution: { maxFunctionToolConcurrency: recursiveMaxSpanConcurrency },
            });
            logAgentUsageEvent(curationCtx, {
              role: "curator",
              model: attemptConfig.model,
              rawResponses: spanResult.rawResponses as unknown[],
              span,
            });
            const decision = parseSpanDecisionOutput(spanResult.finalOutput);
            const elapsedMs = Date.now() - startedAt;
            const tracePayload = {
              span,
              targetBoundary: currentTargetBoundary,
              attempt: attempt + 1,
              elapsedMs,
              finalOutput: spanResult.finalOutput,
              newItems: spanResult.newItems as unknown[],
              rawResponses: spanResult.rawResponses as unknown[],
            };
            const tracePath = writeChapterCurationTrace(curationCtx, `span-${span.path}-attempt-${attempt + 1}-${decision?.kind ?? "none"}`, tracePayload);
            recursiveSpanTraces.push({
              path: span.path,
              depth: span.depth,
              targetBoundary: currentTargetBoundary,
              finalOutput: spanResult.finalOutput,
              newItems: spanResult.newItems as unknown[],
              rawResponses: spanResult.rawResponses as unknown[],
            });
            logChapterCurationEvent(curationCtx, {
              type: "span-agent-result",
              message: `recursive span=${span.path} elapsed_ms=${elapsedMs} attempt=${attempt + 1} decision=${decision?.kind ?? "none"}${
                decision?.kind === "split" ? ` split_epub=${decision.split.epubNodeId} split_time=${Math.round(decision.split.startTime)}s` : ""
              } model=${attemptConfig.model}`,
              span,
              attempt: attempt + 1,
              elapsedMs,
              model: attemptConfig.model,
              decisionKind: decision?.kind ?? null,
              tracePath,
              decision:
                decision?.kind === "split"
                  ? {
                      kind: "split",
                      epubNodeId: decision.split.epubNodeId,
                      epubIndex: decision.split.epubIndex,
                      title: decision.split.title,
                      startTime: decision.split.startTime,
                      result: decision.result,
                    }
                  : null,
            });
            if (!decision && attempt < attempts.length - 1) continue;
            return decision;
          } catch (error) {
            const message = (error as Error).message;
            const elapsedMs = Date.now() - startedAt;
            const serializedError = serializeAgentError(error);
            logAgentUsageEvent(curationCtx, {
              role: "curator",
              model: attemptConfig.model,
              serializedError,
              span,
            });
            const tracePath = writeChapterCurationTrace(curationCtx, `span-${span.path}-attempt-${attempt + 1}-error`, {
              span,
              attempt: attempt + 1,
              elapsedMs,
              error: serializedError,
            });
            recursiveSpanTraces.push({
              path: span.path,
              depth: span.depth,
              finalOutput: null,
              newItems: [],
              rawResponses: [],
              error: serializedError,
            });
            const isMaxTurns = maxTurnsAgentError(error);
            logChapterCurationEvent(curationCtx, {
              type: "span-error",
              message: `recursive span=${span.path} elapsed_ms=${elapsedMs} attempt=${attempt + 1} model=${attemptConfig.model} error=${JSON.stringify(message)}`,
              span,
              attempt: attempt + 1,
              elapsedMs,
              model: attemptConfig.model,
              retryable: retryableAgentError(error) || isMaxTurns,
              tracePath,
              error: serializedError,
            });
            if (attempt < attempts.length - 1 && (retryableAgentError(error) || isMaxTurns)) {
              // On max-turns failures, advance to the next-ranked target boundary so the retry
              // targets a different (usually adjacent) chapter. This minimizes the failing section
              // to the smallest possible span rather than burning another attempt on the same hard target.
              if (isMaxTurns) {
                const nextIndex = targetBoundaryIndex + 1;
                const nextTarget = rankedTargets[nextIndex];
                if (nextTarget) {
                  logChapterCurationEvent(curationCtx, {
                    type: "span-retry-advance-target",
                    message: `recursive span=${span.path} retry=${attempt + 1} max_turns=1 advancing target from ${currentTargetBoundary.epubNodeId} to ${nextTarget.epubNodeId}`,
                    span,
                    attempt: attempt + 1,
                    previousTargetBoundary: currentTargetBoundary,
                    nextTargetBoundary: nextTarget,
                  });
                  targetBoundaryIndex = nextIndex;
                  currentTargetBoundary = nextTarget;
                }
              }
              continue;
            }
            recursiveReports.push({
              ...span,
              outcome: "failed",
              errors: [message],
            });
            return null;
          }
        }
        return null;
      },
      { maxConcurrency: recursiveMaxSpanConcurrency, reports: recursiveReports }
    );
    if (recursiveChapters && recursiveChapters.length > 0) {
      const hasPartialLeaves = recursiveReports.some((report) => report.outcome === "partial_leaf");
      logChapterCurationEvent(curationCtx, {
        type: "recursive-merge-start",
        message: `recursive merge chapters=${recursiveChapters.length} validate=structural`,
        chapters: recursiveChapters.length,
        partial: hasPartialLeaves,
        chapterPlan: summarizeSubmittedChapterObjects(recursiveChapters, 80),
      });
      const recursiveResult = submitChapterPlan(curationCtx, {
        manifestationId: curationCtx.manifestation.id,
        strategy: "Recursive chapter boundary curation",
        chapters: recursiveChapters,
        notes: hasPartialLeaves
          ? "Merged from recursively curated span plans. Some unresolved spans were returned as partial leaves so Podible still receives the markers that were confidently found."
          : "Merged from recursively curated span plans.",
      });
      if (recursiveResult.accepted) {
        logChapterCurationEvent(curationCtx, {
          type: "recursive-merge-accepted",
          message: `recursive merge accepted=1 chapters=${recursiveResult.chapters.length}`,
          chapters: recursiveResult.chapters.length,
          result: recursiveResult,
        });
        return {
          result: recursiveResult,
          finalOutput: recursiveResult,
          newItems: [],
          rawResponses: [],
          recursiveReports,
          recursiveSpanTraces,
        };
      }
      logChapterCurationEvent(curationCtx, {
        type: "recursive-merge-rejected",
        message: `recursive merge accepted=0 chapters=${recursiveChapters.length} errors=${JSON.stringify(recursiveResult.errors.slice(0, 5))}`,
        chapters: recursiveChapters.length,
        errors: recursiveResult.errors,
        warnings: recursiveResult.warnings,
        audit: recursiveResult.audit,
      });
      recursiveReports.push({
        ...createRootCurationSpan(curationCtx),
        outcome: "failed",
        errors: recursiveResult.errors,
        chapters: recursiveChapters.length,
      });
    } else {
      logChapterCurationEvent(curationCtx, {
        type: "recursive-result-null",
        message: "recursive result=null",
        reports: recursiveReports,
      });
    }

    return {
      result: null,
      finalOutput: null,
      newItems: [],
      rawResponses: [],
      recursiveReports,
      recursiveSpanTraces,
    };
  } finally {
    await provider.close().catch(() => undefined);
  }
}

export async function runAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  return runRecursiveAgenticChapterCurationDetailed(ctx);
}
