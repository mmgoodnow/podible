import { z } from "zod";
import { type FunctionToolResult, type ToolsToFinalOutputResult } from "@openai/agents";

import type { EpubChapterEntry } from "./chapter-analysis";
import {
  type AudioOnlyInterval,
  type BoundaryEvidenceCandidate,
  type ChapterCurationContext,
  type ChapterCurationSpan,
  type ChapterCurationSpanBoundary,
  type ChapterCurationTargetBoundary,
  type FulcrumValidationAudit,
  type TranscriptBoundaryWords,
  type TranscriptWindow,
  getEmbeddedAudioChapters,
  getTranscriptWindowFromContext,
  inferEntryEndRatio,
  inferEntryStartRatio,
  msToSeconds,
  normalizeToolText,
  secondsToMs,
  summarizeFirstWords,
  summarizeLastWords,
  transcriptUtterances,
} from "./chapter-curation-tools";

export type SubmittedChapter = z.infer<typeof submittedChapterSchema>;
export type SubmitChapterPlanInput = z.infer<typeof submitChapterPlanSchema>;
export type SubmitFulcrumSplitInput = z.infer<typeof submitFulcrumSplitSchema>;
export type SubmitNodeBoundaryInput = z.infer<typeof submitNodeBoundarySchema>;
export type SubmitFulcrumJudgmentInput = z.infer<typeof submitFulcrumJudgmentSchema>;
export type AudibleEpubNodeSelection = z.infer<typeof audibleEpubNodeSelectionSchema>;

export type ChapterPlanAuditEntry = {
  index: number;
  title: string;
  startTime: number;
  nearestEmbeddedBoundary: {
    title: string;
    startTime: number;
    deltaSeconds: number;
  } | null;
  transcriptAfterStart: string;
  claimedEpubHeading: {
    id: string;
    title: string;
    firstWords: string;
  } | null;
};

export type SubmitChapterPlanResult =
  | {
      accepted: true;
      strategy: string;
      notes: string | null;
      chapters: Array<{ title: string; startTime: number }>;
      warnings: string[];
      audit: ChapterPlanAuditEntry[];
    }
  | {
      accepted: false;
      errors: string[];
      warnings: string[];
      audit: ChapterPlanAuditEntry[];
      instruction: string;
    };

export type SubmitFulcrumSplitResult =
  | {
      accepted: true;
      kind: "split";
      spanPath: string;
      epubNodeId: string;
      epubIndex: number;
      title: string;
      startTime: number;
      notes: string | null;
      audit: FulcrumValidationAudit;
    }
  | {
      accepted: false;
      kind: "split";
      errors: string[];
      warnings: string[];
      audit: FulcrumValidationAudit | null;
      instruction: string;
    };

export type SubmitFulcrumJudgmentResult = SubmitFulcrumJudgmentInput;

export type ChapterBoundaryJudgeProposal = {
  kind: "fulcrum" | "node_boundary";
  spanPath: string;
  epubNodeId: string;
  epubIndex: number;
  title: string;
  startTime: number;
  notes: string | null;
  audit: FulcrumValidationAudit;
};

export type RecursiveSpanDecision = { kind: "split"; split: Extract<SubmitFulcrumSplitResult, { accepted: true }>; result?: SubmitFulcrumSplitResult };

export type SubmitNodeBoundaryResult =
  | {
      accepted: true;
      kind: "node_boundary";
      spanPath: string;
      epubNodeId: string;
      epubIndex: number;
      title: string;
      startTime: number;
      notes: string | null;
      audit: FulcrumValidationAudit;
    }
  | {
      accepted: false;
      kind: "node_boundary";
      errors: string[];
      warnings: string[];
      audit: FulcrumValidationAudit | null;
      instruction: string;
    };

export type NodeBoundaryDecision = Extract<SubmitNodeBoundaryResult, { accepted: true }>;

export type NodeBoundaryCurationReport = {
  epubNodeId: string;
  epubIndex: number;
  title: string;
  expectedStartTime: number;
  outcome: "accepted" | "failed" | "dropped";
  startTime?: number;
  errors?: string[];
  warnings?: string[];
  deterministic?: boolean;
};

export type RecursiveCurationReport = {
  path: string;
  depth: number;
  epubStartIndex: number;
  epubEndIndex: number;
  startTime: number;
  endTime: number;
  outcome: "leaf" | "partial_leaf" | "split" | "failed";
  errors?: string[];
  chapters?: number;
  chapterPlan?: SubmittedChapter[];
  split?: {
    epubNodeId: string;
    title: string;
    startTime: number;
  };
};

export type RecursiveSpanTrace = {
  path: string;
  depth: number;
  targetBoundary?: ChapterCurationTargetBoundary;
  finalOutput: unknown;
  newItems: unknown[];
  rawResponses: unknown[];
  error?: unknown;
};

const submittedChapterSchema = z.object({
  title: z.string().trim().min(1),
  startTime: z.number().finite().nonnegative(),
  epubNodeId: z.string().trim().min(1).optional(),
});

const submitChapterPlanSchema = z.object({
  manifestationId: z.number().int().positive(),
  strategy: z.string().trim().min(1),
  chapters: z.array(submittedChapterSchema).min(1).max(300),
  notes: z.string().trim().optional(),
});

const submitFulcrumSplitSchema = z.object({
  spanPath: z.string().trim().min(1),
  epubNodeId: z.string().trim().min(1),
  title: z.string().trim().min(1),
  startTime: z.number().finite().nonnegative(),
  evidence: z.string().trim().optional(),
  notes: z.string().trim().optional(),
});

const submitNodeBoundarySchema = z.object({
  spanPath: z.string().trim().min(1).optional(),
  epubNodeId: z.string().trim().min(1),
  title: z.string().trim().min(1),
  startTime: z.number().finite().nonnegative(),
  evidence: z.string().trim().optional(),
  notes: z.string().trim().optional(),
});

export const submitFulcrumJudgmentSchema = z.object({
  accepted: z.boolean(),
  confidence: z.enum(["low", "medium", "high"]),
  finding: z.enum([
    "opener_evidence_at_timestamp",
    "opener_evidence_offset_in_window",
    "window_starts_before_opener_evidence",
    "tool_classified_interior_match",
    "generic_or_weak_overlap",
    "submitted_evidence_insufficient",
  ]),
  openerEvidenceAtTimestamp: z.enum(["present", "offset", "absent", "unclear"]),
  reason: z.string().trim().min(1),
  concerns: z.array(z.string().trim().min(1)).max(10),
});

export const audibleEpubNodeSelectionSchema = z.object({
  audibleNodeIds: z.array(z.string().trim().min(1)).min(1).max(300),
  excludedNodes: z
    .array(
      z.object({
        epubNodeId: z.string().trim().min(1),
        reason: z.enum(["copyright", "dedication", "toc", "acknowledgments", "cover", "front_matter", "back_matter", "not_in_audio", "other"]),
        notes: z.string().trim().min(1),
      })
    )
    .max(300),
  audioOnlyIntervals: z
    .array(
      z.object({
        startTime: z.number().finite().nonnegative(),
        endTime: z.number().finite().nonnegative(),
        kind: z.enum(["publisher_intro", "credits", "recap", "part_bumper", "back_matter", "other"]),
        notes: z.string().trim().min(1),
      })
    )
    .max(40),
  notes: z.string().trim().optional(),
});

// --- Private helpers ---

function chapterTitleKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function normalizedWordTokens(value: string): string[] {
  return value
    .toLowerCase()
    .replace(/[’‘]/g, "'")
    .match(/[a-z0-9]+(?:'[a-z0-9]+)?/g)
    ?.map((token) => token.replace(/[^a-z0-9]+/g, ""))
    .filter(Boolean) ?? [];
}

function textTokens(value: string): string[] {
  return normalizedWordTokens(value).filter((token) => token.length >= 4);
}

function summarizeFirstBodyWords(entry: EpubChapterEntry, limit = 40): string {
  const firstBodyIndex = entry.words.findIndex((word) => word.kind === "body");
  const start = firstBodyIndex >= 0 ? firstBodyIndex : entryTitlePrefixWordCount(entry);
  return entry.words.slice(start, start + limit).map((word) => word.text).join(" ").trim();
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

function romanNumeralValue(value: string): number | null {
  const normalized = value.toLowerCase();
  if (!/^(?:i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi|xvii|xviii|xix|xx)$/.test(normalized)) return null;
  const values: Record<string, number> = { i: 1, v: 5, x: 10 };
  let total = 0;
  let previous = 0;
  for (const char of [...normalized].reverse()) {
    const valueForChar = values[char] ?? 0;
    if (valueForChar < previous) total -= valueForChar;
    else {
      total += valueForChar;
      previous = valueForChar;
    }
  }
  return total || null;
}

function titleNumberWord(value: number): string | null {
  const words = [
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
    "twenty",
  ];
  return words[value] ?? null;
}

function spokenTitleVariants(title: string): string[][] {
  const tokens = normalizedWordTokens(title);
  const variants: string[][] = [];
  if (tokens.length >= 2) variants.push(tokens);
  const leading = tokens[0];
  const leadingNumeric = leading && (/^\d+$/.test(leading) ? Number(leading) : romanNumeralValue(leading));
  const leadingWord = typeof leadingNumeric === "number" ? titleNumberWord(leadingNumeric) : null;
  if (leadingWord && tokens.length >= 2 && !isStructuralTitleToken(tokens[1]!)) {
    variants.push([leadingWord, ...tokens.slice(1)]);
    variants.push([String(leadingNumeric), ...tokens.slice(1)]);
  }
  return variants.filter((variant, index, all) => all.findIndex((candidate) => candidate.join("\u0000") === variant.join("\u0000")) === index);
}

function isShortHeadingOnlyEntry(entry: EpubChapterEntry): boolean {
  const titleTokens = normalizedWordTokens(entry.title);
  if (titleTokens.length < 2 || titleTokens.length > 5) return false;
  const meaningful = titleTokens.filter((token) => !isStructuralTitleToken(token));
  if (meaningful.length === 0 || meaningful.length > 3) return false;
  const hasBodyWords = entry.words.some((word) => word.kind === "body");
  return !hasBodyWords || entry.words.length <= titleTokens.length + 2;
}

function chapterTitleSpecificityScore(value: string): number {
  const tokens = normalizedWordTokens(value);
  const meaningful = tokens.filter((token) => !isStructuralTitleToken(token));
  return meaningful.length * 10 + (value.includes(":") || value.includes("—") || value.includes("-") ? 5 : 0) + Math.min(4, value.length / 20);
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

function transcriptAfterStart(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, radiusMs: number): string {
  return transcriptUtterances(ctx)
    .filter((utterance) => utterance.endMs >= startMs && utterance.startMs <= startMs + radiusMs)
    .map((utterance) => normalizeToolText(utterance.text))
    .filter(Boolean)
    .join(" ");
}

function transcriptBeforeStart(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, radiusMs: number): string {
  return transcriptUtterances(ctx)
    .filter((utterance) => utterance.endMs >= startMs - radiusMs && utterance.startMs < startMs)
    .map((utterance) => normalizeToolText(utterance.text))
    .filter(Boolean)
    .join(" ");
}

function transcriptBoundaryWords(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, limit = 8): TranscriptBoundaryWords | undefined {
  const timedWords = ctx.transcript.words
    .filter((word) => Number.isFinite(word.startMs) && Number.isFinite(word.endMs))
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  if (timedWords.length === 0) return undefined;
  const compact = (word: { text: string; startMs: number; endMs: number }) => ({
    text: normalizeToolText(word.text),
    startTime: msToSeconds(word.startMs),
    endTime: msToSeconds(word.endMs),
  });
  const before = timedWords.filter((word) => word.endMs <= startMs).slice(-limit).map(compact);
  const containing = timedWords.filter((word) => word.startMs < startMs && word.endMs > startMs).map(compact);
  const after = timedWords.filter((word) => word.startMs >= startMs).slice(0, limit).map(compact);
  const nearestCleanBoundaryTimes = Array.from(
    new Set(
      [...before.slice(-1).map((word) => word.endTime), ...containing.flatMap((word) => [word.startTime, word.endTime]), ...after.slice(0, 1).map((word) => word.startTime)]
        .filter((time) => Number.isFinite(time))
        .map((time) => Math.round(time * 1000) / 1000)
    )
  ).sort((a, b) => Math.abs(a - msToSeconds(startMs)) - Math.abs(b - msToSeconds(startMs)));
  return {
    before,
    containing,
    after,
    nearestCleanBoundaryTimes,
  };
}

function transcriptWordsBeforeStart(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, radiusMs: number): string | null {
  const words = ctx.transcript.words
    .filter((word) => Number.isFinite(word.startMs) && Number.isFinite(word.endMs) && word.endMs >= startMs - radiusMs && word.endMs <= startMs)
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  if (words.length === 0) return null;
  return words.map((word) => normalizeToolText(word.text)).filter(Boolean).join(" ");
}

function transcriptWordsAfterStart(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, radiusMs: number): string | null {
  const words = ctx.transcript.words
    .filter((word) => Number.isFinite(word.startMs) && Number.isFinite(word.endMs) && word.startMs >= startMs && word.startMs <= startMs + radiusMs)
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  if (words.length === 0) return null;
  return words.map((word) => normalizeToolText(word.text)).filter(Boolean).join(" ");
}

function transcriptBoundaryText(ctx: Pick<ChapterCurationContext, "transcript">, startMs: number, radiusMs: number): {
  before: string;
  after: string;
  precision: "word" | "utterance";
  note: string | null;
  boundaryWords?: TranscriptBoundaryWords;
} {
  const wordBefore = transcriptWordsBeforeStart(ctx, startMs, radiusMs);
  const wordAfter = transcriptWordsAfterStart(ctx, startMs, radiusMs);
  if (wordBefore !== null || wordAfter !== null) {
    return {
      before: normalizeToolText(wordBefore ?? ""),
      after: normalizeToolText(wordAfter ?? ""),
      precision: "word",
      note: null,
      boundaryWords: transcriptBoundaryWords(ctx, startMs),
    };
  }
  return {
    before: normalizeToolText(transcriptBeforeStart(ctx, startMs, radiusMs)),
    after: normalizeToolText(transcriptAfterStart(ctx, startMs, radiusMs)),
    precision: "utterance",
    note: "Boundary context uses utterance timing; a true boundary may fall inside the displayed utterance. If one utterance contains previous EPUB tail followed by target EPUB head, its start timestamp may be the best available boundary.",
  };
}

function nearestEmbeddedBoundary(
  embeddedChapters: ChapterCurationContext["embeddedChapters"],
  startTime: number
): ChapterPlanAuditEntry["nearestEmbeddedBoundary"] {
  if (embeddedChapters.length === 0) return null;
  const startMs = secondsToMs(startTime);
  const nearest = embeddedChapters.reduce((best, chapter) => {
    const delta = Math.abs(chapter.startMs - startMs);
    return delta < best.delta ? { chapter, delta } : best;
  }, { chapter: embeddedChapters[0]!, delta: Math.abs(embeddedChapters[0]!.startMs - startMs) });
  return {
    title: nearest.chapter.title,
    startTime: msToSeconds(nearest.chapter.startMs),
    deltaSeconds: msToSeconds(nearest.delta),
  };
}

function claimedEpubHeading(
  entries: EpubChapterEntry[],
  chapter: SubmittedChapter
): ChapterPlanAuditEntry["claimedEpubHeading"] {
  const entry = chapter.epubNodeId
    ? entries.find((candidate) => candidate.id === chapter.epubNodeId)
    : entries.find((candidate) => chapterTitleKey(candidate.title) === chapterTitleKey(chapter.title));
  if (!entry) return null;
  return {
    id: entry.id,
    title: entry.title,
    firstWords: summarizeFirstWords(entry, 24),
  };
}

function buildPlanAudit(ctx: Pick<ChapterCurationContext, "epubEntries" | "transcript" | "embeddedChapters">, chapters: SubmittedChapter[]): ChapterPlanAuditEntry[] {
  return chapters.map((chapter, index) => ({
    index,
    title: chapter.title,
    startTime: chapter.startTime,
    nearestEmbeddedBoundary: nearestEmbeddedBoundary(ctx.embeddedChapters, chapter.startTime),
    transcriptAfterStart: transcriptAfterStart(ctx, secondsToMs(chapter.startTime), 20_000),
    claimedEpubHeading: claimedEpubHeading(ctx.epubEntries, chapter),
  }));
}

function matchingBadEmbeddedBoundaryRatio(ctx: Pick<ChapterCurationContext, "durationMs" | "embeddedChapters">, chapters: SubmittedChapter[]): number {
  const embedded = getEmbeddedAudioChapters(ctx);
  const badEmbedded =
    embedded.diagnostics.durationPattern === "suspiciously_even" &&
    (embedded.diagnostics.labelQuality === "generic" || embedded.diagnostics.labelQuality === "repeated");
  if (!badEmbedded || chapters.length === 0 || embedded.chapters.length === 0) return 0;
  let matched = 0;
  for (const chapter of chapters) {
    const nearest = nearestEmbeddedBoundary(ctx.embeddedChapters, chapter.startTime);
    if (nearest && Math.abs(nearest.deltaSeconds) <= 2) matched += 1;
  }
  return matched / chapters.length;
}

function claimedEpubIndexes(entries: EpubChapterEntry[], chapters: SubmittedChapter[]): number[] {
  return chapters
    .map((chapter) => (chapter.epubNodeId ? entries.findIndex((entry) => entry.id === chapter.epubNodeId) : entries.findIndex((entry) => chapterTitleKey(entry.title) === chapterTitleKey(chapter.title))))
    .filter((index) => index >= 0)
    .sort((a, b) => a - b);
}

function maxEpubIndexGap(indexes: number[]): number {
  let maxGap = 0;
  for (let index = 1; index < indexes.length; index++) {
    maxGap = Math.max(maxGap, indexes[index]! - indexes[index - 1]! - 1);
  }
  return maxGap;
}

export function createRootCurationSpan(ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs">): ChapterCurationSpan {
  return {
    epubStartIndex: 0,
    epubEndIndex: Math.max(0, ctx.epubEntries.length - 1),
    startTime: 0,
    endTime: msToSeconds(ctx.durationMs),
    depth: 0,
    path: "root",
  };
}

function spanEpubEntries(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): EpubChapterEntry[] {
  return ctx.epubEntries.slice(span.epubStartIndex, span.epubEndIndex + 1);
}

function spanContainsEpubIndex(span: ChapterCurationSpan, index: number): boolean {
  return index >= span.epubStartIndex && index <= span.epubEndIndex;
}

function spanEpubLocalRatio(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan, index: number): number | null {
  if (!spanContainsEpubIndex(span, index)) return null;
  const spanStart = inferEntryStartRatio(ctx.epubEntries, span.epubStartIndex);
  const spanEnd = inferEntryEndRatio(ctx.epubEntries, span.epubEndIndex);
  return localRatio(inferEntryStartRatio(ctx.epubEntries, index), spanStart, spanEnd);
}

export function spanInternalBoundaryCount(span: ChapterCurationSpan): number {
  return Math.max(0, span.epubEndIndex - span.epubStartIndex);
}

export function automaticLeafChapters(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): SubmittedChapter[] {
  if (span.startBoundary) {
    return [
      {
        title: span.startBoundary.title,
        startTime: span.startBoundary.startTime,
        epubNodeId: span.startBoundary.epubNodeId,
      },
    ];
  }
  const entry = ctx.epubEntries[span.epubStartIndex];
  if (!entry) return [];
  return [{ title: entry.title, startTime: span.startTime, epubNodeId: entry.id }];
}

export function rankTargetBoundaries(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): ChapterCurationTargetBoundary[] {
  const candidates: ChapterCurationTargetBoundary[] = [];
  const nodeCount = Math.max(1, span.epubEndIndex - span.epubStartIndex + 1);
  for (let index = span.epubStartIndex + 1; index <= span.epubEndIndex; index++) {
    const entry = ctx.epubEntries[index];
    if (!entry) continue;
    const localNodeRatio = (index - span.epubStartIndex) / nodeCount;
    const localTimePriorRatio = spanEpubLocalRatio(ctx, span, index) ?? localNodeRatio;
    candidates.push({
      epubNodeId: entry.id,
      epubIndex: index,
      title: entry.title,
      expectedStartTime: span.startTime + spanDurationSeconds(span) * localTimePriorRatio,
      localNodeRatio: Math.round(localNodeRatio * 1000) / 1000,
    });
  }
  return candidates.sort((a, b) => Math.abs(a.localNodeRatio - 0.5) - Math.abs(b.localNodeRatio - 0.5) || a.epubIndex - b.epubIndex);
}

export function nodeBoundaryTargets(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs" | "chapterStartTimeHints">
): ChapterCurationTargetBoundary[] {
  const durationSeconds = msToSeconds(ctx.durationMs);
  const nodeCount = Math.max(1, ctx.epubEntries.length);
  return ctx.epubEntries.map((entry, index) => ({
    epubNodeId: entry.id,
    epubIndex: index,
    title: entry.title,
    expectedStartTime:
      ctx.chapterStartTimeHints?.[entry.id] ??
      Math.round(durationSeconds * inferEntryStartRatio(ctx.epubEntries, index) * 1000) / 1000,
    localNodeRatio: Math.round((index / nodeCount) * 1000) / 1000,
  }));
}

function chooseTargetBoundary(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): ChapterCurationTargetBoundary | null {
  return rankTargetBoundaries(ctx, span)[0] ?? null;
}

export function spanDurationSeconds(span: ChapterCurationSpan): number {
  return Math.max(0, span.endTime - span.startTime);
}

const INHERITED_BOUNDARY_TOLERANCE_SECONDS = 2;
const MAX_AUTOMATIC_LEAF_EPUB_NODES = 3;

function childSpanPath(parent: ChapterCurationSpan, side: "L" | "R"): string {
  return parent.path === "root" ? side : `${parent.path}${side}`;
}

function splitSpan(
  span: ChapterCurationSpan,
  split: { epubIndex: number; epubNodeId: string; title: string; startTime: number }
): { left: ChapterCurationSpan; right: ChapterCurationSpan } {
  return {
    left: {
      epubStartIndex: span.epubStartIndex,
      epubEndIndex: split.epubIndex - 1,
      startTime: span.startTime,
      endTime: split.startTime,
      depth: span.depth + 1,
      path: childSpanPath(span, "L"),
      startBoundary: span.startBoundary,
    },
    right: {
      epubStartIndex: split.epubIndex,
      epubEndIndex: span.epubEndIndex,
      startTime: split.startTime,
      endTime: span.endTime,
      depth: span.depth + 1,
      path: childSpanPath(span, "R"),
      startBoundary: {
        epubNodeId: split.epubNodeId,
        epubIndex: split.epubIndex,
        title: split.title,
        startTime: split.startTime,
        source: "parent_split",
      },
    },
  };
}

function normalizeSpanChapters(chapters: SubmittedChapter[]): SubmittedChapter[] {
  const sorted = [...chapters].sort((a, b) => a.startTime - b.startTime || chapterTitleKey(a.title).localeCompare(chapterTitleKey(b.title)));
  const out: SubmittedChapter[] = [];
  for (const chapter of sorted) {
    const previous = out.at(-1);
    if (previous && Math.abs(previous.startTime - chapter.startTime) <= 30) {
      const previousHasEpub = Boolean(previous.epubNodeId);
      const chapterHasEpub = Boolean(chapter.epubNodeId);
      if (!previousHasEpub && chapterHasEpub) out[out.length - 1] = chapter;
      continue;
    }
    out.push(chapter);
  }
  return out;
}

function localRatio(value: number, start: number, end: number): number {
  const width = end - start;
  if (width <= 0) return 0;
  return Math.max(0, Math.min(1, (value - start) / width));
}

function buildBoundaryComparisonAudit(
  ctx: ChapterCurationContext,
  input: {
    epubIndex: number;
    title: string;
    startTime: number;
    transcriptWindow?: TranscriptWindow;
    candidates?: BoundaryEvidenceCandidate[];
  }
): FulcrumValidationAudit {
  const entry = ctx.epubEntries[input.epubIndex] ?? null;
  const previous = input.epubIndex > 0 ? (ctx.epubEntries[input.epubIndex - 1] ?? null) : null;
  const startMs = secondsToMs(input.startTime);
  const transcriptWindow = input.transcriptWindow ?? getTranscriptWindowFromContext(ctx, startMs, 45_000);
  const boundaryText = transcriptBoundaryText(ctx, startMs, 45_000);
  return {
    epubNodeId: entry?.id ?? null,
    title: entry?.title ?? input.title,
    startTime: input.startTime,
    boundaryComparison: {
      transcriptPrecision: boundaryText.precision,
      transcriptPrecisionNote: boundaryText.note,
      previousEpub: {
        epubNodeId: previous?.id ?? null,
        title: previous?.title ?? null,
        tailText: previous ? summarizeLastWords(previous, 56) : "",
      },
      targetEpub: {
        epubNodeId: entry?.id ?? null,
        title: entry?.title ?? input.title,
        headText: entry ? summarizeFirstWords(entry, 56) : "",
        bodyHeadText: entry ? summarizeFirstBodyWords(entry, 56) : "",
        optionalHeadingText: entry ? summarizeOptionalHeadingText(entry) : "",
        headingMayBeUnspoken: true,
      },
      transcriptBefore: boundaryText.before.slice(-1_000),
      transcriptAfter: boundaryText.after.slice(0, 1_000),
      boundaryWords: boundaryText.boundaryWords,
    },
    transcriptWindow: normalizeToolText(transcriptWindow.text).slice(0, 1_000),
    candidates: input.candidates ?? [],
  };
}

function hasTargetOpenerEvidence(audit: FulcrumValidationAudit | null): boolean {
  if (!audit) return false;
  const targetText = audit.boundaryComparison.targetEpub.bodyHeadText || audit.boundaryComparison.targetEpub.headText;
  const targetTokens = textTokens(targetText).filter((token) => !isStructuralTitleToken(token)).slice(0, 24);
  if (targetTokens.length === 0) return false;
  const afterTokens = new Set(textTokens(audit.boundaryComparison.transcriptAfter));
  const matched = targetTokens.filter((token) => afterTokens.has(token));
  const openerMatched = targetTokens.slice(0, Math.min(8, targetTokens.length)).some((token) => afterTokens.has(token));
  return openerMatched && (matched.length >= 4 || matched.length / Math.max(1, targetTokens.length) >= 0.35);
}

export async function validateFulcrumSplit(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  input: unknown,
  options: { targetBoundary?: ChapterCurationTargetBoundary } = {}
): Promise<SubmitFulcrumSplitResult> {
  const parsed = submitFulcrumSplitSchema.safeParse(input);
  if (!parsed.success) {
    return {
      accepted: false,
      kind: "split",
      errors: parsed.error.issues.map((issue) => `${issue.path.join(".") || "input"}: ${issue.message}`),
      warnings: [],
      audit: null,
      instruction: "Revise the submission to match the submitBoundarySplit schema.",
    };
  }

  const split = parsed.data;
  const errors: string[] = [];
  const warnings: string[] = [];
  if (split.spanPath !== span.path) errors.push(`spanPath ${split.spanPath} does not match current span ${span.path}`);
  const epubIndex = ctx.epubEntries.findIndex((entry) => entry.id === split.epubNodeId);
  const entry = epubIndex >= 0 ? ctx.epubEntries[epubIndex] : null;
  if (options.targetBoundary && split.epubNodeId !== options.targetBoundary.epubNodeId) {
    errors.push(`This span is assigned to prove ${options.targetBoundary.epubNodeId}; do not submit ${split.epubNodeId}.`);
  }
  if (!entry || !spanContainsEpubIndex(span, epubIndex)) {
    errors.push(`epubNodeId ${split.epubNodeId} is not inside the current span`);
  }
  if (epubIndex <= span.epubStartIndex || epubIndex > span.epubEndIndex) {
    errors.push("The split must leave non-empty EPUB ranges on both sides.");
  }
  if (split.startTime <= span.startTime || split.startTime >= span.endTime) {
    errors.push("startTime must be inside the current span time range.");
  }
  const edgeMargin = Math.max(120, spanDurationSeconds(span) * 0.05);
  const isOnlyRemainingAssignedBoundary = Boolean(options.targetBoundary && spanInternalBoundaryCount(span) === 1);
  if (!isOnlyRemainingAssignedBoundary && (split.startTime - span.startTime < edgeMargin || span.endTime - split.startTime < edgeMargin)) {
    errors.push("startTime is too close to the span boundary — the split must leave a meaningful region on each side.");
  }

  const window = getTranscriptWindowFromContext(ctx, secondsToMs(split.startTime), 45_000);
  const audit = buildBoundaryComparisonAudit(ctx, {
    epubIndex,
    title: entry?.title ?? split.title,
    startTime: split.startTime,
    transcriptWindow: window,
    candidates: [],
  });

  if (errors.length > 0) {
    return {
      accepted: false,
      kind: "split",
      errors,
      warnings,
      audit,
      instruction: "Find the assigned target boundary with stronger transcript evidence and submit a corrected startTime.",
    };
  }

  return {
    accepted: true,
    kind: "split",
    spanPath: span.path,
    epubNodeId: entry!.id,
    epubIndex,
    title: entry!.title,
    startTime: split.startTime,
    notes: split.notes ?? null,
    audit,
  };
}

export async function validateNodeBoundary(
  ctx: ChapterCurationContext,
  input: unknown,
  options: { span?: ChapterCurationSpan; targetBoundary?: ChapterCurationTargetBoundary } = {}
): Promise<SubmitNodeBoundaryResult> {
  const parsed = submitNodeBoundarySchema.safeParse(input);
  if (!parsed.success) {
    return {
      accepted: false,
      kind: "node_boundary",
      errors: parsed.error.issues.map((issue) => `${issue.path.join(".") || "input"}: ${issue.message}`),
      warnings: [],
      audit: null,
      instruction: "Revise the submission to match the submitNodeBoundary schema.",
    };
  }

  const boundary = parsed.data;
  const errors: string[] = [];
  const warnings: string[] = [];
  const span = options.span;
  if (span && boundary.spanPath && boundary.spanPath !== span.path) {
    errors.push(`spanPath ${boundary.spanPath} does not match current span ${span.path}`);
  }
  const epubIndex = ctx.epubEntries.findIndex((entry) => entry.id === boundary.epubNodeId);
  const entry = epubIndex >= 0 ? ctx.epubEntries[epubIndex] : null;
  if (options.targetBoundary && boundary.epubNodeId !== options.targetBoundary.epubNodeId) {
    errors.push(`This task is assigned to prove ${options.targetBoundary.epubNodeId}; do not submit ${boundary.epubNodeId}.`);
  }
  if (!entry) {
    errors.push(`epubNodeId ${boundary.epubNodeId} is not in the curated audible EPUB node set`);
  }
  if (span && (!entry || !spanContainsEpubIndex(span, epubIndex))) {
    errors.push(`epubNodeId ${boundary.epubNodeId} is not inside the current span`);
  }
  const durationSeconds = msToSeconds(ctx.durationMs);
  if (boundary.startTime < 0 || boundary.startTime >= durationSeconds) {
    errors.push("startTime must be inside the manifestation duration.");
  }
  if (span && (boundary.startTime < span.startTime || boundary.startTime > span.endTime)) {
    errors.push("startTime must be inside the current span time range.");
  }
  const audit =
    epubIndex >= 0
      ? buildBoundaryComparisonAudit(ctx, {
          epubIndex,
          title: entry?.title ?? boundary.title,
          startTime: boundary.startTime,
          transcriptWindow: getTranscriptWindowFromContext(ctx, secondsToMs(boundary.startTime), 45_000),
          candidates: [],
        })
      : null;
  const enclosingAudioOnlyInterval = (ctx.audioOnlyIntervals ?? []).find(
    (interval) => boundary.startTime > interval.startTime + 0.25 && boundary.startTime < interval.endTime - 0.25
  );
  if (enclosingAudioOnlyInterval) {
    const secondsFromNearestEdge = Math.min(
      Math.abs(boundary.startTime - enclosingAudioOnlyInterval.startTime),
      Math.abs(enclosingAudioOnlyInterval.endTime - boundary.startTime)
    );
    if (secondsFromNearestEdge > 5) {
      const message = `startTime is inside audio-only interval ${enclosingAudioOnlyInterval.kind}; EPUB text should begin outside that interval.`;
      if (hasTargetOpenerEvidence(audit)) {
        warnings.push(`${message} Transcript opener evidence is strong, so treating the interval as suspect.`);
      } else {
        errors.push(message);
      }
    } else {
      warnings.push(`startTime is very close to audio-only interval ${enclosingAudioOnlyInterval.kind}; verify the boundary is not credits/preamble.`);
    }
  }

  if (errors.length > 0) {
    return {
      accepted: false,
      kind: "node_boundary",
      errors,
      warnings,
      audit,
      instruction: "Find the assigned audible EPUB node boundary with stronger transcript evidence and submit a corrected startTime.",
    };
  }

  return {
    accepted: true,
    kind: "node_boundary",
    spanPath: boundary.spanPath ?? span?.path ?? "root",
    epubNodeId: entry!.id,
    epubIndex,
    title: entry!.title,
    startTime: boundary.startTime,
    notes: boundary.notes ?? null,
    audit: audit!,
  };
}

function summarizeSubmittedChapters(chapters: SubmittedChapter[], limit = 12): string {
  const shown = chapters.slice(0, limit).map((chapter) => `${Math.round(chapter.startTime)}s:${chapter.title}`).join(" | ");
  return chapters.length > limit ? `${shown} | ... +${chapters.length - limit}` : shown;
}

export function summarizeSubmittedChapterObjects(chapters: SubmittedChapter[], limit = 24): Array<Pick<SubmittedChapter, "title" | "startTime" | "epubNodeId">> {
  return chapters.slice(0, limit).map((chapter) => ({
    title: chapter.title,
    startTime: chapter.startTime,
    epubNodeId: chapter.epubNodeId,
  }));
}

export function submitChapterPlan(ctx: ChapterCurationContext, input: unknown): SubmitChapterPlanResult {
  const parsed = submitChapterPlanSchema.safeParse(input);
  if (!parsed.success) {
    return {
      accepted: false,
      errors: parsed.error.issues.map((issue) => `${issue.path.join(".") || "input"}: ${issue.message}`),
      warnings: [],
      audit: [],
      instruction: "Revise the chapter plan so it matches the submitChapterPlan schema.",
    };
  }

  const plan = parsed.data;
  const errors: string[] = [];
  const warnings: string[] = [];
  if (plan.manifestationId !== ctx.manifestation.id) {
    errors.push(`manifestationId ${plan.manifestationId} does not match current manifestation ${ctx.manifestation.id}`);
  }

  let previousStartTime = -1;
  const seenStarts = new Set<number>();
  const seenTitles = new Set<string>();
  for (const [index, chapter] of plan.chapters.entries()) {
    const roundedStart = Math.round(chapter.startTime * 1000) / 1000;
    if (chapter.startTime >= ctx.durationMs / 1000) {
      errors.push(`chapters[${index}].startTime is outside manifestation duration`);
    }
    if (chapter.startTime <= previousStartTime) {
      errors.push(`chapters[${index}].startTime must be strictly greater than the previous chapter`);
    }
    if (seenStarts.has(roundedStart)) {
      errors.push(`chapters[${index}].startTime duplicates another chapter timestamp`);
    }
    const titleKey = chapterTitleKey(chapter.title);
    if (seenTitles.has(titleKey)) warnings.push(`chapters[${index}].title repeats a previous title`);
    seenStarts.add(roundedStart);
    seenTitles.add(titleKey);
    previousStartTime = chapter.startTime;
  }

  if (plan.chapters.length < 3 && ctx.durationMs >= 30 * 60_000) {
    warnings.push("Long manifestation has fewer than three chapters.");
  }

  const audit = buildPlanAudit(ctx, plan.chapters);
  const missingTranscriptEvidence = audit.filter((entry) => !entry.transcriptAfterStart.trim()).length;
  if (missingTranscriptEvidence > 0) {
    warnings.push(`${missingTranscriptEvidence} chapter(s) have no transcript text within 20 seconds after the proposed start.`);
  }
  const missingEpubClaims = audit.filter((entry) => entry.claimedEpubHeading === null).length;
  if (missingEpubClaims > 0) {
    warnings.push(`${missingEpubClaims} chapter title(s) do not directly match an EPUB heading or supplied epubNodeId.`);
  }
  if (ctx.epubEntries.length >= 10 && missingEpubClaims > plan.chapters.length / 2) {
    errors.push("Plan does not use enough EPUB heading evidence for an EPUB-rich book.");
  }
  if (ctx.epubEntries.length >= 20 && plan.chapters.length < Math.min(12, Math.ceil(ctx.epubEntries.length * 0.25))) {
    errors.push("Plan is too coarse for an EPUB-rich book; propose a fuller listening chapter map.");
  }
  const epubIndexes = claimedEpubIndexes(ctx.epubEntries, plan.chapters);
  if (ctx.epubEntries.length >= 20) {
    const minimumCoverage = Math.ceil(ctx.epubEntries.length * 0.55);
    if (epubIndexes.length < minimumCoverage) {
      errors.push(`Plan covers only ${epubIndexes.length} EPUB node(s); EPUB-rich books need at least ${minimumCoverage} supported nodes or a clear section-level strategy.`);
    }
    const maxGap = maxEpubIndexGap(epubIndexes);
    if (maxGap > 8) {
      errors.push(`Plan skips ${maxGap} consecutive EPUB node(s); fill large structural gaps or split into section-level plans.`);
    }
  }
  for (const [index, chapter] of plan.chapters.entries()) {
    if (!chapter.epubNodeId) continue;
    const heading = audit[index]?.claimedEpubHeading;
    if (!heading) {
      errors.push(`chapters[${index}].epubNodeId does not match an EPUB node`);
      continue;
    }
    if (chapterTitleKey(chapter.title) !== chapterTitleKey(heading.title)) {
      errors.push(`chapters[${index}] title "${chapter.title}" does not match claimed EPUB heading "${heading.title}"`);
    }
  }
  const badEmbeddedRatio = matchingBadEmbeddedBoundaryRatio(ctx, plan.chapters);
  if (badEmbeddedRatio >= 0.8 && plan.chapters.length >= 5) {
    errors.push("Plan appears to copy suspicious evenly-divided embedded chapter markers; use transcript and EPUB evidence to revise boundaries.");
  }

  if (errors.length > 0) {
    return {
      accepted: false,
      errors,
      warnings,
      audit,
      instruction: "Revise the plan, then call submitChapterPlan again. Prefer EPUB structure plus transcript evidence over suspicious embedded markers.",
    };
  }

  return {
    accepted: true,
    strategy: plan.strategy,
    notes: plan.notes ?? null,
    chapters: plan.chapters.map((chapter) => ({
      title: chapter.title,
      startTime: chapter.startTime,
    })),
    warnings,
    audit,
  };
}

function parseSpanDecisionOutput(output: unknown): RecursiveSpanDecision | null {
  const value = typeof output === "string" ? safeJsonParse(output) : output;
  if (!value || typeof value !== "object") return null;
  const record = value as { accepted?: unknown; kind?: unknown; epubNodeId?: unknown };
  if (record.accepted !== true) return null;
  if (record.kind === "split" && typeof record.epubNodeId === "string") {
    return { kind: "split", split: record as Extract<SubmitFulcrumSplitResult, { accepted: true }>, result: record as SubmitFulcrumSplitResult };
  }
  return null;
}

function parseNodeBoundaryOutput(output: unknown): NodeBoundaryDecision | null {
  const value = typeof output === "string" ? safeJsonParse(output) : output;
  if (!value || typeof value !== "object") return null;
  const record = value as { accepted?: unknown; kind?: unknown; epubNodeId?: unknown };
  if (record.accepted !== true) return null;
  if (record.kind === "node_boundary" && typeof record.epubNodeId === "string") return record as NodeBoundaryDecision;
  return null;
}

function parseFulcrumJudgmentOutput(output: unknown): SubmitFulcrumJudgmentResult | null {
  const value = typeof output === "string" ? safeJsonParse(output) : output;
  const parsed = submitFulcrumJudgmentSchema.safeParse(value);
  return parsed.success ? parsed.data : null;
}

function parseAudibleEpubNodeSelectionOutput(output: unknown): AudibleEpubNodeSelection | null {
  const value = typeof output === "string" ? safeJsonParse(output) : output;
  const parsed = audibleEpubNodeSelectionSchema.safeParse(value);
  return parsed.success ? parsed.data : null;
}

function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return null;
  }
}

export function recursiveSpanToolUseBehavior(_: unknown, toolResults: FunctionToolResult[]): ToolsToFinalOutputResult {
  const terminalResult = toolResults.find(
    (result) =>
      result.type === "function_output" &&
      result.tool.name === "submitBoundarySplit" &&
      parseSpanDecisionOutput(result.output)
  );
  if (!terminalResult || terminalResult.type !== "function_output") {
    return { isFinalOutput: false, isInterrupted: undefined };
  }
  return {
    isFinalOutput: true,
    isInterrupted: undefined,
    finalOutput: JSON.stringify(parseSpanDecisionOutput(terminalResult.output)?.result ?? terminalResult.output),
  };
}

export function fulcrumJudgeToolUseBehavior(_: unknown, toolResults: FunctionToolResult[]): ToolsToFinalOutputResult {
  const terminalResult = toolResults.find(
    (result) => result.type === "function_output" && result.tool.name === "submitBoundaryJudgment" && parseFulcrumJudgmentOutput(result.output)
  );
  if (!terminalResult || terminalResult.type !== "function_output") {
    return { isFinalOutput: false, isInterrupted: undefined };
  }
  return {
    isFinalOutput: true,
    isInterrupted: undefined,
    finalOutput: JSON.stringify(parseFulcrumJudgmentOutput(terminalResult.output) ?? terminalResult.output),
  };
}

export function nodeBoundaryToolUseBehavior(_: unknown, toolResults: FunctionToolResult[]): ToolsToFinalOutputResult {
  const terminalResult = toolResults.find(
    (result) => result.type === "function_output" && result.tool.name === "submitNodeBoundary" && parseNodeBoundaryOutput(result.output)
  );
  if (!terminalResult || terminalResult.type !== "function_output") {
    return { isFinalOutput: false, isInterrupted: undefined };
  }
  return {
    isFinalOutput: true,
    isInterrupted: undefined,
    finalOutput: JSON.stringify(parseNodeBoundaryOutput(terminalResult.output) ?? terminalResult.output),
  };
}

export function audibleEpubNodeSelectionToolUseBehavior(_: unknown, toolResults: FunctionToolResult[]): ToolsToFinalOutputResult {
  const terminalResult = toolResults.find(
    (result) => result.type === "function_output" && result.tool.name === "submitAudibleEpubNodeSelection" && parseAudibleEpubNodeSelectionOutput(result.output)
  );
  if (!terminalResult || terminalResult.type !== "function_output") {
    return { isFinalOutput: false, isInterrupted: undefined };
  }
  return {
    isFinalOutput: true,
    isInterrupted: undefined,
    finalOutput: JSON.stringify(parseAudibleEpubNodeSelectionOutput(terminalResult.output) ?? terminalResult.output),
  };
}

export async function resolveNodeBoundaryChapters(
  ctx: ChapterCurationContext,
  decide: (targetBoundary: ChapterCurationTargetBoundary) => Promise<NodeBoundaryDecision | null>,
  options: { maxConcurrency?: number; reports?: NodeBoundaryCurationReport[] } = {}
): Promise<SubmittedChapter[] | null> {
  const duplicateBoundaryWindowSeconds = 5;
  const targets = nodeBoundaryTargets(ctx);
  if (targets.length === 0) return null;
  const maxConcurrency = Math.max(1, options.maxConcurrency ?? 8);
  const reports = options.reports;
  let activeDecisions = 0;
  const waiters: Array<() => void> = [];

  async function withDecisionSlot<T>(fn: () => Promise<T>): Promise<T> {
    if (activeDecisions >= maxConcurrency) {
      await new Promise<void>((resolve) => waiters.push(resolve));
    }
    activeDecisions++;
    try {
      return await fn();
    } finally {
      activeDecisions--;
      waiters.shift()?.();
    }
  }

  const decisions = await Promise.all(
    targets.map(async (target) => {
      try {
        const decision = await withDecisionSlot(() => decide(target));
        if (!decision) {
          reports?.push({
            epubNodeId: target.epubNodeId,
            epubIndex: target.epubIndex,
            title: target.title,
            expectedStartTime: target.expectedStartTime,
            outcome: "failed",
            errors: ["No accepted boundary decision."],
          });
          return null;
        }
        reports?.push({
          epubNodeId: target.epubNodeId,
          epubIndex: target.epubIndex,
          title: target.title,
          expectedStartTime: target.expectedStartTime,
          outcome: "accepted",
          startTime: decision.startTime,
        });
        return decision;
      } catch (error) {
        reports?.push({
          epubNodeId: target.epubNodeId,
          epubIndex: target.epubIndex,
          title: target.title,
          expectedStartTime: target.expectedStartTime,
          outcome: "failed",
          errors: [(error as Error).message],
        });
        return null;
      }
    })
  );

  const resolvedDecisions = recoverAdjacentHeadingOnlyNodeBoundaries(ctx, targets, decisions, reports);

  const chapters: SubmittedChapter[] = [];
  const chapterDecisions: NodeBoundaryDecision[] = [];
  function markReportDropped(decision: NodeBoundaryDecision, reason: string): void {
    const report = reports?.find((item) => item.epubNodeId === decision.epubNodeId && item.outcome === "accepted");
    if (report) {
      report.outcome = "dropped";
      report.errors = [...(report.errors ?? []), reason];
      return;
    }
    reports?.push({
      epubNodeId: decision.epubNodeId,
      epubIndex: decision.epubIndex,
      title: decision.title,
      expectedStartTime: nodeBoundaryTargets(ctx).find((target) => target.epubNodeId === decision.epubNodeId)?.expectedStartTime ?? decision.startTime,
      outcome: "dropped",
      startTime: decision.startTime,
      errors: [reason],
    });
  }
  for (const decision of resolvedDecisions
    .filter((decision): decision is NodeBoundaryDecision => Boolean(decision))
    .sort((a, b) => a.epubIndex - b.epubIndex || a.startTime - b.startTime)) {
    const previous = chapters.at(-1);
    if (previous && Math.abs(previous.startTime - decision.startTime) <= duplicateBoundaryWindowSeconds) {
      const previousDecision = chapterDecisions.at(-1);
      if (previousDecision && shouldKeepAdjacentDistinctEpubBoundaries(ctx, previousDecision, decision)) {
        chapters.push({
          title: decision.title,
          startTime: decision.startTime,
          epubNodeId: decision.epubNodeId,
        });
        chapterDecisions.push(decision);
        continue;
      }
      if (previousDecision && chapterTitleSpecificityScore(decision.title) > chapterTitleSpecificityScore(previous.title)) {
        markReportDropped(previousDecision, `Dropped duplicate/nearby boundary in favor of more specific EPUB title ${decision.title}.`);
        chapters[chapters.length - 1] = { title: decision.title, startTime: decision.startTime, epubNodeId: decision.epubNodeId };
        chapterDecisions[chapterDecisions.length - 1] = decision;
      } else {
        markReportDropped(decision, `Dropped duplicate/nearby boundary after ${previous.title}.`);
      }
      continue;
    }
    if (previous && decision.startTime <= previous.startTime) {
      const previousDecision = chapterDecisions.at(-1);
      if (previousDecision && chapterTitleSpecificityScore(decision.title) > chapterTitleSpecificityScore(previous.title)) {
        markReportDropped(previousDecision, `Dropped non-monotonic boundary in favor of more specific EPUB title ${decision.title}.`);
        chapters[chapters.length - 1] = { title: decision.title, startTime: decision.startTime, epubNodeId: decision.epubNodeId };
        chapterDecisions[chapterDecisions.length - 1] = decision;
      } else {
        markReportDropped(decision, `Dropped non-monotonic boundary after ${previous.title}.`);
      }
      continue;
    }
    chapters.push({
      title: decision.title,
      startTime: decision.startTime,
      epubNodeId: decision.epubNodeId,
    });
    chapterDecisions.push(decision);
  }
  return chapters.length > 0 ? chapters : null;
}

function shouldKeepAdjacentDistinctEpubBoundaries(
  ctx: ChapterCurationContext,
  previous: NodeBoundaryDecision,
  current: NodeBoundaryDecision
): boolean {
  if (current.startTime <= previous.startTime) return false;
  if (current.epubIndex !== previous.epubIndex + 1) return false;
  if (chapterTitleKey(current.title) === chapterTitleKey(previous.title)) return false;
  const previousEntry = ctx.epubEntries[previous.epubIndex];
  const currentEntry = ctx.epubEntries[current.epubIndex];
  return Boolean((previousEntry && isShortHeadingOnlyEntry(previousEntry)) || (currentEntry && isShortHeadingOnlyEntry(currentEntry)));
}

function recoverAdjacentHeadingOnlyNodeBoundaries(
  ctx: ChapterCurationContext,
  targets: ChapterCurationTargetBoundary[],
  decisions: Array<NodeBoundaryDecision | null>,
  reports: NodeBoundaryCurationReport[] | undefined
): Array<NodeBoundaryDecision | null> {
  const resolved = [...decisions];
  for (const [index, decision] of decisions.entries()) {
    if (decision) continue;
    const target = targets[index];
    if (!target) continue;
    const entry = ctx.epubEntries[target.epubIndex];
    if (!entry || !isShortHeadingOnlyEntry(entry)) continue;
    const nextDecision = decisions
      .filter((candidate): candidate is NodeBoundaryDecision => Boolean(candidate))
      .filter((candidate) => candidate.epubIndex > target.epubIndex)
      .sort((a, b) => a.epubIndex - b.epubIndex || a.startTime - b.startTime)[0];
    const recovered = nextDecision ? recoverHeadingFromWordsBeforeNextBoundary(target, entry, nextDecision) : null;
    if (!recovered) continue;
    resolved[index] = recovered;
    const report = reports?.find((item) => item.epubNodeId === target.epubNodeId && item.outcome === "failed");
    if (report) {
      report.outcome = "accepted";
      report.startTime = recovered.startTime;
      report.errors = undefined;
      report.warnings = [...(report.warnings ?? []), "Recovered from spoken heading immediately before the next accepted EPUB boundary."];
      report.deterministic = true;
    } else {
      reports?.push({
        epubNodeId: target.epubNodeId,
        epubIndex: target.epubIndex,
        title: target.title,
        expectedStartTime: target.expectedStartTime,
        outcome: "accepted",
        startTime: recovered.startTime,
        deterministic: true,
        warnings: ["Recovered from spoken heading immediately before the next accepted EPUB boundary."],
      });
    }
  }
  return resolved;
}

function recoverHeadingFromWordsBeforeNextBoundary(
  target: ChapterCurationTargetBoundary,
  entry: EpubChapterEntry,
  nextDecision: NodeBoundaryDecision
): NodeBoundaryDecision | null {
  const beforeWords = nextDecision.audit.boundaryComparison.boundaryWords?.before ?? [];
  const recentWords = beforeWords.filter((word) => word.startTime >= nextDecision.startTime - 90 && word.endTime <= nextDecision.startTime + 1);
  const match = findSpokenTitleInBoundaryWords(spokenTitleVariants(entry.title), recentWords);
  if (!match) return null;
  return {
    accepted: true,
    kind: "node_boundary",
    spanPath: nextDecision.spanPath,
    epubNodeId: target.epubNodeId,
    epubIndex: target.epubIndex,
    title: target.title,
    startTime: match.startTime,
    notes: "Recovered from a spoken heading immediately before the next accepted EPUB boundary.",
    audit: {
      ...nextDecision.audit,
      epubNodeId: target.epubNodeId,
      title: target.title,
      startTime: match.startTime,
    },
  };
}

function findSpokenTitleInBoundaryWords(
  variants: string[][],
  words: TranscriptBoundaryWords["before"]
): { startTime: number } | null {
  const wordTokens = words.map((word) => normalizedWordTokens(word.text)[0] ?? "");
  for (const variant of variants) {
    if (variant.length === 0) continue;
    for (let index = 0; index <= wordTokens.length - variant.length; index++) {
      const candidate = wordTokens.slice(index, index + variant.length);
      if (candidate.every((token, offset) => token === variant[offset])) {
        return { startTime: words[index]?.startTime ?? 0 };
      }
    }
  }
  return null;
}

export async function resolveRecursiveChapterSpans(
  ctx: ChapterCurationContext,
  decide: (span: ChapterCurationSpan, targetBoundary: ChapterCurationTargetBoundary) => Promise<RecursiveSpanDecision | null>,
  options: { maxConcurrency?: number; reports?: RecursiveCurationReport[] } = {}
): Promise<SubmittedChapter[] | null> {
  const maxConcurrency = Math.max(1, options.maxConcurrency ?? 4);
  const reports = options.reports;
  let activeDecisions = 0;
  const waiters: Array<() => void> = [];

  async function withDecisionSlot<T>(fn: () => Promise<T>): Promise<T> {
    if (activeDecisions >= maxConcurrency) {
      await new Promise<void>((resolve) => waiters.push(resolve));
    }
    activeDecisions++;
    try {
      return await fn();
    } finally {
      activeDecisions--;
      waiters.shift()?.();
    }
  }

  async function visit(span: ChapterCurationSpan): Promise<SubmittedChapter[]> {
    function partialLeaf(reason: string, errors: string[]): SubmittedChapter[] {
      const chapters = automaticLeafChapters(ctx, span);
      logChapterCurationEvent(ctx, {
        type: "span-partial-leaf",
        message: `recursive span=${span.path} partial_leaf=1 chapters=${chapters.length} reason=${reason}`,
        span,
        chapters: chapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(chapters, 80),
        errors,
      });
      reports?.push({ ...span, outcome: "partial_leaf", errors, chapters: chapters.length, chapterPlan: chapters });
      return chapters;
    }

    if (spanInternalBoundaryCount(span) === 0) {
      const chapters = automaticLeafChapters(ctx, span);
      logChapterCurationEvent(ctx, {
        type: "span-auto-leaf",
        message: `recursive span=${span.path} auto_leaf=1 chapters=${chapters.length} reason=no_internal_boundaries`,
        span,
        chapters: chapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(chapters, 80),
      });
      reports?.push({ ...span, outcome: "leaf", chapters: chapters.length, chapterPlan: chapters });
      return chapters;
    }
    const targetBoundary = chooseTargetBoundary(ctx, span);
    if (!targetBoundary) {
      return partialLeaf("no_target_boundary", ["Span has unresolved boundaries but no target boundary could be chosen."]);
    }
    const decision = await withDecisionSlot(() => decide(span, targetBoundary));
    if (!decision) {
      logChapterCurationEvent(ctx, {
        type: "span-no-decision",
        message: `recursive span=${span.path} accepted=0 reason=no_decision`,
        span,
        targetBoundary,
      });
      return partialLeaf("no_decision", ["Span curator returned no accepted decision."]);
    }
    if (decision.split.epubNodeId !== targetBoundary.epubNodeId) {
      logChapterCurationEvent(ctx, {
        type: "span-wrong-target-split",
        message: `recursive span=${span.path} accepted=0 reason=wrong_target expected=${targetBoundary.epubNodeId} actual=${decision.split.epubNodeId}`,
        span,
        targetBoundary,
        split: {
          epubNodeId: decision.split.epubNodeId,
          title: decision.split.title,
          startTime: decision.split.startTime,
        },
      });
      return partialLeaf("wrong_target", [`Span curator submitted ${decision.split.epubNodeId} instead of assigned target ${targetBoundary.epubNodeId}.`]);
    }
    const { left, right } = splitSpan(span, decision.split);
    logChapterCurationEvent(ctx, {
      type: "span-split-accepted",
      message: `recursive span=${span.path} split accepted=1 epub=${decision.split.epubNodeId} time=${Math.round(decision.split.startTime)}s left=${left.path} right=${right.path}`,
      span,
      split: {
        epubNodeId: decision.split.epubNodeId,
        epubIndex: decision.split.epubIndex,
        title: decision.split.title,
        startTime: decision.split.startTime,
      },
      left,
      right,
      result: decision.result,
    });
    reports?.push({
      ...span,
      outcome: "split",
      split: {
        epubNodeId: decision.split.epubNodeId,
        title: decision.split.title,
        startTime: decision.split.startTime,
      },
    });
    const [leftChapters, rightChapters] = await Promise.all([visit(left), visit(right)]);
    return normalizeSpanChapters([...leftChapters, ...rightChapters]);
  }

  return visit(createRootCurationSpan(ctx));
}

export function applyAudibleEpubNodeSelection(ctx: ChapterCurationContext, selection: AudibleEpubNodeSelection | null): ChapterCurationContext {
  if (!selection) return ctx;
  const requestedIds = new Set(selection.audibleNodeIds);
  const selectedEntries = ctx.epubEntries.filter((entry) => requestedIds.has(entry.id));
  const audioOnlyIntervals = selection.audioOnlyIntervals
    .filter((interval) => interval.endTime >= interval.startTime && interval.endTime <= msToSeconds(ctx.durationMs))
    .sort((a, b) => a.startTime - b.startTime || a.endTime - b.endTime);
  if (selectedEntries.length === 0) return audioOnlyIntervals.length > 0 ? { ...ctx, audioOnlyIntervals } : ctx;
  if (selectedEntries.length === ctx.epubEntries.length) return audioOnlyIntervals.length > 0 ? { ...ctx, audioOnlyIntervals } : ctx;
  return { ...ctx, epubEntries: selectedEntries, audioOnlyIntervals };
}

function embeddedScopeTitleKey(value: string): string {
  return normalizeToolText(value).toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}

function embeddedScopeTitleMatches(entry: EpubChapterEntry, embeddedTitle: string): boolean {
  const entryKey = embeddedScopeTitleKey(entry.title);
  const embeddedKey = embeddedScopeTitleKey(embeddedTitle);
  if (!entryKey || !embeddedKey) return false;
  if (entryKey.length >= 3 && (entryKey === embeddedKey || embeddedKey.includes(entryKey))) return true;

  const structuralKinds = ["interlude", "part"];
  for (const kind of structuralKinds) {
    if (embeddedKey.includes(kind) !== entryKey.includes(kind)) return false;
  }

  const entryNumber = entryKey.match(/(?:^|\b)(?:chapter\s+)?(\d+)(?:\b|$)/u)?.[1];
  if (!entryNumber) return false;
  return new RegExp(`(?:^|\\b)(?:chapter\\s+)?${entryNumber}(?:\\b|$)`, "u").test(embeddedKey);
}

function localizeEpubRatios(entries: EpubChapterEntry[]): EpubChapterEntry[] {
  const totalWords = entries.reduce((sum, entry) => sum + Math.max(0, entry.wordCount), 0);
  if (totalWords <= 0) return entries;
  let cumulativeWords = 0;
  return entries.map((entry) => {
    cumulativeWords += Math.max(0, entry.wordCount);
    return {
      ...entry,
      cumulativeWords,
      cumulativeRatio: cumulativeWords / totalWords,
    };
  });
}

export function applyEmbeddedAudioChapterNodeScope(ctx: ChapterCurationContext): ChapterCurationContext {
  const embedded = getEmbeddedAudioChapters(ctx);
  const diagnostics = embedded.diagnostics;
  if (diagnostics.labelQuality !== "named" || diagnostics.durationPattern !== "varied" || diagnostics.boundaryDensity !== "plausible") return ctx;

  const matchedIds = new Set<string>();
  for (const entry of ctx.epubEntries) {
    if (embedded.chapters.some((chapter) => embeddedScopeTitleMatches(entry, chapter.title))) matchedIds.add(entry.id);
  }
  const matchedEntries = ctx.epubEntries.filter((entry) => matchedIds.has(entry.id));
  if (matchedEntries.length < 3 || matchedEntries.length === ctx.epubEntries.length) return ctx;

  const matchedRatio = matchedEntries.length / Math.max(1, ctx.epubEntries.length);
  if (matchedRatio > 0.8) return ctx;

  return {
    ...ctx,
    epubEntries: localizeEpubRatios(matchedEntries),
  };
}

const TRANSCRIPT_ENDPOINT_BOILERPLATE =
  /\b(?:graphic\s+audio|movie\s+in\s+your\s+mind|presents|all\s+rights\s+reserved|copyright|newsletter|facebook|twitter|graphicaudio|1-800|www\.|downloads?|podcasts?|loyalty|android|apple|listen\s+on\s+the\s+go)\b/iu;

function normalizedEndpointWords(value: string): string[] {
  return normalizeToolText(value)
    .toLowerCase()
    .split(/\s+/)
    .filter((word) => word.length > 0);
}

function endpointPhraseVariants(text: string): string[] {
  const words = normalizedEndpointWords(text);
  const variants: string[] = [];
  for (const start of [0, 2, 4]) {
    for (const count of [12, 10, 8, 6]) {
      const phrase = words.slice(start, start + count).join(" ");
      if (phrase.length >= 24) variants.push(phrase);
    }
  }
  return variants;
}

type EndpointUtteranceCandidate = { text: string; startTime: number; endTime: number };

function firstEndpointUtterances(ctx: ChapterCurationContext): EndpointUtteranceCandidate[] {
  const openingAudioOnlyEndMs = secondsToMs(
    Math.max(0, ...(ctx.audioOnlyIntervals ?? []).filter((interval) => interval.startTime <= 5).map((interval) => interval.endTime))
  );
  return transcriptUtterances(ctx)
    .filter((item) => item.endMs >= openingAudioOnlyEndMs && !TRANSCRIPT_ENDPOINT_BOILERPLATE.test(item.text))
    .slice(0, 40)
    .map((item) => ({ text: item.text, startTime: msToSeconds(item.startMs), endTime: msToSeconds(item.endMs) }));
}

function lastEndpointUtterances(ctx: ChapterCurationContext): EndpointUtteranceCandidate[] {
  return [...transcriptUtterances(ctx)]
    .reverse()
    .filter((item) => !TRANSCRIPT_ENDPOINT_BOILERPLATE.test(item.text))
    .slice(0, 80)
    .map((item) => ({ text: item.text, startTime: msToSeconds(item.startMs), endTime: msToSeconds(item.endMs) }));
}

function findEndpointEntryIndex(entries: EpubChapterEntry[], text: string): number | null {
  const normalizedEntries = entries.map((entry) => normalizeToolText(entry.text).toLowerCase());
  for (const phrase of endpointPhraseVariants(text)) {
    const matches = normalizedEntries
      .map((entryText, index) => (entryText.includes(phrase) ? index : -1))
      .filter((index) => index >= 0);
    if (matches.length === 1) return matches[0]!;
  }
  return null;
}

export function applyTranscriptEndpointEpubNodeScope(ctx: ChapterCurationContext): ChapterCurationContext {
  const startCandidate = firstEndpointUtterances(ctx).reduce<{ index: number; utterance: EndpointUtteranceCandidate } | null>(
    (found, utterance) =>
      found ??
      (() => {
        const index = findEndpointEntryIndex(ctx.epubEntries, utterance.text);
        return index === null ? null : { index, utterance };
      })(),
    null
  );
  const endCandidate = lastEndpointUtterances(ctx).reduce<{ index: number; utterance: EndpointUtteranceCandidate } | null>(
    (found, utterance) =>
      found ??
      (() => {
        const index = findEndpointEntryIndex(ctx.epubEntries, utterance.text);
        return index === null ? null : { index, utterance };
      })(),
    null
  );
  const startIndex = startCandidate?.index ?? null;
  const endIndex = endCandidate?.index ?? null;
  if (startIndex === null || endIndex === null || endIndex < startIndex) return ctx;

  const scopedEntries = ctx.epubEntries.slice(startIndex, endIndex + 1);
  if (scopedEntries.length < 3 || scopedEntries.length === ctx.epubEntries.length) return ctx;

  const scopedRatio = scopedEntries.length / Math.max(1, ctx.epubEntries.length);
  if (scopedRatio > 0.8) return ctx;

  return {
    ...ctx,
    epubEntries: localizeEpubRatios(scopedEntries),
    chapterStartTimeHints: {
      ...(ctx.chapterStartTimeHints ?? {}),
      [scopedEntries[0]!.id]: startCandidate!.utterance.startTime,
    },
  };
}

// logChapterCurationEvent is needed in resolveRecursiveChapterSpans; import it lazily to avoid circular dependency
// by importing from chapter-curation-debug directly
import {
  logChapterCurationEvent,
} from "./chapter-curation-debug";

// Export Zod schemas needed by runner
export { submitFulcrumSplitSchema, audibleEpubNodeSelectionSchema as audibleEpubNodeSelectionSchemaExport };
export { parseSpanDecisionOutput, parseNodeBoundaryOutput, parseFulcrumJudgmentOutput, parseAudibleEpubNodeSelectionOutput };
