import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { rgPath } from "@vscode/ripgrep";
import uFuzzy from "@leeoniya/ufuzzy";
import { Agent, OpenAIProvider, Runner, tool, type FunctionToolResult, type ToolsToFinalOutputResult } from "@openai/agents";
import { z } from "zod";

import type { AppSettings, AssetFileRow, AssetRow, BookRow, ManifestationRow } from "../app-types";
import type { EpubChapterEntry, StoredTranscriptPayload, StoredTranscriptUtterance } from "./chapter-analysis";
import {
  logAgentUsageEvent,
  logChapterCurationEvent,
  logSpanToolCall,
  logSpanToolError,
  logSpanToolResult,
  writeChapterCurationTrace,
} from "./chapter-curation-debug";

export type ChapterCurationTiming = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
};

export type ChapterCurationContainer = {
  asset: AssetRow;
  files: AssetFileRow[];
};

export type AudioOnlyInterval = {
  startTime: number;
  endTime: number;
  kind: "publisher_intro" | "credits" | "recap" | "part_bumper" | "back_matter" | "other";
  notes: string;
};

export type ChapterCurationContext = {
  book: BookRow;
  manifestation: ManifestationRow;
  containers: ChapterCurationContainer[];
  settings: AppSettings;
  durationMs: number;
  epubEntries: EpubChapterEntry[];
  transcript: StoredTranscriptPayload;
  embeddedChapters: ChapterCurationTiming[];
  audioOnlyIntervals?: AudioOnlyInterval[];
  debugEventLogPath?: string;
  debugTraceDir?: string;
  debugReasoningSummary?: "auto" | "concise" | "detailed";
  debugReasoningEffort?: "none" | "minimal" | "low" | "medium" | "high" | "xhigh";
  debugCuratorModel?: string;
  debugJudgeModel?: string;
};

export type ChapterCurationSpanBoundary = {
  epubNodeId: string;
  epubIndex: number;
  title: string;
  startTime: number;
  source: "parent_split";
};

export type ChapterCurationSpan = {
  epubStartIndex: number;
  epubEndIndex: number;
  startTime: number;
  endTime: number;
  depth: number;
  path: string;
  startBoundary?: ChapterCurationSpanBoundary;
};

export type ChapterCurationTargetBoundary = {
  epubNodeId: string;
  epubIndex: number;
  title: string;
  expectedStartTime: number;
  localNodeRatio: number;
};

export type FulcrumValidationAudit = {
  epubNodeId: string | null;
  title: string;
  startTime: number;
  boundaryComparison: {
    transcriptPrecision: "word" | "utterance";
    transcriptPrecisionNote: string | null;
    previousEpub: {
      epubNodeId: string | null;
      title: string | null;
      tailText: string;
    };
    targetEpub: {
      epubNodeId: string | null;
      title: string;
      headText: string;
      bodyHeadText?: string;
      optionalHeadingText?: string;
      headingMayBeUnspoken?: boolean;
    };
    transcriptBefore: string;
    transcriptAfter: string;
    boundaryWords?: TranscriptBoundaryWords;
  };
  transcriptWindow: string;
  candidates: BoundaryEvidenceCandidate[];
};

export type TranscriptWindow = {
  startMs: number;
  endMs: number;
  utterances: StoredTranscriptUtterance[];
  text: string;
  audioOnlyIntervals?: AudioOnlyInterval[];
  boundaryWords?: TranscriptBoundaryWords;
};

export type TranscriptBoundaryWords = {
  before: Array<{ text: string; startTime: number; endTime: number }>;
  containing: Array<{ text: string; startTime: number; endTime: number }>;
  after: Array<{ text: string; startTime: number; endTime: number }>;
  nearestCleanBoundaryTimes: number[];
};

export type TranscriptSearchScope = {
  startTime?: number;
  endTime?: number;
};

export type TranscriptSearchMatch = {
  index: number;
  startTime: number;
  endTime: number;
  text: string;
  before: TranscriptWindow;
  after: TranscriptWindow;
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

function transcriptWords(ctx: Pick<ChapterCurationContext, "transcript">): Array<{ text: string; token: string; startMs: number; endMs: number }> {
  return [...ctx.transcript.words].sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
}

export function normalizeToolText(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

export function secondsToMs(value: number): number {
  return Math.max(0, Math.round(value * 1000));
}

export function msToSeconds(value: number): number {
  return value / 1000;
}

export function transcriptUtterances(ctx: Pick<ChapterCurationContext, "transcript">): StoredTranscriptUtterance[] {
  return [...(ctx.transcript.utterances ?? [])].sort((a, b) => a.startMs - b.startMs);
}

export function getTranscriptWindowFromContext(
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs" | "audioOnlyIntervals">,
  startMs: number,
  radiusMs: number
): TranscriptWindow {
  const windowStart = Math.max(0, startMs - radiusMs);
  const windowEnd = Math.min(ctx.durationMs, startMs + radiusMs);
  const utterances = transcriptUtterances(ctx).filter((utterance) => {
    if (utterance.endMs < windowStart) return false;
    if (utterance.startMs > windowEnd) return false;
    return true;
  });
  return {
    startMs: windowStart,
    endMs: windowEnd,
    utterances,
    text: utterances.map((utterance) => normalizeToolText(utterance.text)).filter(Boolean).join(" "),
    audioOnlyIntervals: (ctx.audioOnlyIntervals ?? []).filter(
      (interval) => secondsToMs(interval.endTime) >= windowStart && secondsToMs(interval.startTime) <= windowEnd
    ),
    boundaryWords: transcriptBoundaryWords(ctx, startMs),
  };
}

export function summarizeFirstWords(entry: EpubChapterEntry, limit = 40): string {
  return entry.words.slice(0, limit).map((word) => word.text).join(" ").trim();
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

function summarizeFirstBodyWords(entry: EpubChapterEntry, limit = 40): string {
  const firstBodyIndex = entry.words.findIndex((word) => word.kind === "body");
  const start = firstBodyIndex >= 0 ? firstBodyIndex : entryTitlePrefixWordCount(entry);
  return entry.words.slice(start, start + limit).map((word) => word.text).join(" ").trim();
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

export function summarizeLastWords(entry: EpubChapterEntry, limit = 40): string {
  return entry.words.slice(Math.max(0, entry.words.length - limit)).map((word) => word.text).join(" ").trim();
}

export function inferEntryStartRatio(entries: EpubChapterEntry[], index: number): number {
  if (index <= 0) return 0;
  const previous = entries[index - 1];
  return previous ? previous.cumulativeRatio : 0;
}

export function inferEntryEndRatio(entries: EpubChapterEntry[], index: number): number {
  const entry = entries[index];
  return entry ? entry.cumulativeRatio : 1;
}

export type EpubStructureNode = {
  id: string;
  title: string;
  href: string;
  index: number;
  wordCount: number;
  cumulativeRatio: number;
  startRatio: number;
  endRatio: number;
  firstWords: string;
};

export type EpubStructureResult = {
  book: {
    id: number;
    title: string;
    author: string;
  };
  totalWordCount: number;
  nodes: EpubStructureNode[];
};

export type EpubNodeTextResult = {
  id: string;
  title: string;
  index: number;
  startWord: number;
  endWord: number;
  wordCount: number;
  totalWords: number;
  text: string;
  phraseVariants: Array<{
    startWord: number;
    wordCount: number;
    text: string;
  }>;
};

export type EpubTextSearchResult = {
  query: string;
  matches: Array<{
    epubNodeId: string;
    epubIndex: number;
    title: string;
    wordOffset: number;
    wordRatioWithinNode: number;
    targetNodeDistance: number | null;
    targetWordOffset: number | null;
    relationToTarget: "unknown" | "pre_target" | "opener" | "near_opener" | "interior" | "post_target";
    text: string;
  }>;
};

const GENERIC_EPUB_OPENER_TOKENS = new Set([
  "chapter",
  "chapters",
  "part",
  "book",
  "prologue",
  "epilogue",
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
  "thirty",
  "forty",
  "fifty",
]);

const GENERIC_AUDIO_CHAPTER_TITLE_RE = [
  /^\s*chapter\s+[0-9a-z]+\s*$/i,
  /^\s*ch\.?\s*[0-9a-z]+\s*$/i,
  /^\s*[0-9]{1,4}\s*$/i,
  /^\s*track\s+[0-9a-z]+\s*$/i,
  /^\s*part\s+[0-9a-z]+\s*$/i,
];

export function getEpubStructure(ctx: Pick<ChapterCurationContext, "book" | "epubEntries">): EpubStructureResult {
  return {
    book: {
      id: ctx.book.id,
      title: ctx.book.title,
      author: ctx.book.author,
    },
    totalWordCount: ctx.epubEntries.reduce((sum, entry) => sum + entry.wordCount, 0),
    nodes: ctx.epubEntries.map((entry, index) => ({
      id: entry.id,
      title: entry.title,
      href: entry.href,
      index,
      wordCount: entry.wordCount,
      cumulativeRatio: entry.cumulativeRatio,
      startRatio: inferEntryStartRatio(ctx.epubEntries, index),
      endRatio: inferEntryEndRatio(ctx.epubEntries, index),
      firstWords: summarizeFirstWords(entry),
    })),
  };
}

function phraseVariant(words: EpubChapterEntry["words"], startWord: number, wordCount: number): EpubNodeTextResult["phraseVariants"][number] | null {
  const text = words.slice(startWord, startWord + wordCount).map((word) => word.text).join(" ").trim();
  return text ? { startWord, wordCount: text.split(/\s+/).length, text } : null;
}

function firstSearchableEpubWordOffset(entry: EpubChapterEntry, startWord: number, endWord: number): number {
  const titleTokens = new Set(textTokens(entry.title));
  let offset = startWord;
  while (offset < endWord && offset - startWord < 8) {
    const token = entry.words[offset]?.token;
    if (!token || (!titleTokens.has(token) && !GENERIC_EPUB_OPENER_TOKENS.has(token))) break;
    offset++;
  }
  return offset;
}

function epubPhraseVariants(entry: EpubChapterEntry, startWord: number, endWord: number): EpubNodeTextResult["phraseVariants"] {
  const searchStartWord = firstSearchableEpubWordOffset(entry, startWord, endWord);
  const offsets = [searchStartWord, searchStartWord + 4, searchStartWord + 8, searchStartWord + 16, searchStartWord + 32].filter((offset) => offset < endWord);
  const variants: EpubNodeTextResult["phraseVariants"] = [];
  const seen = new Set<string>();
  for (const offset of offsets) {
    for (const count of [6, 10]) {
      const variant = phraseVariant(entry.words, offset, Math.min(count, endWord - offset));
      const key = variant?.text.toLowerCase();
      if (!variant || !key || seen.has(key)) continue;
      seen.add(key);
      variants.push(variant);
    }
  }
  return variants.slice(0, 8);
}

export function getEpubNodeText(
  ctx: Pick<ChapterCurationContext, "epubEntries">,
  input: { epubNodeId: string; startWord?: number; wordCount?: number }
): EpubNodeTextResult | null {
  const index = ctx.epubEntries.findIndex((entry) => entry.id === input.epubNodeId);
  const entry = ctx.epubEntries[index];
  if (!entry) return null;
  const startWord = Math.min(Math.max(0, Math.floor(input.startWord ?? 0)), entry.words.length);
  const requestedWords = Math.min(180, Math.max(1, Math.floor(input.wordCount ?? 90)));
  const endWord = Math.min(entry.words.length, startWord + requestedWords);
  const text = entry.words.slice(startWord, endWord).map((word) => word.text).join(" ").trim();
  return {
    id: entry.id,
    title: entry.title,
    index,
    startWord,
    endWord,
    wordCount: Math.max(0, endWord - startWord),
    totalWords: entry.words.length,
    text,
    phraseVariants: epubPhraseVariants(entry, startWord, endWord),
  };
}

export function searchEpubText(
  ctx: Pick<ChapterCurationContext, "epubEntries">,
  input: { query: string; nodeIds?: string[]; targetNodeId?: string; limit?: number }
): EpubTextSearchResult {
  const queryTokens = normalizedWordTokens(input.query).filter((token) => token.length > 1);
  const requestedIds = new Set(input.nodeIds?.map((id) => id.trim()).filter(Boolean));
  const targetIndex = input.targetNodeId ? ctx.epubEntries.findIndex((entry) => entry.id === input.targetNodeId) : -1;
  const limit = Math.max(1, Math.min(20, input.limit ?? 10));
  const matches: Array<EpubTextSearchResult["matches"][number] & { matchCount: number; sequenceScore: number }> = [];
  if (queryTokens.length === 0) return { query: input.query, matches };

  for (const [epubIndex, entry] of ctx.epubEntries.entries()) {
    if (requestedIds.size > 0 && !requestedIds.has(entry.id)) continue;
    const windowSize = Math.min(entry.words.length, Math.max(queryTokens.length + 6, Math.min(48, queryTokens.length * 3)));
    for (let offset = 0; offset < entry.words.length; offset++) {
      const window = entry.words.slice(offset, offset + windowSize);
      if (window.length === 0) continue;
      const windowWordTokens = window.map(epubWordToken).filter(Boolean);
      const windowTokens = new Set(windowWordTokens);
      const matchedQueryTerms = queryTokens.filter((token, index) => windowTokens.has(token) && queryTokens.indexOf(token) === index);
      if (matchedQueryTerms.length < Math.min(3, queryTokens.length)) continue;

      let orderedMatches = 0;
      let searchFrom = 0;
      for (const token of queryTokens) {
        const found = windowWordTokens.findIndex((wordToken, wordIndex) => wordIndex >= searchFrom && wordToken === token);
        if (found < 0) continue;
        orderedMatches++;
        searchFrom = found + 1;
      }
      if (orderedMatches < Math.min(3, queryTokens.length)) continue;

      const targetWordOffset =
        targetIndex < 0
          ? null
          : epubIndex === targetIndex
            ? offset
            : epubIndex < targetIndex
              ? offset - ctx.epubEntries.slice(epubIndex, targetIndex).reduce((sum, item) => sum + item.wordCount, 0)
              : ctx.epubEntries.slice(targetIndex, epubIndex).reduce((sum, item) => sum + item.wordCount, 0) + offset;
      const sequenceScore = orderedMatches / queryTokens.length;
      matches.push({
        epubNodeId: entry.id,
        epubIndex,
        title: entry.title,
        wordOffset: offset,
        wordRatioWithinNode: entry.words.length === 0 ? 0 : Math.round((offset / entry.words.length) * 1000) / 1000,
        targetNodeDistance: targetIndex < 0 ? null : epubIndex - targetIndex,
        targetWordOffset,
        relationToTarget: classifyEpubTextMatch(targetIndex < 0 ? null : epubIndex - targetIndex, targetWordOffset),
        matchCount: matchedQueryTerms.length,
        sequenceScore: Math.round(sequenceScore * 1000) / 1000,
        text: normalizeToolText(window.map((word) => word.text).join(" ")).slice(0, 500),
      });
    }
  }

  matches.sort((a, b) => {
    const aTextScore = a.matchCount / queryTokens.length;
    const bTextScore = b.matchCount / queryTokens.length;
    const aDistance = a.targetNodeDistance === null ? 0 : Math.abs(a.targetNodeDistance);
    const bDistance = b.targetNodeDistance === null ? 0 : Math.abs(b.targetNodeDistance);
    return (
      bTextScore - aTextScore ||
      b.sequenceScore - a.sequenceScore ||
      epubTextRelationRank(a.relationToTarget) - epubTextRelationRank(b.relationToTarget) ||
      aDistance - bDistance ||
      a.epubIndex - b.epubIndex ||
      a.wordOffset - b.wordOffset
    );
  });

  return {
    query: input.query,
    matches: matches.slice(0, limit).map(({ matchCount: _matchCount, sequenceScore: _sequenceScore, ...match }) => match),
  };
}

function epubWordToken(word: EpubChapterEntry["words"][number]): string {
  return normalizedWordTokens(word.token || word.text)[0] || "";
}

function classifyEpubTextMatch(targetNodeDistance: number | null, targetWordOffset: number | null): EpubTextSearchResult["matches"][number]["relationToTarget"] {
  if (targetWordOffset === null) return "unknown";
  if (targetNodeDistance !== null && targetNodeDistance > 0) return "post_target";
  if (targetWordOffset < 0) return "pre_target";
  if (targetWordOffset <= 8) return "opener";
  if (targetWordOffset <= 50) return "near_opener";
  return "interior";
}

function epubTextRelationRank(relation: EpubTextSearchResult["matches"][number]["relationToTarget"]): number {
  switch (relation) {
    case "opener":
      return 0;
    case "near_opener":
      return 1;
    case "interior":
      return 2;
    case "pre_target":
      return 3;
    case "post_target":
      return 4;
    case "unknown":
      return 5;
  }
}

function coefficientOfVariation(values: number[]): number | null {
  if (values.length === 0) return null;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  if (mean <= 0) return null;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance) / mean;
}

function isGenericAudioChapterTitle(title: string): boolean {
  return GENERIC_AUDIO_CHAPTER_TITLE_RE.some((re) => re.test(title));
}

export type EmbeddedAudioChapterDiagnostics = {
  count: number;
  labelQuality: "missing" | "named" | "generic" | "repeated";
  durationPattern: "none" | "varied" | "suspiciously_even";
  boundaryDensity: "none" | "sparse" | "plausible" | "dense";
  repeatedTitle: string | null;
  genericTitleCount: number;
  averageDurationMs: number | null;
  durationCoefficientOfVariation: number | null;
};

export type EmbeddedAudioChaptersResult = {
  chapters: Array<{
    id: string;
    title: string;
    startTime: number;
    endTime: number;
    durationSeconds: number;
  }>;
  diagnostics: EmbeddedAudioChapterDiagnostics;
};

export type EmbeddedAudioChapterCurationAssessment = {
  action: "ignore" | "seed_boundaries" | "short_circuit_candidate";
  confidence: "low" | "medium" | "high";
  reason: string;
  matchedEpubNodeIds: string[];
};

export function getEmbeddedAudioChapters(ctx: Pick<ChapterCurationContext, "durationMs" | "embeddedChapters">): EmbeddedAudioChaptersResult {
  const sorted = [...ctx.embeddedChapters].sort((a, b) => a.startMs - b.startMs);
  const titles = sorted.map((chapter) => normalizeToolText(chapter.title)).filter(Boolean);
  const genericTitleCount = titles.filter(isGenericAudioChapterTitle).length;
  const titleCounts = new Map<string, number>();
  for (const title of titles) titleCounts.set(title.toLowerCase(), (titleCounts.get(title.toLowerCase()) ?? 0) + 1);
  const repeatedTitleEntry = Array.from(titleCounts.entries()).sort((a, b) => b[1] - a[1])[0];
  const repeatedTitle = repeatedTitleEntry && repeatedTitleEntry[1] >= Math.max(3, Math.ceil(sorted.length * 0.5)) ? repeatedTitleEntry[0] : null;
  const durations = sorted.map((chapter, index) => {
    const next = sorted[index + 1];
    const endMs = next ? Math.min(chapter.endMs, next.startMs) : chapter.endMs;
    return Math.max(0, endMs - chapter.startMs);
  });
  const durationCv = coefficientOfVariation(durations.filter((duration) => duration > 0));
  const chaptersPerHour = ctx.durationMs > 0 ? sorted.length / (ctx.durationMs / 3_600_000) : 0;
  const labelQuality =
    sorted.length === 0 || titles.length === 0
      ? "missing"
      : repeatedTitle
        ? "repeated"
        : genericTitleCount >= Math.ceil(titles.length * 0.7)
          ? "generic"
          : "named";
  const durationPattern =
    sorted.length === 0
      ? "none"
      : sorted.length >= 5 && durationCv !== null && durationCv < 0.08
        ? "suspiciously_even"
        : "varied";
  const boundaryDensity =
    sorted.length === 0 ? "none" : chaptersPerHour < 1 ? "sparse" : chaptersPerHour > 30 ? "dense" : "plausible";

  return {
    chapters: sorted.map((chapter) => ({
      id: chapter.id,
      title: chapter.title,
      startTime: msToSeconds(chapter.startMs),
      endTime: msToSeconds(chapter.endMs),
      durationSeconds: msToSeconds(Math.max(0, chapter.endMs - chapter.startMs)),
    })),
    diagnostics: {
      count: sorted.length,
      labelQuality,
      durationPattern,
      boundaryDensity,
      repeatedTitle,
      genericTitleCount,
      averageDurationMs: durations.length === 0 ? null : durations.reduce((sum, duration) => sum + duration, 0) / durations.length,
      durationCoefficientOfVariation: durationCv,
    },
  };
}

function normalizedTitleKey(value: string): string {
  return normalizeToolText(value).toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}

export function assessEmbeddedAudioChaptersForCuration(
  ctx: Pick<ChapterCurationContext, "durationMs" | "embeddedChapters" | "epubEntries">
): EmbeddedAudioChapterCurationAssessment {
  const embedded = getEmbeddedAudioChapters(ctx);
  const diagnostics = embedded.diagnostics;
  if (diagnostics.count === 0) {
    return {
      action: "ignore",
      confidence: "high",
      reason: "No embedded audio chapter markers are available.",
      matchedEpubNodeIds: [],
    };
  }

  const epubTitleToIds = new Map<string, string[]>();
  for (const entry of ctx.epubEntries) {
    const key = normalizedTitleKey(entry.title);
    if (!key) continue;
    const ids = epubTitleToIds.get(key) ?? [];
    ids.push(entry.id);
    epubTitleToIds.set(key, ids);
  }
  const matchedEpubNodeIds = embedded.chapters
    .flatMap((chapter) => epubTitleToIds.get(normalizedTitleKey(chapter.title)) ?? [])
    .filter((id, index, ids) => ids.indexOf(id) === index);
  const countDelta = Math.abs(diagnostics.count - ctx.epubEntries.length);
  const closeCount = countDelta <= Math.max(2, Math.ceil(ctx.epubEntries.length * 0.1));

  if (diagnostics.labelQuality === "named" && diagnostics.boundaryDensity === "plausible" && diagnostics.durationPattern === "varied" && closeCount) {
    return {
      action: "short_circuit_candidate",
      confidence: matchedEpubNodeIds.length >= Math.min(3, ctx.epubEntries.length) ? "high" : "medium",
      reason:
        "Embedded audio markers have named, varied, plausible boundaries and roughly match the curated EPUB node count; they are worth validating before recursive transcript search.",
      matchedEpubNodeIds,
    };
  }

  if (diagnostics.labelQuality === "generic" && diagnostics.boundaryDensity === "plausible" && diagnostics.durationPattern === "varied" && closeCount) {
    return {
      action: "seed_boundaries",
      confidence: "medium",
      reason:
        "Embedded audio markers are generic but varied and close to the EPUB node count, so they may be useful as boundary priors but should not replace transcript validation.",
      matchedEpubNodeIds,
    };
  }

  return {
    action: "ignore",
    confidence:
      diagnostics.durationPattern === "suspiciously_even" || diagnostics.boundaryDensity === "sparse" || diagnostics.boundaryDensity === "dense" ? "high" : "medium",
    reason: `Embedded audio markers are not trustworthy enough for a pre-pass (labels=${diagnostics.labelQuality}, durations=${diagnostics.durationPattern}, density=${diagnostics.boundaryDensity}, countDelta=${countDelta}).`,
    matchedEpubNodeIds,
  };
}

function scopedTranscriptUtterances(
  ctx: Pick<ChapterCurationContext, "transcript">,
  scope?: TranscriptSearchScope
): Array<StoredTranscriptUtterance & { index: number }> {
  const startMs = scope?.startTime === undefined ? 0 : secondsToMs(scope.startTime);
  const endMs = scope?.endTime === undefined ? Number.POSITIVE_INFINITY : secondsToMs(scope.endTime);
  return transcriptUtterances(ctx)
    .map((utterance, index) => ({ ...utterance, index }))
    .filter((utterance) => utterance.endMs >= startMs && utterance.startMs <= endMs);
}

function runRipgrep(args: string[]): Promise<{ status: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(rgPath, args, { stdio: ["ignore", "pipe", "pipe"] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk) => stdout.push(Buffer.from(chunk)));
    child.stderr.on("data", (chunk) => stderr.push(Buffer.from(chunk)));
    child.on("error", reject);
    child.on("close", (status) => {
      resolve({
        status: status ?? 0,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      });
    });
  });
}

function parseRipgrepLine(line: string): number | null {
  try {
    const event = JSON.parse(line) as {
      type?: string;
      data?: { line_number?: number };
    };
    if (event.type !== "match") return null;
    const lineNumber = event.data?.line_number;
    return typeof lineNumber === "number" ? lineNumber - 1 : null;
  } catch {
    return null;
  }
}

export type RgSearchTranscriptInput = {
  pattern: string;
  regex?: boolean;
  scope?: TranscriptSearchScope;
  beforeSeconds?: number;
  afterSeconds?: number;
  limit?: number;
};

export async function rgSearchTranscript(
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs">,
  input: RgSearchTranscriptInput
): Promise<{ matches: TranscriptSearchMatch[] }> {
  const pattern = input.pattern.trim();
  if (!pattern) return { matches: [] };
  const utterances = scopedTranscriptUtterances(ctx, input.scope);
  if (utterances.length === 0) return { matches: [] };

  const dir = await mkdtemp(path.join(os.tmpdir(), "podible-rg-transcript-"));
  const transcriptPath = path.join(dir, "transcript.txt");
  try {
    await writeFile(
      transcriptPath,
      utterances.map((utterance) => `${utterance.index}\t${utterance.startMs}\t${utterance.endMs}\t${normalizeToolText(utterance.text)}`).join("\n"),
      "utf8"
    );
    const args = [
      "--json",
      "--line-number",
      "--color",
      "never",
      "--max-count",
      String(Math.max(1, Math.min(100, input.limit ?? 20))),
      ...(input.regex ? [] : ["--fixed-strings"]),
      "--",
      pattern,
      transcriptPath,
    ];
    const result = await runRipgrep(args);
    if (result.status !== 0 && result.status !== 1) {
      throw new Error(result.stderr.trim() || `rg exited with status ${result.status}`);
    }
    const byLine = new Map(utterances.map((utterance, lineIndex) => [lineIndex, utterance]));
    const seen = new Set<number>();
    const matches: TranscriptSearchMatch[] = [];
    for (const line of result.stdout.split(/\n/)) {
      if (!line.trim()) continue;
      const lineIndex = parseRipgrepLine(line);
      if (lineIndex === null || seen.has(lineIndex)) continue;
      seen.add(lineIndex);
      const utterance = byLine.get(lineIndex);
      if (!utterance) continue;
      matches.push({
        index: utterance.index,
        startTime: msToSeconds(utterance.startMs),
        endTime: msToSeconds(utterance.endMs),
        text: utterance.text,
        before: getTranscriptWindowFromContext(ctx, utterance.startMs, secondsToMs(input.beforeSeconds ?? 0)),
        after: getTranscriptWindowFromContext(ctx, utterance.endMs, secondsToMs(input.afterSeconds ?? 10)),
      });
    }
    if (!input.regex) {
      const wordMatches = fixedPhraseWordMatches(ctx, pattern, input.scope, Math.max(1, Math.min(100, input.limit ?? 20)));
      for (const match of wordMatches) {
        if (matches.length >= Math.max(1, Math.min(100, input.limit ?? 20))) break;
        if (matches.some((existing) => Math.abs(existing.startTime - match.startTime) < 1)) continue;
        matches.push({
          ...match,
          before: getTranscriptWindowFromContext(ctx, secondsToMs(match.startTime), secondsToMs(input.beforeSeconds ?? 0)),
          after: getTranscriptWindowFromContext(ctx, secondsToMs(match.endTime), secondsToMs(input.afterSeconds ?? 10)),
        });
      }
    }
    return { matches: matches.sort((a, b) => a.startTime - b.startTime).slice(0, Math.max(1, Math.min(100, input.limit ?? 20))) };
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function fixedPhraseWordMatches(
  ctx: Pick<ChapterCurationContext, "transcript">,
  pattern: string,
  scope: TranscriptSearchScope | undefined,
  limit: number
): Array<Omit<TranscriptSearchMatch, "before" | "after">> {
  const queryTokens = normalizedSearchTokens(pattern);
  if (queryTokens.length === 0) return [];
  const startMs = secondsToMs(scope?.startTime ?? 0);
  const endMs = secondsToMs(scope?.endTime ?? Number.POSITIVE_INFINITY);
  const words = transcriptWords(ctx).filter((word) => word.endMs >= startMs && word.startMs <= endMs);
  const matches: Array<Omit<TranscriptSearchMatch, "before" | "after">> = [];

  for (let index = 0; index < words.length && matches.length < limit; index++) {
    const window = words.slice(index, index + queryTokens.length);
    if (window.length < queryTokens.length) break;
    const ok = queryTokens.every((token, queryIndex) => transcriptWordSearchToken(window[queryIndex]) === token);
    if (!ok) continue;
    matches.push({
      index,
      startTime: msToSeconds(window[0]!.startMs),
      endTime: msToSeconds(window.at(-1)!.endMs),
      text: normalizeToolText(window.map((word) => word.text).join(" ")),
    });
  }

  return matches;
}

function normalizedSearchTokens(value: string): string[] {
  return normalizedWordTokens(value);
}

function normalizedWordTokens(value: string): string[] {
  return value
    .toLowerCase()
    .match(/[a-z0-9]+(?:'[a-z0-9]+)?/g)
    ?.map((token) => token.replace(/[^a-z0-9]+/g, ""))
    .filter(Boolean) ?? [];
}

function transcriptWordSearchToken(word: { text: string; token?: string } | undefined): string {
  if (!word) return "";
  return normalizedWordTokens(word.token || word.text)[0] || "";
}

export type FuzzySearchTranscriptInput = {
  query: string;
  scope?: TranscriptSearchScope;
  limit?: number;
};

export async function fuzzySearchTranscript(
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs">,
  input: FuzzySearchTranscriptInput
): Promise<{ matches: TranscriptSearchMatch[] }> {
  const query = input.query.trim();
  if (!query) return { matches: [] };
  const utterances = scopedTranscriptUtterances(ctx, input.scope);
  if (utterances.length === 0) return { matches: [] };
  const haystack = utterances.map((utterance) => normalizeToolText(utterance.text));
  const fuzzy = new uFuzzy({ intraMode: 1, intraIns: 1, intraSub: 1, intraTrn: 1, intraDel: 1 });
  const [idxs, info, order] = fuzzy.search(haystack, query, 5);
  if (!idxs) return { matches: [] };
  const orderedIndexes =
    info && order ? order.map((infoIndex) => info.idx[infoIndex]).filter((index): index is number => typeof index === "number") : idxs;
  const matches: TranscriptSearchMatch[] = [];
  for (const haystackIndex of orderedIndexes.slice(0, Math.max(1, Math.min(100, input.limit ?? 20)))) {
    const utterance = utterances[haystackIndex];
    if (!utterance) continue;
    matches.push({
      index: utterance.index,
      startTime: msToSeconds(utterance.startMs),
      endTime: msToSeconds(utterance.endMs),
      text: utterance.text,
      before: getTranscriptWindowFromContext(ctx, utterance.startMs, 0),
      after: getTranscriptWindowFromContext(ctx, utterance.endMs, 10_000),
    });
  }
  return { matches };
}

export type BoundaryEvidenceCandidate = {
  startTime: number;
  endTime: number;
  text: string;
  afterText: string;
  quality: "none" | "weak" | "medium" | "strong";
};

function evidenceQuality(overlapCount: number): BoundaryEvidenceCandidate["quality"] {
  if (overlapCount >= 5) return "strong";
  if (overlapCount >= 2) return "medium";
  if (overlapCount >= 1) return "weak";
  return "none";
}

function transcriptWordEvidenceMatches(
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs">,
  queryTokens: string[],
  scope: TranscriptSearchScope,
  limit: number
): Array<BoundaryEvidenceCandidate & { score: number }> {
  const distinctQueryTokens = queryTokens.filter((token, index) => token.length >= 4 && queryTokens.indexOf(token) === index);
  if (distinctQueryTokens.length === 0) return [];
  const startMs = secondsToMs(scope.startTime ?? 0);
  const endMs = secondsToMs(scope.endTime ?? msToSeconds(ctx.durationMs));
  const words = [...ctx.transcript.words]
    .filter((word) => word.endMs >= startMs && word.startMs <= endMs)
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  const candidates: Array<BoundaryEvidenceCandidate & { score: number }> = [];
  const windowSize = Math.min(56, Math.max(24, distinctQueryTokens.length * 4));

  for (let index = 0; index < words.length; index++) {
    const window = words.slice(index, index + windowSize);
    if (window.length === 0) continue;
    const windowTokens = new Set(window.map((word) => word.token || textTokens(word.text)[0] || "").filter(Boolean));
    const matchedQueryTerms = distinctQueryTokens.filter((token) => windowTokens.has(token));
    if (matchedQueryTerms.length === 0) continue;
    const evidenceStartIndex = window.findIndex((word) => matchedQueryTerms.includes(word.token || textTokens(word.text)[0] || ""));
    const alignedWindow = evidenceStartIndex > 0 ? window.slice(evidenceStartIndex) : window;
    const orderedBonus = distinctQueryTokens.reduce((score, token, tokenIndex) => {
      const foundIndex = window.findIndex((word) => (word.token || textTokens(word.text)[0]) === token);
      return foundIndex >= 0 && foundIndex <= tokenIndex * 6 + 12 ? score + 0.25 : score;
    }, 0);
    candidates.push({
      startTime: msToSeconds(alignedWindow[0]!.startMs),
      endTime: msToSeconds(alignedWindow.at(-1)!.endMs),
      text: normalizeToolText(alignedWindow.map((word) => word.text).join(" ")),
      afterText: normalizeToolText(getTranscriptWindowFromContext(ctx, alignedWindow[0]!.startMs, 20_000).text).slice(0, 500),
      quality: evidenceQuality(matchedQueryTerms.length),
      score: matchedQueryTerms.length + orderedBonus,
    });
  }

  const deduped: Array<BoundaryEvidenceCandidate & { score: number }> = [];
  for (const candidate of candidates.sort((a, b) => b.score - a.score || a.startTime - b.startTime)) {
    if (deduped.some((existing) => Math.abs(existing.startTime - candidate.startTime) < 30)) continue;
    deduped.push(candidate);
    if (deduped.length >= limit) break;
  }

  return deduped;
}

export async function researchEpubBoundary(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs" | "transcript">,
  input: ResearchEpubBoundaryInput
): Promise<ResearchEpubBoundaryResult | null> {
  const epubIndex = ctx.epubEntries.findIndex((entry) => entry.id === input.epubNodeId);
  const entry = ctx.epubEntries[epubIndex];
  if (!entry) return null;
  const estimate = estimateTimestampFromEpubPosition(ctx, { epubNodeId: entry.id });
  if (!estimate) return null;
  const expectedStartTime = input.expectedTime ?? estimate.estimatedStartTime;
  const spanScope = input.scope ?? { startTime: 0, endTime: msToSeconds(ctx.durationMs) };
  const phraseLimit = Math.max(1, Math.min(12, input.phraseLimit ?? 8));
  const hitLimitPerPhrase = Math.max(1, Math.min(5, input.hitLimitPerPhrase ?? 3));
  const baseRadius = Math.max(120, Math.min(7_200, input.searchRadiusSeconds ?? 1_200));
  const scopeStart = spanScope.startTime ?? 0;
  const scopeEnd = spanScope.endTime ?? msToSeconds(ctx.durationMs);
  const anchorPhrases = generateEpubBoundaryAnchorPhrases(ctx, entry, phraseLimit);
  const hits: Array<EpubBoundaryResearchHit & { score: number }> = [];
  const searchedScopes = new Set<string>();

  const searchPhraseLimit = Math.min(anchorPhrases.length, Math.max(5, Math.min(8, phraseLimit)));
  for (const phrase of anchorPhrases.slice(0, searchPhraseLimit)) {
    const radii = Array.from(new Set([baseRadius, Math.min(baseRadius * 2, Math.max(baseRadius, scopeEnd - scopeStart))]));
    for (const radius of radii) {
      const searchScope = {
        startTime: Math.max(scopeStart, expectedStartTime - radius),
        endTime: Math.min(scopeEnd, expectedStartTime + radius),
      };
      const searchKey = `${phrase.text}:${Math.round(searchScope.startTime)}:${Math.round(searchScope.endTime)}`;
      if (searchedScopes.has(searchKey)) continue;
      searchedScopes.add(searchKey);
      const transcriptHits = await rgSearchTranscript(ctx, {
        pattern: phrase.text,
        scope: searchScope,
        beforeSeconds: 5,
        afterSeconds: 20,
        limit: hitLimitPerPhrase,
      });
      const matches =
        transcriptHits.matches.length > 0
          ? transcriptHits.matches
          : (
              await fuzzySearchTranscript(ctx, {
                query: phrase.text,
                scope: searchScope,
                limit: hitLimitPerPhrase,
              })
            ).matches;
      for (const match of matches) {
        const window = getTranscriptWindow(ctx, { startTime: match.startTime, radiusSeconds: 45 });
        const reverse = searchEpubText(ctx, {
          query: match.text,
          nodeIds: [entry.id],
          targetNodeId: entry.id,
          limit: 5,
        }).matches.find((candidate) => candidate.epubNodeId === entry.id);
        const distanceFromExpectedSeconds = Math.round((match.startTime - expectedStartTime) * 1000) / 1000;
        const relationRank = reverse ? epubTextRelationRank(reverse.relationToTarget) : 6;
        const distancePenalty = Math.min(1, Math.abs(distanceFromExpectedSeconds) / Math.max(1, baseRadius * 2));
        const phraseOffsetPenalty = Math.min(1.5, phrase.startWord / 40);
        hits.push({
          phrase: phrase.text,
          phraseStartWord: phrase.startWord,
          phraseWordCount: phrase.wordCount,
          startTime: match.startTime,
          endTime: match.endTime,
          distanceFromExpectedSeconds,
          transcriptText: normalizeToolText(match.text).slice(0, 500),
          transcriptWindow: normalizeToolText(window.text).slice(0, 900),
          reverseEpubRelation: reverse?.relationToTarget ?? "none",
          boundaryUse:
            phrase.startWord <= 12 && (reverse?.relationToTarget === "opener" || reverse?.relationToTarget === "near_opener")
              ? "candidate_start"
              : "supporting_context",
          score: phrase.score * 4 + Math.max(0, 5 - relationRank) - distancePenalty - phraseOffsetPenalty,
        });
      }
      if (hits.some((hit) => hit.phrase === phrase.text && (hit.reverseEpubRelation === "opener" || hit.reverseEpubRelation === "near_opener"))) break;
    }
  }

  const deduped: EpubBoundaryResearchHit[] = [];
  for (const { score: _score, ...hit } of hits.sort((a, b) => b.score - a.score || Math.abs(a.distanceFromExpectedSeconds) - Math.abs(b.distanceFromExpectedSeconds))) {
    if (deduped.some((existing) => Math.abs(existing.startTime - hit.startTime) < 15)) continue;
    deduped.push(hit);
    if (deduped.length >= 8) break;
  }

  return {
    epubNodeId: entry.id,
    epubIndex,
    title: entry.title,
    expectedStartTime,
    searchScope: {
      startTime: scopeStart,
      endTime: scopeEnd,
    },
    anchorPhrases,
    bestCandidates: deduped,
  };
}

export type EstimateTimestampFromEpubPositionInput = {
  epubNodeId: string;
};

export type EstimateTimestampFromEpubPositionResult = {
  epubNodeId: string;
  title: string;
  estimatedStartTime: number;
  estimatedEndTime: number;
  confidence: "low" | "medium";
  basis: {
    startRatio: number;
    endRatio: number;
    durationSeconds: number;
  };
};

export function estimateTimestampFromEpubPosition(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs">,
  input: EstimateTimestampFromEpubPositionInput
): EstimateTimestampFromEpubPositionResult | null {
  const index = ctx.epubEntries.findIndex((entry) => entry.id === input.epubNodeId);
  if (index < 0 || ctx.durationMs <= 0) return null;
  const entry = ctx.epubEntries[index]!;
  const startRatio = inferEntryStartRatio(ctx.epubEntries, index);
  const endRatio = inferEntryEndRatio(ctx.epubEntries, index);
  const estimatedStartMs = Math.round(startRatio * ctx.durationMs);
  const estimatedEndMs = Math.max(estimatedStartMs, Math.round(endRatio * ctx.durationMs));
  return {
    epubNodeId: entry.id,
    title: entry.title,
    estimatedStartTime: msToSeconds(estimatedStartMs),
    estimatedEndTime: msToSeconds(estimatedEndMs),
    confidence: ctx.epubEntries.length >= 3 ? "medium" : "low",
    basis: {
      startRatio,
      endRatio,
      durationSeconds: msToSeconds(ctx.durationMs),
    },
  };
}

export type GetTranscriptWindowInput = {
  startTime: number;
  radiusSeconds?: number;
};

export function getTranscriptWindow(
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs">,
  input: GetTranscriptWindowInput
): TranscriptWindow {
  return getTranscriptWindowFromContext(ctx, secondsToMs(input.startTime), secondsToMs(input.radiusSeconds ?? 20));
}

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
const submitFulcrumJudgmentSchema = z.object({
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
const audibleEpubNodeSelectionSchema = z.object({
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

export type SubmittedChapter = z.infer<typeof submittedChapterSchema>;
export type SubmitChapterPlanInput = z.infer<typeof submitChapterPlanSchema>;
export type SubmitFulcrumSplitInput = z.infer<typeof submitFulcrumSplitSchema>;
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

type ChapterBoundaryJudgeProposal = {
  kind: "fulcrum";
  spanPath: string;
  epubNodeId: string;
  epubIndex: number;
  title: string;
  startTime: number;
  notes: string | null;
  audit: FulcrumValidationAudit;
};

export type FindFulcrumCandidatesInput = {
  nodeIds?: string[];
  candidateNodeCount?: number;
  searchRadiusSeconds?: number;
  limitPerNode?: number;
};

export type ResearchEpubBoundaryInput = {
  epubNodeId: string;
  expectedTime?: number;
  searchRadiusSeconds?: number;
  phraseLimit?: number;
  hitLimitPerPhrase?: number;
  scope?: TranscriptSearchScope;
};

export type EpubBoundaryAnchorPhrase = {
  text: string;
  startWord: number;
  wordCount: number;
  score: number;
  epubOccurrences: number;
  genericTokenRatio: number;
  properNounRatio: number;
  rareTokens: string[];
};

export type EpubBoundaryResearchHit = {
  phrase: string;
  phraseStartWord: number;
  phraseWordCount: number;
  startTime: number;
  endTime: number;
  distanceFromExpectedSeconds: number;
  transcriptText: string;
  transcriptWindow: string;
  reverseEpubRelation: EpubTextSearchResult["matches"][number]["relationToTarget"] | "none";
  boundaryUse: "candidate_start" | "supporting_context";
};

export type ResearchEpubBoundaryResult = {
  epubNodeId: string;
  epubIndex: number;
  title: string;
  expectedStartTime: number;
  searchScope: TranscriptSearchScope;
  anchorPhrases: EpubBoundaryAnchorPhrase[];
  bestCandidates: EpubBoundaryResearchHit[];
};

export type FulcrumCandidate = {
  epubNodeId: string;
  epubIndex: number;
  title: string;
  expectedStartTime: number;
  startTime: number;
  endTime: number;
  score: number;
  textScore: number;
  boundaryScore: number;
  positionScore: number;
  ratioDistance: number;
  openerTokenOffset: number;
  preStartGapSeconds: number | null;
  query: string;
  transcriptWindow: string;
};

export type FindFulcrumCandidatesResult = {
  spanPath: string;
  midpointEpubRatio: number;
  candidates: FulcrumCandidate[];
};

export type RecursiveSpanDecision = { kind: "split"; split: Extract<SubmitFulcrumSplitResult, { accepted: true }>; result?: SubmitFulcrumSplitResult };

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

function summarizeSubmittedChapters(chapters: SubmittedChapter[], limit = 12): string {
  const shown = chapters.slice(0, limit).map((chapter) => `${Math.round(chapter.startTime)}s:${chapter.title}`).join(" | ");
  return chapters.length > limit ? `${shown} | ... +${chapters.length - limit}` : shown;
}

function summarizeSubmittedChapterObjects(chapters: SubmittedChapter[], limit = 24): Array<Pick<SubmittedChapter, "title" | "startTime" | "epubNodeId">> {
  return chapters.slice(0, limit).map((chapter) => ({
    title: chapter.title,
    startTime: chapter.startTime,
    epubNodeId: chapter.epubNodeId,
  }));
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

function chapterTitleKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
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

function distinctFirstWordTokens(value: string): string[] {
  const stop = new Set(["chapter", "part", "book", "this", "that", "with", "from", "have", "there", "their", "would", "could", "should"]);
  const tokens: string[] = [];
  for (const token of textTokens(value)) {
    if (stop.has(token)) continue;
    if (tokens.includes(token)) continue;
    tokens.push(token);
    if (tokens.length >= 12) break;
  }
  return tokens;
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
  embeddedChapters: ChapterCurationTiming[],
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

function spanInternalBoundaryCount(span: ChapterCurationSpan): number {
  return Math.max(0, span.epubEndIndex - span.epubStartIndex);
}

function automaticLeafChapters(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): SubmittedChapter[] {
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

function rankTargetBoundaries(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): ChapterCurationTargetBoundary[] {
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

function chooseTargetBoundary(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan): ChapterCurationTargetBoundary | null {
  return rankTargetBoundaries(ctx, span)[0] ?? null;
}

function spanDurationSeconds(span: ChapterCurationSpan): number {
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

const evidenceStopTokens = new Set([
  "about",
  "after",
  "again",
  "also",
  "been",
  "being",
  "could",
  "does",
  "from",
  "have",
  "into",
  "just",
  "keep",
  "kept",
  "less",
  "more",
  "only",
  "over",
  "should",
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
  "will",
  "with",
  "would",
  "your",
]);

function distinctiveEvidenceTokens(value: string): string[] {
  const out: string[] = [];
  for (const token of textTokens(value)) {
    if (evidenceStopTokens.has(token)) continue;
    if (out.includes(token)) continue;
    out.push(token);
  }
  return out;
}

function entryStartRatio(entries: EpubChapterEntry[], index: number): number {
  return inferEntryStartRatio(entries, index);
}

function localRatio(value: number, start: number, end: number): number {
  const width = end - start;
  if (width <= 0) return 0;
  return Math.max(0, Math.min(1, (value - start) / width));
}

function candidateQueryTokens(entry: EpubChapterEntry): string[] {
  const out: string[] = [];
  for (const word of entry.words.slice(0, 120)) {
    const token = word.token || textTokens(word.text)[0];
    if (!token || token.length < 4 || evidenceStopTokens.has(token)) continue;
    if (out.includes(token)) continue;
    out.push(token);
    if (out.length >= 24) break;
  }
  return out;
}

function normalizedEpubEntryTokens(entry: EpubChapterEntry): string[] {
  return entry.words.map(epubWordToken).filter(Boolean);
}

function countTokenSequenceOccurrences(haystack: string[], needle: string[]): number {
  if (needle.length === 0 || haystack.length < needle.length) return 0;
  let count = 0;
  for (let index = 0; index <= haystack.length - needle.length; index++) {
    let ok = true;
    for (let offset = 0; offset < needle.length; offset++) {
      if (haystack[index + offset] !== needle[offset]) {
        ok = false;
        break;
      }
    }
    if (ok) count++;
  }
  return count;
}

function epubCorpusTokenCounts(entries: EpubChapterEntry[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const entry of entries) {
    for (const token of normalizedEpubEntryTokens(entry)) {
      counts.set(token, (counts.get(token) ?? 0) + 1);
    }
  }
  return counts;
}

function generateEpubBoundaryAnchorPhrases(
  ctx: Pick<ChapterCurationContext, "epubEntries">,
  entry: EpubChapterEntry,
  limit: number
): EpubBoundaryAnchorPhrase[] {
  const words = entry.words.slice(0, 120);
  const entryTokens = normalizedEpubEntryTokens(entry);
  const corpusTokens = ctx.epubEntries.flatMap(normalizedEpubEntryTokens);
  const corpusTokenCounts = epubCorpusTokenCounts(ctx.epubEntries);
  const phrases: EpubBoundaryAnchorPhrase[] = [];

  for (let startWord = 0; startWord < words.length; startWord++) {
    for (const wordCount of [4, 5, 6, 7, 8]) {
      const window = words.slice(startWord, startWord + wordCount);
      if (window.length < wordCount) continue;
      const tokens = window.map(epubWordToken).filter(Boolean);
      if (tokens.length < Math.min(4, wordCount)) continue;
      const distinctiveTokens = tokens.filter((token) => token.length >= 4 && !evidenceStopTokens.has(token));
      if (distinctiveTokens.length < 2) continue;
      const genericTokenRatio = Math.round(((tokens.length - distinctiveTokens.length) / tokens.length) * 1000) / 1000;
      if (genericTokenRatio > 0.55) continue;
      const properNounRatio =
        window.filter((word, offset) => startWord + offset > 0 && /^[A-Z][a-z]+(?:['-][A-Za-z]+)?$/.test(word.text)).length / window.length;
      const epubOccurrences = countTokenSequenceOccurrences(corpusTokens, tokens);
      const localOccurrences = countTokenSequenceOccurrences(entryTokens, tokens);
      if (localOccurrences > 1) continue;
      const rareTokens = distinctiveTokens
        .filter((token, index) => distinctiveTokens.indexOf(token) === index)
        .sort((a, b) => (corpusTokenCounts.get(a) ?? 0) - (corpusTokenCounts.get(b) ?? 0))
        .slice(0, 4);
      const rarityScore =
        distinctiveTokens.reduce((sum, token) => sum + 1 / Math.sqrt(corpusTokenCounts.get(token) ?? 1), 0) /
        Math.max(1, distinctiveTokens.length);
      const openerBonus = startWord <= 8 ? 0.25 : startWord <= 24 ? 0.1 : 0;
      const occurrencePenalty = Math.max(0, epubOccurrences - 1) * 0.2;
      const score = rarityScore + distinctiveTokens.length / 12 + openerBonus - genericTokenRatio * 0.25 - properNounRatio * 0.2 - occurrencePenalty;
      phrases.push({
        text: normalizeToolText(window.map((word) => word.text).join(" ")),
        startWord,
        wordCount,
        score: Math.round(score * 1000) / 1000,
        epubOccurrences,
        genericTokenRatio,
        properNounRatio: Math.round(properNounRatio * 1000) / 1000,
        rareTokens,
      });
    }
  }

  const deduped: EpubBoundaryAnchorPhrase[] = [];
  for (const phrase of phrases.sort((a, b) => b.score - a.score || a.startWord - b.startWord || a.wordCount - b.wordCount)) {
    const phraseTokens = new Set(normalizedWordTokens(phrase.text));
    if (
      deduped.some((existing) => {
        const existingTokens = normalizedWordTokens(existing.text);
        const overlap = existingTokens.filter((token) => phraseTokens.has(token)).length;
        return overlap / Math.max(1, Math.min(existingTokens.length, phraseTokens.size)) > 0.75;
      })
    ) {
      continue;
    }
    deduped.push(phrase);
    if (deduped.length >= limit) break;
  }
  return deduped;
}

function candidateTranscriptWords(
  ctx: Pick<ChapterCurationContext, "transcript">,
  startTime: number,
  endTime: number
): Array<{ text: string; token: string; startMs: number; endMs: number }> {
  const startMs = secondsToMs(startTime);
  const endMs = secondsToMs(endTime);
  return [...ctx.transcript.words]
    .map((word) => ({
      text: word.text,
      token: word.token || textTokens(word.text)[0] || "",
      startMs: word.startMs,
      endMs: word.endMs,
    }))
    .filter((word) => word.token && word.endMs >= startMs && word.startMs <= endMs)
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
}

function candidatePreStartTokens(ctx: Pick<ChapterCurationContext, "transcript">, startTime: number, seconds = 20): string[] {
  const startMs = secondsToMs(startTime);
  const endMs = Math.max(0, startMs - 1);
  const windowStartMs = Math.max(0, startMs - secondsToMs(seconds));
  return [...ctx.transcript.words]
    .filter((word) => word.endMs >= windowStartMs && word.startMs <= endMs)
    .map((word) => word.token || textTokens(word.text)[0] || "")
    .filter(Boolean);
}

function candidatePreStartGapSeconds(ctx: Pick<ChapterCurationContext, "transcript">, startTime: number, seconds = 20): number | null {
  const startMs = secondsToMs(startTime);
  const windowStartMs = Math.max(0, startMs - secondsToMs(seconds));
  const previous = ctx.transcript.words
    .filter((word) => word.endMs >= windowStartMs && word.endMs <= startMs)
    .sort((a, b) => b.endMs - a.endMs)[0];
  return previous ? msToSeconds(Math.max(0, startMs - previous.endMs)) : null;
}

function selectFulcrumCandidateEntries(
  ctx: Pick<ChapterCurationContext, "epubEntries">,
  span: ChapterCurationSpan,
  input: FindFulcrumCandidatesInput
): Array<{ entry: EpubChapterEntry; epubIndex: number }> {
  const requestedIds = new Set(input.nodeIds?.map((id) => id.trim()).filter(Boolean));
  const internal = ctx.epubEntries
    .map((entry, epubIndex) => ({ entry, epubIndex }))
    .filter(({ epubIndex, entry }) => epubIndex > span.epubStartIndex && epubIndex <= span.epubEndIndex && (requestedIds.size === 0 || requestedIds.has(entry.id)));
  if (requestedIds.size > 0) return internal;

  const spanStartRatio = entryStartRatio(ctx.epubEntries, span.epubStartIndex);
  const spanEndRatio = inferEntryEndRatio(ctx.epubEntries, span.epubEndIndex);
  const midpoint = spanStartRatio + (spanEndRatio - spanStartRatio) / 2;
  const count = Math.max(1, Math.min(15, input.candidateNodeCount ?? 7));
  return internal
    .sort((a, b) => Math.abs(entryStartRatio(ctx.epubEntries, a.epubIndex) - midpoint) - Math.abs(entryStartRatio(ctx.epubEntries, b.epubIndex) - midpoint))
    .slice(0, count)
    .sort((a, b) => a.epubIndex - b.epubIndex);
}

function scoreFulcrumCandidateWindow(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs" | "transcript">,
  span: ChapterCurationSpan,
  entry: EpubChapterEntry,
  epubIndex: number,
  queryTokens: string[],
  words: Array<{ text: string; token: string; startMs: number; endMs: number }>
): FulcrumCandidate | null {
  if (queryTokens.length === 0 || words.length === 0) return null;
  const estimate = estimateTimestampFromEpubPosition(ctx, { epubNodeId: entry.id });
  if (!estimate) return null;
  const spanEpubStart = entryStartRatio(ctx.epubEntries, span.epubStartIndex);
  const spanEpubEnd = inferEntryEndRatio(ctx.epubEntries, span.epubEndIndex);
  const expectedLocalRatio = localRatio(entryStartRatio(ctx.epubEntries, epubIndex), spanEpubStart, spanEpubEnd);
  const windowSize = Math.min(80, Math.max(18, queryTokens.length * 3));
  const anchorTokens = queryTokens.slice(0, Math.min(6, queryTokens.length));
  let best: FulcrumCandidate | null = null;

  for (let index = 0; index < words.length; index++) {
    const window = words.slice(index, index + windowSize);
    if (window.length === 0) continue;
    const windowTokens = new Set(window.map((word) => word.token));
    const matchedQueryTerms = queryTokens.filter((token) => windowTokens.has(token));
    if (matchedQueryTerms.length < Math.min(3, queryTokens.length)) continue;
    const openerTokenOffset = window.findIndex((word) => anchorTokens.includes(word.token));
    if (openerTokenOffset < 0 || openerTokenOffset > 4) continue;
    const anchorWindowTokens = new Set(window.slice(0, Math.min(12, window.length)).map((word) => word.token));
    const anchorMatches = anchorTokens.filter((token) => anchorWindowTokens.has(token));
    if (anchorMatches.length < Math.min(3, anchorTokens.length)) continue;
    let orderedMatches = 0;
    let searchFrom = 0;
    for (const token of queryTokens) {
      const found = window.findIndex((word, wordIndex) => wordIndex >= searchFrom && word.token === token);
      if (found < 0) continue;
      orderedMatches++;
      searchFrom = found + 1;
    }
    const startTime = msToSeconds(window[0]!.startMs);
    const audioLocalRatio = localRatio(startTime, span.startTime, span.endTime);
    const ratioDistance = Math.abs(audioLocalRatio - expectedLocalRatio);
    const positionScore = Math.max(0, 1 - ratioDistance / 0.12);
    const textScore = matchedQueryTerms.length / queryTokens.length + orderedMatches / queryTokens.length;
    const preStartTokens = new Set(candidatePreStartTokens(ctx, startTime));
    const preStartTokenOverlap = queryTokens.filter((token) => preStartTokens.has(token));
    const preStartGapSeconds = candidatePreStartGapSeconds(ctx, startTime);
    const continuousPreRollPenalty = preStartGapSeconds !== null && preStartGapSeconds < 1.5 ? 0.35 : preStartGapSeconds !== null && preStartGapSeconds < 3 ? 0.2 : 0;
    const preStartPenalty = Math.min(1, preStartTokenOverlap.length / Math.max(1, anchorTokens.length));
    const boundaryScore = Math.max(0, anchorMatches.length / anchorTokens.length - preStartPenalty - continuousPreRollPenalty);
    if (boundaryScore < 0.35) continue;
    const score = textScore * 12 + boundaryScore * 18 + positionScore * 8 - ratioDistance * 10;
    const candidate: FulcrumCandidate = {
      epubNodeId: entry.id,
      epubIndex,
      title: entry.title,
      expectedStartTime: estimate.estimatedStartTime,
      startTime,
      endTime: msToSeconds(window.at(-1)!.endMs),
      score: Math.round(score * 1000) / 1000,
      textScore: Math.round(textScore * 1000) / 1000,
      boundaryScore: Math.round(boundaryScore * 1000) / 1000,
      positionScore: Math.round(positionScore * 1000) / 1000,
      ratioDistance: Math.round(ratioDistance * 1000) / 1000,
      openerTokenOffset,
      preStartGapSeconds: preStartGapSeconds === null ? null : Math.round(preStartGapSeconds * 1000) / 1000,
      query: queryTokens.join(" "),
      transcriptWindow: normalizeToolText(window.map((word) => word.text).join(" ")).slice(0, 700),
    };
    if (!best || candidate.score > best.score || (candidate.score === best.score && candidate.ratioDistance < best.ratioDistance)) best = candidate;
  }

  return best;
}

export function findFulcrumCandidates(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs" | "transcript">,
  span: ChapterCurationSpan,
  input: FindFulcrumCandidatesInput = {}
): FindFulcrumCandidatesResult {
  const spanEpubStart = entryStartRatio(ctx.epubEntries, span.epubStartIndex);
  const spanEpubEnd = inferEntryEndRatio(ctx.epubEntries, span.epubEndIndex);
  const midpointEpubRatio = spanEpubStart + (spanEpubEnd - spanEpubStart) / 2;
  const radiusSeconds = Math.max(600, Math.min(7_200, input.searchRadiusSeconds ?? spanDurationSeconds(span) / 4));
  const limitPerNode = Math.max(1, Math.min(5, input.limitPerNode ?? 3));
  const candidates: FulcrumCandidate[] = [];

  for (const { entry, epubIndex } of selectFulcrumCandidateEntries(ctx, span, input)) {
    const estimate = estimateTimestampFromEpubPosition(ctx, { epubNodeId: entry.id });
    if (!estimate) continue;
    const queryTokens = candidateQueryTokens(entry);
    const words = candidateTranscriptWords(
      ctx,
      Math.max(span.startTime, estimate.estimatedStartTime - radiusSeconds),
      Math.min(span.endTime, estimate.estimatedStartTime + radiusSeconds)
    );
    const best = scoreFulcrumCandidateWindow(ctx, span, entry, epubIndex, queryTokens, words);
    if (!best) continue;
    const alternates = transcriptWordEvidenceMatches(
      ctx,
      queryTokens,
      {
        startTime: Math.max(span.startTime, estimate.estimatedStartTime - radiusSeconds),
        endTime: Math.min(span.endTime, estimate.estimatedStartTime + radiusSeconds),
      },
      limitPerNode
    ).map((match) => {
      const audioLocalRatio = localRatio(match.startTime, span.startTime, span.endTime);
      const expectedLocalRatio = localRatio(entryStartRatio(ctx.epubEntries, epubIndex), spanEpubStart, spanEpubEnd);
      const ratioDistance = Math.abs(audioLocalRatio - expectedLocalRatio);
      const positionScore = Math.max(0, 1 - ratioDistance / 0.12);
      const matchTokens = queryTokens.filter((token) => new Set(textTokens(`${match.text} ${match.afterText}`)).has(token));
      const textScore = matchTokens.length / Math.max(1, queryTokens.length);
      const preStartTokens = new Set(candidatePreStartTokens(ctx, match.startTime));
      const anchorTokens = queryTokens.slice(0, Math.min(6, queryTokens.length));
      const preStartTokenOverlap = queryTokens.filter((token) => preStartTokens.has(token));
      const preStartGapSeconds = candidatePreStartGapSeconds(ctx, match.startTime);
      const continuousPreRollPenalty = preStartGapSeconds !== null && preStartGapSeconds < 1.5 ? 0.35 : preStartGapSeconds !== null && preStartGapSeconds < 3 ? 0.2 : 0;
      const boundaryScore = Math.max(
        0,
        matchTokens.filter((token) => anchorTokens.includes(token)).length / Math.max(1, anchorTokens.length) -
          Math.min(1, preStartTokenOverlap.length / Math.max(1, anchorTokens.length)) -
          continuousPreRollPenalty
      );
      return {
        epubNodeId: entry.id,
        epubIndex,
        title: entry.title,
        expectedStartTime: estimate.estimatedStartTime,
        startTime: match.startTime,
        endTime: match.endTime,
        score: Math.round((textScore * 12 + boundaryScore * 18 + positionScore * 8 - ratioDistance * 10) * 1000) / 1000,
        textScore: Math.round(textScore * 1000) / 1000,
        boundaryScore: Math.round(boundaryScore * 1000) / 1000,
        positionScore: Math.round(positionScore * 1000) / 1000,
        ratioDistance: Math.round(ratioDistance * 1000) / 1000,
        openerTokenOffset: 0,
        preStartGapSeconds: preStartGapSeconds === null ? null : Math.round(preStartGapSeconds * 1000) / 1000,
        query: queryTokens.join(" "),
        transcriptWindow: match.afterText,
      } satisfies FulcrumCandidate;
    }).filter((candidate) => candidate.boundaryScore >= 0.35);
    candidates.push(best, ...alternates);
  }

  const deduped: FulcrumCandidate[] = [];
  for (const candidate of candidates.sort((a, b) => b.score - a.score || a.ratioDistance - b.ratioDistance || a.startTime - b.startTime)) {
    if (deduped.some((existing) => existing.epubNodeId === candidate.epubNodeId && Math.abs(existing.startTime - candidate.startTime) < 30)) continue;
    deduped.push(candidate);
  }
  return {
    spanPath: span.path,
    midpointEpubRatio: Math.round(midpointEpubRatio * 1000) / 1000,
    candidates: deduped.slice(0, 20),
  };
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
      instruction: "Revise the split so it matches the submitFulcrumSplit schema.",
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
    errors.push("Fulcrum must leave non-empty EPUB ranges on both sides of the split.");
  }
  if (split.startTime <= span.startTime || split.startTime >= span.endTime) {
    errors.push("Fulcrum startTime must be inside the current span time range.");
  }
  const edgeMargin = Math.max(120, spanDurationSeconds(span) * 0.05);
  const isOnlyRemainingAssignedBoundary = Boolean(options.targetBoundary && spanInternalBoundaryCount(span) === 1);
  if (!isOnlyRemainingAssignedBoundary && (split.startTime - span.startTime < edgeMargin || span.endTime - split.startTime < edgeMargin)) {
    errors.push("Fulcrum startTime is too close to a span edge.");
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
      instruction: "Pick a different internal fulcrum with stronger transcript/prose evidence for the assigned target boundary.",
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
const findFulcrumCandidatesSchema = z.object({
  nodeIds: z.array(z.string()).max(20).optional(),
  candidateNodeCount: z.number().int().positive().max(15).optional(),
  searchRadiusSeconds: z.number().positive().max(7_200).optional(),
  limitPerNode: z.number().int().positive().max(5).optional(),
});
const researchEpubBoundarySchema = z.object({
  epubNodeId: z.string().trim().min(1),
  expectedTime: z.number().finite().nonnegative().optional(),
  searchRadiusSeconds: z.number().positive().max(7_200).optional(),
  phraseLimit: z.number().int().positive().max(12).optional(),
  hitLimitPerPhrase: z.number().int().positive().max(5).optional(),
});
const assignedResearchEpubBoundarySchema = z.object({});
const getEpubNodeTextSchema = z.object({
  epubNodeId: z.string().trim().min(1),
  startWord: z.number().int().nonnegative().optional(),
  wordCount: z.number().int().positive().max(180).optional(),
});
const assignedGetEpubNodeTextSchema = z.object({
  startWord: z.number().int().nonnegative().optional(),
  wordCount: z.number().int().positive().max(180).optional(),
});
const searchEpubTextSchema = z.object({
  query: z.string().trim().min(1),
  nodeIds: z.array(z.string().trim().min(1)).max(80).optional(),
  targetNodeId: z.string().trim().min(1).optional(),
  limit: z.number().int().positive().max(20).optional(),
});
const assignedSearchEpubTextSchema = z.object({
  query: z.string().trim().min(1),
  limit: z.number().int().positive().max(20).optional(),
});
const estimateTimestampFromEpubPositionSchema = z.object({
  epubNodeId: z.string(),
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
      result.tool.name === "submitFulcrumSplit" &&
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
    (result) => result.type === "function_output" && result.tool.name === "submitFulcrumJudgment" && parseFulcrumJudgmentOutput(result.output)
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

function spanScope(span: ChapterCurationSpan, inputScope?: TranscriptSearchScope): TranscriptSearchScope {
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
  const entries = spanEpubEntries(ctx, span);
  const spanAudioOnlyIntervals = (ctx.audioOnlyIntervals ?? []).filter((interval) => interval.endTime >= span.startTime && interval.startTime <= span.endTime);
  const inheritedBoundaryInstructions = span.startBoundary
    ? [
        `This span starts at an already accepted parent split: ${span.startBoundary.title} (${span.startBoundary.epubNodeId}) at ${span.startBoundary.startTime}s.`,
        "Treat that inherited first boundary as already proven; spend research effort only on the assigned target boundary.",
      ]
    : [];
  const fulcrumWorkflow = [
        "Single-target boundary workflow:",
        "1. Your only goal is to find the audiobook start timestamp for the assigned target EPUB node. Do not switch to another chapter, even after rejection.",
        "2. Use the targetBoundaryContext below as your initial EPUB structure/opener context. Do not call getEpubStructure; that context is already provided.",
        "3. Call researchEpubBoundary. It researches only the assigned target node: rare opener phrases, nearby transcript hits, transcript windows, and opener/near_opener reverse checks.",
        "4. If researchEpubBoundary returns no opener/near_opener candidate, call getEpubNodeText and manually try the opener words: first 4-8 distinctive opener words, the next distinctive clause, a shorter exact phrase, and one phrase from the next word window if needed. Drop generic chapter numbers, titles, standalone names, punctuation-only differences, and repeated formulaic text.",
        "5. Estimate the likely transcript neighborhood from the EPUB node's word-position ratio, then search near that neighborhood with rgSearchTranscript first for manual follow-up. If a phrase misses, shorten it or try a different phrase from the same EPUB node; do not pivot to guessed timestamps.",
        "6. Inspect the best match with getTranscriptWindow. Use radiusSeconds=45 when the boundary is ambiguous, when nearby context may include pre-target or interior transcript, or after a rejected fulcrum. The proposed start must be the first matched opener word or the silence immediately before it; do not submit the start of a broad evidence window.",
        "7. If transcript context looks like pre-roll or interior prose, call searchEpubText with the transcript phrase. Trust relationToTarget: opener/near_opener is usable boundary evidence; interior means do not submit that timestamp as a chapter start; pre_target means move later to the target opener.",
        "8. Treat submitFulcrumSplit as a final evidence-backed claim, not a probe. The call asserts that startTime is the audiobook start of the assigned EPUB node. It is never an arbitrary scene, sentence, dialogue turn, or later distinctive phrase inside that node.",
        "9. Call submitFulcrumSplit only when you can already prove the chosen EPUB node opener begins at that exact timestamp or immediately after it. Distinctive later prose helps locate the area, but it is not valid split evidence unless reverse EPUB search says opener/near_opener.",
        "10. Before submitFulcrumSplit, have this proof in hand: target EPUB node opener text, the transcript search hit that found that opener, a transcript window showing the opener at/just after the proposed start, and reverse EPUB evidence that the transcript phrase is opener/near_opener for the target node. Put that proof in the evidence/notes fields.",
        "11. If you do not have that proof yet, keep researching instead of submitting. If rejected, keep the same EPUB node and search earlier/different opener phrases or a wider same-node word window.",
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
    `Your singular goal is to prove where ${targetBoundary.epubNodeId} (${targetBoundary.title}) starts in the transcript. submitFulcrumSplit only needs the timestamp and evidence notes.`,
    spanAudioOnlyIntervals.length > 0
      ? `audioOnlyIntervalsInSpan: ${JSON.stringify(spanAudioOnlyIntervals)}`
      : "audioOnlyIntervalsInSpan: []",
    "You must call submitFulcrumSplit with a validated start for the assigned targetBoundary.",
    "For a fulcrum, pick a high-confidence internal EPUB node start with transcript opener evidence at the timestamp.",
    spanAudioOnlyIntervals.length > 0
      ? "Some transcript time ranges in this span are classified as audio-only material with no EPUB node. Do not align EPUB chapter starts to those intervals unless opener evidence proves the EPUB text starts there."
      : "",
    ...inheritedBoundaryInstructions,
    "Prefer submitFulcrumSplit for spans with more than 8 EPUB nodes or more than 2 hours duration unless the whole span is already strongly evidenced.",
    ...fulcrumWorkflow,
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
      "You are a strict reviewer for audiobook chapter split points.",
      "Your job is to decide whether the proposed timestamp is actually the start of the proposed EPUB node.",
      "Judge the boundary claim directly: node X starts at timestamp Y.",
      "Use audit.boundaryComparison first. Compare previousEpub.tailText to transcriptBefore, and targetEpub.bodyHeadText or targetEpub.headText to transcriptAfter.",
      "EPUB chapter headings/titles are often printed but not spoken in audiobook transcripts. If targetEpub.optionalHeadingText is absent but targetEpub.bodyHeadText begins at the timestamp, that is strong opener evidence.",
      "Sometimes the ASR omits the printed heading and the first few opener words. If the submitted notes identify a deterministic near-opener fallback, accept only when transcriptAfter starts with the earliest clear target body phrase and transcriptBefore plausibly matches the previous EPUB tail.",
      "Check audit.boundaryComparison.transcriptPrecision. If it is word, transcriptBefore/transcriptAfter are split by word timing and should be treated as precise boundary context.",
      "If audit.boundaryComparison.boundaryWords.containing is non-empty, the timestamp falls inside a word. Prefer a nearestCleanBoundaryTimes value unless the containing word itself is the first target opener word.",
      "If transcriptPrecision is utterance, the supplied before/after text is lower precision. A true boundary may fall inside a displayed utterance.",
      "For utterance-level context, accept the utterance timestamp when one utterance contains previous EPUB tail followed by the target EPUB head, such as 'tail tail chapter twelve head head'.",
      "Transcript before the timestamp may match the previous EPUB node; that is positive boundary evidence, not evidence that the target opener is interior.",
      "Reject only when target EPUB body opener evidence is absent at or immediately after the proposed timestamp, offset earlier/later, generic, pre-target, or clearly only an interior target-node match.",
      "Accept when transcriptBefore plausibly matches the previous EPUB tail and transcriptAfter begins with distinctive target EPUB body opener prose at or immediately after the proposed timestamp, even if the printed title/heading is omitted.",
      "When notes identify a deterministic near-opener fallback, the proposed start can be the first audible target body phrase if earlier heading/opener tokens are absent from ASR and the previous-tail context supports a real boundary.",
      "Do not invent a full chapter plan. Judge only this proposed split.",
      "Do not recommend alternate timestamps or EPUB nodes. If the split is not proven, classify the evidence problem and explain only what is visible in the supplied evidence.",
      "The finding enum is an evidence classification from the submitted audit/window, not a ground-truth timestamp label. Use it only when the supplied evidence directly supports that classification.",
      "Do not use scene-language such as 'inside a scene', 'mid-scene', or claims about ongoing narrative continuity. Prefer evidence terms: opener_evidence_at_timestamp, opener_evidence_offset_in_window, window_starts_before_opener_evidence, tool_classified_interior_match, generic_or_weak_overlap, or submitted_evidence_insufficient.",
      "You must call submitFulcrumJudgment.",
    ].join("\n"),
    tools: [
      tool({
        name: "submitFulcrumJudgment",
        description: "Submit the final judgment for this proposed fulcrum split.",
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
    `Judge this proposed audiobook chapter boundary for "${ctx.book.title}" by ${ctx.book.author}.`,
    "Return accepted=false if this timestamp is not clearly the start of the EPUB node.",
    "Prefer rejecting over accepting a suspicious split; the curator owns the next search.",
    "Do not suggest alternate timestamps or nodes.",
    "Do not make claims about scenes or narrative continuity. You are only judging whether this boundary comparison supports node X starting at timestamp Y.",
    "Primary evidence: audit.boundaryComparison. If transcriptBefore matches previousEpub.tailText and transcriptAfter matches targetEpub.bodyHeadText or targetEpub.headText, that supports acceptance.",
    "Printed EPUB headings are optional audio evidence. Do not reject merely because targetEpub.optionalHeadingText is absent from the ASR transcript when targetEpub.bodyHeadText begins at the proposed timestamp.",
    "If notes identify a deterministic near-opener fallback, accept the first audible target body phrase when the printed heading/opening words appear absent from ASR and transcriptBefore supports the previous EPUB tail.",
    "Use audit.boundaryComparison.transcriptPrecision. Word precision is exact enough to split a boundary inside an ASR utterance. If boundaryWords.containing is non-empty, prefer a nearestCleanBoundaryTimes value unless the containing word itself is the first target opener word. Utterance precision is lower precision; if one utterance includes previous EPUB tail followed by the target EPUB head, accepting that utterance start is allowed.",
    "Do not reject merely because transcriptBefore contains non-target prose; that is expected at a real boundary when it matches the previous EPUB node.",
    "Treat the finding as an evidence classification from the supplied audit/window, not as ground truth about the true boundary.",
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

async function judgeChapterBoundary(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  proposal: ChapterBoundaryJudgeProposal
): Promise<SubmitFulcrumJudgmentResult | null> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) return null;
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

async function judgeFulcrumSplit(
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

function orderedCurationContainers(ctx: Pick<ChapterCurationContext, "containers">): ChapterCurationContainer[] {
  return [...ctx.containers].sort((a, b) => a.asset.sequence_in_manifestation - b.asset.sequence_in_manifestation || a.asset.id - b.asset.id);
}

function containerDurationMs(container: ChapterCurationContainer): number {
  return container.asset.duration_ms ?? container.files.reduce((sum, file) => sum + file.duration_ms, 0);
}

function audioAssetBoundaryExcerpts(ctx: ChapterCurationContext): Array<Record<string, unknown>> {
  const containers = orderedCurationContainers(ctx).filter((container) => container.asset.kind !== "ebook");
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

async function classifyAudibleEpubNodes(ctx: ChapterCurationContext, runner: Runner, signal: AbortSignal): Promise<AudibleEpubNodeSelection | null> {
  try {
    const result = await runner.run(createAudibleEpubNodeSelectionAgent(ctx), audibleEpubNodeSelectionPrompt(ctx), {
      maxTurns: 4,
      signal,
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
      return "Use boundaryWords/nearestCleanBoundaryTimes, then search or inspect the adjusted opener boundary before resubmitting.";
    case "tool_classified_interior_match":
    case "generic_or_weak_overlap":
      return "Do not resubmit nearby body-text overlap. Search a different target opener phrase and reverse-check it.";
    case "submitted_evidence_insufficient":
      return "Get stronger opener evidence with researchEpubBoundary or transcript search plus searchEpubText.";
    case "opener_evidence_at_timestamp":
      return "Reinspect boundaryWords. Submit only if the target opener starts at or immediately after startTime.";
  }
}

function conciseFulcrumJudgmentMessage(judgment: SubmitFulcrumJudgmentResult): string {
  const concern = judgment.concerns[0] ? ` ${judgment.concerns[0]}` : "";
  return `${judgment.finding}; openerEvidenceAtTimestamp=${judgment.openerEvidenceAtTimestamp}.${concern}`;
}

function createRecursiveSpanCuratorAgent(
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
      errors: ["submitFulcrumSplit was called again before gathering new transcript/EPUB evidence after the previous fulcrum rejection."],
      warnings: [],
      audit: null,
      instruction:
        "Do more research before proposing another fulcrum. Use rgSearchTranscript or fuzzySearchTranscript on distinctive EPUB opener prose, then inspect the match with getTranscriptWindow.",
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
      "You curate audiobook chapter markers for one bounded span, not the whole book.",
      "You must propose one validated fulcrum split.",
      `Your singular goal is to find the audiobook start of ${targetBoundary.epubNodeId} (${targetBoundary.title}). Do not switch target nodes.`,
      "Submit a fulcrum split for the assigned target boundary, even if one child span will have no remaining unsolved boundaries.",
      "Concrete single-boundary workflow:",
      "1. Use the targetBoundaryContext in the prompt as your initial EPUB structure/opener context. Do not choose a different EPUB node.",
      "2. Call researchEpubBoundary. It researches only the assigned target node: rare opener phrases, nearby transcript hits, transcript windows, and opener/near_opener reverse checks.",
      "3. If researchEpubBoundary returns a strong opener/near_opener candidate, use that exact timestamp or the silence immediately before the first matched opener word. If it returns no strong candidate, call getEpubNodeText and manually try different opener phrases from the same node.",
      "4. Estimate the likely timestamp neighborhood from the EPUB node position within the current span. Search that neighborhood with rgSearchTranscript first when doing manual follow-up. If a phrase misses, shorten it or try a different phrase from the same EPUB node; do not pivot to guessed timestamps.",
      "5. Inspect the best candidate with getTranscriptWindow. Use radiusSeconds=45 when the boundary is ambiguous, when nearby context may include pre-target or interior transcript, or after a rejected fulcrum. Check both sides of the timestamp: the proposed start should be the first matched opener word or the silence immediately before it, not the start of a broad evidence window.",
      "6. If transcript context looks like it may include pre-roll or interior prose, call searchEpubText with the transcript phrase. Trust relationToTarget: opener/near_opener is usable boundary evidence; interior means do not submit that timestamp as a chapter start; pre_target means move later to the target opener.",
      "7. Treat submitFulcrumSplit as a final evidence-backed claim, not a probing tool. The call asserts that startTime is the audiobook start of the assigned EPUB node. It is never an arbitrary scene, sentence, dialogue turn, or later distinctive phrase inside that node.",
      "8. Call submitFulcrumSplit only when you can already prove the chosen EPUB node opener begins at that exact timestamp or immediately after it. Distinctive later prose helps locate the area, but it is not valid split evidence unless reverse EPUB search says opener/near_opener.",
      "9. Before submitFulcrumSplit, have this proof in hand: target EPUB node opener text, the transcript search hit that found that opener, a transcript window showing the opener at/just after the proposed start, and reverse EPUB evidence that the transcript phrase is opener/near_opener for the target node. Put that proof in the evidence/notes fields.",
      "10. If you do not have that proof yet, keep researching instead of submitting. If the judge rejects it, do not use the judge as the search engine and do not resubmit the same node/timestamp; keep the same EPUB node and search earlier/different opener phrases or a wider same-node word window.",
      "Do not submit guessed timestamps or timestamps copied from estimated EPUB position. A broad-span fulcrum must be backed by a transcript search result from rgSearchTranscript or fuzzySearchTranscript.",
      "After any rejected fulcrum, run a fresh rgSearchTranscript or fuzzySearchTranscript query from the same EPUB node's opener text before trying another fulcrum.",
      "All tool times and submitted startTime values are seconds, not milliseconds.",
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
        name: "submitFulcrumSplit",
        description: "Submit the final startTime for the assigned target EPUB node after opener evidence is in hand.",
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
          return runToolWithEvents("submitFulcrumSplit", splitInput, async () => {
            if (rejectedFulcrumRequiresEvidence && evidenceCallsSinceRejectedFulcrum === 0) return rejectedFulcrumWithoutEvidence();
            if (transcriptSearchesSinceFulcrumSubmit === 0) {
              return {
                accepted: false,
                kind: "split",
                errors: ["Broad-span fulcrums require a recent rgSearchTranscript or fuzzySearchTranscript result before submission."],
                warnings: [],
                audit: null,
                instruction:
                  "Call getEpubNodeText for the target EPUB node, search distinctive opener phrases from that same node with rgSearchTranscript or fuzzySearchTranscript, inspect the result with getTranscriptWindow, then submit the evidenced timestamp.",
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
                errors: [`Fulcrum ${splitInput.epubNodeId} near ${Math.round(splitInput.startTime)}s was already rejected for this span.`],
                warnings: [],
                audit: null,
                instruction: "Stay on this EPUB node unless you have exhausted its opener text. Call getEpubNodeText for an earlier/different word window, search a different phrase from that node, and submit a materially different timestamp with stronger evidence.",
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

async function findSpokenHeadingBoundaryCandidate(
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

function chooseResearchBoundaryCandidate(research: ResearchEpubBoundaryResult | null, span: ChapterCurationSpan): DeterministicBoundaryCandidate | null {
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

export type ChapterCurationDetailedResult = {
  result: SubmitChapterPlanResult | null;
  finalOutput: unknown;
  newItems: unknown[];
  rawResponses: unknown[];
  recursiveReports?: RecursiveCurationReport[];
  recursiveSpanTraces?: RecursiveSpanTrace[];
};

export async function runRecursiveAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) {
    logChapterCurationEvent(ctx, {
      type: "recursive-run-skipped",
      message: "recursive run skipped=no_api_key",
    });
    return { result: null, finalOutput: null, newItems: [], rawResponses: [], recursiveReports: [], recursiveSpanTraces: [] };
  }
  const abort = new AbortController();
  const timeout = setTimeout(() => abort.abort(), Math.max(5_000, ctx.settings.agents.timeoutMs));
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    const audibleNodeSelection = await classifyAudibleEpubNodes(ctx, runner, abort.signal);
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
              signal: abort.signal,
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
        strategy: "Recursive fulcrum chapter curation",
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
    clearTimeout(timeout);
    await provider.close().catch(() => undefined);
  }
}

export async function runAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<ChapterCurationDetailedResult> {
  return runRecursiveAgenticChapterCurationDetailed(ctx);
}
