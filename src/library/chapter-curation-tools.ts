import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { rgPath } from "@vscode/ripgrep";
import uFuzzy from "@leeoniya/ufuzzy";
import { z } from "zod";

import type { AppSettings, AssetFileRow, AssetRow, BookRow, ManifestationRow } from "../app-types";
import type { EpubChapterEntry, StoredTranscriptPayload, StoredTranscriptUtterance } from "./chapter-analysis";

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
  chapterStartTimeHints?: Record<string, number>;
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
    extendedTranscriptAfter?: string;
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
    const token = entry.words[offset] ? epubWordToken(entry.words[offset]!) : "";
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
  const closeCount = countDelta <= Math.max(8, Math.ceil(ctx.epubEntries.length * 0.2));

  if (diagnostics.labelQuality === "named" && diagnostics.boundaryDensity === "plausible" && diagnostics.durationPattern === "varied" && closeCount) {
    return {
      action: "short_circuit_candidate",
      confidence: "high",
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

export type FindFulcrumCandidatesInput = {
  nodeIds?: string[];
  candidateNodeCount?: number;
  searchRadiusSeconds?: number;
  limitPerNode?: number;
};

export type FindFulcrumCandidatesResult = {
  spanPath: string;
  midpointEpubRatio: number;
  candidates: FulcrumCandidate[];
};

function textTokens(value: string): string[] {
  return normalizedWordTokens(value).filter((token) => token.length >= 4);
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
  const firstSearchableWord = firstSearchableEpubWordOffset(entry, 0, words.length);
  const earlyOpenerOffsets = new Set(
    [firstSearchableWord, firstSearchableWord + 2, firstSearchableWord + 4, firstSearchableWord + 6, firstSearchableWord + 8, firstSearchableWord + 12].filter(
      (offset) => offset >= 0 && offset < Math.min(words.length, 32)
    )
  );
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
      const openerSeedBonus = earlyOpenerOffsets.has(startWord) ? 1.25 : 0;
      const occurrencePenalty = Math.max(0, epubOccurrences - 1) * 0.2;
      const score =
        rarityScore + distinctiveTokens.length / 12 + openerBonus + openerSeedBonus - genericTokenRatio * 0.25 - properNounRatio * 0.2 - occurrencePenalty;
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

function spanDurationSeconds(span: ChapterCurationSpan): number {
  return Math.max(0, span.endTime - span.startTime);
}

// Re-export z so validation module can import it
export { z };
