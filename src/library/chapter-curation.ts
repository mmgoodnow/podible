import { spawn } from "node:child_process";
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { rgPath } from "@vscode/ripgrep";
import uFuzzy from "@leeoniya/ufuzzy";
import { Agent, OpenAIProvider, Runner, tool, type FunctionToolResult, type ToolsToFinalOutputResult } from "@openai/agents";
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
  debugEventLogPath?: string;
  debugTraceDir?: string;
  debugReasoningSummary?: "auto" | "concise" | "detailed";
  debugReasoningEffort?: "none" | "minimal" | "low" | "medium" | "high" | "xhigh";
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

export type FulcrumValidationAudit = {
  epubNodeId: string | null;
  title: string;
  startTime: number;
  expectedTokens: string[];
  proseTokens: string[];
  matchedTokens: string[];
  proseMatchedTokens: string[];
  overlapRatio: number;
  transcriptWindow: string;
  candidates: EpubChapterEvidenceResult["nodes"][number]["matches"];
};

export type TranscriptWindow = {
  startMs: number;
  endMs: number;
  utterances: StoredTranscriptUtterance[];
  text: string;
  audioOnlyIntervals?: AudioOnlyInterval[];
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
  };
}

export function summarizeFirstWords(entry: EpubChapterEntry, limit = 40): string {
  return entry.words.slice(0, limit).map((word) => word.text).join(" ").trim();
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
  queryTokens: string[];
  matches: Array<{
    epubNodeId: string;
    epubIndex: number;
    title: string;
    wordOffset: number;
    wordRatioWithinNode: number;
    targetNodeDistance: number | null;
    targetWordOffset: number | null;
    relationToTarget: "unknown" | "pre_target" | "opener" | "near_opener" | "interior" | "post_target";
    matchedTokens: string[];
    orderedMatchRatio: number;
    text: string;
  }>;
};

type LeafBoundaryEvidence = Pick<
  EpubTextSearchResult["matches"][number],
  "relationToTarget" | "orderedMatchRatio" | "matchedTokens"
>;

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
  const matches: EpubTextSearchResult["matches"] = [];
  if (queryTokens.length === 0) return { query: input.query, queryTokens, matches };

  for (const [epubIndex, entry] of ctx.epubEntries.entries()) {
    if (requestedIds.size > 0 && !requestedIds.has(entry.id)) continue;
    const windowSize = Math.min(entry.words.length, Math.max(queryTokens.length + 6, Math.min(48, queryTokens.length * 3)));
    for (let offset = 0; offset < entry.words.length; offset++) {
      const window = entry.words.slice(offset, offset + windowSize);
      if (window.length === 0) continue;
      const windowWordTokens = window.map(epubWordToken).filter(Boolean);
      const windowTokens = new Set(windowWordTokens);
      const matchedTokens = queryTokens.filter((token, index) => windowTokens.has(token) && queryTokens.indexOf(token) === index);
      if (matchedTokens.length < Math.min(3, queryTokens.length)) continue;

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
      const orderedMatchRatio = orderedMatches / queryTokens.length;
      matches.push({
        epubNodeId: entry.id,
        epubIndex,
        title: entry.title,
        wordOffset: offset,
        wordRatioWithinNode: entry.words.length === 0 ? 0 : Math.round((offset / entry.words.length) * 1000) / 1000,
        targetNodeDistance: targetIndex < 0 ? null : epubIndex - targetIndex,
        targetWordOffset,
        relationToTarget: classifyEpubTextMatch(targetIndex < 0 ? null : epubIndex - targetIndex, targetWordOffset),
        matchedTokens,
        orderedMatchRatio: Math.round(orderedMatchRatio * 1000) / 1000,
        text: normalizeToolText(window.map((word) => word.text).join(" ")).slice(0, 500),
      });
    }
  }

  matches.sort((a, b) => {
    const aTextScore = a.matchedTokens.length / queryTokens.length;
    const bTextScore = b.matchedTokens.length / queryTokens.length;
    const aDistance = a.targetNodeDistance === null ? 0 : Math.abs(a.targetNodeDistance);
    const bDistance = b.targetNodeDistance === null ? 0 : Math.abs(b.targetNodeDistance);
    return (
      bTextScore - aTextScore ||
      b.orderedMatchRatio - a.orderedMatchRatio ||
      epubTextRelationRank(a.relationToTarget) - epubTextRelationRank(b.relationToTarget) ||
      aDistance - bDistance ||
      a.epubIndex - b.epubIndex ||
      a.wordOffset - b.wordOffset
    );
  });

  return { query: input.query, queryTokens, matches: matches.slice(0, limit) };
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

export type FindEpubChapterEvidenceInput = {
  nodeIds?: string[];
  searchRadiusSeconds?: number;
  limitPerNode?: number;
};

export type EpubChapterEvidenceMatch = {
  startTime: number;
  endTime: number;
  text: string;
  afterText: string;
  tokenOverlap: string[];
  quality: "none" | "weak" | "medium" | "strong";
};

export type EpubChapterEvidenceResult = {
  nodes: Array<{
    epubNodeId: string;
    title: string;
    estimatedStartTime: number;
    query: string;
    matches: EpubChapterEvidenceMatch[];
  }>;
};

function evidenceQuality(overlapCount: number): EpubChapterEvidenceMatch["quality"] {
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
): EpubChapterEvidenceMatch[] {
  const distinctQueryTokens = queryTokens.filter((token, index) => token.length >= 4 && queryTokens.indexOf(token) === index);
  if (distinctQueryTokens.length === 0) return [];
  const startMs = secondsToMs(scope.startTime ?? 0);
  const endMs = secondsToMs(scope.endTime ?? msToSeconds(ctx.durationMs));
  const words = [...ctx.transcript.words]
    .filter((word) => word.endMs >= startMs && word.startMs <= endMs)
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  const candidates: Array<EpubChapterEvidenceMatch & { score: number }> = [];
  const windowSize = Math.min(56, Math.max(24, distinctQueryTokens.length * 4));

  for (let index = 0; index < words.length; index++) {
    const window = words.slice(index, index + windowSize);
    if (window.length === 0) continue;
    const windowTokens = new Set(window.map((word) => word.token || textTokens(word.text)[0] || "").filter(Boolean));
    const tokenOverlap = distinctQueryTokens.filter((token) => windowTokens.has(token));
    if (tokenOverlap.length === 0) continue;
    const evidenceStartIndex = window.findIndex((word) => tokenOverlap.includes(word.token || textTokens(word.text)[0] || ""));
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
      tokenOverlap,
      quality: evidenceQuality(tokenOverlap.length),
      score: tokenOverlap.length + orderedBonus,
    });
  }

  const deduped: Array<EpubChapterEvidenceMatch & { score: number }> = [];
  for (const candidate of candidates.sort((a, b) => b.score - a.score || a.startTime - b.startTime)) {
    if (deduped.some((existing) => Math.abs(existing.startTime - candidate.startTime) < 30)) continue;
    deduped.push(candidate);
    if (deduped.length >= limit) break;
  }

  return deduped.map(({ score: _score, ...match }) => match);
}

export async function findEpubChapterEvidence(
  ctx: Pick<ChapterCurationContext, "epubEntries" | "durationMs" | "transcript">,
  input: FindEpubChapterEvidenceInput
): Promise<EpubChapterEvidenceResult> {
  const requestedIds = new Set(input.nodeIds?.map((id) => id.trim()).filter(Boolean));
  const entries = (requestedIds.size > 0 ? ctx.epubEntries.filter((entry) => requestedIds.has(entry.id)) : ctx.epubEntries).slice(0, 80);
  const limitPerNode = Math.max(1, Math.min(5, input.limitPerNode ?? 3));
  const averageChapterSeconds = ctx.epubEntries.length > 0 ? msToSeconds(ctx.durationMs) / ctx.epubEntries.length : msToSeconds(ctx.durationMs);
  const radiusSeconds = Math.max(300, Math.min(7_200, input.searchRadiusSeconds ?? averageChapterSeconds * 2));
  const nodes: EpubChapterEvidenceResult["nodes"] = [];

  for (const entry of entries) {
    const estimate = estimateTimestampFromEpubPosition(ctx, { epubNodeId: entry.id });
    if (!estimate) continue;
    const firstWords = summarizeFirstWords(entry, 72);
    const queryTokens = distinctFirstWordTokens(firstWords);
    const query = queryTokens.length > 0 ? queryTokens.slice(0, 18).join(" ") : firstWords || entry.title;
    if (!query.trim()) {
      nodes.push({
        epubNodeId: entry.id,
        title: entry.title,
        estimatedStartTime: estimate.estimatedStartTime,
        query: "",
        matches: [],
      });
      continue;
    }

    const scopedMatches = await fuzzySearchTranscript(ctx, {
      query,
      scope: {
        startTime: Math.max(0, estimate.estimatedStartTime - radiusSeconds),
        endTime: Math.min(msToSeconds(ctx.durationMs), estimate.estimatedStartTime + radiusSeconds),
      },
      limit: limitPerNode,
    });
    const wordMatches = transcriptWordEvidenceMatches(
      ctx,
      queryTokens,
      {
        startTime: Math.max(0, estimate.estimatedStartTime - radiusSeconds),
        endTime: Math.min(msToSeconds(ctx.durationMs), estimate.estimatedStartTime + radiusSeconds),
      },
      limitPerNode
    );
    const matches = scopedMatches.matches.length > 0 ? scopedMatches.matches : (await fuzzySearchTranscript(ctx, { query, limit: limitPerNode })).matches;
    const matchResults = [
      ...wordMatches,
      ...matches.map((match) => {
        const candidateText = `${match.text} ${match.after.text}`;
        const candidateTokens = new Set(textTokens(candidateText));
        const tokenOverlap = queryTokens.filter((token) => candidateTokens.has(token));
        return {
          startTime: match.startTime,
          endTime: match.endTime,
          text: normalizeToolText(match.text),
          afterText: normalizeToolText(match.after.text).slice(0, 500),
          tokenOverlap,
          quality: evidenceQuality(tokenOverlap.length),
        };
      }),
    ];
    nodes.push({
      epubNodeId: entry.id,
      title: entry.title,
      estimatedStartTime: estimate.estimatedStartTime,
      query,
      matches: matchResults
        .sort((a, b) => b.tokenOverlap.length - a.tokenOverlap.length || a.startTime - b.startTime)
        .filter((match, index, array) => array.findIndex((candidate) => Math.abs(candidate.startTime - match.startTime) < 30) === index)
        .slice(0, limitPerNode),
    });
  }
  return { nodes };
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
        const orderedMatchRatio = reverse?.orderedMatchRatio ?? 0;
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
          orderedMatchRatio,
          matchedTokens: reverse?.matchedTokens ?? [],
          boundaryUse:
            phrase.startWord <= 12 && (reverse?.relationToTarget === "opener" || reverse?.relationToTarget === "near_opener")
              ? "candidate_start"
              : "supporting_context",
          score: phrase.score * 4 + orderedMatchRatio * 6 + Math.max(0, 5 - relationRank) - distancePenalty - phraseOffsetPenalty,
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

const submitLeafChapterPlanSchema = z.object({
  spanPath: z.string().trim().min(1),
  strategy: z.string().trim().min(1),
  chapters: z.array(submittedChapterSchema).min(1).max(80),
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
export type SubmitLeafChapterPlanInput = z.infer<typeof submitLeafChapterPlanSchema>;
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

export type SubmitLeafChapterPlanResult =
  | {
      accepted: true;
      kind: "leaf";
      spanPath: string;
      strategy: string;
      notes: string | null;
      chapters: SubmittedChapter[];
      warnings: string[];
      audit: ChapterPlanAuditEntry[];
    }
  | {
      accepted: false;
      kind: "leaf";
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
  orderedMatchRatio: number;
  matchedTokens: string[];
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
  preStartTokenOverlap: string[];
  query: string;
  matchedTokens: string[];
  transcriptWindow: string;
};

export type FindFulcrumCandidatesResult = {
  spanPath: string;
  midpointEpubRatio: number;
  candidates: FulcrumCandidate[];
};

export type RecursiveSpanDecision =
  | { kind: "leaf"; chapters: SubmittedChapter[]; result?: SubmitLeafChapterPlanResult }
  | { kind: "split"; split: Extract<SubmitFulcrumSplitResult, { accepted: true }>; result?: SubmitFulcrumSplitResult };

export type RecursiveCurationReport = {
  path: string;
  depth: number;
  epubStartIndex: number;
  epubEndIndex: number;
  startTime: number;
  endTime: number;
  forceLeaf: boolean;
  outcome: "leaf" | "split" | "failed" | "limit";
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
  forceLeaf: boolean;
  finalOutput: unknown;
  newItems: unknown[];
  rawResponses: unknown[];
  error?: unknown;
};

function logChapterCurationProgress(ctx: Pick<ChapterCurationContext, "manifestation">, message: string): void {
  console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} ${message}`);
}

function logChapterCurationEvent(
  ctx: Pick<ChapterCurationContext, "manifestation" | "debugEventLogPath">,
  event: Record<string, unknown>
): void {
  const payload = {
    ts: new Date().toISOString(),
    manifestationId: ctx.manifestation.id,
    ...event,
  };
  if (ctx.debugEventLogPath) {
    try {
      appendFileSync(ctx.debugEventLogPath, `${JSON.stringify(payload)}\n`, "utf8");
    } catch (error) {
      console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} event_log_error=${JSON.stringify((error as Error).message)}`);
    }
  }
  if (typeof event.message === "string") logChapterCurationProgress(ctx, event.message);
}

function eventPreview(value: string, limit = 180): string {
  const text = normalizeToolText(value);
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

function summarizeToolResultForEvent(result: unknown): unknown {
  if (!result || typeof result !== "object") return result;
  const record = result as Record<string, unknown>;
  if (Array.isArray(record.matches)) {
    return {
      matches: record.matches.length,
      firstMatches: record.matches.slice(0, 3).map((match) => {
        const item = match as Record<string, unknown>;
        return {
          epubNodeId: item.epubNodeId,
          title: item.title,
          startTime: item.startTime,
          wordOffset: item.wordOffset,
          targetWordOffset: item.targetWordOffset,
          relationToTarget: item.relationToTarget,
          orderedMatchRatio: item.orderedMatchRatio,
          text: typeof item.text === "string" ? eventPreview(item.text) : undefined,
        };
      }),
    };
  }
  if (Array.isArray(record.nodes)) {
    return {
      nodes: record.nodes.length,
      firstNodes: record.nodes.slice(0, 3).map((node) => {
        const item = node as Record<string, unknown>;
        return {
          id: item.id,
          title: item.title,
          matches: Array.isArray(item.matches) ? item.matches.length : undefined,
        };
      }),
    };
  }
  if (Array.isArray(record.chapters)) return { chapters: record.chapters.length, diagnostics: record.diagnostics };
  if (Array.isArray(record.utterances)) return { startMs: record.startMs, endMs: record.endMs, utterances: record.utterances.length, text: typeof record.text === "string" ? eventPreview(record.text) : undefined };
  if (Array.isArray(record.phraseVariants)) {
    return {
      id: record.id,
      title: record.title,
      startWord: record.startWord,
      endWord: record.endWord,
      text: typeof record.text === "string" ? eventPreview(record.text) : undefined,
      phraseVariants: record.phraseVariants,
    };
  }
  if ("accepted" in record) return { accepted: record.accepted, kind: record.kind, errors: record.errors, warnings: record.warnings, instruction: record.instruction };
  if ("error" in record) return record;
  return record;
}

function logSpanToolCall(ctx: ChapterCurationContext, span: ChapterCurationSpan, toolName: string, input: unknown): void {
  logChapterCurationEvent(ctx, {
    type: "span-tool-call",
    message: `recursive span=${span.path} tool=${toolName} call`,
    span,
    toolName,
    input,
  });
}

function logSpanToolResult(ctx: ChapterCurationContext, span: ChapterCurationSpan, toolName: string, result: unknown): void {
  logChapterCurationEvent(ctx, {
    type: "span-tool-result",
    message: `recursive span=${span.path} tool=${toolName} result`,
    span,
    toolName,
    result: summarizeToolResultForEvent(result),
  });
}

function logSpanToolError(ctx: ChapterCurationContext, span: ChapterCurationSpan, toolName: string, error: unknown): void {
  logChapterCurationEvent(ctx, {
    type: "span-tool-error",
    message: `recursive span=${span.path} tool=${toolName} error=${JSON.stringify((error as Error).message ?? String(error))}`,
    span,
    toolName,
    error: {
      name: (error as Error).name,
      message: (error as Error).message,
    },
  });
}

function writeChapterCurationTrace(
  ctx: Pick<ChapterCurationContext, "manifestation" | "debugTraceDir">,
  name: string,
  payload: unknown
): string | undefined {
  if (!ctx.debugTraceDir) return undefined;
  try {
    mkdirSync(ctx.debugTraceDir, { recursive: true });
    const safeName = name.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
    const tracePath = path.join(ctx.debugTraceDir, `${new Date().toISOString().replace(/[:.]/g, "-")}-${safeName || "trace"}.json`);
    writeFileSync(tracePath, JSON.stringify(payload, null, 2), "utf8");
    return tracePath;
  } catch (error) {
    console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} trace_write_error=${JSON.stringify((error as Error).message)}`);
    return undefined;
  }
}

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

function chapterTitleKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function textTokens(value: string): string[] {
  return normalizedWordTokens(value).filter((token) => token.length >= 4);
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

function leafBoundaryEvidence(
  ctx: Pick<ChapterCurationContext, "epubEntries">,
  heading: NonNullable<ChapterPlanAuditEntry["claimedEpubHeading"]>,
  transcriptAfterStartText: string
): LeafBoundaryEvidence | null {
  if (!transcriptAfterStartText.trim()) return null;
  const result = searchEpubText(ctx, {
    query: transcriptAfterStartText,
    nodeIds: [heading.id],
    targetNodeId: heading.id,
    limit: 5,
  });
  return result.matches.find((match) => match.epubNodeId === heading.id) ?? null;
}

function hasStrongLeafBoundaryEvidence(evidence: LeafBoundaryEvidence | null, evidenceTokenCount: number): boolean {
  if (!evidence) return false;
  if (evidence.relationToTarget !== "opener" && evidence.relationToTarget !== "near_opener") return false;
  if (evidence.orderedMatchRatio < LEAF_BOUNDARY_MIN_ORDERED_MATCH_RATIO) return false;
  return evidence.matchedTokens.length >= Math.min(3, evidenceTokenCount);
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

function isMiddleFulcrumEpubIndex(ctx: Pick<ChapterCurationContext, "epubEntries">, span: ChapterCurationSpan, index: number): boolean {
  const ratio = spanEpubLocalRatio(ctx, span, index);
  return ratio !== null && ratio >= 0.3 && ratio <= 0.7;
}

function spanDurationSeconds(span: ChapterCurationSpan): number {
  return Math.max(0, span.endTime - span.startTime);
}

const INHERITED_BOUNDARY_TOLERANCE_SECONDS = 2;
const LEAF_BOUNDARY_MIN_ORDERED_MATCH_RATIO = 0.5;
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
    const matchedTokens = queryTokens.filter((token) => windowTokens.has(token));
    if (matchedTokens.length < Math.min(3, queryTokens.length)) continue;
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
    const textScore = matchedTokens.length / queryTokens.length + orderedMatches / queryTokens.length;
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
      preStartTokenOverlap,
      query: queryTokens.join(" "),
      matchedTokens,
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
      const textScore = match.tokenOverlap.length / Math.max(1, queryTokens.length);
      const preStartTokens = new Set(candidatePreStartTokens(ctx, match.startTime));
      const anchorTokens = queryTokens.slice(0, Math.min(6, queryTokens.length));
      const preStartTokenOverlap = queryTokens.filter((token) => preStartTokens.has(token));
      const preStartGapSeconds = candidatePreStartGapSeconds(ctx, match.startTime);
      const continuousPreRollPenalty = preStartGapSeconds !== null && preStartGapSeconds < 1.5 ? 0.35 : preStartGapSeconds !== null && preStartGapSeconds < 3 ? 0.2 : 0;
      const boundaryScore = Math.max(
        0,
        match.tokenOverlap.filter((token) => anchorTokens.includes(token)).length / Math.max(1, anchorTokens.length) -
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
        preStartTokenOverlap,
        query: queryTokens.join(" "),
        matchedTokens: match.tokenOverlap,
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

export async function validateFulcrumSplit(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  input: unknown
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
  if (split.startTime - span.startTime < edgeMargin || span.endTime - split.startTime < edgeMargin) {
    errors.push("Fulcrum startTime is too close to a span edge.");
  }
  const broadSpanRequiresMiddleFulcrum = !recursiveSpanAllowsLeaf(span, false);
  const splitEpubRatio = epubIndex >= 0 ? spanEpubLocalRatio(ctx, span, epubIndex) : null;
  if (broadSpanRequiresMiddleFulcrum && splitEpubRatio !== null && (splitEpubRatio < 0.3 || splitEpubRatio > 0.7)) {
    const leftPercent = Math.round(splitEpubRatio * 100);
    const rightPercent = Math.round((1 - splitEpubRatio) * 100);
    errors.push(
      `Broad-span fulcrum must be roughly in the middle by EPUB word position; proposed split leaves ${leftPercent}% of EPUB words on the left and ${rightPercent}% on the right.`
    );
  }

  const firstWords = entry ? summarizeFirstWords(entry, 72) : "";
  const titleTokens = new Set(textTokens(entry?.title ?? split.title));
  const expectedTokens = distinctiveEvidenceTokens(`${entry?.title ?? split.title} ${firstWords}`);
  const proseTokens = distinctiveEvidenceTokens(firstWords).filter((token) => !titleTokens.has(token));
  const window = getTranscriptWindowFromContext(ctx, secondsToMs(split.startTime), 45_000);
  const transcriptTokens = new Set(distinctiveEvidenceTokens(window.text));
  const matchedTokens = expectedTokens.filter((token) => transcriptTokens.has(token));
  const proseMatchedTokens = proseTokens.filter((token) => transcriptTokens.has(token));
  const overlapRatio = expectedTokens.length === 0 ? 0 : matchedTokens.length / expectedTokens.length;
  const evidence = entry
    ? await findEpubChapterEvidence(ctx, { nodeIds: [entry.id], searchRadiusSeconds: Math.max(600, spanDurationSeconds(span) / 2), limitPerNode: 3 })
    : { nodes: [] };
  const candidateMatches = evidence.nodes[0]?.matches ?? [];
  const nearestCandidateDelta = candidateMatches.length === 0 ? null : Math.min(...candidateMatches.map((match) => Math.abs(match.startTime - split.startTime)));
  const laterProseCandidate = candidateMatches
    .filter((match) => match.startTime > split.startTime && match.startTime - split.startTime <= 180 && match.tokenOverlap.length >= 3)
    .sort((a, b) => b.tokenOverlap.length - a.tokenOverlap.length || a.startTime - b.startTime)[0];
  const audit: FulcrumValidationAudit = {
    epubNodeId: entry?.id ?? null,
    title: entry?.title ?? split.title,
    startTime: split.startTime,
    expectedTokens,
    proseTokens,
    matchedTokens,
    proseMatchedTokens,
    overlapRatio,
    transcriptWindow: normalizeToolText(window.text).slice(0, 1_000),
    candidates: candidateMatches,
  };

  if (expectedTokens.length > 0 && matchedTokens.length < 3 && overlapRatio < 0.35) {
    errors.push("Fulcrum transcript window does not pass the fuzzy evidence threshold.");
  }
  if (proseTokens.length > 0 && proseMatchedTokens.length < Math.min(2, proseTokens.length)) {
    errors.push("Fulcrum evidence only matches title/metadata; at least one EPUB prose token must match.");
  }
  if (laterProseCandidate && proseMatchedTokens.length < Math.min(2, proseTokens.length)) {
    errors.push(
      `Submitted timestamp appears to anchor pre-boundary context before the EPUB opener; stronger ${entry?.id ?? "node"} prose evidence starts ${Math.round(laterProseCandidate.startTime - split.startTime)}s later at ${Math.round(laterProseCandidate.startTime)}s.`
    );
  }
  if (nearestCandidateDelta !== null && nearestCandidateDelta > 90) {
    errors.push(`Submitted fulcrum is ${Math.round(nearestCandidateDelta)}s from the nearest transcript evidence candidate; gather stronger candidate evidence before resubmitting.`);
  }

  if (errors.length > 0) {
    return {
      accepted: false,
      kind: "split",
      errors,
      warnings,
      audit,
      instruction:
        broadSpanRequiresMiddleFulcrum && splitEpubRatio !== null && (splitEpubRatio < 0.3 || splitEpubRatio > 0.7)
          ? "Pick an internal EPUB node closer to the span midpoint, ideally leaving 30-70% of the EPUB word span on each side, then search that node's opener prose in the transcript."
          : laterProseCandidate
            ? "The submitted phrase looks like context before the target EPUB opener. Inspect the later prose candidate with getTranscriptWindow and submit only if the opener begins exactly there; otherwise pick a different internal EPUB node."
            : "Pick a different internal fulcrum with stronger transcript/prose evidence, or submit a leaf plan for this span.",
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

export function validateLeafChapterPlan(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  input: unknown,
  options: { forceLeaf?: boolean } = {}
): SubmitLeafChapterPlanResult {
  const parsed = submitLeafChapterPlanSchema.safeParse(input);
  if (!parsed.success) {
    return {
      accepted: false,
      kind: "leaf",
      errors: parsed.error.issues.map((issue) => `${issue.path.join(".") || "input"}: ${issue.message}`),
      warnings: [],
      audit: [],
      instruction: "Revise the leaf chapter plan so it matches the submitLeafChapterPlan schema.",
    };
  }

  const plan = parsed.data;
  const errors: string[] = [];
  const warnings: string[] = [];
  if (plan.spanPath !== span.path) errors.push(`spanPath ${plan.spanPath} does not match current span ${span.path}`);
  const inheritedStartBoundary = span.startBoundary;
  if (inheritedStartBoundary) {
    const firstChapter = plan.chapters[0];
    if (!firstChapter) {
      errors.push(`span starts at accepted boundary "${inheritedStartBoundary.title}" but the leaf plan has no chapters`);
    } else {
      if (firstChapter.epubNodeId !== inheritedStartBoundary.epubNodeId) {
        errors.push(
          `chapters[0] must use inherited accepted boundary "${inheritedStartBoundary.title}" (${inheritedStartBoundary.epubNodeId}) at the span start`
        );
      }
      if (Math.abs(firstChapter.startTime - inheritedStartBoundary.startTime) > INHERITED_BOUNDARY_TOLERANCE_SECONDS) {
        errors.push(
          `chapters[0].startTime must use inherited accepted boundary ${inheritedStartBoundary.startTime}s for "${inheritedStartBoundary.title}"`
        );
      }
    }
  }
  let previousStartTime = span.startTime - 0.001;
  for (const [index, chapter] of plan.chapters.entries()) {
    if (chapter.startTime < span.startTime || chapter.startTime > span.endTime) {
      errors.push(`chapters[${index}].startTime is outside the current span`);
    }
    if (chapter.startTime <= previousStartTime) {
      errors.push(`chapters[${index}].startTime must be strictly greater than the previous chapter`);
    }
    previousStartTime = chapter.startTime;
    if (chapter.epubNodeId) {
      const epubIndex = ctx.epubEntries.findIndex((entry) => entry.id === chapter.epubNodeId);
      if (!spanContainsEpubIndex(span, epubIndex)) errors.push(`chapters[${index}].epubNodeId is outside the current span`);
    }
  }

  const audit = buildPlanAudit(ctx, plan.chapters);
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
    const isInheritedStartChapter =
      index === 0 &&
      inheritedStartBoundary !== undefined &&
      chapter.epubNodeId === inheritedStartBoundary.epubNodeId &&
      Math.abs(chapter.startTime - inheritedStartBoundary.startTime) <= INHERITED_BOUNDARY_TOLERANCE_SECONDS;
    if (isInheritedStartChapter) continue;
    const evidenceTokens = distinctFirstWordTokens(heading.firstWords);
    if (evidenceTokens.length > 0) {
      const transcriptTokens = new Set(textTokens(audit[index]?.transcriptAfterStart ?? ""));
      const overlap = evidenceTokens.filter((token) => transcriptTokens.has(token));
      if (overlap.length < Math.min(2, evidenceTokens.length)) {
        errors.push(`chapters[${index}] has weak transcript evidence for claimed EPUB heading "${heading.title}"`);
      }
      const boundaryEvidence = leafBoundaryEvidence(ctx, heading, audit[index]?.transcriptAfterStart ?? "");
      if (!hasStrongLeafBoundaryEvidence(boundaryEvidence, evidenceTokens.length)) {
        errors.push(
          `chapters[${index}] lacks strong opener/near-opener EPUB evidence for claimed heading "${heading.title}" ` +
            `(relation=${boundaryEvidence?.relationToTarget ?? "none"}, orderedMatchRatio=${boundaryEvidence?.orderedMatchRatio ?? 0})`
        );
      }
    }
  }

  const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
  const claimedIndexes = claimedEpubIndexes(ctx.epubEntries, plan.chapters).filter((index) => spanContainsEpubIndex(span, index));
  const spanDurationSeconds = span.endTime - span.startTime;
  if (!options.forceLeaf && (spanNodeCount > MAX_AUTOMATIC_LEAF_EPUB_NODES || spanDurationSeconds > 2 * 60 * 60)) {
    errors.push(
      `Span is too broad for a leaf plan (${spanNodeCount} EPUB node(s), ${Math.round(spanDurationSeconds / 60)} min); propose a validated fulcrum split instead.`
    );
  }
  if (spanNodeCount >= 4 && claimedIndexes.length < Math.ceil(spanNodeCount * 0.5)) {
    warnings.push(`Leaf covers ${claimedIndexes.length}/${spanNodeCount} EPUB node(s) in this span.`);
  }

  if (errors.length > 0) {
    return {
      accepted: false,
      kind: "leaf",
      errors,
      warnings,
      audit,
      instruction: "Revise this leaf plan with stronger transcript evidence or propose a validated fulcrum split instead.",
    };
  }

  return {
    accepted: true,
    kind: "leaf",
    spanPath: span.path,
    strategy: plan.strategy,
    notes: plan.notes ?? null,
    chapters: plan.chapters,
    warnings,
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
    const evidenceTokens = distinctFirstWordTokens(heading.firstWords);
    if (evidenceTokens.length > 0) {
      const transcriptTokens = new Set(textTokens(audit[index]?.transcriptAfterStart ?? ""));
      const overlap = evidenceTokens.filter((token) => transcriptTokens.has(token));
      if (overlap.length < Math.min(2, evidenceTokens.length)) {
        errors.push(`chapters[${index}] has weak transcript evidence for claimed EPUB heading "${heading.title}"`);
      }
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

const emptyToolSchema = z.object({});
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
const findEpubChapterEvidenceSchema = z.object({
  nodeIds: z.array(z.string()).max(80).optional(),
  searchRadiusSeconds: z.number().positive().max(7_200).optional(),
  limitPerNode: z.number().int().positive().max(5).optional(),
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
const getEpubNodeTextSchema = z.object({
  epubNodeId: z.string().trim().min(1),
  startWord: z.number().int().nonnegative().optional(),
  wordCount: z.number().int().positive().max(180).optional(),
});
const searchEpubTextSchema = z.object({
  query: z.string().trim().min(1),
  nodeIds: z.array(z.string().trim().min(1)).max(80).optional(),
  targetNodeId: z.string().trim().min(1).optional(),
  limit: z.number().int().positive().max(20).optional(),
});
const estimateTimestampFromEpubPositionSchema = z.object({
  epubNodeId: z.string(),
});
const getTranscriptWindowSchema = z.object({
  startTime: z.number(),
  radiusSeconds: z.number().positive().max(300).optional(),
});

function parseSubmitToolOutput(output: unknown): SubmitChapterPlanResult | null {
  if (!output) return null;
  if (typeof output === "string") {
    try {
      const parsed = JSON.parse(output) as unknown;
      return typeof parsed === "object" && parsed !== null && "accepted" in parsed ? (parsed as SubmitChapterPlanResult) : null;
    } catch {
      return null;
    }
  }
  if (typeof output === "object" && "accepted" in output) return output as SubmitChapterPlanResult;
  return null;
}

function parseSpanDecisionOutput(output: unknown): RecursiveSpanDecision | null {
  const value = typeof output === "string" ? safeJsonParse(output) : output;
  if (!value || typeof value !== "object") return null;
  const record = value as { accepted?: unknown; kind?: unknown; chapters?: unknown; epubNodeId?: unknown };
  if (record.accepted !== true) return null;
  if (record.kind === "leaf" && Array.isArray(record.chapters)) {
    return { kind: "leaf", chapters: record.chapters as SubmittedChapter[], result: record as SubmitLeafChapterPlanResult };
  }
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
      (result.tool.name === "submitLeafChapterPlan" || result.tool.name === "submitFulcrumSplit") &&
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
  decide: (span: ChapterCurationSpan, forceLeaf: boolean) => Promise<RecursiveSpanDecision | null>,
  options: { maxCalls?: number; maxConcurrency?: number; reports?: RecursiveCurationReport[] } = {}
): Promise<SubmittedChapter[] | null> {
  const maxCalls = options.maxCalls ?? 24;
  const maxConcurrency = Math.max(1, options.maxConcurrency ?? 4);
  const reports = options.reports;
  let calls = 0;
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

  async function visit(span: ChapterCurationSpan, forceLeaf: boolean): Promise<SubmittedChapter[] | null> {
    if (calls >= maxCalls) {
      logChapterCurationEvent(ctx, {
        type: "span-limit",
        message: `recursive span=${span.path} limit=max_calls`,
        span,
        forceLeaf,
        maxCalls,
      });
      reports?.push({ ...span, forceLeaf, outcome: "limit", errors: ["Recursive curation call limit reached."] });
      return null;
    }
    const mustLeaf = forceLeaf || recursiveSpanShouldForceLeaf(span) || calls >= maxCalls - 1;
    calls++;
    const decision = await withDecisionSlot(() => decide(span, mustLeaf));
    if (!decision) {
      logChapterCurationEvent(ctx, {
        type: "span-no-decision",
        message: `recursive span=${span.path} accepted=0 reason=no_decision`,
        span,
        forceLeaf: mustLeaf,
      });
      reports?.push({ ...span, forceLeaf: mustLeaf, outcome: "failed", errors: ["Span curator returned no accepted decision."] });
      return null;
    }
    if (decision.kind === "leaf") {
      logChapterCurationEvent(ctx, {
        type: "span-leaf-accepted",
        message: `recursive span=${span.path} leaf accepted=1 chapters=${decision.chapters.length} summary=${JSON.stringify(summarizeSubmittedChapters(decision.chapters))}`,
        span,
        forceLeaf: mustLeaf,
        chapters: decision.chapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(decision.chapters, 80),
        result: decision.result,
      });
      reports?.push({ ...span, forceLeaf: mustLeaf, outcome: "leaf", chapters: decision.chapters.length, chapterPlan: decision.chapters });
      return decision.chapters;
    }
    if (mustLeaf) {
      logChapterCurationEvent(ctx, {
        type: "span-forced-leaf-rejected-split",
        message: `recursive span=${span.path} accepted=0 reason=split_when_forced_leaf`,
        span,
        forceLeaf: mustLeaf,
        split: {
          epubNodeId: decision.split.epubNodeId,
          title: decision.split.title,
          startTime: decision.split.startTime,
        },
      });
      reports?.push({ ...span, forceLeaf: mustLeaf, outcome: "failed", errors: ["Span curator proposed a split when forced to submit a leaf."] });
      return null;
    }
    const { left, right } = splitSpan(span, decision.split);
    logChapterCurationEvent(ctx, {
      type: "span-split-accepted",
      message: `recursive span=${span.path} split accepted=1 epub=${decision.split.epubNodeId} time=${Math.round(decision.split.startTime)}s left=${left.path} right=${right.path}`,
      span,
      forceLeaf: mustLeaf,
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
      forceLeaf: mustLeaf,
      outcome: "split",
      split: {
        epubNodeId: decision.split.epubNodeId,
        title: decision.split.title,
        startTime: decision.split.startTime,
      },
    });
    const [leftChapters, rightChapters] = await Promise.all([visit(left, false), visit(right, false)]);
    if (!leftChapters || !rightChapters) return null;
    return normalizeSpanChapters([...leftChapters, ...rightChapters]);
  }

  return visit(createRootCurationSpan(ctx), false);
}

export function chapterCuratorToolUseBehavior(_: unknown, toolResults: FunctionToolResult[]): ToolsToFinalOutputResult {
  const submitResult = toolResults.find((result) => result.type === "function_output" && result.tool.name === "submitChapterPlan");
  if (!submitResult || submitResult.type !== "function_output") {
    return { isFinalOutput: false, isInterrupted: undefined };
  }
  const parsed = parseSubmitToolOutput(submitResult.output);
  if (parsed?.accepted) {
    return {
      isFinalOutput: true,
      isInterrupted: undefined,
      finalOutput: JSON.stringify(parsed),
    };
  }
  return { isFinalOutput: false, isInterrupted: undefined };
}

function spanScope(span: ChapterCurationSpan, inputScope?: TranscriptSearchScope): TranscriptSearchScope {
  return {
    startTime: Math.max(span.startTime, inputScope?.startTime ?? span.startTime),
    endTime: Math.min(span.endTime, inputScope?.endTime ?? span.endTime),
  };
}

function spanPrompt(ctx: ChapterCurationContext, span: ChapterCurationSpan, forceLeaf: boolean): string {
  const entries = spanEpubEntries(ctx, span);
  const allowLeaf = recursiveSpanAllowsLeaf(span, forceLeaf);
  const spanAudioOnlyIntervals = (ctx.audioOnlyIntervals ?? []).filter((interval) => interval.endTime >= span.startTime && interval.startTime <= span.endTime);
  const inheritedBoundaryInstructions = span.startBoundary
    ? [
        `This span starts at an already accepted parent split: ${span.startBoundary.title} (${span.startBoundary.epubNodeId}) at ${span.startBoundary.startTime}s.`,
        "For a leaf plan, the first chapter must normally be this inherited boundary at the span start; do not move it later unless you have evidence that the accepted parent split is wrong.",
        "The validator will treat this inherited first boundary as already proven, so spend research effort on later chapter starts in the span.",
      ]
    : [];
  const fulcrumWorkflow = forceLeaf
    ? []
      : [
        "Fulcrum workflow for broad spans:",
        "1. Call getEpubStructure and choose an internal EPUB node near the span midpoint by word position, not near either edge. The fulcrum should generally leave at least 30% of the EPUB word span on both sides.",
        "2. Call researchEpubBoundary for that node. It pre-ranks rare opener phrases, searches them near the expected time, inspects transcript windows, and reverse-checks hits against the target EPUB node.",
        "3. If researchEpubBoundary returns no opener/near_opener candidate, call getEpubNodeText and manually try the opener words: first 4-8 distinctive opener words, the next distinctive clause, a shorter exact phrase, and one phrase from the next word window if needed. Drop generic chapter numbers, titles, standalone names, punctuation-only differences, and repeated formulaic text.",
        "4. Estimate the likely transcript neighborhood from the EPUB node's word-position ratio, then search near that neighborhood with rgSearchTranscript first for manual follow-up. If a phrase misses, shorten it or try a different phrase from the same EPUB node; do not pivot to guessed timestamps.",
        "5. Inspect the best match with getTranscriptWindow. Use radiusSeconds=45 when the boundary is ambiguous, when nearby context may include pre-target or interior transcript, or after a rejected fulcrum. The proposed start must be the first matched opener word or the silence immediately before it; do not submit the start of a broad evidence window.",
        "6. If transcript context looks like pre-roll or interior prose, call searchEpubText with the transcript phrase and targetNodeId. Trust relationToTarget: opener/near_opener is usable boundary evidence; interior means do not submit that timestamp as a chapter start; pre_target means move later to the target opener.",
        "7. Treat submitFulcrumSplit as a final evidence-backed claim, not a probe. The call asserts that startTime is the audiobook start of epubNodeId. It is never an arbitrary scene, sentence, dialogue turn, or later distinctive phrase inside that node.",
        "8. Call submitFulcrumSplit only when you can already prove the chosen EPUB node opener begins at that exact timestamp or immediately after it. Distinctive later prose helps locate the area, but it is not valid split evidence unless reverse EPUB search says opener/near_opener.",
        "9. Before submitFulcrumSplit, have this proof in hand: target EPUB node opener text, the transcript search hit that found that opener, a transcript window showing the opener at/just after the proposed start, and reverse EPUB evidence that the transcript phrase is opener/near_opener for the target node. Put that proof in the evidence/notes fields.",
        "10. If you do not have that proof yet, keep researching instead of submitting. If rejected, keep the same EPUB node and search earlier/different opener phrases or a wider same-node word window before switching to a different middle node.",
      ];
  return [
    `Curate chapter markers for span ${span.path} of "${ctx.book.title}" by ${ctx.book.author}.`,
    `manifestationId: ${ctx.manifestation.id}`,
    `spanPath: ${span.path}`,
    `spanDepth: ${span.depth}`,
    `spanTimeSeconds: ${span.startTime}..${span.endTime}`,
    `spanEpubIndexes: ${span.epubStartIndex}..${span.epubEndIndex}`,
    `spanEpubNodeCount: ${entries.length}`,
    spanAudioOnlyIntervals.length > 0
      ? `audioOnlyIntervalsInSpan: ${JSON.stringify(spanAudioOnlyIntervals)}`
      : "audioOnlyIntervalsInSpan: []",
    forceLeaf
      ? "You are forced to submit a leaf chapter plan for this span. Do not call submitFulcrumSplit."
    : allowLeaf
        ? "This span is small enough for a leaf plan. SubmitLeafChapterPlan is the expected outcome."
        : "This span is too broad for a leaf plan. You must call submitFulcrumSplit with a validated internal EPUB node start.",
    "For a fulcrum, pick a high-confidence internal EPUB node start with transcript opener evidence at the timestamp.",
    allowLeaf
      ? "For a leaf, submit only chapter starts inside this span and include epubNodeId for every EPUB-backed chapter. Each non-inherited EPUB-backed start must reverse-search to opener/near_opener for that EPUB node."
      : "",
    spanAudioOnlyIntervals.length > 0
      ? "Some transcript time ranges in this span are classified as audio-only material with no EPUB node. Do not align EPUB chapter starts to those intervals unless opener evidence proves the EPUB text starts there."
      : "",
    ...inheritedBoundaryInstructions,
    "Prefer submitFulcrumSplit for spans with more than 8 EPUB nodes or more than 2 hours duration unless the whole span is already strongly evidenced.",
    ...fulcrumWorkflow,
    "All times are seconds.",
  ].filter(Boolean).join("\n");
}

export function recursiveSpanAllowsLeaf(span: ChapterCurationSpan, forceLeaf: boolean): boolean {
  const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
  const spanDurationSeconds = span.endTime - span.startTime;
  return forceLeaf || (spanNodeCount <= MAX_AUTOMATIC_LEAF_EPUB_NODES && spanDurationSeconds <= 2 * 60 * 60);
}

function recursiveSpanShouldForceLeaf(span: ChapterCurationSpan): boolean {
  const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
  const spanDurationSeconds = span.endTime - span.startTime;
  return spanNodeCount <= MAX_AUTOMATIC_LEAF_EPUB_NODES && spanDurationSeconds <= 2 * 60 * 60;
}

function createFulcrumJudgeAgent(ctx: ChapterCurationContext): Agent {
  return new Agent({
    name: "FulcrumJudge",
    model: ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You are a strict reviewer for audiobook chapter split points.",
      "Your job is to decide whether the proposed fulcrum timestamp is actually the start of the proposed EPUB node.",
      "Judge only whether the submitted evidence proves this exact timestamp. You do not know scene boundaries, narrative continuity, or the true timestamp beyond the provided audit/window.",
      "Reject a split when distinctive title/prose opener evidence is absent at the proposed timestamp, offset earlier/later in the window, generic, pre-target, or only an interior text match.",
      "Accept only when distinctive title/prose evidence appears at or immediately after the proposed timestamp.",
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

function fulcrumJudgePrompt(ctx: ChapterCurationContext, span: ChapterCurationSpan, split: Extract<SubmitFulcrumSplitResult, { accepted: true }>): string {
  return [
    `Judge this proposed audiobook chapter fulcrum for "${ctx.book.title}" by ${ctx.book.author}.`,
    "Return accepted=false if this timestamp is not clearly the start of the EPUB node.",
    "Prefer rejecting over accepting a suspicious split; the curator owns the next search.",
    "Do not suggest alternate timestamps or nodes.",
    "Do not make claims about scenes or narrative continuity. You are only judging whether opener/title evidence is present at or immediately after the submitted timestamp.",
    "Treat the finding as an evidence classification from the supplied audit/window, not as ground truth about the true boundary.",
    "",
    JSON.stringify(
      {
        span,
        proposed: {
          epubNodeId: split.epubNodeId,
          epubIndex: split.epubIndex,
          title: split.title,
          startTime: split.startTime,
          notes: split.notes,
        },
        audit: split.audit,
      },
      null,
      2
    ),
  ].join("\n");
}

async function judgeFulcrumSplit(
  ctx: ChapterCurationContext,
  span: ChapterCurationSpan,
  split: Extract<SubmitFulcrumSplitResult, { accepted: true }>
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
    const result = await runner.run(createFulcrumJudgeAgent(ctx), fulcrumJudgePrompt(ctx, span, split), {
      maxTurns: 4,
      signal: abort.signal,
      toolExecution: { maxFunctionToolConcurrency: 1 },
    });
    const judgment = parseFulcrumJudgmentOutput(result.finalOutput);
    const tracePath = writeChapterCurationTrace(ctx, `fulcrum-judge-${span.path}-${split.epubNodeId}`, {
      span,
      split,
      judgment,
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
    });
    logChapterCurationEvent(ctx, {
      type: "fulcrum-judge-result",
      message: `fulcrum judge span=${span.path} epub=${split.epubNodeId} accepted=${judgment?.accepted ?? "none"} confidence=${judgment?.confidence ?? "none"}`,
      span,
      split: {
        epubNodeId: split.epubNodeId,
        title: split.title,
        startTime: split.startTime,
      },
      judgment,
      tracePath,
    });
    return judgment;
  } catch (error) {
    logChapterCurationEvent(ctx, {
      type: "fulcrum-judge-error",
      message: `fulcrum judge span=${span.path} epub=${split.epubNodeId} error=${JSON.stringify((error as Error).message)}`,
      span,
      split: {
        epubNodeId: split.epubNodeId,
        title: split.title,
        startTime: split.startTime,
      },
      error: serializeAgentError(error),
    });
    return null;
  } finally {
    clearTimeout(timeout);
    await provider.close().catch(() => undefined);
  }
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
      return "The judge only classified the submitted evidence as showing opener evidence away from the submitted timestamp. Verify any adjusted timestamp with your own transcript search/window evidence before resubmitting.";
    case "tool_classified_interior_match":
    case "generic_or_weak_overlap":
      return "Do not resubmit nearby body-text overlap. Return to the target EPUB opener text, search a different distinctive opener phrase, and reverse-check transcript context with searchEpubText before submitting.";
    case "submitted_evidence_insufficient":
      return "Gather stronger evidence before another split: call researchEpubBoundary or search distinctive opener prose with rgSearchTranscript/fuzzySearchTranscript, inspect the window, and reverse-check the transcript phrase.";
    case "opener_evidence_at_timestamp":
      return "The judge rejected despite opener_evidence_at_timestamp classification. Reinspect the transcript window and submit only if the first opener word is at or immediately after the proposed timestamp.";
  }
}

function createRecursiveSpanCuratorAgent(ctx: ChapterCurationContext, span: ChapterCurationSpan, forceLeaf: boolean): Agent {
  let invalidFulcrums = 0;
  let rejectedLeafRequiresEvidence = false;
  let evidenceCallsSinceRejectedLeaf = 0;
  const allowLeaf = recursiveSpanAllowsLeaf(span, forceLeaf);
  const rejectedFulcrums = new Set<string>();
  let rejectedFulcrumRequiresEvidence = false;
  let evidenceCallsSinceRejectedFulcrum = 0;
  let transcriptSearchesSinceFulcrumSubmit = 0;

  function markEvidenceToolUsed(): void {
    if (rejectedLeafRequiresEvidence) evidenceCallsSinceRejectedLeaf++;
    if (rejectedFulcrumRequiresEvidence) evidenceCallsSinceRejectedFulcrum++;
  }

  function markTranscriptSearchToolUsed(): void {
    markEvidenceToolUsed();
    transcriptSearchesSinceFulcrumSubmit++;
  }

  function rejectedLeafWithoutEvidence(): SubmitLeafChapterPlanResult {
    return {
      accepted: false,
      kind: "leaf",
      errors: ["submitLeafChapterPlan was called again before gathering new transcript evidence after the previous rejection."],
      warnings: [],
      audit: [],
      instruction: "Call findEpubChapterEvidence, rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow before resubmitting this leaf.",
    };
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
    model: ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You curate audiobook chapter markers for one bounded span, not the whole book.",
      "You must either submit a leaf chapter plan or propose one validated fulcrum split.",
      "Use submitFulcrumSplit when the span is broad and you can identify the audiobook start of an internal EPUB node.",
      allowLeaf
        ? "Use submitLeafChapterPlan when the span is small enough or already well evidenced. For each non-inherited EPUB-backed chapter start, verify transcript text at the proposed start reverse-searches to opener/near_opener for that EPUB node."
        : "This span is too broad for a leaf plan. The submitLeafChapterPlan tool is intentionally unavailable; you must find a fulcrum split.",
      "Concrete fulcrum workflow for broad spans:",
      "1. Call getEpubStructure, compare node word-position ratios inside this span, and choose an internal EPUB node roughly near the span midpoint. Avoid boundaries that leave less than 30% of the EPUB word span on either side; they are poor fulcrums for divide-and-conquer.",
      "2. Call researchEpubBoundary for that node. It pre-ranks rare opener phrases, searches them near the expected time, inspects transcript windows, and reverse-checks whether hits are opener/near_opener for the target EPUB node.",
      "3. If researchEpubBoundary returns a strong opener/near_opener candidate, use that exact timestamp or the silence immediately before the first matched opener word. If it returns no strong candidate, call getEpubNodeText and manually try different opener phrases from the same node before switching nodes.",
      "4. Estimate the likely timestamp neighborhood from the EPUB node position within the current span. Search that neighborhood with rgSearchTranscript first when doing manual follow-up. If a phrase misses, shorten it or try a different phrase from the same EPUB node; do not pivot to guessed timestamps.",
      "5. Inspect the best candidate with getTranscriptWindow. Use radiusSeconds=45 when the boundary is ambiguous, when nearby context may include pre-target or interior transcript, or after a rejected fulcrum. Check both sides of the timestamp: the proposed start should be the first matched opener word or the silence immediately before it, not the start of a broad evidence window.",
      "6. If transcript context looks like it may include pre-roll or interior prose, call searchEpubText with the transcript phrase and targetNodeId. Trust relationToTarget: opener/near_opener is usable boundary evidence; interior means do not submit that timestamp as a chapter start; pre_target means move later to the target opener.",
      "7. Treat submitFulcrumSplit as a final evidence-backed claim, not a probing tool. The call asserts that startTime is the audiobook start of epubNodeId. It is never an arbitrary scene, sentence, dialogue turn, or later distinctive phrase inside that node.",
      "8. Call submitFulcrumSplit only when you can already prove the chosen EPUB node opener begins at that exact timestamp or immediately after it. Distinctive later prose helps locate the area, but it is not valid split evidence unless reverse EPUB search says opener/near_opener.",
      "9. Before submitFulcrumSplit, have this proof in hand: target EPUB node opener text, the transcript search hit that found that opener, a transcript window showing the opener at/just after the proposed start, and reverse EPUB evidence that the transcript phrase is opener/near_opener for the target node. Put that proof in the evidence/notes fields.",
      "10. If you do not have that proof yet, keep researching instead of submitting. If the judge rejects it, do not use the judge as the search engine and do not resubmit the same node/timestamp; keep the same EPUB node and search earlier/different opener phrases or a wider same-node word window before switching to a different middle node.",
      "Do not submit guessed timestamps or timestamps copied from estimated EPUB position. A broad-span fulcrum must be backed by a transcript search result from rgSearchTranscript or fuzzySearchTranscript.",
      "After any rejected fulcrum, run a fresh rgSearchTranscript or fuzzySearchTranscript query from the same EPUB node's opener text before trying another fulcrum.",
      "All tool times and submitted startTime values are seconds, not milliseconds.",
      forceLeaf ? "This span is forced leaf mode. You must call submitLeafChapterPlan, not submitFulcrumSplit." : "",
      allowLeaf && !forceLeaf ? "This small span is leaf-first. Submit a leaf plan instead of spending turns on a fulcrum split." : "",
    ]
      .filter(Boolean)
      .join("\n"),
    tools: [
      tool({
        name: "getEpubStructure",
        description: allowLeaf
          ? "Return ordered EPUB nodes for this span only."
          : "Return ordered EPUB nodes that are eligible as middle-band fulcrums for this broad span.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => {
          return runToolWithEvents("getEpubStructure", {}, () => {
            const full = getEpubStructure(ctx);
            const spanNodes = full.nodes.filter((node) => node.index >= span.epubStartIndex && node.index <= span.epubEndIndex);
            return {
              ...full,
              nodes: allowLeaf ? spanNodes : spanNodes.filter((node) => isMiddleFulcrumEpubIndex(ctx, span, node.index)),
            };
          });
        },
      }),
      tool({
        name: "getEmbeddedAudioChapters",
        description: "Return embedded audio chapter boundaries and diagnostics for context.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => {
          return runToolWithEvents("getEmbeddedAudioChapters", {}, () => {
            const result = getEmbeddedAudioChapters(ctx);
            return {
              ...result,
              chapters: result.chapters.filter((chapter) => chapter.endTime >= span.startTime && chapter.startTime <= span.endTime),
            };
          });
        },
      }),
      tool({
        name: "getEpubNodeText",
        description: "Return a bounded EPUB word window and suggested exact-search phrase variants for one node. Use this to try different opener phrases from the same fulcrum chapter before switching nodes.",
        parameters: getEpubNodeTextSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("getEpubNodeText", input, () => {
            markEvidenceToolUsed();
            const entryIndex = ctx.epubEntries.findIndex((entry) => entry.id === input.epubNodeId);
            if (entryIndex < span.epubStartIndex || entryIndex > span.epubEndIndex || (!allowLeaf && !isMiddleFulcrumEpubIndex(ctx, span, entryIndex))) {
              return {
                error: `EPUB node ${input.epubNodeId} is not eligible for this span.`,
                spanPath: span.path,
              };
            }
            return getEpubNodeText(ctx, input);
          });
        },
      }),
      tool({
        name: "researchEpubBoundary",
        description:
          "Pre-research one target EPUB boundary: ranked rare opener phrases, transcript hits near the expected time, transcript windows, and reverse EPUB opener/near_opener classifications. Use this after choosing a middle fulcrum node.",
        parameters: researchEpubBoundarySchema,
        strict: true,
        execute: async (input) => {
          return runToolWithEvents("researchEpubBoundary", input, async () => {
            markTranscriptSearchToolUsed();
            const entryIndex = ctx.epubEntries.findIndex((entry) => entry.id === input.epubNodeId);
            if (entryIndex < span.epubStartIndex || entryIndex > span.epubEndIndex || (!allowLeaf && !isMiddleFulcrumEpubIndex(ctx, span, entryIndex))) {
              return {
                error: `EPUB node ${input.epubNodeId} is not eligible for this span.`,
                spanPath: span.path,
              };
            }
            return researchEpubBoundary(ctx, {
              ...input,
              scope: { startTime: span.startTime, endTime: span.endTime },
              searchRadiusSeconds: input.searchRadiusSeconds ?? Math.max(600, Math.min(7_200, spanDurationSeconds(span) / 4)),
            });
          });
        },
      }),
      tool({
        name: "searchEpubText",
        description:
          "Reverse-search normalized EPUB text for a phrase taken from the transcript. Use targetNodeId to see whether a transcript phrase is opener, near_opener, interior, pre_target, or post_target relative to the intended EPUB node.",
        parameters: searchEpubTextSchema,
        strict: true,
        execute: (input) => {
          return runToolWithEvents("searchEpubText", input, () => {
            markEvidenceToolUsed();
            const allowedIds = new Set(spanEpubEntries(ctx, span).map((entry) => entry.id));
            const nodeIds = (input.nodeIds?.length ? input.nodeIds : Array.from(allowedIds)).filter((id) => allowedIds.has(id));
            const targetNodeId = input.targetNodeId && allowedIds.has(input.targetNodeId) ? input.targetNodeId : input.targetNodeId;
            return searchEpubText(ctx, { ...input, nodeIds, targetNodeId });
          });
        },
      }),
      tool({
        name: "findEpubChapterEvidence",
        description: "Batch fuzzy-search transcript evidence for EPUB chapter nodes in this span. Times are seconds.",
        parameters: findEpubChapterEvidenceSchema,
        strict: true,
        execute: async (input) => {
          return runToolWithEvents("findEpubChapterEvidence", input, async () => {
            markEvidenceToolUsed();
            const allowedEntries = spanEpubEntries(ctx, span).filter((_, offset) => {
              const index = span.epubStartIndex + offset;
              return allowLeaf || isMiddleFulcrumEpubIndex(ctx, span, index);
            });
            const allowedIds = new Set(allowedEntries.map((entry) => entry.id));
            const nodeIds = (input.nodeIds?.length ? input.nodeIds : Array.from(allowedIds)).filter((id) => allowedIds.has(id));
            const result = await findEpubChapterEvidence(ctx, { ...input, nodeIds });
            return {
              nodes: result.nodes.map((node) => ({
                ...node,
                matches: node.matches.filter((match) => match.endTime >= span.startTime - 300 && match.startTime <= span.endTime + 300),
              })),
            };
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
        description:
          "Submit a final, evidence-backed EPUB node start for this span. This asserts startTime is the audiobook start of epubNodeId, not an arbitrary interior phrase. Use only after opener text, transcript-search hit, inspected transcript window, and reverse EPUB opener/near_opener proof for the exact timestamp.",
        parameters: submitFulcrumSplitSchema,
        strict: true,
        execute: async (input) => {
          return runToolWithEvents("submitFulcrumSplit", input, async () => {
            if (rejectedFulcrumRequiresEvidence && evidenceCallsSinceRejectedFulcrum === 0) return rejectedFulcrumWithoutEvidence();
            if (!allowLeaf && transcriptSearchesSinceFulcrumSubmit === 0) {
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
            if (forceLeaf || (allowLeaf && invalidFulcrums >= 2)) {
              return {
                accepted: false,
                kind: "split",
                errors: ["Fulcrum splitting is no longer allowed for this span; submit a leaf plan."],
                warnings: [],
                audit: null,
                instruction: "Call submitLeafChapterPlan for this span.",
              } satisfies SubmitFulcrumSplitResult;
            }
            const rejectedKey = `${input.epubNodeId}:${Math.round(input.startTime)}`;
            if (rejectedFulcrums.has(rejectedKey)) {
              rejectedFulcrumRequiresEvidence = true;
              evidenceCallsSinceRejectedFulcrum = 0;
              return {
                accepted: false,
                kind: "split",
                errors: [`Fulcrum ${input.epubNodeId} near ${Math.round(input.startTime)}s was already rejected for this span.`],
                warnings: [],
                audit: null,
                instruction: "Stay on this EPUB node unless you have exhausted its opener text. Call getEpubNodeText for an earlier/different word window, search a different phrase from that node, and submit a materially different timestamp with stronger evidence.",
              } satisfies SubmitFulcrumSplitResult;
            }
            const result = await validateFulcrumSplit(ctx, span, input);
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
                  errors: [
                    `Fulcrum judge rejected this split: ${judgment.reason}`,
                    `Judge finding: ${judgment.finding}`,
                    `Judge opener evidence at timestamp: ${judgment.openerEvidenceAtTimestamp}`,
                    ...judgment.concerns.map((concern) => `Judge evidence note: ${concern}`),
                  ],
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
      ...(allowLeaf
        ? [
            tool({
              name: "submitLeafChapterPlan",
              description: "Submit the final chapter plan for this span. Validation feedback is returned if rejected.",
              parameters: submitLeafChapterPlanSchema,
              strict: true,
              execute: (input) => {
                return runToolWithEvents("submitLeafChapterPlan", input, () => {
                  if (rejectedLeafRequiresEvidence && evidenceCallsSinceRejectedLeaf === 0) return rejectedLeafWithoutEvidence();
                  const result = validateLeafChapterPlan(ctx, span, input, { forceLeaf });
                  rejectedLeafRequiresEvidence = !result.accepted;
                  evidenceCallsSinceRejectedLeaf = 0;
                  return result;
                });
              },
            }),
          ]
        : []),
    ],
    toolUseBehavior: recursiveSpanToolUseBehavior,
  });
}

export function createChapterCuratorAgent(ctx: ChapterCurationContext): Agent {
  let rejectedSubmitRequiresEvidence = false;
  let evidenceCallsSinceRejectedSubmit = 0;
  let submitAttempts = 0;

  function markEvidenceToolUsed(): void {
    if (rejectedSubmitRequiresEvidence) evidenceCallsSinceRejectedSubmit++;
  }

  function rejectSubmitUntilEvidence(): SubmitChapterPlanResult {
    return {
      accepted: false,
      errors: [
        "submitChapterPlan was called again before gathering new transcript evidence after the previous rejection.",
        "Call rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow for the rejected chapters before resubmitting.",
      ],
      warnings: [],
      audit: [],
      instruction:
        "Do not call submitChapterPlan again yet. Your next tool call must gather transcript evidence for the rejected chapter starts.",
    };
  }

  return new Agent({
    name: "ChapterCurator",
    model: ctx.settings.agents.model,
    modelSettings: chapterCurationModelSettings(ctx, {
      toolChoice: "required",
      parallelToolCalls: false,
    }),
    resetToolChoice: false,
    instructions: [
      "You curate audiobook chapter markers from EPUB structure, embedded audio chapters, and transcript evidence.",
      "You must use tools to inspect the available evidence. Embedded audio chapters are evidence, not truth; equal divisions and generic labels are suspicious.",
      "Use submitChapterPlan sparingly. It is the final validation gate, not a search or brainstorming tool.",
      "Before your first submitChapterPlan call, inspect EPUB structure, embedded audio chapters, and transcript evidence with rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow.",
      "For EPUB-rich books, build the plan from transcript evidence. Estimate tools and embedded boundaries are only priors; they are not enough evidence for an EPUB-heading claim.",
      "Use findEpubChapterEvidence to gather candidate transcript anchors for many EPUB chapters at once before manually searching individual failures.",
      "All tool inputs and submitted chapter startTime values are seconds, not milliseconds. Example: 3 minutes 10 seconds is 190, not 190000.",
      "If you claim an epubNodeId for a chapter, verify the proposed start against transcript text near that timestamp or by searching for distinctive words from that EPUB node.",
      "If submitChapterPlan rejects a chapter for weak transcript evidence, do not resubmit the same timestamp/title. First call rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow to find better evidence for that chapter.",
      "If submitChapterPlan rejects a plan as too coarse, do not fall back to part boundaries. Add supported EPUB-level chapter markers and validate them with transcript evidence.",
      "Never call submitChapterPlan twice in a row after a rejection. The next tool call after a rejected submit must be rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow.",
      "Do not make no-op, compliance, last-resort, or best-effort retries. If you cannot fix a rejected plan with new evidence, gather more evidence instead of resubmitting.",
      "Aim for at most three submitChapterPlan calls total: one initial plan and up to two evidence-driven revisions.",
      "Do not invent chapters that are not supported by either EPUB structure or transcript context.",
      "When you have a plan, call submitChapterPlan. If submitChapterPlan rejects it, use the audit feedback and call submitChapterPlan again.",
      "Never provide a natural-language final answer instead of submitChapterPlan.",
      ctx.settings.agents.editionPreference ? `Global edition preference, for context only: ${ctx.settings.agents.editionPreference}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
    tools: [
      tool({
        name: "getEpubStructure",
        description: "Return ordered EPUB headings, word counts, cumulative ratios, and first words for each EPUB node.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => getEpubStructure(ctx),
      }),
      tool({
        name: "getEmbeddedAudioChapters",
        description: "Return embedded audio chapter boundaries and diagnostics about whether they appear trustworthy.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => getEmbeddedAudioChapters(ctx),
      }),
      tool({
        name: "rgSearchTranscript",
        description:
          "Search transcript utterances with ripgrep. Time scopes are in seconds, not milliseconds. Use regex=false for literal phrases and regex=true for regular expressions.",
        parameters: rgSearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return rgSearchTranscript(ctx, input);
        },
      }),
      tool({
        name: "fuzzySearchTranscript",
        description: "Fuzzy-search transcript utterances for approximate chapter-heading or first-words matches. Time scopes are in seconds, not milliseconds.",
        parameters: fuzzySearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return fuzzySearchTranscript(ctx, input);
        },
      }),
      tool({
        name: "findEpubChapterEvidence",
        description:
          "Batch fuzzy-search transcript evidence for EPUB chapter nodes. Use this before submitting EPUB-rich plans so each epubNodeId has candidate transcript anchors. Times are seconds.",
        parameters: findEpubChapterEvidenceSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return findEpubChapterEvidence(ctx, input);
        },
      }),
      tool({
        name: "estimateTimestampFromEpubPosition",
        description: "Estimate the audio timestamp for an EPUB node from its cumulative word position and total audio duration.",
        parameters: estimateTimestampFromEpubPositionSchema,
        strict: true,
        execute: (input) => estimateTimestampFromEpubPosition(ctx, input),
      }),
      tool({
        name: "getTranscriptWindow",
        description: "Return transcript utterances around a timestamp. startTime is seconds, not milliseconds.",
        parameters: getTranscriptWindowSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return getTranscriptWindow(ctx, input);
        },
      }),
      tool({
        name: "submitChapterPlan",
        description: "Submit the final chapter plan. Validation feedback is returned if the plan needs revision.",
        parameters: submitChapterPlanSchema,
        strict: true,
        execute: (input) => {
          submitAttempts++;
          if (rejectedSubmitRequiresEvidence && evidenceCallsSinceRejectedSubmit === 0) return rejectSubmitUntilEvidence();
          if (submitAttempts > 6 && evidenceCallsSinceRejectedSubmit === 0) return rejectSubmitUntilEvidence();
          const result = submitChapterPlan(ctx, input);
          rejectedSubmitRequiresEvidence = !result.accepted;
          evidenceCallsSinceRejectedSubmit = 0;
          return result;
        },
      }),
    ],
    toolUseBehavior: chapterCuratorToolUseBehavior,
  });
}

export function chapterCuratorPrompt(ctx: ChapterCurationContext): string {
  return [
    `Curate chapter markers for "${ctx.book.title}" by ${ctx.book.author}.`,
    `manifestationId: ${ctx.manifestation.id}`,
    `durationSeconds: ${msToSeconds(ctx.durationMs)}`,
    `epubNodeCount: ${ctx.epubEntries.length}`,
    `transcriptUtteranceCount: ${ctx.transcript.utterances?.length ?? 0}`,
    `embeddedChapterCount: ${ctx.embeddedChapters.length}`,
    "",
    "Find a chapter plan that is useful for listening. Prefer real narrative sections over front/back matter unless the front/back matter is audibly distinct and useful.",
    "All times you pass to tools or submit in the final plan must be seconds. Do not use milliseconds.",
    "Workflow requirement:",
    "1. Inspect EPUB structure and embedded audio chapters.",
    "2. Gather transcript evidence before submitting a plan. For an EPUB-rich book, use findEpubChapterEvidence for candidate anchors, then search/window-check individual uncertain starts.",
    "3. Submit one evidence-backed plan.",
    "4. If rejected, your next tool call must be transcript search/window evidence, not another submit. Fix rejected chapters before resubmitting.",
    "5. Never make no-op, compliance, last-resort, or best-effort retry submissions. Repeated invalid submissions waste the turn budget and fail the task.",
    "Return only by calling submitChapterPlan.",
  ].join("\n");
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
    const recursiveReports: RecursiveCurationReport[] = [];
    const recursiveSpanTraces: RecursiveSpanTrace[] = [];
    const recursiveMaxSpanConcurrency = 24;
    logChapterCurationEvent(curationCtx, {
      type: "recursive-run-start",
      message: "recursive run start=1",
      model: curationCtx.settings.agents.model,
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
      async (span, forceLeaf) => {
        const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
        logChapterCurationEvent(curationCtx, {
          type: "span-start",
          message: `recursive span=${span.path} depth=${span.depth} epub=${span.epubStartIndex}-${span.epubEndIndex} nodes=${spanNodeCount} time=${Math.round(span.startTime)}-${Math.round(span.endTime)}s force_leaf=${forceLeaf} start=1`,
          span,
          forceLeaf,
          spanNodeCount,
        });
        const startedAt = Date.now();
        const delays = [0, 15_000, 45_000];
        for (let attempt = 0; attempt < delays.length; attempt++) {
          if (delays[attempt]! > 0) {
            logChapterCurationEvent(curationCtx, {
              type: "span-retry-sleep",
              message: `recursive span=${span.path} retry=${attempt} sleep_ms=${delays[attempt]}`,
              span,
              forceLeaf,
              attempt,
              sleepMs: delays[attempt],
            });
            await sleep(delays[attempt]!);
          }
          try {
            const spanResult = await runner.run(createRecursiveSpanCuratorAgent(curationCtx, span, forceLeaf), spanPrompt(curationCtx, span, forceLeaf), {
              maxTurns: forceLeaf ? 24 : 64,
              signal: abort.signal,
              toolExecution: { maxFunctionToolConcurrency: recursiveMaxSpanConcurrency },
            });
            const decision = parseSpanDecisionOutput(spanResult.finalOutput);
            const elapsedMs = Date.now() - startedAt;
            const tracePayload = {
              span,
              forceLeaf,
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
              forceLeaf,
              finalOutput: spanResult.finalOutput,
              newItems: spanResult.newItems as unknown[],
              rawResponses: spanResult.rawResponses as unknown[],
            });
            logChapterCurationEvent(curationCtx, {
              type: "span-agent-result",
              message: `recursive span=${span.path} elapsed_ms=${elapsedMs} attempt=${attempt + 1} decision=${decision?.kind ?? "none"}${
                decision?.kind === "leaf" ? ` chapters=${decision.chapters.length}` : ""
              }${decision?.kind === "split" ? ` split_epub=${decision.split.epubNodeId} split_time=${Math.round(decision.split.startTime)}s` : ""}`,
              span,
              forceLeaf,
              attempt: attempt + 1,
              elapsedMs,
              decisionKind: decision?.kind ?? null,
              tracePath,
              decision:
                decision?.kind === "leaf"
                  ? {
                      kind: "leaf",
                      chapters: decision.chapters.length,
                      summary: summarizeSubmittedChapters(decision.chapters),
                      chapterPlan: summarizeSubmittedChapterObjects(decision.chapters),
                      result: decision.result,
                    }
                  : decision?.kind === "split"
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
            return decision;
          } catch (error) {
            const message = (error as Error).message;
            const elapsedMs = Date.now() - startedAt;
            const serializedError = serializeAgentError(error);
            const tracePath = writeChapterCurationTrace(curationCtx, `span-${span.path}-attempt-${attempt + 1}-error`, {
              span,
              forceLeaf,
              attempt: attempt + 1,
              elapsedMs,
              error: serializedError,
            });
            recursiveSpanTraces.push({
              path: span.path,
              depth: span.depth,
              forceLeaf,
              finalOutput: null,
              newItems: [],
              rawResponses: [],
              error: serializedError,
            });
            logChapterCurationEvent(curationCtx, {
              type: "span-error",
              message: `recursive span=${span.path} elapsed_ms=${elapsedMs} attempt=${attempt + 1} error=${JSON.stringify(message)}`,
              span,
              forceLeaf,
              attempt: attempt + 1,
              elapsedMs,
              retryable: retryableAgentError(error),
              tracePath,
              error: serializedError,
            });
            if (attempt < delays.length - 1 && retryableAgentError(error)) continue;
            recursiveReports.push({
              ...span,
              forceLeaf,
              outcome: "failed",
              errors: [message],
            });
            return null;
          }
        }
        return null;
      },
      { maxCalls: 64, maxConcurrency: recursiveMaxSpanConcurrency, reports: recursiveReports }
    );
    if (recursiveChapters && recursiveChapters.length > 0) {
      logChapterCurationEvent(curationCtx, {
        type: "recursive-merge-start",
        message: `recursive merge chapters=${recursiveChapters.length} validate=1`,
        chapters: recursiveChapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(recursiveChapters, 80),
      });
      const recursiveResult = submitChapterPlan(curationCtx, {
        manifestationId: curationCtx.manifestation.id,
        strategy: "Recursive fulcrum chapter curation",
        chapters: recursiveChapters,
        notes: "Merged from recursively curated span plans.",
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
        forceLeaf: false,
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
  const recursive = await runRecursiveAgenticChapterCurationDetailed(ctx);
  if (recursive.result) return recursive;

  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) return recursive;
  const abort = new AbortController();
  const timeout = setTimeout(() => abort.abort(), Math.max(5_000, ctx.settings.agents.timeoutMs));
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    logChapterCurationEvent(ctx, {
      type: "global-fallback-start",
      message: "recursive result=null fallback=global",
    });
    logChapterCurationEvent(ctx, {
      type: "global-fallback-agent-start",
      message: "global fallback start=1",
    });
    const result = await runner.run(createChapterCuratorAgent(ctx), chapterCuratorPrompt(ctx), {
      maxTurns: 64,
      signal: abort.signal,
      toolExecution: { maxFunctionToolConcurrency: 4 },
    });
    const parsedResult = parseSubmitToolOutput(result.finalOutput);
    const tracePath = writeChapterCurationTrace(ctx, "global-fallback", {
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
    });
    logChapterCurationEvent(ctx, {
      type: "global-fallback-agent-result",
      message: `global fallback done=1 result=${parsedResult?.accepted ? "accepted" : "none"}`,
      tracePath,
      finalOutput: result.finalOutput,
    });
    return {
      result: parsedResult,
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
      recursiveReports: recursive.recursiveReports,
      recursiveSpanTraces: recursive.recursiveSpanTraces,
    };
  } finally {
    clearTimeout(timeout);
    await provider.close().catch(() => undefined);
  }
}
