import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream, readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { initEpubFile } from "@lingo-reader/epub-parser";
import OpenAI from "openai";
import type { TranscriptionVerbose } from "openai/resources/audio/transcriptions";

import { selectPreferredAudioAsset } from "./asset-selection";
import type { BooksRepo } from "../repo";
import type { AppSettings, AssetFileRow, AssetRow, AssetTranscriptRow, BookRow, ChapterAnalysisRow, JobRow } from "../app-types";

const CHAPTER_ANALYSIS_SOURCE = "full_transcript_epub";
const CHAPTER_ANALYSIS_ALGORITHM_VERSION = "2026-04-05-v6";
const CHAPTERS_API_VERSION = "1.4.0";
const TRANSCRIPTION_MODEL = "whisper-1";
const CHUNK_MS = 30 * 60_000;
const CHUNK_OVERLAP_MS = 30_000;
const TRANSCRIPTION_TIMEOUT_MS = 5 * 60_000;
const PROBE_WORDS = 24;
const MIN_SHINGLE = 6;
const ALIGNMENT_PROBE_WORDS = 96;
const ALIGNMENT_WINDOW_WORDS_MULTIPLIER = 4;
const ALIGNMENT_WINDOW_FLOOR_WORDS = 240;
const CHAPTER_ANCHOR_INTERVAL_WORDS = 480;
const CHAPTER_ANCHOR_PROBE_WORDS = 8;
const CHAPTER_ANCHOR_SEARCH_RADIUS_WORDS = 72;
const MAX_DRIFT_MS = 150_000;
const GLOSSARY_LIMIT = 48;
const ORDINARY_WORDS_PATH = "/usr/share/dict/words";
const MAX_FRONT_MATTER_SKIP = 8;
const MAX_SECTION_DIVIDER_WORDS = 120;

const STOPWORDS = new Set([
  "a",
  "about",
  "after",
  "all",
  "also",
  "an",
  "and",
  "any",
  "are",
  "as",
  "at",
  "be",
  "been",
  "before",
  "being",
  "but",
  "by",
  "can",
  "could",
  "did",
  "do",
  "does",
  "done",
  "down",
  "each",
  "even",
  "for",
  "from",
  "get",
  "got",
  "had",
  "has",
  "have",
  "he",
  "her",
  "here",
  "him",
  "his",
  "how",
  "i",
  "if",
  "in",
  "into",
  "is",
  "it",
  "its",
  "just",
  "know",
  "like",
  "many",
  "may",
  "me",
  "more",
  "most",
  "my",
  "new",
  "no",
  "not",
  "now",
  "of",
  "on",
  "one",
  "only",
  "or",
  "other",
  "our",
  "out",
  "over",
  "really",
  "said",
  "same",
  "see",
  "she",
  "should",
  "so",
  "some",
  "such",
  "than",
  "that",
  "the",
  "their",
  "them",
  "then",
  "there",
  "these",
  "they",
  "this",
  "those",
  "through",
  "to",
  "too",
  "up",
  "use",
  "very",
  "was",
  "we",
  "well",
  "were",
  "what",
  "when",
  "where",
  "which",
  "who",
  "why",
  "will",
  "with",
  "would",
  "you",
  "your",
]);

const GLOSSARY_ARTIFACT_WORDS = new Set([
  "acknowledgements",
  "author",
  "chapter",
  "copyright",
  "doesn",
  "epigraph",
  "page",
  "title",
  "xhtml",
]);

const ISO_639_2_TO_1 = new Map<string, string>([
  ["deu", "de"],
  ["eng", "en"],
  ["fra", "fr"],
  ["fre", "fr"],
  ["ger", "de"],
  ["ita", "it"],
  ["jpn", "ja"],
  ["por", "pt"],
  ["spa", "es"],
  ["zho", "zh"],
  ["chi", "zh"],
]);

type StoredChapterTiming = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
  startOffset?: number;
  endOffset?: number;
};

type StoredChapterPayload = {
  version: string;
  chapters: StoredChapterTiming[];
};

type StoredTranscriptWord = {
  startMs: number;
  endMs: number;
  text: string;
  token: string;
};

type StoredTranscriptSegment = {
  id: string;
  title: string;
  text: string;
  startMs: number;
  endMs: number;
  wordStartIndex: number;
  wordEndIndex: number;
  tokenCount: number;
  matchedWordCount: number;
  anchorTokenCount: number;
  matchedAnchorTokenCount: number;
  anchorCoverage: number;
};

type StoredTranscriptPayload = {
  version: string;
  text: string;
  words: StoredTranscriptWord[];
  segments?: StoredTranscriptSegment[];
  rawText?: string;
  rawWords?: StoredTranscriptWord[];
};

type EpubWord = {
  text: string;
  token: string;
};

type EpubChapterEntry = {
  id: string;
  title: string;
  href: string;
  text: string;
  words: EpubWord[];
  tokens: string[];
  wordCount: number;
  cumulativeWords: number;
  cumulativeRatio: number;
};

type ChapterBoundaryProbe = {
  boundaryIndex: number;
  estimateMs: number;
  estimateCandidatesMs: number[];
  previousTitle: string;
  nextTitle: string;
  previousProbes: string[][];
  nextProbes: string[][];
};

type BoundaryMatch = {
  boundaryIndex: number;
  resolvedMs: number | null;
  estimateMs: number;
  reason: string;
  previousMatchMs?: number;
  nextMatchMs?: number;
};

type ProbeAlignmentResult = {
  resolvedMs: number;
  startTranscriptIndex: number;
  endTranscriptIndex: number;
  score: number;
  matchedTokenCount: number;
  coverage: number;
  mode: "start" | "end";
  matchedPairs: Array<{ probeIndex: number; transcriptIndex: number }>;
};

type ChapterSegmentMatch = {
  chapterIndex: number;
  startMs: number | null;
  endMs: number | null;
  matchedWordCount: number;
  anchorTokenCount: number;
  matchedAnchorTokenCount: number;
  anchorCoverage: number;
  words: StoredTranscriptWord[];
};

type ChapterAnchor = {
  epubStart: number;
  epubEnd: number;
  transcriptStart: number;
  transcriptEnd: number;
  matchedPairs: Array<{ epubIndex: number; transcriptIndex: number }>;
  attemptedTokenCount: number;
};

type TranscriptWord = {
  startMs: number;
  endMs: number;
  token: string;
  raw: string;
};

type TranscriptChunkPlan = {
  index: number;
  startMs: number;
  durationMs: number;
  trimStartMs: number;
  trimEndMs: number;
};

type GlossaryTermStats = {
  display: string;
  score: number;
  freq: number;
  titleFreq: number;
  bodyFreq: number;
  capitalizedFreq: number;
  lowercaseFreq: number;
  chapterHits: Set<number>;
};

let ordinaryWordsCache: Set<string> | null | undefined;

type TranscriptChunk = TranscriptChunkPlan & {
  words: TranscriptWord[];
};

export type AnalysisResult = {
  transcriptWords: TranscriptWord[];
  chapters: StoredChapterTiming[];
  transcriptSegments: ChapterSegmentMatch[];
  resolvedBoundaryCount: number;
  totalBoundaryCount: number;
  debug: Record<string, unknown>;
};

type ChapterAnalysisContext = {
  repo: BooksRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
};

type ChapterAnalysisDeps = {
  loadEpubEntries: typeof loadEpubEntries;
  extractChunkClip: typeof extractChunkClip;
  transcribeChunk: typeof transcribeChunk;
};

type ExtractChunkArgs = {
  asset: AssetRow;
  files: AssetFileRow[];
  startMs: number;
  durationMs: number;
  tempDir: string;
  clipName: string;
};

function log(ctx: ChapterAnalysisContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

export function selectPreferredEpubAsset(assets: AssetRow[]): AssetRow | null {
  const ebooks = assets.filter((asset) => asset.kind === "ebook" && asset.mime === "application/epub+zip");
  if (ebooks.length === 0) return null;
  return [...ebooks].sort((a, b) => {
    if (a.created_at !== b.created_at) return b.created_at.localeCompare(a.created_at);
    return b.id - a.id;
  })[0] ?? null;
}

function normalizeToken(value: string): string {
  return value.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "");
}

export function normalizeTranscriptionLanguage(value: string | null | undefined): string | null {
  const trimmed = value?.trim().toLowerCase() ?? "";
  if (!trimmed) return null;
  if (/^[a-z]{2}$/u.test(trimmed)) return trimmed;
  const localeMatch = trimmed.match(/^([a-z]{2})[-_][a-z0-9]+$/u);
  if (localeMatch?.[1]) return localeMatch[1];
  if (/^[a-z]{3}$/u.test(trimmed)) return ISO_639_2_TO_1.get(trimmed) ?? null;
  return null;
}

function tokenize(value: string): string[] {
  return value
    .split(/\s+/)
    .map((token) => normalizeToken(token))
    .filter(Boolean);
}

function tokenizeChapterWords(value: string): EpubWord[] {
  const out: EpubWord[] = [];
  const matches = value.matchAll(/\b[\p{L}][\p{L}\p{N}'-]*\b/gu);
  for (const match of matches) {
    const text = match[0] ?? "";
    const token = normalizeToken(text);
    if (!token) continue;
    out.push({ text, token });
  }
  return out;
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#(\d+);/g, (_, num) => String.fromCharCode(Number(num)))
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCharCode(Number.parseInt(hex, 16)));
}

function stripHtmlToText(html: string): string {
  return decodeHtmlEntities(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
  ).trim();
}

function flattenToc(nodes: Array<{ label?: string; href?: string; children?: unknown[] }>, out: Array<{ label: string; href: string }>): void {
  for (const node of nodes) {
    if (typeof node.href === "string" && node.href.trim()) {
      out.push({
        label: typeof node.label === "string" && node.label.trim() ? node.label.trim() : "",
        href: node.href,
      });
    }
    if (Array.isArray(node.children) && node.children.length > 0) {
      flattenToc(node.children as Array<{ label?: string; href?: string; children?: unknown[] }>, out);
    }
  }
}

export async function loadEpubEntries(epubPath: string): Promise<EpubChapterEntry[]> {
  const resourceDir = await mkdtemp(path.join(os.tmpdir(), "podible-epub-"));
  const epub = await initEpubFile(epubPath, resourceDir);

  try {
    const spine = epub
      .getSpine()
      .filter((item) => item && item.id && item.href && item.mediaType?.includes("html") && item.linear !== "no");
    const spineIds = spine.map((item) => item.id);
    const titleById = new Map<string, string>();
    const hrefById = new Map<string, string>();
    spine.forEach((item) => {
      hrefById.set(item.id, item.href);
    });

    const flatToc: Array<{ label: string; href: string }> = [];
    flattenToc(epub.getToc() as Array<{ label?: string; href?: string; children?: unknown[] }>, flatToc);

    const tocResolved: string[] = [];
    const seenTocIds = new Set<string>();
    for (const entry of flatToc) {
      const resolved = epub.resolveHref(entry.href);
      if (!resolved?.id || !spineIds.includes(resolved.id) || seenTocIds.has(resolved.id)) continue;
      tocResolved.push(resolved.id);
      seenTocIds.add(resolved.id);
      if (entry.label) titleById.set(resolved.id, entry.label);
    }

    const orderedIds =
      tocResolved.length >= Math.max(1, Math.ceil(spineIds.length / 2))
        ? [...tocResolved, ...spineIds.filter((id) => !seenTocIds.has(id))]
        : spineIds;

    const entries: EpubChapterEntry[] = [];
    let cumulativeWords = 0;

    for (const [index, id] of orderedIds.entries()) {
      const chapter = await epub.loadChapter(id).catch(() => null);
      if (!chapter || typeof chapter.html !== "string") continue;
      const text = stripHtmlToText(chapter.html);
      const words = tokenizeChapterWords(text);
      const tokens = words.map((word) => word.token);
      if (tokens.length === 0) continue;
      cumulativeWords += tokens.length;
      const title = titleById.get(id) || `Chapter ${index + 1}`;
      entries.push({
        id,
        title,
        href: hrefById.get(id) ?? id,
        text,
        words,
        tokens,
        wordCount: tokens.length,
        cumulativeWords,
        cumulativeRatio: 0,
      });
    }

    const totalWords = cumulativeWords;
    if (entries.length < 2 || totalWords < PROBE_WORDS * 2) {
      return [];
    }

    return entries.map((entry) => ({
      ...entry,
      cumulativeRatio: totalWords > 0 ? entry.cumulativeWords / totalWords : 0,
    }));
  } finally {
    try {
      epub.destroy();
    } catch {
      // ignore cleanup failures
    }
    await rm(resourceDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

export async function computeEpubWordCount(epubPath: string): Promise<number | null> {
  const entries = await loadEpubEntries(epubPath);
  if (entries.length === 0) return null;
  return entries.reduce((sum, entry) => sum + entry.wordCount, 0);
}

function chapterStartRatios(entries: EpubChapterEntry[]): number[] {
  const totalWords = entries[entries.length - 1]?.cumulativeWords ?? 0;
  const ratios = [0];
  let runningWords = 0;
  for (let index = 0; index < entries.length - 1; index += 1) {
    runningWords += entries[index]!.wordCount;
    ratios.push(totalWords > 0 ? runningWords / totalWords : 0);
  }
  return ratios;
}

function boundaryEstimateRatios(entries: EpubChapterEntry[]): number[] {
  const weightedCounts = entries.map((entry) => {
    if (isFrontMatterTitle(entry.title)) return 0;
    if (isSectionDividerEntry(entry)) return Math.min(entry.wordCount, 16);
    return entry.wordCount;
  });
  const totalWords = weightedCounts.reduce((sum, count) => sum + count, 0);
  const ratios = [0];
  let runningWords = 0;
  for (let index = 0; index < entries.length - 1; index += 1) {
    runningWords += weightedCounts[index] ?? 0;
    ratios.push(totalWords > 0 ? runningWords / totalWords : 0);
  }
  return ratios;
}

function boundaryProbes(entries: EpubChapterEntry[], durationMs: number): ChapterBoundaryProbe[] {
  const weightedRatios = boundaryEstimateRatios(entries);
  const rawRatios = chapterStartRatios(entries);
  const out: ChapterBoundaryProbe[] = [];
  for (let index = 0; index < entries.length - 1; index += 1) {
    const previous = entries[index]!;
    const next = entries[index + 1]!;
    const weightedEstimateMs = Math.round(durationMs * (weightedRatios[index + 1] ?? 0));
    const rawEstimateMs = Math.round(durationMs * (rawRatios[index + 1] ?? 0));
    out.push({
      boundaryIndex: index,
      estimateMs: weightedEstimateMs,
      estimateCandidatesMs: [...new Set([weightedEstimateMs, rawEstimateMs])],
      previousTitle: previous.title,
      nextTitle: next.title,
      previousProbes: boundaryProbeCandidates(previous, "end"),
      nextProbes: boundaryProbeCandidates(next, "start"),
    });
  }
  return out;
}

function uniqueProbeCandidates(candidates: string[][]): string[][] {
  const seen = new Set<string>();
  const out: string[][] = [];
  for (const candidate of candidates) {
    if (candidate.length < PROBE_WORDS) continue;
    const key = candidate.join("\u0001");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(candidate);
  }
  return out;
}

function boundaryProbeCandidates(entry: EpubChapterEntry, side: "start" | "end"): string[][] {
  if (entry.tokens.length === 0) return [];

  const candidates: string[][] = [];
  if (side === "start") {
    for (const offset of [0, 24, 48, 72, 96, 144, 192, 240]) {
      if (offset >= entry.tokens.length) break;
      candidates.push(entry.tokens.slice(offset, offset + ALIGNMENT_PROBE_WORDS));
    }
  } else {
    for (const backoff of [0, 24, 48, 72, 96, 144, 192, 240]) {
      const end = entry.tokens.length - backoff;
      if (end <= 0) break;
      const start = Math.max(0, end - ALIGNMENT_PROBE_WORDS);
      candidates.push(entry.tokens.slice(start, end));
    }
  }

  return uniqueProbeCandidates(candidates);
}

function contiguousMatchLength(haystack: string[], needle: string[], haystackStart: number, needleStart: number): number {
  let matched = 0;
  let h = haystackStart;
  let n = needleStart;
  while (h < haystack.length && n < needle.length && haystack[h] === needle[n]) {
    matched += 1;
    h += 1;
    n += 1;
  }
  return matched;
}

function overlapRatio(haystackSlice: string[], needle: string[]): number {
  if (needle.length === 0) return 0;
  const haystackSet = new Set(haystackSlice);
  let overlap = 0;
  for (const token of needle) {
    if (haystackSet.has(token)) overlap += 1;
  }
  return overlap / needle.length;
}

function findProbeTimestamp(words: TranscriptWord[], probe: string[], mode: "start" | "end", estimateMs: number): number | null {
  if (probe.length === 0 || words.length === 0) return null;
  const transcriptTokens = words.map((word) => word.token);
  const probeShingles: Array<{ start: number; tokens: string[] }> = [];
  if (probe.length >= MIN_SHINGLE) {
    for (let index = 0; index <= probe.length - MIN_SHINGLE; index += 1) {
      probeShingles.push({ start: index, tokens: probe.slice(index, index + MIN_SHINGLE) });
    }
  } else {
    probeShingles.push({ start: 0, tokens: probe });
  }

  let best: { score: number; overlap: number; timeMs: number } | null = null;
  for (const shingle of probeShingles) {
    for (let index = 0; index <= transcriptTokens.length - shingle.tokens.length; index += 1) {
      const matched = shingle.tokens.every((token, offset) => transcriptTokens[index + offset] === token);
      if (!matched) continue;
      const haystackStart = Math.max(0, index - shingle.start);
      const needleStart = shingle.start > index ? shingle.start - index : 0;
      const contiguous = contiguousMatchLength(transcriptTokens, probe, haystackStart, needleStart);
      const slice = transcriptTokens.slice(haystackStart, haystackStart + probe.length);
      const overlap = overlapRatio(slice, probe);
      const boundaryWordIndex = mode === "start" ? haystackStart : Math.min(words.length - 1, haystackStart + probe.length - 1);
      const timeMs = mode === "start" ? words[boundaryWordIndex]!.startMs : words[boundaryWordIndex]!.endMs;
      if (Math.abs(timeMs - estimateMs) > MAX_DRIFT_MS) continue;
      if (contiguous < MIN_SHINGLE && overlap < 0.75) continue;
      const score = contiguous * 100 - Math.abs(timeMs - estimateMs);
      if (!best || score > best.score || (score === best.score && overlap > best.overlap)) {
        best = { score, overlap, timeMs };
      }
    }
  }
  return best?.timeMs ?? null;
}

function transcriptWordIndexForTime(words: TranscriptWord[], timeMs: number): number {
  if (words.length === 0) return 0;
  let low = 0;
  let high = words.length - 1;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if ((words[mid]?.endMs ?? 0) < timeMs) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return low;
}

function levenshteinDistance(left: string, right: string, maxDistance = 3): number {
  if (left === right) return 0;
  if (!left.length) return right.length;
  if (!right.length) return left.length;
  if (Math.abs(left.length - right.length) > maxDistance) return maxDistance + 1;

  const previous = new Array<number>(right.length + 1).fill(0);
  const current = new Array<number>(right.length + 1).fill(0);
  for (let index = 0; index <= right.length; index += 1) previous[index] = index;

  for (let i = 1; i <= left.length; i += 1) {
    current[0] = i;
    let rowMin = current[0];
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      current[j] = Math.min(previous[j]! + 1, current[j - 1]! + 1, previous[j - 1]! + cost);
      rowMin = Math.min(rowMin, current[j]!);
    }
    if (rowMin > maxDistance) return maxDistance + 1;
    for (let j = 0; j <= right.length; j += 1) previous[j] = current[j]!;
  }
  return previous[right.length]!;
}

function tokenAlignmentScore(left: string, right: string): number {
  if (!left || !right) return -2;
  if (left === right) return 3;
  const minLength = Math.min(left.length, right.length);
  if (minLength >= 5 && (left.startsWith(right) || right.startsWith(left))) return 1.5;
  if (minLength >= 5 && left.slice(0, 3) === right.slice(0, 3) && left.slice(-2) === right.slice(-2)) return 1.5;
  const distance = levenshteinDistance(left, right, 2);
  const maxLength = Math.max(left.length, right.length);
  if (distance <= 2 && maxLength > 0 && 1 - distance / maxLength >= 0.7) return 1.5;
  return -2;
}

function alignProbeToTranscriptWindow(
  transcriptWords: TranscriptWord[],
  probeTokens: string[],
  estimateMs: number,
  mode: "start" | "end"
): ProbeAlignmentResult | null {
  if (probeTokens.length === 0 || transcriptWords.length === 0) return null;

  const estimateIndex = transcriptWordIndexForTime(transcriptWords, estimateMs);
  const halfWindow = Math.max(ALIGNMENT_WINDOW_FLOOR_WORDS, probeTokens.length * ALIGNMENT_WINDOW_WORDS_MULTIPLIER);
  const windowStart = Math.max(0, estimateIndex - halfWindow);
  const windowEnd = Math.min(transcriptWords.length, estimateIndex + halfWindow);
  const windowWords = transcriptWords.slice(windowStart, windowEnd);
  if (windowWords.length === 0) return null;

  const gapPenalty = -1;
  const rows = probeTokens.length;
  const cols = windowWords.length;
  const scores = Array.from({ length: rows + 1 }, () => new Float64Array(cols + 1));
  const trace = Array.from({ length: rows + 1 }, () => new Uint8Array(cols + 1));

  for (let j = 0; j <= cols; j += 1) scores[0]![j] = 0;
  for (let i = 1; i <= rows; i += 1) {
    scores[i]![0] = i * gapPenalty;
    trace[i]![0] = 2;
  }

  let bestEndCol = 0;
  let bestEndScore = Number.NEGATIVE_INFINITY;
  for (let i = 1; i <= rows; i += 1) {
    for (let j = 1; j <= cols; j += 1) {
      const diagScore = scores[i - 1]![j - 1]! + tokenAlignmentScore(probeTokens[i - 1]!, windowWords[j - 1]!.token);
      const upScore = scores[i - 1]![j]! + gapPenalty;
      const leftScore = scores[i]![j - 1]! + gapPenalty;
      if (diagScore >= upScore && diagScore >= leftScore) {
        scores[i]![j] = diagScore;
        trace[i]![j] = 1;
      } else if (leftScore >= upScore) {
        scores[i]![j] = leftScore;
        trace[i]![j] = 3;
      } else {
        scores[i]![j] = upScore;
        trace[i]![j] = 2;
      }
      if (i === rows && scores[i]![j]! > bestEndScore) {
        bestEndScore = scores[i]![j]!;
        bestEndCol = j;
      }
    }
  }

  let i = rows;
  let j = bestEndCol;
  const matchedIndices: number[] = [];
  const matchedPairs: Array<{ probeIndex: number; transcriptIndex: number }> = [];
  while (i > 0 && j >= 0) {
    const direction = trace[i]![j] ?? 0;
    if (direction === 1) {
      const transcriptIndex = windowStart + j - 1;
      if (tokenAlignmentScore(probeTokens[i - 1]!, windowWords[j - 1]!.token) > 0) {
        matchedIndices.push(transcriptIndex);
        matchedPairs.push({ probeIndex: i - 1, transcriptIndex });
      }
      i -= 1;
      j -= 1;
      continue;
    }
    if (direction === 2) {
      i -= 1;
      continue;
    }
    if (direction === 3) {
      j -= 1;
      continue;
    }
    break;
  }

  matchedIndices.reverse();
  matchedPairs.reverse();
  if (matchedIndices.length === 0) return null;
  const coverage = matchedIndices.length / probeTokens.length;
  if (coverage < 0.25 || matchedIndices.length < Math.min(12, probeTokens.length)) return null;
  const startTranscriptIndex = matchedIndices[0]!;
  const endTranscriptIndex = matchedIndices[matchedIndices.length - 1]!;
  const resolvedMs =
    mode === "start" ? transcriptWords[startTranscriptIndex]!.startMs : transcriptWords[endTranscriptIndex]!.endMs;
  if (Math.abs(resolvedMs - estimateMs) > MAX_DRIFT_MS) return null;
  return {
    resolvedMs,
    startTranscriptIndex,
    endTranscriptIndex,
    score: Math.round(bestEndScore * 100) / 100,
    matchedTokenCount: matchedIndices.length,
    coverage: Math.round(coverage * 1000) / 1000,
    mode,
    matchedPairs,
  };
}

function alignBestProbeToTranscriptWindow(
  transcriptWords: TranscriptWord[],
  probeCandidates: string[][],
  estimateCandidatesMs: number[],
  mode: "start" | "end"
): ProbeAlignmentResult | null {
  let best: ProbeAlignmentResult | null = null;
  for (const estimateMs of estimateCandidatesMs) {
    for (const candidate of probeCandidates) {
      const result = alignProbeToTranscriptWindow(transcriptWords, candidate, estimateMs, mode);
      if (!result) continue;
      if (
        !best ||
        result.coverage > best.coverage ||
        (result.coverage === best.coverage && result.matchedTokenCount > best.matchedTokenCount) ||
        (result.coverage === best.coverage &&
          result.matchedTokenCount === best.matchedTokenCount &&
          Math.abs(result.resolvedMs - estimateMs) < Math.abs(best.resolvedMs - estimateMs))
      ) {
        best = result;
      }
    }
  }
  return best;
}

export function interpolateChapterStarts(
  entries: EpubChapterEntry[],
  resolvedBoundaries: Array<number | null>,
  durationMs: number
): number[] | null {
  const chapterCount = entries.length;
  const startTimes = new Array<number>(chapterCount).fill(0);
  const ratios = chapterStartRatios(entries);
  const anchors = [{ chapterIndex: 0, timeMs: 0 }];
  resolvedBoundaries.forEach((timeMs, index) => {
    if (typeof timeMs === "number" && Number.isFinite(timeMs)) {
      anchors.push({ chapterIndex: index + 1, timeMs });
    }
  });
  anchors.push({ chapterIndex: chapterCount, timeMs: durationMs });
  anchors.sort((a, b) => a.chapterIndex - b.chapterIndex || a.timeMs - b.timeMs);

  for (let anchorIndex = 0; anchorIndex < anchors.length - 1; anchorIndex += 1) {
    const left = anchors[anchorIndex]!;
    const right = anchors[anchorIndex + 1]!;
    if (right.chapterIndex <= left.chapterIndex) continue;
    if (right.timeMs < left.timeMs) return null;
    startTimes[left.chapterIndex] = left.timeMs;
    const leftRatio = ratios[left.chapterIndex] ?? 0;
    const rightRatio = right.chapterIndex >= chapterCount ? 1 : (ratios[right.chapterIndex] ?? 1);
    const ratioSpan = rightRatio - leftRatio;
    for (let chapterIndex = left.chapterIndex + 1; chapterIndex < Math.min(right.chapterIndex, chapterCount); chapterIndex += 1) {
      const currentRatio = ratios[chapterIndex] ?? leftRatio;
      const fraction =
        ratioSpan > 0
          ? (currentRatio - leftRatio) / ratioSpan
          : (chapterIndex - left.chapterIndex) / (right.chapterIndex - left.chapterIndex);
      startTimes[chapterIndex] = Math.round(left.timeMs + (right.timeMs - left.timeMs) * fraction);
    }
  }

  for (let index = 1; index < startTimes.length; index += 1) {
    if (startTimes[index]! < startTimes[index - 1]!) return null;
  }
  return startTimes;
}

export function timingsFromChapterStarts(entries: EpubChapterEntry[], startTimes: number[], durationMs: number): StoredChapterTiming[] | null {
  if (entries.length !== startTimes.length) return null;
  const chapters: StoredChapterTiming[] = [];
  for (let index = 0; index < entries.length; index += 1) {
    const startMs = Math.max(0, Math.round(startTimes[index] ?? 0));
    const nextStart = index < entries.length - 1 ? startTimes[index + 1] ?? durationMs : durationMs;
    const endMs = Math.max(startMs, Math.round(nextStart));
    chapters.push({
      id: `ch${index}`,
      title: entries[index]!.title,
      startMs,
      endMs,
    });
  }
  if (chapters.some((chapter, index) => index > 0 && chapter.startMs < chapters[index - 1]!.startMs)) {
    return null;
  }
  return chapters;
}

async function computeTranscriptFingerprint(asset: AssetRow, files: AssetFileRow[]): Promise<string> {
  const fileStats = await Promise.all(
    files.map(async (file) => {
      const stat = await Bun.file(file.path).stat();
      return {
        path: file.path,
        size: file.size,
        mtimeMs: stat.mtimeMs,
      };
    })
  );
  const hash = createHash("sha256");
  hash.update(
    JSON.stringify({
      version: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      kind: "transcript",
      assetId: asset.id,
      files: fileStats,
    })
  );
  return hash.digest("hex");
}

async function computeChapterFingerprint(asset: AssetRow, files: AssetFileRow[], epubPath: string): Promise<string> {
  const fileStats = await Promise.all(
    files.map(async (file) => {
      const stat = await Bun.file(file.path).stat();
      return {
        path: file.path,
        size: file.size,
        mtimeMs: stat.mtimeMs,
      };
    })
  );
  const epubStat = await Bun.file(epubPath).stat();
  const hash = createHash("sha256");
  hash.update(
    JSON.stringify({
      version: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      kind: "chapters",
      assetId: asset.id,
      files: fileStats,
      epub: {
        path: epubPath,
        mtimeMs: epubStat.mtimeMs,
      },
    })
  );
  return hash.digest("hex");
}

function fileOffsetForTime(files: AssetFileRow[], timeMs: number): number | undefined {
  let cursor = 0;
  for (const file of files) {
    const fileStart = cursor;
    const fileEnd = fileStart + file.duration_ms;
    if (timeMs <= fileEnd || file === files[files.length - 1]) {
      if (file.duration_ms <= 0 || file.size <= 0) return file.start;
      const fraction = Math.max(0, Math.min(1, (timeMs - fileStart) / file.duration_ms));
      return Math.max(file.start, Math.min(file.end, file.start + Math.floor(file.size * fraction)));
    }
    cursor = fileEnd;
  }
  return undefined;
}

function applyOffsets(files: AssetFileRow[], chapters: StoredChapterTiming[]): StoredChapterTiming[] {
  if (files.length === 0) return chapters;
  return chapters.map((chapter, index) => ({
    ...chapter,
    startOffset: fileOffsetForTime(files, chapter.startMs),
    endOffset:
      index < chapters.length - 1
        ? fileOffsetForTime(files, Math.max(chapter.startMs, chapters[index + 1]!.startMs - 1))
        : files[files.length - 1]!.end,
  }));
}

export async function loadStoredChapterTimings(
  repo: BooksRepo,
  asset: AssetRow,
  files: AssetFileRow[]
): Promise<StoredChapterTiming[] | null> {
  const row = repo.getChapterAnalysis(asset.id);
  if (!row || row.status !== "succeeded" || !row.chapters_json) return null;
  try {
    const parsed = JSON.parse(row.chapters_json) as StoredChapterPayload;
    if (!parsed || !Array.isArray(parsed.chapters) || parsed.chapters.length === 0) return null;
    const timings = parsed.chapters
      .map((chapter, index) => ({
        id: typeof chapter.id === "string" ? chapter.id : `ch${index}`,
        title: typeof chapter.title === "string" && chapter.title.trim() ? chapter.title : `Chapter ${index + 1}`,
        startMs: Math.max(0, Math.round(Number(chapter.startMs) || 0)),
        endMs: Math.max(0, Math.round(Number(chapter.endMs) || 0)),
      }))
      .sort((a, b) => a.startMs - b.startMs);
    return asset.kind === "multi" ? applyOffsets(files, timings) : timings;
  } catch {
    return null;
  }
}

function storedTranscriptPayload(
  rawWords: TranscriptWord[],
  entries?: EpubChapterEntry[],
  chapters?: StoredChapterTiming[],
  segmentMatches?: ChapterSegmentMatch[]
): StoredTranscriptPayload {
  const rawPayloadWords = rawWords.map((word) => ({
    startMs: word.startMs,
    endMs: word.endMs,
    text: word.raw,
    token: word.token,
  }));
  const payload: StoredTranscriptPayload = {
    version: CHAPTERS_API_VERSION,
    text: rawWords.map((word) => word.raw).filter(Boolean).join(" ").trim(),
    words: rawPayloadWords,
    rawText: rawWords.map((word) => word.raw).filter(Boolean).join(" ").trim(),
    rawWords: rawPayloadWords,
  };
  if (!entries || !chapters || entries.length !== chapters.length || !segmentMatches || segmentMatches.length !== chapters.length) {
    return payload;
  }

  const canonicalWords: StoredTranscriptWord[] = [];
  const segments = chapters.map((chapter, index) => {
    const match = segmentMatches[index]!;
    const wordStartIndex = canonicalWords.length;
    canonicalWords.push(...match.words);
    const wordEndIndex = canonicalWords.length - 1;
    return {
      id: chapter.id,
      title: chapter.title,
      text: entries[index]!.text,
      startMs: match.startMs ?? chapter.startMs,
      endMs: match.endMs ?? chapter.endMs,
      wordStartIndex,
      wordEndIndex,
      tokenCount: entries[index]!.tokens.length,
      matchedWordCount: match.matchedWordCount,
      anchorTokenCount: match.anchorTokenCount,
      matchedAnchorTokenCount: match.matchedAnchorTokenCount,
      anchorCoverage: match.anchorCoverage,
    };
  });

  return {
    version: CHAPTERS_API_VERSION,
    text: segments.map((segment) => segment.text).join("\n\n"),
    words: canonicalWords,
    segments,
    rawText: payload.rawText,
    rawWords: payload.rawWords,
  };
}

function publicStoredTranscriptPayload(payload: StoredTranscriptPayload): StoredTranscriptPayload {
  return {
    version: payload.version,
    text: payload.text,
    words: payload.words,
    ...(Array.isArray(payload.segments) ? { segments: payload.segments } : {}),
  };
}

export async function readStoredTranscriptPayload(row: AssetTranscriptRow | null): Promise<StoredTranscriptPayload | null> {
  if (!row || row.status !== "succeeded" || !row.transcript_json) return null;
  try {
    const parsed = JSON.parse(row.transcript_json) as StoredTranscriptPayload;
    if (!parsed || !Array.isArray(parsed.words)) return null;
    return parsed;
  } catch {
    return null;
  }
}

export async function loadStoredTranscriptPayload(repo: BooksRepo, assetId: number): Promise<StoredTranscriptPayload | null> {
  const parsed = await readStoredTranscriptPayload(repo.getAssetTranscript(assetId));
  return parsed ? publicStoredTranscriptPayload(parsed) : null;
}

function loadOrdinaryWords(): Set<string> | null {
  if (ordinaryWordsCache !== undefined) return ordinaryWordsCache;
  try {
    const dictionary = readFileSync(ORDINARY_WORDS_PATH, "utf8");
    ordinaryWordsCache = new Set(
      dictionary
        .split(/\r?\n/u)
        .map((word) => normalizeToken(word))
        .filter((word) => word.length >= 4)
    );
  } catch {
    ordinaryWordsCache = null;
  }
  return ordinaryWordsCache;
}

function canonicalizeGlossaryTerm(term: string): string {
  const trimmed = term.trim();
  if (trimmed.length < 3) return trimmed;
  return trimmed.replace(/['’](?:s|S)\b$/u, "").replace(/['’]\b$/u, "");
}

function hasDistinctiveGlossarySurface(term: string): boolean {
  return /[a-z][A-Z]/.test(term) || /[-']/u.test(term) || (/^[A-Z]{2,}$/u.test(term) && term.length <= 5);
}

function normalizeTitleForMatching(value: string): string {
  return value.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}

function isFrontMatterTitle(title: string): boolean {
  const normalized = normalizeTitleForMatching(title);
  return /^(copyright( page)?|contents|table of contents|dedication|acknowledg(e)?ments|author s note|authors note|note from the author|preface|foreword|introduction|title page|half title|about the author|praise|epigraph|cover)$/u.test(
    normalized
  );
}

function isSectionDividerEntry(entry: EpubChapterEntry): boolean {
  if (entry.wordCount > MAX_SECTION_DIVIDER_WORDS) return false;
  const normalized = normalizeTitleForMatching(entry.title);
  return /^(part|book|section|act|volume|interlude|appendix)\b/u.test(normalized);
}

function glossaryEntries(entries: EpubChapterEntry[]): EpubChapterEntry[] {
  let startIndex = 0;
  while (startIndex < entries.length && startIndex < MAX_FRONT_MATTER_SKIP && isFrontMatterTitle(entries[startIndex]!.title)) {
    startIndex += 1;
  }
  return entries.slice(startIndex).filter((entry) => !isSectionDividerEntry(entry));
}

function isOrdinaryDictionaryWord(term: string, ordinaryWords: Set<string> | null): boolean {
  if (!ordinaryWords) return false;
  if (ordinaryWords.has(term)) return true;
  if (term.endsWith("'s") && ordinaryWords.has(term.slice(0, -2))) return true;
  if (term.endsWith("ies") && term.length > 4 && ordinaryWords.has(`${term.slice(0, -3)}y`)) return true;
  if (term.endsWith("es") && term.length > 4) {
    const stem = term.slice(0, -2);
    if (ordinaryWords.has(stem) || ordinaryWords.has(stem.slice(0, -1))) return true;
  }
  if (term.endsWith("s") && term.length > 3 && ordinaryWords.has(term.slice(0, -1))) return true;
  if (term.endsWith("ing") && term.length > 5) {
    const stem = term.slice(0, -3);
    if (ordinaryWords.has(stem) || ordinaryWords.has(`${stem}e`) || ordinaryWords.has(stem.slice(0, -1))) return true;
  }
  if (term.endsWith("ed") && term.length > 4) {
    const stem = term.slice(0, -2);
    if (ordinaryWords.has(stem) || ordinaryWords.has(`${stem}e`) || ordinaryWords.has(stem.slice(0, -1))) return true;
  }
  return false;
}

function collectGlossaryTerm(
  stats: Map<string, GlossaryTermStats>,
  term: string,
  chapterIndex: number,
  source: "title" | "body"
): void {
  const canonical = canonicalizeGlossaryTerm(term);
  const normalized = normalizeToken(canonical);
  if (!normalized || normalized.length < 4 || STOPWORDS.has(normalized) || /^\d+$/.test(normalized)) return;
  const existing = stats.get(normalized) ?? {
    display: canonical,
    score: 0,
    freq: 0,
    titleFreq: 0,
    bodyFreq: 0,
    capitalizedFreq: 0,
    lowercaseFreq: 0,
    chapterHits: new Set<number>(),
  };
  const isCapitalized = /^[A-Z]/.test(canonical);
  existing.freq += 1;
  existing.chapterHits.add(chapterIndex);
  if (source === "title") {
    existing.titleFreq += 1;
    existing.score += 3;
  } else {
    existing.bodyFreq += 1;
    existing.score += 1;
  }
  if (isCapitalized) {
    existing.capitalizedFreq += 1;
    existing.score += 2;
  } else {
    existing.lowercaseFreq += 1;
  }
  existing.score += canonical.length / 12;
  if (
    canonical.length > existing.display.length ||
    (/[A-Z]/.test(canonical) && !/[A-Z]/.test(existing.display))
  ) {
    existing.display = canonical;
  }
  stats.set(normalized, existing);
}

export function extractGlossaryTerms(entries: EpubChapterEntry[], ordinaryWords = loadOrdinaryWords()): string[] {
  const stats = new Map<string, GlossaryTermStats>();
  const termPattern = /\b[\p{L}][\p{L}\p{N}'-]{2,}\b/gu;

  for (const [chapterIndex, entry] of glossaryEntries(entries).entries()) {
    const titleTerms = entry.title.match(termPattern) ?? [];
    for (const term of titleTerms) {
      collectGlossaryTerm(stats, term, chapterIndex, "title");
    }
    const textTerms = entry.text.match(termPattern) ?? [];
    for (const term of textTerms) {
      collectGlossaryTerm(stats, term, chapterIndex, "body");
    }
  }

  return Array.from(stats.values())
    .filter((entry) => {
      const chapterFreq = entry.chapterHits.size;
      const capitalizedRatio = entry.freq > 0 ? entry.capitalizedFreq / entry.freq : 0;
      const normalized = normalizeToken(entry.display);
      const isOrdinaryWord = isOrdinaryDictionaryWord(normalized, ordinaryWords);
      if (/^[A-Z]{4,}$/u.test(entry.display)) {
        return false;
      }
      if (GLOSSARY_ARTIFACT_WORDS.has(normalized)) {
        return false;
      }
      if (entry.bodyFreq === 0) {
        return !isOrdinaryWord && entry.capitalizedFreq >= 3 && entry.titleFreq === 0;
      }
      if (isOrdinaryWord) {
        return (
          hasDistinctiveGlossarySurface(entry.display) &&
          entry.capitalizedFreq >= 5 &&
          capitalizedRatio >= 0.95 &&
          chapterFreq >= 2 &&
          entry.titleFreq === 0
        );
      }
      if (entry.titleFreq > 0 && entry.bodyFreq >= 1) {
        return chapterFreq >= 2 && (entry.freq >= 2 || entry.capitalizedFreq >= 2);
      }
      if (entry.capitalizedFreq >= 2 && capitalizedRatio >= 0.55) {
        return chapterFreq >= 2 || entry.freq >= 4;
      }
      return chapterFreq >= 2 && entry.freq >= 3 && entry.lowercaseFreq <= entry.capitalizedFreq;
    })
    .sort(
      (a, b) =>
        Number(isOrdinaryDictionaryWord(normalizeToken(a.display), ordinaryWords)) -
          Number(isOrdinaryDictionaryWord(normalizeToken(b.display), ordinaryWords)) ||
        b.titleFreq - a.titleFreq ||
        b.capitalizedFreq - a.capitalizedFreq ||
        b.chapterHits.size - a.chapterHits.size ||
        b.score - a.score ||
        b.freq - a.freq ||
        b.display.length - a.display.length
    )
    .slice(0, GLOSSARY_LIMIT)
    .map((entry) => entry.display);
}

export function buildChunkPlan(durationMs: number): TranscriptChunkPlan[] {
  if (!Number.isFinite(durationMs) || durationMs <= 0) return [];
  const stepMs = CHUNK_MS - CHUNK_OVERLAP_MS;
  const overlapTrimMs = Math.round(CHUNK_OVERLAP_MS / 2);
  const chunks: TranscriptChunkPlan[] = [];
  for (let startMs = 0, index = 0; startMs < durationMs; startMs += stepMs, index += 1) {
    const chunkDurationMs = Math.min(CHUNK_MS, durationMs - startMs);
    chunks.push({
      index,
      startMs,
      durationMs: chunkDurationMs,
      trimStartMs: index === 0 ? 0 : overlapTrimMs,
      trimEndMs: startMs + chunkDurationMs >= durationMs ? 0 : overlapTrimMs,
    });
  }
  return chunks;
}

async function runChildProcess(command: string, args: string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    const stderr: Buffer[] = [];
    child.stderr.on("data", (chunk) => stderr.push(Buffer.from(chunk)));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} exited with code ${code}: ${Buffer.concat(stderr).toString("utf8").trim()}`));
    });
  });
}

export async function extractChunkClip(args: ExtractChunkArgs): Promise<string> {
  await mkdir(args.tempDir, { recursive: true });
  const clipPath = path.join(args.tempDir, `${args.clipName}.mp3`);
  const clipStartMs = Math.max(0, Math.round(args.startMs));
  const clipDurationMs = Math.max(1_000, Math.round(args.durationMs));

  if (args.asset.kind === "single") {
    const input = args.files[0];
    if (!input) throw new Error("Single-file asset missing source file");
    await runChildProcess("ffmpeg", [
      "-v",
      "error",
      "-y",
      "-ss",
      String(clipStartMs / 1000),
      "-t",
      String(clipDurationMs / 1000),
      "-i",
      input.path,
      "-vn",
      "-ac",
      "1",
      "-ar",
      "16000",
      "-c:a",
      "mp3",
      "-b:a",
      "48k",
      clipPath,
    ]);
    return clipPath;
  }

  let cursor = 0;
  const selected: AssetFileRow[] = [];
  let selectedStartMs = 0;
  for (const file of args.files) {
    const fileStartMs = cursor;
    const fileEndMs = fileStartMs + file.duration_ms;
    if (clipStartMs < fileEndMs && clipStartMs + clipDurationMs > fileStartMs) {
      if (selected.length === 0) selectedStartMs = fileStartMs;
      selected.push(file);
    }
    cursor = fileEndMs;
  }
  if (selected.length === 0) throw new Error("No overlapping files for chapter analysis chunk");

  const concatPath = path.join(args.tempDir, `${args.clipName}.concat.txt`);
  const concatBody = selected.map((file) => `file '${file.path.replace(/'/g, "'\\''")}'`).join("\n");
  await writeFile(concatPath, concatBody, "utf8");
  try {
    await runChildProcess("ffmpeg", [
      "-v",
      "error",
      "-y",
      "-f",
      "concat",
      "-safe",
      "0",
      "-ss",
      String(Math.max(0, clipStartMs - selectedStartMs) / 1000),
      "-t",
      String(clipDurationMs / 1000),
      "-i",
      concatPath,
      "-vn",
      "-ac",
      "1",
      "-ar",
      "16000",
      "-c:a",
      "mp3",
      "-b:a",
      "48k",
      clipPath,
    ]);
  } finally {
    await rm(concatPath, { force: true }).catch(() => undefined);
  }
  return clipPath;
}

function promptForChunk(book: BookRow, glossary: string[]): string {
  const promptParts = [`${book.title} by ${book.author}.`];
  const transcriptionLanguage = normalizeTranscriptionLanguage(book.language);
  if (transcriptionLanguage) {
    promptParts.push(`Language: ${transcriptionLanguage}.`);
  }
  if (glossary.length > 0) {
    promptParts.push(`Important names and terms may include: ${glossary.join(", ")}.`);
    promptParts.push("Preserve these spellings when spoken.");
  }
  return promptParts.join(" ");
}

export async function transcribeChunk(
  settings: AppSettings,
  clipPath: string,
  prompt: string,
  book: BookRow
): Promise<TranscriptWord[]> {
  const apiKey = settings.agents.apiKey.trim();
  if (!apiKey) throw new Error("OpenAI API key not configured");
  const requestTimeoutMs = Math.max(TRANSCRIPTION_TIMEOUT_MS, Math.trunc(settings.agents.timeoutMs || 30_000));
  const client = new OpenAI({
    apiKey,
    timeout: requestTimeoutMs,
  });
  const transcriptionLanguage = normalizeTranscriptionLanguage(book.language);
  const response = (await client.audio.transcriptions.create(
    {
      file: createReadStream(clipPath),
      model: TRANSCRIPTION_MODEL,
      response_format: "verbose_json",
      timestamp_granularities: ["word"],
      ...(transcriptionLanguage ? { language: transcriptionLanguage } : {}),
      prompt,
    },
    {
      timeout: requestTimeoutMs,
    }
  )) as TranscriptionVerbose;
  const words = Array.isArray(response.words) ? response.words : [];
  return words
    .map((word) => ({
      startMs: Math.max(0, Math.round(Number(word.start) * 1000)),
      endMs: Math.max(0, Math.round(Number(word.end) * 1000)),
      token: normalizeToken(word.word),
      raw: String(word.word ?? "").trim(),
    }))
    .filter((word) => Boolean(word.token));
}

function dedupeTranscriptWords(words: TranscriptWord[]): TranscriptWord[] {
  const deduped: TranscriptWord[] = [];
  for (const word of words) {
    const last = deduped[deduped.length - 1];
    if (
      last &&
      last.token === word.token &&
      Math.abs(last.startMs - word.startMs) <= 1_500 &&
      Math.abs(last.endMs - word.endMs) <= 1_500
    ) {
      continue;
    }
    deduped.push(word);
  }
  return deduped;
}

function mergeTranscriptChunks(chunks: TranscriptChunk[]): TranscriptWord[] {
  const merged = chunks
    .flatMap((chunk) => {
      const keepStartMs = chunk.startMs + chunk.trimStartMs;
      const keepEndMs = chunk.startMs + chunk.durationMs - chunk.trimEndMs;
      return chunk.words
        .map((word) => ({
          ...word,
          startMs: word.startMs + chunk.startMs,
          endMs: word.endMs + chunk.startMs,
        }))
        .filter((word) => word.startMs >= keepStartMs && word.endMs <= keepEndMs);
    })
    .sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
  return dedupeTranscriptWords(merged);
}

function transcriptWordsFromStoredPayload(payload: StoredTranscriptPayload): TranscriptWord[] {
  return (payload.rawWords ?? payload.words)
    .map((word) => ({
      startMs: Math.max(0, Math.round(Number(word.startMs) || 0)),
      endMs: Math.max(0, Math.round(Number(word.endMs) || 0)),
      raw: typeof word.text === "string" ? word.text.trim() : "",
      token: typeof word.token === "string" && word.token.trim() ? word.token.trim() : normalizeToken(String(word.text ?? "")),
    }))
    .filter((word) => Boolean(word.token));
}

function tokenFrequencyMap(entries: EpubChapterEntry[]): Map<string, number> {
  const frequencies = new Map<string, number>();
  for (const entry of entries) {
    for (const token of entry.tokens) {
      frequencies.set(token, (frequencies.get(token) ?? 0) + 1);
    }
  }
  return frequencies;
}

function scoreAnchorWindow(tokens: string[], frequencies: Map<string, number>): number {
  let informative = 0;
  let score = 0;
  for (const token of tokens) {
    if (!token || STOPWORDS.has(token)) continue;
    informative += 1;
    score += 1 / Math.max(1, frequencies.get(token) ?? 1);
    if (token.length >= 7) score += 0.15;
  }
  if (informative < Math.ceil(tokens.length / 2)) return Number.NEGATIVE_INFINITY;
  return score;
}

function selectChapterAnchorStarts(entry: EpubChapterEntry, frequencies: Map<string, number>): number[] {
  if (entry.words.length <= ALIGNMENT_PROBE_WORDS * 2) return [];
  const starts: number[] = [];
  for (let center = CHAPTER_ANCHOR_INTERVAL_WORDS; center < entry.words.length - CHAPTER_ANCHOR_INTERVAL_WORDS; center += CHAPTER_ANCHOR_INTERVAL_WORDS) {
    let bestStart = -1;
    let bestScore = Number.NEGATIVE_INFINITY;
    const minStart = Math.max(0, center - CHAPTER_ANCHOR_SEARCH_RADIUS_WORDS);
    const maxStart = Math.min(entry.words.length - CHAPTER_ANCHOR_PROBE_WORDS, center + CHAPTER_ANCHOR_SEARCH_RADIUS_WORDS);
    for (let start = minStart; start <= maxStart; start += 1) {
      const tokens = entry.words.slice(start, start + CHAPTER_ANCHOR_PROBE_WORDS).map((word) => word.token);
      const score = scoreAnchorWindow(tokens, frequencies);
      if (score > bestScore) {
        bestScore = score;
        bestStart = start;
      }
    }
    if (bestStart >= 0 && starts.every((existing) => Math.abs(existing - bestStart) >= CHAPTER_ANCHOR_PROBE_WORDS * 2)) {
      starts.push(bestStart);
    }
  }
  return starts.sort((a, b) => a - b);
}

function alignTokenSpan(
  epubWords: EpubWord[],
  transcriptWords: TranscriptWord[],
  epubOffset: number,
  transcriptOffset: number
): Array<{ epubIndex: number; transcriptIndex: number }> {
  if (epubWords.length === 0 || transcriptWords.length === 0) return [];
  const gapPenalty = -1;
  const rows = epubWords.length;
  const cols = transcriptWords.length;
  const scores = Array.from({ length: rows + 1 }, () => new Float64Array(cols + 1));
  const trace = Array.from({ length: rows + 1 }, () => new Uint8Array(cols + 1));

  for (let i = 1; i <= rows; i += 1) {
    scores[i]![0] = i * gapPenalty;
    trace[i]![0] = 2;
  }
  for (let j = 1; j <= cols; j += 1) {
    scores[0]![j] = j * gapPenalty;
    trace[0]![j] = 3;
  }

  for (let i = 1; i <= rows; i += 1) {
    for (let j = 1; j <= cols; j += 1) {
      const diagScore = scores[i - 1]![j - 1]! + tokenAlignmentScore(epubWords[i - 1]!.token, transcriptWords[j - 1]!.token);
      const upScore = scores[i - 1]![j]! + gapPenalty;
      const leftScore = scores[i]![j - 1]! + gapPenalty;
      if (diagScore >= upScore && diagScore >= leftScore) {
        scores[i]![j] = diagScore;
        trace[i]![j] = 1;
      } else if (leftScore >= upScore) {
        scores[i]![j] = leftScore;
        trace[i]![j] = 3;
      } else {
        scores[i]![j] = upScore;
        trace[i]![j] = 2;
      }
    }
  }

  let i = rows;
  let j = cols;
  const pairs: Array<{ epubIndex: number; transcriptIndex: number }> = [];
  while (i > 0 && j > 0) {
    const direction = trace[i]![j] ?? 0;
    if (direction === 1) {
      if (tokenAlignmentScore(epubWords[i - 1]!.token, transcriptWords[j - 1]!.token) > 0) {
        pairs.push({ epubIndex: epubOffset + i - 1, transcriptIndex: transcriptOffset + j - 1 });
      }
      i -= 1;
      j -= 1;
      continue;
    }
    if (direction === 2) {
      i -= 1;
      continue;
    }
    if (direction === 3) {
      j -= 1;
      continue;
    }
    break;
  }
  return pairs.reverse();
}

function projectChapterWords(
  entry: EpubChapterEntry,
  chapter: StoredChapterTiming,
  pairs: Array<{ epubIndex: number; transcriptIndex: number }>,
  transcriptWords: TranscriptWord[]
): { words: StoredTranscriptWord[]; matchedWordCount: number; startMs: number; endMs: number } {
  const transcriptIndexByWord = new Map<number, number>();
  for (const pair of pairs) {
    if (!transcriptIndexByWord.has(pair.epubIndex)) {
      transcriptIndexByWord.set(pair.epubIndex, pair.transcriptIndex);
    }
  }

  const words = entry.words.map((word, index) => {
    const transcriptIndex = transcriptIndexByWord.get(index);
    if (transcriptIndex === undefined) {
      return {
        startMs: -1,
        endMs: -1,
        text: word.text,
        token: word.token,
      };
    }
    const transcriptWord = transcriptWords[transcriptIndex]!;
    return {
      startMs: transcriptWord.startMs,
      endMs: transcriptWord.endMs,
      text: word.text,
      token: word.token,
    };
  });

  for (let index = 0; index < words.length; index += 1) {
    if (words[index]!.startMs >= 0) continue;
    let nextIndex = index;
    while (nextIndex < words.length && words[nextIndex]!.startMs < 0) nextIndex += 1;
    const previous = index > 0 ? words[index - 1]! : null;
    const next = nextIndex < words.length ? words[nextIndex]! : null;
    const gapCount = nextIndex - index;
    const startBoundary = previous ? previous.endMs : chapter.startMs;
    const endBoundary = next ? next.startMs : chapter.endMs;
    const span = Math.max(gapCount, endBoundary - startBoundary);
    for (let offset = 0; offset < gapCount; offset += 1) {
      const wordStart = Math.round(startBoundary + (span * offset) / gapCount);
      const wordEnd = Math.round(startBoundary + (span * (offset + 1)) / gapCount);
      words[index + offset] = {
        startMs: wordStart,
        endMs: Math.max(wordStart, wordEnd),
        text: entry.words[index + offset]!.text,
        token: entry.words[index + offset]!.token,
      };
    }
    index = nextIndex - 1;
  }

  return {
    words,
    matchedWordCount: transcriptIndexByWord.size,
    startMs: words[0]?.startMs ?? chapter.startMs,
    endMs: words[words.length - 1]?.endMs ?? chapter.endMs,
  };
}

function chapterSegmentMatches(
  entries: EpubChapterEntry[],
  chapters: StoredChapterTiming[],
  transcriptWords: TranscriptWord[]
): ChapterSegmentMatch[] {
  const frequencies = tokenFrequencyMap(entries);
  return entries.map((entry, index) => {
    const chapter = chapters[index]!;
    const chapterStartIndex = transcriptWordIndexForTime(transcriptWords, chapter.startMs);
    const chapterEndIndex = Math.min(
      transcriptWords.length - 1,
      Math.max(chapterStartIndex, transcriptWordIndexForTime(transcriptWords, Math.max(chapter.startMs, chapter.endMs)))
    );
    const anchors: ChapterAnchor[] = [];

    const startProbeLength = Math.min(ALIGNMENT_PROBE_WORDS, entry.words.length);
    const startAlignment = alignProbeToTranscriptWindow(
      transcriptWords,
      entry.words.slice(0, startProbeLength).map((word) => word.token),
      chapter.startMs,
      "start"
    );
    if (startAlignment) {
      anchors.push({
        epubStart: 0,
        epubEnd: startProbeLength - 1,
        transcriptStart: startAlignment.startTranscriptIndex,
        transcriptEnd: startAlignment.endTranscriptIndex,
        matchedPairs: startAlignment.matchedPairs.map((pair) => ({ epubIndex: pair.probeIndex, transcriptIndex: pair.transcriptIndex })),
        attemptedTokenCount: startProbeLength,
      });
    }

    for (const anchorStart of selectChapterAnchorStarts(entry, frequencies)) {
      const anchorTokens = entry.words.slice(anchorStart, anchorStart + CHAPTER_ANCHOR_PROBE_WORDS).map((word) => word.token);
      const estimateMs = chapter.startMs + Math.round(((chapter.endMs - chapter.startMs) * anchorStart) / Math.max(1, entry.words.length));
      const alignment = alignProbeToTranscriptWindow(transcriptWords, anchorTokens, estimateMs, "start");
      if (!alignment) continue;
      anchors.push({
        epubStart: anchorStart,
        epubEnd: anchorStart + anchorTokens.length - 1,
        transcriptStart: alignment.startTranscriptIndex,
        transcriptEnd: alignment.endTranscriptIndex,
        matchedPairs: alignment.matchedPairs.map((pair) => ({
          epubIndex: anchorStart + pair.probeIndex,
          transcriptIndex: pair.transcriptIndex,
        })),
        attemptedTokenCount: anchorTokens.length,
      });
    }

    const endProbeLength = Math.min(ALIGNMENT_PROBE_WORDS, entry.words.length);
    const endProbeStart = Math.max(0, entry.words.length - endProbeLength);
    const endAlignment = alignProbeToTranscriptWindow(
      transcriptWords,
      entry.words.slice(endProbeStart).map((word) => word.token),
      chapter.endMs,
      "end"
    );
    if (endAlignment) {
      anchors.push({
        epubStart: endProbeStart,
        epubEnd: entry.words.length - 1,
        transcriptStart: endAlignment.startTranscriptIndex,
        transcriptEnd: endAlignment.endTranscriptIndex,
        matchedPairs: endAlignment.matchedPairs.map((pair) => ({
          epubIndex: endProbeStart + pair.probeIndex,
          transcriptIndex: pair.transcriptIndex,
        })),
        attemptedTokenCount: endProbeLength,
      });
    }

    const filteredAnchors = anchors
      .sort((left, right) => left.epubStart - right.epubStart || left.transcriptStart - right.transcriptStart)
      .filter((anchor) => anchor.transcriptStart >= chapterStartIndex && anchor.transcriptEnd <= chapterEndIndex)
      .reduce<ChapterAnchor[]>((acc, anchor) => {
        const previous = acc[acc.length - 1];
        if (!previous || (anchor.epubStart > previous.epubEnd && anchor.transcriptStart > previous.transcriptEnd)) {
          acc.push(anchor);
        }
        return acc;
      }, []);

    const pairMap = new Map<number, number>();
    const anchorAttemptedTokenCount = filteredAnchors.reduce((sum, anchor) => sum + anchor.attemptedTokenCount, 0);
    const matchedAnchorTokenCount = filteredAnchors.reduce((sum, anchor) => sum + anchor.matchedPairs.length, 0);
    for (const anchor of filteredAnchors) {
      for (const pair of anchor.matchedPairs) {
        if (!pairMap.has(pair.epubIndex)) pairMap.set(pair.epubIndex, pair.transcriptIndex);
      }
    }

    const boundaries = [
      { epubEnd: -1, transcriptEnd: chapterStartIndex - 1 },
      ...filteredAnchors,
      { epubStart: entry.words.length, transcriptStart: chapterEndIndex + 1 },
    ] as Array<Partial<ChapterAnchor> & { epubStart?: number; epubEnd?: number; transcriptStart?: number; transcriptEnd?: number }>;

    for (let boundaryIndex = 0; boundaryIndex < boundaries.length - 1; boundaryIndex += 1) {
      const left = boundaries[boundaryIndex]!;
      const right = boundaries[boundaryIndex + 1]!;
      const epubStart = (left.epubEnd ?? -1) + 1;
      const epubEnd = (right.epubStart ?? entry.words.length) - 1;
      const transcriptStart = (left.transcriptEnd ?? (chapterStartIndex - 1)) + 1;
      const transcriptEnd = (right.transcriptStart ?? (chapterEndIndex + 1)) - 1;
      if (epubStart > epubEnd || transcriptStart > transcriptEnd) continue;
      const spanPairs = alignTokenSpan(
        entry.words.slice(epubStart, epubEnd + 1),
        transcriptWords.slice(transcriptStart, transcriptEnd + 1),
        epubStart,
        transcriptStart
      );
      for (const pair of spanPairs) {
        if (!pairMap.has(pair.epubIndex)) pairMap.set(pair.epubIndex, pair.transcriptIndex);
      }
    }

    const projected = projectChapterWords(
      entry,
      chapter,
      Array.from(pairMap.entries()).map(([epubIndex, transcriptIndex]) => ({ epubIndex, transcriptIndex })),
      transcriptWords
    );
    return {
      chapterIndex: index,
      startMs: projected.startMs,
      endMs: projected.endMs,
      matchedWordCount: projected.matchedWordCount,
      anchorTokenCount: anchorAttemptedTokenCount,
      matchedAnchorTokenCount,
      anchorCoverage:
        anchorAttemptedTokenCount > 0 ? Math.round((matchedAnchorTokenCount / anchorAttemptedTokenCount) * 1000) / 1000 : 0,
      words: projected.words,
    };
  });
}

async function transcribeAssetWithDeps(
  ctx: ChapterAnalysisContext,
  asset: AssetRow,
  files: AssetFileRow[],
  book: BookRow,
  settings: AppSettings,
  deps: ChapterAnalysisDeps,
  job: JobRow,
  glossary: string[]
): Promise<{ transcriptWords: TranscriptWord[]; plans: TranscriptChunkPlan[] }> {
  const durationMs = asset.duration_ms ?? 0;
  if (durationMs <= 0) {
    throw new Error("Audio asset duration is required for transcription");
  }
  const prompt = promptForChunk(book, glossary);
  const plans = buildChunkPlan(durationMs);
  if (plans.length === 0) {
    throw new Error("No audio chunks available for transcription");
  }

  const tempDir = path.join(os.tmpdir(), "podible-chapter-analysis");
  await mkdir(tempDir, { recursive: true });

  const transcriptChunks = await Promise.all(
    plans.map(async (plan) => {
      const clipName = `asset-${asset.id}-chunk-${plan.index}-attempt-${job.attempt_count}`;
      let clipPath: string | null = null;
      try {
        log(
          ctx,
          `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=extract start_ms=${plan.startMs} duration_ms=${plan.durationMs}`
        );
        clipPath = await deps.extractChunkClip({
          asset,
          files,
          startMs: plan.startMs,
          durationMs: plan.durationMs,
          tempDir,
          clipName,
        });
        log(
          ctx,
          `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=transcribe clip=${JSON.stringify(clipPath)}`
        );
        const words = await deps.transcribeChunk(settings, clipPath, prompt, book);
        log(
          ctx,
          `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=done words=${words.length}`
        );
        return {
          ...plan,
          words,
        };
      } finally {
        if (clipPath) {
          await rm(clipPath, { force: true }).catch(() => undefined);
        }
      }
    })
  );

  const transcriptWords = mergeTranscriptChunks(transcriptChunks);
  if (transcriptWords.length === 0) {
    throw new Error("Whole-book transcription did not produce usable word timestamps");
  }
  return { transcriptWords, plans };
}

function analyzeTranscript(
  entries: EpubChapterEntry[],
  durationMs: number,
  transcriptWords: TranscriptWord[],
  glossary: string[],
  plans: TranscriptChunkPlan[],
  transcriptSource: "new" | "cached",
  options?: { includeTranscriptSegments?: boolean }
): AnalysisResult {
  const probes = boundaryProbes(entries, durationMs);
  const probeAlignments = probes.map((probe) => ({
    previous: alignBestProbeToTranscriptWindow(transcriptWords, probe.previousProbes, probe.estimateCandidatesMs, "end"),
    next: alignBestProbeToTranscriptWindow(transcriptWords, probe.nextProbes, probe.estimateCandidatesMs, "start"),
  }));
  const matches: BoundaryMatch[] = probes.map((probe) => {
    const previousAlignment = probeAlignments[probe.boundaryIndex]?.previous ?? null;
    const nextAlignment = probeAlignments[probe.boundaryIndex]?.next ?? null;
    const previousMatchMs = previousAlignment?.resolvedMs ?? null;
    const nextMatchMs = nextAlignment?.resolvedMs ?? null;
    const resolvedMs =
      typeof nextMatchMs === "number"
        ? nextMatchMs
        : typeof previousMatchMs === "number"
          ? previousMatchMs
          : null;
    return {
      boundaryIndex: probe.boundaryIndex,
      estimateMs: probe.estimateMs,
      resolvedMs,
      reason:
        resolvedMs === null ? "no_match" : typeof nextMatchMs === "number" ? "next_probe" : "previous_probe",
      previousMatchMs: previousMatchMs ?? undefined,
      nextMatchMs: nextMatchMs ?? undefined,
    };
  });

  const resolvedBoundaryCount = matches.filter((match) => typeof match.resolvedMs === "number").length;
  const totalBoundaryCount = matches.length;
  if (totalBoundaryCount === 0) {
    throw new Error("No chapter boundaries available for analysis");
  }
  const startTimes = interpolateChapterStarts(
    entries,
    matches.map((match) => match.resolvedMs),
    durationMs
  );
  if (!startTimes) {
    throw new Error("Interpolated chapter timings were not monotonic");
  }

  const chapters = timingsFromChapterStarts(entries, startTimes, durationMs);
  if (!chapters) {
    throw new Error("Failed to assemble chapter timings");
  }

  const transcriptSegments =
    options?.includeTranscriptSegments === false ? [] : chapterSegmentMatches(entries, chapters, transcriptWords);

  return {
    transcriptWords,
    chapters,
    transcriptSegments,
    resolvedBoundaryCount,
    totalBoundaryCount,
    debug: {
      transcriptSource,
      chunkCount: plans.length,
      glossary,
      chunks: plans.map((plan) => ({
        index: plan.index,
        startMs: plan.startMs,
        durationMs: plan.durationMs,
        trimStartMs: plan.trimStartMs,
        trimEndMs: plan.trimEndMs,
      })),
      transcriptWordCount: transcriptWords.length,
      matches: matches.map((match, index) => ({
        ...match,
        previousCoverage: probeAlignments[index]?.previous?.coverage ?? null,
        nextCoverage: probeAlignments[index]?.next?.coverage ?? null,
        previousMatchedTokenCount: probeAlignments[index]?.previous?.matchedTokenCount ?? 0,
        nextMatchedTokenCount: probeAlignments[index]?.next?.matchedTokenCount ?? 0,
      })),
    },
  };
}

export async function replayChapterAnalysisFromStoredTranscript(
  entries: EpubChapterEntry[],
  durationMs: number,
  transcriptPayload: StoredTranscriptPayload
): Promise<AnalysisResult> {
  return analyzeTranscript(
    entries,
    durationMs,
    transcriptWordsFromStoredPayload(transcriptPayload),
    extractGlossaryTerms(entries),
    [],
    "cached"
  );
}

export async function replayChapterBoundaryAnalysisFromStoredTranscript(
  entries: EpubChapterEntry[],
  durationMs: number,
  transcriptPayload: StoredTranscriptPayload
): Promise<AnalysisResult> {
  return analyzeTranscript(
    entries,
    durationMs,
    transcriptWordsFromStoredPayload(transcriptPayload),
    extractGlossaryTerms(entries),
    [],
    "cached",
    { includeTranscriptSegments: false }
  );
}

export async function queueChapterAnalysisForBook(repo: BooksRepo, bookId: number): Promise<JobRow | null> {
  const assets = repo.listAssetsByBook(bookId);
  const audioAsset = selectPreferredAudioAsset(assets);
  const epubAsset = selectPreferredEpubAsset(assets);
  if (!audioAsset || !epubAsset) return null;
  const existing = repo.findQueuedOrRunningJobByAsset("chapter_analysis", audioAsset.id);
  if (existing) return existing;
  return repo.createJob({
    type: "chapter_analysis",
    bookId,
    payload: {
      assetId: audioAsset.id,
      ebookAssetId: epubAsset.id,
    },
  });
}

function storedPayload(chapters: StoredChapterTiming[]): StoredChapterPayload {
  return {
    version: CHAPTERS_API_VERSION,
    chapters,
  };
}

export async function processChapterAnalysisJob(
  ctx: ChapterAnalysisContext,
  job: JobRow,
  deps: ChapterAnalysisDeps = {
    loadEpubEntries,
    extractChunkClip,
    transcribeChunk,
  }
): Promise<"done"> {
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as { assetId?: number; ebookAssetId?: number }) : {};
  if (!payload.assetId) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const target = ctx.repo.getAssetWithFiles(payload.assetId);
  if (!target || target.asset.kind === "ebook") {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const assets = ctx.repo.listAssetsByBook(target.asset.book_id);
  const preferredAudio = selectPreferredAudioAsset(assets);
  if (!preferredAudio || preferredAudio.id !== target.asset.id) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const ebookAsset = payload.ebookAssetId ? ctx.repo.getAssetWithFiles(payload.ebookAssetId) : null;
  if (!ebookAsset || ebookAsset.asset.kind !== "ebook" || ebookAsset.asset.mime !== "application/epub+zip") {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const ebookFile = ebookAsset.files[0];
  const book = ctx.repo.getBookByAsset(target.asset.id);
  if (!ebookFile || !book) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const chapterFingerprint = await computeChapterFingerprint(target.asset, target.files, ebookFile.path);
  const transcriptFingerprint = await computeTranscriptFingerprint(target.asset, target.files);
  const existing = ctx.repo.getChapterAnalysis(target.asset.id);
  if (
    existing &&
    existing.status === "succeeded" &&
    existing.fingerprint === chapterFingerprint &&
    existing.transcript_fingerprint === transcriptFingerprint
  ) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const settings = ctx.getSettings();
  const existingTranscriptRow = ctx.repo.getAssetTranscript(target.asset.id);
  const existingTranscriptPayload = await readStoredTranscriptPayload(existingTranscriptRow);
  const hasCachedTranscript =
    Boolean(existingTranscriptPayload) &&
    existingTranscriptRow?.status === "succeeded" &&
    existingTranscriptRow?.fingerprint === transcriptFingerprint;
  if (!hasCachedTranscript && !settings.agents.apiKey.trim()) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const entries = await deps.loadEpubEntries(ebookFile.path);
  if (entries.length < 2) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const durationMs = target.asset.duration_ms ?? 0;
  if (durationMs <= 0) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const glossary = extractGlossaryTerms(entries);

  ctx.repo.upsertChapterAnalysis({
    assetId: target.asset.id,
    status: "pending",
    source: CHAPTER_ANALYSIS_SOURCE,
    algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
    fingerprint: chapterFingerprint,
    transcriptFingerprint,
    chaptersJson: null,
    debugJson: null,
    resolvedBoundaryCount: 0,
    totalBoundaryCount: 0,
    error: null,
  });

  try {
    let transcriptWords: TranscriptWord[];
    let plans: TranscriptChunkPlan[];
    let transcriptSource: "new" | "cached";

    if (hasCachedTranscript && existingTranscriptPayload) {
      transcriptWords = transcriptWordsFromStoredPayload(existingTranscriptPayload);
      plans = [];
      transcriptSource = "cached";
    } else {
      ctx.repo.upsertAssetTranscript({
        assetId: target.asset.id,
        status: "pending",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: transcriptFingerprint,
        transcriptJson: null,
        error: null,
      });
      const transcript = await transcribeAssetWithDeps(ctx, target.asset, target.files, book, settings, deps, job, glossary);
      transcriptWords = transcript.transcriptWords;
      plans = transcript.plans;
      transcriptSource = "new";
    }

    const result = analyzeTranscript(entries, durationMs, transcriptWords, glossary, plans, transcriptSource);
    ctx.repo.upsertAssetTranscript({
      assetId: target.asset.id,
      status: "succeeded",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: transcriptFingerprint,
      transcriptJson: JSON.stringify(
        storedTranscriptPayload(
          transcriptWords,
          entries,
          result.chapters,
          result.transcriptSegments
        )
      ),
      error: null,
    });
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "succeeded",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: chapterFingerprint,
      transcriptFingerprint,
      chaptersJson: JSON.stringify(storedPayload(result.chapters)),
      debugJson: JSON.stringify(result.debug),
      resolvedBoundaryCount: result.resolvedBoundaryCount,
      totalBoundaryCount: result.totalBoundaryCount,
      error: null,
    });
    ctx.repo.markJobSucceeded(job.id);
    log(
      ctx,
      `[chapter-analysis] job=${job.id} asset=${target.asset.id} chunks=${result.debug.chunkCount} boundaries=${result.resolvedBoundaryCount}/${result.totalBoundaryCount} success=1`
    );
    return "done";
  } catch (error) {
    const message = (error as Error).message;
    const transcript = ctx.repo.getAssetTranscript(target.asset.id);
    if (!transcript || transcript.fingerprint !== transcriptFingerprint || transcript.status !== "succeeded") {
      ctx.repo.upsertAssetTranscript({
        assetId: target.asset.id,
        status: "failed",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: transcriptFingerprint,
        transcriptJson: transcript?.transcript_json ?? null,
        error: message,
      });
    }
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "failed",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: chapterFingerprint,
      transcriptFingerprint,
      chaptersJson: null,
      debugJson: JSON.stringify({ error: message }),
      resolvedBoundaryCount: 0,
      totalBoundaryCount: 0,
      error: message,
    });
    throw error;
  }
}

export async function readStoredChapterPayload(row: ChapterAnalysisRow | null): Promise<StoredChapterPayload | null> {
  if (!row || row.status !== "succeeded" || !row.chapters_json) return null;
  try {
    const parsed = JSON.parse(row.chapters_json) as StoredChapterPayload;
    return parsed && Array.isArray(parsed.chapters) ? parsed : null;
  } catch {
    return null;
  }
}

export { CHAPTERS_API_VERSION, CHAPTER_ANALYSIS_ALGORITHM_VERSION };
export type { StoredTranscriptPayload };
