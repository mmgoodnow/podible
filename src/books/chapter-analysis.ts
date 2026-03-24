import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream, readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { initEpubFile } from "@lingo-reader/epub-parser";
import OpenAI from "openai";
import type { TranscriptionVerbose } from "openai/resources/audio/transcriptions";

import type { BooksRepo } from "./repo";
import type { AppSettings, AssetFileRow, AssetRow, BookRow, ChapterAnalysisRow, JobRow } from "./types";

const CHAPTER_ANALYSIS_SOURCE = "full_transcript_epub";
const CHAPTER_ANALYSIS_ALGORITHM_VERSION = "2026-03-24-v2";
const CHAPTERS_API_VERSION = "1.2.0";
const TRANSCRIPTION_MODEL = "whisper-1";
const CHUNK_MS = 60 * 60_000;
const CHUNK_OVERLAP_MS = 30_000;
const PROBE_WORDS = 24;
const MIN_SHINGLE = 6;
const MAX_DRIFT_MS = 150_000;
const MIN_RESOLVED_BOUNDARY_RATIO = 0.5;
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

type EpubChapterEntry = {
  id: string;
  title: string;
  href: string;
  text: string;
  tokens: string[];
  wordCount: number;
  cumulativeWords: number;
  cumulativeRatio: number;
};

type ChapterBoundaryProbe = {
  boundaryIndex: number;
  estimateMs: number;
  previousTitle: string;
  nextTitle: string;
  previousProbe: string[];
  nextProbe: string[];
};

type BoundaryMatch = {
  boundaryIndex: number;
  resolvedMs: number | null;
  estimateMs: number;
  reason: string;
  previousMatchMs?: number;
  nextMatchMs?: number;
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

type AnalysisResult = {
  chapters: StoredChapterTiming[];
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

function assetMediaType(asset: AssetRow): "audio" | "ebook" {
  return asset.kind === "ebook" ? "ebook" : "audio";
}

function scoreAudioAsset(asset: AssetRow): number {
  let score = 0;
  if (asset.kind === "single" && asset.mime === "audio/mp4") score += 100;
  if (asset.kind === "single" && asset.mime === "audio/mpeg") score += 80;
  if (asset.kind === "multi") score += 60;
  score += Math.min(50, Math.trunc((asset.duration_ms ?? 0) / 60_000));
  return score;
}

function selectPreferredAudioAsset(assets: AssetRow[]): AssetRow | null {
  const audio = assets.filter((asset) => assetMediaType(asset) === "audio");
  if (audio.length === 0) return null;
  return [...audio].sort((a, b) => {
    const score = scoreAudioAsset(b) - scoreAudioAsset(a);
    if (score !== 0) return score;
    if (a.created_at !== b.created_at) return b.created_at.localeCompare(a.created_at);
    return b.id - a.id;
  })[0] ?? null;
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

function tokenize(value: string): string[] {
  return value
    .split(/\s+/)
    .map((token) => normalizeToken(token))
    .filter(Boolean);
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
      const tokens = tokenize(text);
      if (tokens.length === 0) continue;
      cumulativeWords += tokens.length;
      const title = titleById.get(id) || `Chapter ${index + 1}`;
      entries.push({
        id,
        title,
        href: hrefById.get(id) ?? id,
        text,
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

function boundaryProbes(entries: EpubChapterEntry[], durationMs: number): ChapterBoundaryProbe[] {
  const ratios = chapterStartRatios(entries);
  const out: ChapterBoundaryProbe[] = [];
  for (let index = 0; index < entries.length - 1; index += 1) {
    const previous = entries[index]!;
    const next = entries[index + 1]!;
    out.push({
      boundaryIndex: index,
      estimateMs: Math.round(durationMs * (ratios[index + 1] ?? 0)),
      previousTitle: previous.title,
      nextTitle: next.title,
      previousProbe: previous.tokens.slice(Math.max(0, previous.tokens.length - PROBE_WORDS)),
      nextProbe: next.tokens.slice(0, PROBE_WORDS),
    });
  }
  return out;
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

async function computeFingerprint(asset: AssetRow, files: AssetFileRow[], epubPath: string): Promise<string> {
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
  if (book.language) {
    promptParts.push(`Language: ${book.language}.`);
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
  const client = new OpenAI({
    apiKey,
    timeout: Math.max(1_000, Math.trunc(settings.agents.timeoutMs || 30_000)),
  });
  const response = (await client.audio.transcriptions.create(
    {
      file: createReadStream(clipPath),
      model: TRANSCRIPTION_MODEL,
      response_format: "verbose_json",
      timestamp_granularities: ["word"],
      ...(book.language ? { language: book.language } : {}),
      prompt,
    },
    {
      timeout: Math.max(1_000, Math.trunc(settings.agents.timeoutMs || 30_000)),
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

async function analyzeWithDeps(
  asset: AssetRow,
  files: AssetFileRow[],
  epubPath: string,
  book: BookRow,
  settings: AppSettings,
  deps: ChapterAnalysisDeps,
  job: JobRow
): Promise<AnalysisResult> {
  const entries = await deps.loadEpubEntries(epubPath);
  if (entries.length < 2) {
    throw new Error("EPUB parser did not yield enough usable chapters");
  }
  const durationMs = asset.duration_ms ?? 0;
  if (durationMs <= 0) {
    throw new Error("Audio asset duration is required for chapter analysis");
  }

  const glossary = extractGlossaryTerms(entries);
  const prompt = promptForChunk(book, glossary);
  const plans = buildChunkPlan(durationMs);
  if (plans.length === 0) {
    throw new Error("No audio chunks available for transcription");
  }

  const tempDir = path.join(os.tmpdir(), "podible-chapter-analysis");
  await mkdir(tempDir, { recursive: true });

  const transcriptChunks: TranscriptChunk[] = [];
  for (const plan of plans) {
    const clipName = `asset-${asset.id}-chunk-${plan.index}-attempt-${job.attempt_count}`;
    let clipPath: string | null = null;
    try {
      clipPath = await deps.extractChunkClip({
        asset,
        files,
        startMs: plan.startMs,
        durationMs: plan.durationMs,
        tempDir,
        clipName,
      });
      const words = await deps.transcribeChunk(settings, clipPath, prompt, book);
      transcriptChunks.push({
        ...plan,
        words,
      });
    } finally {
      if (clipPath) {
        await rm(clipPath, { force: true }).catch(() => undefined);
      }
    }
  }

  const transcriptWords = mergeTranscriptChunks(transcriptChunks);
  if (transcriptWords.length === 0) {
    throw new Error("Whole-book transcription did not produce usable word timestamps");
  }

  const probes = boundaryProbes(entries, durationMs);
  const matches: BoundaryMatch[] = probes.map((probe) => {
    const previousMatchMs = findProbeTimestamp(transcriptWords, probe.previousProbe, "end", probe.estimateMs);
    const nextMatchMs = findProbeTimestamp(transcriptWords, probe.nextProbe, "start", probe.estimateMs);
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
  if (resolvedBoundaryCount / totalBoundaryCount < MIN_RESOLVED_BOUNDARY_RATIO) {
    throw new Error("Too few chapter boundaries resolved from full transcript");
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

  return {
    chapters,
    resolvedBoundaryCount,
    totalBoundaryCount,
    debug: {
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
      matches,
    },
  };
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

  const settings = ctx.getSettings();
  if (!settings.agents.apiKey.trim()) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const fingerprint = await computeFingerprint(target.asset, target.files, ebookFile.path);
  const existing = ctx.repo.getChapterAnalysis(target.asset.id);
  if (existing && existing.status === "succeeded" && existing.fingerprint === fingerprint) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  ctx.repo.upsertChapterAnalysis({
    assetId: target.asset.id,
    status: "pending",
    source: CHAPTER_ANALYSIS_SOURCE,
    algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
    fingerprint,
    chaptersJson: null,
    debugJson: null,
    resolvedBoundaryCount: 0,
    totalBoundaryCount: 0,
    error: null,
  });

  try {
    const result = await analyzeWithDeps(target.asset, target.files, ebookFile.path, book, settings, deps, job);
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "succeeded",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint,
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
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "failed",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint,
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
