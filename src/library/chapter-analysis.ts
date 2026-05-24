import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream, readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { initEpubFile } from "@lingo-reader/epub-parser";
import OpenAI from "openai";
import type { TranscriptionVerbose } from "openai/resources/audio/transcriptions";

import { promises as fsPromises } from "node:fs";

import { configDir, ensureConfigDirSync } from "../config";
import type { AppSettings, AssetFileRow, AssetRow, BookRow, JobRow, ManifestationRow } from "../app-types";
import { readFfprobeChapters } from "../media/probe-cache";
import type { BooksRepo } from "../repo";
import { slugify } from "../utils/strings";
import { selectPreferredAudioManifestation } from "./asset-selection";
import { runRecursiveAgenticChapterCurationDetailed } from "./chapter-curation";
import type { ChapterCurationTiming } from "./chapter-curation";

const CHAPTER_ANALYSIS_SOURCE = "whisper_transcript";
const CHAPTER_ANALYSIS_ALGORITHM_VERSION = "2026-04-22-atempo-2x-v1";
const CHAPTERS_API_VERSION = "1.5.0";
const TIMESTAMP_TRANSCRIPTION_MODEL = "whisper-1";
// We pre-process audio with ffmpeg `atempo` before sending to Whisper so OpenAI
// charges us for half the duration (they bill by minutes of input audio).
// Whisper tolerates this well — voice is still intelligible at 2x, and the
// timestamps it returns are in the sped-up frame, so we multiply them back by
// this factor to recover real-audio timestamps.
const TRANSCRIPTION_SPEED_MULTIPLIER = 2;
const CHAPTER_ANALYSIS_TRANSCRIPTION_CONCURRENCY = 2;
const CHUNK_MS = 30 * 60_000;
const CHUNK_OVERLAP_MS = 30_000;
const TRANSCRIPTION_TIMEOUT_MS = 5 * 60_000;
const GLOSSARY_LIMIT = 48;
const ORDINARY_WORDS_PATH = "/usr/share/dict/words";
const MAX_FRONT_MATTER_SKIP = 8;
const MAX_SECTION_DIVIDER_WORDS = 120;
const TRANSCRIPTS_DIR = path.join(configDir, "transcripts");

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

type StoredTranscriptWord = {
  startMs: number;
  endMs: number;
  text: string;
  token: string;
};

type StoredTranscriptUtterance = {
  startMs: number;
  endMs: number;
  text: string;
};

type StoredTranscriptChunk = {
  index: number;
  startMs: number;
  durationMs: number;
  trimStartMs: number;
  trimEndMs: number;
  wordStartIndex: number;
  wordEndIndex: number;
  text: string;
};

type StoredTranscriptPayload = {
  version: string;
  text: string;
  words: StoredTranscriptWord[];
  utterances?: StoredTranscriptUtterance[];
  chunks?: StoredTranscriptChunk[];
  rawText?: string;
  rawWords?: StoredTranscriptWord[];
};

type EpubTextKind = "heading" | "body";

type EpubWord = {
  text: string;
  token: string;
  kind?: EpubTextKind;
};

export type EpubChapterEntry = {
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

type TranscriptWord = {
  startMs: number;
  endMs: number;
  token: string;
  raw: string;
};

type TranscriptSegment = {
  startMs: number;
  endMs: number;
  text: string;
};

type TranscribedChunk = {
  words: TranscriptWord[];
  segments: TranscriptSegment[];
};

type TranscriptChunkPlan = {
  index: number;
  startMs: number;
  durationMs: number;
  trimStartMs: number;
  trimEndMs: number;
};

type PersistedTranscriptChunk = TranscriptChunkPlan & {
  path: string;
  wordCount: number;
  segmentCount: number;
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

type ChapterAnalysisContext = {
  repo: BooksRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
};

type ChapterAnalysisDeps = {
  loadEpubEntries: typeof loadEpubEntries;
  extractChunkClip: typeof extractChunkClip;
  transcribeChunk: typeof transcribeChunk;
  runAgenticCuration: typeof runRecursiveAgenticChapterCurationDetailed;
};

type ExtractChunkArgs = {
  asset: AssetRow;
  files: AssetFileRow[];
  startMs: number;
  durationMs: number;
  tempDir: string;
  clipName: string;
};

let ordinaryWordsCache: Set<string> | null | undefined;

function log(ctx: ChapterAnalysisContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

function createAsyncLimiter(limit: number) {
  let active = 0;
  const queue: Array<() => void> = [];

  const release = () => {
    active -= 1;
    const next = queue.shift();
    if (!next) return;
    active += 1;
    next();
  };

  return async function runLimited<T>(fn: () => Promise<T>): Promise<T> {
    if (active >= limit) {
      await new Promise<void>((resolve) => queue.push(resolve));
    } else {
      active += 1;
    }

    try {
      return await fn();
    } finally {
      release();
    }
  };
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

type EpubTextSegment = {
  kind: EpubTextKind;
  text: string;
};

function tokenizeChapterWords(value: string, kind?: EpubTextKind): EpubWord[] {
  const out: EpubWord[] = [];
  const matches = value.matchAll(/\b[\p{L}][\p{L}\p{N}'-]*\b/gu);
  for (const match of matches) {
    const text = match[0] ?? "";
    const token = normalizeToken(text);
    if (!token) continue;
    out.push({ text, token, kind });
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

function extractHtmlTextSegments(html: string): EpubTextSegment[] {
  const cleaned = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ");
  const segments: EpubTextSegment[] = [];
  let headingDepth = 0;

  for (const match of cleaned.matchAll(/<[^>]+>|[^<]+/g)) {
    const value = match[0] ?? "";
    if (value.startsWith("<")) {
      const tagMatch = value.match(/^<\s*(\/?)\s*([a-zA-Z][\w:-]*)\b/);
      if (!tagMatch) continue;
      const closing = tagMatch[1] === "/";
      const tag = tagMatch[2]!.toLowerCase().replace(/^.*:/, "");
      if (/^h[1-6]$/.test(tag)) {
        headingDepth = Math.max(0, headingDepth + (closing ? -1 : 1));
      }
      continue;
    }

    const text = decodeHtmlEntities(value).replace(/\s+/g, " ").trim();
    if (!text) continue;
    const kind: EpubTextKind = headingDepth > 0 ? "heading" : "body";
    const previous = segments[segments.length - 1];
    if (previous?.kind === kind) {
      previous.text = `${previous.text} ${text}`.replace(/\s+/g, " ").trim();
    } else {
      segments.push({ kind, text });
    }
  }

  return segments;
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
      const segments = extractHtmlTextSegments(chapter.html);
      const text = segments.map((segment) => segment.text).join(" ").replace(/\s+/g, " ").trim();
      const words = segments.flatMap((segment) => tokenizeChapterWords(segment.text, segment.kind));
      const tokens = words.map((word) => word.token);
      if (tokens.length === 0) continue;
      cumulativeWords += tokens.length;
      entries.push({
        id,
        title: titleById.get(id) || `Chapter ${index + 1}`,
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
    if (entries.length === 0 || totalWords === 0) return [];
    return entries.map((entry) => ({
      ...entry,
      cumulativeRatio: entry.cumulativeWords / totalWords,
    }));
  } finally {
    try {
      epub.destroy();
    } catch {
      // Ignore cleanup failures from the EPUB parser.
    }
    await rm(resourceDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

export async function computeEpubWordCount(epubPath: string): Promise<number | null> {
  const entries = await loadEpubEntries(epubPath);
  if (entries.length === 0) return null;
  return entries.reduce((sum, entry) => sum + entry.wordCount, 0);
}

function selectPreferredAudioAssetsForBook(repo: BooksRepo, bookId: number): AssetRow[] {
  const manifestations = repo.listManifestationsByBook(bookId);
  if (manifestations.length === 0) return [];
  const candidates = manifestations.map((manifestation) => ({
    manifestation,
    containers: repo.listAssetsByManifestation(manifestation.id),
  }));
  const chosen = selectPreferredAudioManifestation(candidates);
  return chosen?.containers ?? [];
}

function selectAudioAssetsForManifestation(repo: BooksRepo, bookId: number, manifestationId: number): AssetRow[] {
  const manifestation = repo.getManifestation(manifestationId);
  if (!manifestation || manifestation.book_id !== bookId || manifestation.kind !== "audio") return [];
  return repo.listAssetsByManifestation(manifestationId);
}

export function selectPreferredEpubAsset(assets: AssetRow[]): AssetRow | null {
  const ebooks = assets.filter((asset) => asset.mime === "application/epub+zip");
  if (ebooks.length === 0) return null;
  return [...ebooks].sort((a, b) => {
    if (a.created_at !== b.created_at) return b.created_at.localeCompare(a.created_at);
    return b.id - a.id;
  })[0] ?? null;
}

export function selectPreferredDownloadableEbookAsset(assets: AssetRow[]): AssetRow | null {
  const ebooks = assets.filter(
    (asset) => asset.mime === "application/epub+zip" || asset.mime === "application/pdf"
  );
  if (ebooks.length === 0) return null;
  return [...ebooks].sort((a, b) => {
    const formatScore = (asset: AssetRow) => (asset.mime === "application/epub+zip" ? 1 : 0);
    const score = formatScore(b) - formatScore(a);
    if (score !== 0) return score;
    if (a.created_at !== b.created_at) return b.created_at.localeCompare(a.created_at);
    return b.id - a.id;
  })[0] ?? null;
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

function canonicalizeGlossaryTerm(term: string): string {
  const trimmed = term.trim();
  if (trimmed.length < 3) return trimmed;
  return trimmed.replace(/['’](?:s|S)\b$/u, "").replace(/['’]\b$/u, "");
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

function hasDistinctiveGlossarySurface(term: string): boolean {
  return /[a-z][A-Z]/.test(term) || /[-']/u.test(term) || (/^[A-Z]{2,}$/u.test(term) && term.length <= 5);
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
  if (canonical.length > existing.display.length || (/[A-Z]/.test(canonical) && !/[A-Z]/.test(existing.display))) {
    existing.display = canonical;
  }
  stats.set(normalized, existing);
}

export function extractGlossaryTerms(entries: EpubChapterEntry[], ordinaryWords = loadOrdinaryWords()): string[] {
  const stats = new Map<string, GlossaryTermStats>();
  const termPattern = /\b[\p{L}][\p{L}\p{N}'-]{2,}\b/gu;

  for (const [chapterIndex, entry] of glossaryEntries(entries).entries()) {
    for (const term of entry.title.match(termPattern) ?? []) {
      collectGlossaryTerm(stats, term, chapterIndex, "title");
    }
    for (const term of entry.text.match(termPattern) ?? []) {
      collectGlossaryTerm(stats, term, chapterIndex, "body");
    }
  }

  return Array.from(stats.values())
    .filter((entry) => {
      const chapterFreq = entry.chapterHits.size;
      const capitalizedRatio = entry.freq > 0 ? entry.capitalizedFreq / entry.freq : 0;
      const normalized = normalizeToken(entry.display);
      const isOrdinaryWord = isOrdinaryDictionaryWord(normalized, ordinaryWords);
      if (/^[A-Z]{4,}$/u.test(entry.display) || GLOSSARY_ARTIFACT_WORDS.has(normalized)) return false;
      if (entry.bodyFreq === 0) return !isOrdinaryWord && entry.capitalizedFreq >= 3 && entry.titleFreq === 0;
      if (isOrdinaryWord) {
        return (
          hasDistinctiveGlossarySurface(entry.display) &&
          entry.capitalizedFreq >= 5 &&
          capitalizedRatio >= 0.95 &&
          chapterFreq >= 2 &&
          entry.titleFreq === 0
        );
      }
      if (entry.titleFreq > 0 && entry.bodyFreq >= 1) return chapterFreq >= 2 && (entry.freq >= 2 || entry.capitalizedFreq >= 2);
      if (entry.capitalizedFreq >= 2 && capitalizedRatio >= 0.55) return chapterFreq >= 2 || entry.freq >= 4;
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

const CHUNK_BOUNDARY_SNAP_WINDOW_MS = 60_000;

async function collectAssetChapterBoundariesMs(asset: AssetRow, files: AssetFileRow[]): Promise<number[]> {
  const boundaries = new Set<number>();
  if (asset.kind === "single") {
    const file = files[0];
    if (!file) return [];
    const stat = await fsPromises.stat(file.path).catch(() => null);
    if (!stat) return [];
    const chapters = readFfprobeChapters(file.path, Number(stat.mtimeMs));
    if (chapters) for (const chapter of chapters) boundaries.add(chapter.startMs);
    return [...boundaries].filter((b) => b > 0).sort((a, b) => a - b);
  }
  // Multi-file assets: per-file boundaries are at the cumulative file start times.
  let cursor = 0;
  for (const file of files) {
    if (cursor > 0) boundaries.add(cursor);
    cursor += file.duration_ms;
  }
  return [...boundaries].sort((a, b) => a - b);
}

function snapChunkEndToChapterBoundary(
  nominalEndMs: number,
  startMs: number,
  durationMs: number,
  chapterBoundariesMs: number[]
): number {
  if (chapterBoundariesMs.length === 0) return Math.min(nominalEndMs, durationMs);
  const minEndMs = startMs + CHUNK_MS / 2; // never snap so short that we'd make a useless tiny chunk
  const maxEndMs = Math.min(durationMs, startMs + CHUNK_MS + CHUNK_BOUNDARY_SNAP_WINDOW_MS);
  let best: number | null = null;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (const boundary of chapterBoundariesMs) {
    if (boundary <= minEndMs) continue;
    if (boundary > maxEndMs) break;
    if (Math.abs(boundary - nominalEndMs) > CHUNK_BOUNDARY_SNAP_WINDOW_MS) continue;
    const delta = Math.abs(boundary - nominalEndMs);
    if (delta < bestDelta) {
      best = boundary;
      bestDelta = delta;
    }
  }
  if (best == null) return Math.min(nominalEndMs, durationMs);
  return best;
}

export function buildChunkPlan(durationMs: number, chapterBoundariesMs: number[] = []): TranscriptChunkPlan[] {
  if (!Number.isFinite(durationMs) || durationMs <= 0) return [];
  const overlapTrimMs = Math.round(CHUNK_OVERLAP_MS / 2);
  const boundaries = [...chapterBoundariesMs]
    .filter((b) => Number.isFinite(b) && b > 0 && b < durationMs)
    .sort((a, b) => a - b);
  const chunks: TranscriptChunkPlan[] = [];
  let startMs = 0;
  for (let index = 0; startMs < durationMs; index += 1) {
    const nominalEndMs = Math.min(durationMs, startMs + CHUNK_MS);
    const endMs = snapChunkEndToChapterBoundary(nominalEndMs, startMs, durationMs, boundaries);
    const chunkDurationMs = endMs - startMs;
    const isLast = endMs >= durationMs;
    chunks.push({
      index,
      startMs,
      durationMs: chunkDurationMs,
      trimStartMs: index === 0 ? 0 : overlapTrimMs,
      trimEndMs: isLast ? 0 : overlapTrimMs,
    });
    if (isLast) break;
    startMs = endMs - CHUNK_OVERLAP_MS;
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
      "-filter:a",
      `atempo=${TRANSCRIPTION_SPEED_MULTIPLIER}`,
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
  if (selected.length === 0) throw new Error("No overlapping files for transcript chunk");

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
      "-filter:a",
      `atempo=${TRANSCRIPTION_SPEED_MULTIPLIER}`,
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
): Promise<TranscribedChunk> {
  const apiKey = settings.agents.apiKey.trim();
  if (!apiKey) throw new Error("OpenAI API key not configured");
  const requestTimeoutMs = Math.max(TRANSCRIPTION_TIMEOUT_MS, Math.trunc(settings.agents.timeoutMs || 30_000));
  const client = new OpenAI({ apiKey, timeout: requestTimeoutMs });
  const transcriptionLanguage = normalizeTranscriptionLanguage(book.language);
  const response = (await client.audio.transcriptions.create(
    {
      file: createReadStream(clipPath),
      model: TIMESTAMP_TRANSCRIPTION_MODEL,
      response_format: "verbose_json",
      timestamp_granularities: ["word", "segment"],
      ...(transcriptionLanguage ? { language: transcriptionLanguage } : {}),
      prompt,
    },
    { timeout: requestTimeoutMs }
  )) as TranscriptionVerbose;
  return parseWhisperResponse(response, TRANSCRIPTION_SPEED_MULTIPLIER);
}

// Exported for testing. Whisper sees audio sped up by `speedMultiplier`, so its
// timestamps are in the sped-up frame. We multiply by the same factor to
// recover real-audio timestamps.
export function parseWhisperResponse(response: TranscriptionVerbose, speedMultiplier: number): TranscribedChunk {
  const words = (Array.isArray(response.words) ? response.words : [])
    .map((word) => ({
      startMs: Math.max(0, Math.round(Number(word.start) * 1000 * speedMultiplier)),
      endMs: Math.max(0, Math.round(Number(word.end) * 1000 * speedMultiplier)),
      token: normalizeToken(word.word),
      raw: String(word.word ?? "").trim(),
    }))
    .filter((word) => Boolean(word.token));
  const segments = (Array.isArray(response.segments) ? response.segments : [])
    .map((segment) => ({
      startMs: Math.max(0, Math.round(Number(segment.start) * 1000 * speedMultiplier)),
      endMs: Math.max(0, Math.round(Number(segment.end) * 1000 * speedMultiplier)),
      text: String(segment.text ?? "").trim(),
    }))
    .filter((segment) => segment.text.length > 0 && segment.endMs > segment.startMs);
  return { words, segments };
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

async function persistTranscriptChunk(
  workDir: string,
  chunk: TranscriptChunkPlan,
  transcribed: TranscribedChunk
): Promise<PersistedTranscriptChunk> {
  const chunkPath = path.join(workDir, `chunk-${String(chunk.index).padStart(4, "0")}.json`);
  await writeFile(chunkPath, JSON.stringify({ ...chunk, words: transcribed.words, segments: transcribed.segments }));
  return { ...chunk, path: chunkPath, wordCount: transcribed.words.length, segmentCount: transcribed.segments.length };
}

async function readPersistedTranscriptChunk(chunk: PersistedTranscriptChunk): Promise<TranscribedChunk> {
  const parsed = JSON.parse(await Bun.file(chunk.path).text()) as TranscriptChunkPlan & {
    words?: TranscriptWord[];
    segments?: TranscriptSegment[];
  };
  return {
    words: Array.isArray(parsed.words) ? parsed.words : [],
    segments: Array.isArray(parsed.segments) ? parsed.segments : [],
  };
}

export function mergeChunkSegments(
  chunks: Array<{ plan: TranscriptChunkPlan; segments: TranscriptSegment[] }>
): TranscriptSegment[] {
  const merged: TranscriptSegment[] = [];
  for (const { plan, segments } of [...chunks].sort((a, b) => a.plan.index - b.plan.index)) {
    const keepStartMs = plan.startMs + plan.trimStartMs;
    const keepEndMs = plan.startMs + plan.durationMs - plan.trimEndMs;
    for (const segment of segments) {
      const startMs = segment.startMs + plan.startMs;
      const endMs = segment.endMs + plan.startMs;
      // Midpoint-wins: whichever chunk contains the segment's midpoint is the
      // canonical owner. Segments that straddle a seam are kept once (from the
      // chunk whose keep range includes the midpoint) instead of dropped twice.
      const midMs = Math.round((startMs + endMs) / 2);
      if (midMs < keepStartMs || midMs >= keepEndMs) continue;
      merged.push({ startMs, endMs, text: segment.text });
    }
  }
  return dedupeTranscriptSegments(merged.sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs));
}

async function mergePersistedTranscriptChunks(
  chunks: PersistedTranscriptChunk[]
): Promise<{ words: TranscriptWord[]; segments: TranscriptSegment[] }> {
  const mergedWords: TranscriptWord[] = [];
  const segmentInputs: Array<{ plan: TranscriptChunkPlan; segments: TranscriptSegment[] }> = [];
  for (const chunk of [...chunks].sort((a, b) => a.index - b.index)) {
    const { words, segments } = await readPersistedTranscriptChunk(chunk);
    const keepStartMs = chunk.startMs + chunk.trimStartMs;
    const keepEndMs = chunk.startMs + chunk.durationMs - chunk.trimEndMs;
    for (const word of words) {
      const startMs = word.startMs + chunk.startMs;
      const endMs = word.endMs + chunk.startMs;
      if (startMs < keepStartMs || endMs > keepEndMs) continue;
      mergedWords.push({ ...word, startMs, endMs });
    }
    segmentInputs.push({ plan: chunk, segments });
  }
  return {
    words: dedupeTranscriptWords(mergedWords.sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs)),
    segments: mergeChunkSegments(segmentInputs),
  };
}

function dedupeTranscriptSegments(segments: TranscriptSegment[]): TranscriptSegment[] {
  const deduped: TranscriptSegment[] = [];
  for (const segment of segments) {
    const last = deduped[deduped.length - 1];
    if (
      last &&
      last.text === segment.text &&
      Math.abs(last.startMs - segment.startMs) <= 1_500 &&
      Math.abs(last.endMs - segment.endMs) <= 1_500
    ) {
      continue;
    }
    deduped.push(segment);
  }
  return deduped;
}

function storedTranscriptPayload(
  rawWords: TranscriptWord[],
  plans: TranscriptChunkPlan[],
  rawSegments: TranscriptSegment[] = []
): StoredTranscriptPayload {
  const words = rawWords.map((word) => ({
    startMs: word.startMs,
    endMs: word.endMs,
    text: word.raw,
    token: word.token,
  }));
  const utterances: StoredTranscriptUtterance[] = rawSegments.map((segment) => ({
    startMs: segment.startMs,
    endMs: segment.endMs,
    text: segment.text,
  }));
  const text = rawWords.map((word) => word.raw).filter(Boolean).join(" ").trim();
  const chunks = plans.map((plan) => {
    const keepStartMs = plan.startMs + plan.trimStartMs;
    const keepEndMs = plan.startMs + plan.durationMs - plan.trimEndMs;
    const firstWordIndex = rawWords.findIndex((word) => word.startMs >= keepStartMs && word.endMs <= keepEndMs);
    let lastWordIndex = -1;
    for (let index = rawWords.length - 1; index >= 0; index -= 1) {
      const word = rawWords[index]!;
      if (word.startMs >= keepStartMs && word.endMs <= keepEndMs) {
        lastWordIndex = index;
        break;
      }
    }
    const chunkWords = firstWordIndex >= 0 && lastWordIndex >= firstWordIndex ? rawWords.slice(firstWordIndex, lastWordIndex + 1) : [];
    return {
      ...plan,
      wordStartIndex: firstWordIndex,
      wordEndIndex: lastWordIndex,
      text: chunkWords.map((word) => word.raw).filter(Boolean).join(" ").trim(),
    };
  });
  return {
    version: CHAPTERS_API_VERSION,
    text,
    words,
    ...(utterances.length > 0 ? { utterances } : {}),
    chunks,
    rawText: text,
    rawWords: words,
  };
}

function publicStoredTranscriptPayload(payload: StoredTranscriptPayload): StoredTranscriptPayload {
  return {
    version: payload.version,
    text: payload.text,
    words: payload.words,
    ...(Array.isArray(payload.utterances) ? { utterances: payload.utterances } : {}),
    ...(Array.isArray(payload.chunks) ? { chunks: payload.chunks } : {}),
  };
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

function transcriptArtifactPath(manifestationId: number, fingerprint: string): string {
  const safeFingerprint = fingerprint.replace(/[^a-zA-Z0-9._-]+/g, "_");
  return path.join(TRANSCRIPTS_DIR, `m${manifestationId}-${safeFingerprint}.json`);
}

async function persistTranscriptArtifact(manifestationId: number, fingerprint: string, transcriptJson: string): Promise<string> {
  ensureConfigDirSync();
  await mkdir(TRANSCRIPTS_DIR, { recursive: true });
  const filePath = transcriptArtifactPath(manifestationId, fingerprint);
  await writeFile(filePath, transcriptJson);
  return filePath;
}

export function hasStoredManifestationTranscriptPayload(repo: BooksRepo, manifestationId: number): boolean {
  const row = repo.getManifestationTranscript(manifestationId);
  return Boolean(row && row.status === "succeeded" && row.transcript_path);
}

export async function loadStoredManifestationTranscriptPayload(
  repo: BooksRepo,
  manifestationId: number
): Promise<StoredTranscriptPayload | null> {
  const row = repo.getManifestationTranscript(manifestationId);
  if (!row || row.status !== "succeeded" || !row.transcript_path) return null;
  const file = Bun.file(row.transcript_path);
  if (!(await file.exists())) return null;
  try {
    const parsed = JSON.parse(await file.text()) as StoredTranscriptPayload;
    if (!parsed || !Array.isArray(parsed.words)) return null;
    return publicStoredTranscriptPayload(parsed);
  } catch {
    return null;
  }
}

function offsetStoredTranscriptPayload(
  payload: StoredTranscriptPayload,
  offsetMs: number,
  chunkIndexOffset: number,
  wordIndexOffset: number
): StoredTranscriptPayload {
  return {
    version: payload.version,
    text: payload.text,
    words: payload.words.map((word) => ({
      ...word,
      startMs: word.startMs + offsetMs,
      endMs: word.endMs + offsetMs,
    })),
    ...(Array.isArray(payload.utterances)
      ? {
          utterances: payload.utterances.map((utterance) => ({
            ...utterance,
            startMs: utterance.startMs + offsetMs,
            endMs: utterance.endMs + offsetMs,
          })),
        }
      : {}),
    ...(Array.isArray(payload.chunks)
      ? {
          chunks: payload.chunks.map((chunk) => ({
            ...chunk,
            index: chunk.index + chunkIndexOffset,
            startMs: chunk.startMs + offsetMs,
            wordStartIndex: chunk.wordStartIndex + wordIndexOffset,
            wordEndIndex: chunk.wordEndIndex + wordIndexOffset,
          })),
        }
      : {}),
  };
}

function assetDurationMs(asset: AssetRow, files: AssetFileRow[]): number {
  return asset.duration_ms ?? files.reduce((sum, file) => sum + file.duration_ms, 0);
}

/**
 * Combine per-asset transcript payloads (each with its own durationMs) into a
 * single manifestation-level payload, offsetting timestamps and indices so that
 * all timestamps are relative to the start of the manifestation.
 */
function combineStoredTranscriptPayloads(
  parts: Array<{ payload: StoredTranscriptPayload; durationMs: number }>
): StoredTranscriptPayload {
  if (parts.length === 0) throw new Error("Cannot combine empty transcript parts");
  const payloads: StoredTranscriptPayload[] = [];
  let offsetMs = 0;
  let chunkIndexOffset = 0;
  let wordIndexOffset = 0;
  for (const { payload, durationMs } of parts) {
    const offsetPayload = offsetStoredTranscriptPayload(payload, offsetMs, chunkIndexOffset, wordIndexOffset);
    payloads.push(offsetPayload);
    offsetMs += durationMs;
    chunkIndexOffset += offsetPayload.chunks?.length ?? 0;
    wordIndexOffset += offsetPayload.words.length;
  }
  const first = payloads[0]!;
  return {
    version: first.version,
    text: payloads.map((p) => p.text).filter(Boolean).join("\n\n"),
    words: payloads.flatMap((p) => p.words),
    ...(payloads.some((p) => p.utterances?.length) ? { utterances: payloads.flatMap((p) => p.utterances ?? []) } : {}),
    ...(payloads.some((p) => p.chunks?.length) ? { chunks: payloads.flatMap((p) => p.chunks ?? []) } : {}),
    rawText: payloads.map((p) => p.rawText ?? p.text).filter(Boolean).join("\n\n"),
    rawWords: payloads.flatMap((p) => p.rawWords ?? p.words),
  };
}

async function fileFingerprintData(files: AssetFileRow[]): Promise<Array<{ path: string; size: number; mtimeMs: number }>> {
  return Promise.all(
    files.map(async (file) => {
      const stat = await Bun.file(file.path).stat();
      return {
        path: file.path,
        size: file.size,
        mtimeMs: stat.mtimeMs,
      };
    })
  );
}

async function computeTranscriptFingerprint(asset: AssetRow, files: AssetFileRow[]): Promise<string> {
  const hash = createHash("sha256");
  hash.update(
    JSON.stringify({
      version: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      kind: "transcript",
      model: TIMESTAMP_TRANSCRIPTION_MODEL,
      assetId: asset.id,
      durationMs: asset.duration_ms,
      files: await fileFingerprintData(files),
    })
  );
  return hash.digest("hex");
}

async function computeManifestationFingerprint(containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>): Promise<string> {
  const perAsset = await Promise.all(containers.map((c) => computeTranscriptFingerprint(c.asset, c.files)));
  return createHash("sha1")
    .update(containers.map((c, i) => `${c.asset.id}:${perAsset[i]}`).join("|"))
    .digest("hex");
}

async function loadGlossary(ctx: ChapterAnalysisContext, deps: ChapterAnalysisDeps, epubPath: string | null): Promise<string[]> {
  if (!epubPath) return [];
  try {
    return extractGlossaryTerms(await deps.loadEpubEntries(epubPath));
  } catch (error) {
    log(ctx, `[chapter-analysis] glossary load failed: ${(error as Error).message}`);
    return [];
  }
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
): Promise<{
  transcriptWords: TranscriptWord[];
  transcriptSegments: TranscriptSegment[];
  plans: TranscriptChunkPlan[];
  chunkWordCounts: number[];
}> {
  const durationMs = asset.duration_ms ?? 0;
  if (durationMs <= 0) throw new Error("Audio asset duration is required for transcription");
  const chapterBoundariesMs = await collectAssetChapterBoundariesMs(asset, files);
  const plans = buildChunkPlan(durationMs, chapterBoundariesMs);
  if (plans.length === 0) throw new Error("No audio chunks available for transcription");

  const workDir = await mkdtemp(path.join(os.tmpdir(), "podible-transcript-"));
  const prompt = promptForChunk(book, glossary);
  const limitChunkWork = createAsyncLimiter(CHAPTER_ANALYSIS_TRANSCRIPTION_CONCURRENCY);
  try {
    const persistedChunks = await Promise.all(
      plans.map((plan) =>
        limitChunkWork(async () => {
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
              tempDir: workDir,
              clipName,
            });
            log(
              ctx,
              `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=transcribe clip=${JSON.stringify(clipPath)}`
            );
            const transcribed = await deps.transcribeChunk(settings, clipPath, prompt, book);
            const persisted = await persistTranscriptChunk(workDir, plan, transcribed);
            log(
              ctx,
              `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=done words=${transcribed.words.length} segments=${transcribed.segments.length}`
            );
            return persisted;
          } finally {
            if (clipPath) await rm(clipPath, { force: true }).catch(() => undefined);
          }
        })
      )
    );
    const merged = await mergePersistedTranscriptChunks(persistedChunks);
    if (merged.words.length === 0) throw new Error("Whole-book transcription did not produce usable word timestamps");
    return {
      transcriptWords: merged.words,
      transcriptSegments: merged.segments,
      plans,
      chunkWordCounts: persistedChunks.sort((a, b) => a.index - b.index).map((chunk) => chunk.wordCount),
    };
  } finally {
    await rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

export async function queueChapterAnalysisForBook(repo: BooksRepo, bookId: number): Promise<JobRow | null> {
  const manifestations = repo.listManifestationsByBook(bookId);
  const candidates = manifestations.map((m) => ({ manifestation: m, containers: repo.listAssetsByManifestation(m.id) }));
  const chosen = selectPreferredAudioManifestation(candidates);
  if (!chosen) return null;
  return queueChapterAnalysisForManifestationRow(repo, bookId, chosen.manifestation.id);
}

export async function queueChapterAnalysisForManifestation(
  repo: BooksRepo,
  bookId: number,
  manifestationId: number
): Promise<JobRow | null> {
  const audioAssets = selectAudioAssetsForManifestation(repo, bookId, manifestationId);
  if (audioAssets.length === 0) return null;
  return queueChapterAnalysisForManifestationRow(repo, bookId, manifestationId);
}

async function queueChapterAnalysisForManifestationRow(repo: BooksRepo, bookId: number, manifestationId: number): Promise<JobRow | null> {
  const existing = repo.findQueuedOrRunningJobByManifestation("chapter_analysis", manifestationId);
  if (existing) return existing;
  const epubAsset = selectPreferredEpubAsset(repo.listAssetsByBook(bookId));
  return repo.createJob({
    type: "chapter_analysis",
    bookId,
    payload: {
      manifestationId,
      ...(epubAsset ? { ebookAssetId: epubAsset.id } : {}),
    },
  });
}

/**
 * Enqueue curation jobs for books that already have a succeeded transcript but
 * no chapters_json. Called once at server startup so existing books get curated
 * without manual intervention.
 */
export async function queuePendingCurationJobs(repo: BooksRepo): Promise<number> {
  const bookIds = repo.listBookIdsNeedingCuration();
  for (const bookId of bookIds) {
    await queueChapterAnalysisForBook(repo, bookId);
  }
  return bookIds.length;
}

export type TranscriptRequestStatus =
  | "current"
  | "stale"
  | "pending"
  | "running"
  | "failed"
  | "missing_audio"
  | "missing_config";

export type TranscriptRequestResult = {
  status: TranscriptRequestStatus;
  fingerprint: string | null;
  currentFingerprint: string | null;
  jobId: number | null;
  error: string | null;
};

async function buildTranscriptStatus(
  repo: BooksRepo,
  bookId: number,
  options: { apiKeyConfigured: boolean; manifestationId?: number | null }
): Promise<{
  hasAudio: boolean;
  epubAsset: AssetRow | null;
  base: TranscriptRequestResult;
}> {
  const audioAssets =
    options.manifestationId == null
      ? selectPreferredAudioAssetsForBook(repo, bookId)
      : selectAudioAssetsForManifestation(repo, bookId, options.manifestationId);
  const epubAsset = selectPreferredEpubAsset(repo.listAssetsByBook(bookId));
  if (audioAssets.length === 0) {
    return {
      hasAudio: false,
      epubAsset,
      base: { status: "missing_audio", fingerprint: null, currentFingerprint: null, jobId: null, error: null },
    };
  }

  const manifestationId = audioAssets[0]!.manifestation_id!;
  const existingJob = repo.findQueuedOrRunningJobByManifestation("chapter_analysis", manifestationId);
  const analysisRow = repo.getChapterAnalysis(manifestationId);

  // Compute the live combined fingerprint (one stat() per file, all containers in parallel).
  const containers = audioAssets.map((asset) => ({ asset, files: repo.getAssetFiles(asset.id) }));
  let currentFingerprint: string | null;
  try {
    currentFingerprint = await computeManifestationFingerprint(containers);
  } catch {
    currentFingerprint = null;
  }

  const storedFingerprint = analysisRow?.fingerprint ?? null;
  const runningJob = existingJob?.status === "running" ? existingJob : null;
  const pendingJob = existingJob ?? null;

  let status: TranscriptRequestStatus;
  if (runningJob) {
    status = "running";
  } else if (pendingJob) {
    status = "pending";
  } else if (currentFingerprint !== null && analysisRow?.status === "succeeded" && storedFingerprint === currentFingerprint) {
    status = "current";
  } else if (analysisRow?.status === "failed" && storedFingerprint === currentFingerprint) {
    status = "failed";
  } else if (analysisRow?.status === "succeeded") {
    status = "stale";
  } else if (!options.apiKeyConfigured) {
    status = "missing_config";
  } else {
    status = analysisRow?.status === "failed" ? "failed" : "stale";
  }

  // Surface any error message for display (only needed when failed).
  const error =
    status === "failed"
      ? (analysisRow?.error ?? repo.getManifestationTranscript(manifestationId)?.error ?? null)
      : null;

  return {
    hasAudio: true,
    epubAsset,
    base: {
      status,
      fingerprint: storedFingerprint,
      currentFingerprint,
      jobId: runningJob?.id ?? pendingJob?.id ?? null,
      error,
    },
  };
}

export async function getBookTranscriptStatus(
  repo: BooksRepo,
  bookId: number,
  options: { apiKeyConfigured: boolean; manifestationId?: number | null }
): Promise<TranscriptRequestResult> {
  const { base } = await buildTranscriptStatus(repo, bookId, options);
  return base;
}

export async function requestBookTranscription(
  repo: BooksRepo,
  bookId: number,
  options: { apiKeyConfigured: boolean; manifestationId?: number | null }
): Promise<TranscriptRequestResult> {
  const { hasAudio, base } = await buildTranscriptStatus(repo, bookId, options);
  if (!hasAudio) return base;
  if (base.status === "current") return base;
  if (base.status === "missing_config") return base;

  const job =
    options.manifestationId == null
      ? await queueChapterAnalysisForBook(repo, bookId)
      : await queueChapterAnalysisForManifestation(repo, bookId, options.manifestationId);
  if (!job) return base;
  return { ...base, status: base.status === "running" ? "running" : "pending", jobId: base.jobId ?? job.id };
}

type AgenticCurationResult = {
  manifestationId: number;
  chaptersJson: string;
  resolvedBoundaryCount: number;
  totalBoundaryCount: number;
  debugInfo: Record<string, unknown>;
};

async function buildEmbeddedChaptersForContainers(
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>
): Promise<ChapterCurationTiming[]> {
  const out: ChapterCurationTiming[] = [];
  let timeCursor = 0;
  for (const [containerIndex, container] of containers.entries()) {
    const containerDurationMs = container.asset.duration_ms ?? container.files.reduce((sum, file) => sum + file.duration_ms, 0);
    if (container.asset.kind === "single") {
      const file = container.files[0];
      if (file) {
        const stat = await fsPromises.stat(file.path).catch(() => null);
        const chapters = stat ? readFfprobeChapters(file.path, Number(stat.mtimeMs)) : null;
        if (chapters && chapters.length > 0) {
          for (const [chapterIndex, chapter] of chapters.entries()) {
            out.push({
              id: `c${containerIndex}-${chapter.id ?? `ch${chapterIndex}`}`,
              title: chapter.title,
              startMs: timeCursor + chapter.startMs,
              endMs: timeCursor + chapter.endMs,
            });
          }
          timeCursor += containerDurationMs;
          continue;
        }
      }
    }
    // Multi-file container or single-file with no embedded chapters: one entry per file.
    let fileCursor = timeCursor;
    for (const [fileIndex, file] of container.files.entries()) {
      out.push({
        id: `c${containerIndex}-f${fileIndex}`,
        title: file.title ?? `Part ${containerIndex + 1}`,
        startMs: fileCursor,
        endMs: fileCursor + file.duration_ms,
      });
      fileCursor += file.duration_ms;
    }
    timeCursor += containerDurationMs;
  }
  return out;
}

async function tryAgenticCuration(
  ctx: ChapterAnalysisContext,
  manifestation: ManifestationRow,
  audioContainers: Array<{ asset: AssetRow; files: AssetFileRow[] }>,
  payload: { manifestationId?: number; ebookAssetId?: number },
  settings: AppSettings,
  combinedFingerprint: string,
  runCuration: typeof runRecursiveAgenticChapterCurationDetailed
): Promise<AgenticCurationResult | null> {
  if (!settings.agents.apiKey.trim()) return null;
  if (!payload.ebookAssetId) return null;
  if (audioContainers.length === 0) return null;

  const ebookAsset = ctx.repo.getAssetWithFiles(payload.ebookAssetId);
  const ebookFile = ebookAsset?.files[0];
  if (!ebookFile) return null;

  const bookId = audioContainers[0]!.asset.book_id;
  const book = ctx.repo.getBookRow(bookId);
  if (!book) return null;

  let epubEntries: EpubChapterEntry[];
  try {
    epubEntries = await loadEpubEntries(ebookFile.path);
  } catch {
    return null;
  }

  const transcript = await loadStoredManifestationTranscriptPayload(ctx.repo, manifestation.id);
  if (!transcript) return null;

  const embeddedChapters = await buildEmbeddedChaptersForContainers(audioContainers);

  const totalDurationMs = manifestation.duration_ms ?? audioContainers.reduce((sum, c) => sum + (c.asset.duration_ms ?? 0), 0);
  const runId = new Date().toISOString();
  const curationRunDir = path.join(configDir, "chapter-curation-runs", slugify(`${book.author}-${book.title}`) || `book-${book.id}`);
  const debugEventLogPath = path.join(curationRunDir, `agent-events-${runId}.jsonl`);
  const debugTraceDir = path.join(curationRunDir, `agent-traces-${runId}`);
  await mkdir(curationRunDir, { recursive: true });

  try {
    const result = await runCuration({
      book,
      manifestation,
      containers: audioContainers,
      settings,
      durationMs: totalDurationMs,
      epubEntries,
      transcript,
      embeddedChapters,
      debugEventLogPath,
      debugTraceDir,
    });

    const reports = result.recursiveReports ?? [];
    const resolvedBoundaryCount = reports.filter((r) => r.outcome === "split").length;
    const totalBoundaryCount = reports.filter((r) => r.outcome === "split" || r.outcome === "failed").length;

    if (!result.result?.accepted || !result.result.chapters || result.result.chapters.length === 0) {
      return null;
    }

    const chapters = result.result.chapters.map((chapter) => ({
      title: chapter.title,
      startTime: chapter.startTime,
    }));

    return {
      manifestationId: manifestation.id,
      chaptersJson: JSON.stringify(chapters),
      resolvedBoundaryCount,
      totalBoundaryCount,
      debugInfo: {
        curation: {
          accepted: result.result.accepted,
          chapterCount: chapters.length,
          resolvedBoundaryCount,
          totalBoundaryCount,
          combinedFingerprint,
        },
      },
    };
  } catch (error) {
    // Curation failure is non-fatal: transcript job still succeeds.
    console.warn(`[chapter-analysis] curation failed for manifestation ${manifestation.id}: ${(error as Error).message}`);
    return null;
  }
}

export async function processChapterAnalysisJob(
  ctx: ChapterAnalysisContext,
  job: JobRow,
  deps: Partial<ChapterAnalysisDeps> = {}
): Promise<"done"> {
  const resolvedDeps: ChapterAnalysisDeps = {
    loadEpubEntries,
    extractChunkClip,
    transcribeChunk,
    runAgenticCuration: runRecursiveAgenticChapterCurationDetailed,
    ...deps,
  };
  const jobStartedAt = performance.now();
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as { manifestationId?: number; ebookAssetId?: number }) : {};
  if (!payload.manifestationId) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const manifestationId = payload.manifestationId;
  const manifestationData = ctx.repo.getManifestationWithContainers(manifestationId);
  if (!manifestationData) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const audioContainers = manifestationData.containers;
  if (audioContainers.length === 0) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  // Verify this manifestation is still the preferred one for its book.
  const preferredAudio = selectPreferredAudioAssetsForBook(ctx.repo, job.book_id ?? audioContainers[0]!.asset.book_id);
  if (!preferredAudio.some((asset) => asset.manifestation_id === manifestationId)) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const bookId = audioContainers[0]!.asset.book_id;
  const book = ctx.repo.getBookRow(bookId);
  if (!book) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  // Compute the combined manifestation fingerprint.
  const combinedFingerprint = await computeManifestationFingerprint(audioContainers);

  // Check if the manifestation-level transcript is already cached.
  const existingTranscriptRow = ctx.repo.getManifestationTranscript(manifestationId);
  const isCached =
    existingTranscriptRow?.status === "succeeded" &&
    existingTranscriptRow.fingerprint === combinedFingerprint &&
    existingTranscriptRow.transcript_path != null;
  const existingCachedPayload = isCached ? await loadStoredManifestationTranscriptPayload(ctx.repo, manifestationId) : null;
  const allCached = Boolean(existingCachedPayload);

  const settings = ctx.getSettings();
  if (!allCached && !settings.agents.apiKey.trim()) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  ctx.repo.upsertChapterAnalysis({
    manifestationId,
    status: "pending",
    source: CHAPTER_ANALYSIS_SOURCE,
    algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
    fingerprint: combinedFingerprint,
    chaptersJson: null,
    debugJson: null,
    resolvedBoundaryCount: 0,
    totalBoundaryCount: 0,
    error: null,
  });

  // Load the epub once for glossary building.
  const ebookAsset = payload.ebookAssetId ? ctx.repo.getAssetWithFiles(payload.ebookAssetId) : null;
  const ebookFile = ebookAsset?.asset.mime === "application/epub+zip" ? ebookAsset.files[0] : null;
  const glossary = allCached ? [] : await loadGlossary(ctx, resolvedDeps, ebookFile?.path ?? null);

  let totalChunkCount = 0;
  let totalWordCount = 0;
  let transcriptSource: "new" | "cached" = allCached ? "cached" : "new";

  try {
    if (allCached && existingCachedPayload) {
      // Reuse existing manifestation transcript — count words for logging.
      totalWordCount = transcriptWordsFromStoredPayload(existingCachedPayload).length;
    } else {
      // Transcribe each container sequentially, collect per-asset payloads,
      // then combine and persist a single manifestation-level transcript.
      ctx.repo.upsertManifestationTranscript({
        manifestationId,
        status: "pending",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: combinedFingerprint,
        error: null,
      });

      const perAssetPayloads: Array<{ payload: StoredTranscriptPayload; durationMs: number }> = [];
      for (const container of audioContainers) {
        const transcript = await transcribeAssetWithDeps(
          ctx,
          container.asset,
          container.files,
          book,
          settings,
          resolvedDeps,
          job,
          glossary
        );
        totalChunkCount += transcript.plans.length;
        totalWordCount += transcript.transcriptWords.length;
        transcriptSource = "new";
        perAssetPayloads.push({
          payload: storedTranscriptPayload(transcript.transcriptWords, transcript.plans, transcript.transcriptSegments),
          durationMs: assetDurationMs(container.asset, container.files),
        });
      }

      // Combine per-asset payloads with time offsets into one manifestation transcript.
      const combinedPayload = combineStoredTranscriptPayloads(perAssetPayloads);
      const transcriptJson = JSON.stringify(combinedPayload);
      const newTranscriptPath = await persistTranscriptArtifact(manifestationId, combinedFingerprint, transcriptJson);
      if (existingTranscriptRow?.transcript_path && existingTranscriptRow.transcript_path !== newTranscriptPath) {
        await rm(existingTranscriptRow.transcript_path, { force: true });
      }
      ctx.repo.upsertManifestationTranscript({
        manifestationId,
        status: "succeeded",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: combinedFingerprint,
        transcriptPath: newTranscriptPath,
        error: null,
      });
    }

    // Run agentic chapter curation once for the whole manifestation.
    const curationResult = await tryAgenticCuration(
      ctx,
      manifestationData.manifestation,
      audioContainers,
      payload,
      settings,
      combinedFingerprint,
      resolvedDeps.runAgenticCuration
    );

    ctx.repo.upsertChapterAnalysis({
      manifestationId,
      status: "succeeded",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: combinedFingerprint,
      chaptersJson: curationResult?.chaptersJson ?? null,
      debugJson: JSON.stringify({
        transcriptSource,
        chunkCount: totalChunkCount,
        transcriptWordCount: totalWordCount,
        model: TIMESTAMP_TRANSCRIPTION_MODEL,
        ...(curationResult?.debugInfo ?? {}),
      }),
      resolvedBoundaryCount: curationResult?.resolvedBoundaryCount ?? 0,
      totalBoundaryCount: curationResult?.totalBoundaryCount ?? 0,
      error: null,
    });
    ctx.repo.markJobSucceeded(job.id);
    const totalJobMs = Math.round((performance.now() - jobStartedAt) * 100) / 100;
    log(
      ctx,
      `[chapter-analysis] job=${job.id} manifestation=${manifestationId} containers=${audioContainers.length} chunks=${totalChunkCount} words=${totalWordCount} source=${transcriptSource} chapters=${curationResult?.chaptersJson ? "yes" : "no"} success=1 total_ms=${totalJobMs}`
    );
    return "done";
  } catch (error) {
    const message = (error as Error).message;
    // Mark the manifestation transcript as failed if it was in progress.
    const currentTranscript = ctx.repo.getManifestationTranscript(manifestationId);
    if (!currentTranscript || currentTranscript.fingerprint !== combinedFingerprint || currentTranscript.status !== "succeeded") {
      ctx.repo.upsertManifestationTranscript({
        manifestationId,
        status: "failed",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: combinedFingerprint,
        transcriptPath: currentTranscript?.transcript_path ?? null,
        error: message,
      });
    }
    ctx.repo.upsertChapterAnalysis({
      manifestationId,
      status: "failed",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: combinedFingerprint,
      chaptersJson: null,
      debugJson: JSON.stringify({ error: message }),
      resolvedBoundaryCount: 0,
      totalBoundaryCount: 0,
      error: message,
    });
    throw error;
  }
}

export { CHAPTERS_API_VERSION, CHAPTER_ANALYSIS_ALGORITHM_VERSION };
export type { StoredTranscriptPayload, StoredTranscriptUtterance, TranscriptWord, TranscriptChunkPlan, TranscriptSegment };
