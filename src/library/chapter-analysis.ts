import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream, readFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { initEpubFile } from "@lingo-reader/epub-parser";
import OpenAI from "openai";
import type { TranscriptionVerbose } from "openai/resources/audio/transcriptions";

import { configDir, ensureConfigDirSync } from "../config";
import type { AppSettings, AssetFileRow, AssetRow, AssetTranscriptRow, BookRow, JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { selectPreferredAudioAsset } from "./asset-selection";

const CHAPTER_ANALYSIS_SOURCE = "whisper_transcript";
const CHAPTER_ANALYSIS_ALGORITHM_VERSION = "2026-04-16-transcript-only-v1";
const CHAPTERS_API_VERSION = "1.4.0";
const TIMESTAMP_TRANSCRIPTION_MODEL = "whisper-1";
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
  chunks?: StoredTranscriptChunk[];
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

type PersistedTranscriptChunk = TranscriptChunkPlan & {
  path: string;
  wordCount: number;
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

export function selectPreferredEpubAsset(assets: AssetRow[]): AssetRow | null {
  const ebooks = assets.filter((asset) => asset.kind === "ebook" && asset.mime === "application/epub+zip");
  if (ebooks.length === 0) return null;
  return [...ebooks].sort((a, b) => {
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
  const client = new OpenAI({ apiKey, timeout: requestTimeoutMs });
  const transcriptionLanguage = normalizeTranscriptionLanguage(book.language);
  const response = (await client.audio.transcriptions.create(
    {
      file: createReadStream(clipPath),
      model: TIMESTAMP_TRANSCRIPTION_MODEL,
      response_format: "verbose_json",
      timestamp_granularities: ["word"],
      ...(transcriptionLanguage ? { language: transcriptionLanguage } : {}),
      prompt,
    },
    { timeout: requestTimeoutMs }
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

async function persistTranscriptChunk(workDir: string, chunk: TranscriptChunkPlan, words: TranscriptWord[]): Promise<PersistedTranscriptChunk> {
  const chunkPath = path.join(workDir, `chunk-${String(chunk.index).padStart(4, "0")}.json`);
  await writeFile(chunkPath, JSON.stringify({ ...chunk, words }));
  return { ...chunk, path: chunkPath, wordCount: words.length };
}

async function readPersistedTranscriptChunk(chunk: PersistedTranscriptChunk): Promise<TranscriptWord[]> {
  const parsed = JSON.parse(await Bun.file(chunk.path).text()) as TranscriptChunkPlan & { words?: TranscriptWord[] };
  return Array.isArray(parsed.words) ? parsed.words : [];
}

async function mergePersistedTranscriptChunks(chunks: PersistedTranscriptChunk[]): Promise<TranscriptWord[]> {
  const merged: TranscriptWord[] = [];
  for (const chunk of [...chunks].sort((a, b) => a.index - b.index)) {
    const words = await readPersistedTranscriptChunk(chunk);
    const keepStartMs = chunk.startMs + chunk.trimStartMs;
    const keepEndMs = chunk.startMs + chunk.durationMs - chunk.trimEndMs;
    for (const word of words) {
      const startMs = word.startMs + chunk.startMs;
      const endMs = word.endMs + chunk.startMs;
      if (startMs < keepStartMs || endMs > keepEndMs) continue;
      merged.push({ ...word, startMs, endMs });
    }
  }
  return dedupeTranscriptWords(merged.sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs));
}

function storedTranscriptPayload(rawWords: TranscriptWord[], plans: TranscriptChunkPlan[]): StoredTranscriptPayload {
  const words = rawWords.map((word) => ({
    startMs: word.startMs,
    endMs: word.endMs,
    text: word.raw,
    token: word.token,
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

function transcriptArtifactPath(assetId: number, fingerprint: string): string {
  const safeFingerprint = fingerprint.replace(/[^a-zA-Z0-9._-]+/g, "_");
  return path.join(TRANSCRIPTS_DIR, `${assetId}-${safeFingerprint}.json`);
}

async function persistTranscriptArtifact(assetId: number, fingerprint: string, transcriptJson: string): Promise<string> {
  ensureConfigDirSync();
  await mkdir(TRANSCRIPTS_DIR, { recursive: true });
  const filePath = transcriptArtifactPath(assetId, fingerprint);
  await writeFile(filePath, transcriptJson);
  return filePath;
}

async function ensureStoredTranscriptFileForRow(repo: BooksRepo, row: AssetTranscriptRow | null): Promise<string | null> {
  if (!row || row.status !== "succeeded") return null;
  if (row.transcript_path && (await Bun.file(row.transcript_path).exists())) {
    return row.transcript_path;
  }
  if (!row.transcript_json) return null;
  const filePath = await persistTranscriptArtifact(row.asset_id, row.fingerprint, row.transcript_json);
  repo.upsertAssetTranscript({
    assetId: row.asset_id,
    status: row.status,
    source: row.source,
    algorithmVersion: row.algorithm_version,
    fingerprint: row.fingerprint,
    transcriptPath: filePath,
    transcriptJson: null,
    error: row.error,
  });
  return filePath;
}

export function hasStoredTranscriptPayload(repo: BooksRepo, assetId: number): boolean {
  const row = repo.getAssetTranscript(assetId);
  return Boolean(row && row.status === "succeeded" && (row.transcript_path || row.transcript_json));
}

export async function ensureStoredTranscriptFile(repo: BooksRepo, assetId: number): Promise<string | null> {
  return ensureStoredTranscriptFileForRow(repo, repo.getAssetTranscript(assetId));
}

export async function readStoredTranscriptPayload(
  repo: BooksRepo,
  row: AssetTranscriptRow | null
): Promise<StoredTranscriptPayload | null> {
  if (!row || row.status !== "succeeded") return null;
  const filePath = await ensureStoredTranscriptFileForRow(repo, row);
  const jsonText = filePath ? await Bun.file(filePath).text() : row.transcript_json;
  if (!jsonText) return null;
  try {
    const parsed = JSON.parse(jsonText) as StoredTranscriptPayload;
    if (!parsed || !Array.isArray(parsed.words)) return null;
    return parsed;
  } catch {
    return null;
  }
}

export async function loadStoredTranscriptPayload(repo: BooksRepo, assetId: number): Promise<StoredTranscriptPayload | null> {
  const parsed = await readStoredTranscriptPayload(repo, repo.getAssetTranscript(assetId));
  return parsed ? publicStoredTranscriptPayload(parsed) : null;
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
): Promise<{ transcriptWords: TranscriptWord[]; plans: TranscriptChunkPlan[]; chunkWordCounts: number[] }> {
  const durationMs = asset.duration_ms ?? 0;
  if (durationMs <= 0) throw new Error("Audio asset duration is required for transcription");
  const plans = buildChunkPlan(durationMs);
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
            const words = await deps.transcribeChunk(settings, clipPath, prompt, book);
            const persisted = await persistTranscriptChunk(workDir, plan, words);
            log(
              ctx,
              `[chapter-analysis] job=${job.id} asset=${asset.id} chunk=${plan.index + 1}/${plans.length} stage=done words=${words.length}`
            );
            return persisted;
          } finally {
            if (clipPath) await rm(clipPath, { force: true }).catch(() => undefined);
          }
        })
      )
    );
    const transcriptWords = await mergePersistedTranscriptChunks(persistedChunks);
    if (transcriptWords.length === 0) throw new Error("Whole-book transcription did not produce usable word timestamps");
    return {
      transcriptWords,
      plans,
      chunkWordCounts: persistedChunks.sort((a, b) => a.index - b.index).map((chunk) => chunk.wordCount),
    };
  } finally {
    await rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

export async function queueChapterAnalysisForBook(repo: BooksRepo, bookId: number): Promise<JobRow | null> {
  const assets = repo.listAssetsByBook(bookId);
  const audioAsset = selectPreferredAudioAsset(assets);
  if (!audioAsset) return null;
  const existing = repo.findQueuedOrRunningJobByAsset("chapter_analysis", audioAsset.id);
  if (existing) return existing;
  const epubAsset = selectPreferredEpubAsset(assets);
  return repo.createJob({
    type: "chapter_analysis",
    bookId,
    payload: {
      assetId: audioAsset.id,
      ...(epubAsset ? { ebookAssetId: epubAsset.id } : {}),
    },
  });
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
    ...deps,
  };
  const jobStartedAt = performance.now();
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

  const book = ctx.repo.getBookByAsset(target.asset.id);
  const durationMs = target.asset.duration_ms ?? 0;
  if (!book || durationMs <= 0) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  const transcriptFingerprint = await computeTranscriptFingerprint(target.asset, target.files);
  const existingTranscriptRow = ctx.repo.getAssetTranscript(target.asset.id);
  const existingTranscriptPayload = await readStoredTranscriptPayload(ctx.repo, existingTranscriptRow);
  const hasCachedTranscript =
    Boolean(existingTranscriptPayload) &&
    existingTranscriptRow?.status === "succeeded" &&
    existingTranscriptRow?.fingerprint === transcriptFingerprint;

  const settings = ctx.getSettings();
  if (!hasCachedTranscript && !settings.agents.apiKey.trim()) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }

  ctx.repo.upsertChapterAnalysis({
    assetId: target.asset.id,
    status: "pending",
    source: CHAPTER_ANALYSIS_SOURCE,
    algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
    fingerprint: transcriptFingerprint,
    transcriptFingerprint,
    chaptersJson: null,
    debugJson: null,
    resolvedBoundaryCount: 0,
    totalBoundaryCount: 0,
    error: null,
  });

  try {
    let transcriptWords: TranscriptWord[];
    let plans: TranscriptChunkPlan[] = [];
    let chunkWordCounts: number[] = [];
    let transcriptSource: "new" | "cached" = "cached";
    let transcriptPayload: StoredTranscriptPayload | null = existingTranscriptPayload;

    if (hasCachedTranscript && existingTranscriptPayload) {
      transcriptWords = transcriptWordsFromStoredPayload(existingTranscriptPayload);
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
      const ebookAsset = payload.ebookAssetId ? ctx.repo.getAssetWithFiles(payload.ebookAssetId) : null;
      const ebookFile = ebookAsset?.asset.kind === "ebook" && ebookAsset.asset.mime === "application/epub+zip" ? ebookAsset.files[0] : null;
      const glossary = await loadGlossary(ctx, resolvedDeps, ebookFile?.path ?? null);
      const transcript = await transcribeAssetWithDeps(ctx, target.asset, target.files, book, settings, resolvedDeps, job, glossary);
      transcriptWords = transcript.transcriptWords;
      plans = transcript.plans;
      chunkWordCounts = transcript.chunkWordCounts;
      transcriptSource = "new";
      transcriptPayload = storedTranscriptPayload(transcriptWords, plans);
    }

    if (!transcriptPayload) {
      throw new Error("Transcript payload was not available");
    }
    let transcriptPath = existingTranscriptRow?.transcript_path ?? null;
    if (transcriptSource === "new" || !transcriptPath) {
      const transcriptJson = JSON.stringify(transcriptPayload);
      transcriptPath = await persistTranscriptArtifact(target.asset.id, transcriptFingerprint, transcriptJson);
      if (existingTranscriptRow?.transcript_path && existingTranscriptRow.transcript_path !== transcriptPath) {
        await rm(existingTranscriptRow.transcript_path, { force: true });
      }
      ctx.repo.upsertAssetTranscript({
        assetId: target.asset.id,
        status: "succeeded",
        source: CHAPTER_ANALYSIS_SOURCE,
        algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
        fingerprint: transcriptFingerprint,
        transcriptPath,
        transcriptJson: null,
        error: null,
      });
    }
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "succeeded",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: transcriptFingerprint,
      transcriptFingerprint,
      chaptersJson: null,
      debugJson: JSON.stringify({
        transcriptSource,
        chunkCount: plans.length,
        chunkWordCounts,
        transcriptWordCount: transcriptWords.length,
        model: TIMESTAMP_TRANSCRIPTION_MODEL,
      }),
      resolvedBoundaryCount: 0,
      totalBoundaryCount: 0,
      error: null,
    });
    ctx.repo.markJobSucceeded(job.id);
    const totalJobMs = Math.round((performance.now() - jobStartedAt) * 100) / 100;
    log(
      ctx,
      `[chapter-analysis] job=${job.id} asset=${target.asset.id} chunks=${plans.length} words=${transcriptWords.length} source=${transcriptSource} success=1 total_ms=${totalJobMs}`
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
        transcriptPath: transcript?.transcript_path ?? null,
        transcriptJson: transcript?.transcript_json ?? null,
        error: message,
      });
    }
    ctx.repo.upsertChapterAnalysis({
      assetId: target.asset.id,
      status: "failed",
      source: CHAPTER_ANALYSIS_SOURCE,
      algorithmVersion: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      fingerprint: transcriptFingerprint,
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

export { CHAPTERS_API_VERSION, CHAPTER_ANALYSIS_ALGORITHM_VERSION };
export type { StoredTranscriptPayload, TranscriptWord, TranscriptChunkPlan, EpubChapterEntry };
