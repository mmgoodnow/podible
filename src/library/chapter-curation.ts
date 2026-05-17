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

export type ChapterCurationContext = {
  book: BookRow;
  manifestation: ManifestationRow;
  containers: ChapterCurationContainer[];
  settings: AppSettings;
  durationMs: number;
  epubEntries: EpubChapterEntry[];
  transcript: StoredTranscriptPayload;
  embeddedChapters: ChapterCurationTiming[];
};

export type TranscriptWindow = {
  startMs: number;
  endMs: number;
  utterances: StoredTranscriptUtterance[];
  text: string;
};

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
  ctx: Pick<ChapterCurationContext, "transcript" | "durationMs">,
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
