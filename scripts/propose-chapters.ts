#!/usr/bin/env bun
import { spawnSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { loadEpubEntries } from "../src/library/chapter-analysis";
import {
  proposeChapterMarkers,
  wordsToTranscriptUtterances,
  type RawAudioChapter,
  type TranscriptUtterance,
  type TranscriptWord,
} from "../src/library/chapter-markers";

type TranscriptPayload = {
  utterances?: unknown;
  words?: unknown;
};

type CliOptions = {
  epub: string;
  transcript: string;
  audio?: string;
  rawChapters?: string;
  assetFiles?: string;
  out?: string;
  report?: string;
};

function usage(): never {
  console.error(`Usage:
  bun run propose-chapters -- --epub <book.epub> --transcript <transcript.json> (--audio <audio.m4a> | --raw-chapters <chapters.json> | --asset-files <asset-files.json>) [--out chapters.json] [--report report.md]`);
  process.exit(1);
}

function parseArgs(argv: string[]): CliOptions {
  const options: Partial<CliOptions> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) usage();
    if (arg === "--epub") options.epub = next;
    else if (arg === "--transcript") options.transcript = next;
    else if (arg === "--audio") options.audio = next;
    else if (arg === "--raw-chapters") options.rawChapters = next;
    else if (arg === "--asset-files") options.assetFiles = next;
    else if (arg === "--out") options.out = next;
    else if (arg === "--report") options.report = next;
    else usage();
    i += 1;
  }
  const audioSources = [options.audio, options.rawChapters, options.assetFiles].filter(Boolean);
  if (!options.epub || !options.transcript || audioSources.length !== 1) usage();
  return options as CliOptions;
}

function toUtterance(value: unknown): TranscriptUtterance | null {
  if (!value || typeof value !== "object") return null;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.startMs !== "number" || typeof candidate.endMs !== "number" || typeof candidate.text !== "string") {
    return null;
  }
  return {
    startMs: candidate.startMs,
    endMs: candidate.endMs,
    text: candidate.text,
  };
}

function toWord(value: unknown): TranscriptWord | null {
  if (!value || typeof value !== "object") return null;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.startMs !== "number" || typeof candidate.endMs !== "number" || typeof candidate.text !== "string") {
    return null;
  }
  return {
    startMs: candidate.startMs,
    endMs: candidate.endMs,
    text: candidate.text,
  };
}

async function loadTranscriptUtterances(transcriptPath: string): Promise<TranscriptUtterance[]> {
  const payload = (await Bun.file(transcriptPath).json()) as TranscriptPayload;
  if (Array.isArray(payload.utterances)) {
    const utterances = payload.utterances.map(toUtterance).filter((utterance): utterance is TranscriptUtterance => utterance !== null);
    if (utterances.length > 0) return utterances;
  }
  if (Array.isArray(payload.words)) {
    const words = payload.words.map(toWord).filter((word): word is TranscriptWord => word !== null);
    const utterances = wordsToTranscriptUtterances(words);
    if (utterances.length > 0) return utterances;
  }
  throw new Error(`Transcript has no usable utterances or timestamped words: ${transcriptPath}`);
}

function loadEmbeddedChapters(audioPath: string): RawAudioChapter[] {
  const result = spawnSync("ffprobe", ["-v", "error", "-print_format", "json", "-show_chapters", audioPath], {
    encoding: "utf8",
  });
  if (result.error || result.status !== 0) {
    const detail = result.error ? result.error.message : result.stderr || String(result.status);
    throw new Error(`ffprobe failed for ${audioPath}: ${detail}`);
  }
  const parsed = JSON.parse(result.stdout) as { chapters?: Array<Record<string, unknown>> };
  const chapters = parsed.chapters ?? [];
  return chapters.map((chapter, index) => {
    const tags = chapter.tags && typeof chapter.tags === "object" ? (chapter.tags as Record<string, unknown>) : {};
    const title = typeof tags.title === "string" ? tags.title : typeof tags.TITLE === "string" ? tags.TITLE : `Chapter ${index + 1}`;
    return {
      startMs: Math.max(0, Math.round(Number.parseFloat(String(chapter.start_time ?? "0")) * 1000)),
      endMs: Math.max(0, Math.round(Number.parseFloat(String(chapter.end_time ?? "0")) * 1000)),
      title,
    };
  });
}

async function loadRawChapters(chaptersPath: string): Promise<RawAudioChapter[]> {
  const payload = await Bun.file(chaptersPath).json();
  const chapters = Array.isArray(payload) ? payload : Array.isArray(payload?.chapters) ? payload.chapters : null;
  if (!chapters) throw new Error(`Raw chapter file must be an array or { chapters: [...] }: ${chaptersPath}`);
  return chapters
    .map((value: unknown): RawAudioChapter | null => {
      if (!value || typeof value !== "object") return null;
      const chapter = value as Record<string, unknown>;
      if (typeof chapter.startMs !== "number") return null;
      return {
        startMs: chapter.startMs,
        endMs: typeof chapter.endMs === "number" ? chapter.endMs : undefined,
        title: typeof chapter.title === "string" ? chapter.title : undefined,
      };
    })
    .filter((chapter): chapter is RawAudioChapter => chapter !== null);
}

async function loadAssetFileBoundaries(assetFilesPath: string): Promise<RawAudioChapter[]> {
  const payload = await Bun.file(assetFilesPath).json();
  const files = Array.isArray(payload) ? payload : Array.isArray(payload?.files) ? payload.files : null;
  if (!files) throw new Error(`Asset file boundary input must be an array or { files: [...] }: ${assetFilesPath}`);
  let cursor = 0;
  return files
    .map((value: unknown, index: number): RawAudioChapter | null => {
      if (!value || typeof value !== "object") return null;
      const file = value as Record<string, unknown>;
      const durationMs = typeof file.durationMs === "number" ? file.durationMs : typeof file.duration_ms === "number" ? file.duration_ms : null;
      if (durationMs === null) return null;
      const startMs = cursor;
      cursor += durationMs;
      return {
        startMs,
        endMs: cursor,
        title: typeof file.title === "string" && file.title.trim() ? file.title : `Part ${index + 1}`,
      };
    })
    .filter((chapter): chapter is RawAudioChapter => chapter !== null);
}

function formatTimestamp(seconds: number): string {
  const totalTenths = Math.round(seconds * 10);
  const total = Math.floor(totalTenths / 10);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const tenths = totalTenths % 10;
  const base = h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`;
  return `${base}.${tenths}`;
}

function markdownReport(options: CliOptions, report: ReturnType<typeof proposeChapterMarkers>): string {
  const lines: string[] = [];
  lines.push("# Chapter Marker Proposal");
  lines.push("");
  lines.push("## Inputs");
  lines.push("");
  lines.push(`- EPUB: \`${options.epub}\``);
  lines.push(`- Transcript: \`${options.transcript}\``);
  if (options.audio) lines.push(`- Audio: \`${options.audio}\``);
  if (options.rawChapters) lines.push(`- Raw chapters: \`${options.rawChapters}\``);
  if (options.assetFiles) lines.push(`- Asset files: \`${options.assetFiles}\``);
  lines.push(`- EPUB major headings: ${report.epubHeadings.length}`);
  lines.push(`- Embedded audio chapters: ${report.embeddedChapterCount}`);
  lines.push(`- Transcript utterances: ${report.transcriptUtteranceCount}`);
  lines.push("");
  lines.push("## Proposed Chapters");
  lines.push("");
  lines.push("| Time | Seconds | Title | Confidence | Reason |");
  lines.push("| --- | ---: | --- | --- | --- |");
  for (const chapter of report.chapters) {
    lines.push(
      `| ${formatTimestamp(chapter.startTime)} | ${chapter.startTime.toFixed(2)} | ${chapter.title} | ${chapter.confidence} | ${chapter.reason} |`
    );
  }
  lines.push("");
  return lines.join("\n");
}

async function writeJson(filePath: string, value: unknown): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const [epubEntries, transcriptUtterances] = await Promise.all([
    loadEpubEntries(options.epub),
    loadTranscriptUtterances(options.transcript),
  ]);
  const embeddedChapters = options.audio
    ? loadEmbeddedChapters(options.audio)
    : options.rawChapters
      ? await loadRawChapters(options.rawChapters)
      : await loadAssetFileBoundaries(options.assetFiles!);
  const report = proposeChapterMarkers({
    epubEntries,
    transcriptUtterances,
    embeddedChapters,
  });
  const reportMarkdown = markdownReport(options, report);
  console.log(reportMarkdown);
  if (options.out) {
    await writeJson(options.out, {
      version: "1.2.0",
      chapters: report.chapters.map((chapter) => ({
        startTime: chapter.startTime,
        title: chapter.title,
      })),
    });
  }
  if (options.report) {
    await mkdir(path.dirname(options.report), { recursive: true });
    await writeFile(options.report, reportMarkdown, "utf8");
  }
}

await main();
