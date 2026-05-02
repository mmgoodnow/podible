#!/usr/bin/env bun
import { spawnSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { loadEpubEntries } from "../src/library/chapter-analysis";
import { proposeChapterMarkers, type RawAudioChapter, type TranscriptUtterance } from "../src/library/chapter-markers";

type TranscriptPayload = {
  utterances?: unknown;
};

type CliOptions = {
  epub: string;
  transcript: string;
  audio: string;
  out?: string;
  report?: string;
};

function usage(): never {
  console.error(`Usage:
  bun run propose-chapters -- --epub <book.epub> --transcript <transcript.json> --audio <audio.m4a> [--out chapters.json] [--report report.md]`);
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
    else if (arg === "--out") options.out = next;
    else if (arg === "--report") options.report = next;
    else usage();
    i += 1;
  }
  if (!options.epub || !options.transcript || !options.audio) usage();
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

async function loadTranscriptUtterances(transcriptPath: string): Promise<TranscriptUtterance[]> {
  const payload = (await Bun.file(transcriptPath).json()) as TranscriptPayload;
  if (!Array.isArray(payload.utterances)) {
    throw new Error(`Transcript does not contain an utterances array: ${transcriptPath}`);
  }
  const utterances = payload.utterances.map(toUtterance).filter((utterance): utterance is TranscriptUtterance => utterance !== null);
  if (utterances.length === 0) {
    throw new Error(`Transcript has no usable utterances: ${transcriptPath}`);
  }
  return utterances;
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
  lines.push(`- Audio: \`${options.audio}\``);
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
  const embeddedChapters = loadEmbeddedChapters(options.audio);
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
