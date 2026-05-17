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
    return { matches };
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
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

export type SubmittedChapter = z.infer<typeof submittedChapterSchema>;
export type SubmitChapterPlanInput = z.infer<typeof submitChapterPlanSchema>;

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

function chapterTitleKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function textTokens(value: string): string[] {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 4);
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
    modelSettings: {
      toolChoice: "required",
      parallelToolCalls: false,
    },
    resetToolChoice: false,
    instructions: [
      "You curate audiobook chapter markers from EPUB structure, embedded audio chapters, and transcript evidence.",
      "You must use tools to inspect the available evidence. Embedded audio chapters are evidence, not truth; equal divisions and generic labels are suspicious.",
      "Use submitChapterPlan sparingly. It is the final validation gate, not a search or brainstorming tool.",
      "Before your first submitChapterPlan call, inspect EPUB structure, embedded audio chapters, and transcript evidence with rgSearchTranscript, fuzzySearchTranscript, or getTranscriptWindow.",
      "For EPUB-rich books, build the plan from transcript evidence. Estimate tools and embedded boundaries are only priors; they are not enough evidence for an EPUB-heading claim.",
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
        description: "Search transcript utterances with ripgrep. Use regex=false for literal phrases and regex=true for regular expressions.",
        parameters: rgSearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return rgSearchTranscript(ctx, input);
        },
      }),
      tool({
        name: "fuzzySearchTranscript",
        description: "Fuzzy-search transcript utterances for approximate chapter-heading or first-words matches.",
        parameters: fuzzySearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return fuzzySearchTranscript(ctx, input);
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
        description: "Return transcript utterances around a timestamp.",
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
    "Workflow requirement:",
    "1. Inspect EPUB structure and embedded audio chapters.",
    "2. Gather transcript evidence before submitting a plan. For an EPUB-rich book, search or window-check distinctive text for the chapter starts you intend to claim.",
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

export async function runAgenticChapterCurationDetailed(ctx: ChapterCurationContext): Promise<{
  result: SubmitChapterPlanResult | null;
  finalOutput: unknown;
  newItems: unknown[];
  rawResponses: unknown[];
}> {
  const apiKey = ctx.settings.agents.apiKey.trim();
  if (!apiKey) return { result: null, finalOutput: null, newItems: [], rawResponses: [] };
  const abort = new AbortController();
  const timeout = setTimeout(() => abort.abort(), Math.max(5_000, ctx.settings.agents.timeoutMs));
  const provider = new OpenAIProvider({ apiKey, useResponses: true });
  try {
    const runner = new Runner({
      modelProvider: provider,
      tracingDisabled: true,
      traceIncludeSensitiveData: false,
    });
    const result = await runner.run(createChapterCuratorAgent(ctx), chapterCuratorPrompt(ctx), {
      maxTurns: 64,
      signal: abort.signal,
      toolExecution: { maxFunctionToolConcurrency: 4 },
    });
    return {
      result: parseSubmitToolOutput(result.finalOutput),
      finalOutput: result.finalOutput,
      newItems: result.newItems as unknown[],
      rawResponses: result.rawResponses as unknown[],
    };
  } finally {
    clearTimeout(timeout);
    await provider.close().catch(() => undefined);
  }
}
