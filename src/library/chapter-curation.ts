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

export type ChapterCurationContext = {
  book: BookRow;
  manifestation: ManifestationRow;
  containers: ChapterCurationContainer[];
  settings: AppSettings;
  durationMs: number;
  epubEntries: EpubChapterEntry[];
  transcript: StoredTranscriptPayload;
  embeddedChapters: ChapterCurationTiming[];
  debugEventLogPath?: string;
  debugTraceDir?: string;
};

export type ChapterCurationSpan = {
  epubStartIndex: number;
  epubEndIndex: number;
  startTime: number;
  endTime: number;
  depth: number;
  path: string;
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
    const orderedBonus = distinctQueryTokens.reduce((score, token, tokenIndex) => {
      const foundIndex = window.findIndex((word) => (word.token || textTokens(word.text)[0]) === token);
      return foundIndex >= 0 && foundIndex <= tokenIndex * 6 + 12 ? score + 0.25 : score;
    }, 0);
    candidates.push({
      startTime: msToSeconds(window[0]!.startMs),
      endTime: msToSeconds(window.at(-1)!.endMs),
      text: normalizeToolText(window.map((word) => word.text).join(" ")),
      afterText: normalizeToolText(getTranscriptWindowFromContext(ctx, window[0]!.startMs, 20_000).text).slice(0, 500),
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
    const firstWords = summarizeFirstWords(entry, 18);
    const queryTokens = distinctFirstWordTokens(firstWords);
    const query = queryTokens.length > 0 ? queryTokens.slice(0, 10).join(" ") : firstWords || entry.title;
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
  reason: z.string().trim().min(1),
  concerns: z.array(z.string().trim().min(1)).max(10),
  suggestedStartTime: z.number().finite().nonnegative().nullable(),
  suggestedEpubNodeId: z.string().trim().min(1).nullable(),
});

export type SubmittedChapter = z.infer<typeof submittedChapterSchema>;
export type SubmitChapterPlanInput = z.infer<typeof submitChapterPlanSchema>;
export type SubmitLeafChapterPlanInput = z.infer<typeof submitLeafChapterPlanSchema>;
export type SubmitFulcrumSplitInput = z.infer<typeof submitFulcrumSplitSchema>;
export type SubmitFulcrumJudgmentInput = z.infer<typeof submitFulcrumJudgmentSchema>;

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

function spanDurationSeconds(span: ChapterCurationSpan): number {
  return Math.max(0, span.endTime - span.startTime);
}

function childSpanPath(parent: ChapterCurationSpan, side: "L" | "R"): string {
  return parent.path === "root" ? side : `${parent.path}${side}`;
}

function splitSpan(span: ChapterCurationSpan, splitIndex: number, splitTime: number): { left: ChapterCurationSpan; right: ChapterCurationSpan } {
  return {
    left: {
      epubStartIndex: span.epubStartIndex,
      epubEndIndex: splitIndex - 1,
      startTime: span.startTime,
      endTime: splitTime,
      depth: span.depth + 1,
      path: childSpanPath(span, "L"),
    },
    right: {
      epubStartIndex: splitIndex,
      epubEndIndex: span.epubEndIndex,
      startTime: splitTime,
      endTime: span.endTime,
      depth: span.depth + 1,
      path: childSpanPath(span, "R"),
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

  const firstWords = entry ? summarizeFirstWords(entry, 28) : "";
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
  if (nearestCandidateDelta !== null && nearestCandidateDelta > 90) {
    const nearest = candidateMatches.reduce((best, match) =>
      Math.abs(match.startTime - split.startTime) < Math.abs(best.startTime - split.startTime) ? match : best
    );
    errors.push(
      `Submitted fulcrum is ${Math.round(nearestCandidateDelta)}s from the nearest transcript evidence candidate; use the candidate near ${Math.round(nearest.startTime)}s or find stronger local evidence.`
    );
  }

  if (errors.length > 0) {
    return {
      accepted: false,
      kind: "split",
      errors,
      warnings,
      audit,
      instruction: "Pick a different internal fulcrum with stronger transcript/prose evidence, or submit a leaf plan for this span.",
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
    const evidenceTokens = distinctFirstWordTokens(heading.firstWords);
    if (evidenceTokens.length > 0) {
      const transcriptTokens = new Set(textTokens(audit[index]?.transcriptAfterStart ?? ""));
      const overlap = evidenceTokens.filter((token) => transcriptTokens.has(token));
      if (overlap.length < Math.min(2, evidenceTokens.length)) {
        errors.push(`chapters[${index}] has weak transcript evidence for claimed EPUB heading "${heading.title}"`);
      }
    }
  }

  const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
  const claimedIndexes = claimedEpubIndexes(ctx.epubEntries, plan.chapters).filter((index) => spanContainsEpubIndex(span, index));
  const spanDurationSeconds = span.endTime - span.startTime;
  if (!options.forceLeaf && (spanNodeCount > 8 || spanDurationSeconds > 2 * 60 * 60)) {
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

export async function resolveRecursiveChapterSpans(
  ctx: ChapterCurationContext,
  decide: (span: ChapterCurationSpan, forceLeaf: boolean) => Promise<RecursiveSpanDecision | null>,
  options: { maxDepth?: number; maxCalls?: number; maxConcurrency?: number; reports?: RecursiveCurationReport[] } = {}
): Promise<SubmittedChapter[] | null> {
  const maxDepth = options.maxDepth ?? 5;
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
    const mustLeaf = forceLeaf || span.depth >= maxDepth || calls >= maxCalls - 1;
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
    const { left, right } = splitSpan(span, decision.split.epubIndex, decision.split.startTime);
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
  return [
    `Curate chapter markers for span ${span.path} of "${ctx.book.title}" by ${ctx.book.author}.`,
    `manifestationId: ${ctx.manifestation.id}`,
    `spanPath: ${span.path}`,
    `spanDepth: ${span.depth}`,
    `spanTimeSeconds: ${span.startTime}..${span.endTime}`,
    `spanEpubIndexes: ${span.epubStartIndex}..${span.epubEndIndex}`,
    `spanEpubNodeCount: ${entries.length}`,
    forceLeaf
      ? "You are forced to submit a leaf chapter plan for this span. Do not call submitFulcrumSplit."
      : allowLeaf
        ? "Choose exactly one outcome: submitLeafChapterPlan if this span is locally solvable, or submitFulcrumSplit if this span should be divided."
        : "This span is too broad for a leaf plan. You must call submitFulcrumSplit with a validated internal boundary.",
    "For a fulcrum, pick a high-confidence internal EPUB node boundary with transcript prose evidence near the timestamp.",
    allowLeaf ? "For a leaf, submit only chapter starts inside this span and include epubNodeId for every EPUB-backed chapter." : "",
    "Prefer submitFulcrumSplit for spans with more than 8 EPUB nodes or more than 2 hours duration unless the whole span is already strongly evidenced.",
    "All times are seconds.",
  ].filter(Boolean).join("\n");
}

export function recursiveSpanAllowsLeaf(span: ChapterCurationSpan, forceLeaf: boolean): boolean {
  const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
  const spanDurationSeconds = span.endTime - span.startTime;
  return forceLeaf || (spanNodeCount <= 8 && spanDurationSeconds <= 2 * 60 * 60);
}

function createFulcrumJudgeAgent(ctx: ChapterCurationContext): Agent {
  return new Agent({
    name: "FulcrumJudge",
    model: ctx.settings.agents.model,
    modelSettings: {
      toolChoice: "required",
      parallelToolCalls: false,
    },
    resetToolChoice: false,
    instructions: [
      "You are a strict reviewer for audiobook chapter split points.",
      "Your job is to decide whether the proposed fulcrum timestamp is actually the start of the proposed EPUB node.",
      "Reject a split when the transcript window is only generic overlap, when a listed candidate is clearly better, or when the window starts inside the previous chapter.",
      "Accept only when distinctive title/prose evidence appears at or immediately after the proposed timestamp.",
      "Do not invent a full chapter plan. Judge only this proposed split.",
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
    "Prefer rejecting over accepting a suspicious split; recursion can try another fulcrum.",
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

function createRecursiveSpanCuratorAgent(ctx: ChapterCurationContext, span: ChapterCurationSpan, forceLeaf: boolean): Agent {
  let invalidFulcrums = 0;
  let rejectedLeafRequiresEvidence = false;
  let evidenceCallsSinceRejectedLeaf = 0;
  const allowLeaf = recursiveSpanAllowsLeaf(span, forceLeaf);
  const rejectedFulcrums = new Set<string>();

  function markEvidenceToolUsed(): void {
    if (rejectedLeafRequiresEvidence) evidenceCallsSinceRejectedLeaf++;
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

  return new Agent({
    name: "SectionChapterCurator",
    model: ctx.settings.agents.model,
    modelSettings: {
      toolChoice: "required",
      parallelToolCalls: false,
    },
    resetToolChoice: false,
    instructions: [
      "You curate audiobook chapter markers for one bounded span, not the whole book.",
      "You must either submit a leaf chapter plan or propose one validated fulcrum split.",
      "Use submitFulcrumSplit when the span is broad and you can identify a strong internal boundary.",
      allowLeaf
        ? "Use submitLeafChapterPlan when the span is small enough or already well evidenced."
        : "This span is too broad for a leaf plan. The submitLeafChapterPlan tool is intentionally unavailable; you must find a fulcrum split.",
      "Do not submit guessed timestamps. Use transcript evidence tools first.",
      "All tool times and submitted startTime values are seconds, not milliseconds.",
      forceLeaf ? "This span is forced leaf mode. You must call submitLeafChapterPlan, not submitFulcrumSplit." : "",
    ]
      .filter(Boolean)
      .join("\n"),
    tools: [
      tool({
        name: "getEpubStructure",
        description: "Return ordered EPUB nodes for this span only.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => {
          const full = getEpubStructure(ctx);
          return {
            ...full,
            nodes: full.nodes.filter((node) => node.index >= span.epubStartIndex && node.index <= span.epubEndIndex),
          };
        },
      }),
      tool({
        name: "getEmbeddedAudioChapters",
        description: "Return embedded audio chapter boundaries and diagnostics for context.",
        parameters: emptyToolSchema,
        strict: true,
        execute: () => {
          const result = getEmbeddedAudioChapters(ctx);
          return {
            ...result,
            chapters: result.chapters.filter((chapter) => chapter.endTime >= span.startTime && chapter.startTime <= span.endTime),
          };
        },
      }),
      tool({
        name: "findEpubChapterEvidence",
        description: "Batch fuzzy-search transcript evidence for EPUB chapter nodes in this span. Times are seconds.",
        parameters: findEpubChapterEvidenceSchema,
        strict: true,
        execute: async (input) => {
          markEvidenceToolUsed();
          const allowedIds = new Set(spanEpubEntries(ctx, span).map((entry) => entry.id));
          const nodeIds = (input.nodeIds?.length ? input.nodeIds : Array.from(allowedIds)).filter((id) => allowedIds.has(id));
          const result = await findEpubChapterEvidence(ctx, { ...input, nodeIds });
          return {
            nodes: result.nodes.map((node) => ({
              ...node,
              matches: node.matches.filter((match) => match.endTime >= span.startTime - 300 && match.startTime <= span.endTime + 300),
            })),
          };
        },
      }),
      tool({
        name: "rgSearchTranscript",
        description: "Search transcript utterances with ripgrep inside this span. Time scopes are seconds.",
        parameters: rgSearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return rgSearchTranscript(ctx, { ...input, scope: spanScope(span, input.scope) });
        },
      }),
      tool({
        name: "fuzzySearchTranscript",
        description: "Fuzzy-search transcript utterances inside this span. Time scopes are seconds.",
        parameters: fuzzySearchTranscriptSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return fuzzySearchTranscript(ctx, { ...input, scope: spanScope(span, input.scope) });
        },
      }),
      tool({
        name: "getTranscriptWindow",
        description: "Return transcript utterances around a timestamp inside this span. startTime is seconds.",
        parameters: getTranscriptWindowSchema,
        strict: true,
        execute: (input) => {
          markEvidenceToolUsed();
          return getTranscriptWindow(ctx, { ...input, startTime: Math.min(span.endTime, Math.max(span.startTime, input.startTime)) });
        },
      }),
      tool({
        name: "submitFulcrumSplit",
        description: "Submit a proposed internal split boundary for this span. Validation returns feedback if rejected.",
        parameters: submitFulcrumSplitSchema,
        strict: true,
        execute: async (input) => {
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
            return {
              accepted: false,
              kind: "split",
              errors: [`Fulcrum ${input.epubNodeId} near ${Math.round(input.startTime)}s was already rejected for this span.`],
              warnings: [],
              audit: null,
              instruction: "Pick a different EPUB node or a materially different timestamp with stronger transcript evidence.",
            } satisfies SubmitFulcrumSplitResult;
          }
          const result = await validateFulcrumSplit(ctx, span, input);
          if (!result.accepted) {
            invalidFulcrums++;
            rejectedFulcrums.add(rejectedKey);
          }
          if (result.accepted) {
            const judgment = await judgeFulcrumSplit(ctx, span, result);
            if (judgment && !judgment.accepted) {
              invalidFulcrums++;
              rejectedFulcrums.add(rejectedKey);
              return {
                accepted: false,
                kind: "split",
                errors: [
                  `Fulcrum judge rejected this split: ${judgment.reason}`,
                  ...judgment.concerns.map((concern) => `Judge concern: ${concern}`),
                ],
                warnings: [],
                audit: result.audit,
                instruction:
                  judgment.suggestedStartTime !== null
                    ? `Pick a stronger fulcrum. The judge suggested checking ${judgment.suggestedEpubNodeId ?? result.epubNodeId} near ${Math.round(judgment.suggestedStartTime)}s.`
                    : "Pick a different fulcrum with stronger local transcript evidence.",
              } satisfies SubmitFulcrumSplitResult;
            }
          }
          return result;
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
                if (rejectedLeafRequiresEvidence && evidenceCallsSinceRejectedLeaf === 0) return rejectedLeafWithoutEvidence();
                const result = validateLeafChapterPlan(ctx, span, input, { forceLeaf });
                rejectedLeafRequiresEvidence = !result.accepted;
                evidenceCallsSinceRejectedLeaf = 0;
                return result;
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
    const recursiveReports: RecursiveCurationReport[] = [];
    const recursiveSpanTraces: RecursiveSpanTrace[] = [];
    logChapterCurationEvent(ctx, {
      type: "recursive-run-start",
      message: "recursive run start=1",
      model: ctx.settings.agents.model,
      timeoutMs: ctx.settings.agents.timeoutMs,
      maxSpanConcurrency: 4,
      durationSeconds: ctx.durationMs / 1000,
      epubEntries: ctx.epubEntries.length,
      transcriptUtterances: ctx.transcript.utterances?.length ?? 0,
      embeddedChapters: ctx.embeddedChapters.length,
    });
    const recursiveChapters = await resolveRecursiveChapterSpans(
      ctx,
      async (span, forceLeaf) => {
        const spanNodeCount = span.epubEndIndex - span.epubStartIndex + 1;
        logChapterCurationEvent(ctx, {
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
            logChapterCurationEvent(ctx, {
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
            const spanResult = await runner.run(createRecursiveSpanCuratorAgent(ctx, span, forceLeaf), spanPrompt(ctx, span, forceLeaf), {
              maxTurns: forceLeaf ? 24 : 64,
              signal: abort.signal,
              toolExecution: { maxFunctionToolConcurrency: 4 },
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
            const tracePath = writeChapterCurationTrace(ctx, `span-${span.path}-attempt-${attempt + 1}-${decision?.kind ?? "none"}`, tracePayload);
            recursiveSpanTraces.push({
              path: span.path,
              depth: span.depth,
              forceLeaf,
              finalOutput: spanResult.finalOutput,
              newItems: spanResult.newItems as unknown[],
              rawResponses: spanResult.rawResponses as unknown[],
            });
            logChapterCurationEvent(ctx, {
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
            const tracePath = writeChapterCurationTrace(ctx, `span-${span.path}-attempt-${attempt + 1}-error`, {
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
            logChapterCurationEvent(ctx, {
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
      { maxDepth: 5, maxCalls: 64, maxConcurrency: 4, reports: recursiveReports }
    );
    if (recursiveChapters && recursiveChapters.length > 0) {
      logChapterCurationEvent(ctx, {
        type: "recursive-merge-start",
        message: `recursive merge chapters=${recursiveChapters.length} validate=1`,
        chapters: recursiveChapters.length,
        chapterPlan: summarizeSubmittedChapterObjects(recursiveChapters, 80),
      });
      const recursiveResult = submitChapterPlan(ctx, {
        manifestationId: ctx.manifestation.id,
        strategy: "Recursive fulcrum chapter curation",
        chapters: recursiveChapters,
        notes: "Merged from recursively curated span plans.",
      });
      if (recursiveResult.accepted) {
        logChapterCurationEvent(ctx, {
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
      logChapterCurationEvent(ctx, {
        type: "recursive-merge-rejected",
        message: `recursive merge accepted=0 chapters=${recursiveChapters.length} errors=${JSON.stringify(recursiveResult.errors.slice(0, 5))}`,
        chapters: recursiveChapters.length,
        errors: recursiveResult.errors,
        warnings: recursiveResult.warnings,
        audit: recursiveResult.audit,
      });
      recursiveReports.push({
        ...createRootCurationSpan(ctx),
        forceLeaf: false,
        outcome: "failed",
        errors: recursiveResult.errors,
        chapters: recursiveChapters.length,
      });
    } else {
      logChapterCurationEvent(ctx, {
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
