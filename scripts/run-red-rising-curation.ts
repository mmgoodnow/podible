#!/usr/bin/env bun
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import type { AssetFileRow, AssetRow, BookRow, ManifestationRow } from "../src/app-types";
import { loadEpubEntries, type StoredTranscriptPayload } from "../src/library/chapter-analysis";
import {
  runNodeParallelAgenticChapterCurationDetailed,
  runRecursiveAgenticChapterCurationDetailed,
  type ChapterCurationTiming,
} from "../src/library/chapter-curation";
import { defaultSettings } from "../src/settings";

type Metadata = {
  book: BookRow;
  manifestations: ManifestationRow[];
  assets: AssetRow[];
  files: AssetFileRow[];
  transcripts: Array<{
    asset_id: number;
    transcript_path: string;
  }>;
};

const caseDir = path.resolve("tmp/chapter-cases/red-rising/prod");
const runId = new Date().toISOString().replace(/[:.]/g, "-");
const mode = (process.env.RED_RISING_MODE?.trim() || "recursive") as "recursive" | "node";
if (mode !== "recursive" && mode !== "node") throw new Error("RED_RISING_MODE must be recursive or node");
const eventLogPath = path.join(caseDir, `${mode}-agent-events-m59-${runId}.jsonl`);
const traceDir = path.join(caseDir, `${mode}-agent-traces-m59-${runId}`);
const resultPath = path.join(caseDir, `${mode}-agent-result-m59-${runId}.json`);
const errorPath = path.join(caseDir, `${mode}-agent-error-m59-${runId}.json`);
const baseModel = process.env.RED_RISING_MODEL?.trim() || "gpt-5.4-mini";
const curatorModel = process.env.RED_RISING_CURATOR_MODEL?.trim() || baseModel;
const judgeModel = process.env.RED_RISING_JUDGE_MODEL?.trim() || baseModel;

function localCasePath(originalPath: string): string {
  return path.join(caseDir, originalPath.replace(/^\//, ""));
}

async function loadJson<T>(filePath: string): Promise<T> {
  return (await Bun.file(filePath).json()) as T;
}

function offsetTranscript(payload: StoredTranscriptPayload, offsetMs: number): StoredTranscriptPayload {
  return {
    ...payload,
    words: payload.words.map((word) => ({
      ...word,
      startMs: word.startMs + offsetMs,
      endMs: word.endMs + offsetMs,
    })),
    utterances: payload.utterances?.map((utterance) => ({
      ...utterance,
      startMs: utterance.startMs + offsetMs,
      endMs: utterance.endMs + offsetMs,
    })),
    chunks: payload.chunks?.map((chunk) => ({
      ...chunk,
      startMs: chunk.startMs + offsetMs,
    })),
  };
}

async function main(): Promise<void> {
  await mkdir(traceDir, { recursive: true });
  const metadata = await loadJson<Metadata>(path.join(caseDir, "metadata.json"));
  const manifestation = metadata.manifestations.find((item) => item.id === 59);
  if (!manifestation) throw new Error("Missing manifestation 59");

  const audioAssets = metadata.assets
    .filter((asset) => asset.manifestation_id === manifestation.id)
    .sort((a, b) => a.sequence_in_manifestation - b.sequence_in_manifestation);
  const containers = audioAssets.map((asset) => ({
    asset,
    files: metadata.files.filter((file) => file.asset_id === asset.id),
  }));
  const ebookFile = metadata.files.find((file) => file.asset_id === 64);
  if (!ebookFile) throw new Error("Missing EPUB file metadata");

  const epubEntries = await loadEpubEntries(localCasePath(ebookFile.path));
  const transcriptPieces: StoredTranscriptPayload[] = [];
  let transcriptOffset = 0;
  for (const asset of audioAssets) {
    const transcriptMeta = metadata.transcripts.find((item) => item.asset_id === asset.id);
    if (!transcriptMeta) throw new Error(`Missing transcript metadata for asset ${asset.id}`);
    const payload = await loadJson<StoredTranscriptPayload>(localCasePath(transcriptMeta.transcript_path));
    transcriptPieces.push(offsetTranscript(payload, transcriptOffset));
    transcriptOffset += asset.duration_ms ?? 0;
  }

  const transcript: StoredTranscriptPayload = {
    version: "combined-red-rising-case",
    text: transcriptPieces.map((payload) => payload.text).join("\n"),
    words: transcriptPieces.flatMap((payload) => payload.words),
    utterances: transcriptPieces.flatMap((payload) => payload.utterances ?? []),
    chunks: transcriptPieces.flatMap((payload) => payload.chunks ?? []),
  };

  const embeddedChapters: ChapterCurationTiming[] = [
    { id: "70-ch0", title: "Part 01", startMs: 0, endMs: 4_395_000 },
    { id: "70-ch1", title: "Part 02", startMs: 4_395_000, endMs: 8_993_000 },
    { id: "70-ch2", title: "Part 03", startMs: 8_993_000, endMs: 13_478_000 },
    { id: "70-ch3", title: "Part 04", startMs: 13_478_000, endMs: 18_007_000 },
    { id: "70-ch4", title: "Part 05", startMs: 18_007_000, endMs: 22_631_000 },
    { id: "70-ch5", title: "Part 06", startMs: 22_631_000, endMs: 27_104_235 },
    { id: "71-ch0", title: "Part 01", startMs: 27_104_235, endMs: 31_723_235 },
    { id: "71-ch1", title: "Part 02", startMs: 31_723_235, endMs: 36_356_235 },
    { id: "71-ch2", title: "Part 03", startMs: 36_356_235, endMs: 41_049_235 },
    { id: "71-ch3", title: "Part 04", startMs: 41_049_235, endMs: 45_688_235 },
    { id: "71-ch4", title: "Part 05", startMs: 45_688_235, endMs: 50_197_235 },
    { id: "71-ch5", title: "Part 06", startMs: 50_197_235, endMs: 54_246_235 },
    { id: "71-ch6", title: "Part 07", startMs: 54_246_235, endMs: 57_097_358 },
  ];

  try {
    const runCuration = mode === "node" ? runNodeParallelAgenticChapterCurationDetailed : runRecursiveAgenticChapterCurationDetailed;
    const startedAt = Date.now();
    const result = await runCuration({
      book: metadata.book,
      manifestation,
      containers,
      settings: defaultSettings({
        agents: {
          apiKey: process.env.OPENAI_API_KEY ?? "",
          model: baseModel,
          timeoutMs: 1_800_000,
        },
      }),
      durationMs: manifestation.duration_ms ?? transcriptOffset,
      epubEntries,
      transcript,
      embeddedChapters,
      debugEventLogPath: eventLogPath,
      debugTraceDir: traceDir,
      debugReasoningSummary: "detailed",
      debugReasoningEffort: "medium",
      debugCuratorModel: curatorModel,
      debugJudgeModel: judgeModel,
    });
    const elapsedMs = Date.now() - startedAt;
    const output = { ok: true, mode, resultPath, eventLogPath, traceDir, accepted: result.result?.accepted ?? false, chapters: result.result?.accepted ? result.result.chapters.length : 0, elapsedMs, model: baseModel, curatorModel, judgeModel };
    await writeFile(resultPath, `${JSON.stringify({ ...result, mode, elapsedMs, debugModels: { model: baseModel, curatorModel, judgeModel } }, null, 2)}\n`, "utf8");
    console.log(JSON.stringify(output, null, 2));
  } catch (error) {
    const payload = {
      name: (error as Error).name,
      message: (error as Error).message,
      stack: (error as Error).stack,
    };
    await writeFile(errorPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    console.error(JSON.stringify({ ok: false, mode, errorPath, eventLogPath, traceDir, error: payload }, null, 2));
    process.exitCode = 1;
  }
}

await main();
