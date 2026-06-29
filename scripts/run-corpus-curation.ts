#!/usr/bin/env bun
import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { loadEpubEntries, type StoredTranscriptPayload } from "../src/library/chapter-analysis";
import {
  runNodeParallelAgenticChapterCurationDetailed,
  runRecursiveAgenticChapterCurationDetailed,
  type ChapterCurationTiming,
} from "../src/library/chapter-curation";
import { defaultSettings } from "../src/settings";

const slug = process.env.CORPUS_SLUG?.trim() || process.argv[2];
if (!slug) throw new Error("Usage: CORPUS_SLUG=<slug> bun scripts/run-corpus-curation.ts");
const mode = (process.env.CORPUS_MODE?.trim() || "recursive") as "recursive" | "node";
if (mode !== "recursive" && mode !== "node") throw new Error("CORPUS_MODE must be recursive or node");

const caseDir = path.resolve("tmp/chapter-cases/corpus", slug);
const runId = new Date().toISOString().replace(/[:.]/g, "-");
const eventLogPath = path.join(caseDir, `${mode}-agent-events-${runId}.jsonl`);
const traceDir = path.join(caseDir, `${mode}-agent-traces-${runId}`);
const resultPath = path.join(caseDir, `${mode}-agent-result-${runId}.json`);
const errorPath = path.join(caseDir, `${mode}-agent-error-${runId}.json`);
const baseModel = process.env.CORPUS_MODEL?.trim() || "gpt-5.4-mini";
const curatorModel = process.env.CORPUS_CURATOR_MODEL?.trim() || "gpt-5.4-nano";
const judgeModel = process.env.CORPUS_JUDGE_MODEL?.trim() || baseModel;

function gitString(args: string[]): string | null {
  try {
    return execFileSync("git", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim() || null;
  } catch {
    return null;
  }
}

function gitProvenance(): { commit: string | null; branch: string | null; dirty: boolean | null } {
  const commit = gitString(["rev-parse", "HEAD"]);
  const branch = gitString(["branch", "--show-current"]);
  const status = gitString(["status", "--porcelain"]);
  return {
    commit,
    branch,
    dirty: status === null ? null : status.length > 0,
  };
}

function titleFromPath(filePath: string, fallback: string): string {
  const base = path.basename(filePath || "").replace(/\.[^.]+$/, "").trim();
  return base || fallback;
}

async function embeddedChaptersFromCase(): Promise<ChapterCurationTiming[]> {
  const raw = (await Bun.file(path.join(caseDir, "raw-chapters.json")).json().catch(() => ({ chapters: [] }))) as { chapters?: any[] };
  if (Array.isArray(raw.chapters) && raw.chapters.length > 0) {
    return raw.chapters.map((chapter, index) => ({
      id: `raw-${index}`,
      title: String(chapter.tags?.title ?? chapter.title ?? `Chapter ${index + 1}`),
      startMs: Math.round(Number(chapter.start_time ?? 0) * 1000) || Number(chapter.startMs ?? 0),
      endMs: Math.round(Number(chapter.end_time ?? 0) * 1000) || Number(chapter.endMs ?? 0),
    }));
  }

  const files = (await Bun.file(path.join(caseDir, "asset-files.json")).json().catch(() => [])) as any[];
  if (!Array.isArray(files) || files.length <= 1) return [];
  let cursor = 0;
  return files.map((file, index) => {
    const startMs = cursor;
    const durationMs = Number(file.duration_ms ?? 0);
    cursor += durationMs;
    return {
      id: `file-${file.id ?? index}`,
      title: String(file.title ?? titleFromPath(file.path, `Track ${index + 1}`)),
      startMs,
      endMs: cursor,
    };
  });
}

async function main(): Promise<void> {
  await mkdir(traceDir, { recursive: true });
  const metadata = (await Bun.file(path.join(caseDir, "metadata.json")).json()) as any;
  if (metadata.transcript_status !== "succeeded") throw new Error(`Transcript is not runnable: ${metadata.transcript_status ?? "missing"}`);

  const transcript = (await Bun.file(path.join(caseDir, "transcript.json")).json()) as StoredTranscriptPayload;
  const epubEntries = await loadEpubEntries(path.join(caseDir, "book.epub"));
  const assetFiles = (await Bun.file(path.join(caseDir, "asset-files.json")).json().catch(() => [])) as any[];
  const durationMs = Number(metadata.audio_duration_ms ?? transcript.words.at(-1)?.endMs ?? 0);
  const embeddedChapters = await embeddedChaptersFromCase();
  const containers = [
    {
      asset: {
        id: Number(metadata.audio_asset_id),
        book_id: Number(metadata.book_id),
        manifestation_id: Number(metadata.audio_manifestation_id),
        kind: metadata.audio_kind,
        mime: metadata.audio_mime,
        duration_ms: durationMs,
        sequence_in_manifestation: 0,
      } as any,
      files: assetFiles.map((file, index) => ({
        id: Number(file.id ?? index),
        asset_id: Number(metadata.audio_asset_id),
        path: String(file.path ?? ""),
        size: Number(file.size ?? 0),
        start: Number(file.start ?? 0),
        end: Number(file.end ?? 0),
        duration_ms: Number(file.duration_ms ?? 0),
        title: file.title ?? null,
      })) as any,
    },
  ];

  try {
    const runCuration = mode === "node" ? runNodeParallelAgenticChapterCurationDetailed : runRecursiveAgenticChapterCurationDetailed;
    const startedAt = Date.now();
    const git = gitProvenance();
    const detailed = await runCuration({
      book: {
        id: Number(metadata.book_id),
        title: String(metadata.title),
        author: String(metadata.author ?? ""),
      } as any,
      manifestation: {
        id: Number(metadata.audio_manifestation_id),
        book_id: Number(metadata.book_id),
        label: null,
        duration_ms: durationMs,
      } as any,
      containers,
      settings: defaultSettings({
        agents: {
          apiKey: process.env.OPENAI_API_KEY ?? "",
          model: baseModel,
          timeoutMs: 1_800_000,
        },
      }),
      durationMs,
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
    await writeFile(resultPath, `${JSON.stringify({ ...detailed, mode, elapsedMs, debugModels: { model: baseModel, curatorModel, judgeModel }, git }, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ ok: true, slug, mode, accepted: detailed.result?.accepted ?? false, chapters: detailed.result?.accepted ? detailed.result.chapters.length : 0, elapsedMs, resultPath, eventLogPath, traceDir, model: baseModel, curatorModel, judgeModel, git }, null, 2));
  } catch (error) {
    const payload = {
      name: (error as Error).name,
      message: (error as Error).message,
      stack: (error as Error).stack,
    };
    await writeFile(errorPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    console.error(JSON.stringify({ ok: false, slug, mode, errorPath, eventLogPath, traceDir, error: payload }, null, 2));
    process.exitCode = 1;
  }
}

await main();
