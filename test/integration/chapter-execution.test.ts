import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { loadStoredManifestationTranscriptPayload, processChapterAnalysisJob } from "../../src/library/chapter-analysis";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { createJobExecutionRuntime } from "../../src/worker/execution";

async function until(check: () => Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("Chapter execution acceptance timed out");
    await Bun.sleep(15);
  }
}

test("real Reflow resumes parallel chapter chunks across file-backed runtime restart", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-chapter-execution-"));
  const domainPath = path.join(root, "books.sqlite");
  const executionPath = path.join(root, "execution.sqlite");
  let db = new Database(domainPath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Chapter execution integration", author: "Fixture" });
  const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
  const audioPath = path.join(root, "audio.mp3");
  await writeFile(audioPath, "fixture audio");
  const asset = repo.addAsset({
    bookId: book.id, manifestationId: manifestation.id, kind: "multi", mime: "audio/mpeg",
    totalSize: 13, durationMs: 31 * 60_000,
    files: [{ path: audioPath, size: 13, start: 0, end: 12, durationMs: 31 * 60_000 }],
  });
  const job = repo.createJob({ type: "chapter_analysis", bookId: book.id, maxAttempts: 1,
    payload: { manifestationId: manifestation.id } });
  const calls: number[] = [];
  const checkpoints: string[] = [];
  let firstChunkCheckpointed = false;
  let fixed = false;
  let overlap = false;
  let inFlight = 0;
  const deps = {
    extractChunkClip: async ({ clipName }: { clipName: string }) => path.join(root, `${clipName}.mp3`),
    transcribeChunk: async (_settings: unknown, clipPath: string) => {
      const index = Number(clipPath.match(/chunk-(\d+)-attempt/u)?.[1]);
      calls.push(index);
      inFlight++;
      overlap ||= inFlight > 1;
      try {
        if (index === 1 && !fixed) {
          // Bounded wait: prove siblings can overlap, then fail only after the
          // successful sibling is durably saved. No gate can hang shutdown.
          await until(async () => firstChunkCheckpointed);
          throw new Error("fixture second chunk unavailable");
        }
        await Bun.sleep(30);
        return { words: [{ raw: `chunk-${index}`, token: `chunk${index}`, startMs: 20_000, endMs: 20_500 }], segments: [] };
      } finally { inFlight--; }
    },
  };
  const start = () => createJobExecutionRuntime({ repo,
    getSettings: () => defaultSettings({ agents: { apiKey: "fake-no-network", timeoutMs: 1000 } }), onLog() {},
  }, executionPath, {
    retryDelayMs: 1,
    processJob: (ctx, current) => processChapterAnalysisJob(ctx, current, deps),
    onCheckpoint: async (_jobId, name) => {
      checkpoints.push(name);
      if (name.includes(`chapter:asset:${asset.id}:chunk:0`)) firstChunkCheckpointed = true;
    },
  });
  let runtime: Awaited<ReturnType<typeof start>> | undefined;
  try {
    runtime = await start();
    await runtime.reconcile();
    await until(async () => {
      await runtime!.reconcile();
      return repo.getJob(job.id)?.status === "failed";
    });
    expect(overlap).toBe(true);
    expect([...calls].sort()).toEqual([0, 1]);
    const runId = repo.getJobExecution(job.id)!.run_id;
    const before = await runtime.engine.getRunStatus(runId);
    expect(JSON.stringify(before?.steps)).toContain(`chapter:asset:${asset.id}:chunk:0`);
    await runtime.stop();
    runtime = undefined;
    db.close();

    db = new Database(domainPath);
    repo = new BooksRepo(db);
    fixed = true;
    repo.retryJob(job.id);
    runtime = await start();
    await runtime.reconcile();
    await until(async () => repo.getJob(job.id)?.status === "succeeded");
    expect(repo.getJobExecution(job.id)!.run_id).toBe(runId);
    expect(calls.filter((index) => index === 0)).toHaveLength(1);
    expect(calls.filter((index) => index === 1)).toHaveLength(2);
    expect(checkpoints.filter((name) => name.includes(`chapter:asset:${asset.id}:chunk:0`))).toHaveLength(1);
    expect(repo.getChapterAnalysis(manifestation.id)?.status).toBe("succeeded");
    const transcript = await loadStoredManifestationTranscriptPayload(repo, manifestation.id);
    expect(transcript?.text).toContain("chunk-0");
    expect(transcript?.text).toContain("chunk-1");
    expect(transcript?.words).toHaveLength(2);
  } finally {
    if (runtime) await runtime.stop();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
