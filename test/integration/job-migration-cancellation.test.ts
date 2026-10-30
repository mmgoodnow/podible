import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createEngine, createWorkflow } from "reflow-ts";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { z } from "zod";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

test("migration cancels a linked pending run without replaying effects or losing its checkpoint", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-migration-cancellation-"));
  const domainPath = path.join(root, "books.sqlite");
  const journalPath = path.join(root, "execution.sqlite");
  let db = new Database(domainPath);
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let seedStorage: SQLiteStorage | undefined;
  let handlerEntries = 0;
  let effects = 0;
  let checkpointRepeats = 0;

  try {
    runMigrations(db);
    let repo = new BooksRepo(db);
    repo.ensureSettings();
    const job = repo.createJob({ type: "metadata_hydration", status: "cancelled" });
    const runId = randomUUID();
    const now = Date.now();
    seedStorage = new SQLiteStorage(journalPath);
    await seedStorage.initialize();
    await seedStorage.createRun({
      id: runId,
      workflow: "media-metadata_hydration-v1",
      input: { jobId: job.id, generation: 0 },
      idempotencyKey: `job:${job.id}:0`,
      status: "pending",
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      output: null,
      error: null,
      wakeAt: null,
      waitingFor: null
    });
    await seedStorage.saveStepResult({
      id: randomUUID(),
      runId,
      name: "saved-before-cancellation",
      kind: "step",
      status: "completed",
      output: { marker: "preserve this completed decision" },
      error: null,
      attempts: 2,
      createdAt: now,
      updatedAt: now,
      startedAt: now,
      history: [{ startedAt: now, finishedAt: now, error: null }]
    });
    const checkpoints = await seedStorage.getStepResults(runId);
    expect(checkpoints).toHaveLength(1);
    repo.attachJobExecution(job.id, runId);
    expect(repo.getJob(job.id)?.status).toBe("cancelled");
    expect((await seedStorage.getRun(runId))?.status).toBe("pending");
    await seedStorage.close();
    seedStorage = undefined;

    const start = () => createJobExecutionRuntime(
      { repo, getSettings: () => repo.getSettings(), onLog: () => {} },
      journalPath,
      {
        processJob: async (ctx) => {
          handlerEntries++;
          await ctx.execution!.run("saved-before-cancellation", () => {
            checkpointRepeats++;
            return { marker: "must not replace the saved decision" };
          });
          await ctx.execution!.run("forbidden-after-cancellation", () => { effects++; });
          return "done";
        }
      }
    );

    runtime = await start();
    await runtime.reconcile();
    await runtime.engine.tick();
    await Bun.sleep(350);
    expect(handlerEntries).toBe(0);
    expect(effects).toBe(0);
    expect(checkpointRepeats).toBe(0);
    expect(repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(repo.getJob(job.id)?.status).toBe("cancelled");
    expect((await runtime.engine.getRunStatus(runId))?.run.status).toBe("cancelled");
    expect((await runtime.engine.getRunStatus(runId))?.steps).toEqual(checkpoints);

    await runtime.stop();
    runtime = undefined;
    db.close();
    db = new Database(domainPath);
    runMigrations(db);
    repo = new BooksRepo(db);
    runtime = await start();
    await runtime.reconcile();
    await runtime.engine.tick();
    expect(repo.getJob(job.id)?.status).toBe("cancelled");
    expect((await runtime.engine.getRunStatus(runId))?.run.status).toBe("cancelled");
    expect((await runtime.engine.getRunStatus(runId))?.steps).toEqual(checkpoints);
    expect(handlerEntries).toBe(0);
    expect(effects).toBe(0);
    expect(checkpointRepeats).toBe(0);
  } finally {
    await runtime?.stop();
    await seedStorage?.close();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);

test("migration applies a legacy queued retry once even if history copy crashes after its next failure", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-migration-retry-"));
  const domainPath = path.join(root, "books.sqlite");
  const journalPath = path.join(root, "execution.sqlite");
  let db = new Database(domainPath);
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  const storage = new SQLiteStorage(journalPath);
  let completedRepeats = 0;
  let retryEffects = 0;
  let postRestartEntries = 0;

  try {
    runMigrations(db);
    let repo = new BooksRepo(db);
    repo.ensureSettings();
    const job = repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
    const runId = randomUUID();
    const now = Date.now();
    await storage.initialize();
    await storage.createRun({
      id: runId,
      workflow: "media-metadata_hydration-v1",
      input: { jobId: job.id, generation: 0 },
      idempotencyKey: `job:${job.id}:0`,
      status: "failed",
      createdAt: now,
      updatedAt: now + 1,
      startedAt: now,
      finishedAt: now + 1,
      output: null,
      error: { stepName: "retry-once", message: "failure before user requested retry", code: null },
      wakeAt: null,
      waitingFor: null
    });
    const commonStep = { runId, attempts: 1, createdAt: now, startedAt: now,
      history: [{ startedAt: now, finishedAt: now, error: null }], kind: "step" as const };
    await storage.saveStepResult({ ...commonStep, id: randomUUID(), name: "saved-before-retry",
      status: "completed", output: { marker: "retain successful work" }, error: null, updatedAt: now });
    await storage.saveStepResult({ ...commonStep, id: randomUUID(), name: "retry-once",
      status: "failed", output: null, error: "failure before user requested retry", updatedAt: now + 1 });
    const completedCheckpoint = (await storage.getStepResults(runId)).find((step) => step.name === "saved-before-retry")!;
    repo.attachJobExecution(job.id, runId);
    expect(repo.getJob(job.id)?.status).toBe("queued");
    expect((await storage.getRun(runId))?.status).toBe("failed");

    const migrationWorker = createEngine({
      storage,
      workflows: [createWorkflow({ name: "media-metadata_hydration-v1", input: z.object({ jobId: z.number(), generation: z.number() }) }, async ({ step }) => {
        await step.run("saved-before-retry", async () => { completedRepeats++; return { marker: "incorrect replay" }; });
        await step.run("retry-once", {
          retry: { maxAttempts: 1 },
          handler: () => { retryEffects++; throw new Error("retry still unavailable"); }
        });
      })]
    });
    try {
      await expect(createJobExecutionRuntime(
        { repo, getSettings: () => repo.getSettings(), onLog: () => {} }, journalPath,
        {
          afterHistoryCopy: async () => {
            expect((await storage.getRun(runId))?.status).toBe("pending");
            // Another engine consumes the imported intent while startup is
            // interrupted, so its newer failure must win over legacy 'queued'.
            await migrationWorker.tick();
            expect(retryEffects).toBe(1);
            expect(completedRepeats).toBe(0);
            expect((await storage.getRun(runId))?.error?.message).toBe("retry still unavailable");
            throw new Error("history copy interrupted after retry failure");
          }
        }
      )).rejects.toThrow("history copy interrupted after retry failure");
    } finally {
      await migrationWorker.stop();
    }
    expect((db.query("PRAGMA table_info(jobs)").all() as Array<{ name: string }>).some((column) => column.name === "status")).toBe(true);
    db.close();
    db = new Database(domainPath);
    runMigrations(db);
    repo = new BooksRepo(db);

    runtime = await createJobExecutionRuntime(
      { repo, getSettings: () => repo.getSettings(), onLog: () => {} }, journalPath,
      { processJob: async () => { postRestartEntries++; throw new Error("must not apply the old retry twice"); } }
    );
    await runtime.reconcile();
    await runtime.engine.tick();
    await Bun.sleep(350);
    const after = await runtime.engine.getRunStatus(runId);
    expect(repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(repo.getJob(job.id)?.status).toBe("failed");
    expect(repo.getJob(job.id)?.error).toBe("retry still unavailable");
    expect(after?.run.status).toBe("failed");
    expect(after?.steps.find((step) => step.name === "saved-before-retry")).toEqual(completedCheckpoint);
    expect(retryEffects).toBe(1);
    expect(completedRepeats).toBe(0);
    expect(postRestartEntries).toBe(0);
  } finally {
    await runtime?.stop();
    await storage.close();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
