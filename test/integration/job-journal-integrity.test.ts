import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

type Runtime = Awaited<ReturnType<typeof createJobExecutionRuntime>>;

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-journal-integrity-"));
  const domainPath = path.join(root, "books.sqlite");
  const journalPath = path.join(root, "execution.sqlite");
  let db = new Database(domainPath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  repo.ensureSettings();
  let runtime: Runtime | undefined;
  const effects: string[] = [];

  return {
    get repo() { return repo; },
    effects,
    async start() {
      runtime = await createJobExecutionRuntime(
        { repo, getSettings: () => repo.getSettings(), onLog: () => {} },
        journalPath,
        {
          processJob: async (ctx, job) => {
            await ctx.execution!.run("record-input", () => {
              const payload = JSON.parse(job.payload_json!) as { marker: string };
              effects.push(payload.marker);
            });
            return "done";
          }
        }
      );
      return runtime;
    },
    async stop() {
      await runtime?.stop();
      runtime = undefined;
    },
    reopen() {
      db.close();
      db = new Database(domainPath);
      runMigrations(db);
      repo = new BooksRepo(db);
    },
    async close() {
      try {
        await runtime?.stop();
      } finally {
        db.close();
        await rm(root, { recursive: true, force: true });
      }
    }
  };
}

async function complete(runtime: Runtime, repo: BooksRepo, jobId: number): Promise<string> {
  await runtime.reconcile();
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const link = repo.getJobExecution(jobId);
    if (link && (await runtime.engine.getRunStatus(link.run_id))?.run.status === "completed") {
      return link.run_id;
    }
    await runtime.reconcile();
    await Bun.sleep(10);
  }
  throw new Error(`Job ${jobId} did not complete within the acceptance deadline`);
}

test("wipe preserves job ID high-water and never reuses a surviving journal run", async () => {
  const f = await fixture();
  try {
    let runtime = await f.start();
    const oldJob = f.repo.createJob({ type: "metadata_hydration", payload: { marker: "before-wipe" } });
    const oldRunId = await complete(runtime, f.repo, oldJob.id);
    expect(f.effects).toEqual(["before-wipe"]);

    await f.stop();
    f.repo.wipeDatabase();
    expect(f.repo.getJob(oldJob.id)).toBeNull();
    f.reopen();
    runtime = await f.start();

    expect((await runtime.engine.getRunStatus(oldRunId))?.run.status).toBe("completed");
    const newJob = f.repo.createJob({ type: "metadata_hydration", payload: { marker: "after-wipe" } });
    const newRunId = await complete(runtime, f.repo, newJob.id);
    expect(newJob.id).toBeGreaterThan(oldJob.id);
    expect(newRunId).not.toBe(oldRunId);
    expect(f.effects).toEqual(["before-wipe", "after-wipe"]);
    expect(f.repo.getJob(newJob.id)?.status).toBe("succeeded");
    expect((await runtime.engine.getRunStatus(oldRunId))?.run.input).toMatchObject({ jobId: oldJob.id });
    expect((await runtime.engine.getRunStatus(newRunId))?.run.input).toMatchObject({ jobId: newJob.id });
  } finally {
    await f.close();
  }
}, 15_000);

test("a missing linked journal run is an explicit integrity failure, never fresh queued work", async () => {
  const f = await fixture();
  try {
    let runtime = await f.start();
    const job = f.repo.createJob({ type: "metadata_hydration", payload: { marker: "must-run-once" } });
    const runId = await complete(runtime, f.repo, job.id);
    expect(f.repo.getJob(job.id)?.status).toBe("succeeded");

    // Model an incomplete restore or accidental retention cleanup using only
    // Reflow's public API; leave the application's execution link untouched.
    await Bun.sleep(10);
    expect(await runtime.engine.prune({ olderThan: 0, statuses: ["completed"] })).toBe(1);
    expect(await runtime.engine.getRunStatus(runId)).toBeNull();
    await f.stop();
    f.reopen();
    runtime = await f.start();

    expect(f.repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(() => f.repo.getJob(job.id)).toThrow(`Missing execution ${runId} for job ${job.id}`);
    let reconciliationError: unknown;
    try {
      await runtime.reconcile();
    } catch (error) {
      reconciliationError = error;
    }
    expect(f.repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(await runtime.engine.listRuns()).toHaveLength(0);
    expect(f.effects).toEqual(["must-run-once"]);
    expect(reconciliationError).toBeInstanceOf(Error);
    expect((reconciliationError as Error).message).toContain(`Missing execution ${runId} for job ${job.id}`);
  } finally {
    await f.close();
  }
}, 15_000);
