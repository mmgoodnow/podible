import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-job-journal-"));
  const db = new Database(path.join(root, "books.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  return {
    root,
    db,
    repo,
    ctx: { repo, getSettings: () => repo.getSettings(), onLog: () => {} },
    journal: path.join(root, "execution.sqlite")
  };
}
async function until(check: () => boolean) {
  const end = Date.now() + 4000;
  while (!check()) {
    if (Date.now() > end) throw new Error("Timed out");
    await Bun.sleep(10);
  }
}

test("legacy lifecycle migrates losslessly to Reflow, leaving no application lifecycle columns", async () => {
  const f = await fixture();
  const jobs = ["succeeded", "failed", "cancelled"].map((status) => {
    const job = f.repo.createJob({ type: "acquire" });
    f.db
      .query("UPDATE jobs SET status = ?, error = ?, attempt_count = 7, updated_at = ? WHERE id = ?")
      .run(status, `${status} historical detail`, "2026-01-02T03:04:05.000Z", job.id);
    return { id: job.id, status };
  });
  const runtime = await createJobExecutionRuntime(f.ctx, f.journal);
  try {
    const columns = f.db.query("PRAGMA table_info(jobs)").all() as Array<{ name: string }>;
    expect(columns.map((column) => column.name)).not.toContain("status");
    for (const name of ["error", "attempt_count", "next_run_at", "updated_at"])
      expect(columns.map((column) => column.name)).not.toContain(name);
    for (const job of jobs) {
      expect(f.repo.getJob(job.id)).toMatchObject({
        status: job.status,
        error: `${job.status} historical detail`,
        attempt_count: 7,
        updated_at: "2026-01-02T03:04:05.000Z"
      });
      expect(await runtime.engine.getRunStatus(f.repo.getJobExecution(job.id)!.run_id)).not.toBeNull();
    }
  } finally {
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("interruption after history copy is idempotent and never drops the only copy", async () => {
  const f = await fixture();
  const job = f.repo.createJob({ type: "acquire" });
  f.db.query("UPDATE jobs SET status = 'failed', error = 'preserve me' WHERE id = ?").run(job.id);
  await expect(
    createJobExecutionRuntime(f.ctx, f.journal, {
      afterHistoryCopy: async () => {
        throw new Error("migration interruption");
      }
    })
  ).rejects.toThrow("migration interruption");
  expect(
    (f.db.query("PRAGMA table_info(jobs)").all() as Array<{ name: string }>).some((column) => column.name === "status")
  ).toBe(true);
  const runtime = await createJobExecutionRuntime(f.ctx, f.journal);
  try {
    expect(f.repo.getJob(job.id)?.error).toBe("preserve me");
    expect(
      (await runtime.engine.listRuns()).filter((run) => (run.input as { jobId?: number }).jobId === job.id)
    ).toHaveLength(1);
  } finally {
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("lifecycle reads come from the journal without running a status pump", async () => {
  const f = await fixture();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const runtime = await createJobExecutionRuntime(f.ctx, f.journal, {
    processJob: async (ctx) => {
      await ctx.execution!.run("work", () => gate);
      return "done";
    }
  });
  try {
    const job = f.repo.createJob({ type: "metadata_hydration" });
    expect(f.repo.getJob(job.id)?.status).toBe("queued");
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "running");
    release();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    expect(f.repo.getHealthSummary().jobs.succeeded).toBe(1);
    expect(f.repo.forExecution(() => {}).getJob(job.id)?.status).toBe("succeeded");
  } finally {
    release();
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});
