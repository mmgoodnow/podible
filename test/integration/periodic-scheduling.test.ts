import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { queuePeriodicJob } from "../../src/worker/periodic";

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "periodic-work-"));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db); repo.ensureSettings();
  return { root, db, repo, ctx: { repo, getSettings: () => repo.getSettings(), onLog() {} } };
}
async function until(check: () => boolean) {
  const deadline = Date.now() + 6000;
  while (!check()) { if (Date.now() > deadline) throw new Error("Periodic work did not settle"); await Bun.sleep(20); }
}

test("periodic admission reuses outstanding observation across slots and boots without merging explicit commands", async () => {
  const f = await fixture();
  try {
    const first = queuePeriodicJob(f.repo, { type: "reconcile", idempotencyKey: "book-maintenance:boot-a:1" });
    const second = queuePeriodicJob(f.repo, { type: "reconcile", idempotencyKey: "book-maintenance:boot-b:2" });
    expect(second.id).toBe(first.id);
    const manual = queuePeriodicJob(f.repo, { type: "reconcile", idempotencyKey: "manual:1" });
    expect(manual.id).not.toBe(first.id);
    const backfill = queuePeriodicJob(f.repo, { type: "tv_sync", idempotencyKey: "tv-seerr:1", payload: { backfillRequestIds: [42] } });
    const observation = queuePeriodicJob(f.repo, { type: "tv_sync", idempotencyKey: "tv-seerr:2" });
    expect(observation.id).not.toBe(backfill.id);
    const manualTv = queuePeriodicJob(f.repo, { type: "tv_sync", idempotencyKey: "tv-sync:operator", payload: {} });
    expect(manualTv.id).not.toBe(observation.id);
  } finally { f.db.close(); await rm(f.root, { recursive: true, force: true }); }
});

test("existing unstarted periodic backlog completes as coalesced receipts while explicit commands still run", async () => {
  const f = await fixture();
  const jobs = Array.from({ length: 45 }, (_, i) => f.repo.createJob({ type: "retention_cleanup", idempotencyKey: `retention-pressure:${i}` }));
  const manual = f.repo.createJob({ type: "retention_cleanup", idempotencyKey: "operator-cleanup" });
  const ran: number[] = [];
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => { await ctx.execution!.run("observe", () => { ran.push(job.id); }); return "done"; },
  });
  try {
    await runtime.reconcile(); await until(() => [...jobs, manual].every(j => f.repo.getJob(j.id)?.status === "succeeded"));
    expect(ran.sort((a,b) => a-b)).toEqual([jobs.at(-1)!.id, manual.id]);
    expect((await runtime.engine.getRunStatus(f.repo.getJobExecution(jobs[0]!.id)!.run_id))?.run.output)
      .toEqual({ jobId: jobs[0]!.id, coalescedInto: jobs.at(-1)!.id });
  } finally { await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
});

test("a periodic execution with existing effects resumes its original receipts instead of being coalesced away", async () => {
  const f = await fixture();
  const first = f.repo.createJob({ type: "torrent_metrics_collect", idempotencyKey: "torrent-metrics:1" });
  const effects: number[] = [], finished: number[] = [];
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("effect", () => { effects.push(job.id); });
      await ctx.execution!.sleep("wait", 500);
      await ctx.execution!.run("finish", () => { finished.push(job.id); }); return "done";
    },
  });
  try {
    await runtime.reconcile(); await until(() => effects.includes(first.id) && f.repo.getJob(first.id)?.status === "queued");
    const newer = f.repo.createJob({ type: "torrent_metrics_collect", idempotencyKey: "torrent-metrics:2" });
    await runtime.reconcile(); await until(() => [first, newer].every(j => f.repo.getJob(j.id)?.status === "succeeded"));
    expect(effects.filter(id => id === first.id)).toHaveLength(1);
    expect(finished.sort((a,b) => a-b)).toEqual([first.id, newer.id]);
  } finally { await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
});
