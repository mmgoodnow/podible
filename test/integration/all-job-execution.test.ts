import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { JobType } from "../../src/app-types";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime, EXECUTION_JOB_TYPES, executionRetryPolicy } from "../../src/worker/execution";
import { jobLockKeys } from "../../src/worker/locks";
import { jobJournalReader } from "../../src/worker/job-journal";
import { randomUUID } from "node:crypto";
import { crossSeedCommandSchema, crossSeedStateKey } from "../../src/media/cross-seed-state";
import { currentEffects, effectPhase } from "../../src/runtime/execution-effects";
import { withKeyedMutex } from "../../src/runtime/keyed-mutex";

const types = EXECUTION_JOB_TYPES;

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-all-execution-"));
  const db = new Database(path.join(root, "books.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  return { root, db, repo, ctx: { repo, getSettings: () => repo.getSettings(), onLog: () => {} } };
}

async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("Runtime acceptance timed out");
    await Bun.sleep(15);
  }
}

test("two independent reuse targets run while same-target work parks and a third handler waits", async () => {
  const f = await fixture();
  const queue = (targetId: number) => {
    const operationId = randomUUID();
    const job = f.repo.createJob({ type: "torrent_reuse", payload: { operationId } });
    const command = crossSeedCommandSchema.parse({ operationId, scope: { media: "tv", targetId }, releaseId: 1 });
    f.repo.setJsonState(crossSeedStateKey(operationId), { command, jobId: job.id });
    return job;
  };
  const first = queue(1);
  let releaseFirst!: () => void, releaseSecond!: () => void;
  const firstGate = new Promise<void>(resolve => { releaseFirst = resolve; });
  const secondGate = new Promise<void>(resolve => { releaseSecond = resolve; });
  let secondHeldId = 0, active = 0, peak = 0;
  const entered = new Set<number>();
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("reuse:guarded-work", async () => {
        entered.add(job.id); active++; peak = Math.max(peak, active);
        try {
          if (job.id === first.id) await firstGate;
          if (job.id === secondHeldId) await secondGate;
        } finally { active--; }
      });
      return "done";
    },
  });
  try {
    await runtime.reconcile(); await until(() => entered.has(first.id));
    const same = queue(1), independent = queue(2);
    await runtime.reconcile(); await until(() => f.repo.getJobStatus(independent.id) === "succeeded");
    expect(entered.has(same.id)).toBe(false);
    const secondHeld = queue(3); secondHeldId = secondHeld.id;
    await runtime.reconcile(); await until(() => entered.has(secondHeld.id));
    const third = queue(4);
    await runtime.reconcile(); await Bun.sleep(400);
    expect(active).toBe(2); expect(entered.has(third.id)).toBe(false);
    expect(entered.has(same.id)).toBe(false);
    releaseSecond(); await until(() => f.repo.getJobStatus(third.id) === "succeeded");
    expect(entered.has(same.id)).toBe(false);
    releaseFirst(); await until(() => f.repo.getJobStatus(same.id) === "succeeded");
    expect(peak).toBe(2);
    const unknown = f.repo.createJob({ type: "torrent_reuse", payload: { operationId: randomUUID() } });
    expect(jobLockKeys(f.repo, unknown)).toEqual(["*"]);
    for (const media of ["audio", "ebook", "movie", "tv"] as const) {
      const operationId = randomUUID(), job = f.repo.createJob({ type: "torrent_reuse", payload: { operationId } });
      f.repo.setJsonState(crossSeedStateKey(operationId), { command: { scope: { media, targetId: 7 } } });
      expect(jobLockKeys(f.repo, job)).toEqual([media === "tv" ? "reuse:tv:7" : media === "audio" || media === "ebook" ? `book:7:media:${media}` : `${media}-target:7`]);
    }
  } finally {
    releaseFirst(); releaseSecond(); await runtime.stop(); f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
}, 15_000);

test("active fences avoid history while observing external ownership, status and cancellation changes", async () => {
  const f = await fixture();
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let assertActive: (() => void) | undefined;
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async ctx => {
      await ctx.execution!.run("guarded-read", async () => {
        assertActive = ctx.execution!.assertActive;
        await gate;
      });
      return "done";
    },
  });
  const domainWriter = new Database(path.join(f.root, "books.sqlite"));
  const journalWriter = new Database(path.join(f.root, "execution.sqlite"));
  try {
    const job = f.repo.createJob({ type: "metadata_hydration" });
    await runtime.reconcile(); await until(() => !!assertActive);
    const reader = jobJournalReader(f.repo.executionDatabasePath());
    f.repo.bindJobJournalReader({ ...reader, read() { throw new Error("Active fence hydrated history"); } });
    expect(() => assertActive!()).not.toThrow();
    const owner = f.repo.getJobExecution(job.id)!;
    domainWriter.query("UPDATE job_executions SET owner_token='replacement' WHERE job_id=?").run(job.id);
    expect(() => assertActive!()).toThrow(LeaseExpiredError);
    domainWriter.query("UPDATE job_executions SET owner_token=? WHERE job_id=?").run(owner.owner_token, job.id);
    expect(() => assertActive!()).not.toThrow();
    journalWriter.query("UPDATE workflow_runs SET status='cancelled' WHERE id=?").run(owner.run_id);
    expect(() => assertActive!()).toThrow(RunCancelledError);
    journalWriter.query("UPDATE workflow_runs SET status='running' WHERE id=?").run(owner.run_id);
    expect(() => assertActive!()).not.toThrow();
    domainWriter.query(`INSERT INTO job_commands(job_id,kind,generation,run_id,created_at)
      VALUES (?,'cancel',?,?,?)`).run(job.id, f.repo.jobExecutionGeneration(job.id), owner.run_id, new Date().toISOString());
    expect(() => assertActive!()).toThrow(RunCancelledError);
  } finally {
    f.repo.bindJobJournalReader(jobJournalReader(f.repo.executionDatabasePath()));
    release(); await runtime.stop(); domainWriter.close(); journalWriter.close(); f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("all registered job types use checkpointed execution without claiming or resetting rows", async () => {
  expect([...types].sort()).toEqual([...EXECUTION_JOB_TYPES].sort());
  const f = await fixture();
  const seen: string[] = [];
  const jobs = types.map((type) => f.repo.createJob({ type }));
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("first", () => {
        seen.push(`${job.type}:first`);
      });
      await ctx.execution!.run("publish", () => {
        seen.push(`${job.type}:publish`);
      });
      return "done";
    }
  });
  try {
    await runtime.reconcile();
    await until(() => jobs.every((job) => f.repo.getJob(job.id)?.status === "succeeded"));
    for (const job of jobs) {
      const status = await runtime.engine.getRunStatus(f.repo.getJobExecution(job.id)!.run_id);
      expect(status?.steps.some((step) => step.name === "publish")).toBe(true);
      expect(seen.filter((value) => value === `${job.type}:publish`)).toHaveLength(1);
    }
  } finally {
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("restart replays completed input and effects without admission while a competitor holds their lane", async () => {
  const f = await fixture(), journalPath = path.join(f.root, "execution.sqlite");
  const book = f.repo.createBook({ title: "Cached lane replay", author: "Fixture" });
  const original = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } });
  let effects = 0, replayed = false, fresh = false, competitorEntered = false, release!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  let competitorId = 0;
  const processJob = async (ctx: import("../../src/worker/context").WorkerContext, job: import("../../src/app-types").JobRow) => {
    if (job.id === competitorId) {
      await ctx.execution!.run("hold-lane", async () => { competitorEntered = true; await hold; });
    } else {
      await ctx.execution!.run("cached-effect", () => { effects++; return "recorded"; });
      replayed = true;
      await ctx.execution!.sleep("restart-boundary", 600_000);
      await ctx.execution!.run("fresh-effect", () => { fresh = true; });
    }
    return "done" as const;
  };
  let runtime = await createJobExecutionRuntime(f.ctx, journalPath, { processJob });
  try {
    await runtime.reconcile();
    const runId = f.repo.getJobExecution(original.id)!.run_id;
    await until(async () => (await runtime.engine.getRunStatus(runId))?.run.status === "sleeping");
    await runtime.stop();
    // Model historical waits preceding a completed job-input checkpoint. Skipping
    // these named operations must not strand Reflow's ordered replay results.
    const db = new Database(journalPath);
    try {
      const input = db.query("SELECT created_at FROM workflow_steps WHERE run_id=? AND name='job-input'").get(runId) as { created_at: number };
      const insert = db.prepare(`INSERT INTO workflow_steps(id,run_id,name,status,output,attempts,created_at,updated_at,kind)
        VALUES(?,?,?,'completed','null',0,?,?,'sleep')`);
      db.transaction(() => {
        for (let i = 0; i < 128; i++) insert.run(randomUUID(), runId, `podible:lane:job-input:${i}`, input.created_at - 256 + i, input.created_at - 256 + i);
      })();
    } finally { db.close(); }
    replayed = false;
    competitorId = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } }).id;
    runtime = await createJobExecutionRuntime(f.ctx, journalPath, { processJob });
    await runtime.reconcile(); await until(() => competitorEntered);
    const wake = new Database(journalPath);
    try {
      wake.query("UPDATE workflow_steps SET output=? WHERE run_id=? AND name='restart-boundary'").run(String(Date.now() - 1), runId);
      wake.query("UPDATE workflow_runs SET wake_at=? WHERE id=?").run(Date.now() - 1, runId);
    } finally { wake.close(); }
    await until(() => replayed);
    expect(effects).toBe(1);
    expect(fresh).toBe(false);
    expect(runtime.diagnostics().reservations).toBe(1);
    release(); await until(() => f.repo.getJobStatus(original.id) === "succeeded");
    expect(fresh).toBe(true);
    expect(effects).toBe(1);
  } finally { release(); await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
}, 15000);

test("long cached lane histories yield to filesystem completion before exhausting historical waits", async () => {
  const f = await fixture(), journalPath = path.join(f.root, "execution.sqlite");
  const book = f.repo.createBook({ title: "Fair lane replay", author: "Fixture" });
  const original = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } });
  let competitorId = 0, competitorEntered = false, resumed = false, ioFinished = false, fresh = false, release!: () => void;
  let ioTask: Promise<void> | undefined;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const historyLength = 1024;
  const processJob = async (ctx: import("../../src/worker/context").WorkerContext, job: import("../../src/app-types").JobRow) => {
    if (job.id === competitorId) await ctx.execution!.run("hold-lane", async () => { competitorEntered = true; await hold; });
    else {
      await ctx.execution!.sleep("restart-boundary", 600_000);
      if (resumed) ioTask = readFile(path.join(f.root, "books.sqlite")).then(() => { ioFinished = true; release(); });
      await ctx.execution!.run("fresh-effect", () => { fresh = true; });
    }
    return "done" as const;
  };
  let runtime = await createJobExecutionRuntime(f.ctx, journalPath, { processJob });
  try {
    await runtime.reconcile();
    const runId = f.repo.getJobExecution(original.id)!.run_id;
    await until(async () => (await runtime.engine.getRunStatus(runId))?.run.status === "sleeping");
    await runtime.stop();
    const seed = new Database(journalPath);
    try {
      const insert = seed.prepare(`INSERT INTO workflow_steps(id,run_id,name,status,output,attempts,created_at,updated_at,kind)
        VALUES(?,?,?,'completed','null',0,?,?,'sleep')`);
      const base = Date.now();
      seed.transaction(() => {
        for (let i = 0; i < historyLength; i++) insert.run(randomUUID(), runId, `podible:lane:fresh-effect:${i}`, base + i, base + i);
      })();
    } finally { seed.close(); }
    competitorId = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } }).id;
    resumed = true;
    runtime = await createJobExecutionRuntime(f.ctx, journalPath, { processJob });
    await runtime.reconcile(); await until(() => competitorEntered);
    const wake = new Database(journalPath);
    try {
      wake.query("UPDATE workflow_steps SET output=? WHERE run_id=? AND name='restart-boundary'").run(String(Date.now() - 1), runId);
      wake.query("UPDATE workflow_runs SET wake_at=? WHERE id=?").run(Date.now() - 1, runId);
    } finally { wake.close(); }
    await until(() => f.repo.getJobStatus(original.id) === "succeeded");
    expect(ioFinished).toBe(true); expect(fresh).toBe(true);
    const status = await runtime.engine.getRunStatus(runId);
    expect(status!.steps.some(step => step.name === `podible:lane:fresh-effect:${historyLength}`)).toBe(false);
  } finally { release(); await ioTask; await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
}, 15000);

test("lock-blocked maintenance parks durably instead of starving unrelated foreground work", async () => {
  const f = await fixture();
  const book = f.repo.createBook({ title: "Active work", author: "Fixture" });
  const held = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } });
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let holding = false, foreground = false;
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("work", async () => {
        if (job.id === held.id) { holding = true; await gate; }
        if (job.type === "cover_generation") foreground = true;
      });
      return "done";
    },
  });
  try {
    await runtime.reconcile(); await until(() => holding);
    for (let i = 0; i < 25; i++) {
      f.repo.createJob({ type: "retention_cleanup" });
      f.repo.createJob({ type: "tv_retention_pressure" });
    }
    await runtime.reconcile(); await Bun.sleep(400);
    const job = f.repo.createJob({ type: "cover_generation" });
    await runtime.reconcile();
    await until(() => foreground);
    expect(f.repo.getJob(held.id)?.status).toBe("running");
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
  } finally {
    release(); await runtime.stop(); f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
}, 15000);

test("workflow operations fail fast inside effects or held mutexes without leaking lane reservations", async () => {
  const f = await fixture(); let checked = false;
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async ctx => {
      await ctx.execution!.run("outer", async () => {
        await expect(ctx.execution!.run("inner", async () => {})).rejects.toThrow("outside effect handlers");
        expect(() => ctx.execution!.sleep("inner-sleep", 1)).toThrow("outside effect handlers");
      });
      await withKeyedMutex({ kind: "storage", key: "workflow-fixture" }, async () => {
        await expect(ctx.execution!.run("under-mutex", async () => {})).rejects.toThrow("holding a mutex");
        expect(() => ctx.execution!.sleep("under-mutex-sleep", 1)).toThrow("holding a mutex");
      });
      checked = true; return "done";
    },
  });
  try {
    const job = f.repo.createJob({ type: "retention_cleanup" });
    await runtime.reconcile(); await until(() => f.repo.getJobStatus(job.id) === "succeeded");
    expect(checked).toBe(true); expect(runtime.diagnostics().reservations).toBe(0);
  } finally { await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
});

test("step deadline signals a hung owner without allowing conflicting effects before callback exit", async () => {
  const f = await fixture(); let release!: () => void, signal: AbortSignal | undefined, foreground = false;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const logs: string[] = [];
  const runtime = await createJobExecutionRuntime({ ...f.ctx, onLog: line => logs.push(line) }, path.join(f.root, "execution.sqlite"), {
    stallTimeoutMs: 10,
    processJob: async (ctx, job) => {
      await ctx.execution!.run("work", async () => {
        if (job.type === "retention_cleanup") await withKeyedMutex({ kind: "publication", key: "/private/view?token=secret-never-log" }, async () => {
          effectPhase("fixture:holding-publication");
          signal = currentEffects()!.signal; await hold;
        });
        else foreground = true;
      }, { timeoutMs: job.type === "retention_cleanup" ? 10 : 1000 });
      return "done";
    },
  });
  try {
    const owner = f.repo.createJob({ type: "retention_cleanup", maxAttempts: 1 });
    await runtime.reconcile(); await until(() => signal?.aborted === true);
    const next = f.repo.createJob({ type: "cover_generation" }); await runtime.reconcile();
    await Bun.sleep(30); await runtime.reconcile(); await runtime.reconcile();
    expect(foreground).toBe(false); expect(runtime.diagnostics().effects[0]?.stalled).toBe(true);
    expect(logs.filter(line => line.includes("stalled job="))).toHaveLength(1);
    const diagnosticLog = logs.find(line => line.includes("stalled job="))!;
    const diagnostic = JSON.parse(diagnosticLog.split(" diagnostics=")[1]!);
    expect(diagnostic.phases).toEqual([expect.objectContaining({ name: "fixture:holding-publication", startedAt: expect.any(Number) })]);
    expect(diagnostic.mutexes).toHaveLength(1);
    expect(diagnostic.mutexes[0]).toMatchObject({ kind: "publication", waiters: 0,
      owner: { jobId: owner.id, stepName: "work" } });
    expect(diagnostic.mutexes[0].owner.runId).toBe(f.repo.getJobExecution(owner.id)!.run_id);
    expect(diagnostic.lanes.find((lane: { key: string }) => lane.key === "*").owner.jobId).toBe(owner.id);
    expect(diagnostic.reservations).toContainEqual(expect.objectContaining({ jobId: owner.id, stepName: "work", entered: true }));
    expect(diagnosticLog).not.toContain("secret-never-log");
    expect(diagnosticLog).not.toContain("/private/view");
    expect(diagnostic.reservations.flatMap((item: { keys: string[] }) => item.keys).some((key: string) => key.startsWith("execution:"))).toBe(false);
    release(); await until(() => f.repo.getJobStatus(next.id) === "succeeded");
    expect(foreground).toBe(true); expect(runtime.diagnostics().effects).toEqual([]);
  } finally { release(); await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true }); }
});

test("cancellation after admission but before handler entry releases the global reservation on reconcile", async () => {
  const f = await fixture(), activeLocks = new Set<string>();
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let cancellation: Promise<boolean> | undefined;
  let enteredCancelledHandler = false, foregroundEntered = false;
  const ctx = { ...f.ctx, activeLocks, onLog(line: string) {
    if (line.endsWith("step=cancel-before-handler") && !cancellation) {
      const runId = /run=(\S+)/.exec(line)![1]!;
      cancellation = runtime!.engine.cancel(runId);
    }
  } };
  runtime = await createJobExecutionRuntime(ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (context, job) => {
      if (job.type === "retention_cleanup")
        await context.execution!.run("cancel-before-handler", () => { enteredCancelledHandler = true; });
      else await context.execution!.run("foreground", () => { foregroundEntered = true; });
      return "done";
    },
  });
  try {
    const cancelled = f.repo.createJob({ type: "retention_cleanup" });
    await runtime.reconcile();
    await until(() => cancellation !== undefined);
    await cancellation;
    await until(() => f.repo.getJob(cancelled.id)?.status === "cancelled");
    expect(enteredCancelledHandler).toBe(false);
    // Production pumps reconcile after cancellation commands; operation promises need not settle.
    await runtime.reconcile();
    expect([...activeLocks]).toEqual([]);
    const foreground = f.repo.createJob({ type: "cover_generation" });
    await runtime.reconcile();
    await until(() => f.repo.getJob(foreground.id)?.status === "succeeded");
    expect(foregroundEntered).toBe(true);
    expect([...activeLocks]).toEqual([]);
  } finally {
    await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true });
  }
}, 10000);

test("terminal-run cleanup never releases an entered handler's global lane before its actual exit", async () => {
  const f = await fixture(), activeLocks = new Set<string>();
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let entered = false, exited = false, foregroundEntered = false;
  const runtime = await createJobExecutionRuntime({ ...f.ctx, activeLocks }, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("work", async () => {
        if (job.type === "retention_cleanup") {
          entered = true;
          await gate; // Model an external side effect that ignores cancellation until it returns.
          exited = true;
        } else foregroundEntered = true;
      });
      return "done";
    },
  });
  try {
    const cancelled = f.repo.createJob({ type: "retention_cleanup" });
    await runtime.reconcile(); await until(() => entered);
    await runtime.engine.cancel(f.repo.getJobExecution(cancelled.id)!.run_id);
    await runtime.reconcile();
    expect(f.repo.getJob(cancelled.id)?.status).toBe("cancelled");
    expect(activeLocks.has("*")).toBe(true);
    const foreground = f.repo.createJob({ type: "cover_generation" });
    await runtime.reconcile(); await Bun.sleep(150); await runtime.reconcile();
    expect(foregroundEntered).toBe(false);
    expect(exited).toBe(false);
    expect(activeLocks.has("*")).toBe(true);
    release();
    await until(() => f.repo.getJob(foreground.id)?.status === "succeeded");
    expect(exited).toBe(true);
    expect(foregroundEntered).toBe(true);
    expect([...activeLocks]).toEqual([]);
  } finally {
    release(); await runtime.stop(); f.db.close(); await rm(f.root, { recursive: true, force: true });
  }
}, 10000);

test.each(["full_library_refresh", "reconcile", "retention_cleanup"] as const)("same execution parallel steps overlap, conflicting jobs and %s do not", async (maintenanceType) => {
  const f = await fixture();
  const book = f.repo.createBook({ title: "Parallel", author: "Fixture" });
  const entered: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const first = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } });
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      await Promise.all(
        [0, 1, 2].map((index) =>
          ctx.execution!.run(`parallel-${index}`, async () => {
            entered.push(`${job.id}:${index}`);
            if (job.id === first.id) await gate;
          })
        )
      );
      return "done";
    }
  });
  try {
    await runtime.reconcile();
    await until(() => entered.length === 3);
    const second = f.repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { manifestationId: 1 } });
    const refresh = f.repo.createJob({ type: maintenanceType });
    await runtime.reconcile();
    await Bun.sleep(150);
    expect(entered).toEqual([`${first.id}:0`, `${first.id}:1`, `${first.id}:2`]);
    release();
    await until(() => [first, second, refresh].every((job) => f.repo.getJob(job.id)?.status === "succeeded"));
    expect(entered).toHaveLength(9);
  } finally {
    release();
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("Reflow retries only failed steps and exhaustion stays failed until explicit retry", async () => {
  const f = await fixture();
  const job = f.repo.createJob({ type: "metadata_hydration", maxAttempts: 2 });
  let first = 0;
  let failing = 0;
  let fixed = false;
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    retryDelayMs: 1,
    processJob: async (ctx) => {
      await ctx.execution!.run("first", () => {
        first++;
      });
      await ctx.execution!.run("unreliable", () => {
        failing++;
        if (!fixed) throw new Error("fixture failure");
      });
      return "done";
    }
  });
  try {
    await runtime.reconcile();
    await until(async () => {
      await runtime.reconcile();
      return f.repo.getJob(job.id)?.status === "failed";
    });
    expect(first).toBe(1);
    expect(failing).toBe(2);
    await runtime.reconcile();
    await Bun.sleep(100);
    expect(failing).toBe(2);
    fixed = true;
    f.repo.retryJob(job.id);
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    expect(first).toBe(1);
    expect(failing).toBe(3);
  } finally {
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

test("jobs schema accepts new execution names without rebuilding domain tables or losing links", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const job = repo.createJob({ type: "acquire" });
  repo.attachJobExecution(job.id, "saved-run");
  db.query("UPDATE jobs SET type = 'future_workflow' WHERE id = ?").run(job.id);
  expect(repo.getJobExecution(job.id)?.run_id).toBe("saved-run");
  expect(db.query("PRAGMA foreign_key_check").all()).toHaveLength(0);
  db.close();
});

for (const type of ["cover_generation", "metadata_hydration", "reconcile"] as const) {
  test(`real ${type} handler resumes its domain checkpoint after SIGKILL`, async () => {
    const f = await fixture();
    f.repo.updateSettings({ ...f.repo.getSettings(), libraryRoot: f.root });
    const book = f.repo.createBook({ title: "Fixture", author: "Author" });
    f.repo.updateBookMetadata(book.id, { identifiers: { openlibrary: "/works/OL123W" } });
    const job = f.repo.createJob({ type, bookId: book.id });
    const phase =
      type === "cover_generation"
        ? "cover-generate"
        : type === "metadata_hydration"
          ? `metadata:${book.id}:fetch`
          : "reconcile-targets";
    const launch = (stopAt = "") =>
      Bun.spawn([process.execPath, "run", "test/fixtures/all-job-execution-process.ts", f.root, stopAt, "domain"], {
        cwd: path.resolve(import.meta.dir, "../.."),
        stdout: "pipe",
        stderr: "pipe"
      });
    const first = launch(phase);
    let second: ReturnType<typeof launch> | undefined;
    try {
      await until(async () =>
        (await readFile(`${f.root}/events`, "utf8").catch(() => "")).includes(`${job.id}:${phase}`)
      );
      const calls = f.db.query("SELECT COUNT(*) AS n FROM fixture_attempts").get();
      first.kill("SIGKILL");
      await first.exited;
      second = launch();
      const exit = await Promise.race([second.exited, Bun.sleep(8000).then(() => -1)]);
      expect(exit).toBe(0);
      expect(await new Response(second.stderr).text()).toBe("");
      expect(f.repo.getJob(job.id)?.status).toBe("succeeded");
      expect(f.db.query("SELECT COUNT(*) AS n FROM fixture_attempts").get()).toEqual(calls);
      if (type === "cover_generation")
        expect(await readFile(f.repo.getBookRow(book.id)!.cover_path!, "utf8")).toBe("fixture-image");
      if (type === "metadata_hydration")
        expect(f.repo.getBookRow(book.id)?.description).toBe("Saved domain description.");
    } finally {
      first.kill();
      second?.kill();
      if (second) await second.exited;
      f.db.close();
      await rm(f.root, { recursive: true, force: true });
    }
  }, 15000);
}

// Registration stays exhaustive above; these share the same substituted handler, not domain recovery.
const crashPolicies = [
  { type: "acquire", name: "serialized book-media lane", maxAttempts: 5, backoff: "exponential", locks: ["book:1:media:audio"] },
  { type: "chapter_analysis", name: "two-wide manifestation lane", maxAttempts: 5, backoff: "exponential", locks: ["manifestation:1", "book:1:media:audio"] },
  { type: "download", name: "wide release lane with long retry budget", maxAttempts: 20, backoff: "linear", locks: ["release:1", "book:1:media:audio"] },
  { type: "metadata_hydration", name: "wide unscoped work", maxAttempts: 5, backoff: "exponential", locks: [] },
  { type: "retention_cleanup", name: "wide exclusive maintenance", maxAttempts: 5, backoff: "exponential", locks: ["*"] },
  { type: "torrent_stewardship_collect", name: "serialized exclusive maintenance", maxAttempts: 20, backoff: "linear", locks: ["*"] },
] as const satisfies readonly { type: JobType; name: string; maxAttempts: number; backoff: string; locks: readonly string[] }[];

for (const policy of crashPolicies) {
  test(`${policy.name} survives real process death after a saved step`, async () => {
    const f = await fixture();
    const book = f.repo.createBook({ title: "Policy fixture", author: "Fixture" });
    const manifestation = f.repo.addManifestation({ bookId: book.id, kind: "audio" });
    const release = f.repo.createRelease({ bookId: book.id, mediaType: "audio", provider: "Fixture", title: "Policy fixture",
      infoHash: "a".repeat(40), url: "https://fixture.invalid/unused" });
    const job = f.repo.createJob({ type: policy.type, bookId: book.id, maxAttempts: policy.maxAttempts,
      releaseId: policy.type === "download" ? release.id : null,
      payload: { bookId: book.id, media: ["audio"], manifestationId: manifestation.id } });
    expect(jobLockKeys(f.repo, job)).toEqual([...policy.locks]);
    expect(executionRetryPolicy(job.max_attempts).backoff).toBe(policy.backoff);
    const launch = (stopAt = "") =>
      Bun.spawn([process.execPath, "run", "test/fixtures/all-job-execution-process.ts", f.root, stopAt], {
        cwd: path.resolve(import.meta.dir, "../.."),
        stdout: "pipe",
        stderr: "pipe"
      });
    const first = launch("research");
    let second: ReturnType<typeof launch> | undefined;
    try {
      await until(async () =>
        (await readFile(`${f.root}/events`, "utf8").catch(() => "")).includes(`${job.id}:research`)
      );
      const identity = f.repo.getJobExecution(job.id)!.run_id;
      first.kill("SIGKILL");
      await first.exited;
      second = launch();
      const exit = await Promise.race([second.exited, Bun.sleep(8000).then(() => -1)]);
      expect(exit).toBe(0);
      expect(await new Response(second.stderr).text()).toBe("");
      expect(f.repo.getJob(job.id)?.status).toBe("succeeded");
      expect(f.repo.getJobExecution(job.id)?.run_id).toBe(identity);
      expect(
        f.db.query("SELECT phase, COUNT(*) AS n FROM fixture_attempts GROUP BY phase ORDER BY phase").all()
      ).toEqual([
        { phase: "publish", n: 1 },
        { phase: "research", n: 1 }
      ]);
    } finally {
      first.kill();
      second?.kill();
      if (second) await second.exited;
      f.db.close();
      await rm(f.root, { recursive: true, force: true });
    }
  }, 15000);
}

test("migration preserves deleted job ID high-water and existing execution identities", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const saved = repo.createJob({ type: "acquire" });
  repo.attachJobExecution(saved.id, "original-run");
  const deleted = repo.createJob({ type: "metadata_hydration" });
  db.query("DELETE FROM jobs WHERE id = ?").run(deleted.id);
  db.exec("DELETE FROM schema_migrations WHERE id = 32");
  runMigrations(db);
  expect(repo.createJob({ type: "cover_generation" }).id).toBeGreaterThan(deleted.id);
  expect(repo.getJobExecution(saved.id)?.run_id).toBe("original-run");
  expect(db.query("PRAGMA foreign_key_check").all()).toHaveLength(0);
  db.close();
});

test("unsupported names fail visibly without blocking registered jobs", async () => {
  const f = await fixture();
  const unknown = f.repo.createJob({ type: "acquire" });
  f.db.query("UPDATE jobs SET type = 'unknown_future_type' WHERE id = ?").run(unknown.id);
  const good = f.repo.createJob({ type: "metadata_hydration" });
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async () => "done"
  });
  try {
    await runtime.reconcile();
    await until(() => f.repo.getJob(good.id)?.status === "succeeded");
    expect(f.repo.getJob(unknown.id)?.status).toBe("failed");
    expect(f.repo.getJob(unknown.id)?.error).toContain("Unsupported job type");
  } finally {
    await runtime.stop();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});

for (const [type, limit] of [
  ["acquire", 1],
  ["chapter_analysis", 2],
  ["metadata_hydration", 16]
] as const) {
  test(`${type} workflow limit ${limit} leaves room for nonconflicting work`, async () => {
    const f = await fixture();
    const entered: number[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const jobs = Array.from({ length: limit + 1 }, () => f.repo.createJob({ type }));
    const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
      processJob: async (ctx, job) => {
        await ctx.execution!.run("work", async () => {
          entered.push(job.id);
          if (job.type === type) await gate;
        });
        return "done";
      }
    });
    try {
      await runtime.reconcile();
      await until(() => entered.length === limit);
      const unrelated = f.repo.createJob({ type: type === "metadata_hydration" ? "cover_generation" : "metadata_hydration" });
      await runtime.reconcile();
      await until(() => f.repo.getJob(unrelated.id)?.status === "succeeded");
      expect(entered.filter((id) => jobs.some((job) => job.id === id))).toHaveLength(limit);
      release();
      await until(() => jobs.every((job) => f.repo.getJob(job.id)?.status === "succeeded"));
    } finally {
      release();
      await runtime.stop();
      f.db.close();
      await rm(f.root, { recursive: true, force: true });
    }
  });
}

test("shutdown revokes publication before waiting for in-flight callbacks to unwind", async () => {
  const f = await fixture();
  const book = f.repo.createBook({ title: "Original", author: "Fixture" });
  f.repo.createJob({ type: "metadata_hydration", bookId: book.id });
  let entered = false;
  let release!: () => void;
  let fenced = false;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    processJob: async (ctx) => {
      await ctx.execution!.run("delayed-publication", async () => {
        entered = true;
        await gate;
        try {
          ctx.repo.updateBookMetadata(book.id, { description: "must not publish" });
        } catch (error) {
          fenced = true;
          throw error;
        }
      });
      return "done";
    }
  });
  try {
    await runtime.reconcile();
    await until(() => entered);
    const stopped = runtime.stop();
    release();
    await stopped;
    expect(fenced).toBe(true);
    expect(f.repo.getBookRow(book.id)?.description).toBeNull();
  } finally {
    release();
    f.db.close();
    await rm(f.root, { recursive: true, force: true });
  }
});
