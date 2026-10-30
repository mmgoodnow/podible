import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("Command replay acceptance timed out");
    await Bun.sleep(15);
  }
}

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-command-replay-"));
  const domainPath = path.join(root, "books.sqlite");
  const executionPath = path.join(root, "execution.sqlite");
  let db = new Database(domainPath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  repo.ensureSettings();
  let failAcknowledgement: number | null = null;
  const proxied = () => new Proxy(repo, {
    get(target, key) {
      if (key === "acknowledgeJobCommand") return (id: number) => {
        if (id === failAcknowledgement) throw new Error("fixture crash before app acknowledgement");
        target.acknowledgeJobCommand(id);
      };
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return {
    executionPath,
    get repo() { return repo; },
    context() { return { repo: proxied(), getSettings: () => repo.getSettings(), onLog() {} }; },
    failAck(id: number | null) { failAcknowledgement = id; },
    reopen() { db.close(); db = new Database(domainPath); repo = new BooksRepo(db); },
    async close() { db.close(); await rm(root, { recursive: true, force: true }); },
  };
}

test("consumed retry receipt survives missing app ack and cannot retry a second failure", async () => {
  const f = await fixture();
  const job = f.repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
  let paidResearch = 0;
  let failures = 0;
  const start = () => createJobExecutionRuntime(f.context(), f.executionPath, {
    retryDelayMs: 1,
    processJob: async (ctx) => {
      await ctx.execution!.run("research", () => { paidResearch++; return { selected: "fixture" }; });
      await ctx.execution!.run("failing-step", () => { failures++; throw new Error(`fixture failure ${failures}`); });
      return "done";
    },
  });
  let runtime: Awaited<ReturnType<typeof start>> | undefined;
  try {
    runtime = await start();
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "failed");
    expect(failures).toBe(1);
    const runId = f.repo.getJobExecution(job.id)!.run_id;

    f.repo.retryJob(job.id);
    const retry = f.repo.listJobCommands().find((command) => command.kind === "retry")!;
    expect(retry).toBeDefined();
    f.failAck(retry.id);
    await expect(runtime.reconcile()).rejects.toThrow("fixture crash before app acknowledgement");
    await until(() => failures === 2 && f.repo.getJob(job.id)?.status === "failed");
    expect(f.repo.listJobCommands().map((command) => command.id)).toContain(retry.id);
    expect(f.repo.getJob(job.id)?.error).toContain("fixture failure 2");
    await runtime.stop();
    runtime = undefined;
    f.reopen();
    f.failAck(null);

    runtime = await start();
    await runtime.reconcile();
    await runtime.reconcile();
    await Bun.sleep(650); // More than two scheduler polls: detect an accidental third retry.
    expect(f.repo.listJobCommands()).toHaveLength(0);
    expect(f.repo.getJobExecution(job.id)!.run_id).toBe(runId);
    expect(f.repo.getJob(job.id)?.status).toBe("failed");
    expect(f.repo.getJob(job.id)?.error).toContain("fixture failure 2");
    expect(failures).toBe(2);
    expect(paidResearch).toBe(1);
  } finally {
    if (runtime) await runtime.stop();
    await f.close();
  }
}, 15_000);

test("an unacked original enqueue cannot suppress a later explicit retry", async () => {
  const f = await fixture();
  let attempts = 0;
  const runtime = await createJobExecutionRuntime(f.context(), f.executionPath, {
    processJob: async (ctx) => {
      await ctx.execution!.run("work", () => {
        if (++attempts === 1) throw new Error("first attempt failed");
      });
      return "done";
    },
  });
  try {
    const job = f.repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
    const enqueue = f.repo.listJobCommands()[0]!;
    f.failAck(enqueue.id);
    await expect(runtime.reconcile()).rejects.toThrow("fixture crash before app acknowledgement");
    await until(() => f.repo.getJob(job.id)?.status === "failed");
    f.repo.retryJob(job.id);
    expect(f.repo.listJobCommands().filter((command) => command.kind === "retry")).toHaveLength(1);
    f.failAck(null);
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    expect(attempts).toBe(2);
    expect(f.repo.listJobCommands()).toHaveLength(0);
  } finally {
    await runtime.stop();
    await f.close();
  }
}, 15_000);

test("cancel and retry before first dispatch only publishes the new generation", async () => {
  const f = await fixture();
  const generations: number[] = [];
  const runtime = await createJobExecutionRuntime(f.context(), f.executionPath, {
    processJob: async (ctx, job) => {
      await ctx.execution!.run("publish", () => {
        generations.push(ctx.repo.jobExecutionGeneration(job.id)!);
        return { published: true };
      });
      return "done";
    },
  });
  try {
    // Initialize the runtime first so creation goes through the current outbox,
    // rather than the historical-job migration path.
    const job = f.repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
    f.repo.markJobCancelled(job.id, "cancel before dispatch");
    f.repo.retryJob(job.id);
    f.repo.retryJob(job.id);
    expect(f.repo.getJobExecution(job.id)).toBeNull();
    expect(f.repo.jobExecutionGeneration(job.id)).toBe(1);
    const cancellation = f.repo.listJobCommands().find((command) => command.kind === "cancel")!;
    expect(cancellation.run_id).toBeNull();

    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    await runtime.reconcile();
    expect(generations).toEqual([1]);
    expect(f.repo.listJobCommands()).toHaveLength(0);
    const runId = f.repo.getJobExecution(job.id)!.run_id;
    const status = await runtime.engine.getRunStatus(runId);
    expect(status?.run.input).toEqual({ jobId: job.id, generation: 1 });
    expect(status?.run.status).toBe("completed");
  } finally {
    await runtime.stop();
    await f.close();
  }
}, 15_000);

test("unacked cancellation stays targeted at the old run after retry creates a new generation", async () => {
  const f = await fixture();
  const job = f.repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
  let publications = 0;
  const start = () => createJobExecutionRuntime(f.context(), f.executionPath, {
    processJob: async (ctx, current) => {
      if (ctx.repo.jobExecutionGeneration(current.id) === 0) {
        await ctx.execution!.sleep("await-cancellation", 60_000);
      }
      await ctx.execution!.run("publish", () => { publications++; return { published: true }; });
      return "done";
    },
  });
  let runtime: Awaited<ReturnType<typeof start>> | undefined;
  try {
    runtime = await start();
    await runtime.reconcile();
    await until(async () => {
      const link = f.repo.getJobExecution(job.id);
      return Boolean(link && (await runtime!.engine.getRunStatus(link.run_id))?.run.status === "sleeping");
    });
    const oldRun = f.repo.getJobExecution(job.id)!.run_id;
    f.repo.markJobCancelled(job.id, "fixture cancellation");
    f.repo.retryJob(job.id);
    const cancel = f.repo.listJobCommands().find((command) => command.kind === "cancel")!;
    expect(cancel.run_id).toBe(oldRun);
    expect(f.repo.jobExecutionGeneration(job.id)).toBe(1);
    f.failAck(cancel.id);
    await expect(runtime.reconcile()).rejects.toThrow("fixture crash before app acknowledgement");
    expect((await runtime.engine.getRunStatus(oldRun))?.run.status).toBe("cancelled");
    await runtime.stop();
    runtime = undefined;
    f.reopen();
    f.failAck(null);

    runtime = await start();
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    const newRun = f.repo.getJobExecution(job.id)!.run_id;
    expect(newRun).not.toBe(oldRun);
    await runtime.reconcile();
    expect((await runtime.engine.getRunStatus(oldRun))?.run.status).toBe("cancelled");
    expect((await runtime.engine.getRunStatus(newRun))?.run.status).toBe("completed");
    expect(f.repo.listJobCommands()).toHaveLength(0);
    expect(publications).toBe(1);
  } finally {
    if (runtime) await runtime.stop();
    await f.close();
  }
}, 15_000);
