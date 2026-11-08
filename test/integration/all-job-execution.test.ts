import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { JobType } from "../../src/app-types";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime, EXECUTION_JOB_TYPES, executionRetryPolicy } from "../../src/worker/execution";
import { jobLockKeys } from "../../src/worker/locks";

const types = [
  "indexer_feed_sync", "movie_feed_match", "book_feed_match",
  "movie_acquire", "movie_reconcile", "movie_shadow_sync", "movie_shadow_evaluate", "movie_dual_write_sync",
  "tv_acquire", "tv_sync", "tv_reconcile", "tv_shadow_sync", "tv_shadow_evaluate", "tv_feed_match",
  "acquire",
  "download",
  "import",
  "reconcile",
  "full_library_refresh",
  "chapter_analysis",
  "cover_generation",
  "metadata_hydration",
  "retention_cleanup",
  "tv_retention_cleanup",
  "tv_retention_pressure",
  "torrent_stewardship_collect",
  "torrent_reuse", "torrent_reuse_feed", "torrent_metrics_collect",
] as const satisfies readonly JobType[];
const exhaustive: Exclude<JobType, (typeof types)[number]> extends never ? true : never = true;
void exhaustive;

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
