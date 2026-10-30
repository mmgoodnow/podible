import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

test("job RPC reads journal failures and cancellation without a reconciliation pump", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-job-api-authority-"));
  const db = new Database(path.join(root, "books.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "authority-admin", username: "admin", displayName: "Admin", isAdmin: true });
  const token = "journal-authority-test";
  repo.createSession(user.id, hashSessionToken(token), new Date(Date.now() + 60_000).toISOString());
  const failed = repo.createJob({ type: "download", maxAttempts: 1 });
  const cancelled = repo.createJob({ type: "cover_generation", maxAttempts: 1 });
  const runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), onLog() {} }, path.join(root, "execution.sqlite"), {
    processJob: async (ctx, job) => {
      if (job.id === failed.id) {
        await ctx.execution!.run("intentional-failure", () => { throw new Error("journal-owned failure"); });
      } else {
        await ctx.execution!.sleep("wait-for-cancellation", 60_000);
      }
      return "done";
    },
  });
  const handler = createPodibleFetchHandler(repo, Date.now());
  const rpc = async (method: string, params: unknown) => {
    const response = await handler(new Request("http://localhost/rpc", {
      method: "POST", headers: { "Content-Type": "application/json", cookie: `podible_session=${token}` },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    }));
    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.error).toBeUndefined();
    return body.result;
  };
  try {
    // Only dispatch once. API correctness must not require a status-copying loop.
    await runtime.reconcile();
    const failedRun = repo.getJobExecution(failed.id)!.run_id;
    const cancelledRun = repo.getJobExecution(cancelled.id)!.run_id;
    const deadline = Date.now() + 5_000;
    while ((await runtime.engine.getRunStatus(failedRun))?.run.status !== "failed") {
      if (Date.now() > deadline) throw new Error("Failure did not reach the execution journal");
      await Bun.sleep(10);
    }
    await runtime.engine.cancel(cancelledRun);
    const failedResult = await rpc("jobs.get", { jobId: failed.id });
    expect(failedResult.job.status).toBe("failed");
    expect(failedResult.job.error).toContain("journal-owned failure");
    expect(failedResult.job.attempt_count).toBe(1);
    const cancelledResult = await rpc("jobs.get", { jobId: cancelled.id });
    expect(cancelledResult.job.status).toBe("cancelled");
    const listed = await rpc("jobs.list", {});
    expect(listed.jobs.find((job: { id: number }) => job.id === failed.id)?.status).toBe("failed");
    expect(listed.jobs.find((job: { id: number }) => job.id === cancelled.id)?.status).toBe("cancelled");
    const download = await rpc("downloads.get", { jobId: failed.id });
    expect(download.job_status).toBe("failed");
    expect(download.job_error).toContain("journal-owned failure");
    const health = await rpc("system.health", {});
    expect(health.jobs).toEqual({ failed: 1, cancelled: 1 });
    expect(health.queueSize).toBe(0);
  } finally {
    await runtime.stop();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
