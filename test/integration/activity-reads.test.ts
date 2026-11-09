import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createAdminWebRouter } from "../../src/rpc/admin-web-router";
import { jobJournalReader } from "../../src/worker/job-journal";

test("Activity hydrates only relevant disk journal runs and still reports broken links", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "activity-reads-"));
  const filename = path.join(root, "podible.sqlite");
  const db = new Database(filename);
  const execution = path.join(root, "execution.sqlite");
  const storage = new SQLiteStorage(execution);
  try {
    runMigrations(db);
    finalizeJobJournalMigration(db);
    const repo = new BooksRepo(db);
    await storage.initialize();
    const now = Date.now();
    const queued = repo.createJob({ type: "full_library_refresh" });
    const jobs = new Map();
    for (const status of ["completed", "cancelled", "sleeping", "failed"] as const) {
      const job = repo.createJob({ type: "indexer_feed_sync" });
      jobs.set(status, job.id);
      await storage.createRun({ id: status, workflow: "fixture", input: {}, status, idempotencyKey: status,
        createdAt: now, updatedAt: now, startedAt: now, finishedAt: null, output: null,
        error: null, wakeAt: status === "sleeping" ? now + 60_000 : null, waitingFor: null });
      repo.attachJobExecution(job.id, status);
    }
    const journal = jobJournalReader(execution);
    const hydrated: string[] = [];
    using readonly = new Database(filename, { readonly: true });
    const reader = new BooksRepo(readonly);
    reader.bindJobJournalReader({ ...journal, read: ids => {
      hydrated.push(...ids);
      return journal.read(ids);
    } });
    expect(reader.listActivityJobs().map(job => [job.id, job.status])).toEqual([
      [queued.id, "queued"], [jobs.get("sleeping"), "queued"], [jobs.get("failed"), "failed"],
    ]);
    expect(hydrated).toEqual(["sleeping", "failed"]);
    expect(reader.activityReads.newerJobs([jobs.get("sleeping")]).has(jobs.get("sleeping"))).toBe(true);

    const broken = repo.createJob({ type: "indexer_feed_sync" });
    repo.attachJobExecution(broken.id, "missing");
    expect(() => reader.listActivityJobs()).toThrow("Missing execution missing");
    const result = await createAdminWebRouter({ transmission: null }).routes.ops.handler(
      { repo: reader, startTime: now, request: new Request("http://localhost/ops"), session: { user_id: 1 } as never },
      { includeJobs: false },
    );
    expect(result.incidents.some(item => item.code === "execution-state-unknown")).toBe(true);
  } finally {
    await storage.close();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
