import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { openDatabase } from "../../src/db";
import { BooksRepo } from "../../src/repo";

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-command-transaction-"));
  const filename = path.join(root, "app.sqlite"), db = openDatabase(filename);
  const repo = new BooksRepo(db);
  repo.finishJobJournalMigration();
  repo.bindJobJournalReader({ read: () => new Map() });
  const writer = new Database(filename);
  writer.exec("PRAGMA busy_timeout=1");
  return { db, repo, writer, async close() { writer.close(); db.close(); await rm(root, { recursive: true, force: true }); } };
}

for (const nested of [false, true]) test(`cancel/retry reserve the WAL writer before reading job state${nested ? " inside an outer repo transaction" : ""}`, async () => {
  const f = await fixture();
  try {
    const job = f.repo.createJob({ type: "metadata_hydration" });
    const original = f.repo.getJob.bind(f.repo);
    let armed = false, blocked = 0;
    f.repo.getJob = id => {
      const result = original(id);
      if (armed) {
        armed = false;
        try {
          f.writer.query("INSERT OR REPLACE INTO app_state(key,value_json,updated_at) VALUES('competing-worker','true','fixture')").run();
        } catch (error) {
          if ((error as { code?: string }).code !== "SQLITE_BUSY") throw error;
          blocked++;
        }
      }
      return result;
    };
    const execute = (action: () => unknown) => nested ? f.repo.transaction(action) : action();
    armed = true;
    execute(() => f.repo.markJobCancelled(job.id, "fixture cancellation"));
    expect(blocked).toBe(1);
    expect(f.repo.isJobCancellationRequested(job.id)).toBe(true);
    armed = true;
    execute(() => f.repo.retryJob(job.id));
    expect(blocked).toBe(2);
    expect(f.repo.jobExecutionGeneration(job.id)).toBe(1);
    expect(f.repo.listJobCommands().some(command => command.job_id === job.id && command.kind === "enqueue" && command.generation === 1)).toBe(true);
    expect(f.repo.getJsonState("competing-worker")).toBeNull();
    f.writer.query("INSERT INTO app_state(key,value_json,updated_at) VALUES('after-commit','true','fixture')").run();
    expect(f.repo.getJsonState<boolean>("after-commit")).toBe(true);
  } finally { await f.close(); }
});

test("immediate command transactions retain beforeWrite fencing and nested rollback semantics", async () => {
  const f = await fixture();
  try {
    const job = f.repo.createJob({ type: "metadata_hydration" });
    const before = f.repo.listJobCommands();
    expect(() => f.repo.transaction(() => {
      f.repo.markJobCancelled(job.id);
      f.repo.retryJob(job.id);
      throw new Error("rollback outer operation");
    })).toThrow("rollback outer operation");
    expect(f.repo.jobExecutionGeneration(job.id)).toBe(0);
    expect(f.repo.listJobCommands()).toEqual(before);
    f.repo.transaction(() => {
      expect(() => f.repo.transaction(() => {
        f.repo.markJobCancelled(job.id);
        throw new Error("rollback savepoint");
      })).toThrow("rollback savepoint");
      expect(f.repo.isJobCancellationRequested(job.id)).toBe(false);
      f.repo.setJsonState("outer-continues", true);
    });
    expect(f.repo.getJsonState<boolean>("outer-continues")).toBe(true);
    const fenced = new BooksRepo(f.db, () => { throw new Error("execution ownership lost"); });
    expect(() => fenced.markJobCancelled(job.id)).toThrow("execution ownership lost");
    expect(f.repo.listJobCommands()).toEqual(before);
    f.writer.query("INSERT INTO app_state(key,value_json,updated_at) VALUES('after-fence','true','fixture')").run();
    expect(f.repo.getJsonState<boolean>("after-fence")).toBe(true);
  } finally { await f.close(); }
});
