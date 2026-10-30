import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { processCoverGenerationJob } from "../../src/library/cover-generation";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

test("real Reflow cover retry after reopening reuses the paid image artifact", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-cover-reflow-"));
  const databasePath = path.join(root, "books.sqlite");
  let db = new Database(databasePath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  repo.updateSettings({ ...repo.getSettings(), libraryRoot: path.join(root, "library") });
  const book = repo.createBook({ title: "Cover recovery", author: "Fixture" });
  const job = repo.createJob({ type: "cover_generation", bookId: book.id, maxAttempts: 1 });
  let requests = 0;
  let watermarkWorks = false;
  const start = () => createJobExecutionRuntime(
    { repo, getSettings: () => repo.getSettings(), onLog() {} },
    path.join(root, "execution.sqlite"),
    {
      retryDelayMs: 1,
      processJob: (ctx, row) => processCoverGenerationJob(ctx, row, {
        generateImage: async () => { requests++; return Buffer.from("saved image"); },
        watermark: async (bytes) => {
          if (!watermarkWorks) throw new Error("watermark temporarily unavailable");
          return Buffer.from(bytes);
        },
      }),
    }
  );
  let runtime = await start();
  const untilStatus = async (status: string) => {
    const deadline = Date.now() + 5_000;
    while (repo.getJob(job.id)?.status !== status) {
      if (Date.now() > deadline) throw new Error(`Timed out waiting for ${status}`);
      await runtime.reconcile();
      await Bun.sleep(10);
    }
  };
  try {
    await untilStatus("failed");
    expect(requests).toBe(1);
    expect(repo.getBookRow(book.id)?.cover_path).toBeNull();
    const runId = repo.getJobExecution(job.id)!.run_id;
    await runtime.stop();
    db.close();
    db = new Database(databasePath);
    repo = new BooksRepo(db);
    watermarkWorks = true;
    repo.retryJob(job.id);
    runtime = await start();
    await untilStatus("succeeded");
    expect(repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(requests).toBe(1);
    expect(await readFile(repo.getBookRow(book.id)!.cover_path!, "utf8")).toBe("saved image");
  } finally {
    await runtime.stop();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
