import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { defaultSettings } from "../../src/settings";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { notifyJobFailure } from "../../src/worker/failure";

test("job handoffs and manifestations have stable retry identities", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Acceptance", author: "Test" });
  const makeJob = () => repo.createJob({ type: "import", bookId: book.id, idempotencyKey: "download:1:import" });
  expect(makeJob().id).toBe(makeJob().id);
  const makeManifestation = () => repo.ensureManifestation("acquire:1:audio:0", { bookId: book.id, kind: "audio" });
  expect(makeManifestation().id).toBe(makeManifestation().id);
  expect(repo.listManifestationsByBook(book.id)).toHaveLength(1);
  db.close();
});

test("superseded execution cannot publish or complete after cancellation", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Acceptance", author: "Test" });
  const job = repo.createJob({ type: "acquire", bookId: book.id });
  repo.attachJobExecution(job.id, "run");
  expect(repo.ownJobExecution(job.id, "run", "old")).toBe(true);
  const fenced = repo.forExecution(() => {
    if (repo.getJobExecution(job.id)?.owner_token !== "old" || repo.getJob(job.id)?.status === "cancelled") {
      throw new Error("Execution lost ownership");
    }
  });
  expect(repo.ownJobExecution(job.id, "run", "new")).toBe(true);
  expect(() => fenced.addManifestation({ bookId: book.id, kind: "audio" })).toThrow("lost ownership");
  expect(() => fenced.createJob({ type: "import" })).toThrow("lost ownership");
  expect(repo.listManifestationsByBook(book.id)).toHaveLength(0);
  repo.markJobCancelled(job.id);
  expect(repo.isJobCancellationRequested(job.id)).toBe(true);
  expect(repo.ownJobExecution(job.id, "run", "third")).toBe(false);
  db.close();
});

test("durable import respects the filesystem-refresh lane", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-execution-lane-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.updateSettings(defaultSettings({ libraryRoot: path.join(root, "library") }));
  const book = repo.createBook({ title: "Lane fixture", author: "Test" });
  const source = path.join(root, "source.pdf");
  await writeFile(source, "%PDF-fixture");
  const release = repo.createRelease({
    bookId: book.id,
    mediaType: "ebook",
    provider: "fixture",
    title: book.title,
    infoHash: "b".repeat(40),
    url: "https://unused.invalid",
    status: "downloaded"
  });
  const job = repo.createJob({ type: "import", bookId: book.id, releaseId: release.id, payload: { basePath: source } });
  const activeLocks = new Set(["*"]);
  const runtime = await createJobExecutionRuntime(
    { repo, getSettings: () => repo.getSettings(), activeLocks, onLog: () => {} },
    path.join(root, "execution.sqlite")
  );
  try {
    await runtime.reconcile();
    await Bun.sleep(100);
    expect(repo.getJob(job.id)?.status).toBe("running");
    expect(repo.listAssetsByBook(book.id)).toHaveLength(0);
    activeLocks.clear();
    const deadline = Date.now() + 3_000;
    while (repo.getJob(job.id)?.status !== "succeeded" && Date.now() < deadline) await Bun.sleep(30);
    expect(repo.listAssetsByBook(book.id)).toHaveLength(1);
    expect(repo.getJob(job.id)?.status).toBe("succeeded");
  } finally {
    activeLocks.clear();
    await runtime.stop();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("failure notifications cannot mutate authoritative execution status or attempts", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const job = repo.createJob({ type: "acquire", payload: {}, maxAttempts: 1 });
  const ctx = { repo, getSettings: () => defaultSettings(), onLog: () => {} };
  const runtime = await createJobExecutionRuntime(ctx, ":memory:", {
    processJob: async (ctx) => {
      await ctx.execution!.run("fails", () => {
        throw new Error("original failure");
      });
      return "done";
    }
  });
  try {
    await runtime.reconcile();
    const id = repo.getJobExecution(job.id)!.run_id;
    await runtime.engine.waitForRun(id);
    const original = repo.getJob(job.id);
    await notifyJobFailure(ctx, job, new Error("old failure"));
    await notifyJobFailure(ctx, job, new Error("duplicate failure"));
    expect(repo.getJob(job.id)).toEqual(original);
    expect(original?.attempt_count).toBe(1);
    expect(original?.status).toBe("failed");
  } finally {
    await runtime.stop();
    db.close();
  }
});
