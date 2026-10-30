import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { defaultSettings } from "../../src/settings";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { handleJobFailure } from "../../src/worker/failure";

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
  expect(() => fenced.markJobSucceeded(job.id)).toThrow("lost ownership");
  expect(repo.listManifestationsByBook(book.id)).toHaveLength(0);
  repo.markJobCancelled(job.id);
  repo.markJobSucceeded(job.id);
  expect(repo.getJob(job.id)?.status).toBe("cancelled");
  expect(repo.ownJobExecution(job.id, "run", "third")).toBe(false);
  db.close();
});

test("durable import respects the legacy filesystem-refresh lane", async () => {
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

test("stale and duplicate failure projections do not consume retry attempts", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const job = repo.createJob({ type: "acquire", payload: {} });
  repo.attachJobExecution(job.id, "run");
  repo.ownJobExecution(job.id, "run", "current");
  const ctx = { repo, getSettings: () => defaultSettings(), onLog: () => {} };
  await handleJobFailure(ctx, job, new Error("old failure"), { runId: "run", token: "old" });
  expect(repo.getJob(job.id)?.attempt_count).toBe(0);
  await handleJobFailure(ctx, job, new Error("current failure"), { runId: "run", token: "current" });
  await handleJobFailure(ctx, job, new Error("duplicate projection"), { runId: "run", token: "current" });
  expect(repo.getJob(job.id)?.attempt_count).toBe(1);
  expect(repo.getJob(job.id)?.status).toBe("queued");
  db.close();
});
