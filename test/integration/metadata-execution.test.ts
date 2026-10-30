import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

test("Reflow metadata failures preserve independent book progress and replay completed fetches", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-metadata-execution-"));
  const db = new Database(path.join(root, "books.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const missing = repo.createBook({ title: "Missing", author: "Author" });
  const secondMissing = repo.createBook({ title: "Missing Two", author: "Author" });
  const found = repo.createBook({ title: "Found", author: "Author" });
  repo.updateBookMetadata(found.id, { identifiers: { openlibrary: "/works/OL456W" } });
  const job = repo.createJob({ type: "metadata_hydration", maxAttempts: 1 });
  const originalFetch = globalThis.fetch;
  let fixed = false;
  let foundDetails = 0;
  globalThis.fetch = (async (input: unknown) => {
    const url = new URL(String(input));
    if (url.pathname === "/search.json") {
      const query = url.searchParams.get("q") ?? "";
      const isMissing = query.includes("Missing");
      return Response.json({ docs: isMissing && !fixed ? [] : [{
        key: isMissing ? (query.includes("Two") ? "/works/OL234W" : "/works/OL123W") : "/works/OL456W",
        title: isMissing ? "Missing" : "Found", author_name: ["Author"],
      }] });
    }
    if (url.pathname.endsWith("editions.json")) return Response.json({ entries: [] });
    if (url.pathname === "/works/OL456W.json") foundDetails++;
    return Response.json({ description: "Metadata is available." });
  }) as typeof fetch;
  const start = () => createJobExecutionRuntime(
    { repo, getSettings: () => repo.getSettings(), onLog() {} }, path.join(root, "execution.sqlite"),
    { retryDelayMs: 1 }
  );
  let runtime = await start();
  const until = async (status: string) => {
    const deadline = Date.now() + 5000;
    while (repo.getJob(job.id)?.status !== status) {
      if (Date.now() > deadline) throw new Error(`Timed out waiting for ${status}`);
      await runtime.reconcile();
      await Bun.sleep(10);
    }
  };
  try {
    await until("failed");
    expect(repo.getBookRow(found.id)?.description).toBe("Metadata is available.");
    expect(repo.getBookRow(missing.id)?.openlibrary_hydrated_at).toBeNull();
    expect(repo.getBookRow(secondMissing.id)?.openlibrary_hydrated_at).toBeNull();
    const callsBeforeRetry = foundDetails;
    expect(callsBeforeRetry).toBeGreaterThan(0);
    const runId = repo.getJobExecution(job.id)!.run_id;
    await runtime.stop();
    fixed = true;
    repo.retryJob(job.id);
    runtime = await start();
    await until("succeeded");
    expect(repo.getBookRow(missing.id)?.description).toBe("Metadata is available.");
    expect(repo.getBookRow(secondMissing.id)?.description).toBe("Metadata is available.");
    expect(repo.getJobExecution(job.id)?.run_id).toBe(runId);
    expect(foundDetails).toBe(callsBeforeRetry);
  } finally {
    await runtime.stop();
    globalThis.fetch = originalFetch;
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
