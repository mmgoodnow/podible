import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { processCoverGenerationJob } from "../../src/library/cover-generation";
import { BooksRepo } from "../../src/repo";
import type { WorkerContext } from "../../src/worker/context";
import { processMetadataHydrationJob } from "../../src/worker/metadata-hydration";
import { processReconcileJob } from "../../src/worker/maintenance";

function journalContext(repo: BooksRepo, results: Map<string, unknown>, failAfter?: string): WorkerContext {
  return {
    repo,
    getSettings: () => repo.getSettings(),
    onLog() {},
    execution: {
      key: "test-execution",
      assertActive() {},
      async sleep() {},
      async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
        if (results.has(name)) return results.get(name) as T;
        const result = await handler();
        results.set(name, result);
        if (name === failAfter) throw new Error("simulated interruption after checkpoint");
        return result;
      },
    },
  };
}

test("cover resumes from generated artifact without another paid request", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "podible-cover-execution-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.updateSettings({ ...repo.getSettings(), libraryRoot: root });
  const book = repo.createBook({ title: "Book", author: "Author" });
  const job = repo.createJob({ type: "cover_generation", bookId: book.id });
  const results = new Map<string, unknown>();
  let requests = 0;
  const deps = {
    generateImage: async () => { requests++; return Buffer.from("image"); },
    watermark: async (bytes: Uint8Array) => Buffer.from(bytes),
  };
  try {
    await expect(processCoverGenerationJob(journalContext(repo, results, "cover-generate"), job, deps)).rejects.toThrow("interruption");
    expect(repo.getBookRow(book.id)?.cover_path).toBeNull();
    await processCoverGenerationJob(journalContext(repo, results), job, deps);
    expect(requests).toBe(1);
    expect(await readFile(repo.getBookRow(book.id)!.cover_path!, "utf8")).toBe("image");
    expect(repo.getJob(job.id)?.status).toBe("succeeded");
  } finally {
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("cover publication preserves a user-selected cover and fences cancelled execution", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "podible-cover-fence-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.updateSettings({ ...repo.getSettings(), libraryRoot: root });
  const book = repo.createBook({ title: "Book", author: "Author" });
  const job = repo.createJob({ type: "cover_generation", bookId: book.id });
  const ctx = journalContext(repo, new Map());
  try {
    await processCoverGenerationJob(ctx, job, {
      generateImage: async () => {
        repo.updateBookMetadata(book.id, { coverPath: "/user/cover.jpg" });
        return Buffer.from("image");
      },
      watermark: async (bytes) => Buffer.from(bytes),
    });
    expect(repo.getBookRow(book.id)?.cover_path).toBe("/user/cover.jpg");
    const cancelledBook = repo.createBook({ title: "Cancelled", author: "Author" });
    const cancelledJob = repo.createJob({ type: "cover_generation", bookId: cancelledBook.id });
    ctx.execution!.assertActive = () => { throw new Error("cancelled"); };
    await expect(processCoverGenerationJob(ctx, cancelledJob, {
      generateImage: async () => Buffer.from("image"),
      watermark: async (bytes) => Buffer.from(bytes),
    })).rejects.toThrow("cancelled");
    expect(repo.getBookRow(cancelledBook.id)?.cover_path).toBeNull();
  } finally {
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("metadata resumes a saved fetch without refetching after interruption", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Book", author: "Author" });
  repo.updateBookMetadata(book.id, { identifiers: { openlibrary: "/works/OL123W" } });
  const job = repo.createJob({ type: "metadata_hydration" });
  const results = new Map<string, unknown>();
  const originalFetch = globalThis.fetch;
  let requests = 0;
  globalThis.fetch = (async (input: unknown) => {
    requests++;
    const url = new URL(String(input));
    if (url.pathname.endsWith("editions.json")) return Response.json({ entries: [] });
    if (url.pathname === "/search.json") return Response.json({ docs: [{ key: "/works/OL123W", title: "Book", author_name: ["Author"] }] });
    return Response.json({ description: "Saved description." });
  }) as typeof fetch;
  try {
    await expect(processMetadataHydrationJob(journalContext(repo, results, `metadata:${book.id}:fetch`), job)).rejects.toThrow();
    const firstRequests = requests;
    expect(firstRequests).toBeGreaterThan(0);
    await processMetadataHydrationJob(journalContext(repo, results), job);
    expect(requests).toBe(firstRequests);
    expect(repo.getBookRow(book.id)?.description).toBe("Saved description.");
  } finally {
    globalThis.fetch = originalFetch;
    db.close();
  }
});

test("reconciliation retry after a lost checkpoint does not duplicate its import handoff", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Book", author: "Author" });
  const release = repo.createRelease({
    bookId: book.id, mediaType: "ebook", provider: "fixture", title: "Book",
    url: "https://unused.invalid", infoHash: "c".repeat(40), status: "downloaded",
  });
  const job = repo.createJob({ type: "reconcile" });
  const results = new Map<string, unknown>();
  const step = `reconcile-import:${release.id}`;
  try {
    await expect(processReconcileJob(journalContext(repo, results, step), job)).rejects.toThrow("interruption");
    results.delete(step);
    await processReconcileJob(journalContext(repo, results), job);
    const imports = repo.listJobsByType("import");
    expect(imports).toHaveLength(1);
    expect(imports[0]?.release_id).toBe(release.id);
  } finally {
    db.close();
  }
});
