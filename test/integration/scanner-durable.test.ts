import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { scanLibraryRoot } from "../../src/library/scanner";
import type { WorkerContext } from "../../src/worker/context";

async function fixture(run: (repo: BooksRepo, root: string, db: Database) => Promise<void>) {
  const root = await mkdtemp(path.join(os.tmpdir(), "scanner-durable-"));
  const db = new Database(path.join(root, "library.sqlite"));
  runMigrations(db);
  const fetch = globalThis.fetch;
  // No metadata or cover requests leave this test process.
  globalThis.fetch = (async () => new Response(JSON.stringify({
    docs: [{ key: "/works/OL123W", title: "Fixture", author_name: ["Author"], language: ["eng"] }],
  }))) as unknown as typeof fetch;
  try {
    await run(new BooksRepo(db), path.join(root, "books"), db);
  } finally {
    globalThis.fetch = fetch;
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}

async function addPdf(root: string, title: string) {
  const dir = path.join(root, "Author", title);
  await mkdir(dir, { recursive: true });
  await writeFile(path.join(dir, "book.pdf"), "%PDF-test");
  return dir;
}

function context(repo: BooksRepo, run: NonNullable<WorkerContext["execution"]>["run"]): WorkerContext {
  return { repo, getSettings: () => repo.getSettings(), execution: {
    key: "scan-test", run, sleep: async () => {}, assertActive: () => {},
  } };
}

test("scanner resume reuses completed directory stages and stable discovery", async () => {
  await fixture(async (repo, root, db) => {
    const firstDir = await addPdf(root, "A");
    await addPdf(root, "B");
    db.exec("CREATE TABLE scan_checkpoints (name TEXT PRIMARY KEY, result TEXT NOT NULL)");
    let interrupted = false;
    let inStep = false;
    const executed: string[] = [];
    const ctx = context(repo, async (name, handler) => {
      expect(inStep).toBe(false);
      const saved = db.query("SELECT result FROM scan_checkpoints WHERE name = ?").get(name) as { result: string } | null;
      if (saved) return JSON.parse(saved.result).value;
      if (!interrupted && name.includes("B") && name.endsWith(":files")) {
        interrupted = true;
        throw new Error("interrupted before second directory");
      }
      executed.push(name);
      inStep = true;
      let value;
      try {
        value = await handler();
      } finally {
        inStep = false;
      }
      db.query("INSERT INTO scan_checkpoints VALUES (?, ?)").run(name, JSON.stringify({ value }));
      return value;
    });
    await expect(scanLibraryRoot(repo, root, ctx)).rejects.toThrow("interrupted");
    expect(repo.listAssetsByBook(repo.findBookByTitleAuthor("A", "Author")!.id)).toHaveLength(1);
    const completed = [...executed];
    // A disappears between activations; replay must use its saved results.
    await rm(firstDir, { recursive: true });
    const result = await scanLibraryRoot(repo, root, ctx);
    expect(result).toEqual({ booksCreated: 2, assetsCreated: 2 });
    for (const name of completed) expect(executed.filter((value) => value === name)).toHaveLength(1);
    expect(repo.listAssetsByBook(repo.findBookByTitleAuthor("B", "Author")!.id)).toHaveLength(1);
  });
});

test("scanner retries publication after a lost checkpoint without duplicate assets", async () => {
  await fixture(async (repo, root) => {
    await addPdf(root, "Book");
    let failed = false;
    const ctx = context(repo, async (name, handler) => {
      const result = await handler();
      if (!failed && name.endsWith(":ebook")) {
        failed = true;
        throw new Error("checkpoint not persisted");
      }
      return result;
    });
    await expect(scanLibraryRoot(repo, root, ctx)).rejects.toThrow("checkpoint not persisted");
    await scanLibraryRoot(repo, root, ctx);
    const book = repo.findBookByTitleAuthor("Book", "Author")!;
    expect(repo.listAssetsByBook(book.id)).toHaveLength(1);
    expect(repo.listManifestationsByBook(book.id)).toHaveLength(1);
  });
});

test.each(["book.pdf", "book.mp3", "book.m4b"])("scanner rolls back %s manifestation and asset together on publication failure", async (filename) => {
  await fixture(async (repo, root) => {
    const dir = path.join(root, "Author", "Book");
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, filename), "fixture");
    const addAsset = repo.addAsset.bind(repo);
    repo.addAsset = (input) => { addAsset(input); throw new Error("publication failed"); };
    await expect(scanLibraryRoot(repo, root)).rejects.toThrow("publication failed");
    const book = repo.findBookByTitleAuthor("Book", "Author")!;
    expect(repo.listManifestationsByBook(book.id)).toHaveLength(0);
    expect(repo.listAssetsByBook(book.id)).toHaveLength(0);
    repo.addAsset = addAsset;
    await scanLibraryRoot(repo, root);
    expect(repo.listAssetsByBook(book.id)).toHaveLength(1);
  });
});

test("scanner uses the context repository ownership guard before publication", async () => {
  await fixture(async (repo, root) => {
    await addPdf(root, "Book");
    let active = true;
    const scoped = repo.forExecution(() => { if (!active) throw new Error("lost ownership"); });
    const ctx = context(scoped, async (name, handler) => {
      if (name.endsWith(":ebook")) active = false;
      return handler();
    });
    await expect(scanLibraryRoot(repo, root, ctx)).rejects.toThrow("lost ownership");
    const book = repo.findBookByTitleAuthor("Book", "Author")!;
    expect(repo.listAssetsByBook(book.id)).toHaveLength(0);
    expect(repo.listManifestationsByBook(book.id)).toHaveLength(0);
  });
});
