import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/books/db";
import { importReleaseFromPath } from "../../src/books/importer";
import { BooksRepo } from "../../src/books/repo";

const tempDirs: string[] = [];

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (!dir) continue;
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(prefix: string): string {
  const dir = mkdtempSync(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  return { db, repo: new BooksRepo(db) };
}

describe("importer path collisions", () => {
  test("allocates a unique library path when a different source collides with an existing import path", async () => {
    const { db, repo } = setupRepo();
    const libraryRoot = tempDir("books-lib-");
    const srcA = tempDir("books-src-a-");
    const srcB = tempDir("books-src-b-");

    const sourceA = path.join(srcA, "book.epub");
    const sourceB = path.join(srcB, "book.epub");
    writeFileSync(sourceA, "first-ebook-content");
    writeFileSync(sourceB, "second-ebook-content");

    const book = repo.createBook({ title: "Twilight", author: "Stephenie Meyer" });
    const release1 = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight [EPUB]",
      mediaType: "ebook",
      infoHash: "1111111111111111111111111111111111111111",
      url: "https://example.com/1.torrent",
      status: "downloaded",
    });
    const release2 = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight (other source) [EPUB]",
      mediaType: "ebook",
      infoHash: "2222222222222222222222222222222222222222",
      url: "https://example.com/2.torrent",
      status: "downloaded",
    });

    const import1 = await importReleaseFromPath(repo, release1, sourceA, libraryRoot);
    const import2 = await importReleaseFromPath(repo, release2, sourceB, libraryRoot);

    expect(import1.linkedFiles).toHaveLength(1);
    expect(import2.linkedFiles).toHaveLength(1);
    expect(import1.linkedFiles[0]).not.toBe(import2.linkedFiles[0]);
    expect(path.basename(import1.linkedFiles[0] ?? "")).toBe("Twilight.epub");
    expect(path.basename(import2.linkedFiles[0] ?? "")).toBe("Twilight (2).epub");

    const assets = repo.listAssetsByBook(book.id);
    expect(assets).toHaveLength(2);
    const asset1 = assets.find((asset) => asset.source_release_id === release1.id);
    const asset2 = assets.find((asset) => asset.source_release_id === release2.id);
    const files1 = repo.getAssetFiles(asset1!.id);
    const files2 = repo.getAssetFiles(asset2!.id);
    expect(files1[0]?.path).toBe(import1.linkedFiles[0]);
    expect(files2[0]?.path).toBe(import2.linkedFiles[0]);
    expect(files1[0]?.path).not.toBe(files2[0]?.path);
    expect(files1[0]?.source_path).toBe(sourceA);
    expect(files2[0]?.source_path).toBe(sourceB);

    db.close();
  });
});
