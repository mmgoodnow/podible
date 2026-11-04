import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { reconcileBookStorage } from "../../src/library/book-publication";
import { bookViewEntries } from "../../src/library/book-storage";
import { getBookTranscriptStatus } from "../../src/library/chapter-analysis";
import { streamAudioManifestation } from "../../src/library/media";

test("legacy books become rebuildable views without changing IDs, bytes, or paid transcript identity", async () => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "book-publication-"));
  const root = await fs.realpath(temporary);
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  try {
    const library = path.join(root, "library");
    const oldPath = path.join(library, "Writer", "Original", "Original.m4b");
    await fs.mkdir(path.dirname(oldPath), { recursive: true });
    const bytes = Buffer.from("synthetic audiobook bytes");
    await fs.writeFile(oldPath, bytes);
    const originalStat = await fs.stat(oldPath);
    const book = repo.createBook({ title: "Original", author: "Writer" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    const asset = repo.addAsset({ bookId: book.id, manifestationId: manifestation.id,
      kind: "single", mime: "audio/mp4", totalSize: bytes.length, durationMs: 10000,
      files: [{ path: oldPath, size: bytes.length, start: 0, end: bytes.length - 1, durationMs: 10000 }] });
    const originalFile = repo.getAssetFiles(asset.id)[0]!;
    const options = { apiKeyConfigured: true, manifestationId: manifestation.id };
    const before = await getBookTranscriptStatus(repo, book.id, options);
    const fingerprint = before.currentFingerprint!;
    repo.upsertChapterAnalysis({ manifestationId: manifestation.id, status: "succeeded", source: "agent",
      algorithmVersion: "fixture", fingerprint, chaptersJson: JSON.stringify([{ startTime: 0, title: "Chapter 1" }]) });
    repo.upsertManifestationTranscript({ manifestationId: manifestation.id, status: "succeeded", source: "openai",
      algorithmVersion: "fixture", fingerprint, transcriptPath: "/existing-paid-transcript.json" });

    const result = await reconcileBookStorage(repo, library);
    expect(result.failures).toEqual([]);
    expect(result.adopted).toBe(1);
    const file = repo.getAssetFiles(asset.id)[0]!;
    expect(file.id).toBe(originalFile.id);
    expect(file.identity_path).toBe(oldPath);
    expect(file.updated_at).toBe(originalFile.updated_at);
    expect(repo.getAsset(asset.id)).toEqual(asset);
    expect(await fs.stat(file.path).then((stat) => stat.ino)).toBe(originalStat.ino);
    expect((await fs.lstat(file.path)).isSymbolicLink()).toBe(false);
    expect((await getBookTranscriptStatus(repo, book.id, options)).status).toBe("current");
    expect(repo.getManifestationTranscript(manifestation.id)?.fingerprint).toBe(fingerprint);
    const firstView = bookViewEntries(repo, library)[0]!;
    expect((await fs.lstat(path.join(library, firstView.relativePath))).isSymbolicLink()).toBe(true);

    db.query("UPDATE books SET title = ?, author = ? WHERE id = ?").run("Renamed", "Renamed Writer", book.id);
    await reconcileBookStorage(repo, library);
    const renamedView = bookViewEntries(repo, library)[0]!;
    expect(renamedView.relativePath).not.toBe(firstView.relativePath);
    expect(await fs.lstat(path.join(library, firstView.relativePath)).catch(() => null)).toBeNull();
    expect(await fs.readFile(path.join(library, renamedView.relativePath))).toEqual(bytes);
    await fs.unlink(path.join(library, renamedView.relativePath));
    await reconcileBookStorage(repo, library);
    expect(await fs.readFile(path.join(library, renamedView.relativePath))).toEqual(bytes);
    expect(repo.getAssetFiles(asset.id)[0]?.path).toBe(file.path);
    expect((await getBookTranscriptStatus(repo, book.id, options)).currentFingerprint).toBe(fingerprint);
    expect(repo.listJobsByType("chapter_analysis")).toHaveLength(0);
    const response = await streamAudioManifestation(new Request("http://fixture/stream/m/1.m4a"), repo, manifestation,
      [{ asset, files: repo.getAssetFiles(asset.id) }]);
    expect(Buffer.from(await response.arrayBuffer())).toEqual(bytes);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
