import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import {
  buildGeneratedCoverPrompt,
  createAiBadgePam,
  queueMissingCoverGeneration,
  watermarkGeneratedCover,
  watermarkLegacyGeneratedCovers,
} from "../../src/library/cover-generation";
import { BooksRepo } from "../../src/repo";

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  return { db, repo };
}

describe("generated cover art", () => {
  test("prompt includes rich book metadata and forbids cover text", () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Hyperion", author: "Dan Simmons" });
      repo.updateBookMetadata(book.id, {
        publishedAt: "1989",
        language: "eng",
        descriptionHtml: "<p>A pilgrimage across a distant world to face the Shrike.</p>",
        identifiers: { openlibrary: "/works/OL45804W" },
      });

      const prompt = buildGeneratedCoverPrompt(repo.getBookRow(book.id)!, "Kindling");

      expect(prompt).toContain("Title: Hyperion");
      expect(prompt).toContain("Author: Dan Simmons");
      expect(prompt).toContain("A pilgrimage across a distant world");
      expect(prompt).toContain("/works/OL45804W");
      expect(prompt).toContain("Do not include readable text");
      expect(prompt).toContain("Do not imitate a known published cover");
    } finally {
      db.close();
    }
  });

  test("queues one missing-cover generation job only when OpenAI is configured", () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

      expect(queueMissingCoverGeneration(repo, book.id)).toBeNull();

      repo.updateSettings({
        ...repo.getSettings(),
        agents: {
          ...repo.getSettings().agents,
          apiKey: "test-key",
        },
      });

      const first = queueMissingCoverGeneration(repo, book.id);
      const second = queueMissingCoverGeneration(repo, book.id);
      expect(first?.type).toBe("cover_generation");
      expect(second?.id).toBe(first?.id);
      expect(repo.listJobsByType("cover_generation")).toHaveLength(1);

      repo.updateBookMetadata(book.id, { coverPath: "/tmp/cover.jpg" });
      expect(queueMissingCoverGeneration(repo, book.id)).toBeNull();
    } finally {
      db.close();
    }
  });

  test("renders a translucent AI badge onto generated cover bytes", async () => {
    const output = await watermarkGeneratedCover(createSolidPpm(320, 480));

    expect(output.subarray(0, 3).equals(Buffer.from([0xff, 0xd8, 0xff]))).toBe(true);
    expect(output.length).toBeGreaterThan(1_000);

    const badge = createAiBadgePam();
    expect(badge.subarray(0, 2).toString("ascii")).toBe("P7");
    expect(badge.indexOf(Buffer.from("TUPLTYPE RGB_ALPHA"))).toBeGreaterThan(-1);
  });

  test("watermarks legacy generated covers once and records the marked filename", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "podible-cover-watermark-test-"));
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const legacyPath = path.join(root, "cover.generated.jpg");
      await writeFile(legacyPath, createSolidPpm(320, 480));
      repo.updateBookMetadata(book.id, { coverPath: legacyPath });

      expect(await watermarkLegacyGeneratedCovers(repo)).toEqual({ updated: 1, failed: 0 });

      const updatedPath = repo.getBookRow(book.id)?.cover_path;
      expect(path.basename(updatedPath ?? "")).toBe("cover.generated.ai.jpg");
      expect((await readFile(updatedPath!)).subarray(0, 3).equals(Buffer.from([0xff, 0xd8, 0xff]))).toBe(true);
      await expect(stat(legacyPath)).rejects.toThrow();
      expect(await watermarkLegacyGeneratedCovers(repo)).toEqual({ updated: 0, failed: 0 });
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});

function createSolidPpm(width: number, height: number): Buffer {
  const header = Buffer.from(`P6\n${width} ${height}\n255\n`, "ascii");
  const pixels = Buffer.alloc(width * height * 3);
  for (let offset = 0; offset < pixels.length; offset += 3) {
    pixels[offset] = 180;
    pixels[offset + 1] = 104;
    pixels[offset + 2] = 72;
  }
  return Buffer.concat([header, pixels]);
}
