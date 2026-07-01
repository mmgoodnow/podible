import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/db";
import { buildGeneratedCoverPrompt, queueMissingCoverGeneration } from "../../src/library/cover-generation";
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
});
