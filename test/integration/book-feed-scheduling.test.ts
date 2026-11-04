import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { runWorker } from "../../src/worker";

test("a book-only installation schedules shared RSS once, without per-book search jobs", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "book-feed-schedule-"));
  const db = new Database(path.join(root, "domain.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.torznab = [
    { name: "Book feed", baseUrl: "https://fixture.invalid/api" },
  ];
  repo.updateSettings(settings);
  repo.createBook({ title: "A wanted book", author: "Synthetic Author" });
  expect(repo.movies.hasTargets()).toBe(false);
  let stop = false;
  const seen: string[] = [];
  const worker = runWorker(
    {
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stop,
      onLog() {},
      indexerFeedMediaSources: async (media) => {
        seen.push(media);
        return [];
      },
    },
    { databasePath: path.join(root, "execution.sqlite"), retryDelayMs: 1 },
  );
  try {
    const deadline = Date.now() + 5000;
    while (
      repo.listJobsByType("indexer_feed_sync")[0]?.status !== "succeeded"
    ) {
      if (Date.now() > deadline)
        throw new Error("Book-only RSS scheduling timed out");
      await Bun.sleep(20);
    }
    await Bun.sleep(220);
    expect(seen).toContain("audio");
    expect(seen).toContain("ebook");
    expect(repo.listJobsByType("indexer_feed_sync")).toHaveLength(1);
    expect(repo.listJobsByType("acquire")).toHaveLength(0);
    expect(repo.listJobsByType("movie_acquire")).toHaveLength(0);
    expect(repo.listJobsByType("movie_reconcile")).toHaveLength(0);
  } finally {
    stop = true;
    await worker;
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
