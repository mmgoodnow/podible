import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { KindlingRepo } from "../../src/kindling/repo";

function setupRepo(): { db: Database; repo: KindlingRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new KindlingRepo(db);
  return { db, repo };
}

describe("kindling repo", () => {
  test("creates and updates settings", () => {
    const { db, repo } = setupRepo();

    const initial = repo.ensureSettings();
    expect(initial.auth.mode).toBe("apikey");
    expect(initial.auth.key.length).toBeGreaterThan(20);

    const next = {
      ...initial,
      libraryRoot: "/tmp/library",
      auth: { ...initial.auth, mode: "local" as const },
    };
    const saved = repo.updateSettings(next);
    expect(saved.libraryRoot).toBe("/tmp/library");
    expect(saved.auth.mode).toBe("local");

    const fetched = repo.getSettings();
    expect(fetched.libraryRoot).toBe("/tmp/library");
    expect(fetched.auth.mode).toBe("local");

    db.close();
  });

  test("requeues running jobs and claims queued work in order", () => {
    const { db, repo } = setupRepo();

    const running = repo.createJob({ type: "download", status: "running" });
    const first = repo.createJob({ type: "download", status: "queued" });
    const second = repo.createJob({ type: "import", status: "queued" });

    expect(repo.requeueRunningJobs()).toBe(1);

    const claimed1 = repo.claimNextRunnableJob();
    expect(claimed1?.id).toBe(running.id);
    expect(claimed1?.status).toBe("running");

    const claimed2 = repo.claimNextRunnableJob();
    expect(claimed2?.id).toBe(first.id);

    const claimed3 = repo.claimNextRunnableJob();
    expect(claimed3?.id).toBe(second.id);

    const claimed4 = repo.claimNextRunnableJob();
    expect(claimed4).toBeNull();

    db.close();
  });

  test("derives partial status across media", () => {
    const { db, repo } = setupRepo();

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Dune audio",
      mediaType: "audio",
      infoHash: "aaa111",
      url: "magnet:?xt=urn:btih:aaa111",
      status: "downloaded",
    });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1000,
      files: [
        {
          path: "/tmp/a.mp3",
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1000,
          title: "Part 1",
        },
      ],
    });

    const hydrated = repo.getBook(book.id);
    expect(hydrated?.audioStatus).toBe("imported");
    expect(hydrated?.ebookStatus).toBe("open");
    expect(hydrated?.status).toBe("partial");

    db.close();
  });
});
