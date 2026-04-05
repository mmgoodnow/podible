import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomBytes } from "node:crypto";

import { runMigrations } from "../../src/books/db";
import { BooksRepo } from "../../src/books/repo";

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  return { db, repo };
}

describe("books repo", () => {
  test("creates and updates settings", () => {
    const { db, repo } = setupRepo();

    const initial = repo.ensureSettings();
    expect(initial.auth.mode).toBe("plex");
    expect(initial.auth.appRedirectURIs).toEqual([]);
    expect(initial.recovery.stalledTorrentMinutes).toBe(10);
    expect(initial.notifications.pushover.enabled).toBe(false);

    const next = {
      ...initial,
      libraryRoot: "/tmp/library",
      auth: { ...initial.auth, mode: "plex" as const },
      recovery: { stalledTorrentMinutes: 15 },
      notifications: {
        pushover: {
          enabled: true,
          apiToken: "token",
          userKey: "user",
        },
      },
    };
    const saved = repo.updateSettings(next);
    expect(saved.libraryRoot).toBe("/tmp/library");
    expect(saved.auth.mode).toBe("plex");
    expect(saved.recovery.stalledTorrentMinutes).toBe(15);
    expect(saved.notifications.pushover.enabled).toBe(true);

    const fetched = repo.getSettings();
    expect(fetched.libraryRoot).toBe("/tmp/library");
    expect(fetched.auth.mode).toBe("plex");
    expect(fetched.recovery.stalledTorrentMinutes).toBe(15);
    expect(fetched.notifications.pushover.userKey).toBe("user");

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

  test("creates users and resolves sessions by token hash", () => {
    const { db, repo } = setupRepo();
    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: "plex-123",
      username: "andy",
      displayName: "Andy",
      isAdmin: true,
    });
    const tokenHash = randomBytes(16).toString("hex");
    repo.createSession(user.id, tokenHash, new Date(Date.now() + 60_000).toISOString());
    const session = repo.getSessionByTokenHash(tokenHash);
    expect(session?.user_id).toBe(user.id);
    expect(session?.username).toBe("andy");
    expect(session?.is_admin).toBe(1);
    expect(session?.kind).toBe("browser");
    db.close();
  });

  test("stores word count on books", () => {
    const { db, repo } = setupRepo();
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { wordCount: 188_000 });
    const hydrated = repo.getBook(book.id);
    expect(hydrated?.wordCount).toBe(188_000);
    db.close();
  });

  test("stores and loads json app state through the main repo", () => {
    const { db, repo } = setupRepo();
    repo.setJsonState("probe_cache_v1", [{ file: "/tmp/a.mp3", mtimeMs: 123, data: null, error: "boom" }]);
    expect(
      repo.getJsonState<Array<{ file: string; mtimeMs: number; data: null; error: string }>>("probe_cache_v1")
    ).toEqual([
      { file: "/tmp/a.mp3", mtimeMs: 123, data: null, error: "boom" },
    ]);
    db.close();
  });

  test("rescheduleJob clears stale error", () => {
    const { db, repo } = setupRepo();

    const job = repo.createJob({ type: "download", status: "queued", payload: { infoHash: "abc123" } });
    const failed = repo.markJobFailed(job.id, "temporary socket error", new Date(Date.now() + 1000).toISOString());
    expect(failed.error).toContain("socket");

    const rescheduled = repo.rescheduleJob(job.id, new Date(Date.now() + 5000).toISOString(), {
      infoHash: "abc123",
      telemetry: {
        lastBytesDone: 42,
      },
    });
    expect(rescheduled.status).toBe("queued");
    expect(rescheduled.error).toBeNull();
    expect(JSON.parse(rescheduled.payload_json ?? "{}").telemetry.lastBytesDone).toBe(42);

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
      url: "https://example.com/dune-audio.torrent",
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
    expect(hydrated?.ebookStatus).toBe("wanted");
    expect(hydrated?.status).toBe("partial");

    db.close();
  });

  test("deletes book with cascade and only selects exclusive artifacts", () => {
    const { db, repo } = setupRepo();

    const book1 = repo.createBook({ title: "Book One", author: "Author" });
    const book2 = repo.createBook({ title: "Book Two", author: "Author" });
    const release = repo.createRelease({
      bookId: book1.id,
      provider: "test",
      title: "Book One audio",
      mediaType: "audio",
      infoHash: "fff111",
      url: "https://example.com/book-one.torrent",
      status: "downloaded",
    });

    repo.addAsset({
      bookId: book1.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 300,
      sourceReleaseId: release.id,
      files: [
        {
          path: "/tmp/book-one-only.mp3",
          size: 200,
          start: 0,
          end: 199,
          durationMs: 1000,
          title: null,
        },
        {
          path: "/tmp/shared.mp3",
          size: 100,
          start: 200,
          end: 299,
          durationMs: 1000,
          title: null,
        },
      ],
    });
    repo.addAsset({
      bookId: book2.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 100,
      files: [
        {
          path: "/tmp/shared.mp3",
          size: 100,
          start: 0,
          end: 99,
          durationMs: 1000,
          title: null,
        },
      ],
    });
    repo.updateBookMetadata(book1.id, { coverPath: "/tmp/cover-shared.jpg" });
    repo.updateBookMetadata(book2.id, { coverPath: "/tmp/cover-shared.jpg" });
    const linkedJob = repo.createJob({ type: "download", releaseId: release.id, bookId: book1.id });

    const artifacts = repo.getBookDeleteArtifacts(book1.id);
    expect(artifacts.assetPaths).toContain("/tmp/book-one-only.mp3");
    expect(artifacts.assetPaths).not.toContain("/tmp/shared.mp3");
    expect(artifacts.coverPath).toBeNull();

    const deleted = repo.deleteBook(book1.id);
    expect(deleted).toBe(true);
    expect(repo.getBook(book1.id)).toBeNull();
    expect(repo.getRelease(release.id)).toBeNull();
    expect(repo.getJob(linkedJob.id)).toBeNull();
    expect(repo.listAssetsByBook(book1.id)).toEqual([]);

    db.close();
  });
});
