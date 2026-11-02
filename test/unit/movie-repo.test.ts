import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations, finalizeJobJournalMigration } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultMovieQualityPolicy as policy } from "../../src/movies/quality";

const movie = {
  tmdbId: 42,
  title: "Fixture",
  originalTitle: "Fixture",
  year: 2024,
  overview: "Test",
  originalLanguage: "en",
  posterUrl: null,
  imdbId: null,
};
function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  finalizeJobJournalMigration(db);
  const repo = new BooksRepo(db);
  repo.bindJobJournalReader({ read: () => new Map() });
  const users = [1, 2].map((n) =>
    repo.upsertUser({
      provider: "plex",
      providerUserId: String(n),
      username: `test${n}`,
      isAdmin: false,
    }),
  );
  return { db, repo, users };
}
test("movie requests share compatible targets, retain user ownership and enqueue atomically", () => {
  const { db, repo, users } = setup();
  try {
    const first = repo.movies.request(users[0]!.id, movie, policy);
    const second = repo.movies.request(users[1]!.id, movie, policy);
    expect(first.targetId).toBe(second.targetId);
    expect(repo.movies.request(users[0]!.id, movie, policy).requestId).toBe(
      first.requestId,
    );
    expect(repo.movies.canAccessTarget(999, first.targetId)).toBe(false);
    expect(repo.movies.requests(users[1]!.id)).toHaveLength(1);
    expect(
      repo.movies.request(users[0]!.id, movie, { ...policy, cut: "extended" })
        .targetId,
    ).not.toBe(first.targetId);
    expect(() =>
      repo.movies.startAcquisition(first.targetId, "initial", () => {
        throw new Error("failed enqueue");
      }),
    ).toThrow("failed enqueue");
    expect(repo.movies.acquisitions(first.targetId)).toHaveLength(0);
    let calls = 0;
    const enqueue = () => {
      calls++;
      return repo.createJob({
        type: "reconcile",
        idempotencyKey: "movie-fixture",
      }).id;
    };
    const a = repo.movies.startAcquisition(first.targetId, "initial", enqueue);
    const b = repo.movies.startAcquisition(first.targetId, "initial", enqueue);
    expect(a.shared).toBe(false);
    expect(b.shared).toBe(true);
    expect(calls).toBe(1);
    expect(repo.listJobCommands()).toHaveLength(1);
    expect(db.query("SELECT COUNT(*) AS n FROM books").get()).toEqual({ n: 0 });
  } finally {
    db.close();
  }
});
test("movie writes respect execution fencing, version identity and publication revisions", () => {
  const { db, repo, users } = setup();
  try {
    const target = repo.movies.request(users[0]!.id, movie, policy);
    const a = repo.movies.startAcquisition(
      target.targetId,
      "initial",
      () => repo.createJob({ type: "reconcile" }).id,
    ).acquisition;
    const facts = {
      identityVerified: true,
      cut: "theatrical",
      source: "web" as const,
      width: 1920,
      height: 800,
      videoCodec: "h264",
      dynamicRange: "sdr" as const,
      audioLanguages: ["en"],
      proper: false,
    };
    const id = repo.movies.recordVersion(a.id, facts, [
      {
        id: "file-a",
        relativePath: "objects/a.mkv",
        sizeBytes: 100,
        probe: {},
      },
    ]);
    expect(() => repo.movies.recordVersion(a.id, facts, [])).toThrow(
      "needs content files",
    );
    expect(
      repo.movies.recordVersion(a.id, facts, [
        {
          id: "file-a",
          relativePath: "objects/a.mkv",
          sizeBytes: 100,
          probe: {},
        },
      ]),
    ).toBe(id);
    repo.movies.selectVersion(target.targetId, id, 0);
    expect(repo.movies.target(target.targetId)?.active_version_id).toBe(id);
    expect(repo.movies.version(id)?.files[0]?.id).toBe("file-a");
    const other = repo.movies.request(
      users[0]!.id,
      { ...movie, tmdbId: 43 },
      policy,
    );
    expect(() => repo.movies.selectVersion(other.targetId, id, 0)).toThrow(
      "another movie",
    );
    const fenced = repo.forExecution(() => {
      throw new Error("stale owner");
    });
    expect(() =>
      fenced.movies.event(target.targetId, a.id, "bad", "available", "stale"),
    ).toThrow("stale owner");
    expect(
      repo.movies.events(target.targetId).some((e) => e.message === "stale"),
    ).toBe(false);
  } finally {
    db.close();
  }
});
