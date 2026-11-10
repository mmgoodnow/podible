import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { readHomeRecent } from "../../src/media/home-recent";
import { publishRailMovie, publishRailTv } from "../fixtures/home-rails-data";
import { fixtureMovie } from "../fixtures/movie-services";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

test("recent rails use real publication dates, bounded SQL and no private pending data or writes", () => {
  using db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db); repo.ensureSettings();
  const user = repo.upsertUser({ provider: "local", providerUserId: "rails", username: "Reader", isAdmin: false });
  const newest = publishRailMovie(repo, db, user.id, 400, "Newly published", "2026-11-02T00:00:00Z");
  publishRailMovie(repo, db, user.id, 401, "Higher ID but older", "2025-01-01T00:00:00Z");
  const future = publishRailMovie(repo, db, user.id, 402, "Future event", "2099-01-01T00:00:00Z");
  db.query("UPDATE movies SET updated_at='2099-01-01' WHERE tmdb_id=401").run();
  db.query("UPDATE movies SET metadata_json=json_set(metadata_json,'$.posterUrl','https://private-sonarr.invalid/art?apikey=secret') WHERE tmdb_id=400").run();
  repo.movies.event(newest.request.targetId, null, `published:${newest.request.targetId}:upgrade`, "available", "Upgrade");
  const book = repo.createBook({ title: "Imported book", author: "Author" });
  const manifestation = repo.addManifestation({ bookId: book.id, kind: "ebook", label: "Imported edition" });
  repo.addAsset({ bookId: book.id, kind: "single", mime: "application/epub+zip", totalSize: 10, durationMs: null, sourceReleaseId: null,
    manifestationId: manifestation.id,
    files: [{ path: "/fixture/book.epub", size: 10, start: 0, end: 9, durationMs: 0, title: "Book" }] });
  db.query("UPDATE books SET added_at='2099-01-01',published_at='1900-01-01'").run();
  db.query("UPDATE assets SET created_at='2026-10-30T12:00:00Z'").run();
  repo.createBook({ title: "Someone else's pending book", author: "Private" });
  const before = db.query("SELECT total_changes() AS n").get();
  const queries = spyOn(db, "query");
  const result = readHomeRecent(db, { now: "2026-11-04T00:00:00Z", limit: 1 });
  expect(queries).toHaveBeenCalledTimes(4); queries.mockRestore();
  expect(result.movies.map(m => m.title)).toEqual(["Newly published"]);
  expect(result.movies[0]!.publishedAt).toBe("2026-11-02T00:00:00Z");
  expect(result.movies[0]!.posterUrl).toBeNull();
  expect(result.books.map(b => [b.title, b.publishedAt])).toEqual([["Imported book", "2026-10-30T12:00:00Z"]]);
  expect(db.query("SELECT total_changes() AS n").get()).toEqual(before);
  db.query("INSERT INTO media_content_losses(file_id,namespace,target_id,operation_id,observed_at) VALUES(?,'movie-target',?,'loss',?)").run(newest.fileId, newest.request.targetId, "2026-11-03");
  expect(readHomeRecent(db, { now: "2026-11-04T00:00:00Z" }).movies.map(m => m.title)).toEqual(["Higher ID but older"]);
  db.query("DELETE FROM movie_events WHERE target_id=?").run(future.request.targetId);
  expect(() => readHomeRecent(db, { limit: 21 })).toThrow();
});

test("aired rail requires recent real dates and exact healthy selected episode coverage", () => {
  using db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db); repo.ensureSettings();
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-rails", username: "Reader", isAdmin: false });
  const selected = publishRailTv(repo, db, user.id, 800, "Recent selected episode", "2026-10-30T00:00:00Z", "2026-11-01");
  publishRailTv(repo, db, user.id, 801, "Future", "2026-10-30T00:00:00Z", "2099-01-01");
  publishRailTv(repo, db, user.id, 802, "Undated", "2026-10-30T00:00:00Z", null);
  publishRailTv(repo, db, user.id, 803, "Old", "2026-10-30T00:00:00Z", "2025-01-01");
  const result = readHomeRecent(db, { now: "2026-11-04T00:00:00Z" });
  expect(result.episodes.map(e => [e.title, e.airDate, e.season, e.episode])).toEqual([["Recent selected episode", "2026-11-01", 1, 1]]);
  expect(result.tv).toHaveLength(4);
  db.query("INSERT INTO media_content_losses(file_id,namespace,target_id,operation_id,observed_at) VALUES(?,'tv-target',?,'loss',?)").run(selected.fileId, selected.request.targetId, "2026-11-03");
  const afterLoss = readHomeRecent(db, { now: "2026-11-04T00:00:00Z" });
  expect(afterLoss.episodes).toEqual([]);
  expect(afterLoss.tv.map(t => t.title)).not.toContain("Recent selected episode");
});

test("upcoming rail shows only the viewer's dated, unfulfilled requests in the next two weeks", () => {
  using db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db); repo.ensureSettings();
  const viewer = repo.upsertUser({ provider: "local", providerUserId: "viewer", username: "Viewer", isAdmin: false });
  const other = repo.upsertUser({ provider: "local", providerUserId: "other", username: "Other", isAdmin: false });
  const policy = repo.getSettings().movies.qualityPolicy;
  const movie = { ...fixtureMovie, releaseDate: "2026-11-06" };
  repo.movies.request(viewer.id, { ...movie, tmdbId: 101, title: "Upcoming movie" }, policy);
  repo.movies.request(other.id, { ...movie, tmdbId: 102, title: "Private movie" }, policy);
  repo.movies.request(viewer.id, { ...movie, tmdbId: 103, releaseDate: "2026-12-06" }, policy);
  const entry = fixtureTvEntry();
  entry.episodes[2]!.airDate = "2026-11-05";
  repo.tv.requestSeasons(viewer.id, entry, policy, [1]);
  const result = readHomeRecent(db, { userId: viewer.id, now: "2026-11-04T00:00:00Z" });
  expect(result.upcoming.map(item => [item.kind, item.title, item.date])).toEqual([
    ["tv", entry.title, "2026-11-05"], ["movie", "Upcoming movie", "2026-11-06"],
  ]);
});
