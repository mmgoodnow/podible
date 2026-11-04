import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { movieOfferFromResult } from "../../src/movies/offers";
import { refreshMovieFeedOffer } from "../../src/movies/feed-offers";
import type { SearchResult } from "../../src/media/indexer-gateway";

const result: SearchResult<"movie"> = {
  title: "A Film",
  provider: "Fixture",
  mediaType: "movie",
  guid: "same-guid",
  infoHash: "a".repeat(40),
  sizeBytes: 100,
  url: "https://download.invalid/old-token",
  seeders: 1,
  leechers: 0,
  raw: {},
};
test("changed torrent contents under the same GUID receive distinct acquisition identities", () => {
  const first = movieOfferFromResult(result);
  expect(
    movieOfferFromResult({ ...result, infoHash: "b".repeat(40) }).key,
  ).not.toBe(first.key);
  expect(
    movieOfferFromResult({ ...result, infoHash: null, sizeBytes: 200 }).key,
  ).not.toBe(first.key);
  expect(
    movieOfferFromResult({
      ...result,
      url: "https://download.invalid/new-token",
      seeders: 100,
    }).key,
  ).toBe(first.key);
});
test("inbox replay refreshes an expiring URL without fanout and queued acquisition resolves the latest payload", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const now = new Date().toISOString();
  repo.releaseInbox.ensureFeed("fixture", "Fixture", "movie", now);
  const [id] = repo.releaseInbox.ingest("fixture", [result], now);
  const queued = { ...movieOfferFromResult(result), inboxId: id };
  const rotated = {
    ...result,
    url: "https://download.invalid/new-token",
    seeders: 100,
  };
  expect(repo.releaseInbox.ingest("fixture", [rotated], now)).toEqual([]);
  expect(refreshMovieFeedOffer(repo, queued)).toMatchObject({
    key: queued.key,
    inboxId: id,
    url: rotated.url,
    seeders: 100,
  });
  expect(
    refreshMovieFeedOffer(repo, { ...queued, inboxId: undefined }),
  ).toMatchObject({ url: result.url });
  expect(() =>
    refreshMovieFeedOffer(repo, { ...queued, inboxId: 999 }),
  ).toThrow("missing from the inbox");
});
