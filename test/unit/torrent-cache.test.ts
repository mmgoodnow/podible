import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { getOrFetchCachedTorrentBytes, torrentCacheKeyFor } from "../../src/library/torrent-cache";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { cachedMovieMetainfo } from "../../src/movies/offers";
import { indexerTrafficPrometheus } from "../../src/media/indexer-traffic";

const torrent = (name: string) => bencode.encode({ info: {
  name, length: 10, "piece length": 16384, pieces: Buffer.from("12345678901234567890"),
} });

async function fixture(run: (repo: BooksRepo, calls: string[]) => Promise<void>) {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const original = globalThis.fetch;
  const calls: string[] = [];
  globalThis.fetch = (async (input: unknown) => {
    calls.push(String(input));
    return new Response(torrent("current.mp3"));
  }) as typeof fetch;
  try { await run(repo, calls); } finally { globalThis.fetch = original; db.close(); }
}

const identity = { provider: "fixture", providerGuid: "reused", url: "https://fixture.invalid/download" };

test("a verified matching cache hit avoids a network request", async () => fixture(async (repo, calls) => {
  const bytes = torrent("current.mp3");
  repo.putTorrentCache({ key: torrentCacheKeyFor(identity), ...identity, infoHash: "a".repeat(40), torrentBytes: bytes });
  const result = await getOrFetchCachedTorrentBytes(repo, { ...identity, infoHash: infoHashFromTorrentBytes(bytes) });
  expect(result.cacheHit).toBe(true);
  expect(result.bytes).toEqual(bytes);
  expect(calls).toHaveLength(0);
}));

test("same GUID with a differing hash bypasses stale bytes even if cached metadata claims that hash", async () => fixture(async (repo, calls) => {
  const current = torrent("current.mp3");
  const hash = infoHashFromTorrentBytes(current);
  repo.putTorrentCache({ key: torrentCacheKeyFor(identity), ...identity, infoHash: hash, torrentBytes: torrent("old.mp3") });
  const result = await getOrFetchCachedTorrentBytes(repo, { ...identity, infoHash: hash });
  expect(result.cacheHit).toBe(false);
  expect(result.bytes).toEqual(current);
  expect(calls).toHaveLength(1);
  expect(repo.getTorrentCache(torrentCacheKeyFor(identity))?.info_hash).toBe(hash);
}));

test("unknown-hash requests can explicitly revalidate mutable GUID content", async () => fixture(async (repo, calls) => {
  repo.putTorrentCache({ key: torrentCacheKeyFor(identity), ...identity, torrentBytes: torrent("old.mp3") });
  const result = await getOrFetchCachedTorrentBytes(repo, identity, { revalidate: true });
  expect(result.cacheHit).toBe(false);
  expect(result.bytes).toEqual(torrent("current.mp3"));
  expect(calls).toHaveLength(1);
}));

test("a refreshed GUID URL is fetched rather than replaced with the previous URL's bytes", async () => fixture(async (repo, calls) => {
  repo.putTorrentCache({ key: torrentCacheKeyFor(identity), ...identity, torrentBytes: torrent("old.mp3") });
  const url = "https://fixture.invalid/updated";
  const result = await getOrFetchCachedTorrentBytes(repo, { ...identity, url });
  expect(result.cacheHit).toBe(false);
  expect(calls).toEqual([url]);
}));

test("new cache entries persist the hash of fetched bytes, not an unverified supplied hash", async () => fixture(async (repo) => {
  const result = await getOrFetchCachedTorrentBytes(repo, { ...identity, infoHash: "a".repeat(40) });
  expect(repo.getTorrentCache(result.cacheKey)?.info_hash).toBe(infoHashFromTorrentBytes(result.bytes));
}));

test("corrupt legacy metainfo is refetched rather than treated as a cache hit", async () => fixture(async (repo, calls) => {
  repo.putTorrentCache({ key: torrentCacheKeyFor(identity), ...identity, torrentBytes: Buffer.from("invalid") });
  const result = await getOrFetchCachedTorrentBytes(repo, identity);
  expect(result.cacheHit).toBe(false);
  expect(result.bytes).toEqual(torrent("current.mp3"));
  expect(calls).toHaveLength(1);
}));

test("video inspection and acquisition share one fetch, then reuse the persisted bytes", async () => fixture(async (repo, calls) => {
  const offer = { ...identity, key: "fixture", title: "Fixture", sizeBytes: 10, seeders: 1,
    infoHash: infoHashFromTorrentBytes(torrent("current.mp3")), attributes: { guid: identity.providerGuid } };
  const first = cachedMovieMetainfo(repo, offer, "inspection");
  const second = cachedMovieMetainfo(repo, offer, "acquisition");
  expect(await first).toEqual(await second);
  expect(await cachedMovieMetainfo(repo, offer, "reuse")).toEqual(torrent("current.mp3"));
  expect(calls).toHaveLength(1);
  const metrics = indexerTrafficPrometheus(repo);
  expect(metrics).toContain('purpose="inspection",event="metainfo_fetch"} 1');
  expect(metrics).toContain('purpose="acquisition",event="metainfo_coalesced"} 1');
  expect(metrics).toContain('purpose="reuse",event="metainfo_cache_hit"} 1');
  expect(metrics).not.toContain(identity.url);
}));
