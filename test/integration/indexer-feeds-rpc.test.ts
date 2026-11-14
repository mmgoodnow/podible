import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { indexerFailure } from "../../src/media/indexer-errors";
import type { IndexerMedia } from "../../src/media/indexer-gateway";
import { indexerFeedsRouter } from "../../src/rpc/indexer-feeds-router";

test("RSS controls and coverage are admin-only and never expose feed credentials or payloads", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.prowlarr = {
    url: "https://prowlarr.invalid",
    apiKey: "private-key",
  };
  repo.updateSettings(settings);
  for (const name of ["admin", "reader"]) {
    const u = repo.upsertUser({
      provider: "local",
      providerUserId: name,
      username: name,
      isAdmin: name === "admin",
    });
    repo.createSession(u.id, hashSessionToken(name), null, "browser");
  }
  const now = new Date().toISOString();
  repo.releaseInbox.ensureFeed("opaque", "Fixture", "movie", now);
  repo.releaseInbox.fail(
    "opaque",
    indexerFailure("Fixture", 429, Date.now() + 60_000),
    now,
  );
  const handler = createPodibleFetchHandler(repo, Date.now());
  async function rpc(method: string, token: string, params: unknown = {}) {
    const r = await handler(
      new Request("http://localhost/rpc", {
        method: "POST",
        headers: {
          cookie: "podible_session=" + token,
          "content-type": "application/json",
        },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      }),
    );
    return (await r.json()) as any;
  }
  for (const method of ["indexerFeeds.status", "indexerFeeds.sync"]) {
    expect((await rpc(method, "bad")).error.code).toBe(-32001);
    expect((await rpc(method, "reader")).error.code).toBe(-32003);
  }
  const status = (await rpc("indexerFeeds.status", "admin")).result;
  expect(status.intervalMinutes).toBe(15);
  expect(status.feeds[0].error).toContain("HTTP 429");
  expect(JSON.stringify(status)).not.toContain("private-key");
  expect(JSON.stringify(status)).not.toContain("prowlarr.invalid");
  expect(status.feeds[0]).not.toHaveProperty("payload_json");
  expect(
    (await rpc("indexerFeeds.sync", "admin")).result.jobId,
  ).toBeGreaterThan(0);
  expect(
    (
      await rpc("indexerFeeds.sync", "admin", {
        url: "https://client-authored.invalid",
      })
    ).error,
  ).toBeTruthy();
  const allowed = await handler(
    new Request("http://localhost/api/trpc/indexerFeeds.status?input=%7B%7D", {
      headers: { cookie: "podible_session=admin" },
    }),
  );
  expect(allowed.status).toBe(200);
  const forbidden = await handler(
    new Request("http://localhost/api/trpc/indexerFeeds.status?input=%7B%7D", {
      headers: { cookie: "podible_session=reader" },
    }),
  );
  expect(forbidden.status).toBe(403);
});

test("release feed reads are admin-only and expose only safe display fields and recorded routing counts", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const handler = createPodibleFetchHandler(repo, Date.now());
  for (const name of ["admin", "reader"]) {
    const user = repo.upsertUser({
      provider: "local", providerUserId: name, username: name, isAdmin: name === "admin",
    });
    repo.createSession(user.id, hashSessionToken(name), null, "browser");
  }
  async function trpc(token: string, input: object = {}) {
    return handler(new Request(
      "http://localhost/api/trpc/indexerFeeds.releases?input=" + encodeURIComponent(JSON.stringify(input)),
      { headers: { cookie: "podible_session=" + token } },
    ));
  }
  async function rpc(token: string) {
    const response = await handler(new Request("http://localhost/rpc", {
      method: "POST",
      headers: { cookie: "podible_session=" + token, "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "indexerFeeds.releases" }),
    }));
    return response.json();
  }
  expect(indexerFeedsRouter.routes.releases.readOnly).toBe(true);
  for (const [token, status, code] of [["bad", 401, -32001], ["reader", 403, -32003]] as const) {
    expect((await trpc(token)).status).toBe(status);
    expect(await rpc(token)).toMatchObject({ error: { code } });
  }

  const now = "2026-11-14T20:00:00.000Z";
  const infoHash = "abcdef1234".repeat(4);
  const url = "https://tracker.invalid/download?passkey=private-passkey";
  const provider = "Fixture https://tracker.invalid/api?apikey=private-api-key";
  const title = `Fixture Audiobook ${url} ${infoHash}`;
  repo.releaseInbox.ensureFeed("private-feed-id", provider, "audio", now);
  const [unrouted, routed] = repo.releaseInbox.ingest("private-feed-id", [
    {
      title: "Unrouted Audiobook", provider, mediaType: "audio", url, infoHash,
      guid: "private-guid-unrouted", seeders: 10, leechers: 1, sizeBytes: null, raw: {},
    },
    {
      title, provider, mediaType: "audio", url, infoHash, guid: "private-guid-routed",
      seeders: 10, leechers: 1, sizeBytes: null,
      raw: { attr: [{ name: "private-attribute", value: "private-attribute-value" }] },
    },
  ], now);
  for (const [targetId, revision] of [[801, "v1"], [801, "v2"], [802, "v1"]] as const) {
    const job = repo.createJob({ type: "acquire", payload: { secret: "private-job-payload" } });
    repo.releaseInbox.consider(routed!, "book:audio", targetId, revision, job.id, "private-matcher-note");
  }

  const expected = {
    items: [
      {
        id: routed, title: "Fixture Audiobook [redacted] [redacted]", provider: "Fixture [redacted]",
        media: "audio", receivedAt: now, acquisitionRoutes: [{ targetKind: "audio", targetCount: 2 }],
      },
      {
        id: unrouted, title: "Unrouted Audiobook", provider: "Fixture [redacted]",
        media: "audio", receivedAt: now, acquisitionRoutes: [],
      },
    ],
    nextCursor: null,
  };
  db.exec("PRAGMA query_only = ON");
  const response = await trpc("admin");
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ result: { data: expected } });
  expect(await rpc("admin")).toEqual({ jsonrpc: "2.0", id: 1, result: expected });
  for (const input of [
    { media: "other" }, { cursor: 0 }, { cursor: Number.MAX_SAFE_INTEGER + 1 },
    { cursor: 1.5 }, { limit: 0 }, { limit: 101 }, { url },
  ]) expect((await trpc("admin", input)).status).toBe(400);
});

test("release feed keysets seek across providers by inbox ID without duplicating concurrent arrivals", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const inbox = repo.releaseInbox;
  const now = "2026-11-14T20:00:00.000Z";
  const add = (guid: string, mediaType: IndexerMedia, feedId = mediaType as string) => {
    inbox.ensureFeed(feedId, feedId, mediaType, now);
    return inbox.ingest(feedId, [{
      title: guid, provider: feedId, mediaType, guid, url: "https://tracker.invalid/download",
      infoHash: null, seeders: null, leechers: null, sizeBytes: null, raw: {},
    }], now)[0]!;
  };
  const movie1 = add("Movie 1", "movie");
  const tv = add("TV", "tv");
  const audio = add("Audiobook", "audio");
  const movie2 = add("Movie 2", "movie", "autobrr:fixture");
  const ebook = add("Ebook", "ebook");
  const movie3 = add("Movie 3", "movie");
  inbox.ensureFeed("empty-movie-feed", "Empty", "movie", now);

  const first = inbox.releaseFeedPage({ limit: 2 });
  expect(first.items.map(item => item.id)).toEqual([movie3, ebook]);
  expect(first.nextCursor).toBe(ebook);
  const movies = inbox.releaseFeedPage({ media: "movie", limit: 2 });
  expect(movies.items.map(item => item.id)).toEqual([movie3, movie2]);
  expect(movies.nextCursor).toBe(movie2);
  const movie4 = add("Movie 4", "movie");
  const second = inbox.releaseFeedPage({ limit: 2, cursor: first.nextCursor! });
  expect(second.items.map(item => item.id)).toEqual([movie2, audio]);
  expect(second.nextCursor).toBe(audio);
  const last = inbox.releaseFeedPage({ limit: 2, cursor: second.nextCursor! });
  expect(last.items.map(item => item.id)).toEqual([tv, movie1]);
  expect(last.nextCursor).toBeNull();
  const moreMovies = inbox.releaseFeedPage({ media: "movie", limit: 2, cursor: movies.nextCursor! });
  expect(moreMovies.items.map(item => item.id)).toEqual([movie1]);
  expect(moreMovies.nextCursor).toBeNull();
  expect(inbox.releaseFeedPage({ limit: 1 }).items[0]?.id).toBe(movie4);
  for (const [media, id] of [["tv", tv], ["audio", audio], ["ebook", ebook]] as const) {
    const filtered = inbox.releaseFeedPage({ media, limit: 1 });
    expect(filtered.items.map(item => [item.id, item.media])).toEqual([[id, media]]);
    expect(filtered.nextCursor).toBeNull();
    expect(inbox.releaseFeedPage({ media, limit: 1, cursor: id })).toEqual({ items: [], nextCursor: null });
  }
});

test("migration removes the periodic movie search setting without changing customized policy", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db),
    s = repo.ensureSettings();
  const legacy = {
    ...s,
    movies: {
      ...s.movies,
      searchIntervalHours: 24,
      qualityPolicy: { ...s.movies.qualityPolicy, cut: "director" },
    },
  };
  db.query("UPDATE settings SET value_json=? WHERE id=1").run(
    JSON.stringify(legacy),
  );
  db.query("DELETE FROM schema_migrations WHERE id=42").run();
  // Rehearse the setting migration separately because the new inventory tables already exist.
  for (const table of [
    "release_considerations",
    "release_inbox",
    "indexer_feeds",
  ])
    db.exec("DROP TABLE " + table);
  runMigrations(db);
  expect(repo.getSettings().movies.qualityPolicy.cut).toBe("director");
  expect(repo.getSettings().movies).not.toHaveProperty("searchIntervalHours");
});
