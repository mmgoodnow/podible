import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { indexerFailure } from "../../src/media/indexer-errors";

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
