import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { fetchAutobrrTorrentBytes } from "../../src/library/torrent-cache";
import { parseAutobrrRelease, readAutobrrConfig, reconcileAutobrrWebhook } from "../../src/media/autobrr";
import { BooksRepo } from "../../src/repo";

const env = {
  AUTOBRR_URL: "https://autobrr.invalid/subpath/",
  AUTOBRR_PODIBLE_URL: "http://podible.invalid/",
  AUTOBRR_API_KEY: "fixture-api-key",
  AUTOBRR_DOWNLOAD_ORIGINS: "https://tracker.invalid",
};
const config = readAutobrrConfig(env)!;
const release = {
  protocol: "torrent", type: "series", name: "Example.S02E04",
  external_id: "42", size: 1_000_000, meta_tvdb: 123,
  download_url: "https://tracker.invalid/download?passkey=private",
};
const event = {
  event: "release.new", timestamp: "2026-11-09T21:30:11.123456789Z",
  data: { release, indexer: { name: "Fixture Indexer", identifier: "fixture" } },
};

test("autobrr registration uses the global notification API and is idempotent across key rotation", async () => {
  const notifications: Record<string, unknown>[] = [];
  const calls: string[] = [];
  let key = env.AUTOBRR_API_KEY;
  const fetcher = (async (url: string, init: RequestInit) => {
    expect(new Headers(init.headers).get("X-API-Token")).toBe(key);
    expect(init.redirect).toBe("error");
    calls.push(init.method!);
    if (init.method === "GET") {
      expect(url).toBe("https://autobrr.invalid/subpath/api/notification");
      return Response.json(notifications.length ? notifications : null);
    }
    const body = JSON.parse(init.body as string);
    expect(body).toMatchObject({
      type: "WEBHOOK", webhook: "http://podible.invalid/hooks/autobrr",
      enabled: true, method: "POST", events: ["RELEASE_NEW"],
    });
    expect(body.headers).not.toContain(key);
    if (init.method === "POST") {
      notifications.push({ ...body, id: 7 });
      // The remote write happened but its response was lost.
      throw new Error("connection dropped");
    }
    expect(url).toBe("https://autobrr.invalid/subpath/api/notification/7");
    expect(body.id).toBe(7);
    notifications[0] = body;
    return Response.json(body);
  }) as typeof fetch;
  await expect(reconcileAutobrrWebhook(config, fetcher)).rejects.toThrow("connection dropped");
  await reconcileAutobrrWebhook(config, fetcher);
  key = "rotated-fixture-api-key";
  const rotated = readAutobrrConfig({ ...env, AUTOBRR_API_KEY: key })!;
  await reconcileAutobrrWebhook(rotated, fetcher);
  expect(notifications[0]?.headers).toBe("Authorization=" + rotated.authorization);
  await reconcileAutobrrWebhook(rotated, fetcher);
  expect(calls).toEqual(["GET", "POST", "GET", "GET", "PUT", "GET"]);
});

test("release webhook authenticates, deduplicates deliveries and schedules both matchers without moving RSS", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const now = new Date().toISOString();
  repo.releaseInbox.ensureFeed("rss-id", "Fixture Indexer", "tv", now);
  repo.releaseInbox.succeed("rss-id", "rss-anchor", null, now);
  const rssBefore = repo.releaseInbox.feed("rss-id");
  const fetch = createPodibleFetchHandler(repo, Date.now(), null, { autobrr: config });
  const deliver = (body: unknown, authorization = config.authorization) =>
    fetch(new Request(config.callbackUrl, {
      method: "POST", headers: { authorization, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }));
  expect((await deliver(event, "Bearer " + env.AUTOBRR_API_KEY)).status).toBe(401);
  expect((await deliver({ payload: "x".repeat(40 * 1024) })).status).toBe(413);
  expect((await deliver({ ...event, data: { ...event.data, release: { ...release, download_url: "http://localhost/private" } } })).status).toBe(204);
  expect(repo.listRecentJobs()).toHaveLength(0);
  expect((await deliver(event)).status).toBe(204);
  expect((await deliver({ ...event, id: "retry", data: { ...event.data, release: { ...release, seeders: 14 } } })).status).toBe(204);
  const jobs = repo.listRecentJobs();
  expect(jobs.map(j => j.type).sort()).toEqual(["torrent_reuse_feed", "tv_feed_match"]);
  const { releaseIds } = JSON.parse(jobs[0]!.payload_json!);
  const [entry] = repo.releaseInbox.entries(releaseIds);
  expect(entry?.result).toMatchObject({
    mediaType: "tv", provider: "Fixture Indexer", seeders: 14,
    raw: { attr: [{ name: "tvdbid", value: "123" }] },
  });
  expect(repo.releaseInbox.feed("rss-id")).toEqual(rssBefore);
  expect(repo.releaseInbox.feeds()).toEqual([rssBefore!]);
});

test("autobrr uses specific book categories, rejects non-torrents and never follows download redirects", async () => {
  const withRelease = (changes: Partial<typeof release>) => ({ ...event, data: { ...event.data, release: { ...release, ...changes } } });
  expect(parseAutobrrRelease(config, withRelease({ type: "Movie" }))?.result.mediaType).toBe("movie");
  for (const [category, media] of [["Audiobook", "audio"], ["Ebook", "ebook"]] as const) {
    const input = withRelease({ type: "book" });
    expect(parseAutobrrRelease(config, { ...input, data: { ...input.data, release: { ...input.data.release, category } } })?.result.mediaType).toBe(media);
  }
  expect(parseAutobrrRelease(config, withRelease({ protocol: "usenet" }))).toBeNull();
  expect(parseAutobrrRelease(config, withRelease({ download_url: "https://tracker.invalid@other.invalid/file" }))).toBeNull();
  const redirect = (async (_: string, init: RequestInit) => {
    expect(init.redirect).toBe("manual");
    return Response.redirect("http://localhost/private");
  }) as typeof fetch;
  await expect(fetchAutobrrTorrentBytes(release.download_url, redirect)).rejects.toThrow("HTTP 302");
});
