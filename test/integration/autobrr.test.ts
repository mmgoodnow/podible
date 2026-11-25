import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { fetchAutobrrTorrentBytes } from "../../src/library/torrent-cache";
import { parseAutobrrRelease, readAutobrrConfig, reconcileAutobrrWebhook, refreshAutobrrDownloadOrigins } from "../../src/media/autobrr";
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

test("a Prowlarr canary does not authorize direct IRC downloads from the same indexer", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const canaryConfig = readAutobrrConfig({ ...env, AUTOBRR_DOWNLOAD_ORIGINS: "http://prowlarr.invalid:9696" })!;
  const directConfig = readAutobrrConfig({
    ...env, AUTOBRR_DOWNLOAD_ORIGINS: "http://prowlarr.invalid:9696,https://tracker.invalid",
  })!;
  expect(directConfig.authorization).toBe(canaryConfig.authorization);
  const deliver = (settings: typeof config, input: unknown) =>
    createPodibleFetchHandler(repo, Date.now(), null, { autobrr: settings })(new Request(settings.callbackUrl, {
      method: "POST", headers: { authorization: settings.authorization, "Content-Type": "application/json" },
      body: JSON.stringify(input),
    }));
  const canaryEvent = {
    ...event, data: { ...event.data, release: { ...release, download_url: "http://prowlarr.invalid:9696/1/download?apikey=fixture" } },
  };
  const parsedCanary = parseAutobrrRelease(canaryConfig, canaryEvent)!;
  expect((await deliver(canaryConfig, canaryEvent)).status).toBe(204);
  const canaryJobs = repo.listRecentJobs();
  expect(canaryJobs).toHaveLength(2);

  expect(parseAutobrrRelease(canaryConfig, event)).toBeNull();
  expect((await deliver(canaryConfig, event)).status).toBe(204);
  expect(repo.listRecentJobs()).toEqual(canaryJobs);
  expect(repo.releaseInbox.feed(parsedCanary.feedId)?.provider).toBe("Fixture Indexer");

  const directEvent = { ...event, data: { ...event.data, release: { ...release, external_id: "43" } } };
  expect(parseAutobrrRelease(directConfig, directEvent)?.result.mediaType).toBe("tv");
  expect((await deliver(directConfig, directEvent)).status).toBe(204);
  expect(repo.listRecentJobs()).toHaveLength(4);
  expect(parseAutobrrRelease(directConfig, {
    ...event, data: { ...event.data, release: { ...release, download_url: "https://other.invalid/download" } },
  })).toBeNull();
});

test("real webhook ingress accepts FileList's authenticated Prowlarr origin only for that provider", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = readAutobrrConfig({ ...env, AUTOBRR_DOWNLOAD_ORIGINS: "http://prowlarr.invalid:9696" })!;
  const fetchHandler = createPodibleFetchHandler(repo, Date.now(), null, { autobrr: settings });
  const receipts = () => (db.query("SELECT COUNT(*) AS n FROM release_inbox").get() as { n: number }).n;
  const deliver = (provider: string, url: string, authorization = settings.authorization, hints: Record<string, unknown> = {}) => fetchHandler(new Request(settings.callbackUrl, {
    method: "POST", headers: { authorization, "Content-Type": "application/json" },
    body: JSON.stringify({ ...event, data: { indexer: { ...hints, name: provider, identifier: "fl" }, release: { ...hints, ...release, external_id: provider, download_url: url } } }),
  }));
  let calls = 0;
  const fetcher = (async (input, init) => {
    calls++;
    const url = new URL(String(input));
    expect(url.origin).toBe("https://prowlarr.invalid");
    expect(new Headers(init?.headers).get("X-Api-Key")).toBe("private-prowlarr-fixture");
    expect(init?.redirect).toBe("manual");
    expect(init?.signal).toBeDefined();
    return Response.json(url.pathname.endsWith("/indexerstatus") ? [] : [
      { id: 6, name: "FileList", definitionName: "FileList.io", enable: true, protocol: "torrent", fields: [{ name: "baseUrl", value: "https://filelist.io/" }] },
      { id: 7, name: "Other Tracker", enable: true, protocol: "torrent", fields: [{ name: "baseUrl", value: "https://other.invalid/" }] },
    ]);
  }) as typeof fetch;
  expect((await deliver("FileList", "https://filelist.io/download?id=42", "wrong")).status).toBe(401);
  const hints = { baseUrl: "https://filelist.io", definitionName: "FileList", fields: [{ name: "baseUrl", value: "https://filelist.io" }], downloadOrigins: ["https://filelist.io"] };
  expect((await deliver("FileList", "https://filelist.io/download?id=42", settings.authorization, hints)).status).toBe(204);
  expect(receipts()).toBe(0);
  expect(calls).toBe(0);
  await Promise.all(Array.from({ length: 8 }, () => refreshAutobrrDownloadOrigins(settings, {
    url: "https://prowlarr.invalid/base/", apiKey: "private-prowlarr-fixture",
  }, fetcher)));
  expect(calls).toBe(2);
  expect((await deliver("FileList", "https://filelist.io/download?id=42")).status).toBe(204);
  expect(receipts()).toBe(1);
  expect(repo.listRecentJobs()).toHaveLength(2);
  for (const [provider, url] of [
    ["FileList", "https://unrelated.invalid/download"],
    ["FileList", "https://filelist.io.unrelated.invalid/download"],
    ["FileList", "https://other.invalid/download"],
    ["FileList", "https://credential@filelist.io/download"],
    ["FileList", "https://filelist.io:444/download"],
    ["FileList", "http://filelist.io/download"],
    ["FileList", "https://download.filelist.io/download"],
    ["FileList", "file:///etc/passwd"],
    ["FileList", "https://filelist.io/download#fragment"],
    ["Unconfigured Tracker", "https://filelist.io/download"],
  ]) {
    expect((await deliver(provider!, url!, settings.authorization, hints)).status).toBe(204);
    expect(receipts()).toBe(1);
  }
  expect((await deliver("Unconfigured Tracker", "http://prowlarr.invalid:9696/1/download?id=42")).status).toBe(204);
  expect(receipts()).toBe(2);
  expect((await deliver("  FILELIST.IO  ", "https://filelist.io:443/download?id=43")).status).toBe(204);
  expect(receipts()).toBe(3);
  expect(calls).toBe(2);
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
