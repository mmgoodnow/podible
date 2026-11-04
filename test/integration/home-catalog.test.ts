import { expect, spyOn, test } from "bun:test";
import { createTRPCClient, httpLink } from "@trpc/client";
import type { AppRouter } from "../../src/trpc";
import { createPodibleFetchHandler } from "../../src/http";
import { createBookSpaData } from "../fixtures/book-spa-data";
import { publishRailMovie, publishRailTv } from "../fixtures/home-rails-data";
import { fixtureMovie } from "../fixtures/movie-services";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

test("typed HTTP catalog rails share publications, not private requests, and reject anonymous/oversized reads", async () => {
  const data = await createBookSpaData();
  try {
    const { repo, db, admin } = data;
    publishRailMovie(repo, db, admin.id, 900, "A shared movie", "2026-10-01T00:00:00Z");
    publishRailTv(repo, db, admin.id, 901, "A shared series", "2026-10-02T00:00:00Z", "2026-10-02");
    repo.movies.request(admin.id, { ...fixtureMovie, tmdbId: 902, title: "Private pending movie" }, repo.getSettings().movies.qualityPolicy);
    repo.tv.requestSeasons(admin.id, { ...fixtureTvEntry(), key: "tvdb:903", tmdbId: 903, tvdbId: 903, title: "Private pending TV" }, repo.getSettings().movies.qualityPolicy, [2]);
    const handle = createPodibleFetchHandler(repo, Date.now());
    const client = createTRPCClient<AppRouter>({ links: [httpLink({ url: "http://localhost/api/trpc", headers: { cookie: "podible_session=book-spa-reader" },
      fetch: async (url, init) => { const r = await handle(new Request(String(url), init)); return { ok: r.ok, status: r.status, headers: r.headers, json: () => r.json() }; } })] });
    const before = db.query("SELECT total_changes() AS n").get();
    const result = await client.catalog.recent.query({ limit: 2 });
    expect(result.books.map(b => b.title)).toEqual(["Piranesi"]);
    expect(result.movies.map(m => m.title)).toEqual(["A shared movie"]);
    expect(result.tv.map(m => m.title)).toEqual(["A shared series"]);
    const privateRequests = await client.requests.list.query({});
    expect(privateRequests.requests).toEqual([]);
    expect(JSON.stringify(result)).not.toMatch(/Private pending|libraryRoot|acquisition|requestId|apiKey|token|relativePath/);
    expect(db.query("SELECT total_changes() AS n").get()).toEqual(before);
    await expect(client.catalog.recent.query({ limit: 21 })).rejects.toThrow();
    for (const procedure of ["catalog.recent", "catalog.discovery"]) {
      const response = await handle(new Request(`http://localhost/api/trpc/${procedure}?input=%7B%7D`));
      expect(response.status).toBe(401);
      expect(response.headers.get("cache-control")).toBe("no-store");
    }
  } finally { await data.close(); }
});

test("typed discovery reuses configured credentials and sanitizes a partial remote failure", async () => {
  const data = await createBookSpaData();
  const calls: string[] = [];
  const upstream = spyOn(globalThis, "fetch").mockImplementation(Object.assign(async (input: Parameters<typeof fetch>[0]) => {
    const url = new URL(String(input)); calls.push(url.pathname);
    if (url.hostname !== "api.themoviedb.org") throw new Error("Unexpected network call");
    return url.pathname.includes("/movie/") ? new Response("super-secret credential upstream body", { status: 503 })
      : Response.json({ results: [{ id: 904, name: "Public discovery series", poster_path: "/public.jpg" }] });
  }, { preconnect: fetch.preconnect }));
  try {
    const settings = data.repo.getSettings(); settings.movies.tmdbAccessToken = "home-integration-secret"; data.repo.updateSettings(settings);
    const handle = createPodibleFetchHandler(data.repo, Date.now());
    const client = createTRPCClient<AppRouter>({ links: [httpLink({ url: "http://localhost/api/trpc", headers: { cookie: "podible_session=book-spa-reader" },
      fetch: async (url, init) => { const r = await handle(new Request(String(url), init)); return { ok: r.ok, status: r.status, headers: r.headers, json: () => r.json() }; } })] });
    const result = await client.catalog.discovery.query({});
    expect(result.movies).toEqual({ unavailable: true, items: [] });
    expect(result.tv.items[0]!.title).toBe("Public discovery series");
    expect(JSON.stringify(result)).not.toMatch(/super-secret|credential|upstream|home-integration-secret/);
    expect(await client.catalog.discovery.query({})).toEqual(result);
    expect(calls.sort()).toEqual(["/3/trending/movie/week", "/3/trending/tv/week"]);
  } finally { upstream.mockRestore(); await data.close(); }
});
