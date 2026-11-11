import { expect, test } from "bun:test";
import { canonicalTvSeasons, configuredTvCatalog, TmdbTvCatalog, TvCatalogError } from "../../src/tv/catalog";
import { defaultSettings } from "../../src/settings";
import type { TvFetcher } from "../../src/tv/upstream";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

const summary = { id: 70, name: "A Work", original_name: "A Work", first_air_date: "2020-01-01", overview: "A story", poster_path: "/poster.jpg" };
const details = { ...summary, external_ids: { tvdb_id: 700 }, seasons: [{ season_number: 9, name: "TMDB grouping", episode_count: 5 }] };
const sonarr = { url: "https://sonarr.invalid/base", apiKey: "sonarr-private" };
const skyhookShow = { tvdbId: 700, tmdbId: 70, title: "A Work", runtime: 24, episodes: [
  { tvdbShowId: 700, tvdbId: 7001, seasonNumber: 2, episodeNumber: 1, title: "First", airDate: "2020-01-01" },
  { tvdbShowId: 700, tvdbId: 7002, seasonNumber: 2, episodeNumber: 2, absoluteEpisodeNumber: 15, title: "Future", airDate: "2099-01-01" },
] };
function fixture(options: { episodeId?: number; series?: unknown[]; skyhook?: unknown; tmdb?: unknown } = {}) {
  const calls: { url: URL; init?: RequestInit }[] = [];
  const fetcher: TvFetcher = async (input, init) => {
    const url = new URL(String(input)); calls.push({ url, init });
    if (url.pathname === "/3/search/tv") return Response.json({ results: [summary] });
    if (url.pathname === "/3/tv/70") return Response.json(options.tmdb ?? details);
    if (url.hostname === "skyhook.sonarr.tv") return options.skyhook
      ? Response.json(options.skyhook) : new Response("Not found", { status: 404 });
    if (url.pathname === "/base/api/v3/series") return Response.json(options.series ?? [{
      id: 7, tvdbId: 700, tmdbId: 70, title: "A Work", year: 2020, seriesType: "standard",
    }]);
    if (url.pathname === "/base/api/v3/episode") return Response.json([{
      id: 17, seriesId: 7, tvdbId: options.episodeId ?? 7001, title: "First", seasonNumber: 2, episodeNumber: 1, airDate: "2099-01-01",
    }]);
    throw new Error("Unexpected provider route");
  };
  return { calls, fetcher };
}

test("TV discovery uses the shared token, bounded GET transport, cache/coalescing and provider-scoped credentials", async () => {
  const f = fixture(); let now = 0;
  const catalog = new TmdbTvCatalog("tmdb-private", sonarr, { fetcher: f.fetcher, now: () => now });
  const results = await Promise.all([catalog.search("a & b"), catalog.search("a & b")]);
  expect(results[0]).toEqual(results[1]); expect(f.calls).toHaveLength(1);
  expect(f.calls[0]!.url.searchParams.get("query")).toBe("a & b");
  expect(results[0]![0]!.posterUrl).toBe("https://image.tmdb.org/t/p/w342/poster.jpg");
  const detail = await catalog.details(70);
  expect(detail.seasons.map((s) => s.seasonNumber)).toEqual([2]); // Never infer TMDB season 9 -> TVDB season 9.
  expect(detail.seasons[0]!.requestable).toBe(true);
  const mapped = await catalog.resolve(70);
  expect(mapped.key).toBe("tvdb:700"); expect(mapped.episodes[0]!.key).toBe("tvdb:7001");
  expect(mapped.posterUrl).toBe("https://image.tmdb.org/t/p/w342/poster.jpg");
  expect(f.calls).toHaveLength(4);
  for (const call of f.calls) {
    const headers = new Headers(call.init?.headers);
    expect(call.init?.method).toBe("GET"); expect(call.init?.redirect).toBe("error");
    expect(call.init?.signal).toBeInstanceOf(AbortSignal);
    expect(call.url.href).not.toContain("private");
    if (call.url.hostname === "api.themoviedb.org") {
      expect(headers.get("authorization")).toBe("Bearer tmdb-private"); expect(headers.has("x-api-key")).toBe(false);
    } else { expect(headers.get("x-api-key")).toBe("sonarr-private"); expect(headers.has("authorization")).toBe(false); }
  }
  expect(JSON.stringify(catalog)).not.toContain("tmdb-private");
  expect(JSON.stringify(catalog)).not.toContain("sonarr-private");
  now = 300_001; await catalog.search("a & b"); expect(f.calls).toHaveLength(5);
});

test("legacy shared TMDB v3 keys are accepted and cache cardinality is bounded", async () => {
  const f = fixture(), key = "a".repeat(32);
  const catalog = new TmdbTvCatalog(` ${key} `, sonarr, { fetcher: f.fetcher });
  await catalog.search("first");
  expect(f.calls[0]!.url.searchParams.get("api_key")).toBe(key);
  expect(new Headers(f.calls[0]!.init?.headers).has("authorization")).toBe(false);
  for (let i = 0; i < 64; i++) await catalog.search(`other-${i}`);
  await catalog.search("first"); expect(f.calls).toHaveLength(66);
});

test("Sonarr-only local episode IDs and unknown or ambiguous series are visibly unsupported, never added", async () => {
  for (const options of [{ episodeId: 0 }, { series: [] }, { series: [
    { id: 7, tvdbId: 700, tmdbId: 70, title: "A", seriesType: "standard" },
    { id: 8, tvdbId: 700, tmdbId: 70, title: "A", seriesType: "standard" },
  ] }]) {
    const f = fixture(options);
    const catalog = new TmdbTvCatalog("private", sonarr, { fetcher: f.fetcher });
    const found = await catalog.details(70);
    expect(found.seasons.every((s) => !s.requestable && !!s.unavailableReason)).toBe(true);
    await expect(catalog.resolve(70)).rejects.toBeInstanceOf(TvCatalogError);
    expect(f.calls.every((call) => call.init?.method === "GET")).toBe(true);
    expect(f.calls.some((call) => /command|monitor|request|lookup/.test(call.url.pathname))).toBe(false);
  }
});

test("missing credentials and provider errors are actionable, bounded and secret-safe", async () => {
  let calls = 0;
  const catalog = new TmdbTvCatalog("", { url: "", apiKey: "" }, { fetcher: async () => { calls++; return Response.json({}); } });
  await expect(catalog.search("show")).rejects.toThrow("movies.tmdbAccessToken"); expect(calls).toBe(0);
  const noMapping = new TmdbTvCatalog("token", { url: "", apiKey: "" }, { fetcher: fixture().fetcher });
  expect((await noMapping.details(70)).seasons[0]!.unavailableReason).toContain("SkyHook");
  for (const response of [new Response("secret body", { status: 429 }), new Response("not json"),
    new Response("x".repeat(8 * 1024 * 1024 + 1))]) {
    const c = new TmdbTvCatalog("secret", sonarr, { fetcher: async () => response });
    try { await c.search("show"); throw new Error("Expected failure"); }
    catch (error) { expect(error).toBeInstanceOf(TvCatalogError); expect(String(error)).not.toContain("secret"); }
  }
});

test("identity disagreements and number collisions fail without inventing mappings or classifying structure by anime", async () => {
  const original = fixtureTvEntry();
  for (const anime of [true, false]) {
    const work = { ...original, anime, key: "anilist:800" };
    expect(canonicalTvSeasons(work, 70).entry.key).toBe("anilist:800");
    expect([...canonicalTvSeasons(work, 70).seasons.keys()]).toEqual([0, 1, 2]);
  }
  const duplicate = fixtureTvEntry(); duplicate.episodes[2]!.numbering = structuredClone(duplicate.episodes[1]!.numbering);
  expect(() => canonicalTvSeasons(duplicate, 70)).toThrow("Ambiguous");
  const ephemeral = fixtureTvEntry(); ephemeral.episodes[0]!.identity = { stability: "ephemeral", source: "fixture" };
  expect(() => canonicalTvSeasons(ephemeral, 70)).toThrow("identity/numbering");
  expect(() => canonicalTvSeasons(original, 71)).toThrow("mapping");
  expect(() => canonicalTvSeasons({ ...original, key: "tvdb:701" }, 70)).toThrow("Conflicting canonical TV work identity");
  const f = fixture();
  const mismatch = new TmdbTvCatalog("private", sonarr, { fetcher: f.fetcher, canonical: {
    async resolve() { return { ...fixtureTvEntry(), tvdbId: 701 }; },
  } });
  await expect(mismatch.resolve(70)).rejects.toThrow("disagree");
});

test("configured catalog is reused without ignoring credential rotation", () => {
  const settings = defaultSettings();
  const first = configuredTvCatalog(settings);
  expect(configuredTvCatalog(settings)).toBe(first);
  settings.movies.tmdbAccessToken = "rotated";
  expect(configuredTvCatalog(settings)).not.toBe(first);
});

test("SkyHook artwork is retained only from explicit public provider images when TMDB has no poster", async () => {
  const posterUrl = "https://artworks.thetvdb.com/banners/posters/700.jpg";
  const f = fixture({ tmdb: { ...details, poster_path: null }, skyhook: { ...skyhookShow, images: [{ coverType: "poster", url: posterUrl }] } });
  const catalog = new TmdbTvCatalog("token", { url: "", apiKey: "" }, { fetcher: f.fetcher });
  expect((await catalog.resolve(70)).posterUrl).toBe(posterUrl);
  expect(f.calls).toHaveLength(2);
});

test("special discovery uses only canonical TVDB episode IDs, not TMDB counts or ordinals", async () => {
  const f = fixture({ tmdb: { ...details, seasons: [{ season_number: 0, name: "TMDB Specials", episode_count: 20 }] },
    skyhook: { ...skyhookShow, episodes: [...skyhookShow.episodes,
      { tvdbShowId: 700, tvdbId: 7021, seasonNumber: 0, episodeNumber: 8, title: "Second" },
      { tvdbShowId: 700, tvdbId: 7034, seasonNumber: 0, episodeNumber: 3, title: "First", airDate: "2020-01-01" },
    ] } });
  const catalog = new TmdbTvCatalog("token", { url: "", apiKey: "" }, { fetcher: f.fetcher });
  const detail = await catalog.details(70);
  expect(detail.seasons).toMatchObject([{ seasonNumber: 0, episodeCount: 2, requestable: false }, { seasonNumber: 2, requestable: true }]);
  expect(detail.specials).toEqual([
    { episodeKey: "tvdb:7034", episodeNumber: 3, title: "First", airDate: "2020-01-01" },
    { episodeKey: "tvdb:7021", episodeNumber: 8, title: "Second", airDate: null },
  ]);
});

test("provider timeouts bound both response headers and streaming bodies, and concurrency is capped", async () => {
  for (const fetcher of [
    async () => new Promise<Response>(() => {}),
    async () => new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("{")); } })),
  ] as TvFetcher[]) {
    const catalog = new TmdbTvCatalog("token", sonarr, { fetcher, timeoutMs: 5 });
    await expect(catalog.search("show")).rejects.toBeInstanceOf(TvCatalogError);
  }
  const catalog = new TmdbTvCatalog("token", sonarr, { fetcher: async () => new Promise<Response>(() => {}), timeoutMs: 20 });
  const requests = Array.from({ length: 8 }, (_, index) => catalog.search(`work-${index}`).catch((error) => error));
  await expect(catalog.search("ninth")).rejects.toThrow("busy");
  await Promise.all(requests);
});

test("malformed Sonarr configuration does not prevent TMDB discovery or safe public SkyHook mapping", async () => {
  const f = fixture({ skyhook: skyhookShow });
  const catalog = new TmdbTvCatalog("token", { url: "https://user:private@sonarr.invalid", apiKey: "secret" }, { fetcher: f.fetcher });
  expect(await catalog.search("show")).toHaveLength(1);
  const detail = await catalog.details(70);
  expect(detail.seasons[0]!.requestable).toBe(true);
  expect(detail.seasons[0]!.unavailableReason).toBeNull();
  expect(JSON.stringify(detail)).not.toContain("private"); expect(JSON.stringify(detail)).not.toContain("secret");
});

test("shows absent from Sonarr resolve stable TVDB aired identity via bounded GET-only SkyHook without adding or monitoring them", async () => {
  for (const connection of [sonarr, { url: "", apiKey: "" }]) {
    const f = fixture({ series: [], skyhook: skyhookShow });
    const catalog = new TmdbTvCatalog("token", connection, { fetcher: f.fetcher });
    const result = await catalog.resolve(70);
    expect(result.posterUrl).toBe("https://image.tmdb.org/t/p/w342/poster.jpg");
    expect(result.key).toBe("tvdb:700"); expect(result.episodes.map((ep) => ep.key)).toEqual(["tvdb:7001", "tvdb:7002"]);
    expect(result.episodes[1]!.numbering).toEqual([
      { scheme: "tvdb:aired", season: 2, episode: 2, absolute: null, source: "skyhook:v1:tvdb:aired:700" },
      { scheme: "tvdb:absolute", season: null, episode: null, absolute: 15, source: "skyhook:v1:tvdb:aired:700" },
    ]);
    expect(result.episodes[1]!.airDate).toBe("2099-01-01");
    expect((await catalog.details(70)).seasons).toMatchObject([{ seasonNumber: 2, episodeCount: 2, requestable: true }]);
    const skyhook = f.calls.filter((call) => call.url.hostname === "skyhook.sonarr.tv");
    expect(skyhook).toHaveLength(1); expect(skyhook[0]!.url.href).toBe("https://skyhook.sonarr.tv/v1/tvdb/shows/en/700");
    expect([...new Headers(skyhook[0]!.init?.headers)]).toEqual([]);
    expect(f.calls.every((call) => call.init?.method === "GET")).toBe(true);
    expect(f.calls.some((call) => /command|monitor/.test(call.url.pathname))).toBe(false);
  }
});

test("SkyHook fallback never accepts mismatched works, foreign episode membership, missing provider IDs, or colliding aired coordinates", async () => {
  for (const show of [
    { ...skyhookShow, tvdbId: 701 }, { ...skyhookShow, tmdbId: 71 },
    { ...skyhookShow, episodes: [{ ...skyhookShow.episodes[0], tvdbShowId: 701 }] },
    { ...skyhookShow, episodes: [{ ...skyhookShow.episodes[0], tvdbId: undefined }] },
    { ...skyhookShow, episodes: [skyhookShow.episodes[0], { ...skyhookShow.episodes[1], episodeNumber: 1 }] },
    { ...skyhookShow, episodes: [skyhookShow.episodes[0], { ...skyhookShow.episodes[1], tvdbId: 7001 }] },
  ]) {
    const f = fixture({ series: [], skyhook: show });
    const catalog = new TmdbTvCatalog("token", sonarr, { fetcher: f.fetcher });
    await expect(catalog.resolve(70)).rejects.toBeInstanceOf(TvCatalogError);
    expect((await catalog.details(70)).seasons.every((s) => !s.requestable && !!s.unavailableReason)).toBe(true);
  }
  const f = fixture({ series: [], skyhook: skyhookShow, tmdb: { ...details, external_ids: { tvdb_id: null } } });
  await expect(new TmdbTvCatalog("token", sonarr, { fetcher: f.fetcher }).resolve(70)).rejects.toThrow("does not supply a TVDB work mapping");
  expect(f.calls).toHaveLength(1);
});
