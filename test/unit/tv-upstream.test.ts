import { describe, expect, test } from "bun:test";
import { SonarrCatalog, SonarrObserver } from "../../src/tv/sonarr";
import { tvCatalogEntrySchema } from "../../src/tv/types";
import {
  approvedSeerrTv,
  SeerrTvClient,
  selectedSeerrTvSeasons,
  sonarrInstance,
  TvUpstreamError,
  TvUpstreamHttp,
  type SeerrTvRequest,
  type TvFetcher,
} from "../../src/tv/upstream";

const secret = "server-private-test-key";
const connection = { url: "https://sonarr.example/sonarr", apiKey: secret };
const seerrSettings = { url: "https://seerr.example/seerr", apiKey: secret, pollIntervalMinutes: 1 };
const modified = "2026-11-03T12:00:00.000Z";
const series = {
  id: 17, tvdbId: 307375, tmdbId: 67075, title: "Mob Psycho 100",
  alternateTitles: [{ title: "Mob Psycho" }], year: 2016, seriesType: "anime",
  images: [{ coverType: "poster", url: "/sonarr/MediaCover/17/poster.jpg", remoteUrl: "https://artworks.thetvdb.com/banners/posters/307375.jpg" }],
};
const episode = {
  id: 172, seriesId: 17, tvdbId: 5704031, title: "Episode title",
  seasonNumber: 2, episodeNumber: 3, absoluteEpisodeNumber: 15,
  sceneSeasonNumber: 1, sceneEpisodeNumber: 15, sceneAbsoluteEpisodeNumber: 15,
  airDate: "2019-01-21", airDateUtc: "2019-01-21T14:00:00Z", runtime: 24,
};
function request(overrides: Record<string, unknown> = {}): SeerrTvRequest {
  return {
    id: 726, status: 2, type: "tv", is4k: false, createdAt: modified, updatedAt: modified,
    media: { tmdbId: 67075, tvdbId: 307375, externalServiceId: 17 },
    requestedBy: { id: 5, plexId: 8, displayName: "User" },
    seasons: [{ seasonNumber: 2, status: 2 }], ...overrides,
  } as SeerrTvRequest;
}
type Call = { url: URL; init: RequestInit | undefined };
function fixture(route: (url: URL) => unknown | Response) {
  const calls: Call[] = [];
  const fetcher: TvFetcher = async (input, init) => {
    const url = new URL(String(input));
    calls.push({ url, init });
    const response = route(url);
    return response instanceof Response ? response : Response.json(response);
  };
  return { calls, fetcher };
}
function catalogFixture(seriesRows: readonly unknown[] = [series], episodes: readonly unknown[] = [episode]) {
  return fixture((url) => {
    if (url.pathname === "/sonarr/api/v3/series") return seriesRows;
    if (url.pathname === "/sonarr/api/v3/series/17") return seriesRows[0];
    if (url.pathname === "/sonarr/api/v3/episode") return episodes;
    throw new Error(`Unexpected fixture route: ${url.pathname}`);
  });
}
async function failure(action: Promise<unknown>): Promise<TvUpstreamError> {
  try { await action; } catch (error) {
    expect(error).toBeInstanceOf(TvUpstreamError);
    return error as TvUpstreamError;
  }
  throw new Error("Expected a typed upstream error");
}

describe("TV GET transport", () => {
  test("artwork-only reads use exact Sonarr identity, public remote images and no episode or search requests", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    expect(await catalog.poster({ sonarrSeriesId: 17, tvdbId: 307375, tmdbId: 67075 }))
      .toBe("https://artworks.thetvdb.com/banners/posters/307375.jpg");
    expect(f.calls).toHaveLength(1);
    expect(f.calls[0]!.url.pathname).toBe("/sonarr/api/v3/series/17");
    expect((await catalog.resolve({ sonarrSeriesId: 17, tvdbId: 307375 })).posterUrl)
      .toBe("https://artworks.thetvdb.com/banners/posters/307375.jpg");
    await expect(catalog.poster({ sonarrSeriesId: 17, tvdbId: 999 })).rejects.toThrow("identity cross-check");
    for (const remoteUrl of ["http://artworks.thetvdb.com/banners/p.jpg", "https://artworks.thetvdb.com/banners/p.jpg?apikey=secret",
      "https://private.invalid/poster.jpg", "https://user:pass@artworks.thetvdb.com/banners/p.jpg", undefined]) {
      const invalid = catalogFixture([{ ...series, images: [{ coverType: "poster", remoteUrl, url: "/MediaCover/17/poster.jpg" }] }]);
      expect(await new SonarrCatalog(connection, invalid.fetcher).poster({ sonarrSeriesId: 17, tvdbId: 307375 })).toBeNull();
    }
  });

  test("preserves URL bases, refuses redirects and keeps credentials out of objects", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    await catalog.get("tvdb:307375");
    for (const call of f.calls) {
      expect(call.url.origin).toBe("https://sonarr.example");
      expect(call.url.pathname.startsWith("/sonarr/api/v3/")).toBe(true);
      expect(call.url.href).not.toContain(secret);
      expect(call.init?.method).toBe("GET");
      expect(call.init?.redirect).toBe("manual");
      expect(new Headers(call.init?.headers).get("x-api-key")).toBe(secret);
      expect(call.init?.signal).toBeInstanceOf(AbortSignal);
    }
    expect(JSON.stringify(catalog)).not.toContain(secret);
    expect(Object.values(catalog)).not.toContain(secret);
  });

  test("invalid URL/configuration errors never reflect configured secrets", () => {
    for (const url of ["file:///tmp/secret", `https://user:${secret}@example.com`, `https://example.com?apiKey=${secret}`, `https://example.com/#${secret}`]) {
      expect(() => new SonarrCatalog({ url, apiKey: secret })).toThrow(TvUpstreamError);
      try { new SonarrCatalog({ url, apiKey: secret }); } catch (error) {
        expect(String(error)).not.toContain(secret);
      }
    }
    expect(() => new SonarrCatalog({ ...connection, apiKey: "" })).toThrow("server API key");
  });

  test.each([301, 302, 401, 403, 404])("HTTP %i never returns body, headers or credentials", async (status) => {
    const f = fixture(() => new Response(`private body ${secret}`, {
      status, headers: { location: `https://remote.example/?key=${secret}` },
    }));
    const error = await failure(new SonarrCatalog(connection, f.fetcher).get("tvdb:307375"));
    expect(error.status).toBe(status);
    expect(error.retryable).toBe(false);
    expect(String(error)).not.toContain(secret);
    expect(JSON.stringify(error)).not.toContain(secret);
    expect(f.calls).toHaveLength(1);
  });

  test.each(["90", "0", "nonsense"])("429 exposes retry metadata (%s), without retrying fetch", async (value) => {
    const f = fixture(() => new Response(secret, { status: 429, headers: { "Retry-After": value } }));
    const error = await failure(new SeerrTvClient(seerrSettings, f.fetcher).latest());
    expect(error.provider).toBe("seerr");
    expect(error.status).toBe(429);
    expect(error.retryable).toBe(true);
    expect(error.retryAfterMs).toBe(value === "90" ? 90_000 : value === "0" ? 1000 : 60_000);
    expect(f.calls).toHaveLength(1);
  });

  test("HTTP-date Retry-After and temporary service outages stay retryable", async () => {
    const retryAt = new Date(Date.now() + 180_000).toUTCString();
    const f = fixture(() => new Response(null, { status: 503, headers: { "Retry-After": retryAt } }));
    const error = await failure(new SeerrTvClient(seerrSettings, f.fetcher).latest());
    expect(error.retryAfterMs).toBeGreaterThan(178_000);
    expect(error.retryAfterMs).toBeLessThanOrEqual(180_000);
    expect(error.retryable).toBe(true);
    expect(f.calls).toHaveLength(1);
  });

  test("network exceptions and invalid JSON are redacted", async () => {
    const fetcher: TvFetcher = async () => { throw new Error(`private URL https://user:${secret}@example.com`); };
    const network = await failure(new SonarrCatalog(connection, fetcher).get("tvdb:307375"));
    expect(network.retryable).toBe(true);
    expect(String(network)).not.toContain(secret);
    const f = fixture(() => new Response(secret));
    const json = await failure(new SeerrTvClient(seerrSettings, f.fetcher).latest());
    expect(String(json)).toContain("invalid JSON");
    expect(String(json)).not.toContain(secret);
  });

  test("a timed out request throws durable retry data, never the raw exception", async () => {
    const fetcher: TvFetcher = async (_url, init) => new Promise((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new Error(secret)), { once: true });
    });
    const http = new TvUpstreamHttp("sonarr", connection, fetcher, 5);
    const error = await failure(http.get("series"));
    expect(error.message).toContain("timed out");
    expect(error.retryable).toBe(true);
    expect(error.message).not.toContain(secret);
  });
});

describe("Sonarr raw catalog", () => {
  test("maps wire TVDB episode identity separately from aired/absolute/scene coordinates", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    const entry = await catalog.resolve({ tvdbId: 307375 });
    expect(tvCatalogEntrySchema.safeParse(entry).success).toBe(true);
    expect(entry.key).toBe("tvdb:307375");
    expect(entry.anime).toBe(true);
    expect(entry.relations).toEqual([]);
    expect(entry.episodes[0]?.key).toBe("tvdb:5704031");
    expect(entry.episodes[0]?.numbering).toContainEqual({
      scheme: "tvdb:aired", season: 2, episode: 3, absolute: null,
      source: `sonarr:${catalog.instance}:api/v3/episode`,
    });
    expect(entry.episodes[0]?.numbering).toContainEqual({
      scheme: `sonarr:${catalog.instance}:scene`, season: 1, episode: 15, absolute: null,
      source: `sonarr:${catalog.instance}:api/v3/episode`,
    });
    expect(entry.episodes[0]?.numbering.find((n) => n.scheme === "tvdb:absolute")?.absolute).toBe(15);
    expect(entry.episodes[0]?.refs?.find((r) => r.provider === "sonarr")?.instance).toBe(catalog.instance);
    expect(f.calls.map((c) => c.url.searchParams.get("tvdbId"))).toContain("307375");
  });

  test("renumbering does not change stable episode identity, and titles do not imply relationships", async () => {
    const a = catalogFixture();
    const b = catalogFixture([{ ...series, title: "Mob Psycho 100 II: Sequel" }], [{ ...episode, seasonNumber: 9, episodeNumber: 90, absoluteEpisodeNumber: 190 }]);
    const first = await new SonarrCatalog(connection, a.fetcher).get("tvdb:307375");
    const second = await new SonarrCatalog(connection, b.fetcher).get("tvdb:307375");
    expect(second.episodes[0]?.key).toBe(first.episodes[0]?.key);
    expect(second.episodes[0]?.numbering).not.toEqual(first.episodes[0]?.numbering);
    expect(second.relations).toEqual([]);
  });

  test("local series/episode fallback keys are explicit, instance scoped and not provider-stable", async () => {
    const f = catalogFixture([{ ...series, tvdbId: 0, tmdbId: null }], [{ ...episode, tvdbId: 0 }]);
    const catalog = new SonarrCatalog(connection, f.fetcher);
    const entry = await catalog.get(`sonarr:${catalog.instance}:17`);
    expect(entry.key).toBe(`sonarr:${catalog.instance}:17`);
    expect(entry.episodes[0]?.key).toBe(`sonarr:${catalog.instance}:172`);
    expect(entry.episodes[0]?.identity?.stability).toBe("ephemeral");
    expect(entry.episodes[0]?.numbering.some((n) => n.scheme.startsWith("tvdb:"))).toBe(false);
    expect(sonarrInstance("https://another.example/sonarr")).not.toBe(catalog.instance);
    expect(sonarrInstance(connection.url + "/")).toBe(catalog.instance);
  });

  test("foreign-instance keys are rejected without issuing a request", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    await failure(catalog.get(`sonarr:${sonarrInstance("https://other.example")}:17`));
    expect(f.calls).toHaveLength(0);
  });

  test("uses TMDB equality only when TVDB is not provided, never a matching title", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    expect((await catalog.resolve({ tmdbId: 67075 })).key).toBe("tvdb:307375");
    await failure(catalog.resolve({ tvdbId: 999, tmdbId: 67075 }));
    await failure(catalog.resolve({ tmdbId: 999 }));
    await failure(catalog.resolve({ sonarrSeriesId: 17 }));
  });

  test("ambiguous provider matches and inconsistent direct identities fail closed", async () => {
    const ambiguous = catalogFixture([series, { ...series, id: 18 }]);
    await failure(new SonarrCatalog(connection, ambiguous.fetcher).resolve({ tvdbId: 307375 }));
    const wrong = catalogFixture([{ ...series, id: 18 }]);
    await failure(new SonarrCatalog(connection, wrong.fetcher).get(`sonarr:${sonarrInstance(connection.url)}:17`));
    const f = catalogFixture();
    await failure(new SonarrCatalog(connection, f.fetcher).resolve({ sonarrSeriesId: 17, tvdbId: 999 }));
    await failure(new SonarrCatalog(connection, f.fetcher).resolve({ sonarrSeriesId: 17, tvdbId: 307375, tmdbId: 999 }));
  });

  test("Seerr resolution crosschecks local IDs and does not reuse standard association for 4K", async () => {
    const f = catalogFixture();
    const catalog = new SonarrCatalog(connection, f.fetcher);
    expect((await catalog.resolveSeerr(request())).key).toBe("tvdb:307375");
    expect(f.calls[0]?.url.pathname).toBe("/sonarr/api/v3/series/17");
    f.calls.length = 0;
    await catalog.resolveSeerr(request({ is4k: true }));
    expect(f.calls[0]?.url.pathname).toBe("/sonarr/api/v3/series");
    await failure(catalog.resolveSeerr(request({ media: { tvdbId: 999, tmdbId: 67075, externalServiceId: 17 } })));
    expect(() => catalog.resolveSeerr(request({ status: 1 }))).toThrow("approved seasons");
  });

  test("unverified scene numbers are not trusted as usable mappings", async () => {
    const f = catalogFixture([series], [{ ...episode, unverifiedSceneNumbering: true }]);
    const entry = await new SonarrCatalog(connection, f.fetcher).get("tvdb:307375");
    expect(entry.episodes[0]?.numbering.some((n) => n.scheme.includes(":scene"))).toBe(false);
  });

  test.each([
    { episodes: [{ ...episode, seriesId: 18 }] },
    { episodes: [episode, { ...episode, id: 173 }] },
    { episodes: [episode, { ...episode, tvdbId: 5704032 }] },
    { episodes: [{ ...episode, seasonNumber: -1 }] },
    { episodes: [{ ...episode, airDate: "2026-02-30" }] },
  ])("rejects malformed or conflicting episode metadata", async ({ episodes }) => {
    const f = catalogFixture([series], episodes);
    await failure(new SonarrCatalog(connection, f.fetcher).get("tvdb:307375"));
  });
});

describe("Sonarr comparison observations", () => {
  test("reads history + queue only, emits episode-scoped evidence and redacts diagnostics", async () => {
    const now = new Date().toISOString();
    const f = fixture((url) => {
      if (url.pathname.endsWith("/queue")) return { totalRecords: 1, records: [{
        id: 9, seriesId: 17, episodeId: 172, series, episode, sizeleft: 0,
        title: "Release", outputPath: "/downloads/Release", trackedDownloadState: "importBlocked",
        errorMessage: `GET https://user:${secret}@private.example/path?passkey=private`,
        statusMessages: [{ title: "Import blocked", messages: [`apiKey=${secret}`] }],
      }] };
      if (url.pathname.endsWith("/history")) return { totalRecords: 1, records: [{
        id: 8, seriesId: 17, episodeId: 172, series, episode,
        eventType: "downloadFolderImported", sourceTitle: "Release", date: now,
        data: { droppedPath: "/downloads/Release/a.mkv", importedPath: "/library/a.mkv" },
      }] };
      if (url.pathname.endsWith("/series/17")) return series;
      if (url.pathname.endsWith("/episode")) return [episode];
      throw new Error("Unexpected route");
    });
    const observer = new SonarrObserver(connection, f.fetcher);
    const observations = await observer.observe();
    expect(observations).toHaveLength(2);
    expect(observations.map((o) => o.sonarr.outcome)).toEqual(["blocked", "imported"]);
    expect(observations.every((o) => o.episodeKeys[0] === "tvdb:5704031")).toBe(true);
    expect(observations[1]?.libraryPath).toBe("/library/a.mkv");
    expect(observations[0]?.key).toBe(`sonarr:${observer.instance}:queue:9`);
    expect(JSON.stringify(observations)).not.toContain(secret);
    expect(JSON.stringify(observations)).not.toContain("private.example");
    expect(JSON.stringify(observer)).not.toContain(secret);
    expect(f.calls.filter((c) => c.url.pathname.endsWith("/series/17"))).toHaveLength(1);
    expect(f.calls.filter((c) => c.url.pathname.endsWith("/episode"))).toHaveLength(1);
    for (const call of f.calls) expect(call.init?.method).toBe("GET");
    expect(f.calls[0]?.url.searchParams.get("sortKey")).toBe("added");
    expect(f.calls.at(-1)?.url.searchParams.get("includeEpisode")).toBe("true");
  });

  test("does not infer episodes from unassociated queue packs, or retain old history", async () => {
    const f = fixture((url) => ({ totalRecords: 1, records: url.pathname.endsWith("queue")
      ? [{ id: 1, seriesId: 17, seasonNumber: 2, sizeleft: 0 }]
      : [{ id: 2, seriesId: 17, episodeId: 172, eventType: "downloadFolderImported", date: "2000-01-01T00:00:00Z" }],
    }));
    expect(await new SonarrObserver(connection, f.fetcher).observe()).toEqual([]);
    expect(f.calls).toHaveLength(2);
  });

  test("malformed history cannot masquerade as an empty/partial successful observation", async () => {
    const now = new Date().toISOString();
    for (const records of [
      [{ id: 1, eventType: "downloadFolderImported", date: now }],
      [{ id: 1, seriesId: 17, episodeId: 172, eventType: "downloadFolderImported" }],
      [{ id: 1, date: "2000-01-01T00:00:00Z" }, { id: 2, date: now }],
    ]) {
      const f = fixture((url) => url.pathname.endsWith("/queue")
        ? { totalRecords: 0, records: [] } : { totalRecords: records.length, records });
      await failure(new SonarrObserver(connection, f.fetcher).observe());
    }
  });

  test("truncation and misleading pagination are errors, not successful partial observations", async () => {
    for (const result of [{ totalRecords: 101, records: [] }, { totalRecords: 1, page: 2, records: [{ id: 1 }] }]) {
      const f = fixture(() => result);
      await failure(new SonarrObserver(connection, f.fetcher).observe());
    }
    const f = fixture((url) => {
      const page = Number(url.searchParams.get("page"));
      return { totalRecords: 1001, records: Array.from({ length: 100 }, (_, i) => ({ id: page * 100 + i })) };
    });
    await failure(new SonarrObserver(connection, f.fetcher).observe());
    expect(f.calls).toHaveLength(10);
  });
});

describe("Seerr TV intent feed", () => {
  test("latest baseline is a single GET for newest TV request, with no detail/history fanout", async () => {
    const f = fixture(() => ({ pageInfo: { results: 1000 }, results: [request()] }));
    const client = new SeerrTvClient(seerrSettings, f.fetcher);
    expect((await client.latest())?.id).toBe(726);
    expect(f.calls).toHaveLength(1);
    expect(f.calls[0]?.url.pathname).toBe("/seerr/api/v1/request");
    expect(Object.fromEntries(f.calls[0]!.url.searchParams)).toEqual({
      take: "1", skip: "0", sort: "added", sortDirection: "desc", mediaType: "tv",
    });
    expect(JSON.stringify(client)).not.toContain(secret);
    const empty = fixture(() => ({ pageInfo: { results: 0 }, results: [] }));
    expect(await new SeerrTvClient(seerrSettings, empty.fetcher).latest()).toBeNull();
  });

  test("request objects strip unneeded server credentials and requester permissions", async () => {
    const f = fixture(() => ({ pageInfo: { results: 1 }, results: [request({
      apiKey: secret, server: { apiKey: secret },
      requestedBy: { id: 5, plexId: 8, email: "private@example.com", plexToken: secret, permissions: 2 },
      media: { tmdbId: 67075, tvdbId: 307375, apiKey: secret },
    })] }));
    const result = await new SeerrTvClient(seerrSettings, f.fetcher).latest();
    expect(JSON.stringify(result)).not.toContain(secret);
    expect(result?.requestedBy).not.toHaveProperty("permissions");
    expect(result?.requestedBy).not.toHaveProperty("email");
  });

  test("only explicitly selected approved-lineage seasons qualify; never all seasons", () => {
    const selected = request({ seasons: [
      { seasonNumber: 0, status: 2 }, { seasonNumber: 1, status: 1 },
      { seasonNumber: 2, status: 2 }, { seasonNumber: 3, status: 3 },
      { seasonNumber: 4, status: 4 }, { seasonNumber: 5, status: 5 },
    ] });
    expect(selectedSeerrTvSeasons(selected)).toEqual([0, 2, 4, 5]);
    expect(approvedSeerrTv(selected)).toBe(true);
    expect(approvedSeerrTv(request({ status: 1 }))).toBe(false);
    expect(approvedSeerrTv(request({ status: 3 }))).toBe(false);
    expect(approvedSeerrTv(request({ seasons: [{ seasonNumber: 2, status: 1 }] }))).toBe(false);
    expect(() => selectedSeerrTvSeasons(request({ seasons: undefined }))).toThrow(TvUpstreamError);
  });

  test("modified scans retain pending transitions, include the two-minute overlap and advance through only after success", async () => {
    const rows = [
      request({ id: 730, updatedAt: "2026-11-03T12:01:00Z" }),
      request({ id: 729, status: 1, updatedAt: modified }),
      request({ id: 728, updatedAt: "2026-11-03T11:58:00Z" }),
      request({ id: 727, updatedAt: "2026-11-03T11:57:59Z" }),
    ];
    const f = fixture(() => ({ pageInfo: { results: rows.length }, results: rows }));
    const result = await new SeerrTvClient(seerrSettings, f.fetcher).changes(modified);
    expect(result.requests.map((r) => r.id)).toEqual([730, 729, 728]);
    expect(result.through).toBe("2026-11-03T12:01:00.000Z");
    expect(result.requests.filter(approvedSeerrTv).map((r) => r.id)).toEqual([730, 728]);
    expect(f.calls[0]?.url.searchParams.get("sort")).toBe("modified");
    expect(f.calls[0]?.url.searchParams.get("mediaType")).toBe("tv");
  });

  test("pagination is bounded and deduplicates overlap without per-request lookups", async () => {
    const rows = Array.from({ length: 100 }, (_, i) => request({ id: 1000 - i }));
    const f = fixture((url) => ({ pageInfo: { results: 101 },
      results: url.searchParams.get("skip") === "0" ? rows : [rows[99]],
    }));
    const result = await new SeerrTvClient(seerrSettings, f.fetcher).changes(modified);
    expect(result.requests).toHaveLength(100);
    expect(f.calls).toHaveLength(2);
    expect(f.calls.every((c) => c.url.pathname.endsWith("/request"))).toBe(true);
  });

  test("malformed seasons, identities and page metadata fail closed before returning a new cursor", async () => {
    for (const bad of [
      request({ seasons: undefined }), request({ seasons: [] }),
      request({ seasons: [{ seasonNumber: 2 }] }),
      request({ seasons: [{ seasonNumber: 2, status: 2 }, { seasonNumber: 2, status: 5 }] }),
      request({ media: { tmdbId: 0 } }), request({ type: "movie" }),
      request({ updatedAt: "invalid" }),
    ]) {
      const f = fixture(() => ({ pageInfo: { results: 1 }, results: [bad] }));
      let cursor = modified;
      await failure(new SeerrTvClient(seerrSettings, f.fetcher).changes(cursor).then((r) => { cursor = r.through; }));
      expect(cursor).toBe(modified);
    }
    for (const body of [
      { pageInfo: { results: 101 }, results: [] },
      { pageInfo: { results: 0 }, results: [request()] },
      { pageInfo: { results: 2 }, results: [request(), request({ id: 727, updatedAt: "2026-11-03T12:01:00Z" })] },
    ]) {
      const f = fixture(() => body);
      await failure(new SeerrTvClient(seerrSettings, f.fetcher).changes(modified));
    }
  });

  test("a later-page failure cannot yield a partial batch/cursor", async () => {
    const rows = Array.from({ length: 100 }, (_, i) => request({ id: 1000 - i }));
    const f = fixture((url) => ({ pageInfo: { results: 101 },
      results: url.searchParams.get("skip") === "0" ? rows : [request({ seasons: undefined })],
    }));
    let cursor = modified;
    await failure(new SeerrTvClient(seerrSettings, f.fetcher).changes(cursor).then((r) => { cursor = r.through; }));
    expect(cursor).toBe(modified);
    expect(f.calls).toHaveLength(2);
  });

  test("scan exhaustion and repeated pages are failures, never silent truncation", async () => {
    const f = fixture((url) => {
      const skip = Number(url.searchParams.get("skip"));
      return { pageInfo: { results: 10001 }, results: Array.from({ length: 100 }, (_, i) => request({ id: 20000 - skip - i })) };
    });
    const error = await failure(new SeerrTvClient(seerrSettings, f.fetcher).changes(modified));
    expect(error.message).toContain("bounded scan");
    expect(f.calls).toHaveLength(100);
    const repeated = fixture(() => ({ pageInfo: { results: 201 }, results: Array.from({ length: 100 }, (_, i) => request({ id: i + 1 })) }));
    await failure(new SeerrTvClient(seerrSettings, repeated.fetcher).changes(modified));
    expect(repeated.calls).toHaveLength(2);
  });

  test("explicit detail reads verify identity and reject non-TV payloads", async () => {
    const f = fixture(() => request());
    const client = new SeerrTvClient(seerrSettings, f.fetcher);
    expect((await client.request(726)).id).toBe(726);
    await failure(client.request(725));
    await failure(client.request(-1));
    expect(f.calls).toHaveLength(2);
    const movie = fixture(() => request({ type: "movie" }));
    await failure(new SeerrTvClient(seerrSettings, movie.fetcher).request(726));
  });
});
