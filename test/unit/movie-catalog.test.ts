import { expect, test } from "bun:test";
import { TmdbCatalog, CatalogUnavailableError } from "../../src/movies/catalog";

test("movie page extras batch credits and related titles, cache them, and skip bad optional data", async () => {
  const seen: URL[] = [];
  const movie = (id: number, title: string) => ({
    id, title, original_title: title, original_language: "en", release_date: "2025-01-01", poster_path: "/movie.jpg",
  });
  const catalog = new TmdbCatalog("test", async input => {
    seen.push(new URL(input));
    return Response.json({
      id: 11, runtime: 110, genres: [{ name: "Drama" }, {}, { name: "Drama" }],
      credits: {
        crew: [{ id: 1, name: "Ada", job: "Director" }, { id: 2, name: "Bea", job: "Producer" }],
        cast: [{ id: 3, name: "Cam", character: "The Detective", profile_path: "/portrait.jpg" },
          { id: 3, name: "Cam", character: "Duplicate" },
          { id: 4, name: "Dan", profile_path: "https://invalid.test/picture" }, {}],
      },
      recommendations: { results: [movie(11, "Self"), movie(22, "Recommended"), { ...movie(33, "Adult"), adult: true }] },
      similar: { results: [movie(22, "Duplicate"), movie(44, "Similar")] },
    });
  });
  const extras = await catalog.pageExtras(11);
  expect(await catalog.pageExtras(11)).toEqual(extras);
  expect(seen).toHaveLength(1);
  expect(seen[0]?.pathname).toBe("/3/movie/11");
  expect(seen[0]?.searchParams.get("append_to_response")).toBe("credits,recommendations,similar");
  expect(extras).toMatchObject({
    runtimeMinutes: 110, genres: ["Drama"], directors: ["Ada"],
    cast: [
      { id: 3, name: "Cam", character: "The Detective", profileUrl: "https://image.tmdb.org/t/p/w185/portrait.jpg" },
      { id: 4, name: "Dan", character: "", profileUrl: null },
    ],
    related: [{ tmdbId: 22, title: "Recommended" }, { tmdbId: 44, title: "Similar" }],
  });
  const sparse = new TmdbCatalog("test", async () => Response.json({ id: 11, credits: { error: "not found" } }));
  expect(await sparse.pageExtras(11)).toEqual({ runtimeMinutes: null, genres: [], directors: [], cast: [], related: [] });
});

test("TMDB search normalizes identity and caches without exposing download or credential URLs", async () => {
  const calls: string[] = [];
  const fetcher = async (input: string, init?: RequestInit) => {
    calls.push(String(input));
    expect(new Headers(init?.headers).get("Authorization")).toBe(
      "Bearer fixture-secret",
    );
    return Response.json({
      results: [
        {
          id: 7,
          title: "Movie",
          original_title: "Original",
          original_language: "fr",
          release_date: "2020-01-01",
          poster_path: "/safe.jpg",
          backdrop_path: "/wide.jpg",
        },
      ],
    });
  };
  const catalog = new TmdbCatalog("fixture-secret", fetcher);
  const results = await catalog.search("  a & b  ");
  expect(await catalog.search("a & b")).toEqual(results);
  expect(calls).toHaveLength(1);
  expect(new URL(calls[0]!).searchParams.get("query")).toBe("a & b");
  expect(results[0]?.year).toBe(2020);
  expect(results[0]?.posterUrl).toBe(
    "https://image.tmdb.org/t/p/w342/safe.jpg",
  );
  expect(results[0]?.backdropUrl).toBe("https://image.tmdb.org/t/p/w1280/wide.jpg");
  expect(JSON.stringify(results)).not.toContain("fixture-secret");
});
test("TMDB accepts a legacy API key in the existing credential setting", async () => {
  const key = "0123456789abcdef0123456789abcdef";
  const calls: string[] = [];
  const catalog = new TmdbCatalog(` ${key} `, async (input, init) => {
    calls.push(input);
    const url = new URL(input);
    expect(url.origin).toBe("https://api.themoviedb.org");
    expect(url.searchParams.get("api_key")).toBe(key);
    expect(url.searchParams.get("query")).toBe("a & b");
    expect(new Headers(init?.headers).has("Authorization")).toBe(false);
    expect(init?.redirect).toBe("error");
    return Response.json({ results: [] });
  });
  expect(await catalog.search("a & b")).toEqual([]);
  expect(await catalog.search("a & b")).toEqual([]);
  expect(calls).toHaveLength(1);
});
test("TMDB failures do not expose legacy API keys", async () => {
  const key = "0123456789abcdef0123456789abcdef";
  const catalog = new TmdbCatalog(key, async () =>
    new Response("", { status: 401 }),
  );
  await expect(catalog.get(11)).rejects.toThrow("TMDB returned 401");
  const offline = new TmdbCatalog(key, async (url) => {
    throw new Error(`Could not fetch ${url}`);
  });
  await expect(offline.get(11)).rejects.toThrow("TMDB is temporarily unreachable");
});
test("TMDB failures are not empty results; rate limits retain retry information", async () => {
  const catalog = new TmdbCatalog(
    "fixture",
    (async () =>
      new Response("", {
        status: 429,
        headers: { "retry-after": "45" },
      })),
  );
  try {
    await catalog.search("Movie");
    throw new Error("expected failure");
  } catch (error) {
    expect(error).toBeInstanceOf(CatalogUnavailableError);
    expect((error as CatalogUnavailableError).retryAfterSeconds).toBe(45);
  }
  await expect(new TmdbCatalog("").get(1)).rejects.toThrow("not configured");
});
