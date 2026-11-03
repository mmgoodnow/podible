import { expect, test } from "bun:test";
import { TmdbCatalog, CatalogUnavailableError } from "../../src/movies/catalog";

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
