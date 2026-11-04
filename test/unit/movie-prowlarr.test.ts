import { expect, spyOn, test } from "bun:test";
import { ProwlarrDiscovery } from "../../src/media/prowlarr";
import {
  DiscoveredMovieIndexers,
  MovieIndexerError,
  MovieIndexers,
} from "../../src/movies/offers";
import { fixtureMovie } from "../fixtures/movie-services";

const settings = {
  url: "https://prowlarr.invalid/base",
  apiKey: "private-key",
};
const indexer = (id: number, category: number, overrides = {}) => ({
  id,
  name: `indexer-${id}`,
  enable: true,
  protocol: "torrent",
  supportsSearch: true,
  capabilities: { categories: [{ id: category, subCategories: [] }] },
  ...overrides,
});

test("Prowlarr discovers configured enabled searchable torrents by category without discarding temporarily disabled sources", async () => {
  const calls: string[] = [];
  const until = new Date(Date.now() + 3_600_000).toISOString();
  const discovery = new ProwlarrDiscovery(settings, (async (input, init) => {
    const url = new URL(String(input));
    calls.push(url.pathname);
    expect(new Headers(init?.headers).get("x-api-key")).toBe("private-key");
    expect(init?.redirect).toBe("manual");
    if (url.pathname.endsWith("/indexerstatus"))
      return Response.json([{ indexerId: 2, disabledTill: until }]);
    return Response.json([
      indexer(1, 2000),
      indexer(2, 2040),
      indexer(3, 3030),
      indexer(4, 7020),
      indexer(5, 2000, { enable: false }),
      indexer(6, 2000, { protocol: "usenet" }),
      indexer(7, 2000, { supportsSearch: false }),
      indexer(8, 100000, {
        capabilities: {
          categories: [{ id: 100000, subCategories: [{ id: 2010 }] }],
        },
      }),
    ]);
  }) as typeof fetch);
  const [movies, audio, ebooks] = await Promise.all([
    discovery.sources("movie"),
    discovery.sources("audio"),
    discovery.sources("ebook"),
  ]);
  expect(calls).toEqual(["/base/api/v1/indexer", "/base/api/v1/indexerstatus"]);
  expect(movies.map((source) => source.name)).toEqual([
    "indexer-1",
    "indexer-2",
    "indexer-8",
  ]);
  expect(movies[1]?.availableAfter).toBe(Date.parse(until));
  expect(movies[0]?.baseUrl).toBe("https://prowlarr.invalid/base/1/api");
  expect(movies[0]?.apiKey).toBe("private-key");
  expect(audio.map((source) => source.name)).toEqual(["indexer-3"]);
  expect(ebooks.map((source) => source.name)).toEqual(["indexer-4"]);
});

test("Prowlarr discovery refreshes membership/health and a partial search retries recovered providers", async () => {
  let now = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const until = now + 30_000;
  let generation = 0;
  const searched: string[] = [];
  const fetcher = (async (input) => {
    const url = new URL(String(input));
    if (url.pathname.endsWith("/indexerstatus"))
      return Response.json(
        generation
          ? []
          : [{ indexerId: 2, disabledTill: new Date(until).toISOString() }],
      );
    if (url.pathname.endsWith("/indexer"))
      return Response.json(
        generation
          ? [indexer(2, 2000), indexer(3, 2000)]
          : [indexer(1, 2000), indexer(2, 2000)],
      );
    expect(url.searchParams.get("cat")).toBe("2000");
    searched.push(url.pathname);
    return new Response(
      `<rss><channel>${url.pathname.endsWith("/2/api") ? "<item><title>Recovered movie</title><guid>recovered</guid><link>https://download.invalid/torrent</link></item>" : ""}</channel></rss>`,
    );
  }) as typeof fetch;
  const discovery = new ProwlarrDiscovery(settings, fetcher);
  const movies = new DiscoveredMovieIndexers(discovery, fetcher);
  try {
    const partial = await movies.search(fixtureMovie);
    expect(partial.offers).toHaveLength(0);
    expect(partial.deferred).toEqual([
      { provider: "indexer-2", retryAt: new Date(until).toISOString() },
    ]);
    expect(searched).toEqual(["/base/1/api"]);
    generation++;
    now += 61_000;
    const recovered = await movies.search(fixtureMovie);
    expect(recovered.offers).toHaveLength(1);
    expect(recovered.deferred).toBeUndefined();
    expect(searched).toEqual(["/base/1/api", "/base/2/api", "/base/3/api"]);
  } finally {
    clock.mockRestore();
  }
});

test("partial search cache expires at provider retry time rather than retaining a false empty result", async () => {
  let now = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  let blocked = true;
  let calls = 0;
  const movies = new MovieIndexers(
    [
      { name: "empty", baseUrl: "https://indexer.invalid/1/api" },
      { name: "retry", baseUrl: "https://indexer.invalid/2/api" },
    ],
    async (url) => {
      calls++;
      if (blocked && new URL(url).pathname === "/2/api")
        return new Response("auth failure must not escape", {
          status: 429,
          headers: { "retry-after": "1" },
        });
      return new Response("<rss><channel/></rss>");
    },
  );
  try {
    expect((await movies.search(fixtureMovie)).deferred).toHaveLength(1);
    expect(calls).toBe(2);
    blocked = false;
    now += 60_001;
    expect((await movies.search(fixtureMovie)).deferred).toBeUndefined();
    expect(calls).toBe(4);
  } finally {
    clock.mockRestore();
  }
});

test("all deferred providers, discovery outages and discovery health failures are retryable, never no-results", async () => {
  let searches = 0;
  const movies = new MovieIndexers(
    [1, 2].map((id) => ({
      name: `provider-${id}`,
      baseUrl: `https://indexer.invalid/${id}/api`,
      availableAfter: Date.now() + 3_600_000,
    })),
    async () => {
      searches++;
      return new Response();
    },
  );
  await expect(movies.search(fixtureMovie)).rejects.toBeInstanceOf(
    MovieIndexerError,
  );
  expect(searches).toBe(0);
  for (const endpoint of ["indexer", "indexerstatus"]) {
    const discovery = new ProwlarrDiscovery(settings, (async (input) => {
      if (String(input).endsWith(`/${endpoint}`))
        throw new Error("https://private-key@upstream.invalid");
      return Response.json([indexer(1, 2000)]);
    }) as typeof fetch);
    const dynamic = new DiscoveredMovieIndexers(discovery);
    await expect(dynamic.search(fixtureMovie)).rejects.toThrow(
      "temporarily unavailable",
    );
    await expect(dynamic.search(fixtureMovie)).rejects.not.toThrow(
      "private-key",
    );
  }
});

test("missing capabilities remain planned/deferred until a discovery refresh can categorize the provider", async () => {
  const discovery = new ProwlarrDiscovery(settings, async (input) =>
    Response.json(
      String(input).endsWith("/indexerstatus")
        ? []
        : [indexer(1, 2000, { capabilities: null })],
    ),
  );
  const sources = await discovery.sources("movie");
  expect(sources).toHaveLength(1);
  expect(sources[0]?.availableAfter).toBeGreaterThan(Date.now());
});
