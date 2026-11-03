import { expect, test } from "bun:test";
import {
  SeerrClient,
  seerrInstance,
  seerrRequestSchema,
  seerrSettingsSchema,
} from "../../src/movies/seerr";
import { movieCatalog } from "../../src/movies/runtime";
import { defaultSettings } from "../../src/settings";
import { seerrRequest } from "../fixtures/seerr";

const settings = {
  url: "https://seerr.invalid/base",
  apiKey: "private-key",
  pollIntervalMinutes: 1,
};
type Fetcher = NonNullable<ConstructorParameters<typeof SeerrClient>[1]>;

test("Seerr uses real camelCase API payloads and only GETs requests/catalog under the URL base", async () => {
  const routes: string[] = [];
  const client = new SeerrClient(settings, (async (input, init) => {
    const url = new URL(String(input));
    routes.push(url.pathname);
    expect(init?.method).toBe("GET");
    expect(init?.redirect).toBe("manual");
    expect(new Headers(init?.headers).get("x-api-key")).toBe("private-key");
    expect(url.searchParams.has("apikey")).toBe(false);
    const movie = {
      id: 550,
      title: "Fight Club",
      originalTitle: "Fight Club",
      releaseDate: "1999-10-15",
      originalLanguage: "en",
      posterPath: "/poster.jpg",
      overview: "A movie",
      runtime: 139,
      externalIds: { imdbId: "tt0137523" },
    };
    if (url.pathname.endsWith("/request")) {
      expect(url.searchParams.get("mediaType")).toBe("movie");
      expect(url.searchParams.get("sortDirection")).toBe("desc");
      return Response.json({
        pageInfo: { pages: 1, pageSize: 100, results: 1, page: 1 },
        results: [
          {
            ...seerrRequest(731),
            requestedBy: {
              ...seerrRequest(731).requestedBy,
              email: "private@example.invalid",
              plexToken: "secret-token",
              permissions: 2,
            },
          },
        ],
      });
    }
    if (url.pathname.endsWith("/request/731"))
      return Response.json(seerrRequest(731));
    if (url.pathname.endsWith("/movie/550")) return Response.json(movie);
    if (url.pathname.endsWith("/search"))
      return Response.json({
        results: [
          { mediaType: "movie", ...movie },
          { mediaType: "tv", id: 1 },
        ],
      });
    throw new Error("Unexpected endpoint");
  }) as Fetcher);
  expect((await client.latest())?.requestedBy).not.toHaveProperty("plexToken");
  expect(
    (await client.changes(seerrRequest(730).updatedAt)).requests,
  ).toHaveLength(1);
  expect((await client.request(731)).id).toBe(731);
  const metadata = await client.get(550);
  expect(metadata).toMatchObject({
    tmdbId: 550,
    year: 1999,
    imdbId: "tt0137523",
    runtimeMinutes: 139,
  });
  expect((await client.search("Fight Club"))[0]).toEqual(metadata);
  expect(
    routes.every((route) =>
      /^\/base\/api\/v1\/(request(?:\/731)?|movie\/550|search)$/.test(route),
    ),
  ).toBe(true);
  expect(JSON.stringify(await client.latest())).not.toContain(
    "private@example",
  );
});

test("Seerr modified pagination overlaps the cursor, stops on old pages, and deduplicates moving rows", async () => {
  const recent = seerrRequest(101, 2, {
    updatedAt: "2026-11-02T14:00:00.000Z",
  });
  let calls = 0;
  const client = new SeerrClient(settings, (async (input) => {
    const skip = Number(new URL(String(input)).searchParams.get("skip"));
    calls++;
    return Response.json({
      pageInfo: { results: 1000 },
      results:
        skip === 0
          ? Array.from({ length: 100 }, (_, i) => ({ ...recent, id: 101 + i }))
          : skip === 100
            ? [
                { ...recent, status: 1, updatedAt: "2026-11-02T13:59:59.000Z" },
                seerrRequest(1),
              ]
            : [seerrRequest(1)],
    });
  }) as Fetcher);
  const result = await client.changes("2026-11-02T14:00:30.000Z");
  expect(result.requests).toHaveLength(100);
  expect(result.requests.find((r) => r.id === 101)?.status).toBe(2);
  expect(result.through).toBe("2026-11-02T14:00:30.000Z");
  // The second page includes an overlap row, so the third page is needed.
  expect(calls).toBe(3);
});

test("Seerr refuses an unbounded modified scan instead of advancing past unseen rows", async () => {
  let calls = 0;
  const client = new SeerrClient(settings, (async () => {
    calls++;
    return Response.json({
      pageInfo: { results: 10_001 },
      results: [seerrRequest(100)],
    });
  }) as Fetcher);
  await expect(client.changes(seerrRequest(100).updatedAt)).rejects.toThrow(
    "cursor was not advanced",
  );
  expect(calls).toBe(100);
});

test("Seerr schema rejects invalid timestamps/identities and errors never echo secrets or follow redirects", async () => {
  expect(
    seerrRequestSchema.safeParse({ ...seerrRequest(1), updatedAt: "yesterday" })
      .success,
  ).toBe(false);
  expect(
    seerrSettingsSchema.safeParse({
      ...settings,
      url: "https://user:password@seerr.invalid",
    }).success,
  ).toBe(false);
  expect(seerrInstance(settings.url + "/")).toBe(seerrInstance(settings.url));
  for (const status of [302, 401, 429, 503]) {
    const client = new SeerrClient(
      settings,
      (async () =>
        new Response("private-key", {
          status,
          headers: { location: "https://other.invalid" },
        })) as Fetcher,
    );
    await expect(client.latest()).rejects.toThrow(`HTTP ${status}`);
    await expect(client.latest()).rejects.not.toThrow("private-key");
  }
  const client = new SeerrClient(settings, (async () =>
    Response.json(seerrRequest(2))) as Fetcher);
  await expect(client.request(1)).rejects.toThrow("different request identity");
  const broken = new SeerrClient(settings, (async () => {
    throw new Error("https://private-key@upstream.invalid");
  }) as Fetcher);
  await expect(broken.latest()).rejects.toThrow(
    "Seerr request feed is unavailable",
  );
});

test("movie metadata falls back to the configured Seerr catalog, not a bundled TMDB credential", () => {
  const s = defaultSettings();
  s.movies.seerr = settings;
  expect(movieCatalog(s)).toBeInstanceOf(SeerrClient);
  s.movies.tmdbAccessToken = "operator-supplied";
  expect(movieCatalog(s)).not.toBeInstanceOf(SeerrClient);
});
