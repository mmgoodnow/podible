import { expect, test } from "bun:test";
import { HomeDiscovery } from "../../src/media/home-discovery";

function fetcher(fn: (url: URL, init?: RequestInit) => Promise<Response>): typeof fetch {
  return Object.assign((input: Parameters<typeof fetch>[0], init?: RequestInit) => fn(new URL(String(input)), init), { preconnect: fetch.preconnect });
}

test("discovery coalesces whole lists, bounds cards, filters adult entries and never performs per-card requests", async () => {
  let calls = 0, now = 0;
  const urls: string[] = [], authorizations: unknown[] = [];
  const cache = new HomeDiscovery(fetcher(async (url, init) => {
    calls++; urls.push(url.pathname); authorizations.push(new Headers(init?.headers).get("authorization"));
    await Bun.sleep(2);
    return Response.json({ results: Array.from({ length: 30 }, (_, i) => ({ id: i + 1, title: `Movie ${i}`, name: `TV ${i}`,
      release_date: "2026-01-01", first_air_date: "2025-01-01", adult: i === 0, poster_path: i === 1 ? "https://secret.invalid/poster" : `/poster${i}.jpg` })) });
  }), () => now);
  const [first, other] = await Promise.all([cache.get("private-token"), cache.get("private-token")]);
  expect(calls).toBe(2); expect(first).toEqual(other);
  expect(urls.sort()).toEqual(["/3/trending/movie/week", "/3/trending/tv/week"]);
  expect(authorizations).toEqual(["Bearer private-token", "Bearer private-token"]);
  expect(first.movies.items).toHaveLength(20);
  expect(first.movies.items[0]).toMatchObject({ tmdbId: 2, posterUrl: null, year: 2026 });
  expect(first.tv.items[1]!.posterUrl).toBe("https://image.tmdb.org/t/p/w342/poster2.jpg");
  expect(JSON.stringify(first)).not.toContain("private-token");
  await cache.get("private-token"); expect(calls).toBe(2);
  now += 600_001; await cache.get("private-token"); expect(calls).toBe(4);
  await cache.get("changed-credential"); expect(calls).toBe(6);
});

test("one failing provider list does not erase another; failure is cached without sensitive error bodies", async () => {
  let calls = 0, now = 0;
  const cache = new HomeDiscovery(fetcher(async url => {
    calls++;
    return url.pathname.includes("/movie/") ? new Response("private token upstream secret stack", { status: 429 })
      : Response.json({ results: [{ id: 7, name: "A series", first_air_date: "", poster_path: null }] });
  }), () => now);
  const result = await cache.get("secret");
  expect(result.movies).toEqual({ items: [], unavailable: true });
  expect(result.tv.items[0]!.title).toBe("A series");
  expect(JSON.stringify(result)).not.toMatch(/secret|token|stack|429/);
  await cache.get("secret"); expect(calls).toBe(2);
  now += 60_001; await cache.get("secret"); expect(calls).toBe(3);
});

test("missing credentials, invalid/oversized payloads and credential churn stay bounded", async () => {
  let calls = 0;
  const cache = new HomeDiscovery(fetcher(async () => { calls++; return new Response("x".repeat(512 * 1024 + 1)); }));
  expect((await cache.get(null)).configured).toBe(false); expect(calls).toBe(0);
  expect((await cache.get("configured")).movies.unavailable).toBe(true);
  await Promise.all(Array.from({ length: 30 }, (_, i) => cache.get(`token-${i}`)));
  expect(calls).toBeLessThanOrEqual(16);
});
