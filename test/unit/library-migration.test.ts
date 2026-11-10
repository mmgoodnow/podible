import { expect, test } from "bun:test";
import { defaultSettings } from "../../src/settings";
import { previewLibraryMigration } from "../../src/media/library-migration";

test("migration inventories preserve unknowns, source numbering and paging without commands or file claims", async () => {
  const requests: { path: string; method: string; key: string | null }[] = [];
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch(request) {
    const url = new URL(request.url);
    requests.push({ path: url.pathname, method: request.method, key: request.headers.get("X-Api-Key") });
    if (url.pathname === "/radarr/api/v3/movie") return Response.json([
      { id: 1, tmdbId: 42, title: "Zed", monitored: true, hasFile: true, path: "/secret/media" },
      { id: 2, tmdbId: 0, title: "Alpha", monitored: false, hasFile: false },
      { id: 3, title: "Unknown" },
    ]);
    if (url.pathname === "/sonarr/api/v3/series") return Response.json([
      { id: 4, tvdbId: 98, title: "Series", monitored: true, statistics: { episodeFileCount: 24 }, path: "/secret/shows" },
    ]);
    return new Response("Forbidden", { status: 403 });
  } });
  try {
    const settings = defaultSettings();
    settings.movies.shadow.radarrUrl = `${server.url.origin}/radarr`;
    settings.movies.shadow.radarrApiKey = "private-key";
    settings.tv.sonarrUrl = `${server.url.origin}/sonarr`;
    settings.tv.sonarrApiKey = "other-private-key";
    const first = await previewLibraryMigration(settings, { source: "radarr", limit: 2 });
    expect(first).toMatchObject({ total: 3, monitored: 1, withFiles: 1, withoutIdentity: 2, nextOffset: 2 });
    expect(first.items.map(item => item.title)).toEqual(["Alpha", "Unknown"]);
    expect(first.items[1]).toMatchObject({ catalogId: null, monitored: null, filesReported: null });
    const second = await previewLibraryMigration(settings, { source: "radarr", offset: 2, limit: 2 });
    expect(second.nextOffset).toBeNull();
    expect(second.items[0]).toMatchObject({ title: "Zed", catalogId: 42, filesReported: true });
    const tv = await previewLibraryMigration(settings, { source: "sonarr" });
    expect(tv.items[0]).toMatchObject({ catalog: "tvdb", catalogId: 98, episodeFiles: 24, filesReported: true });
    expect(JSON.stringify([first, second, tv])).not.toMatch(/private-key|secret|path|plex|available/i);
    expect(requests).toEqual([
      { path: "/radarr/api/v3/movie", method: "GET", key: "private-key" },
      { path: "/radarr/api/v3/movie", method: "GET", key: "private-key" },
      { path: "/sonarr/api/v3/series", method: "GET", key: "other-private-key" },
    ]);
  } finally { await server.stop(true); }
});

test("migration refuses redirects, invalid schemas and oversized responses without exposing upstream secrets", async () => {
  const settings = defaultSettings();
  settings.movies.shadow.radarrUrl = "https://upstream.invalid/base";
  settings.movies.shadow.radarrApiKey = "private-key";
  for (const response of [new Response(null, { status: 302, headers: { Location: "https://elsewhere.invalid/" } }),
    new Response("private-key", { status: 401 }), Response.json([{ id: 1, title: "Bad", statistics: { episodeFileCount: -1 } }]),
    new Response("x".repeat(16 * 1024 * 1024 + 1))]) {
    let calls = 0;
    const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => {
      calls++;
      expect(init?.redirect).toBe("manual");
      return response;
    }, { preconnect: fetch.preconnect }) as typeof fetch;
    await expect(previewLibraryMigration(settings, { source: "radarr" }, { fetcher })).rejects.toThrow("Nothing was changed");
    expect(calls).toBe(1);
  }
});
