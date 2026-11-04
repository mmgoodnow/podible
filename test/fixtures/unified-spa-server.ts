import { createPodibleFetchHandler } from "../../src/http";
import { createBookSpaData } from "./book-spa-data";
const data = await createBookSpaData();
const settings = data.repo.getSettings();
settings.movies.tmdbAccessToken = "fixture-token";
settings.tv.sonarrUrl = "https://sonarr.invalid";
settings.tv.sonarrApiKey = "fixture-key";
data.repo.updateSettings(settings);
const originalFetch = globalThis.fetch;
globalThis.fetch = Object.assign(async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  if (url.hostname === "api.themoviedb.org") {
    if (url.pathname === "/3/trending/movie/week" || url.pathname === "/3/trending/tv/week") return Response.json({ results: Array.from({ length: 14 }, (_, i) => ({
      id: i === 0 ? url.pathname.includes("/movie/") ? 42 : 70 : 1000 + i,
      title: i === 0 ? "Piranesi on Screen" : `Trending movie ${i}`,
      name: i === 0 ? "Piranesi Series" : `Trending series ${i}`,
      poster_path: `/poster${i}.jpg`, release_date: "2026-01-01", first_air_date: "2026-01-01", adult: false,
    })) });
    if (url.pathname === "/3/search/movie") return Response.json({ results: [{ id: 42, title: "Piranesi on Screen", original_title: "Piranesi on Screen", overview: "Fixture movie", original_language: "en", release_date: "2020-01-01" }] });
    if (url.pathname === "/3/search/tv") return Response.json({ results: [{ id: 70, name: "Piranesi Series", original_name: "Piranesi Series", overview: "Fixture TV", first_air_date: "2099-01-01" }] });
    if (url.pathname === "/3/tv/70") return Response.json({ id: 70, name: "Piranesi Series", original_name: "Piranesi Series", overview: "Fixture TV", first_air_date: "2099-01-01", external_ids: { tvdb_id: 700 }, seasons: [
      { season_number: 1, name: "Season 1", episode_count: 2, air_date: "2099-01-01" },
      { season_number: 2, name: "Season 2", episode_count: 1, air_date: "2099-02-01" },
    ] });
    return new Response("Fixture route not implemented", { status: 404 });
  }
  if (url.hostname === "sonarr.invalid") {
    if (url.pathname === "/api/v3/series") return Response.json([{ id: 7, tvdbId: 700, tmdbId: 70, title: "Piranesi Series", year: 2099, seriesType: "standard" }]);
    if (url.pathname === "/api/v3/episode") return Response.json([
      { id: 17, seriesId: 7, tvdbId: 7001, title: "One", seasonNumber: 1, episodeNumber: 1, airDate: "2099-01-01" },
      { id: 18, seriesId: 7, tvdbId: 7002, title: "Two", seasonNumber: 1, episodeNumber: 2, airDate: "2099-01-02" },
      { id: 19, seriesId: 7, tvdbId: 7003, title: "Three", seasonNumber: 2, episodeNumber: 1, airDate: "2099-02-01" },
    ]);
    return new Response("Fixture route not implemented", { status: 404 });
  }
  if (url.hostname === "openlibrary.org") {
    if (url.pathname === "/search.json") return Response.json({ docs: [{ key: "/works/OL900W", title: "Piranesi Companion", author_name: ["Fixture Author"], first_publish_year: 2024 }] });
    return Response.json({ entries: [] });
  }
  if (["image.tmdb.org", "covers.openlibrary.org"].includes(url.hostname)) return new Response(null, { status: 404 });
  return originalFetch(input, init);
}, { preconnect: originalFetch.preconnect });
const handler = createPodibleFetchHandler(data.repo, Date.now());
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
console.log(`UNIFIED_FIXTURE=${JSON.stringify({ url: server.url.origin })}`);
process.on("SIGTERM", () => { server.stop(true); process.exit(0); });
