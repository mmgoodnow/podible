import type { TvCatalog, TvCatalogDetails } from "../../src/tv/catalog";
import { canonicalTvSeasons } from "../../src/tv/catalog";
import type { TvCatalogEntry } from "../../src/tv/types";

export function fixtureTvEntry(): TvCatalogEntry {
  return {
    key: "tvdb:700", tmdbId: 70, tvdbId: 700, title: "A Work", aliases: ["Another Title"], year: 2020,
    anime: false, source: "verified-catalog", refs: [], relations: [],
    episodes: [
      { key: "tvdb:7000", title: "Special", airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 0, episode: 1, absolute: null, source: "verified-catalog" }] },
      { key: "tvdb:7001", title: "First", airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null, source: "verified-catalog" }] },
      { key: "tvdb:7002", title: "Future", airDate: "2099-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 2, absolute: null, source: "verified-catalog" }] },
      { key: "tvdb:7003", title: "Undated", airDate: null, runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 3, absolute: null, source: "verified-catalog" }] },
      { key: "tvdb:7004", title: "Sequel", airDate: "2099-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 2, episode: 1, absolute: null, source: "verified-catalog" }] },
    ],
  };
}
export function fixtureTvCatalog() {
  let entry = fixtureTvEntry();
  const summary = { tmdbId: 70, title: "A Work", originalTitle: "A Work", year: 2020, overview: "A story", posterUrl: null };
  const calls = { search: 0, details: 0, resolve: 0 };
  const catalog: TvCatalog = {
    async search() { calls.search++; return [summary]; },
    async details(tmdbId): Promise<TvCatalogDetails> {
      calls.details++;
      const { seasons } = canonicalTvSeasons(entry, tmdbId);
      return { ...summary, numberingScheme: "tvdb:aired",
        episodeKeysBySeason: new Map([...seasons].map(([number, episodes]) => [number, episodes.map(ep => ep.key)])),
        seasons: [...seasons].map(([seasonNumber, episodes]) => ({
        seasonNumber, title: `Season ${seasonNumber}`, episodeCount: episodes.length,
        airDate: null, requestable: true, unavailableReason: null,
      })) };
    },
    async resolve() { calls.resolve++; return structuredClone(entry); },
  };
  return { catalog, calls, entry: () => structuredClone(entry), setEntry: (next: TvCatalogEntry) => { entry = next; } };
}
