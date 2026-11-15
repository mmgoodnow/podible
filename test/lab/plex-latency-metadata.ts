import { PlexMovieLibrary } from "../../src/media/plex-library";
import { PlexTvLibrary } from "../../src/tv/plex";
import type { TvEntry, TvEpisode } from "../../src/tv/types";

export const fixtureTitle = "Podible Timing Fixture";
const year = 2024, date = "2024-01-02", movieId = 9_999_001, showId = 9_999_002;
const episodeId = (episode: number) => showId + episode;
const provider = "tv.plex.agents.custom.podible.latency";
const metadataPath = "/library/metadata";

export async function startLatencyMetadata(tv: boolean, count: number, plex: (route: string, method?: string) => Promise<Response>) {
  function item(type: string, ratingKey: string, title: string) {
    return { type, ratingKey, title, year, originallyAvailableAt: date,
      key: `${metadataPath}/${ratingKey}`, guid: `${provider}://${type}/${ratingKey}` };
  }
  const movie = { ...item("movie", "fixture-movie", fixtureTitle), Guid: [{ id: `tmdb://${movieId}` }] };
  const show = { ...item("show", "fixture-show", fixtureTitle), Guid: [{ id: `tvdb://${showId}` }] };
  const parent = (entry: ReturnType<typeof item>) => ({ parentRatingKey: entry.ratingKey, parentGuid: entry.guid,
    parentKey: entry.key, parentType: entry.type, parentTitle: entry.title });
  const season = { ...item("season", "fixture-season", "Season 1"), index: 1, ...parent(show) };
  const episodes = Array.from({ length: count }, (_, index) => ({
    ...item("episode", `fixture-episode-${index + 1}`, `Episode ${index + 1}`), ...parent(season),
    grandparentRatingKey: show.ratingKey, grandparentGuid: show.guid, grandparentKey: show.key,
    grandparentTitle: show.title, parentIndex: 1, index: index + 1, Guid: [{ id: `tvdb://${episodeId(index + 1)}` }],
  }));
  const all = tv ? [show, season, ...episodes] : [movie];
  const container = (Metadata: object[], offset = 0, totalSize = Metadata.length) => ({
    identifier: provider, offset, size: Metadata.length, totalSize, Metadata,
  });
  const children = (entry: ReturnType<typeof item>, includeChildren: unknown) => {
    const entries = entry.type === "show" ? [season] : entry.type === "season" ? episodes : [];
    return includeChildren && entries.length ? { ...entry, Children: container(entries) } : entry;
  };
  const requests: Record<string, number> = {};
  let firstMatchAt: number | null = null, firstMetadataAt: number | null = null;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(req) {
    const url = new URL(req.url), route = url.pathname;
    const json = (body: object) => Response.json(body);
    if (req.method === "GET" && route === "/") {
      requests.describe = (requests.describe ?? 0) + 1;
      return json({ MediaProvider: { identifier: provider, title: fixtureTitle,
        Types: (tv ? [2, 3, 4] : [1]).map(type => ({ type, Scheme: [{ scheme: provider }] })),
        Feature: [{ type: "metadata", key: metadataPath }, { type: "match", key: `${metadataPath}/matches` }],
      } });
    }
    if (req.method === "POST" && route === `${metadataPath}/matches`) {
      firstMatchAt ??= performance.now();
      requests.match = (requests.match ?? 0) + 1;
      const match = await req.json() as { type: number; title?: string; parentTitle?: string; grandparentTitle?: string;
        index?: number; parentIndex?: number; includeChildren?: number };
      const candidate = match.type === 1 && !tv && match.title === fixtureTitle ? movie
        : match.type === 2 && tv && match.title === fixtureTitle ? show
        : match.type === 3 && tv && match.parentTitle === fixtureTitle && match.index === 1 ? season
        : match.type === 4 && tv && match.grandparentTitle === fixtureTitle && match.parentIndex === 1
          ? episodes[(match.index ?? 0) - 1] : undefined;
      return json({ MediaContainer: container(candidate ? [children(candidate, match.includeChildren)] : []) });
    }
    if (req.method === "GET" && route.startsWith(`${metadataPath}/`)) {
      requests.metadata = (requests.metadata ?? 0) + 1;
      const [, , , id, suffix] = route.split("/");
      const entry = all.find(item => item.ratingKey === id);
      if (!entry) return new Response(null, { status: 404 });
      if (!suffix) firstMetadataAt ??= performance.now();
      if (suffix === "images") return json({ MediaContainer: { size: 0, Image: [] } });
      if (suffix === "children" || suffix === "grandchildren") {
        const entries = entry.type === "show" ? suffix === "grandchildren" ? episodes : [season] : entry.type === "season" ? episodes : [];
        const offset = Number(req.headers.get("X-Plex-Container-Start") ?? url.searchParams.get("X-Plex-Container-Start") ?? 0);
        const size = Number(req.headers.get("X-Plex-Container-Size") ?? url.searchParams.get("X-Plex-Container-Size") ?? 20);
        return json({ MediaContainer: container(entries.slice(offset, offset + size), offset, entries.length) });
      }
      return json({ MediaContainer: container([children(entry, url.searchParams.get("includeChildren") === "1")]) });
    }
    return new Response(null, { status: 404 });
  } });
  try {
    await (await plex(`/media/providers/metadata?${new URLSearchParams({ uri: server.url.origin })}`, "POST")).body?.cancel();
    const { MediaContainer } = await (await plex(`/media/providers/metadata/group?${new URLSearchParams({
      title: fixtureTitle, primaryIdentifier: provider,
    })}`, "POST")).json() as { MediaContainer: { MetadataAgentProviderGroup: { id: string }[] } };
    const groupId = MediaContainer.MetadataAgentProviderGroup.at(0)?.id;
    if (!groupId) throw new Error("Plex did not register the lab metadata group");
    return { groupId, identifier: provider, requests, get firstMatchAt() { return firstMatchAt; },
      get firstMetadataAt() { return firstMetadataAt; }, stop: () => server.stop(true) };
  } catch (error) { await server.stop(true); throw error; }
}

export function exactLatencyFixture(tv: boolean, url: string, sectionId: string, paths: string[]) {
  const settings = { url, sectionId, token: "" };
  type Variant = "correct" | "wrong-identity" | "wrong-path";
  if (!tv) {
    const plex = new PlexMovieLibrary(settings);
    const movie = { id: 1, tmdbId: movieId, imdbId: null, title: fixtureTitle, originalTitle: fixtureTitle,
      year, overview: "", runtimeMinutes: null, originalLanguage: "en", posterUrl: null };
    return (variant: Variant) => plex.verify({ ...movie, tmdbId: variant === "wrong-identity" ? movie.tmdbId + 100 : movie.tmdbId },
      variant === "wrong-path" ? paths.map(file => `${file}.missing`) : paths);
  }
  const plex = new PlexTvLibrary(settings);
  const episodes = paths.map((_, i): TvEpisode => ({ id: i + 1, key: `tvdb:${episodeId(i + 1)}`, title: `Episode ${i + 1}`,
    airDate: date, runtimeMinutes: null, refs: [{ provider: "tvdb", id: String(episodeId(i + 1)), instance: null, source: "lab" }],
    identity: { stability: "provider", source: "lab" },
    numbering: [{ scheme: "tvdb:aired", season: 1, episode: i + 1, absolute: null, source: "lab" }],
  }));
  const entry: TvEntry = { id: 1, key: `tvdb:${showId}`, tvdbId: showId, tmdbId: null, title: fixtureTitle, aliases: [],
    year, posterUrl: null, backdropUrl: null, anime: false, episodes, relations: [], refs: [] };
  return (variant: Variant) => plex.verify(entry, paths.map((serverPath, i) => ({
    serverPath: variant === "wrong-path" ? `${serverPath}.missing` : serverPath,
    episodes: variant === "wrong-identity" ? [{ ...episodes[i]!, refs: [{ ...episodes[i]!.refs[0]!, id: String(episodeId(i + 100)) }] }] : [episodes[i]!],
  })));
}
