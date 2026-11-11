import type { ModelProvider } from "@openai/agents-core";
import type { AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";
import type { TorrentClient } from "../media/torrent-client";
import { managedTvTransmission } from "../media/managed-transmission";
import { cachedMovieMetainfo, type MovieOffer, type MovieSearch } from "../movies/offers";
import { probeMovie } from "../movies/probe";
import { createTvDecider, type TvDecider } from "./agents";
import { searchTvReleases } from "./indexers";
import { PlexTvLibrary } from "./plex";
import type { TvLibrary } from "./publication";
import type { TvEntry, TvEpisode } from "./types";
import type { TvCatalog } from "./catalog";

export type TvServices = {
  search(entry: TvEntry, query?: string, episodes?: TvEpisode[]): Promise<MovieSearch>;
  decider(entry: TvEntry): TvDecider;
  client: TorrentClient;
  library: TvLibrary;
  fetchMetainfo(offer: MovieOffer): Promise<Uint8Array>;
  probe: typeof probeMovie;
  seasonCatalog?: Pick<TvCatalog, "resolve">;
  timing?: { pollMs?: number; stallMs?: number; plexAttempts?: number; plexRetryMs?: number };
};
export function configuredTvServices(settings: AppSettings, modelProvider: ModelProvider | undefined, repo: BooksRepo): TvServices {
  return {
    search: (entry, query, episodes) => searchTvReleases(settings, entry, query, { repo, modelProvider, episodes }),
    decider: (entry) => createTvDecider(settings, modelProvider, (query) => searchTvReleases(settings, entry, query, { repo, modelProvider }),
      offer => cachedMovieMetainfo(repo, offer, "inspection")),
    client: managedTvTransmission(settings.tv.managedRoot),
    library: new PlexTvLibrary({ url: settings.movies.plexUrl, token: settings.movies.plexToken, sectionId: settings.tv.plexSectionId }),
    fetchMetainfo: offer => cachedMovieMetainfo(repo, offer, "acquisition"),
    probe: probeMovie,
  };
}
