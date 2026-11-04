import type { ModelProvider } from "@openai/agents-core";
import type { AppSettings } from "../app-types";
import type { TorrentClient } from "../media/torrent-client";
import { managedTvTransmission } from "../media/managed-transmission";
import { fetchMovieMetainfo, type MovieOffer, type MovieSearch } from "../movies/offers";
import { probeMovie } from "../movies/probe";
import { createTvDecider, type TvDecider } from "./agents";
import { searchTvReleases } from "./indexers";
import { PlexTvLibrary } from "./plex";
import type { TvLibrary } from "./publication";
import type { TvEntry } from "./types";

export type TvServices = {
  search(entry: TvEntry, query?: string): Promise<MovieSearch>;
  decider(entry: TvEntry): TvDecider;
  client: TorrentClient;
  library: TvLibrary;
  fetchMetainfo(offer: MovieOffer): Promise<Uint8Array>;
  probe: typeof probeMovie;
  timing?: { pollMs?: number; stallMs?: number; plexAttempts?: number; plexRetryMs?: number };
};
export function configuredTvServices(settings: AppSettings, modelProvider?: ModelProvider): TvServices {
  return {
    search: (entry, query) => searchTvReleases(settings, entry, query),
    decider: (entry) => createTvDecider(settings, modelProvider, (query) => searchTvReleases(settings, entry, query)),
    client: managedTvTransmission(settings.tv.managedRoot),
    library: new PlexTvLibrary({ url: settings.movies.plexUrl, token: settings.movies.plexToken, sectionId: settings.tv.plexSectionId }),
    fetchMetainfo: fetchMovieMetainfo,
    probe: probeMovie,
  };
}
