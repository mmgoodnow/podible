import { createHash } from "node:crypto";
import type { AppSettings } from "../app-types";
import type { ModelProvider } from "@openai/agents-core";
import { managedTransmission } from "../media/managed-transmission";
import { PlexMovieLibrary } from "../media/plex-library";
import { TmdbCatalog } from "./catalog";
import { createMovieDecider } from "./agents";
import { MovieIndexers, fetchMovieMetainfo } from "./offers";
import { probeMovie } from "./probe";
import type { MovieServices } from "./operations";
import { movieRoots } from "./acquisition";

let catalog: { token: string; value: TmdbCatalog } | undefined;
let indexers: { key: string; value: MovieIndexers } | undefined;
export function movieCatalog(settings: AppSettings) {
  if (catalog?.token !== settings.movies.tmdbAccessToken)
    catalog = {
      token: settings.movies.tmdbAccessToken,
      value: new TmdbCatalog(settings.movies.tmdbAccessToken),
    };
  return catalog!.value;
}
export function movieIndexers(settings: AppSettings) {
  const key = createHash("sha256")
    .update(JSON.stringify(settings.torznab))
    .digest("hex");
  if (indexers?.key !== key)
    indexers = { key, value: new MovieIndexers(settings.torznab) };
  return indexers!.value;
}
export function configuredMovieServices(
  settings: AppSettings,
  modelProvider?: ModelProvider,
): MovieServices {
  if (!settings.movies.enabled)
    throw new Error("Movies are disabled by the administrator");
  movieRoots(settings.movies.managedRoot, settings.movies.plexViewPath);
  return {
    catalog: movieCatalog(settings),
    search: (movie, query) => movieIndexers(settings).search(movie, query),
    decider: createMovieDecider(settings, modelProvider),
    client: managedTransmission(settings.movies.managedRoot),
    library: new PlexMovieLibrary({
      url: settings.movies.plexUrl,
      token: settings.movies.plexToken,
      sectionId: settings.movies.plexSectionId,
    }),
    fetchMetainfo: fetchMovieMetainfo,
    probe: probeMovie,
  };
}
