import { createHash } from "node:crypto";
import type { AppSettings } from "../app-types";
import type { ModelProvider } from "@openai/agents-core";
import { managedTransmission } from "../media/managed-transmission";
import { PlexMovieLibrary } from "../media/plex-library";
import { TmdbCatalog } from "./catalog";
import type { MovieCatalog } from "./catalog";
import { SeerrClient } from "./seerr";
import { createMovieDecider } from "./agents";
import {
  MovieIndexers,
  DiscoveredMovieIndexers,
  fetchMovieMetainfo,
} from "./offers";
import { ProwlarrDiscovery } from "../media/prowlarr";
import { probeMovie } from "./probe";
import type { MovieServices } from "./operations";
import { movieRoots } from "./acquisition";

let catalog: { token: string; value: MovieCatalog } | undefined;
let indexers: { key: string; value: Pick<MovieIndexers, "search"> } | undefined;
export function movieCatalog(settings: AppSettings) {
  const key = createHash("sha256")
    .update(
      JSON.stringify([settings.movies.tmdbAccessToken, settings.movies.seerr]),
    )
    .digest("hex");
  if (catalog?.token !== key)
    catalog = {
      token: key,
      value:
        settings.movies.tmdbAccessToken || !settings.movies.seerr.url
          ? new TmdbCatalog(settings.movies.tmdbAccessToken)
          : new SeerrClient(settings.movies.seerr),
    };
  return catalog!.value;
}
export function movieIndexers(settings: AppSettings) {
  const key = createHash("sha256")
    .update(JSON.stringify([settings.prowlarr, settings.torznab]))
    .digest("hex");
  if (indexers?.key !== key)
    indexers = {
      key,
      value: settings.prowlarr.url
        ? new DiscoveredMovieIndexers(new ProwlarrDiscovery(settings.prowlarr))
        : new MovieIndexers(settings.torznab),
    };
  return indexers!.value;
}
export function configuredMovieServices(
  settings: AppSettings,
  modelProvider?: ModelProvider,
): MovieServices {
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
