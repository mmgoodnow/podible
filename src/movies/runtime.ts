import { createHash } from "node:crypto";
import type { AppSettings } from "../app-types";
import type { ModelProvider } from "@openai/agents-core";
import type { BooksRepo } from "../repo";
import { managedTransmission } from "../media/managed-transmission";
import { PlexMovieLibrary } from "../media/plex-library";
import { TmdbCatalog } from "./catalog";
import type { MovieCatalog } from "./catalog";
import { SeerrClient } from "./seerr";
import { createMovieDecider } from "./agents";
import {
  MovieIndexers,
  DiscoveredMovieIndexers,
  cachedMovieMetainfo,
} from "./offers";
import { indexerSources } from "../media/indexer-discovery";
import { probeMovie } from "./probe";
import type { MovieServices } from "./operations";
import { movieRoots } from "./acquisition";
import type { Movie } from "./types";
import { movieQueryEvidence, movieSearchIds, planIndexerQuery } from "../media/indexer-query-plan";
import { recordIndexerTraffic } from "../media/indexer-traffic";

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
        ? new DiscoveredMovieIndexers({
            sources: (kind, mode) => indexerSources(settings, kind, mode),
          })
        : new MovieIndexers(settings.torznab),
    };
  return indexers!.value;
}
export function configuredMovieServices(
  settings: AppSettings,
  modelProvider: ModelProvider | undefined,
  repo: BooksRepo,
): MovieServices {
  movieRoots(settings.movies.managedRoot, settings.movies.plexViewPath);
  return {
    catalog: movieCatalog(settings),
    search: (movie, query) => searchMovieReleases(repo, settings, movie, query, modelProvider),
    decider: createMovieDecider(settings, modelProvider, offer => cachedMovieMetainfo(repo, offer, "inspection")),
    client: managedTransmission(settings.movies.managedRoot),
    library: new PlexMovieLibrary({
      url: settings.movies.plexUrl,
      token: settings.movies.plexToken,
      sectionId: settings.movies.plexSectionId,
    }),
    fetchMetainfo: offer => cachedMovieMetainfo(repo, offer, "acquisition"),
    probe: probeMovie,
  };
}

export async function searchMovieReleases(repo: BooksRepo, settings: AppSettings, movie: Movie, query?: string, modelProvider?: ModelProvider) {
  const onRequest = (mode: "id" | "text") => recordIndexerTraffic(repo, "acquisition", mode === "id" ? "search_id" : "search_text");
  if (query?.trim()) return movieIndexers(settings).search(movie, query, { onRequest });
  const plan = await planIndexerQuery(settings, movieQueryEvidence(movie), modelProvider);
  return movieIndexers(settings).search(movie, plan.primaryQuery, { ids: movieSearchIds(movie), alternateQuery: plan.alternateQuery, onRequest });
}
