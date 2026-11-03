/**
 * Editor-navigable companion to movies-api-map.md. Open this file in your
 * TypeScript editor and Cmd-click an imported type/function to visit its source.
 * Types only: this file does not start services or register any new endpoints.
 * Transmission interfaces at the bottom are proposed; shadow types are implemented.
 */
import type { inferRouterInputs, inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../src/trpc";
import type { AppSettings, JobRow } from "../src/app-types";
import type { moviesRouter, movieRequestsRouter } from "../src/rpc/movies-router";
import type { settingsRouter } from "../src/rpc/settings-router";
import type { Movie, CatalogMovie } from "../src/movies/types";
import type { MovieRequestView } from "../src/movies/contracts";
import type { MovieQualityPolicy, MovieFacts, QualityEvaluation } from "../src/movies/quality";
import type { MovieCatalog } from "../src/movies/catalog";
import type { MovieIndexers, MovieOffer, fetchMovieMetainfo } from "../src/movies/offers";
import type { MovieDecider, MovieServices, MovieSelection, MovieFileCandidate } from "../src/movies/operations";
import type { MovieRepo } from "../src/movies/repo";
import type { MovieLibrary, publishMovieVersion, movieViewEntries, reconcileMovieView } from "../src/movies/publication";
import type { TorrentClient } from "../src/media/torrent-client";
import type { adoptContentFile } from "../src/media/content-store";
import type { initializeManagedContent, reconcileLibraryView } from "../src/media/library-view";
import type { inspectMetainfo } from "../src/media/metainfo";
import type { probeMovie, factsFromMovieProbe } from "../src/movies/probe";
import type { evaluateMovieQuality, assessMovieUpgrade } from "../src/movies/quality";
import type { queueMovieAcquisition, movieRequestView } from "../src/movies/service";
import type { acquireMovie } from "../src/movies/acquisition";
import type { reconcileMovies } from "../src/movies/maintenance";
import type { WorkerContext } from "../src/worker/context";
import type { createJobExecutionRuntime } from "../src/worker/execution";

// These are the actual implementation types, not copied approximations.
export type {
  Movie, CatalogMovie, MovieRequestView, MovieQualityPolicy, MovieFacts,
  QualityEvaluation, MovieCatalog, MovieDecider, MovieServices, MovieSelection,
  MovieFileCandidate, MovieOffer, MovieRepo, MovieLibrary, TorrentClient,
};
export type { MovieProbe } from "../src/movies/probe";
export type { TorrentRef, DownloadLocation, TorrentSnapshot } from "../src/media/torrent-client";
export type { MovieSettings } from "../src/movies/settings";
export type CanonicalFile = Awaited<ReturnType<typeof adoptContentFile>>;

// PUBLIC: user-authenticated RPC. Params/results are inferred from the real
// router, so hover shows current shapes and changes cannot silently drift.
export type Inputs = inferRouterInputs<AppRouter>;
export type Outputs = inferRouterOutputs<AppRouter>;
type RpcCall<N extends "movies" | "requests" | "movieShadow", M extends keyof Inputs[N] & keyof Outputs[N]> =
  (input: Inputs[N][M]) => Promise<Outputs[N][M]>;

export interface PublicMovieRpc {
  movies: {
    status: RpcCall<"movies", "status">;
    search: RpcCall<"movies", "search">;
    get: RpcCall<"movies", "get">;
    request: RpcCall<"movies", "request">;
  };
  requests: {
    list: RpcCall<"requests", "list">;
    get: RpcCall<"requests", "get">;
    retry: RpcCall<"requests", "retry">;
    checkUpgrade: RpcCall<"requests", "checkUpgrade">;
    withdraw: RpcCall<"requests", "withdraw">;
    searchReleases: RpcCall<"requests", "searchReleases">;
    selectRelease: RpcCall<"requests", "selectRelease">;
  };
}

// Cmd-click moviesRouter/movieRequestsRouter/settingsRouter to inspect the
// actual schemas, authorization declarations and handler implementations.
export interface HandlerDefinitions {
  movies: typeof moviesRouter.routes;
  requests: typeof movieRequestsRouter.routes;
  settings: typeof settingsRouter.routes;
}
export interface AdminSettingsRpc {
  get(input: {}): Promise<AppSettings>;
  update(input: { settings: AppSettings }): Promise<AppSettings>;
}

// PRIVATE: server-to-server/code boundaries. No generic torrent RPC proxy.
export interface PrivateMovieApi {
  catalog: MovieCatalog;
  indexers: Pick<MovieIndexers, "search">;
  decisions: MovieDecider;
  torrents: TorrentClient;
  plex: MovieLibrary;
  services: MovieServices;
  repository: MovieRepo;
  evidence: {
    fetchMovieMetainfo: typeof fetchMovieMetainfo;
    inspectMetainfo: typeof inspectMetainfo;
    probeMovie: typeof probeMovie;
    factsFromMovieProbe: typeof factsFromMovieProbe;
    evaluateMovieQuality: typeof evaluateMovieQuality;
    assessMovieUpgrade: typeof assessMovieUpgrade;
  };
  content: {
    initializeManagedContent: typeof initializeManagedContent;
    adoptContentFile: typeof adoptContentFile;
    reconcileLibraryView: typeof reconcileLibraryView;
  };
  publication: {
    movieViewEntries: typeof movieViewEntries;
    reconcileMovieView: typeof reconcileMovieView;
    publishMovieVersion: typeof publishMovieVersion;
  };
  application: {
    queueMovieAcquisition: typeof queueMovieAcquisition;
    movieRequestView: typeof movieRequestView;
    acquireMovie: typeof acquireMovie;
    reconcileMovies: typeof reconcileMovies;
  };
  execution: NonNullable<WorkerContext["execution"]>;
  executionRuntime: Awaited<ReturnType<typeof createJobExecutionRuntime>>;
}

// PROPOSED ONLY: private Docker-managed daemon, not yet implemented.
export interface ManagedTorrentEngine {
  start(): Promise<void>;
  waitUntilReady(signal?: AbortSignal): Promise<void>;
  status(): {
    state: "starting" | "ready" | "restarting" | "stopped" | "failed";
    message: string | null;
  };
  shutdown(input: { graceMs: number }): Promise<void>;
}
export interface ContainerSupervisor {
  run(): Promise<void>;
  shutdown(): Promise<void>;
}

// IMPLEMENTED: read-only import audit. Types follow the live sources/router.
export type { ShadowObservation, ShadowInput, ShadowResult } from "../src/movies/shadow-types";
export type { MovieShadowServices } from "../src/movies/shadow";
export type { RadarrObserver } from "../src/movies/shadow-radarr";
export type ShadowCase = Outputs["movieShadow"]["get"]["case"];
export type MovieShadowRpc = { [M in keyof Inputs["movieShadow"] & keyof Outputs["movieShadow"]]: RpcCall<"movieShadow", M> };
// Future: Seerr request attribution and same-candidate release-selection comparison.
