/**
 * Editor-navigable companion to movies-api-map.md. Open this file in your
 * TypeScript editor and Cmd-click an imported type/function to visit its source.
 * Types only: this file does not start services or register any new endpoints.
 * Transmission and shadow interfaces at the bottom are PROPOSED, NOT IMPLEMENTED.
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
type RpcCall<N extends "movies" | "requests", M extends keyof Inputs[N] & keyof Outputs[N]> =
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

// PROPOSED ONLY: normalized observation contracts, not upstream wire schemas.
export interface ExternalMovieObservation {
  source: { instanceId: string; movieId: number; downloadId: string | null; eventId: string | null };
  movie: CatalogMovie;
  releaseTitle: string;
  observedAt: string;
  outcome: "downloading" | "imported" | "blocked" | "failed" | "unknown";
  messages: string[];
  sourcePath: string | null;
}
export interface RadarrObserver {
  observe(input: { cursor: string | null }): Promise<{
    observations: ExternalMovieObservation[];
    nextCursor: string;
  }>;
}
export interface SeerrObserver {
  approvedMovieRequests(input: { cursor: string | null }): Promise<{
    requests: Array<{ externalRequestId: string; tmdbId: number; requestedAt: string }>;
    nextCursor: string;
  }>;
}
export interface ShadowImportInput {
  movie: Movie;
  policy: MovieQualityPolicy;
  offer: MovieOffer;
  selection: MovieSelection;
  files: MovieFileCandidate[];
  // No Radarr outcome/reasoning is supplied to the deciding agent.
}
export interface ShadowDecision {
  action: "would-import" | "would-reacquire" | "needs-attention" | "unobservable";
  selectedFileIndex: number | null;
  facts: MovieFacts | null;
  quality: QualityEvaluation | null;
  reason: string;
}
export interface ShadowEvaluator {
  evaluate(input: ShadowImportInput): Promise<ShadowDecision>;
}
export type ShadowEvidencePorts = {
  probe: MovieServices["probe"];
  decider: Pick<MovieDecider, "inspect">;
  // Deliberately no TorrentClient, publication, adoption, indexer or shell.
};
export type ShadowVerdict =
  | "podible-correct" | "radarr-correct" | "both-correct" | "both-wrong" | "uncertain";
export interface ShadowCase {
  id: number;
  observation: ExternalMovieObservation;
  policySnapshot: MovieQualityPolicy;
  decision: ShadowDecision | null;
  job: MovieRequestView["job"];
  review: { verdict: ShadowVerdict; notes: string } | null;
}
export interface MovieShadowRpc {
  list(input: { cursor?: string; limit?: number }): Promise<{ cases: ShadowCase[]; nextCursor: string | null }>;
  get(input: { caseId: number }): Promise<{ case: ShadowCase }>;
  sync(input: {}): Promise<{ jobId: JobRow["id"] }>;
  retry(input: { caseId: number }): Promise<{ jobId: JobRow["id"] }>;
  review(input: { caseId: number; verdict: ShadowVerdict; notes: string }): Promise<{ case: ShadowCase }>;
}
