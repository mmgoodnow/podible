import type { ModelProvider } from "@openai/agents-core";

import type { AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";

export type WorkerContext = {
  repo: BooksRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
  shouldStop?: () => boolean;
  /** Override the agent model provider (e.g. a fake for tests). */
  modelProvider?: ModelProvider;
  movieServices?: import("../movies/operations").MovieServices;
  tvServices?: import("../tv/runtime").TvServices;
  tvShadowServices?: import("../tv/shadow").TVShadowServices;
  tvSyncServices?: import("../tv/sync").TvSyncServices;
  tvFeedMatcher?: import("../tv/feed").TvFeedMatcher;
  bookTorrentClient?: import("../media/torrent-client").TorrentClient;
  retentionAdapters?: import("../media/retention-policy").RetentionAdapters;
  tvRetentionAdapters?: import("../media/tv-retention").TvRetentionAdapters & { observeDisk?: typeof import("../media/retention-filesystem").observeRetentionDisk };
  stewardshipAdapters?: import("../media/torrent-stewardship").StewardshipAdapters;
  movieShadowServices?: import("../movies/shadow").MovieShadowServices;
  seerrSource?: import("../movies/seerr").SeerrSource;
  movieFeedMatcher?: import("../movies/agents").MovieFeedMatcher;
  bookFeedMatcher?: import("../library/feed-matcher").BookFeedMatcher;
  indexerFeedMediaSources?: (media: import("../media/indexer-gateway").IndexerMedia) => Promise<import("../media/prowlarr").DiscoveredIndexer[]>;
  indexerFeedSources?: () => Promise<import("../media/prowlarr").DiscoveredIndexer[]>;
  indexerFeedGateway?: Pick<import("../media/indexer-gateway").IndexerGateway, "page">;
  /** In-process resource lanes; sibling steps share execution ownership. */
  activeLocks?: Set<string>;
  execution?: {
    key: string;
    run<T>(name: string, handler: () => T | Promise<T>): Promise<T>;
    sleep(name: string, milliseconds: number): Promise<void>;
    assertActive(): void;
  };
};

export function durableStep<T>(ctx: WorkerContext, name: string, handler: () => T | Promise<T>): Promise<T> {
  return ctx.execution ? ctx.execution.run(name, handler) : Promise.resolve().then(handler);
}

export function workerLog(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}
