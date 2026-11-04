import { processChapterAnalysisJob } from "../library/chapter-analysis";
import { processCoverGenerationJob } from "../library/cover-generation";
import type { JobRow } from "../app-types";

import { processAcquireJob } from "./acquire";
import type { WorkerContext } from "./context";
import { processDownloadJob } from "./downloads";
import { processImportJob } from "./imports";
import { processFullLibraryRefreshJob, processReconcileJob } from "./maintenance";
import { processMetadataHydrationJob } from "./metadata-hydration";
import { acquireMovie } from "../movies/acquisition";
import { configuredMovieServices } from "../movies/runtime";
import { reconcileMovies } from "../movies/maintenance";
import { syncMovieShadow, evaluateMovieShadow } from "../movies/shadow";
import { syncSeerrRequests } from "../movies/dual-write";
import { syncIndexerFeeds } from "./indexer-feeds";
import { matchMovieFeed } from "../movies/feed";
import { matchBookFeed } from "./book-feeds";
import { acquireTv } from "../tv/acquisition";
import { syncTvShadow, evaluateTvShadow } from "../tv/shadow";
import { syncTvRequests } from "../tv/sync";
import { reconcileTv } from "../tv/maintenance";
import { matchTvFeed } from "../tv/feed";

export type JobProcessResult = "done";

/** Domain handlers are code, not a database execution-type enum. */
export async function processJob(ctx: WorkerContext, job: JobRow): Promise<JobProcessResult> {
  if (job.type === "tv_acquire") return acquireTv(ctx, job);
  if (job.type === "tv_sync") return syncTvRequests(ctx, job);
  if (job.type === "tv_reconcile") return reconcileTv(ctx);
  if (job.type === "tv_feed_match") return matchTvFeed(ctx, job);
  if (job.type === "tv_shadow_sync") return syncTvShadow(ctx);
  if (job.type === "tv_shadow_evaluate") return evaluateTvShadow(ctx, job);
  if (job.type === "indexer_feed_sync") return syncIndexerFeeds(ctx);
  if (job.type === "movie_feed_match") return matchMovieFeed(ctx, job);
  if (job.type === "book_feed_match") return matchBookFeed(ctx, job);
  if (job.type === "movie_dual_write_sync") return syncSeerrRequests(ctx, job);
  if (job.type === "movie_shadow_sync") return syncMovieShadow(ctx);
  if (job.type === "movie_shadow_evaluate") return evaluateMovieShadow(ctx, job);
  if (job.type === "movie_acquire") return acquireMovie(ctx, job, ctx.movieServices ?? configuredMovieServices(ctx.getSettings(), ctx.modelProvider));
  if (job.type === "movie_reconcile") return reconcileMovies(ctx);
  if (job.type === "download") {
    return processDownloadJob(ctx, job);
  }
  if (job.type === "import") {
    return processImportJob(ctx, job);
  }
  if (job.type === "reconcile") {
    return processReconcileJob(ctx, job);
  }
  if (job.type === "full_library_refresh") {
    return processFullLibraryRefreshJob(ctx, job);
  }
  if (job.type === "acquire") {
    return processAcquireJob(ctx, job);
  }
  if (job.type === "chapter_analysis") {
    return processChapterAnalysisJob(ctx, job);
  }
  if (job.type === "cover_generation") {
    return processCoverGenerationJob(ctx, job);
  }
  if (job.type === "metadata_hydration") {
    return processMetadataHydrationJob(ctx, job);
  }
  const unhandled: never = job.type;
  throw new Error(`Unsupported job type: ${unhandled}`);
}
