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

export type JobProcessResult = "done";

/** Domain handlers are code, not a database execution-type enum. */
export async function processJob(ctx: WorkerContext, job: JobRow): Promise<JobProcessResult> {
  if (job.type === "indexer_feed_sync") return syncIndexerFeeds(ctx);
  if (job.type === "movie_feed_match") return matchMovieFeed(ctx, job);
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
  throw new Error(`Unsupported job type: ${job.type}`);
}
