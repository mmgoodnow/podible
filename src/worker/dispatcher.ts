import { processChapterAnalysisJob } from "../chapter-analysis";
import type { JobRow } from "../app-types";

import { processAcquireJob } from "./acquire";
import { workerLog, type WorkerContext } from "./context";
import { processDownloadJob } from "./downloads";
import { processImportJob } from "./imports";
import { processFullLibraryRefreshJob, processReconcileJob } from "./maintenance";

export type JobProcessResult = "done" | "rescheduled";

/**
 * Job type dispatch. Unknown types are treated as no-op and marked succeeded
 * so a stale queue entry cannot block worker progress.
 */
export async function processJob(ctx: WorkerContext, job: JobRow): Promise<JobProcessResult> {
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
  ctx.repo.markJobSucceeded(job.id);
  workerLog(ctx, `[worker] job=${job.id} type=${job.type} unknown_type=1 auto_succeeded=1`);
  return "done";
}
