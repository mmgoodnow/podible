import { setTimeout as sleep } from "node:timers/promises";

import { nowIso } from "./db";
import { workerLog, type WorkerContext } from "./worker/context";
import { processJob } from "./worker/dispatcher";
import { handleJobFailure } from "./worker/failure";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

/**
 * Main worker loop: recover abandoned running jobs, then repeatedly claim and
 * execute runnable jobs until shutdown is requested.
 */
export async function runWorker(ctx: WorkerContext): Promise<void> {
  const requeued = ctx.repo.requeueRunningJobs();
  if (requeued > 0) {
    workerLog(ctx, `[worker] requeued ${requeued} running jobs`);
  }

  for (;;) {
    if (ctx.shouldStop?.()) {
      return;
    }
    const job = ctx.repo.claimNextRunnableJob(nowIso());
    if (!job) {
      await sleep(300);
      continue;
    }

    try {
      const result = await processJob(ctx, job);
      if (result === "done") {
        workerLog(ctx, `[worker] job=${job.id} type=${job.type} done`);
      }
    } catch (error) {
      await handleJobFailure(ctx, job, error);
    }
  }
}
