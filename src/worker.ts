import { setTimeout as sleep } from "node:timers/promises";

import { nowIso } from "./db";
import { workerLog, type WorkerContext } from "./worker/context";
import { processJob } from "./worker/dispatcher";
import { handleJobFailure } from "./worker/failure";
import { jobLockKeys, lockSetsConflict } from "./worker/locks";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

const DEFAULT_WORKER_CONCURRENCY = 3;
const RUNNABLE_SCAN_LIMIT = 25;

type WorkerDeps = {
  processJob?: typeof processJob;
  handleJobFailure?: typeof handleJobFailure;
  concurrency?: number;
};

type ActiveJob = {
  keys: string[];
  done: Promise<number>;
};

/**
 * Main worker loop: recover abandoned running jobs, then repeatedly claim and
 * execute runnable jobs until shutdown is requested.
 */
export async function runWorker(ctx: WorkerContext, deps: WorkerDeps = {}): Promise<void> {
  const requeued = ctx.repo.requeueRunningJobs();
  if (requeued > 0) {
    workerLog(ctx, `[worker] requeued ${requeued} running jobs`);
  }

  const active = new Map<number, ActiveJob>();
  const activeLocks = new Set<string>();
  const workerConcurrency = Math.max(1, Math.trunc(deps.concurrency ?? DEFAULT_WORKER_CONCURRENCY));
  const jobProcessor = deps.processJob ?? processJob;
  const failureHandler = deps.handleJobFailure ?? handleJobFailure;

  const launchJob = (job: Parameters<typeof processJob>[1], keys: string[]) => {
    for (const key of keys) {
      activeLocks.add(key);
    }
    const done = (async () => {
      try {
        const result = await jobProcessor(ctx, job);
        if (result === "done") {
          workerLog(ctx, `[worker] job=${job.id} type=${job.type} done`);
        }
      } catch (error) {
        await failureHandler(ctx, job, error);
      } finally {
        for (const key of keys) {
          activeLocks.delete(key);
        }
        active.delete(job.id);
      }
      return job.id;
    })();
    active.set(job.id, { keys, done });
  };

  for (;;) {
    if (ctx.shouldStop?.() && active.size === 0) {
      return;
    }

    if (!ctx.shouldStop?.()) {
      while (active.size < workerConcurrency) {
        const candidates = ctx.repo.listRunnableJobs(nowIso(), RUNNABLE_SCAN_LIMIT);
        const candidate = candidates.find((job) => !lockSetsConflict(activeLocks, jobLockKeys(ctx.repo, job)));
        if (!candidate) break;
        const claimed = ctx.repo.claimQueuedJob(candidate.id, nowIso());
        if (!claimed) continue;
        launchJob(claimed, jobLockKeys(ctx.repo, claimed));
      }
    }

    if (active.size === 0) {
      await sleep(300);
      continue;
    }

    await Promise.race([...active.values()].map((entry) => entry.done));
  }
}
