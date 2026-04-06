import os from "node:os";
import { setTimeout as sleep } from "node:timers/promises";

import { nowIso } from "./db";
import { workerLog, type WorkerContext } from "./worker/context";
import { processJob } from "./worker/dispatcher";
import { handleJobFailure } from "./worker/failure";
import { jobLockKeys, lockSetsConflict } from "./worker/locks";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

const DEFAULT_WORKER_CONCURRENCY = Math.max(8, Math.min(16, os.availableParallelism?.() ?? os.cpus().length));
const RUNNABLE_SCAN_LIMIT = 200;

type WorkerDeps = {
  processJob?: typeof processJob;
  handleJobFailure?: typeof handleJobFailure;
  concurrency?: number;
};

type ActiveJob = {
  jobType: Parameters<typeof processJob>[1]["type"];
  keys: string[];
  done: Promise<number>;
};

const BACKGROUND_JOB_TYPES = new Set<Parameters<typeof processJob>[1]["type"]>(["chapter_analysis"]);

function isBackgroundJobType(type: Parameters<typeof processJob>[1]["type"]): boolean {
  return BACKGROUND_JOB_TYPES.has(type);
}

function chooseCandidateJob(
  candidates: Parameters<typeof processJob>[1][],
  active: Map<number, ActiveJob>,
  workerConcurrency: number
): Parameters<typeof processJob>[1] | null {
  if (candidates.length === 0) return null;

  const foregroundCandidates = candidates.filter((job) => !isBackgroundJobType(job.type));
  const backgroundCandidates = candidates.filter((job) => isBackgroundJobType(job.type));
  const activeForeground = [...active.values()].filter((entry) => !isBackgroundJobType(entry.jobType)).length;
  const activeBackground = active.size - activeForeground;
  const reservedBackgroundSlots = workerConcurrency > 1 ? 1 : 0;
  const targetForegroundSlots = Math.max(1, workerConcurrency - reservedBackgroundSlots);
  const targetBackgroundSlots = Math.max(0, workerConcurrency - targetForegroundSlots);

  if (foregroundCandidates.length > 0 && activeForeground < targetForegroundSlots) {
    return foregroundCandidates[0] ?? null;
  }
  if (backgroundCandidates.length > 0 && activeBackground < targetBackgroundSlots) {
    return backgroundCandidates[0] ?? null;
  }
  if (foregroundCandidates.length > 0) {
    return foregroundCandidates[0] ?? null;
  }
  if (backgroundCandidates.length > 0) {
    return backgroundCandidates[0] ?? null;
  }
  return null;
}

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
    active.set(job.id, { jobType: job.type, keys, done });
  };

  for (;;) {
    if (ctx.shouldStop?.() && active.size === 0) {
      return;
    }

    let launchedAny = false;
    if (!ctx.shouldStop?.()) {
      while (active.size < workerConcurrency) {
        const candidates = ctx.repo.listRunnableJobs(nowIso(), RUNNABLE_SCAN_LIMIT);
        const runnable = candidates.filter((job) => !lockSetsConflict(activeLocks, jobLockKeys(ctx.repo, job)));
        const candidate = chooseCandidateJob(runnable, active, workerConcurrency);
        if (!candidate) break;
        const claimed = ctx.repo.claimQueuedJob(candidate.id, nowIso());
        if (!claimed) continue;
        launchJob(claimed, jobLockKeys(ctx.repo, claimed));
        launchedAny = true;
      }
    }

    if (active.size === 0) {
      await sleep(300);
      continue;
    }

    if (launchedAny && active.size < workerConcurrency) {
      continue;
    }

    await Promise.race([...active.values()].map((entry) => entry.done).concat(sleep(300).then(() => -1)));
  }
}
