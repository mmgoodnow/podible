import { setTimeout as sleep } from "node:timers/promises";

import { processChapterAnalysisJob } from "./chapter-analysis";
import { nowIso } from "./db";
import { sendPushoverNotification } from "./notify";
import { processAcquireJob, type AcquirePayload } from "./worker/acquire";
import { workerLog, type WorkerContext } from "./worker/context";
import { processDownloadJob } from "./worker/downloads";
import { processImportJob } from "./worker/imports";
import { processFullLibraryRefreshJob, processReconcileJob } from "./worker/maintenance";
import type { JobRow } from "./app-types";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

function backoffMs(attempt: number): number {
  const safeAttempt = Math.max(0, Math.trunc(attempt));
  return Math.min(5 * 60_000, 1_000 * 2 ** safeAttempt);
}

async function notifyBestEffort(ctx: WorkerContext, title: string, message: string): Promise<boolean> {
  try {
    const result = await sendPushoverNotification(ctx.getSettings(), { title, message });
    if (!result.delivered) {
      workerLog(ctx, `[notify] skipped reason=${result.reason ?? "disabled"} title=${JSON.stringify(title)}`);
      return false;
    }
    workerLog(ctx, `[notify] delivered title=${JSON.stringify(title)}`);
    return true;
  } catch (error) {
    workerLog(ctx, `[notify] failed title=${JSON.stringify(title)} error=${JSON.stringify((error as Error).message)}`);
    return false;
  }
}

/**
 * Job type dispatch. Unknown types are treated as no-op and marked succeeded
 * so a stale queue entry cannot block worker progress.
 */
async function processJob(ctx: WorkerContext, job: JobRow): Promise<"done" | "rescheduled"> {
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
  return "done";
}

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
      const message = (error as Error).message;
      const next = new Date(Date.now() + backoffMs(job.attempt_count)).toISOString();
      const failed = ctx.repo.markJobFailed(job.id, message, next) as { status: string } | null;
      if (!failed) {
        workerLog(ctx, `[worker] job=${job.id} type=${job.type} failed but row missing (likely deleted) error=${message}`);
        continue;
      }
      if (failed.status === "failed" && job.type === "acquire") {
        const payload = job.payload_json ? (JSON.parse(job.payload_json) as AcquirePayload) : {};
        if (payload.notifyOnFailure === true) {
          const media = payload.media && payload.media.length > 0 ? payload.media.join(",") : "audio,ebook";
          const book = job.book_id ? ctx.repo.getBookRow(job.book_id) : null;
          const title = "Podible recovery failed";
          const body = [
            `Book: ${book ? `${book.title} by ${book.author}` : `book ${job.book_id ?? "unknown"}`}`,
            `Media: ${media}`,
            `Context: ${payload.failureContext ?? "auto-recovery"}`,
            `Reason: ${message}`,
          ].join("\n");
          await notifyBestEffort(ctx, title, body);
        }
      }
      workerLog(ctx, `[worker] job=${job.id} type=${job.type} failed status=${failed.status} error=${message}`);
    }
  }
}
