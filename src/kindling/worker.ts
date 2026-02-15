import { setTimeout as sleep } from "node:timers/promises";

import { nowIso } from "./db";
import { importReleaseFromPath } from "./importer";
import { RtorrentClient } from "./rtorrent";
import { KindlingRepo } from "./repo";
import { scanLibraryRoot } from "./scanner";
import { runSearch, runSnatch } from "./service";
import type { AppSettings, JobRow, MediaType } from "./types";

/**
 * Background worker for Kindling jobs.
 *
 * The loop claims one runnable job at a time from SQLite, executes the job
 * handler, and records terminal state or retry scheduling in the same
 * database so work is durable across process restarts.
 */
export type WorkerContext = {
  repo: KindlingRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
  shouldStop?: () => boolean;
};

function log(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

function backoffMs(attempt: number): number {
  const safeAttempt = Math.max(0, Math.trunc(attempt));
  return Math.min(5 * 60_000, 1_000 * 2 ** safeAttempt);
}

/**
 * Download job: poll rTorrent by info hash until torrent completion, then
 * enqueue an import job for the resolved base path.
 */
async function processDownloadJob(ctx: WorkerContext, job: JobRow): Promise<"done" | "rescheduled"> {
  if (!job.release_id) {
    throw new Error("Download job missing release_id");
  }
  const release = ctx.repo.getRelease(job.release_id);
  if (!release) {
    throw new Error(`Release ${job.release_id} not found`);
  }

  const settings = ctx.getSettings();
  const client = new RtorrentClient(settings.rtorrent);

  const state = await client.getDownloadState(release.info_hash);
  if (!state.complete) {
    ctx.repo.setReleaseStatus(release.id, "downloading", null);
    const nextRun = new Date(Date.now() + Math.max(1000, settings.polling.rtorrentMs || 5000)).toISOString();
    ctx.repo.rescheduleJob(job.id, nextRun);
    return "rescheduled";
  }

  ctx.repo.setReleaseStatus(release.id, "downloaded", null);
  ctx.repo.createJob({
    type: "import",
    releaseId: release.id,
    bookId: release.book_id,
    payload: {
      basePath: state.basePath,
      infoHash: release.info_hash,
    },
  });
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

/**
 * Import job: wait for a resolved base path, then link imported files into the
 * library and create immutable asset rows for playback/download.
 */
async function processImportJob(ctx: WorkerContext, job: JobRow): Promise<"done" | "rescheduled"> {
  if (!job.release_id) {
    throw new Error("Import job missing release_id");
  }

  const release = ctx.repo.getRelease(job.release_id);
  if (!release) {
    throw new Error(`Release ${job.release_id} not found`);
  }

  const settings = ctx.getSettings();
  const client = new RtorrentClient(settings.rtorrent);
  const state = await client.getDownloadState(release.info_hash);
  const basePath = state.basePath;
  if (!basePath) {
    const nextRun = new Date(Date.now() + Math.max(1000, settings.polling.rtorrentMs || 5000)).toISOString();
    ctx.repo.rescheduleJob(job.id, nextRun);
    return "rescheduled";
  }

  await importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot);
  ctx.repo.setReleaseStatus(release.id, "imported", null);
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

/**
 * Reconcile job: find releases marked downloaded but missing assets, and make
 * sure each has exactly one queued/running import job.
 */
async function processReconcileJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const releases = ctx.repo.findReleasesDownloadedWithoutAssets();
  for (const release of releases) {
    const existingImportJobs = ctx.repo
      .listJobsByType("import")
      .some((candidate) => candidate.release_id === release.id && (candidate.status === "queued" || candidate.status === "running"));
    if (existingImportJobs) continue;
    ctx.repo.createJob({
      type: "import",
      releaseId: release.id,
      bookId: release.book_id,
      payload: { reason: "reconcile" },
    });
  }
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

/**
 * Scan job: either run a full filesystem scan or targeted auto-search/snatch
 * for a single book, depending on payload flags.
 */
async function processScanJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const payload = job.payload_json
    ? (JSON.parse(job.payload_json) as { bookId?: number; media?: MediaType[]; fullRefresh?: boolean })
    : {};
  const settings = ctx.getSettings();
  if (payload.fullRefresh) {
    await scanLibraryRoot(ctx.repo, settings.libraryRoot);
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }
  if (!payload.bookId) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }
  const book = ctx.repo.getBookRow(payload.bookId);
  if (!book) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }
  const mediaList: MediaType[] = payload.media && payload.media.length > 0 ? payload.media : ["audio", "ebook"];
  const snatchErrors: string[] = [];
  let snatchSucceeded = false;

  for (const media of mediaList) {
    const query = `${book.title} ${book.author}`.trim();
    const results = await runSearch(settings, { query, media });
    log(ctx, `[scan] job=${job.id} book=${book.id} media=${media} query=${JSON.stringify(query)} results=${results.length}`);
    const result = results[0];
    if (!result) {
      log(ctx, `[scan] job=${job.id} book=${book.id} media=${media} no_result`);
      continue;
    }
    try {
      const snatch = await runSnatch(ctx.repo, settings, {
        bookId: book.id,
        provider: result.provider,
        providerGuid: result.guid ?? null,
        title: result.title,
        mediaType: media,
        url: result.url,
        sizeBytes: result.sizeBytes,
        infoHash: result.infoHash ?? null,
      });
      log(
        ctx,
        `[scan] job=${job.id} book=${book.id} media=${media} snatch_release=${snatch.release.id} download_job=${snatch.jobId} idempotent=${snatch.idempotent}`
      );
      snatchSucceeded = true;
    } catch (error) {
      const message = (error as Error).message;
      log(ctx, `[scan] job=${job.id} book=${book.id} media=${media} snatch_error=${JSON.stringify(message)}`);
      snatchErrors.push(`${media}: ${message}`);
      continue;
    }
  }

  if (!snatchSucceeded && snatchErrors.length > 0) {
    throw new Error(`Auto-acquire failed for book ${book.id}; ${snatchErrors.join(" | ")}`);
  }

  ctx.repo.markJobSucceeded(job.id);
  return "done";
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
  if (job.type === "scan") {
    return processScanJob(ctx, job);
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
    log(ctx, `[worker] requeued ${requeued} running jobs`);
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
        log(ctx, `[worker] job=${job.id} type=${job.type} done`);
      }
    } catch (error) {
      const message = (error as Error).message;
      const next = new Date(Date.now() + backoffMs(job.attempt_count)).toISOString();
      const failed = ctx.repo.markJobFailed(job.id, message, next);
      log(ctx, `[worker] job=${job.id} type=${job.type} failed status=${failed.status} error=${message}`);
    }
  }
}
