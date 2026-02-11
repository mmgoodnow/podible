import { setTimeout as sleep } from "node:timers/promises";

import { nowIso } from "./db";
import { importReleaseFromPath } from "./importer";
import { RtorrentClient } from "./rtorrent";
import { KindlingRepo } from "./repo";
import { scanLibraryRoot } from "./scanner";
import { runSnatch } from "./service";
import { searchTorznab } from "./torznab";
import type { AppSettings, JobRow, MediaType } from "./types";

export type WorkerContext = {
  repo: KindlingRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
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

function rankSearchResults<T extends { sizeBytes: number | null }>(results: T[]): T[] {
  return [...results].sort((a, b) => {
    const aScore = a.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    const bScore = b.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    return aScore - bScore;
  });
}

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

  for (const media of mediaList) {
    const query = `${book.title} ${book.author}`.trim();
    const results = await searchTorznab(settings.torznab, query, media);
    const ranked = rankSearchResults(results).slice(0, 3);
    for (const result of ranked) {
      try {
        await runSnatch(ctx.repo, settings, {
          bookId: book.id,
          provider: result.provider,
          title: result.title,
          mediaType: media,
          url: result.url,
          sizeBytes: result.sizeBytes,
          infoHash: result.infoHash,
        });
      } catch {
        continue;
      }
      break;
    }
  }

  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

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

export async function runWorker(ctx: WorkerContext): Promise<void> {
  const requeued = ctx.repo.requeueRunningJobs();
  if (requeued > 0) {
    log(ctx, `[worker] requeued ${requeued} running jobs`);
  }

  for (;;) {
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
