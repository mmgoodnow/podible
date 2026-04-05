import { setTimeout as sleep } from "node:timers/promises";

import { selectManualImportPaths, selectSearchCandidate } from "./agents";
import { processChapterAnalysisJob } from "./chapter-analysis";
import { nowIso } from "./db";
import { importReleaseFromPath, inspectImportPath } from "./importer";
import { sendPushoverNotification } from "./notify";
import { RtorrentClient } from "./rtorrent";
import { BooksRepo } from "./repo";
import { scanLibraryRoot } from "./scanner";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import { processDownloadJob } from "./worker-downloads";
import type { AppSettings, JobRow, MediaType } from "./types";

export { pollMsForMedia, selectDownloadPollMs } from "./worker-downloads";

/**
 * Background worker for Kindling jobs.
 *
 * The loop claims one runnable job at a time from SQLite, executes the job
 * handler, and records terminal state or retry scheduling in the same
 * database so work is durable across process restarts.
 */
export type WorkerContext = {
  repo: BooksRepo;
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

type ImportJobPayload = {
  reason?: string;
  userReportedIssue?: boolean;
  rejectedSourcePaths?: string[];
  preferAgentFirst?: boolean;
};

async function notifyBestEffort(ctx: WorkerContext, title: string, message: string): Promise<boolean> {
  try {
    const result = await sendPushoverNotification(ctx.getSettings(), { title, message });
    if (!result.delivered) {
      log(ctx, `[notify] skipped reason=${result.reason ?? "disabled"} title=${JSON.stringify(title)}`);
      return false;
    }
    log(ctx, `[notify] delivered title=${JSON.stringify(title)}`);
    return true;
  } catch (error) {
    log(ctx, `[notify] failed title=${JSON.stringify(title)} error=${JSON.stringify((error as Error).message)}`);
    return false;
  }
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
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as ImportJobPayload) : {};
  const client = new RtorrentClient(settings.rtorrent);
  const state = await client.getDownloadState(release.info_hash);
  const basePath = state.basePath;
  if (!basePath) {
    const nextRun = new Date(Date.now() + Math.max(1000, settings.polling.rtorrentMs || 5000)).toISOString();
    ctx.repo.rescheduleJob(job.id, nextRun);
    return "rescheduled";
  }
  log(
    ctx,
    `[import] job=${job.id} release=${release.id} start base_path=${JSON.stringify(basePath)} prefer_agent_first=${payload.preferAgentFirst === true} user_reported_issue=${payload.userReportedIssue === true}`
  );

  if (payload.userReportedIssue === true) {
    let agentDecisionReason = "not_attempted";
    let agentDecisionError: string | null = null;
    try {
      const files = await inspectImportPath(basePath);
      const book = ctx.repo.getBookRow(release.book_id);
      const decision = await selectManualImportPaths(settings, {
        mediaType: release.media_type,
        files,
        rejectedSourcePaths: Array.isArray(payload.rejectedSourcePaths) ? payload.rejectedSourcePaths : [],
        forceAgent: true,
        priorFailure: true,
        book: book ? { id: book.id, title: book.title, author: book.author } : null,
      });
      agentDecisionReason = `${decision.mode}:${decision.reason}`;
      agentDecisionError = decision.error;

      if (decision.selectedPaths.length > 0) {
        await importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
          selectedPaths: decision.selectedPaths,
        });
        ctx.repo.setReleaseStatus(release.id, "imported", null);
        ctx.repo.markJobSucceeded(job.id);
        log(
          ctx,
          `[import] job=${job.id} release=${release.id} wrong_file_agent_reimport=success selected=${decision.selectedPaths.length}`
        );
        return "done";
      }
    } catch (agentImportError) {
      agentDecisionError = (agentImportError as Error).message || "agent import failed";
      agentDecisionReason = "agent_import_exception";
      log(
        ctx,
        `[import] job=${job.id} release=${release.id} wrong_file_agent_error=${JSON.stringify(agentDecisionError)}`
      );
    }

    const terminalError = `Wrong-file review failed to produce alternate import. decision=${agentDecisionReason}; agentError=${agentDecisionError ?? "none"}`;
    ctx.repo.setReleaseStatus(release.id, "failed", terminalError);
    ctx.repo.createJob({
      type: "acquire",
      bookId: release.book_id,
      payload: {
        bookId: release.book_id,
        media: [release.media_type],
        priorFailure: true,
        forceAgent: true,
        rejectedUrls: [release.url],
        rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
        rejectedInfoHashes: release.info_hash ? [release.info_hash] : [],
      },
    });
    ctx.repo.markJobCancelled(job.id, `${terminalError}; recoveryQueued=true`);
    log(ctx, `[import] job=${job.id} release=${release.id} wrong_file_agent_reimport=none queued_acquire_forced_agent=1`);
    return "done";
  }

  if (payload.preferAgentFirst === true) {
    let agentFirstError: string | null = null;
    try {
      const files = await inspectImportPath(basePath);
      const book = ctx.repo.getBookRow(release.book_id);
      const decision = await selectManualImportPaths(settings, {
        mediaType: release.media_type,
        files,
        forceAgent: true,
        priorFailure: false,
        book: book ? { id: book.id, title: book.title, author: book.author } : null,
      });
      if (decision.selectedPaths.length > 0) {
        await importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
          selectedPaths: decision.selectedPaths,
        });
        ctx.repo.setReleaseStatus(release.id, "imported", null);
        ctx.repo.markJobSucceeded(job.id);
        log(
          ctx,
          `[import] job=${job.id} release=${release.id} agent_first=success mode=${decision.mode} selected=${decision.selectedPaths.length}`
        );
        return "done";
      }
      log(
        ctx,
        `[import] job=${job.id} release=${release.id} agent_first=no_selection mode=${decision.mode} reason=${JSON.stringify(decision.reason)}`
      );
    } catch (error) {
      agentFirstError = (error as Error).message || "agent-first import failed";
      log(ctx, `[import] job=${job.id} release=${release.id} agent_first_error=${JSON.stringify(agentFirstError)}`);
    }
  }

  try {
    await importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot);
    ctx.repo.setReleaseStatus(release.id, "imported", null);
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  } catch (firstError) {
    const deterministicMessage = (firstError as Error).message || "deterministic import failed";
    log(
      ctx,
      `[import] job=${job.id} release=${release.id} deterministic_error=${JSON.stringify(deterministicMessage)}`
    );

    let agentDecisionReason = "not_attempted";
    let agentDecisionError: string | null = null;
    try {
      const files = await inspectImportPath(basePath);
      const book = ctx.repo.getBookRow(release.book_id);
      const decision = await selectManualImportPaths(settings, {
        mediaType: release.media_type,
        files,
        forceAgent: true,
        priorFailure: true,
        book: book ? { id: book.id, title: book.title, author: book.author } : null,
      });
      agentDecisionReason = `${decision.mode}:${decision.reason}`;
      agentDecisionError = decision.error;

      if (decision.selectedPaths.length > 0) {
        await importReleaseFromPath(ctx.repo, release, basePath, settings.libraryRoot, {
          selectedPaths: decision.selectedPaths,
        });
        ctx.repo.setReleaseStatus(release.id, "imported", null);
        ctx.repo.markJobSucceeded(job.id);
        log(
          ctx,
          `[import] job=${job.id} release=${release.id} agent_recovery=success selected=${decision.selectedPaths.length}`
        );
        return "done";
      }
    } catch (agentImportError) {
      agentDecisionError = (agentImportError as Error).message || "agent import failed";
      agentDecisionReason = "agent_import_exception";
      log(
        ctx,
        `[import] job=${job.id} release=${release.id} agent_error=${JSON.stringify(agentDecisionError)}`
      );
    }

    const terminalError = `Import failed after deterministic+agent attempts. deterministic=${deterministicMessage}; decision=${agentDecisionReason}; agentError=${agentDecisionError ?? "none"}`;
    ctx.repo.setReleaseStatus(release.id, "failed", terminalError);
    ctx.repo.createJob({
      type: "acquire",
      bookId: release.book_id,
      payload: {
        bookId: release.book_id,
        media: [release.media_type],
        priorFailure: true,
        forceAgent: true,
        rejectedUrls: [release.url],
        rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
        rejectedInfoHashes: release.info_hash ? [release.info_hash] : [],
      },
    });
    ctx.repo.markJobCancelled(job.id, `${terminalError}; recoveryQueued=true`);
    log(
      ctx,
      `[import] job=${job.id} release=${release.id} agent_recovery=none queued_acquire_forced_agent=1`
    );
    return "done";
  }
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

type AcquirePayload = {
  bookId?: number;
  media?: MediaType[];
  forceAgent?: boolean;
  priorFailure?: boolean;
  requireResult?: boolean;
  notifyOnFailure?: boolean;
  failureContext?: string | null;
  rejectedUrls?: string[];
  rejectedGuids?: string[];
  rejectedInfoHashes?: string[];
};

/**
 * Full-library-refresh job: run a full filesystem scan and import local
 * library content.
 */
async function processFullLibraryRefreshJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const settings = ctx.getSettings();
  await scanLibraryRoot(ctx.repo, settings.libraryRoot);
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

/**
 * Acquire job: targeted auto-search/snatch for a specific book and media set.
 */
async function processAcquireJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const settings = ctx.getSettings();
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as AcquirePayload) : {};
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
  const noResultReasons: string[] = [];
  let snatchSucceeded = false;

  for (const media of mediaList) {
    const query = `${book.title} ${book.author}`.trim();
    const results = await runSearch(settings, { query, media });
    log(
      ctx,
      `[acquire] job=${job.id} book=${book.id} media=${media} query=${JSON.stringify(query)} results=${results.length}`
    );
    const decision = await selectSearchCandidate(settings, {
      query,
      media,
      results,
      forceAgent: payload.forceAgent === true,
      priorFailure: payload.priorFailure === true,
      rejectedUrls: Array.isArray(payload.rejectedUrls) ? payload.rejectedUrls : [],
      rejectedGuids: Array.isArray(payload.rejectedGuids) ? payload.rejectedGuids : [],
      rejectedInfoHashes: Array.isArray(payload.rejectedInfoHashes) ? payload.rejectedInfoHashes : [],
      book: { id: book.id, title: book.title, author: book.author },
    }, {
      repo: ctx.repo,
    });
    if (decision.error) {
      log(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} decision_error=${JSON.stringify(decision.error)} trigger=${decision.trigger}`
      );
    }
    if (!decision.candidate) {
      log(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} no_result mode=${decision.mode} trigger=${decision.trigger} reason=${JSON.stringify(decision.reason)}`
      );
      noResultReasons.push(`${media}: ${decision.reason ?? "no candidate selected"}`);
      continue;
    }
    log(
      ctx,
      `[acquire] job=${job.id} book=${book.id} media=${media} candidate_mode=${decision.mode} confidence=${decision.confidence.toFixed(
        2
      )} trigger=${decision.trigger} reason=${JSON.stringify(decision.reason)} candidate_title=${JSON.stringify(
        decision.candidate.title
      )} candidate_guid=${JSON.stringify(decision.candidate.guid ?? null)}`
    );
    try {
      const snatch = await runSnatch(ctx.repo, settings, {
        bookId: book.id,
        provider: decision.candidate.provider,
        providerGuid: decision.candidate.guid ?? null,
        title: decision.candidate.title,
        mediaType: media,
        url: decision.candidate.url,
        sizeBytes: decision.candidate.sizeBytes,
        infoHash: decision.candidate.infoHash ?? null,
      }, {
        onLog: (line) => log(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} ${line}`),
        preferAgentImport: decision.mode === "agent",
      });
      log(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} snatch_release=${snatch.release.id} download_job=${snatch.jobId} idempotent=${snatch.idempotent}`
      );
      snatchSucceeded = true;
    } catch (error) {
      const message = (error as Error).message;
      log(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} snatch_error=${JSON.stringify(message)}`);
      snatchErrors.push(`${media}: ${message}`);
      continue;
    }
  }

  if (snatchErrors.length > 0) {
    const prefix = snatchSucceeded ? "Auto-acquire partially failed" : "Auto-acquire failed";
    throw new Error(`${prefix} for book ${book.id}; ${snatchErrors.join(" | ")}`);
  }

  if (!snatchSucceeded && payload.requireResult === true) {
    const detail = noResultReasons.length > 0 ? noResultReasons.join(" | ") : "no candidate selected";
    throw new Error(`Auto-acquire found no usable release for book ${book.id}; ${detail}`);
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
      const failed = ctx.repo.markJobFailed(job.id, message, next) as { status: string } | null;
      if (!failed) {
        log(ctx, `[worker] job=${job.id} type=${job.type} failed but row missing (likely deleted) error=${message}`);
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
      log(ctx, `[worker] job=${job.id} type=${job.type} failed status=${failed.status} error=${message}`);
    }
  }
}
