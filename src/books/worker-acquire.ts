import { selectSearchCandidate } from "./agents";
import { runSearch, runSnatch } from "./service";
import type { JobRow, MediaType } from "./types";
import { workerLog, type WorkerContext } from "./worker-context";

export type AcquirePayload = {
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

export async function processAcquireJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
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
    workerLog(
      ctx,
      `[acquire] job=${job.id} book=${book.id} media=${media} query=${JSON.stringify(query)} results=${results.length}`
    );
    const decision = await selectSearchCandidate(
      settings,
      {
        query,
        media,
        results,
        forceAgent: payload.forceAgent === true,
        priorFailure: payload.priorFailure === true,
        rejectedUrls: Array.isArray(payload.rejectedUrls) ? payload.rejectedUrls : [],
        rejectedGuids: Array.isArray(payload.rejectedGuids) ? payload.rejectedGuids : [],
        rejectedInfoHashes: Array.isArray(payload.rejectedInfoHashes) ? payload.rejectedInfoHashes : [],
        book: { id: book.id, title: book.title, author: book.author },
      },
      {
        repo: ctx.repo,
      }
    );
    if (decision.error) {
      workerLog(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} decision_error=${JSON.stringify(decision.error)} trigger=${decision.trigger}`
      );
    }
    if (!decision.candidate) {
      workerLog(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} no_result mode=${decision.mode} trigger=${decision.trigger} reason=${JSON.stringify(decision.reason)}`
      );
      noResultReasons.push(`${media}: ${decision.reason ?? "no candidate selected"}`);
      continue;
    }
    workerLog(
      ctx,
      `[acquire] job=${job.id} book=${book.id} media=${media} candidate_mode=${decision.mode} confidence=${decision.confidence.toFixed(
        2
      )} trigger=${decision.trigger} reason=${JSON.stringify(decision.reason)} candidate_title=${JSON.stringify(
        decision.candidate.title
      )} candidate_guid=${JSON.stringify(decision.candidate.guid ?? null)}`
    );
    try {
      const snatch = await runSnatch(
        ctx.repo,
        settings,
        {
          bookId: book.id,
          provider: decision.candidate.provider,
          providerGuid: decision.candidate.guid ?? null,
          title: decision.candidate.title,
          mediaType: media,
          url: decision.candidate.url,
          sizeBytes: decision.candidate.sizeBytes,
          infoHash: decision.candidate.infoHash ?? null,
        },
        {
          onLog: (line) => workerLog(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} ${line}`),
          preferAgentImport: decision.mode === "agent",
        }
      );
      workerLog(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} snatch_release=${snatch.release.id} download_job=${snatch.jobId} idempotent=${snatch.idempotent}`
      );
      snatchSucceeded = true;
    } catch (error) {
      const message = (error as Error).message;
      workerLog(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} snatch_error=${JSON.stringify(message)}`);
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
