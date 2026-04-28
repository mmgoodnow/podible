import { selectSearchCandidates } from "../library/agents";
import { runSearch, runSnatchGroup } from "../library/service";
import type { JobRow, MediaType } from "../app-types";
import { workerLog, type WorkerContext } from "./context";

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

function selectionNote(media: MediaType, decision: { mode: string; trigger: string; confidence: number; reason: string }): string {
  const prefix = decision.mode === "agent" ? "Agent selected" : "Deterministic search selected";
  return `${prefix} this ${media} manifestation (confidence ${decision.confidence.toFixed(2)}, trigger ${decision.trigger}): ${decision.reason}`;
}

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
  const mediaResults = await Promise.all(
    mediaList.map(async (media) => {
      const result: {
        snatchSucceeded: boolean;
        snatchError: string | null;
        noResultReason: string | null;
      } = {
        snatchSucceeded: false,
        snatchError: null,
        noResultReason: null,
      };

      const query = `${book.title} ${book.author}`.trim();
      const results = await runSearch(settings, { query, media });
      workerLog(
        ctx,
        `[acquire] job=${job.id} book=${book.id} media=${media} query=${JSON.stringify(query)} results=${results.length}`
      );
      const decision = await selectSearchCandidates(
        settings,
        {
          query,
          media,
          results,
          editionPreference: settings.agents.editionPreference,
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
      if (decision.selections.length === 0) {
        workerLog(
          ctx,
          `[acquire] job=${job.id} book=${book.id} media=${media} no_result mode=${decision.mode} trigger=${decision.trigger} reason=${JSON.stringify(decision.reason)}`
        );
        result.noResultReason = `${media}: ${decision.reason ?? "no candidate selected"}`;
        return result;
      }
      try {
        for (const [selectionIndex, selection] of decision.selections.entries()) {
          workerLog(
            ctx,
            `[acquire] job=${job.id} book=${book.id} media=${media} selection=${selectionIndex} mode=${decision.mode} confidence=${decision.confidence.toFixed(
              2
            )} trigger=${decision.trigger} parts=${selection.parts.length} reason=${JSON.stringify(decision.reason)}`
          );

          const group = await runSnatchGroup(
            ctx.repo,
            settings,
            {
              bookId: book.id,
              mediaType: media,
              manifestation: {
                ...selection.manifestation,
                selectionNote: selectionNote(media, decision),
              },
              parts: selection.parts.map((part) => ({
                provider: part.provider,
                providerGuid: part.guid ?? null,
                title: part.title,
                url: part.url,
                sizeBytes: part.sizeBytes,
                infoHash: part.infoHash ?? null,
              })),
            },
            {
              onLog: (line) => workerLog(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} ${line}`),
              preferAgentImport: decision.mode === "agent",
            }
          );
          for (const [partIndex, snatch] of group.results.entries()) {
            workerLog(
              ctx,
              `[acquire] job=${job.id} book=${book.id} media=${media} selection=${selectionIndex} part=${partIndex} manifestation=${group.manifestationId ?? "auto"} snatch_release=${snatch.release.id} download_job=${snatch.jobId} idempotent=${snatch.idempotent}`
            );
          }
        }
        result.snatchSucceeded = true;
      } catch (error) {
        const message = (error as Error).message;
        workerLog(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} snatch_error=${JSON.stringify(message)}`);
        result.snatchError = `${media}: ${message}`;
        return result;
      }
      return result;
    })
  );

  const snatchErrors = mediaResults.flatMap((result) => (result.snatchError ? [result.snatchError] : []));
  const noResultReasons = mediaResults.flatMap((result) => (result.noResultReason ? [result.noResultReason] : []));
  const snatchSucceeded = mediaResults.some((result) => result.snatchSucceeded);

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
