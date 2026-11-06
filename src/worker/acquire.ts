import { selectSearchCandidates } from "../library/agents";
import { runSnatchGroup } from "../library/service";
import { currentBookQualityBaseline, bookAutomaticUpgradeAllowed, validateBookRepair } from "../library/quality-state";
import { assessBookUpgrade } from "../library/quality-agent";
import { assessBookQuality } from "../library/quality";
import type { JobRow, MediaType } from "../app-types";
import { durableStep, workerLog, type WorkerContext } from "./context";
import { isRunControlError } from "reflow-ts";
import { selectWithIndexerRecovery } from "./indexer-search";
import { z } from "zod";
import type { TorznabResult } from "../library/torznab";

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
  /** Server-owned durable inbox references; never accepted from search/snatch RPC payloads. */
  feedCandidateIds?: number[];
  feedRoutingNote?: string;
  /** New RSS dispatch deliberately considers an already imported slot; old jobs retain missing-only semantics. */
  considerUpgrade?: boolean;
  repairReviewJobId?: number;
};

const feedCandidateSchema = z.array(z.number().int().positive()).min(1).max(1000);

function currentFeedCandidates(ctx: WorkerContext, ids: number[], media: MediaType) {
  const entries = ctx.repo.releaseInbox.entries(ids);
  if (entries.length !== new Set(ids).size) throw new Error("Book RSS inbox candidates are missing");
  if (entries.some((entry) => entry.result.mediaType !== media))
    throw new Error("Book RSS candidate has the wrong media type");
  return entries.map((entry) => ({ id: entry.id, result: entry.result as TorznabResult }));
}

function mediaReady(ctx: WorkerContext, bookId: number, media: MediaType, considerUpgrade = false): boolean {
  const current = ctx.repo.getBook(bookId);
  if (!current) return true;
  const ready = (media === "audio" ? current.audioStatus : current.ebookStatus) === "imported";
  return ready && (!considerUpgrade || !bookAutomaticUpgradeAllowed(ctx.repo, bookId, media) ||
    (!ctx.modelProvider && !ctx.getSettings().agents.apiKey.trim()));
}

function selectionNote(media: MediaType, decision: { mode: string; trigger: string; confidence: number; reason: string }): string {
  const prefix = decision.mode === "agent" ? "Agent selected" : "Deterministic search selected";
  return `${prefix} this ${media} manifestation (confidence ${decision.confidence.toFixed(2)}, trigger ${decision.trigger}): ${decision.reason}`;
}

export async function processAcquireJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const settings = ctx.getSettings();
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as AcquirePayload) : {};
  if (payload.feedCandidateIds !== undefined) {
    feedCandidateSchema.parse(payload.feedCandidateIds);
    if (payload.media?.length !== 1 || !["audio", "ebook"].includes(payload.media[0]))
      throw new Error("Book RSS acquisition requires exactly one media type");
  }
  if (!payload.bookId) {
    return "done";
  }
  const book = await durableStep(ctx, "book", () => ctx.repo.getBookRow(payload.bookId!));
  if (!book) {
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

      const feedEligible = payload.feedCandidateIds
        ? await durableStep(ctx, `${media}:feed-eligibility`, () => !mediaReady(ctx, book.id, media, payload.considerUpgrade))
        : true;
      if (!feedEligible) return result;
      const repair = payload.repairReviewJobId === undefined ? null :
        validateBookRepair(ctx.repo, book.id, media, payload.repairReviewJobId);
      const baseline = await durableStep(ctx, `${media}:quality-baseline`, () => repair ? null : currentBookQualityBaseline(ctx.repo, book.id, media));
      if (!repair && baseline && !bookAutomaticUpgradeAllowed(ctx.repo, book.id, media)) return result;

      const query = `${book.title} ${book.author}`.trim();
      const select = (results: TorznabResult[]) => selectSearchCandidates(
        settings,
        {
          query,
          media,
          results,
          editionPreference: settings.agents.editionPreference,
          forceAgent: payload.forceAgent === true || Boolean(ctx.modelProvider) || Boolean(settings.agents.apiKey?.trim()),
          priorFailure: payload.priorFailure === true,
          rejectedUrls: Array.isArray(payload.rejectedUrls) ? payload.rejectedUrls : [],
          rejectedGuids: Array.isArray(payload.rejectedGuids) ? payload.rejectedGuids : [],
          rejectedInfoHashes: Array.isArray(payload.rejectedInfoHashes) ? payload.rejectedInfoHashes : [],
          book: { id: book.id, title: book.title, author: book.author },
        },
        {
          repo: ctx.repo,
          modelProvider: ctx.modelProvider,
        }
      );
      const feedSelection = payload.feedCandidateIds
        ? await durableStep(ctx, `${media}:feed-selection`, async () => {
            const candidates = currentFeedCandidates(ctx, payload.feedCandidateIds!, media);
            const decision = await select(candidates.map((candidate) => candidate.result));
            const partIds = decision.selections.map((selection) => selection.parts.map((part) => {
              const candidate = candidates.find((candidate) => candidate.result === part);
              if (!candidate) throw new Error("Book selector returned a release outside its RSS evidence");
              return candidate.id;
            }));
            return { decision, partIds };
          })
        : null;
      const decision = feedSelection?.decision ?? await selectWithIndexerRecovery(
        ctx, { query, media, jobId: job.id, bookId: book.id },
        (results, stepName) => durableStep(ctx, stepName, () => select(results)),
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
          const groupEligible = payload.feedCandidateIds
            ? await durableStep(ctx, `${media}:feed-group:${selectionIndex}:eligibility`, () => !mediaReady(ctx, book.id, media, payload.considerUpgrade))
            : true;
          if (!groupEligible) return result;
          // Resolve server-owned IDs again after agent work/replay: feed polling may
          // have refreshed expiring URLs while this acquisition was pending.
          const parts = feedSelection
            ? feedSelection.partIds[selectionIndex].map((id) =>
                currentFeedCandidates(ctx, [id], media)[0].result)
            : selection.parts;
          workerLog(
            ctx,
            `[acquire] job=${job.id} book=${book.id} media=${media} selection=${selectionIndex} mode=${decision.mode} confidence=${decision.confidence.toFixed(
              2
            )} trigger=${decision.trigger} parts=${selection.parts.length} reason=${JSON.stringify(decision.reason)}`
          );

          let bookUpgrade;
          if (baseline) {
            const currentReleases = ctx.repo.listAssetsByManifestation(baseline.id)
              .flatMap(asset => asset.source_release_id === null ? [] : [ctx.repo.getRelease(asset.source_release_id)])
              .filter(release => release !== null);
            if (parts.some(part => currentReleases.some(release =>
              (part.infoHash && part.infoHash.toLowerCase() === release.info_hash.toLowerCase()) || part.url === release.url))) continue;
            const assessment = await durableStep(ctx, `${media}:selection:${selectionIndex}:quality`, () => assessBookUpgrade(settings, {
              title: book.title, author: book.author, description: book.description?.slice(0, 8000) ?? null,
              media, editionPreference: settings.agents.editionPreference, phase: "release", current: baseline.facts,
              candidate: { manifestation: selection.manifestation, parts: parts.map(part => ({
                title: part.title, provider: part.provider, sizeBytes: part.sizeBytes,
              })) },
            }, ctx.modelProvider));
            if (!assessment || !assessBookQuality(assessment).upgrade) {
              result.noResultReason = assessment?.reason ?? "No book quality reviewer configured";
              continue;
            }
            bookUpgrade = { baseline, preflight: assessment };
          }
          const group = await runSnatchGroup(
            ctx.repo,
            settings,
            {
              bookId: book.id,
              mediaType: media,
              manifestation: {
                ...selection.manifestation,
                selectionNote: [
                  payload.feedRoutingNote?.slice(0, 4000),
                  selectionNote(media, decision),
                ].filter(Boolean).join("\n"),
              },
              parts: parts.map((part) => ({
                provider: part.provider,
                providerGuid: part.guid ?? null,
                title: part.title,
                url: part.url,
                sizeBytes: part.sizeBytes,
                infoHash: part.infoHash ?? null,
              })),
            },
            {
              automatic: true,
              bookUpgrade,
              bookRepair: repair ? { reviewJobId: payload.repairReviewJobId! } : undefined,
              onLog: (line) => workerLog(ctx, `[acquire] job=${job.id} book=${book.id} media=${media} ${line}`),
              preferAgentImport: decision.mode === "agent",
              bookTorrentClient: ctx.bookTorrentClient,
              execution: ctx.execution,
              operationKey: `acquire:${ctx.execution?.key ?? job.id}:${media}:${selectionIndex}`,
              stepPrefix: `${media}:selection:${selectionIndex}`,
              selectedCopiesByPart: parts.map((part, index) => ({
                selected: part,
                alternatives: selection.matchingCopies?.[index] ?? [],
              })),
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
        if (isRunControlError(error)) throw error;
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

  return "done";
}
