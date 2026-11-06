import { nowIso } from "../db";
import { bookDownloadClient, legacyBookSnapshot } from "../media/book-torrents";
import { downloadComplete, downloadRemainingBytes, downloadPollDecision } from "../media/download-lifecycle";
import type { TorrentSnapshot } from "../media/torrent-client";
import type { AppSettings, JobRow, MediaType } from "../app-types";
import { durableStep, type WorkerContext } from "./context";

const FAST_POLL_MS = 500;
const EBOOK_MAX_POLL_MS = 1_000;

type DownloadJobPayload = {
  infoHash?: string;
  preferAgentImport?: boolean;
  manifestationId?: number | null;
  sequenceInManifestation?: number | null;
  repairReviewJobId?: number;
  telemetry?: {
    lastBytesDone?: number | null;
    stagnantSince?: string | null;
  };
};


type DownloadIssueDecision =
  | {
      action: "continue";
      nextPayload: DownloadJobPayload;
    }
  | {
      action: "recover";
      nextPayload: DownloadJobPayload;
      error: string;
    };

function logMessage(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

const derivedLeftBytes = downloadRemainingBytes;

function trimMessage(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function describeDownloadIssue(prefix: string, state: TorrentSnapshot, detail?: string | null): string {
  const leftBytes = derivedLeftBytes(state);
  const parts = [
    prefix,
    `state=${state.state}`,
    `leftBytes=${leftBytes ?? "null"}`,
    `downRate=${state.downloadBytesPerSecond ?? "null"}`
  ];
  const message = trimMessage(detail ?? state.message);
  if (message) {
    parts.push(`message=${JSON.stringify(message)}`);
  }
  return parts.join("; ");
}

function analyzeDownloadIssue(
  state: TorrentSnapshot,
  payload: DownloadJobPayload,
  settings: AppSettings
): DownloadIssueDecision {
  const nextPayload: DownloadJobPayload = { ...payload };
  const leftBytes = derivedLeftBytes(state);
  const lastBytesDone = payload.telemetry?.lastBytesDone ?? null;
  const currentBytesDone = state.bytesDone ?? null;
  const progressed =
    typeof currentBytesDone === "number" && typeof lastBytesDone === "number"
      ? currentBytesDone > lastBytesDone
      : false;
  const activeTransfer = (state.downloadBytesPerSecond ?? 0) > 0 || progressed || state.state === "checking";
  const incomplete = leftBytes === null || leftBytes > 0;
  let stagnantSince = payload.telemetry?.stagnantSince ?? null;

  if (!incomplete || activeTransfer) {
    stagnantSince = null;
  } else if (!stagnantSince) {
    stagnantSince = nowIso();
  }

  nextPayload.telemetry = {
    lastBytesDone: currentBytesDone,
    stagnantSince
  };

  const message = trimMessage(state.message);
  if (message && !activeTransfer && !downloadComplete(state)) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent engine reported an error; queuing forced reacquire", state, message)
    };
  }

  if (!stagnantSince || !incomplete) {
    return {
      action: "continue",
      nextPayload
    };
  }

  const stallMs = Math.max(0, settings.recovery.stalledTorrentMinutes) * 60_000;
  if (stallMs === 0) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent stalled with no progress; queuing forced reacquire", state)
    };
  }

  const stagnantForMs = Date.now() - Date.parse(stagnantSince);
  if (Number.isFinite(stagnantForMs) && stagnantForMs >= stallMs) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent stalled past recovery threshold; queuing forced reacquire", state)
    };
  }

  return {
    action: "continue",
    nextPayload
  };
}

const selectDownloadPollDecision = downloadPollDecision;

export function selectDownloadPollMs(state: TorrentSnapshot, configuredPollMs: number): number {
  return selectDownloadPollDecision(state, configuredPollMs).pollMs;
}

export function pollMsForMedia(mediaType: MediaType, configuredPollMs: number): number {
  const normalized = Math.max(FAST_POLL_MS, Math.trunc(configuredPollMs || 5000));
  if (mediaType === "ebook") {
    return Math.min(normalized, EBOOK_MAX_POLL_MS);
  }
  return normalized;
}

export async function processDownloadJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  if (!job.release_id) {
    throw new Error("Download job missing release_id");
  }
  const release = ctx.repo.getRelease(job.release_id);
  if (!release) {
    throw new Error(`Release ${job.release_id} not found`);
  }

  const settings = ctx.getSettings();
  let payload = job.payload_json ? (JSON.parse(job.payload_json) as DownloadJobPayload) : {};
  const client = bookDownloadClient(ctx.repo, settings, ctx.bookTorrentClient);

  for (let poll = 0; ; poll++) {
    const polled = await durableStep(ctx, `download:poll:${poll}`, async () => {
      ctx.execution?.assertActive();
      const state = await client.inspect(release.info_hash);
      ctx.execution?.assertActive();
      const issue = analyzeDownloadIssue(state, payload, settings);
      const decision = selectDownloadPollDecision(
        state,
        pollMsForMedia(release.media_type, settings.polling.rtorrentMs || 5000)
      );
      return { state, issue, decision };
    });
    // Pre-consolidation successful checkpoints remain replayable without another RPC.
    const state = "observedAt" in polled.state ? polled.state :
      legacyBookSnapshot(polled.state as unknown as Parameters<typeof legacyBookSnapshot>[0], release.info_hash);
    const { issue, decision } = polled;
    if (!downloadComplete(state)) {
      if (issue.action === "recover") {
        await durableStep(ctx, "download:recover", () =>
          ctx.repo.transaction(() => {
            ctx.repo.setReleaseStatus(release.id, "failed", issue.error);
            const acquireJob = ctx.repo.createJob({
              type: "acquire",
              bookId: release.book_id,
              idempotencyKey: `download:${job.id}:recover`,
              payload: {
                bookId: release.book_id,
                media: [release.media_type],
                forceAgent: true,
                priorFailure: true,
                repairReviewJobId: payload.repairReviewJobId,
                requireResult: true,
                notifyOnFailure: true,
                failureContext: `Auto-reacquire after stalled torrent for release ${release.id}`,
                rejectedUrls: [release.url],
                rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
                rejectedInfoHashes: release.info_hash ? [release.info_hash] : []
              }
            });
            const acquireJobId = acquireJob.id;
            ctx.repo.markJobCancelled(job.id, `${issue.error}; recoveryAcquireJob=${acquireJobId}`);
            logMessage(
              ctx,
              `[download] job=${job.id} release=${release.id} action=recover acquire_job=${acquireJobId}`
            );
          })
        );
        return "done";
      }
      ctx.repo.setReleaseStatus(release.id, "downloading", null);
      const etaText = decision.etaSeconds === null ? "null" : (Math.round(decision.etaSeconds * 100) / 100).toFixed(2);
      logMessage(
        ctx,
        `[download] job=${job.id} release=${release.id} media=${release.media_type} hash=${release.info_hash} complete=0 left_bytes=${decision.leftBytes ?? "null"} down_rate=${decision.downRate ?? "null"} eta_s=${etaText} poll_ms=${decision.pollMs} reason=${decision.reason}`
      );
      const pollMs = decision.pollMs;
      if (!ctx.execution) throw new Error("Download polling requires durable execution");
      if (state.state === "inactive") await durableStep(ctx, `download:resume:${poll}`, () => {
        ctx.execution?.assertActive();
        return client.resume(release.info_hash);
      });
      payload = issue.nextPayload;
      await ctx.execution.sleep(`download:wait:${poll}`, pollMs);
      continue;
    }

    const importSource = await durableStep(ctx, "download:source", () => client.getImportSource(release.info_hash));
    logMessage(
      ctx,
      `[download] job=${job.id} release=${release.id} hash=${release.info_hash} complete=1 bytes_done=${state.bytesDone ?? "null"} size_bytes=${state.sizeBytes ?? "null"} base_path=${JSON.stringify(importSource.basePath)} selected_paths=${importSource.selectedPaths.length}`
    );
    await durableStep(ctx, "download:handoff", () =>
      ctx.repo.transaction(() => {
        ctx.repo.setReleaseStatus(release.id, "downloaded", null);
        ctx.repo.createJob({
          type: "import",
          releaseId: release.id,
          bookId: release.book_id,
          idempotencyKey: `download:${job.id}:import`,
          payload: {
            basePath: importSource.basePath,
            selectedPaths: importSource.selectedPaths,
            infoHash: release.info_hash,
            preferAgentFirst: payload.preferAgentImport === true,
            manifestationId: payload.manifestationId ?? null,
            sequenceInManifestation: payload.sequenceInManifestation ?? null,
            repairReviewJobId: payload.repairReviewJobId
          }
        });
      })
    );
    return "done";
  }
}
