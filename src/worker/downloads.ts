import { nowIso } from "../db";
import { RtorrentClient } from "../rtorrent";
import { triggerAutoAcquire } from "../library/service";
import type { RtorrentDownloadState } from "../rtorrent";
import type { AppSettings, JobRow, MediaType } from "../app-types";
import type { WorkerContext } from "./context";

const FAST_POLL_MS = 500;
const MID_POLL_MS = 2_000;
const EBOOK_MAX_POLL_MS = 1_000;
const ETA_FAST_WINDOW_SECONDS = 30;
const ETA_MID_WINDOW_SECONDS = 120;

type DownloadJobPayload = {
  infoHash?: string;
  preferAgentImport?: boolean;
  manifestationId?: number | null;
  sequenceInManifestation?: number | null;
  telemetry?: {
    lastBytesDone?: number | null;
    stagnantSince?: string | null;
  };
};

type PollDecision = {
  pollMs: number;
  reason:
    | "fallback_no_left_bytes"
    | "fallback_no_down_rate"
    | "fallback_nonpositive_rate"
    | "fallback_invalid_eta"
    | "eta_fast"
    | "eta_mid"
    | "eta_slow";
  leftBytes: number | null;
  downRate: number | null;
  etaSeconds: number | null;
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

function derivedLeftBytes(state: RtorrentDownloadState): number | null {
  if (typeof state.leftBytes === "number" && Number.isFinite(state.leftBytes)) {
    return Math.max(0, state.leftBytes);
  }
  if (typeof state.sizeBytes === "number" && typeof state.bytesDone === "number") {
    return Math.max(0, state.sizeBytes - state.bytesDone);
  }
  return null;
}

function trimMessage(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function describeDownloadIssue(prefix: string, state: RtorrentDownloadState, detail?: string | null): string {
  const leftBytes = derivedLeftBytes(state);
  const parts = [
    prefix,
    `active=${state.isActive ? 1 : 0}`,
    `leftBytes=${leftBytes ?? "null"}`,
    `downRate=${state.downRate ?? "null"}`,
  ];
  const message = trimMessage(detail ?? state.message);
  if (message) {
    parts.push(`message=${JSON.stringify(message)}`);
  }
  return parts.join("; ");
}

function analyzeDownloadIssue(
  state: RtorrentDownloadState,
  payload: DownloadJobPayload,
  settings: AppSettings
): DownloadIssueDecision {
  const nextPayload: DownloadJobPayload = { ...payload };
  const leftBytes = derivedLeftBytes(state);
  const lastBytesDone = payload.telemetry?.lastBytesDone ?? null;
  const currentBytesDone = state.bytesDone ?? null;
  const progressed =
    typeof currentBytesDone === "number" && typeof lastBytesDone === "number" ? currentBytesDone > lastBytesDone : false;
  const activeTransfer = (typeof state.downRate === "number" && state.downRate > 0) || progressed;
  const incomplete = leftBytes === null || leftBytes > 0;
  let stagnantSince = payload.telemetry?.stagnantSince ?? null;

  if (!incomplete || activeTransfer) {
    stagnantSince = null;
  } else if (!stagnantSince) {
    stagnantSince = nowIso();
  }

  nextPayload.telemetry = {
    lastBytesDone: currentBytesDone,
    stagnantSince,
  };

  const message = trimMessage(state.message);
  if (message) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent errored in rTorrent; queuing forced reacquire", state, message),
    };
  }

  if (!stagnantSince || !incomplete) {
    return {
      action: "continue",
      nextPayload,
    };
  }

  const stallMs = Math.max(0, settings.recovery.stalledTorrentMinutes) * 60_000;
  if (stallMs === 0) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent stalled with no progress; queuing forced reacquire", state),
    };
  }

  const stagnantForMs = Date.now() - Date.parse(stagnantSince);
  if (Number.isFinite(stagnantForMs) && stagnantForMs >= stallMs) {
    return {
      action: "recover",
      nextPayload,
      error: describeDownloadIssue("Torrent stalled past recovery threshold; queuing forced reacquire", state),
    };
  }

  return {
    action: "continue",
    nextPayload,
  };
}

function selectDownloadPollDecision(state: RtorrentDownloadState, configuredPollMs: number): PollDecision {
  const maxPollMs = Math.max(FAST_POLL_MS, Math.trunc(configuredPollMs || 5000));
  const leftBytes = derivedLeftBytes(state);
  const downRate = typeof state.downRate === "number" && Number.isFinite(state.downRate) ? state.downRate : null;
  if (leftBytes === null) {
    return {
      pollMs: maxPollMs,
      reason: "fallback_no_left_bytes",
      leftBytes,
      downRate,
      etaSeconds: null,
    };
  }
  if (downRate === null) {
    return {
      pollMs: maxPollMs,
      reason: "fallback_no_down_rate",
      leftBytes,
      downRate,
      etaSeconds: null,
    };
  }
  if (downRate <= 0) {
    return {
      pollMs: maxPollMs,
      reason: "fallback_nonpositive_rate",
      leftBytes,
      downRate,
      etaSeconds: null,
    };
  }

  const etaSeconds = leftBytes / downRate;
  if (!Number.isFinite(etaSeconds) || etaSeconds < 0) {
    return {
      pollMs: maxPollMs,
      reason: "fallback_invalid_eta",
      leftBytes,
      downRate,
      etaSeconds: null,
    };
  }
  if (etaSeconds <= ETA_FAST_WINDOW_SECONDS) {
    return {
      pollMs: FAST_POLL_MS,
      reason: "eta_fast",
      leftBytes,
      downRate,
      etaSeconds,
    };
  }
  if (etaSeconds <= ETA_MID_WINDOW_SECONDS) {
    return {
      pollMs: Math.min(maxPollMs, MID_POLL_MS),
      reason: "eta_mid",
      leftBytes,
      downRate,
      etaSeconds,
    };
  }
  return {
    pollMs: maxPollMs,
    reason: "eta_slow",
    leftBytes,
    downRate,
    etaSeconds,
  };
}

export function selectDownloadPollMs(state: RtorrentDownloadState, configuredPollMs: number): number {
  return selectDownloadPollDecision(state, configuredPollMs).pollMs;
}

export function pollMsForMedia(mediaType: MediaType, configuredPollMs: number): number {
  const normalized = Math.max(FAST_POLL_MS, Math.trunc(configuredPollMs || 5000));
  if (mediaType === "ebook") {
    return Math.min(normalized, EBOOK_MAX_POLL_MS);
  }
  return normalized;
}

export async function processDownloadJob(ctx: WorkerContext, job: JobRow): Promise<"done" | "rescheduled"> {
  if (!job.release_id) {
    throw new Error("Download job missing release_id");
  }
  const release = ctx.repo.getRelease(job.release_id);
  if (!release) {
    throw new Error(`Release ${job.release_id} not found`);
  }

  const settings = ctx.getSettings();
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as DownloadJobPayload) : {};
  const client = new RtorrentClient(settings.rtorrent);

  const state = await client.getDownloadState(release.info_hash);
  if (!state.complete) {
    const issue = analyzeDownloadIssue(state, payload, settings);
    if (issue.action === "recover") {
      ctx.repo.setReleaseStatus(release.id, "failed", issue.error);
      const acquireJobId = await triggerAutoAcquire(ctx.repo, release.book_id, [release.media_type], {
        forceAgent: true,
        priorFailure: true,
        requireResult: true,
        notifyOnFailure: true,
        failureContext: `Auto-reacquire after stalled torrent for release ${release.id}`,
        rejectedUrls: [release.url],
        rejectedGuids: release.provider_guid ? [release.provider_guid] : [],
        rejectedInfoHashes: release.info_hash ? [release.info_hash] : [],
      });
      ctx.repo.markJobCancelled(job.id, `${issue.error}; recoveryAcquireJob=${acquireJobId}`);
      logMessage(ctx, `[download] job=${job.id} release=${release.id} action=recover acquire_job=${acquireJobId}`);
      return "done";
    }
    ctx.repo.setReleaseStatus(release.id, "downloading", null);
    const mediaPollMs = pollMsForMedia(release.media_type, settings.polling.rtorrentMs || 5000);
    const decision = selectDownloadPollDecision(state, mediaPollMs);
    const etaText =
      decision.etaSeconds === null ? "null" : (Math.round(decision.etaSeconds * 100) / 100).toFixed(2);
    logMessage(
      ctx,
      `[download] job=${job.id} release=${release.id} media=${release.media_type} hash=${release.info_hash} complete=0 left_bytes=${decision.leftBytes ?? "null"} down_rate=${decision.downRate ?? "null"} eta_s=${etaText} poll_ms=${decision.pollMs} reason=${decision.reason}`
    );
    const pollMs = decision.pollMs;
    const nextRun = new Date(Date.now() + pollMs).toISOString();
    ctx.repo.rescheduleJob(job.id, nextRun, issue.nextPayload);
    return "rescheduled";
  }

  const importSource = await client.getImportSource(release.info_hash);
  logMessage(
    ctx,
    `[download] job=${job.id} release=${release.id} hash=${release.info_hash} complete=1 bytes_done=${state.bytesDone ?? "null"} size_bytes=${state.sizeBytes ?? "null"} base_path=${JSON.stringify(importSource.basePath)} selected_paths=${importSource.selectedPaths.length}`
  );
  ctx.repo.setReleaseStatus(release.id, "downloaded", null);
  ctx.repo.createJob({
    type: "import",
    releaseId: release.id,
    bookId: release.book_id,
    payload: {
      basePath: importSource.basePath,
      selectedPaths: importSource.selectedPaths,
      infoHash: release.info_hash,
      preferAgentFirst: payload.preferAgentImport === true,
      manifestationId: payload.manifestationId ?? null,
      sequenceInManifestation: payload.sequenceInManifestation ?? null,
    },
  });
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}
