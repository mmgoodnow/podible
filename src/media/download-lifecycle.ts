import type { TorrentSnapshot } from "./torrent-client";

export function downloadComplete(snapshot: TorrentSnapshot): boolean {
  return snapshot.complete && snapshot.state !== "checking";
}

export function downloadRemainingBytes(snapshot: Pick<TorrentSnapshot, "sizeBytes" | "bytesDone">): number | null {
  return snapshot.sizeBytes !== null && snapshot.bytesDone !== null &&
    Number.isFinite(snapshot.sizeBytes) && Number.isFinite(snapshot.bytesDone)
    ? Math.max(0, snapshot.sizeBytes - snapshot.bytesDone) : null;
}

export function advanceDownloadProgress(snapshot: TorrentSnapshot, previous: {
  lastBytes: number; progressedAt: number; startedAt: number;
}) {
  const now = Date.parse(snapshot.observedAt);
  if (!Number.isFinite(now)) throw new Error("Invalid client observation time");
  const changed = snapshot.bytesDone === null || snapshot.bytesDone !== previous.lastBytes || snapshot.state === "checking";
  return {
    lastBytes: changed ? snapshot.bytesDone ?? -1 : previous.lastBytes,
    progressedAt: changed ? now : previous.progressedAt,
    startedAt: previous.startedAt || now,
    now,
  };
}

export function downloadPollDecision(snapshot: TorrentSnapshot, configuredPollMs: number) {
  const maxPollMs = Math.max(500, Math.trunc(configuredPollMs || 5000));
  const leftBytes = downloadRemainingBytes(snapshot);
  const downRate = snapshot.downloadBytesPerSecond;
  const base = { pollMs: maxPollMs, leftBytes, downRate, etaSeconds: null as number | null };
  if (leftBytes === null) return { ...base, reason: "fallback_no_left_bytes" };
  if (downRate === null || !Number.isFinite(downRate)) return { ...base, reason: "fallback_no_down_rate" };
  if (downRate <= 0) return { ...base, reason: "fallback_nonpositive_rate" };
  const etaSeconds = leftBytes / downRate;
  if (!Number.isFinite(etaSeconds) || etaSeconds < 0) return { ...base, reason: "fallback_invalid_eta" };
  if (etaSeconds <= 30) return { ...base, pollMs: Math.min(maxPollMs, 500), etaSeconds, reason: "eta_fast" };
  if (etaSeconds <= 120) return { ...base, pollMs: Math.min(maxPollMs, 2000), etaSeconds, reason: "eta_mid" };
  return { ...base, etaSeconds, reason: "eta_slow" };
}
