import { expect, test } from "bun:test";
import { advanceDownloadProgress, downloadComplete, downloadPollDecision, downloadRemainingBytes } from "../../src/media/download-lifecycle";
import type { TorrentSnapshot } from "../../src/media/torrent-client";

const snapshot = (patch: Partial<TorrentSnapshot> = {}): TorrentSnapshot => ({
  ref: { clientId: "fixture", nativeId: "a".repeat(40) }, location: { rootId: "books", relativePath: "a".repeat(40) },
  state: "downloading", complete: false, bytesDone: 10, sizeBytes: 100,
  downloadBytesPerSecond: 1, message: null, observedAt: "2026-11-06T00:01:00Z", ...patch,
});
test("client completion is not publication readiness while a check is active", () => {
  expect(downloadComplete(snapshot({ complete: true, state: "checking" }))).toBe(false);
  expect(downloadComplete(snapshot({ complete: true, state: "seeding" }))).toBe(true);
});
test("polling uses neutral byte/rate evidence and never infers missing telemetry as zero", () => {
  expect(downloadRemainingBytes(snapshot({ bytesDone: null }))).toBeNull();
  expect(downloadPollDecision(snapshot(), 5000)).toMatchObject({ pollMs: 2000, etaSeconds: 90 });
  expect(downloadPollDecision(snapshot({ bytesDone: 99 }), 5000).pollMs).toBe(500);
  expect(downloadPollDecision(snapshot({ downloadBytesPerSecond: null }), 5000).pollMs).toBe(5000);
});
test("shared screen progress clock protects rechecking and unknown telemetry from false stalls", () => {
  const previous = { lastBytes: 10, startedAt: 1, progressedAt: 2 };
  expect(advanceDownloadProgress(snapshot(), previous).progressedAt).toBe(2);
  for (const patch of [{ bytesDone: null }, { bytesDone: 5 }, { state: "checking" }] as Partial<TorrentSnapshot>[])
    expect(advanceDownloadProgress(snapshot(patch), previous).progressedAt).toBe(Date.parse(snapshot().observedAt));
  expect(() => advanceDownloadProgress(snapshot({ observedAt: "invalid" }), previous)).toThrow("observation time");
});
