import { expect, test } from "bun:test";
import { activityItems, activityPhase, attentionItems, downloadProgress, problemAction, type ActivityDownload } from "../../web/src/activity-model";

function download(key: string, overrides: Partial<ActivityDownload> = {}): ActivityDownload {
  return { key, title: "A season", media: "tv", targetId: 1, href: "/tv-sync", stage: "downloading",
    job: { id: 1, type: "tv_acquire", status: "running", bookId: null, releaseId: null,
      attempts: 1, maxAttempts: 3, nextWakeAt: null, error: null, recovery: "running" },
    bytesDone: 25, sizeBytes: 100, bytesPerSecond: 5, observedAt: "2026-11-04T00:00:00Z", telemetry: "live", problem: null, ...overrides };
}
test("one activity card retains every download belonging to the same acquisition", () => {
  const first = download("part1"), second = download("part2", { bytesDone: 75, sizeBytes: 200 });
  const rows = activityItems({ activeAcquisitions: [first], downloads: [first, second] });
  expect(rows).toHaveLength(1);
  expect(rows[0]!.downloads).toEqual([first, second]);
  expect(downloadProgress(rows[0]!.downloads)).toEqual({ done: 100, size: 300, speed: 10, live: true });
});
test("a sleeping download poll remains active downloading, not a blocked or waiting workflow", () => {
  const item = download("one", { stage: "downloading", state: "downloading",
    job: { ...download("one").job, recovery: "scheduled-wake", status: "queued", nextWakeAt: "2099-01-01T00:00:00Z" } });
  expect(activityPhase(item, [item])).toMatchObject({ label: "Downloading", active: true, showWake: false });
  expect(activityPhase({ ...item, stage: "blocked" }, [item])).toMatchObject({ label: "Downloading", active: true });
  expect(activityPhase({ ...item, stage: "publishing" }, [item])).toMatchObject({ label: "Publication pending", active: true, transferLabel: "Other attached downloads" });
  expect(activityPhase({ ...item, stage: "blocked" }, [])).toMatchObject({ label: "Waiting for a suitable release", active: false });
  expect(activityPhase(item, [{ ...item, state: "checking", bytesPerSecond: 0 }])).toMatchObject({ label: "Checking download", active: true });
  expect(activityPhase(item, [{ ...item, telemetry: "recorded" }])).toMatchObject({ label: "Waiting for download status", active: false });
});
test("a historical publication event alone does not claim active Plex work", () => {
  const item = download("one", { stage: "publishing", job: { ...download("one").job, recovery: "scheduled-wake", status: "queued" } });
  expect(activityPhase(item, [])).toMatchObject({ label: "Publication pending", active: false, showWake: true });
  expect(activityPhase({ ...item, stage: "publication-queued" }, [])).toMatchObject({ label: "Waiting to publish", active: false });
});
test("unknown or inconsistent member progress cannot masquerade as whole-request progress", () => {
  expect(downloadProgress([download("one"), download("two", { telemetry: "unknown", bytesDone: null })])).toBeNull();
  expect(downloadProgress([download("one", { sizeBytes: 0 })])).toBeNull();
  expect(downloadProgress([download("one", { bytesDone: 101 })])).toBeNull();
  expect(downloadProgress([])).toBeNull();
  expect(downloadProgress([download("one", { bytesDone: Number.MAX_SAFE_INTEGER, sizeBytes: Number.MAX_SAFE_INTEGER }), download("two")])).toBeNull();
});
test("recorded progress is not presented as current live speed", () => {
  expect(downloadProgress([download("one"), download("two", { telemetry: "recorded", bytesPerSecond: null })]))
    .toEqual({ done: 50, size: 200, speed: null, live: false });
});
test("downloads without an acquisition still receive one complete activity card", () => {
  const rows = activityItems({ activeAcquisitions: [], downloads: [download("one"), download("two")] });
  expect(rows).toHaveLength(1); expect(rows[0]!.downloads).toHaveLength(2);
});
test("configuration and storage failures lead to correcting the cause rather than blind retry", () => {
  expect(problemAction("provider-configuration")).toEqual({ href: "/settings", label: "Review connection settings" });
  expect(problemAction("tracker-client-not-allowed")?.href).toBe("/settings");
  expect(problemAction("storage-capacity")).toEqual({ href: "#activity-storage", label: "Review storage" });
  expect(problemAction("storage-identity")?.href).toBe("#activity-storage");
  expect(problemAction("operation-failed")).toBeNull();
});
test("automatic recovery and informational cooldowns do not become human-attention alerts", () => {
  const base = { key: "issue", title: "A title", detail: "Issue", code: "operation-failed", jobId: 1, href: null, nextWakeAt: null };
  expect(attentionItems([
    { ...base, recovery: "queued" }, { ...base, recovery: "automatic-retry" }, { ...base, recovery: "scheduled-wake" },
    { ...base, recovery: "cancelled" }, { ...base, recovery: "unknown", code: "provider-cooldown" },
    { ...base, recovery: "unknown", code: "provider-unavailable" },
    { ...base, recovery: "manual-retry" }, { ...base, recovery: "unknown", code: "storage-identity" },
  ])).toEqual([{ ...base, recovery: "manual-retry" }, { ...base, recovery: "unknown", code: "storage-identity" }]);
});
