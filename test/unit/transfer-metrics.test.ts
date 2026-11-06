import { expect, test } from "bun:test";
import { advanceTransferCounter, normalizeTrackerHost } from "../../src/media/transfer-metrics";
import { sanitizedMetricTrackers } from "../../src/media/transfer-metrics-collect";

test("transfer counters accumulate observed deltas, survive resets and preserve unknown readings", () => {
  expect(advanceTransferCounter(null, null, 100)).toBe(100);
  expect(advanceTransferCounter(100, 500, 120)).toBe(520);
  expect(advanceTransferCounter(100, 500, 4)).toBe(504);
  expect(advanceTransferCounter(100, 500, null)).toBe(500);
});
test("tracker mappings reject credentials, URL paths, ports and control characters", () => {
  expect(normalizeTrackerHost(" Tracker.Example. ")).toBe("tracker.example");
  for (const bad of ["https://host.example/secret", "host.example:443", "user:pass@host.example", "host.example?passkey=x", "host.example\n/x"])
    expect(() => normalizeTrackerHost(bad)).toThrow();
});
test("tracker sanitization deduplicates endpoints, retains unknown announce state and never emits private text", () => {
  const now = 1_800_000_000_000;
  const rows = sanitizedMetricTrackers([{ announce: "https://tracker.example/secret?a=passkey", lastAnnounceTime: now / 1000,
    hasAnnounced: true, lastAnnounceSucceeded: true, lastAnnounceTimedOut: false, lastAnnounceResult: "secret" },
    { announce: "udp://tracker.example:80/another-key" }, { announce: "file:///secret" }], now);
  expect(rows).toEqual([{ host: "tracker.example", state: "healthy" }]);
});
