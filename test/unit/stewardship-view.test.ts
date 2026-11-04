import { expect, test } from "bun:test";
import { stewardshipItemSchema, stewardshipObservationSchema } from "../../src/media/torrent-stewardship-types";
import { stewardshipIssue, stewardshipView } from "../../web/src/stewardship-view";

const observation = stewardshipObservationSchema.parse({ observedAt: "2026-11-04T12:00:00Z", bindingDigest: "a".repeat(64),
  ownership: "verified-private-binding", state: "seeding", complete: true, bytesRemaining: 0, daemonError: "none",
  trackerState: "healthy", trackerCount: 1, trackerErrorCount: 0, trackerUnknownCount: 0, trackerSetDigest: null,
  announcementTime: null, actualSeedSeconds: 100, seedCounterRegressed: false, seedProtection: "protected", remainingSeedSeconds: 2591900,
  consistentUnregisteredAnnouncements: 0, repeatedUnregistered: false, reason: "SENSITIVE raw tracker text must never become the UI root cause" });
const item = stewardshipItemSchema.parse({ key: "b".repeat(64), media: "books", label: "A real book", titles: [{ title: "A real book", href: "/book/1" }],
  stale: false, observation, retirementAllowed: false, retirementBlockers: [] });

test("fresh healthy records are counted, not emitted as rows; normal seed protection is not an error", () => {
  expect(stewardshipView([item])).toEqual({ problems: [], unknown: [], healthyCount: 1 });
});
test("stale successful and unverified observations never count as healthy or confirmed failures", () => {
  const stale = { ...item, stale: true };
  const missing = { ...item, observation: null };
  const ownership = { ...item, observation: { ...observation, ownership: "binding-unverified" as const } };
  expect(stewardshipView([stale, missing, ownership])).toEqual({ problems: [], unknown: [stale, missing, ownership], healthyCount: 0 });
});
test("storage errors, confirmed tracker rejection and paused incomplete data are actionable problems", () => {
  for (const override of [{ daemonError: "storage-error" }, { trackerState: "confirmed-error" }, { trackerState: "confirmed-unregistered" },
    { state: "paused-incomplete", complete: false }] as const) {
    const problem = { ...item, observation: { ...observation, ...override } };
    expect(stewardshipView([problem]).problems).toEqual([problem]);
    expect(stewardshipIssue(problem)?.action).not.toBeEmpty();
    expect(stewardshipIssue(problem)?.cause).not.toContain("SENSITIVE");
  }
});
test("ambiguous tracker warnings and unknown or regressed seed counters remain unverified", () => {
  for (const override of [{ daemonError: "tracker-warning", trackerState: "unknown" }, { actualSeedSeconds: null }, { seedCounterRegressed: true }] as const) {
    const unknown = { ...item, observation: { ...observation, ...override } };
    expect(stewardshipView([unknown]).unknown).toEqual([unknown]);
    expect(stewardshipView([unknown]).healthyCount).toBe(0);
  }
});
test("stale historical tracker errors do not become current attention alerts", () => {
  const stale = { ...item, stale: true, observation: { ...observation, trackerState: "confirmed-error" as const } };
  expect(stewardshipView([stale]).problems).toEqual([]);
  expect(stewardshipIssue(stale)?.title).toBe("Observation out of date");
});
