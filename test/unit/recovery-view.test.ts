import { expect, test } from "bun:test";
import type { RecoveryResource } from "../../src/media/recovery-types";
import { recoveryResourceKey, recoveryView } from "../../web/src/recovery-view";

function resource(overrides: Partial<RecoveryResource> = {}): RecoveryResource {
  return { key: { namespace: "movie-target", id: "1" }, title: "A title", revision: "a".repeat(64),
    targetRevision: 1, intentRevision: null, desiredPresence: "unknown", requestIds: [],
    mount: { state: "verified", contentId: null, reason: "Verified mount" }, canonical: { state: "present", files: [] },
    view: { state: "present", files: [] }, activeAcquisitions: [], action: "blocked", reasons: [{ code: "intent-unknown", message: "Unknown intent" }], ...overrides };
}

test("healthy bytes and links disappear even when intent blocks recovery", () => {
  expect(recoveryView([resource()])).toEqual({ missing: [], unchecked: [], checkedCount: 1, actionable: [] });
});

test("only confirmed selected canonical loss or missing/conflicting views enter the main list", () => {
  for (const change of [{ canonical: { state: "missing", files: [] } }, { view: { state: "missing", files: [] } },
    { view: { state: "conflict", files: [] } }] satisfies Partial<RecoveryResource>[]) {
    const item = resource(change);
    expect(recoveryView([item]).missing).toEqual([item]);
  }
});

test("unselected historical missing files and initial pending requests are not lost content", () => {
  const historical = resource({ canonical: { state: "present", files: [{ id: "old", relativePath: "old.mkv", sizeBytes: 1, presence: "missing", selected: false }] } });
  const pending = resource({ canonical: { state: "not-selected", files: [] }, view: { state: "not-selected", files: [] }, action: "queue-acquisition" });
  expect(recoveryView([historical, pending])).toEqual({ missing: [], unchecked: [], checkedCount: 2, actionable: [] });
});

test("mount failures and unknown owned paths are separate from confirmed loss and checked counts", () => {
  const mount = resource({ mount: { state: "unavailable", contentId: null, reason: "Missing mount" }, canonical: { state: "unknown", files: [] }, view: { state: "unknown", files: [] } });
  const path = resource({ canonical: { state: "unknown", files: [] } });
  expect(recoveryView([mount, path])).toEqual({ missing: [], unchecked: [mount, path], checkedCount: 0, actionable: [] });
});

test("an unavailable mount cannot turn saved present or missing states into a current finding", () => {
  for (const state of ["present", "missing"] as const) {
    const item = resource({ mount: { state: "unavailable", contentId: null, reason: "Not mounted" },
      canonical: { state, files: [] }, view: { state, files: [] } });
    expect(recoveryView([item])).toEqual({ missing: [], unchecked: [item], checkedCount: 0, actionable: [] });
  }
});

test("select-all includes only visible missing resources with supported actions", () => {
  const repair = resource({ view: { state: "missing", files: [] }, action: "repair-view" });
  const missing = resource({ canonical: { state: "missing", files: [] }, action: "queue-acquisition" });
  const conflict = resource({ view: { state: "conflict", files: [] } });
  expect(recoveryView([resource({ action: "repair-view" }), repair, missing, conflict]).actionable).toEqual([repair, missing]);
});

test("saved result title keys distinguish movie and TV IDs", () => {
  expect(recoveryResourceKey(resource())).toBe("movie-target:1");
  expect(recoveryResourceKey(resource({ key: { namespace: "tv-target", id: "1" } }))).toBe("tv-target:1");
});
