import { expect, test } from "bun:test";
import { retirementPreviewSchema } from "../../src/media/torrent-retirement-types";
import { retirementView } from "../../web/src/torrent-retirement-view";

test("routine protection becomes counts, while unknown evidence and pending/candidate work remain separate", () => {
  const data = retirementPreviewSchema.parse({ source: "all", offset: 0, nextOffset: 20, sourceRows: 100, scannedCount: 20,
    automaticConfigured: true, recordOnly: true, filesDeleted: false, items: ["protected", "unknown", "candidate", "pending", "complete"].map((disposition, i) => ({
      bindingKey: String(i).repeat(64), media: "books", label: `Book ${i}`, disposition, eligible: disposition === "candidate", evidenceQualified: false, blockers: [],
    })) });
  const view = retirementView(data.items);
  expect(view.protectedCount).toBe(1); expect(view.completeCount).toBe(1);
  expect(view.attention.map(item => item.disposition)).toEqual(["candidate", "pending"]);
  expect(view.unknown.map(item => item.disposition)).toEqual(["unknown"]);
  expect(data.sourceRows).toBe(100);
});
