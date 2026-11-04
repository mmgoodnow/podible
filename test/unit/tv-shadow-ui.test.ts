import { expect, test } from "bun:test";
import { parseTvCanaryBackfill } from "../../web/src/tv-sync-input";

test("TV canary form parses only explicitly bounded requests and episode limits", () => {
  expect(parseTvCanaryBackfill("726", "1")).toEqual({ value: { backfillRequestIds: [726], episodeLimit: 1 }, error: null });
  expect(parseTvCanaryBackfill("726, 724\n723", "2")).toEqual({ value: { backfillRequestIds: [726, 724, 723], episodeLimit: 2 }, error: null });
  expect(parseTvCanaryBackfill("1,2,3,4,5", "50").value).toEqual({ backfillRequestIds: [1, 2, 3, 4, 5], episodeLimit: 50 });
});

test("TV canary form rejects empty, duplicate, malformed and out-of-bounds scope", () => {
  for (const ids of ["", " ", "0", "-1", "1.5", "1e2", "726,726", "1,2,3,4,5,6", "9007199254740992", "all", "/etc/passwd"])
    expect(parseTvCanaryBackfill(ids, "1").value).toBeNull();
  for (const limit of ["", "0", "-1", "51", "1.5", "1e1", "all", "NaN"])
    expect(parseTvCanaryBackfill("726", limit).value).toBeNull();
});
