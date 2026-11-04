import { expect, test } from "bun:test";
import { parseTvSeasonBackfill } from "../../web/src/tv-sync-input";

test("TV season form submits only explicit Seerr request IDs, never episode scope", () => {
  expect(parseTvSeasonBackfill("726")).toEqual({ value: { backfillRequestIds: [726] }, error: null });
  expect(parseTvSeasonBackfill("726, 724\n723")).toEqual({ value: { backfillRequestIds: [726, 724, 723] }, error: null });
  expect(parseTvSeasonBackfill("1,2,3,4,5").value).toEqual({ backfillRequestIds: [1, 2, 3, 4, 5] });
});

test("TV season form rejects empty, duplicate, malformed and out-of-bounds request IDs", () => {
  for (const ids of ["", " ", "0", "-1", "1.5", "1e2", "726,726", "1,2,3,4,5,6", "9007199254740992", "all", "/etc/passwd"])
    expect(parseTvSeasonBackfill(ids).value).toBeNull();
});
