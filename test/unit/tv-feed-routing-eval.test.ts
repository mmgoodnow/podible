import { expect, test } from "bun:test";
import { tvFeedCases } from "../fixtures/tv-feed-eval";
import { scoreRoutes } from "../lab/eval-tv-feed-routing";

test("TV feed replay scores exact work/offer pairs, not just whether an offer routed", () => {
  const fixture = tvFeedCases[0]!;
  const index = fixture.expected.findIndex(ids => ids[0] === 1);
  const wrong = { targetId: 2, releaseIndexes: [index, index], reason: "fixture" };
  const result = scoreRoutes(fixture, [wrong]);
  expect(result.positives).toBe(6);
  expect(result.missed).toContainEqual([index, 1]);
  expect(result.extra).toEqual([[index, 2]]);
  expect(() => scoreRoutes(fixture, [{ ...wrong, targetId: 99 }])).toThrow("Unknown target");
  expect(() => scoreRoutes(fixture, [{ ...wrong, releaseIndexes: [fixture.expected.length] }])).toThrow("Unknown offer");
});

test("every TV replay fixture includes both labeled matches and unrelated offers", () => {
  for (const fixture of tvFeedCases) {
    expect(fixture.expected).toHaveLength(fixture.input.offers.length);
    expect(fixture.expected.some(ids => ids.length === 0)).toBe(true);
    const gold = fixture.expected.flatMap((ids, index) => ids.map(targetId => ({ targetId, releaseIndexes: [index], reason: "gold" })));
    expect(gold.length).toBeGreaterThan(0);
    expect(scoreRoutes(fixture, gold)).toEqual({ positives: gold.length, missed: [], extra: [] });
  }
});
