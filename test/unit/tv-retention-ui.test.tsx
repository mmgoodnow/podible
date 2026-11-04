import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TvRetentionAudit, tvRetentionChoices, type TvRetentionClient } from "../../web/src/tv-retention";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

test("TV retention choices use only requested seasons, retain explicit specials and deduplicate shared targets", () => {
  const entry = fixtureTvEntry();
  const choices = tvRetentionChoices([{ targetId: 42, entry, seasonNumbers: [0,1] },{ targetId: 42, entry, seasonNumbers: [1] }]);
  expect(choices.map(choice => choice.label)).toEqual(["A Work (2020) / Season 0","A Work (2020) / Season 1"]);
  expect(choices[1]?.scope).toEqual({ targetId: 42, scheme: "tvdb:aired", seasonNumber: 1 });
  expect(choices.map(choice => choice.key)).toEqual(["42:0","42:1"]);
  expect(choices.every(choice => !choice.label.includes("42"))).toBe(true);
});

test("native season intent and router-derived legacy aired scope stay exact despite a larger catalog", () => {
  const entry = fixtureTvEntry();
  expect(tvRetentionChoices([{ targetId: 1, entry, seasonNumbers: [2] }]).map(choice => choice.scope.seasonNumber)).toEqual([2]);
  expect(tvRetentionChoices([{ targetId: 2, entry, seasonNumbers: [1] }]).map(choice => choice.scope.seasonNumber)).toEqual([1]);
  expect(tvRetentionChoices([{ targetId: 3, entry, seasonNumbers: [] }])).toEqual([]);
  expect(tvRetentionChoices([{ targetId: 4, entry: { ...entry, episodes: [] }, seasonNumbers: [] }])).toEqual([]);
});

test("retention picker does not hide healthy, withdrawn, unavailable or intentionally absent requests", () => {
  const requests = ["Healthy removable title","Withdrawn title","Pending title","Evicted title"].map((title,index) => ({
    targetId: index + 1, entry: { ...fixtureTvEntry(), title }, seasonNumbers: [1], withdrawn: index === 1,
    availableEpisodes: index === 0 ? 5 : 0, intentionallyAbsent: index === 3,
  }));
  expect(new Set(tvRetentionChoices(requests).map(choice => choice.scope.targetId)).size).toBe(4);
});

test("unverified alternate numbering never invents an aired season and same-work targets stay distinct", () => {
  const entry = fixtureTvEntry();
  entry.episodes = entry.episodes.map(episode => ({ ...episode, numbering: [{ scheme: "anidb", season: 99, episode: 1, absolute: 1, source: "explicit-other-scheme" }] }));
  const choices = tvRetentionChoices([{ targetId: 1, entry, seasonNumbers: [2] },{ targetId: 2, entry, seasonNumbers: [2] }]);
  expect(choices.map(choice => choice.scope)).toEqual([{ targetId: 1, scheme: "tvdb:aired", seasonNumber: 2 },{ targetId: 2, scheme: "tvdb:aired", seasonNumber: 2 }]);
});

test("TV retention starts with a title picker, no numeric ID input and no implicit audit/removal", () => {
  let calls = 0;
  const client: TvRetentionClient = { async list() { calls++; return { items: [] }; },
    async audit() { calls++; throw new Error("Unexpected audit"); }, async evict() { calls++; throw new Error("Unexpected removal"); } };
  const html = renderToStaticMarkup(createElement(TvRetentionAudit,{ client }));
  expect(html).toContain("TV title and season");
  expect(html).toContain("Choose a title and season");
  expect(html).not.toContain('type="number"');
  expect(html).not.toContain("TV target ID");
  expect(html).toContain('<button type="submit" disabled="">Audit entire season</button>');
  expect(calls).toBe(0);
});
