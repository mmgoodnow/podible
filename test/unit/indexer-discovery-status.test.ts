import { expect, test } from "bun:test";
import { indexerDiscoveryStatus, indexerSources } from "../../src/media/indexer-discovery";
import { defaultSettings } from "../../src/settings";

test("summarizes discovered indexers and identifies manual aliases by endpoint, not name", async () => {
  const settings = defaultSettings({
    prowlarr: { url: "http://prowlarr:9696/prowlarr", apiKey: "private-fixture-key" },
    torznab: [
      { name: "Manual alias", baseUrl: "http://prowlarr:9696/prowlarr/17?apikey=other-secret" },
      { name: "Fixture", baseUrl: "https://another-indexer.invalid/api" },
    ],
  });
  const status = await indexerDiscoveryStatus(settings, async (_settings, media, mode) =>
    media === "audio" || (media === "ebook" && mode === "search")
      ? [{ name: "Fixture private-fixture-key", baseUrl: "http://prowlarr:9696/prowlarr/17/api", apiKey: "private-fixture-key" }]
      : []);
  expect(status).toEqual({
    state: "available",
    indexers: [{ id: 17, name: "Fixture [redacted]", search: ["audio", "ebook"], feeds: ["audio"], retryAt: null }],
    duplicates: [{ index: 0, name: "Fixture [redacted]" }],
  });
});

test("search and feed resolution union discovered and direct sources by endpoint", async () => {
  const duplicate = { name: "Manual alias", baseUrl: "https://prowlarr.invalid/base/17?apikey=ignored", apiKey: "ignored" };
  const direct = { name: "Direct", baseUrl: "https://direct.invalid/api", categories: { audio: "1234" } };
  const invalid = { name: "Invalid", baseUrl: "not-a-url" };
  const discovered = { name: "Discovered", baseUrl: "https://prowlarr.invalid/base/17/api/", apiKey: "discovered",
    categories: { audio: "3030" }, supportsPagination: false };
  const settings = defaultSettings({
    prowlarr: { url: "https://prowlarr.invalid/base", apiKey: "discovered" },
    torznab: [duplicate, direct, invalid],
  });
  const discover: Parameters<typeof indexerSources>[3] = async (_settings, media, mode) =>
    media === "audio" && mode === "rss" ? [] : [discovered];
  expect(await indexerSources(settings, "movie", "search", discover)).toEqual([discovered, direct, invalid]);
  expect(await indexerSources(settings, "tv", "rss", discover)).toEqual([discovered, direct, invalid]);
  expect(await indexerSources(settings, "audio", "rss", discover)).toEqual([duplicate, direct, invalid]);
  expect(await indexerSources(defaultSettings({ torznab: [direct] }), "ebook", "search", discover)).toEqual([direct]);
});
