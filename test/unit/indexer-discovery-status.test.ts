import { expect, test } from "bun:test";
import { indexerDiscoveryStatus } from "../../src/media/indexer-discovery";
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
