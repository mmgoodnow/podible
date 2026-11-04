import { expect, test } from "bun:test";
import { defaultSettings, parseSettings } from "../../src/settings";
import { tvSettingsSchema } from "../../src/tv/settings";
import { IndexerGateway } from "../../src/media/indexer-gateway";
import { ProwlarrDiscovery } from "../../src/media/prowlarr";

test("TV configuration adds no global enable flag or duplicate Plex/Seerr credentials", () => {
  const settings = parseSettings(JSON.stringify(defaultSettings()));
  expect(settings.tv.managedRoot).toBe("");
  expect(settings.tv).not.toHaveProperty("enabled");
  expect(settings.tv).not.toHaveProperty("seerrApiKey");
  expect(() => tvSettingsSchema.parse({ enabled: true })).toThrow();
  expect(() => tvSettingsSchema.parse({ maximumReleaseBytes: -1 })).toThrow();
});

test("TV discovers aired-TV and anime categories but never book-only or movie-only providers", async () => {
  const categories = [2000, 3030, 7020, 5000, 5040, 5070];
  const discovery = new ProwlarrDiscovery({ url: "https://prowlarr.invalid", apiKey: "private" }, async (input) =>
    Response.json(String(input).endsWith("/indexerstatus") ? [] : categories.map((category) => ({
      id: category, name: String(category), enable: true, protocol: "torrent", supportsSearch: true,
      capabilities: { categories: [{ id: category }] },
    }))));
  expect((await discovery.sources("tv")).map((source) => source.name)).toEqual(["5000", "5040", "5070"]);
});

test("TV search uses the shared category-scoped transport and preserves untagged candidates", async () => {
  const gateway = new IndexerGateway(async (url) => {
    expect(url.searchParams.get("cat")).toBe("5000");
    expect(url.searchParams.get("q")).toBe("A series");
    return new Response('<rss><channel><item><title>Ambiguous episode release</title><link>https://fixture.invalid/torrent</link></item></channel></rss>');
  });
  const result = await gateway.search([{ name: "Fixture", baseUrl: "https://fixture.invalid/api" }], "A series", "tv");
  expect(result.results).toHaveLength(1);
  expect(result.results[0]?.mediaType).toBe("tv");
});
