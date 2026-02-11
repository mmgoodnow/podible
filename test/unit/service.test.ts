import { describe, expect, test } from "bun:test";

import { defaultSettings } from "../../src/kindling/settings";
import { runSearch } from "../../src/kindling/service";

describe("search ranking", () => {
  test("penalizes box-set style matches", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((async () =>
      new Response(
        `<?xml version="1.0"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
  <item>
    <title>Dune</title>
    <enclosure url="https://example.com/dune.torrent" length="100" />
    <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567" />
    <torznab:attr name="seeders" value="5" />
  </item>
  <item>
    <title>Dune Complete Box Set</title>
    <enclosure url="https://example.com/box.torrent" length="50" />
    <torznab:attr name="infohash" value="89abcdef0123456789abcdef0123456789abcdef" />
    <torznab:attr name="seeders" value="80" />
  </item>
</channel></rss>`,
        { headers: { "Content-Type": "application/rss+xml" } }
      )) as unknown) as typeof fetch;

    try {
      const settings = defaultSettings({
        torznab: [{ name: "mock", baseUrl: "http://mock.local" }],
      });
      const ranked = await runSearch(settings, { query: "Dune", media: "audio" });
      expect(ranked).toHaveLength(2);
      expect(ranked[0]?.title).toBe("Dune");
      expect(ranked[1]?.title).toContain("Box Set");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
