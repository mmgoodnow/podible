import { describe, expect, test } from "bun:test";

import { parseTorznabSearch } from "../../src/kindling/torznab";

const SAMPLE = `<?xml version="1.0"?>
<rss><channel>
  <item>
    <title>Dune Audiobook</title>
    <size>12345</size>
    <link>https://example.com/audio.torrent</link>
    <enclosure url="https://example.com/audio.torrent" length="12345" />
    <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567" />
    <torznab:attr name="seeders" value="10" />
    <torznab:attr name="leechers" value="2" />
  </item>
  <item>
    <title>Dune eBook</title>
    <guid>https://tracker.example/torrent/2</guid>
    <enclosure url="https://example.com/download.torrent" length="200" />
  </item>
  <item>
    <title>Dune Magnet</title>
    <enclosure url="magnet:?xt=urn:btih:1111111111111111111111111111111111111111" length="999" />
    <torznab:attr name="infohash" value="1111111111111111111111111111111111111111" />
  </item>
</channel></rss>`;

describe("torznab parser", () => {
  test("normalizes search results", () => {
    const results = parseTorznabSearch(SAMPLE, "prowlarr", "audio");
    expect(results).toHaveLength(2);

    expect(results[0]?.title).toBe("Dune Audiobook");
    expect(results[0]?.provider).toBe("prowlarr");
    expect(results[0]?.infoHash).toBe("0123456789abcdef0123456789abcdef01234567");
    expect(results[0]?.seeders).toBe(10);
    expect(results[0]?.leechers).toBe(2);
    expect(results[0]?.sizeBytes).toBe(12345);

    expect(results[1]?.sizeBytes).toBe(200);
    expect(results[1]?.infoHash).toBeNull();
    expect(results[1]?.guid).toBe("https://tracker.example/torrent/2");
  });
});
