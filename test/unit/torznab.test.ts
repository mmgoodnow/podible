import { describe, expect, test } from "bun:test";

import { parseTorznabSearch } from "../../src/kindling/torznab";

const SAMPLE = `<?xml version="1.0"?>
<rss><channel>
  <item>
    <title>Dune Audiobook</title>
    <size>12345</size>
    <link>magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567</link>
    <enclosure url="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" length="12345" />
    <torznab:attr name="seeders" value="10" />
    <torznab:attr name="leechers" value="2" />
  </item>
  <item>
    <title>Dune eBook</title>
    <enclosure url="https://example.com/download.torrent" length="200" />
    <torznab:attr name="infohash" value="89abcdef0123456789abcdef0123456789abcdef" />
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

    expect(results[1]?.infoHash).toBe("89abcdef0123456789abcdef0123456789abcdef");
    expect(results[1]?.sizeBytes).toBe(200);
  });
});
