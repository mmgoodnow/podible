import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { KindlingRepo } from "../../src/kindling/repo";
import { defaultSettings } from "../../src/kindling/settings";
import { runSearch, runSnatch } from "../../src/kindling/service";
import { infoHashFromTorrentBytes } from "../../src/kindling/torrent";

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

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce13:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

describe("snatch transport", () => {
  test("rejects magnet urls to enforce load.raw_start usage", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

    const settings = defaultSettings({
      rtorrent: {
        transport: "http-xmlrpc",
        url: "http://mock.local/RPC2",
      },
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      throw new Error("fetch should not be called for magnet rejection");
    }) as unknown as typeof fetch;

    try {
      await expect(
        runSnatch(repo, settings, {
          bookId: book.id,
          provider: "mock",
          title: "Dune",
          mediaType: "audio",
          url: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
        })
      ).rejects.toThrow("Magnet URLs are not supported");
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("uses load.raw_start for torrent urls", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

    const torrentUrl = "https://example.com/dune.torrent";
    const torrentBytes = makeTorrentBytes("dune-audio");
    const expectedHash = infoHashFromTorrentBytes(torrentBytes);
    const rpcUrl = "https://rtorrent.example/RPC2";
    const settings = defaultSettings({
      rtorrent: {
        transport: "http-xmlrpc",
        url: rpcUrl,
      },
    });

    const methods: string[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      if (url === torrentUrl) {
        return new Response(torrentBytes, {
          status: 200,
          headers: { "Content-Type": "application/x-bittorrent" },
        });
      }
      if (url === rpcUrl) {
        const body = String(init?.body ?? "");
        const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
        methods.push(method);
        return new Response(
          '<?xml version="1.0"?><methodResponse><params><param><value><int>0</int></value></param></params></methodResponse>',
          { status: 200, headers: { "Content-Type": "text/xml" } }
        );
      }
      throw new Error(`Unexpected fetch url: ${url}`);
    }) as unknown as typeof fetch;

    try {
      const result = await runSnatch(repo, settings, {
        bookId: book.id,
        provider: "mock",
        title: "Dune Audio",
        mediaType: "audio",
        url: torrentUrl,
      });
      expect(result.idempotent).toBe(false);
      expect(result.release.info_hash).toBe(expectedHash);
      expect(methods).toContain("load.raw_start");
      expect(methods).not.toContain("load.start");
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });
});
