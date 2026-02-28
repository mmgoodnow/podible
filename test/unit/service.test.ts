import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/books/db";
import { BooksRepo } from "../../src/books/repo";
import { defaultSettings } from "../../src/books/settings";
import { runSearch, runSnatch } from "../../src/books/service";
import { torrentCacheKeyFor } from "../../src/books/torrent-cache";
import { infoHashFromTorrentBytes } from "../../src/books/torrent";

describe("search ranking", () => {
  test("penalizes box-set style matches", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((async () =>
      new Response(
        `<?xml version="1.0"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
  <item>
    <title>Dune [ENG / M4B]</title>
    <enclosure url="https://example.com/dune.torrent" length="100" />
    <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567" />
    <torznab:attr name="seeders" value="5" />
  </item>
  <item>
    <title>Dune Complete Box Set [ENG / MP3]</title>
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
      expect(ranked[0]?.title).toBe("Dune [ENG / M4B]");
      expect(ranked[1]?.title).toContain("Box Set");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("only returns results that match requested media format", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((async () =>
      new Response(
        `<?xml version="1.0"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
  <item>
    <title>Great Big Beautiful Life by Emily Henry [ENG / EPUB]</title>
    <enclosure url="https://example.com/epub.torrent" length="100" />
    <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567" />
    <torznab:attr name="seeders" value="900" />
  </item>
  <item>
    <title>Great Big Beautiful Life by Emily Henry [ENG / M4B]</title>
    <enclosure url="https://example.com/m4b.torrent" length="100" />
    <torznab:attr name="infohash" value="89abcdef0123456789abcdef0123456789abcdef" />
    <torznab:attr name="seeders" value="1" />
  </item>
</channel></rss>`,
        { headers: { "Content-Type": "application/rss+xml" } }
      )) as unknown) as typeof fetch;

    try {
      const settings = defaultSettings({
        torznab: [{ name: "mock", baseUrl: "http://mock.local" }],
      });

      const audio = await runSearch(settings, { query: "Great Big Beautiful Life Emily Henry", media: "audio" });
      expect(audio).toHaveLength(1);
      expect(audio[0]?.title).toContain("/ M4B");

      const ebook = await runSearch(settings, { query: "Great Big Beautiful Life Emily Henry", media: "ebook" });
      expect(ebook).toHaveLength(1);
      expect(ebook[0]?.title).toContain("/ EPUB");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce15:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

describe("snatch transport", () => {
  test("rejects magnet urls to enforce load.raw_start usage", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
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
          infoHash: "0123456789abcdef0123456789abcdef01234567",
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
    const repo = new BooksRepo(db);
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

    const torrentUrl = "https://example.com/dune.torrent";
    const torrentBytes = makeTorrentBytes("dune-audio");
    const expectedHash = infoHashFromTorrentBytes(torrentBytes);
    const rpcUrl = "https://rtorrent.example/RPC2";
    const settings = defaultSettings({
      rtorrent: {
        transport: "http-xmlrpc",
        url: rpcUrl,
        downloadPath: "/downloads/books",
      },
    });

    const methods: string[] = [];
    const rpcBodies: string[] = [];
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
        rpcBodies.push(body);
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
      expect(rpcBodies.some((body) => body.includes("d.directory_base.set=&quot;/downloads/books&quot;"))).toBe(true);
      expect(rpcBodies.some((body) => body.includes("d.custom1.set=&quot;Podible&quot;"))).toBe(true);
      expect(rpcBodies.some((body) => /d\.custom\.set=addtime,\d{10}/.test(body))).toBe(true);
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("reuses cached torrent bytes and skips torrent fetch", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

    const torrentUrl = "https://example.com/dune-cached.torrent";
    const provider = "mock";
    const providerGuid = "g-cache";
    const torrentBytes = makeTorrentBytes("dune-audio-cached");
    const expectedHash = infoHashFromTorrentBytes(torrentBytes);
    repo.putTorrentCache({
      key: torrentCacheKeyFor({ provider, providerGuid, url: torrentUrl }),
      provider,
      providerGuid,
      url: torrentUrl,
      torrentBytes,
    });

    const rpcUrl = "https://rtorrent.example/RPC2";
    const settings = defaultSettings({
      rtorrent: {
        transport: "http-xmlrpc",
        url: rpcUrl,
        downloadPath: "",
      },
    });

    const fetches: string[] = [];
    const rpcBodies: string[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      fetches.push(url);
      if (url === torrentUrl) {
        throw new Error("torrent url should not be fetched when cache is populated");
      }
      if (url === rpcUrl) {
        const body = String(init?.body ?? "");
        rpcBodies.push(body);
        const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
        expect(method).toBe("load.raw_start");
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
        provider,
        providerGuid,
        title: "Dune Audio",
        mediaType: "audio",
        url: torrentUrl,
      });
      expect(result.idempotent).toBe(false);
      expect(result.release.info_hash).toBe(expectedHash);
      expect(fetches).not.toContain(torrentUrl);
      expect(fetches).toContain(rpcUrl);
      expect(rpcBodies.some((body) => body.includes("d.custom1.set=&quot;Podible&quot;"))).toBe(true);
      expect(rpcBodies.some((body) => /d\.custom\.set=addtime,\d{10}/.test(body))).toBe(true);
      expect(rpcBodies.some((body) => body.includes("d.directory_base.set="))).toBe(false);
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });
});
