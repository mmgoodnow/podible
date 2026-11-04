import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { MockBookTorrentClient } from "../mocks/book-torrents";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { runSearch, runSnatch, runSnatchGroup } from "../../src/library/service";
import { torrentCacheKeyFor } from "../../src/library/torrent-cache";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";

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

  test("ranks requested media first without withholding other evidence from the agent", async () => {
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
      expect(audio).toHaveLength(2);
      expect(audio[0]?.title).toContain("/ M4B");
      expect(audio[1]?.title).toContain("/ EPUB");

      const ebook = await runSearch(settings, { query: "Great Big Beautiful Life Emily Henry", media: "ebook" });
      expect(ebook).toHaveLength(2);
      expect(ebook[0]?.title).toContain("/ EPUB");
      expect(ebook[1]?.title).toContain("/ M4B");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("keeps category-scoped results whose titles omit format markers", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = ((async () =>
      new Response(
        `<?xml version="1.0"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
  <item>
    <title>Foundation by Isaac Asimov</title>
    <enclosure url="https://example.com/foundation.torrent" length="100" />
    <torznab:attr name="seeders" value="20" />
  </item>
  <item>
    <title>Foundation by Isaac Asimov [ENG / EPUB]</title>
    <enclosure url="https://example.com/foundation-epub.torrent" length="100" />
    <torznab:attr name="seeders" value="1" />
  </item>
  <item>
    <title>Foundation by Isaac Asimov [ENG / M4B]</title>
    <enclosure url="https://example.com/foundation-m4b.torrent" length="100" />
  </item>
</channel></rss>`,
        { headers: { "Content-Type": "application/rss+xml" } }
      )) as unknown) as typeof fetch;

    try {
      const settings = defaultSettings({
        torznab: [{ name: "mock", baseUrl: "http://mock.local", categories: { ebook: "7000" } }],
      });
      const ebook = await runSearch(settings, { query: "Foundation Isaac Asimov", media: "ebook" });
      expect(ebook.map((result) => result.title)).toEqual([
        "Foundation by Isaac Asimov [ENG / EPUB]",
        "Foundation by Isaac Asimov",
        "Foundation by Isaac Asimov [ENG / M4B]",
      ]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

});

function makeTorrentBytes(name: string): Uint8Array {
  return bencode.encode({ announce: "http://tracker/", info: {
    name, length: 10, "piece length": 16384, pieces: Buffer.from("12345678901234567890"),
  } });
}

describe("snatch transport", () => {
  async function fixture() {
    const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-snatch-")));
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
    const settings = defaultSettings({ libraryRoot: root, rtorrent: { url: "http://never-legacy.invalid/RPC2" } });
    const client = new MockBookTorrentClient(path.join(root, ".podible", "downloads"));
    const originalFetch = globalThis.fetch;
    const torrents = new Map<string, Uint8Array>();
    let requests = 0;
    globalThis.fetch = (async (input: unknown) => {
      if (!String(input).startsWith("https://fixture.invalid/")) throw new Error("Unexpected non-metainfo request");
      requests++;
      return new Response(torrents.get(String(input)) ?? makeTorrentBytes(String(input).split("/").at(-1)!), {
        headers: { "Content-Type": "application/x-bittorrent" },
      });
    }) as typeof fetch;
    return {
      db, repo, book, settings, client, torrents, requests: () => requests,
      snatch: (overrides: Partial<Parameters<typeof runSnatch>[2]> = {}) => runSnatch(repo, settings, {
        bookId: book.id, provider: "fixture", title: "Red Rising", mediaType: "audio",
        url: "https://fixture.invalid/book.mp3", ...overrides,
      }, { bookTorrentClient: client }),
      async close() { globalThis.fetch = originalFetch; db.close(); await fs.rm(root, { recursive: true, force: true }); },
    };
  }

  test("rejects magnets before contacting a torrent client", async () => {
    const f = await fixture();
    try {
      await expect(f.snatch({ url: "magnet:?xt=urn:btih:" + "a".repeat(40) })).rejects.toThrow("Magnet URLs");
      expect(f.client.adds).toHaveLength(0);
      expect(f.requests()).toBe(0);
    } finally { await f.close(); }
  });

  test("new books reserve private Transmission ownership before adding, then attach the release", async () => {
    const f = await fixture();
    try {
      f.client.beforeAdd = hash => {
        expect(f.repo.bookTorrents.get(hash)).toMatchObject({
          book_id: f.book.id, backend: "transmission", client_id: f.client.id, release_id: null,
          root_path: path.join(f.settings.libraryRoot, ".podible", "downloads"), relative_path: hash,
        });
        expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(0);
      };
      const result = await f.snatch();
      expect(result.idempotent).toBe(false);
      expect(f.client.adds).toEqual([result.release.info_hash]);
      expect(f.client.resumes).toEqual([result.release.info_hash]);
      expect(f.repo.bookTorrents.get(result.release.info_hash)?.release_id).toBe(result.release.id);
      expect(f.requests()).toBe(1);
    } finally { await f.close(); }
  });

  test("uses cached metainfo initially but verifies GUID collisions before hash deduplication", async () => {
    const f = await fixture();
    try {
      const url = "https://fixture.invalid/cached.mp3";
      f.repo.putTorrentCache({
        key: torrentCacheKeyFor({ provider: "fixture", providerGuid: "cached", url }),
        provider: "fixture", providerGuid: "cached", url, torrentBytes: makeTorrentBytes("cached.mp3"),
      });
      const hash = infoHashFromTorrentBytes(makeTorrentBytes("cached.mp3"));
      const first = await f.snatch({ url, providerGuid: "cached", infoHash: hash });
      const second = await f.snatch({ url, providerGuid: "cached", infoHash: hash });
      expect(first.idempotent).toBe(false);
      expect(second.idempotent).toBe(true);
      expect(second.release.id).toBe(first.release.id);
      expect(f.requests()).toBe(1);
      expect(f.client.adds).toHaveLength(1);
    } finally { await f.close(); }
  });

  test("reconciles an external add before a lost acknowledgment without creating another torrent", async () => {
    const f = await fixture();
    try {
      f.client.failResumeOnce = true;
      await expect(f.snatch()).rejects.toThrow("Lost resume acknowledgment");
      expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(0);
      const hash = f.client.adds[0]!;
      expect(f.repo.bookTorrents.get(hash)?.release_id).toBeNull();
      const result = await f.snatch();
      expect(f.client.adds).toHaveLength(1);
      expect(f.repo.bookTorrents.get(hash)?.release_id).toBe(result.release.id);
      expect(f.repo.listJobsByType("download")).toHaveLength(1);
    } finally { await f.close(); }
  });

  test("reused GUID with a new known hash creates a new release and preserves the old seeding torrent", async () => {
    const f = await fixture();
    try {
      const url = "https://fixture.invalid/reused";
      f.torrents.set(url, makeTorrentBytes("old.mp3"));
      const first = await f.snatch({ providerGuid: "reused", url });
      const oldRecord = f.client.records.get(first.release.info_hash)!;
      oldRecord.snapshot.state = "seeding";
      const replacement = makeTorrentBytes("new.mp3");
      const hash = infoHashFromTorrentBytes(replacement);
      f.torrents.set(url, replacement);
      const second = await f.snatch({ providerGuid: "reused", infoHash: hash, url });
      expect(second.idempotent).toBe(false);
      expect(second.release.info_hash).toBe(hash);
      expect(second.release.id).not.toBe(first.release.id);
      expect(f.repo.findReleaseByProviderGuid("fixture", "reused")?.id).toBe(second.release.id);
      expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(2);
      expect(f.client.adds).toHaveLength(2);
      expect(oldRecord.snapshot.state).toBe("seeding");
      expect(f.client.controls).toHaveLength(0);
      expect(f.requests()).toBe(2);
      runMigrations(f.db);
      expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(2);
    } finally { await f.close(); }
  });

  test("reused GUID with unknown hash refetches a changed torrent at the same URL", async () => {
    const f = await fixture();
    try {
      const url = "https://fixture.invalid/reused-unknown";
      f.torrents.set(url, makeTorrentBytes("first.mp3"));
      const first = await f.snatch({ providerGuid: "unknown-hash", url });
      f.torrents.set(url, makeTorrentBytes("second.mp3"));
      const second = await f.snatch({ providerGuid: "unknown-hash", infoHash: null, url });
      expect(second.idempotent).toBe(false);
      expect(second.release.info_hash).not.toBe(first.release.info_hash);
      expect(f.client.adds).toHaveLength(2);
      expect(f.requests()).toBe(2);
    } finally { await f.close(); }
  });

  test("unknown-hash snatch refreshes stale agent-inspection cache before any release exists", async () => {
    const f = await fixture();
    try {
      const url = "https://fixture.invalid/inspected";
      f.repo.putTorrentCache({ key: torrentCacheKeyFor({ provider: "fixture", providerGuid: "inspected", url }),
        provider: "fixture", providerGuid: "inspected", url,
        infoHash: infoHashFromTorrentBytes(makeTorrentBytes("stale.mp3")), torrentBytes: makeTorrentBytes("stale.mp3"),
      });
      const current = makeTorrentBytes("current.mp3");
      f.torrents.set(url, current);
      const result = await f.snatch({ providerGuid: "inspected", url, infoHash: null });
      expect(result.release.info_hash).toBe(infoHashFromTorrentBytes(current));
      expect(f.client.adds).toEqual([result.release.info_hash]);
      expect(f.requests()).toBe(1);
    } finally { await f.close(); }
  });

  test("an unverified old hash claim cannot hide changed metainfo behind an existing release", async () => {
    const f = await fixture();
    try {
      const url = "https://fixture.invalid/stale-claim";
      f.torrents.set(url, makeTorrentBytes("first.mp3"));
      const first = await f.snatch({ providerGuid: "stale", url });
      const replacement = makeTorrentBytes("replacement.mp3");
      f.torrents.set(url, replacement);
      const second = await f.snatch({ providerGuid: "stale", infoHash: first.release.info_hash, url });
      expect(second.idempotent).toBe(false);
      expect(second.release.info_hash).toBe(infoHashFromTorrentBytes(replacement));
      expect(f.client.adds).toHaveLength(2);
    } finally { await f.close(); }
  });

  test("a new hash claim cannot create a second release when fetched metainfo is unchanged", async () => {
    const f = await fixture();
    try {
      const first = await f.snatch({ providerGuid: "same-content" });
      const second = await f.snatch({ providerGuid: "same-content", infoHash: "a".repeat(40) });
      expect(second.idempotent).toBe(true);
      expect(second.release.id).toBe(first.release.id);
      expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(1);
      expect(f.client.adds).toHaveLength(1);
      expect(f.requests()).toBe(2);
    } finally { await f.close(); }
  });

  test("verified hash ownership cannot be stolen with a reused GUID or forged new hash", async () => {
    const f = await fixture();
    try {
      const first = await f.snatch({ providerGuid: "shared-guid" });
      const other = f.repo.createBook({ title: "Other book", author: "Other" });
      await expect(f.snatch({ bookId: other.id, providerGuid: "shared-guid", infoHash: "a".repeat(40) }))
        .rejects.toThrow("Infohash already linked to book " + f.book.id);
      expect(f.repo.bookTorrents.get(first.release.info_hash)?.book_id).toBe(f.book.id);
      expect(f.repo.listReleasesByBook(other.id)).toHaveLength(0);
      expect(f.client.adds).toHaveLength(1);
      expect(f.client.controls).toHaveLength(0);
    } finally { await f.close(); }
  });

  test("carries ordered manifestation attachment metadata through group snatches", async () => {
    const f = await fixture();
    try {
      const result = await runSnatchGroup(f.repo, f.settings, {
        bookId: f.book.id, mediaType: "audio",
        manifestation: { label: "GraphicAudio dramatization", editionNote: null, selectionNote: "Both ordered parts." },
        parts: [1, 2].map(part => ({
          provider: "fixture", title: "Red Rising Part " + part,
          url: "https://fixture.invalid/part-" + part + ".mp3",
        })),
      }, { bookTorrentClient: f.client });
      expect(result.results).toHaveLength(2);
      expect(f.client.adds).toHaveLength(2);
      expect(f.repo.getManifestation(result.manifestationId!)?.selection_note).toBe("Both ordered parts.");
      const payloads = f.repo.listJobsByType("download").map(job => JSON.parse(job.payload_json!))
        .sort((a, b) => a.sequenceInManifestation - b.sequenceInManifestation);
      expect(payloads.map(p => p.manifestationId)).toEqual([result.manifestationId, result.manifestationId]);
      expect(payloads.map(p => p.sequenceInManifestation)).toEqual([0, 1]);
    } finally { await f.close(); }
  });

  test("publication rollback leaves a durable intent that reuses the external torrent", async () => {
    const f = await fixture();
    try {
      f.db.exec("CREATE TRIGGER fail_download BEFORE INSERT ON jobs WHEN NEW.type = 'download' BEGIN SELECT RAISE(ABORT, 'fixture publication failure'); END");
      await expect(f.snatch()).rejects.toThrow("fixture publication failure");
      const hash = f.client.adds[0]!;
      expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(0);
      expect(f.repo.listManifestationsByBook(f.book.id)).toHaveLength(0);
      expect(f.repo.bookTorrents.get(hash)?.release_id).toBeNull();
      f.db.exec("DROP TRIGGER fail_download");
      const result = await f.snatch();
      expect(f.repo.bookTorrents.get(hash)?.release_id).toBe(result.release.id);
      expect(f.client.adds).toHaveLength(1);
      expect(f.repo.listJobsByType("download")).toHaveLength(1);
    } finally { await f.close(); }
  });

  test("preserves legacy rTorrent releases and removes empty redundant group manifestations", async () => {
    const f = await fixture();
    try {
      const release = f.repo.createRelease({
        bookId: f.book.id, provider: "fixture", providerGuid: "legacy", title: "Red Rising", mediaType: "audio",
        infoHash: infoHashFromTorrentBytes(makeTorrentBytes("legacy")), sizeBytes: 100, url: "https://fixture.invalid/legacy", status: "imported",
      });
      const result = await runSnatchGroup(f.repo, f.settings, {
        bookId: f.book.id, mediaType: "audio",
        manifestation: { label: null, editionNote: null, selectionNote: "Existing edition." },
        parts: [{ provider: "fixture", providerGuid: "legacy", title: "Red Rising", url: release.url }],
      }, { bookTorrentClient: f.client });
      expect(result.results[0]?.idempotent).toBe(true);
      expect(result.manifestationId).toBeNull();
      expect(f.repo.bookTorrents.get(release.info_hash)).toBeNull();
      expect(f.repo.listManifestationsByBook(f.book.id)).toHaveLength(0);
      expect(f.client.adds).toHaveLength(0);
      expect(f.requests()).toBe(1);
    } finally { await f.close(); }
  });
});
