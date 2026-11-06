import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { discoverCrossSeeds } from "../../src/media/cross-seed-feed";
import { syncIndexerFeeds } from "../../src/worker/indexer-feeds";
import { IndexerGateway } from "../../src/media/indexer-gateway";
import type { MediaType } from "../../src/app-types";
import type { WorkerContext } from "../../src/worker/context";

function fixture() {
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db); repo.ensureSettings();
  const book = repo.createBook({ title: "A retained book", author: "Synthetic Author" });
  for (const media of ["audio", "ebook"] as const) {
    const manifestation = repo.addManifestation({ bookId: book.id, kind: media });
    repo.addAsset({ bookId: book.id, manifestationId: manifestation.id, kind: "single",
      mime: media === "ebook" ? "application/epub+zip" : "audio/mp4", totalSize: 8,
      files: [{ path: `/fixture/${media}`, size: 8, start: 0, end: 8, durationMs: 1000 }] });
  }
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings() };
  const ingest = (media: MediaType) => {
    const feed = repo.releaseInbox.ensureFeed(`book-reuse-${media}`, "Synthetic indexer", media, new Date().toISOString());
    return repo.releaseInbox.ingest(feed.id, [{ title: book.title, provider: "Synthetic indexer", mediaType: media,
      guid: media, url: `https://fixture.invalid/${media}`, infoHash: null, sizeBytes: 8, seeders: 1, leechers: 0, raw: {} }], new Date().toISOString());
  };
  return { db, repo, book, ctx, ingest };
}

test("fully imported books continue polling category feeds and dispatch reuse without starting title searches", async () => {
  const f = fixture();
  try {
    expect(f.repo.getBook(f.book.id)).toMatchObject({ audioStatus: "imported", ebookStatus: "imported" });
    const settings = f.repo.getSettings(); settings.torznab = [{ name: "Synthetic indexer", baseUrl: "https://fixture.invalid/api" }];
    f.repo.updateSettings(settings);
    const categories: string[] = [];
    f.ctx.indexerFeedGateway = new IndexerGateway(async url => {
      expect(url.searchParams.has("q")).toBe(false);
      const category = url.searchParams.get("cat")!; categories.push(category);
      return new Response(`<rss><channel><item><title>A retained book</title><guid>${category}</guid><enclosure url="https://fixture.invalid/${category}" length="8"/></item></channel></rss>`);
    });
    await syncIndexerFeeds(f.ctx);
    expect(categories).toEqual(["3030", "7020"]);
    expect(f.repo.listJobsByType("torrent_reuse_feed")).toHaveLength(2);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
    await syncIndexerFeeds(f.ctx);
    expect(f.repo.listJobsByType("torrent_reuse_feed")).toHaveLength(2);
  } finally { f.db.close(); }
});

test("book reuse routes audio and ebook independently for already-imported books, without another acquisition", async () => {
  const f = fixture();
  try {
    const scopes: string[] = [];
    f.ctx.bookFeedMatcher = async ({ targets }) => targets.map(target => {
      scopes.push(target.media);
      return { bookId: target.bookId, media: target.media, releaseIndexes: [0], reason: "Same book identity" };
    });
    for (const media of ["audio", "ebook"] as const) {
      const job = f.repo.createJob({ type: "torrent_reuse_feed", payload: { releaseIds: f.ingest(media) } });
      await discoverCrossSeeds(f.ctx, job); await discoverCrossSeeds(f.ctx, job);
    }
    expect(new Set(scopes)).toEqual(new Set(["audio", "ebook"]));
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(2);
    expect(f.repo.crossSeed.receipts().map(receipt => receipt.command.scope.media).sort()).toEqual(["audio", "ebook"]);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
    expect(f.repo.listManifestationsByBook(f.book.id)).toHaveLength(2);
  } finally { f.db.close(); }
});

test("book reuse rejects cross-format and invented target matches without queuing torrents", async () => {
  const f = fixture();
  try {
    const job = f.repo.createJob({ type: "torrent_reuse_feed", payload: { releaseIds: f.ingest("audio") } });
    for (const match of [{ bookId: f.book.id, media: "ebook" as const }, { bookId: f.book.id + 1, media: "audio" as const }]) {
      f.ctx.bookFeedMatcher = async () => [{ ...match, releaseIndexes: [0], reason: "Invalid projection" }];
      await expect(discoverCrossSeeds(f.ctx, job)).rejects.toThrow("unknown scope/release");
    }
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(0);
  } finally { f.db.close(); }
});
