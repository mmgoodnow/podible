import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { feedIdentity, feedReleaseKey } from "../../src/media/release-inbox";
import { IndexerError, indexerFailure } from "../../src/media/indexer-errors";
import { readFeedGapDiagnostics } from "../../src/media/feed-gap";
import type { SearchResult } from "../../src/media/indexer-gateway";
import type { WorkerContext } from "../../src/worker/context";
import { syncIndexerFeeds } from "../../src/worker/indexer-feeds";

const source = { name: "Gap fixture", baseUrl: "https://fixture.invalid/api", apiKey: "private-key", supportsPagination: true };
function result(key: string): SearchResult<"movie"> {
  return { title: key, provider: source.name, mediaType: "movie", sizeBytes: 1, url: `https://fixture.invalid/${key}`,
    guid: key, infoHash: null, seeders: null, leechers: null, raw: {} };
}
function setup() {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db); repo.ensureSettings();
  const id = feedIdentity(source, "movie"), before = "2026-11-01T00:00:00.000Z";
  repo.releaseInbox.ensureFeed(id, source.name, "movie", before);
  repo.releaseInbox.succeed(id, feedReleaseKey(result("old")), null, before);
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings(), indexerFeedSources: async () => [source] };
  return { db, repo, ctx, id, before };
}

for (const reason of ["history-exhausted", "pagination-unsupported", "repeated-page", "page-budget-exhausted"] as const) {
  test(`gap records observed ${reason} without claiming why old entries disappeared`, async () => {
    const f = setup(); let calls = 0;
    try {
      if (reason === "pagination-unsupported") f.ctx.indexerFeedSources = async () => [{ ...source, supportsPagination: false }];
      f.ctx.indexerFeedGateway = { page: async (_source, _media, offset) => {
        calls++;
        const short = reason === "history-exhausted", repeated = reason === "repeated-page";
        return { offset: offset!, total: short ? 2 : 2000, count: short ? 2 : 100,
          results: Array.from({ length: short ? 2 : 100 }, (_, n) => ({ ...result(String((repeated ? 0 : offset!) + n)), mediaType: _media })) };
      } };
      await syncIndexerFeeds(f.ctx);
      const feed = f.repo.releaseInbox.feed(f.id)!;
      const diagnostic = readFeedGapDiagnostics(feed.gap_diagnostics_json)!;
      expect(diagnostic).toMatchObject({ reason, previousSuccessAt: f.before, previousPollAt: f.before,
        previousPollFailed: false, anchorsExpected: 1, pageSize: 100, pageBudget: 10, pagesFetched: calls });
      expect(calls).toBe(reason === "page-budget-exhausted" ? 10 : reason === "repeated-page" ? 2 : 1);
      expect(diagnostic.resultsFetched).toBe(reason === "history-exhausted" ? 2 : calls * 100);
      expect(diagnostic.distinctResults).toBe(reason === "repeated-page" ? 100 : diagnostic.resultsFetched);
      expect(JSON.stringify(diagnostic)).not.toMatch(/private-key|https:|deleted|downtime/);
      expect(feed.gap_since).toBe(diagnostic.detectedAt);
      // Later successful overlap retains historical evidence, not a newly fabricated gap.
      f.ctx.indexerFeedGateway = { page: async (_source, media) => ({ offset: 0, total: 1, count: 1, results: [{ ...result("0"), mediaType: media }] }) };
      await syncIndexerFeeds(f.ctx);
      expect(f.repo.releaseInbox.feed(f.id)!.gap_diagnostics_json).toBe(feed.gap_diagnostics_json);
    } finally { f.db.close(); }
  });
}

test("a failed page preserves checkpoint and prior diagnostic; a later gap records the preceding failure", async () => {
  const f = setup();
  try {
    f.ctx.indexerFeedGateway = { page: async () => { throw new IndexerError(indexerFailure(source.name, 503, Date.now() - 1000)); } };
    await syncIndexerFeeds(f.ctx);
    const failed = f.repo.releaseInbox.feed(f.id)!;
    expect(failed.head_key).toBe(feedReleaseKey(result("old")));
    expect(failed.gap_diagnostics_json).toBeNull();
    expect(failed.gap_since).toBeNull();
    f.ctx.indexerFeedGateway = { page: async () => ({ offset: 0, total: 0, count: 0, results: [] }) };
    await syncIndexerFeeds(f.ctx);
    const diagnostic = readFeedGapDiagnostics(f.repo.releaseInbox.feed(f.id)!.gap_diagnostics_json)!;
    expect(diagnostic).toMatchObject({ previousPollAt: failed.last_polled_at, previousPollFailed: true,
      previousSuccessAt: f.before, reason: "history-exhausted" });
  } finally { f.db.close(); }
});

test("legacy, malformed and oversized diagnostics stay unknown instead of inventing a cause", () => {
  for (const value of [null, undefined, "{}", "not json", JSON.stringify({ reason: "deleted" }), " ".repeat(5000)])
    expect(readFeedGapDiagnostics(value)).toBeNull();
});
