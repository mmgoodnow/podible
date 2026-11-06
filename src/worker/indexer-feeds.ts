import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import { indexerSources } from "../media/indexer-discovery";
import {
  indexerGateway,
  type IndexerMedia,
  type SearchResult,
} from "../media/indexer-gateway";
import type { DiscoveredIndexer } from "../media/prowlarr";
import { IndexerError, indexerFailure, type IndexerFailure } from "../media/indexer-errors";
import { boundedFeedBoundary, feedIdentity, feedReleaseKey } from "../media/release-inbox";
import { durableStep, type WorkerContext } from "./context";
import type { FeedGapDiagnostics } from "../media/feed-gap";

export const INDEXER_FEED_INTERVAL_MS = 15 * 60_000;
const PAGE_SIZE = 100;
const MAX_PAGES = 10;

type FeedPage = Awaited<ReturnType<typeof indexerGateway.page>>;
type FeedPageResult =
  | { kind: "success"; page: FeedPage }
  | { kind: "failure"; failure: IndexerFailure };

export type IndexerFeedContext = WorkerContext & {
  indexerFeedMediaSources?: (
    media: IndexerMedia,
  ) => Promise<DiscoveredIndexer[]>;
};

/** One category-scoped recent-release feed per provider/media, never scheduled title searches. */
export async function syncIndexerFeeds(
  ctx: IndexerFeedContext,
): Promise<"done"> {
  const settingsKey = () =>
    createHash("sha256")
      .update(
        JSON.stringify([ctx.getSettings().prowlarr, ctx.getSettings().torznab]),
      )
      .digest("hex");
  const configuration = await durableStep(
    ctx,
    "feeds:configuration",
    settingsKey,
  );
  const active = () => {
    ctx.execution?.assertActive();
    if (settingsKey() !== configuration)
      throw new Error("Indexer configuration changed; start a new feed sync");
  };
  active();
  // Preserve the legacy no-argument injection as movie-only. Typed injection
  // owns all media and avoids accidental real-network calls in movie fixtures.
  const media: IndexerMedia[] = ctx.indexerFeedMediaSources
    ? ["movie", "tv", "audio", "ebook"]
    : ctx.indexerFeedSources
      ? ["movie"]
      : [
          ...(ctx.repo.movies.hasTargets() ? ["movie" as const] : []),
          ...(ctx.repo.tv.hasTargets() ? ["tv" as const] : []),
          ...(ctx.repo.hasBooks()
            ? ["audio" as const, "ebook" as const]
            : []),
        ];
  const inventory: { source: DiscoveredIndexer; media: IndexerMedia }[] = [];
  for (const kind of media) {
    const sources = ctx.indexerFeedMediaSources
      ? await ctx.indexerFeedMediaSources(kind)
      : ctx.indexerFeedSources
        ? await ctx.indexerFeedSources()
        : await indexerSources(ctx.getSettings(), kind, "rss");
    for (const source of sources)
      inventory.push({
        source: {
          ...source,
          categories: { audio: "3030", ebook: "7020", ...source.categories },
        },
        media: kind,
      });
  }
  const gateway = ctx.indexerFeedGateway ?? indexerGateway;
  for (const { source, media } of inventory) {
    const id = feedIdentity(source, media),
      prefix = "feed:" + id + ":boundary-v2";
    const feed = await durableStep(ctx, prefix + ":inventory", () => {
      active();
      const snapshot = ctx.repo.releaseInbox.ensureFeed(
        id,
        indexerFailure(source.name, null, null).provider,
        media,
        new Date().toISOString(),
      );
      return { ...snapshot, boundaryKeys: ctx.repo.releaseInbox.boundaryKeys(snapshot) };
    });
    if (feed.retry_at && Date.parse(feed.retry_at) > Date.now()) continue;
    const results: SearchResult<IndexerMedia>[] = [];
    const anchors = new Set(feed.boundaryKeys);
    let head: string | null = null,
      covered = anchors.size === 0,
      gap: string | null = null;
    const pageHeads = new Set<string>();
    let offset = 0;
    let pagesFetched = 0, reportedTotal: number | null = null;
    let stopReason: FeedGapDiagnostics["reason"] = "page-budget-exhausted";
    let failure: IndexerFailure | null = null;
    for (
      let pageIndex = 0;
      pageIndex < (anchors.size ? MAX_PAGES : 1);
      pageIndex++
    ) {
      active();
      // Expected provider failures must be values before Reflow sees them:
      // a thrown IndexerError becomes step retries/control/serialized errors.
      const outcome = await durableStep<FeedPageResult | FeedPage>(
        ctx,
        prefix + ":page:" + pageIndex,
        async (): Promise<FeedPageResult> => {
          active();
          try {
            const page = await gateway.page(source, media, offset, PAGE_SIZE);
            active();
            return { kind: "success", page };
          } catch (error) {
            if (isRunControlError(error)) throw error;
            active();
            if (!(error instanceof IndexerError)) throw error;
            // Only the gateway's safe metadata, never the Error/cause/body.
            const { provider, kind, status, retryAt, message } = error.failure;
            return {
              kind: "failure",
              failure: { provider, kind, status, retryAt, message },
            };
          }
        },
      );
      active();
      if ("kind" in outcome && outcome.kind === "failure") {
        failure = outcome.failure;
        break;
      }
      // Retain completed page checkpoints written before result envelopes.
      const page = "kind" in outcome ? outcome.page : outcome;
      pagesFetched++;
      reportedTotal = page.total;
      const first = page.results[0] ? feedReleaseKey(page.results[0]) : null;
      if (pageIndex === 0) head = first;
      results.push(...page.results);
      if (
        !anchors.size ||
        page.results.some((result) => anchors.has(feedReleaseKey(result)))
      ) {
        covered = true;
        break;
      }
      if (
        !page.count ||
        (page.total === null
          ? page.count < PAGE_SIZE
          : offset + page.count >= page.total)
      ) {
        stopReason = "history-exhausted";
        break;
      }
      if (source.supportsPagination === false) {
        stopReason = "pagination-unsupported";
        break;
      }
      if (first !== null && pageHeads.has(first)) {
        stopReason = "repeated-page";
        break;
      }
      if (first) pageHeads.add(first);
      offset += page.count;
    }
    if (failure) {
      // Discard every buffered page; no watermark or partial batch commits.
      await durableStep(ctx, prefix + ":failure", () => {
        active();
        ctx.repo.releaseInbox.fail(
          id,
          failure,
          new Date().toISOString(),
          feed.last_success_at,
        );
      });
      continue;
    }
    if (!covered)
      gap =
        "The previous feed boundary was not found within the available/bounded feed history. Some releases may have been missed; no per-title search was started.";
    await durableStep(ctx, prefix + ":persist", () =>
      ctx.repo.transaction(() => {
        active();
        const now = new Date().toISOString();
        const inserted = ctx.repo.releaseInbox.ingest(id, results, now);
        for (let start = 0; start < inserted.length; start += PAGE_SIZE) {
          const ids = inserted.slice(start, start + PAGE_SIZE);
          ctx.repo.createJob({
            type: media === "movie" ? "movie_feed_match" : media === "tv" ? "tv_feed_match" : "book_feed_match",
            payload: { releaseIds: ids },
            idempotencyKey: "rss-match:" + id + ":" + ids.join(","),
            maxAttempts: 3,
          });
          ctx.repo.createJob({
            type: "torrent_reuse_feed", payload: { releaseIds: ids },
            idempotencyKey: "rss-reuse:" + id + ":" + ids.join(","), maxAttempts: 3,
          });
        }
        const boundary = boundedFeedBoundary([...results.map(feedReleaseKey), ...feed.boundaryKeys]);
        const diagnostics: FeedGapDiagnostics | null = gap ? {
          version: 1, detectedAt: now, reason: stopReason,
          previousSuccessAt: feed.last_success_at, previousPollAt: feed.last_polled_at,
          previousPollFailed: feed.last_error !== null,
          pagesFetched, resultsFetched: results.length, distinctResults: new Set(results.map(feedReleaseKey)).size,
          anchorsExpected: anchors.size, pageSize: PAGE_SIZE, pageBudget: MAX_PAGES,
          supportsPagination: source.supportsPagination ?? null, reportedTotal,
        } : null;
        ctx.repo.releaseInbox.succeed(id, head, gap, now, feed.head_key, boundary, feed.boundary_json, diagnostics);
      }),
    );
  }
  return "done";
}
