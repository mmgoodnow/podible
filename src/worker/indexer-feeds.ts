import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import { indexerSources } from "../media/indexer-discovery";
import {
  indexerGateway,
  type IndexerMedia,
  type SearchResult,
} from "../media/indexer-gateway";
import type { DiscoveredIndexer } from "../media/prowlarr";
import { IndexerError, indexerFailure } from "../media/indexer-errors";
import { feedIdentity, feedReleaseKey } from "../media/release-inbox";
import { durableStep, type WorkerContext } from "./context";
import { hasWantedBookMedia } from "./book-feeds";

export const INDEXER_FEED_INTERVAL_MS = 15 * 60_000;
const PAGE_SIZE = 100;
const MAX_PAGES = 10;

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
    ? ["movie", "audio", "ebook"]
    : ctx.indexerFeedSources
      ? ["movie"]
      : [
          ...(ctx.repo.movies.hasTargets() ? ["movie" as const] : []),
          ...(hasWantedBookMedia(ctx.repo)
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
      prefix = "feed:" + id;
    const feed = await durableStep(ctx, prefix + ":inventory", () => {
      active();
      return ctx.repo.releaseInbox.ensureFeed(
        id,
        indexerFailure(source.name, null, null).provider,
        media,
        new Date().toISOString(),
      );
    });
    if (feed.retry_at && Date.parse(feed.retry_at) > Date.now()) continue;
    const results: SearchResult<IndexerMedia>[] = [];
    let head: string | null = null,
      covered = feed.head_key === null,
      gap: string | null = null;
    const pageHeads = new Set<string>();
    let offset = 0;
    try {
      for (
        let pageIndex = 0;
        pageIndex < (feed.head_key ? MAX_PAGES : 1);
        pageIndex++
      ) {
        active();
        const page = await durableStep(ctx, prefix + ":page:" + pageIndex, () =>
          gateway.page(source, media, offset, PAGE_SIZE),
        );
        active();
        const first = page.results[0] ? feedReleaseKey(page.results[0]) : null;
        if (pageIndex === 0) head = first;
        results.push(...page.results);
        if (
          !feed.head_key ||
          page.results.some(
            (result) => feedReleaseKey(result) === feed.head_key,
          )
        ) {
          covered = true;
          break;
        }
        if (
          !page.count ||
          (page.total === null
            ? page.count < PAGE_SIZE
            : offset + page.count >= page.total)
        )
          break;
        if (
          source.supportsPagination === false ||
          (first !== null && pageHeads.has(first))
        )
          break;
        if (first) pageHeads.add(first);
        offset += page.count;
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
              type: media === "movie" ? "movie_feed_match" : "book_feed_match",
              payload: { releaseIds: ids },
              idempotencyKey: "rss-match:" + id + ":" + ids.join(","),
              maxAttempts: 3,
            });
          }
          ctx.repo.releaseInbox.succeed(id, head, gap, now, feed.head_key);
        }),
      );
    } catch (error) {
      if (isRunControlError(error)) throw error;
      active();
      if (!(error instanceof IndexerError)) throw error;
      const failure = error.failure;
      // A failed page never advances the watermark or commits a partial batch.
      await durableStep(ctx, prefix + ":failure", () => {
        active();
        ctx.repo.releaseInbox.fail(
          id,
          failure,
          new Date().toISOString(),
          feed.last_success_at,
        );
      });
    }
  }
  return "done";
}
