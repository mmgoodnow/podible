import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import { indexerSources } from "../media/indexer-discovery";
import { indexerGateway, type SearchResult } from "../media/indexer-gateway";
import { IndexerError, indexerFailure } from "../media/indexer-errors";
import { feedIdentity, feedReleaseKey } from "../media/release-inbox";
import { durableStep, type WorkerContext } from "./context";

export const INDEXER_FEED_INTERVAL_MS = 15 * 60_000;
const PAGE_SIZE = 100;
const MAX_PAGES = 10;

/** One recent-release feed per provider, never a scheduled search per movie. */
export async function syncIndexerFeeds(ctx: WorkerContext): Promise<"done"> {
  const settingsKey = () =>
    createHash("sha256")
      .update(JSON.stringify(ctx.getSettings().prowlarr))
      .digest("hex");
  const configuration = await durableStep(
    ctx,
    "feeds:configuration",
    settingsKey,
  );
  const active = () => {
    ctx.execution?.assertActive();
    if (settingsKey() !== configuration)
      throw new Error("Prowlarr configuration changed; start a new feed sync");
  };
  active();
  const sources = ctx.indexerFeedSources
    ? await ctx.indexerFeedSources()
    : await indexerSources(ctx.getSettings(), "movie", "rss");
  const gateway = ctx.indexerFeedGateway ?? indexerGateway;
  for (const source of sources) {
    const id = feedIdentity(source, "movie"),
      prefix = "feed:" + id;
    const feed = await durableStep(ctx, prefix + ":inventory", () => {
      active();
      return ctx.repo.releaseInbox.ensureFeed(
        id,
        indexerFailure(source.name, null, null).provider,
        "movie",
        new Date().toISOString(),
      );
    });
    if (feed.retry_at && Date.parse(feed.retry_at) > Date.now()) continue;
    const results: SearchResult<"movie">[] = [];
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
          gateway.page(source, "movie", offset, PAGE_SIZE),
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
              type: "movie_feed_match",
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
