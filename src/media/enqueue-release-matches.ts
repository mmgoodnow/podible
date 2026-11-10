import type { BooksRepo } from "../repo";
import type { IndexerMedia } from "./indexer-gateway";

/** Call in the same transaction that inserts the releases. */
export function enqueueReleaseMatches(repo: BooksRepo, sourceId: string, media: IndexerMedia, ids: number[]) {
  for (let start = 0; start < ids.length; start += 100) {
    const releaseIds = ids.slice(start, start + 100);
    const key = sourceId + ":" + releaseIds.join(",");
    repo.createJob({
      type: media === "movie" ? "movie_feed_match" : media === "tv" ? "tv_feed_match" : "book_feed_match",
      payload: { releaseIds }, idempotencyKey: "rss-match:" + key, maxAttempts: 3,
    });
    repo.createJob({
      type: "torrent_reuse_feed", payload: { releaseIds }, idempotencyKey: "rss-reuse:" + key, maxAttempts: 3,
    });
  }
}
