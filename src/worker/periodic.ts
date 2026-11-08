import type { JobRow, JobType } from "../app-types";
import type { BooksRepo } from "../repo";

const prefixes: Partial<Record<JobType, string | string[]>> = {
  torrent_metrics_collect: "torrent-metrics:", torrent_stewardship_collect: "torrent-stewardship:",
  reconcile: "book-maintenance:", indexer_feed_sync: "indexer-feeds:",
  movie_dual_write_sync: "movie-seerr:", tv_shadow_sync: "tv-shadow:", tv_sync: ["tv-seerr:", "tv-native:"],
  tv_reconcile: "tv-maintenance:", movie_shadow_sync: "movie-shadow-sync:", movie_reconcile: "movie-maintenance:",
  tv_retention_pressure: "tv-retention-pressure:", retention_cleanup: "retention-pressure:",
};
export function periodicPrefix(job: Pick<JobRow, "type" | "payload_json" | "book_id" | "release_id"> & { dedupe_key?: string | null }): string | null {
  const candidates = prefixes[job.type];
  const prefix = (Array.isArray(candidates) ? candidates : candidates ? [candidates] : [])
    .find(value => job.dedupe_key?.startsWith(value));
  return prefix && !job.book_id && !job.release_id &&
    (job.payload_json === null || job.payload_json === "{}") ? prefix : null;
}

/** Polling is a latest-state observation, not one obligation per missed wall-clock interval. */
export function queuePeriodicJob(repo: BooksRepo, input: Parameters<BooksRepo["createJob"]>[0]) {
  const prefix = periodicPrefix({ type: input.type, dedupe_key: input.idempotencyKey ?? null,
    payload_json: input.payload === undefined ? null : JSON.stringify(input.payload), book_id: input.bookId ?? null, release_id: input.releaseId ?? null });
  const previous = prefix ? repo.latestJobByDedupePrefix(input.type, prefix) : null;
  return previous && ["queued", "running"].includes(previous.status) ? previous : repo.createJob(input);
}
