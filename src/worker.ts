import { workerLog, type WorkerContext } from "./worker/context";
import { createJobExecutionRuntime } from "./worker/execution";
import { randomUUID, createHash } from "node:crypto";
import { INDEXER_FEED_INTERVAL_MS } from "./worker/indexer-feeds";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

/** Bridge the public job outbox to Reflow. No job claiming or startup resets. */
export async function runWorker(
  ctx: WorkerContext,
  options: Parameters<typeof createJobExecutionRuntime>[2] & {
    databasePath?: string;
    runtime?: Awaited<ReturnType<typeof createJobExecutionRuntime>>;
  } = {},
): Promise<void> {
  const runtime =
    options.runtime ??
    (await createJobExecutionRuntime(
      ctx,
      options.databasePath ?? ctx.repo.executionDatabasePath(),
      options,
    ));
  try {
    const boot = randomUUID();
    let nextMovieMaintenance = 0;
    let nextBookMaintenance = 0;
    let nextShadowSync = 0;
    let nextSeerrSync = 0;
    let nextFeedSync = 0;
    while (!ctx.shouldStop?.()) {
      try {
        if (ctx.repo.hasBooks() && Date.now() >= nextBookMaintenance) {
          ctx.repo.createJob({
            type: "reconcile",
            idempotencyKey: `book-maintenance:${boot}:${Math.floor(Date.now() / 3_600_000)}`,
            maxAttempts: 2,
          });
          nextBookMaintenance = Date.now() + 3_600_000;
        }
        const prowlarr = ctx.getSettings().prowlarr;
        const feedSettings = ctx.getSettings();
        if ((prowlarr.url || feedSettings.torznab.length) &&
            (ctx.repo.movies.hasTargets() || ctx.repo.hasBooks()) && Date.now() >= nextFeedSync) {
          const key = createHash("sha256").update(JSON.stringify([prowlarr, feedSettings.torznab])).digest("hex");
          ctx.repo.createJob({
            type: "indexer_feed_sync",
            idempotencyKey: "indexer-feeds:" + key + ":" + Math.floor(Date.now() / INDEXER_FEED_INTERVAL_MS),
            maxAttempts: 3,
          });
          nextFeedSync = Date.now() + INDEXER_FEED_INTERVAL_MS;
        }
        const seerr = ctx.getSettings().movies.seerr;
        if (seerr.url && seerr.apiKey && Date.now() >= nextSeerrSync) {
          ctx.repo.createJob({ type: "movie_dual_write_sync", idempotencyKey: `movie-seerr:${seerr.url}:${Math.floor(Date.now()/(seerr.pollIntervalMinutes*60_000))}`, maxAttempts: 3 });
          nextSeerrSync = Date.now() + seerr.pollIntervalMinutes * 60_000;
        }
        const shadow = ctx.getSettings().movies.shadow;
        if (shadow.enabled && Date.now() >= nextShadowSync) {
          ctx.repo.createJob({
            type: "movie_shadow_sync",
            idempotencyKey: `movie-shadow-sync:${Math.floor(Date.now() / (shadow.pollIntervalMinutes * 60_000))}`,
            maxAttempts: 3,
          });
          nextShadowSync = Date.now() + shadow.pollIntervalMinutes * 60_000;
        }
        if (
          ctx.repo.movies.hasTargets() &&
          Date.now() >= nextMovieMaintenance
        ) {
          ctx.repo.createJob({
            type: "movie_reconcile",
            idempotencyKey: `movie-maintenance:${boot}:${Math.floor(Date.now() / 3_600_000)}`,
            maxAttempts: 2,
          });
          nextMovieMaintenance = Date.now() + 3_600_000;
        }
        await runtime.reconcile();
      } catch (error) {
        workerLog(
          ctx,
          `[execution] outbox reconciliation failed: ${String(error)}`,
        );
      }
      await Bun.sleep(100);
    }
  } finally {
    await runtime.stop();
  }
}
