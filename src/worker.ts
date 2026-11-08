import { workerLog, type WorkerContext } from "./worker/context";
import { createJobExecutionRuntime } from "./worker/execution";
import { randomUUID, createHash } from "node:crypto";
import { INDEXER_FEED_INTERVAL_MS } from "./worker/indexer-feeds";
import { RETENTION_PRESSURE_INTERVAL_MS } from "./media/retention-policy-types";
import { STEWARDSHIP_INTERVAL_MS } from "./media/torrent-stewardship-types";
import { hasTorrentStewardshipBindings } from "./media/torrent-stewardship";
import { TRANSFER_METRICS_INTERVAL_MS } from "./media/transfer-metrics-types";
import { queuePeriodicJob } from "./worker/periodic";

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
    const schedule = (input: Parameters<typeof ctx.repo.createJob>[0]) => queuePeriodicJob(ctx.repo, input);
    const boot = randomUUID();
    let nextMovieMaintenance = 0;
    let nextBookMaintenance = 0;
    let nextShadowSync = 0;
    let nextSeerrSync = 0;
    let nextFeedSync = 0;
    let nextTvSync = 0;
    let nextTvMaintenance = 0;
    let nextRetention = 0;
    let nextTvRetention = 0;
    let nextStewardship = 0;
    let nextTransferMetrics = 0;
    while (!ctx.shouldStop?.()) {
      try {
        if (Date.now() >= nextTransferMetrics) {
          schedule({ type: "torrent_metrics_collect",
            idempotencyKey: `torrent-metrics:${Math.floor(Date.now() / TRANSFER_METRICS_INTERVAL_MS)}`, maxAttempts: 2 });
          nextTransferMetrics = Date.now() + TRANSFER_METRICS_INTERVAL_MS;
        }
        if (Date.now() >= nextStewardship) {
          if (hasTorrentStewardshipBindings(ctx.repo)) schedule({
              type: "torrent_stewardship_collect",
              idempotencyKey: `torrent-stewardship:${Math.floor(Date.now() / STEWARDSHIP_INTERVAL_MS)}`,
              maxAttempts: 3,
            });
          nextStewardship = Date.now() + STEWARDSHIP_INTERVAL_MS;
        }
        if (ctx.repo.hasBooks() && Date.now() >= nextBookMaintenance) {
          schedule({
            type: "reconcile",
            idempotencyKey: `book-maintenance:${boot}:${Math.floor(Date.now() / 3_600_000)}`,
            maxAttempts: 2,
          });
          nextBookMaintenance = Date.now() + 3_600_000;
        }
        const prowlarr = ctx.getSettings().prowlarr;
        const feedSettings = ctx.getSettings();
        if ((prowlarr.url || feedSettings.torznab.length) &&
            (ctx.repo.movies.hasTargets() || ctx.repo.tv.hasTargets() || ctx.repo.hasBooks()) && Date.now() >= nextFeedSync) {
          const key = createHash("sha256").update(JSON.stringify([prowlarr, feedSettings.torznab])).digest("hex");
          schedule({
            type: "indexer_feed_sync",
            idempotencyKey: "indexer-feeds:" + key + ":" + Math.floor(Date.now() / INDEXER_FEED_INTERVAL_MS),
            maxAttempts: 3,
          });
          nextFeedSync = Date.now() + INDEXER_FEED_INTERVAL_MS;
        }
        const seerr = ctx.getSettings().movies.seerr;
        if (seerr.url && seerr.apiKey && Date.now() >= nextSeerrSync) {
          schedule({ type: "movie_dual_write_sync", idempotencyKey: `movie-seerr:${seerr.url}:${Math.floor(Date.now()/(seerr.pollIntervalMinutes*60_000))}`, maxAttempts: 3 });
          nextSeerrSync = Date.now() + seerr.pollIntervalMinutes * 60_000;
        }
        const shadow = ctx.getSettings().movies.shadow;
        const tv = ctx.getSettings().tv;
        if (Date.now() >= nextTvSync) {
          const sonarrConfigured = Boolean(tv.sonarrUrl && tv.sonarrApiKey);
          const nativeConfigured = Boolean(ctx.getSettings().movies.tmdbAccessToken.trim()) &&
            ctx.repo.tv.requests().some((request) => !request.withdrawn_at && request.request_key.startsWith("native-seasons:"));
          const slot = Math.floor(Date.now() / (tv.pollIntervalMinutes * 60_000));
          if (sonarrConfigured)
            schedule({ type: "tv_shadow_sync", idempotencyKey: `tv-shadow:${slot}`, maxAttempts: 3 });
          const seerrConfigured = Boolean(sonarrConfigured && seerr.url && seerr.apiKey && tv.managedRoot && tv.plexSectionId && tv.plexViewPath);
          if (seerrConfigured || nativeConfigured)
            schedule({ type: "tv_sync", idempotencyKey: seerrConfigured ? `tv-seerr:${seerr.url}:${slot}` : `tv-native:${slot}`, maxAttempts: 3 });
          if (sonarrConfigured || nativeConfigured) nextTvSync = Date.now() + tv.pollIntervalMinutes * 60_000;
        }
        if (ctx.repo.tv.hasTargets() && tv.managedRoot && tv.plexSectionId && tv.plexViewPath && Date.now() >= nextTvMaintenance) {
          schedule({ type: "tv_reconcile", idempotencyKey: `tv-maintenance:${boot}:${Math.floor(Date.now() / 3_600_000)}`, maxAttempts: 2 });
          nextTvMaintenance = Date.now() + 3_600_000;
        }
        if (shadow.enabled && Date.now() >= nextShadowSync) {
          schedule({
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
          schedule({
            type: "movie_reconcile",
            idempotencyKey: `movie-maintenance:${boot}:${Math.floor(Date.now() / 3_600_000)}`,
            maxAttempts: 2,
          });
          nextMovieMaintenance = Date.now() + 3_600_000;
        }
        const movieStorage = ctx.getSettings().movies;
        if (ctx.repo.tv.hasTargets() && tv.managedRoot && tv.plexViewPath && tv.plexSectionId && Date.now() >= nextTvRetention) {
          schedule({ type: "tv_retention_pressure", idempotencyKey: `tv-retention-pressure:${Math.floor(Date.now() / RETENTION_PRESSURE_INTERVAL_MS)}`, maxAttempts: 3 });
          nextTvRetention = Date.now() + RETENTION_PRESSURE_INTERVAL_MS;
        }
        if (ctx.repo.movies.hasTargets() && movieStorage.managedRoot && movieStorage.plexViewPath && Date.now() >= nextRetention) {
          schedule({
            type: "retention_cleanup",
            idempotencyKey: `retention-pressure:${Math.floor(Date.now() / RETENTION_PRESSURE_INTERVAL_MS)}`,
            maxAttempts: 3,
          });
          nextRetention = Date.now() + RETENTION_PRESSURE_INTERVAL_MS;
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
