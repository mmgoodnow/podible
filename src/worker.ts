import { workerLog, type WorkerContext } from "./worker/context";
import { createJobExecutionRuntime } from "./worker/execution";
import { randomUUID } from "node:crypto";

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
    let nextShadowSync = 0;
    let nextSeerrSync = 0;
    while (!ctx.shouldStop?.()) {
      try {
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
