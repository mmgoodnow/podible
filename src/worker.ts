import { workerLog, type WorkerContext } from "./worker/context";
import { createJobExecutionRuntime } from "./worker/execution";

export { pollMsForMedia, selectDownloadPollMs } from "./worker/downloads";

/** Bridge the public job outbox to Reflow. No job claiming or startup resets. */
export async function runWorker(
  ctx: WorkerContext,
  options: Parameters<typeof createJobExecutionRuntime>[2] & { databasePath?: string } = {}
): Promise<void> {
  const runtime = await createJobExecutionRuntime(
    ctx,
    options.databasePath ?? ctx.repo.executionDatabasePath(),
    options
  );
  try {
    while (!ctx.shouldStop?.()) {
      try {
        await runtime.reconcile();
      } catch (error) {
        workerLog(ctx, `[execution] outbox reconciliation failed: ${String(error)}`);
      }
      await Bun.sleep(100);
    }
  } finally {
    await runtime.stop();
  }
}
