import { isMainThread, parentPort, threadId, workerData } from "node:worker_threads";
import { openDatabase } from "../db";
import { BooksRepo } from "../repo";
import { runWorker } from "../worker";
import { createJobExecutionRuntime } from "../worker/execution";
import { queueStaleMetadataHydration } from "../worker/metadata-hydration";
import { watermarkLegacyGeneratedCovers } from "../library/cover-generation";
import { queuePendingCurationJobs } from "../library/chapter-analysis";
import type { BackgroundWorkerInput, BackgroundWorkerMessage } from "./background-protocol";

async function startupMaintenance(repo: BooksRepo, stopping: () => boolean) {
  const watermarks = await watermarkLegacyGeneratedCovers(repo);
  if (watermarks.updated || watermarks.failed)
    console.log(`[cover-generation] legacy watermarks updated=${watermarks.updated} failed=${watermarks.failed}`);
  if (stopping()) return;
  const hydration = queueStaleMetadataHydration(repo);
  if (hydration) console.log(`[metadata-hydration] job=${hydration.id} status=${hydration.status}`);
  if (stopping()) return;
  const count = await queuePendingCurationJobs(repo);
  if (count) console.log(`[chapter-analysis] queued curation for ${count} already-transcribed book(s)`);
}

/** Local fixture hooks are functions loaded inside the worker, not serialized job executors. */
export async function bootBackgroundWorker(options: {
  startupMaintenance?: boolean;
  execution?: Parameters<typeof createJobExecutionRuntime>[2];
} = {}): Promise<void> {
  if (isMainThread || !parentPort) throw new Error("Background bootstrap must run in a Bun Worker thread");
  const data = workerData as BackgroundWorkerInput;
  if (!data || typeof data.databasePath !== "string" || Object.keys(data).some(key => key !== "databasePath"))
    throw new Error("Invalid background worker input");
  const port = parentPort;
  let stopping = false;
  port.on("message", message => { if (message?.type === "stop") stopping = true; });
  const post = (message: BackgroundWorkerMessage) => port.postMessage(message);
  let db: ReturnType<typeof openDatabase> | undefined;
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let loopOwnsRuntime = false;
  let maintenance: Promise<void> | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  try {
    db = openDatabase(data.databasePath);
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const context = { repo, getSettings: () => repo.getSettings(), shouldStop: () => stopping,
      onLog: (message: string) => console.log(message), activeLocks: new Set<string>() };
    runtime = await createJobExecutionRuntime(context, repo.executionDatabasePath(), options.execution);
    if (stopping) return;
    post({ type: "ready", pid: process.pid, threadId,
      busyTimeoutMs: (db.query("PRAGMA busy_timeout").get() as { timeout: number }).timeout });
    heartbeat = setInterval(() => {
      const diagnostics = runtime!.diagnostics();
      post({ type: "heartbeat", stalledEffects: diagnostics.effects.filter(effect => effect.stalled).length,
        activeEffects: diagnostics.effects.length, reservations: diagnostics.reservations });
    }, 1000);
    loopOwnsRuntime = true;
    const loop = runWorker(context, { runtime });
    maintenance = options.startupMaintenance === false ? Promise.resolve()
      : startupMaintenance(repo, () => stopping).catch(error => { console.error("[background] startup maintenance failed", error); });
    await loop;
  } catch (error) {
    post({ type: "failed" });
    throw error;
  } finally {
    stopping = true;
    clearInterval(heartbeat);
    if (runtime && !loopOwnsRuntime) await runtime.stop();
    await maintenance;
    db?.close();
    post({ type: "stopped" });
    port.close();
  }
}
