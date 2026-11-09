import { randomUUID } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, workerLog, type WorkerContext } from "../worker/context";
import { boundedStewardshipFetch, TorrentStewardship, type StewardshipAdapters } from "./torrent-stewardship";
import { processTorrentRetirements } from "./torrent-retirement-worker";
import { processOptimisticCrossSeeds, type CrossSeedMonitorAdapters } from "./cross-seed-monitor";
import { readTransmissionIdentity } from "./managed-transmission";
import { TransmissionRpc } from "./transmission-client";
import { trackerRotationRoots } from "./tracker-rotation";

const payloadSchema = z.object({ operationId: z.string().uuid().optional() }).strict();

/** Parent registers torrent_stewardship_collect and owns the execution lifecycle. */
export async function processStewardshipCollection(ctx: WorkerContext, job: JobRow,
  adapters: StewardshipAdapters & { optimistic?: CrossSeedMonitorAdapters } = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("Torrent stewardship collection requires durable execution");
  const payload = payloadSchema.parse(JSON.parse(job.payload_json ?? "{}"));
  const active = () => { ctx.execution!.assertActive(); adapters.assertActive?.(); };
  active();
  if (ctx.repo.trackerAnnounces.pendingProvider()) {
    await durableStep(ctx, "stewardship:rotate-announces", async () => {
      active();
      const provider = ctx.repo.trackerAnnounces.pendingProvider();
      if (!provider) return;
      try {
        const identity = readTransmissionIdentity();
        const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username,
          password: identity.password, timeoutMs: 5000, fetcher: boundedStewardshipFetch });
        const result = await ctx.repo.trackerAnnounces.reconcile(provider, {
          rpc, clientId: `transmission:${identity.id}`, roots: trackerRotationRoots(ctx.getSettings()),
        });
        if (!result) ctx.repo.trackerAnnounces.defer(provider);
        else if (result.cached || result.registered) workerLog(ctx,
          `[stewardship] Rotated tracker announces: cache=${result.cached} registrations=${result.registered}`);
      } catch {
        active();
        ctx.repo.trackerAnnounces.defer(provider);
        workerLog(ctx, "[stewardship] Tracker announce rotation deferred; will retry on the next collection.");
      }
    });
  }
  const operationId = await durableStep(ctx, "stewardship:operation", () => payload.operationId ?? randomUUID());
  active();
  const result = await durableStep(ctx, "stewardship:collect", () =>
    new TorrentStewardship(ctx.repo, { ...adapters, assertActive: active }).collect({ operationId }));
  active();
  if (result.unavailableReason)
    workerLog(ctx, `[stewardship] ${result.unavailableReason} Observation will retry on the next scheduled collection.`);
  await processTorrentRetirements(ctx, job, { source: result.source, offset: result.offset }, adapters.retirement ?? {});
  await processOptimisticCrossSeeds(ctx, adapters.optimistic);
  return "done";
}
