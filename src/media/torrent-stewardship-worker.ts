import { randomUUID } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, workerLog, type WorkerContext } from "../worker/context";
import { TorrentStewardship, type StewardshipAdapters } from "./torrent-stewardship";
import { processTorrentRetirements } from "./torrent-retirement-worker";

const payloadSchema = z.object({ operationId: z.string().uuid().optional() }).strict();

/** Parent registers torrent_stewardship_collect; this handler never owns execution lifecycle or controls torrents. */
export async function processStewardshipCollection(ctx: WorkerContext, job: JobRow,
  adapters: StewardshipAdapters = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("Torrent stewardship collection requires durable execution");
  const payload = payloadSchema.parse(JSON.parse(job.payload_json ?? "{}"));
  const active = () => { ctx.execution!.assertActive(); adapters.assertActive?.(); };
  active();
  const operationId = await durableStep(ctx, "stewardship:operation", () => payload.operationId ?? randomUUID());
  active();
  const result = await durableStep(ctx, "stewardship:collect", () =>
    new TorrentStewardship(ctx.repo, { ...adapters, assertActive: active }).collect({ operationId }));
  active();
  if (result.unavailableReason)
    workerLog(ctx, `[stewardship] ${result.unavailableReason} Observation will retry on the next scheduled collection.`);
  await processTorrentRetirements(ctx, job, { source: result.source, offset: result.offset }, adapters.retirement ?? {});
  return "done";
}
