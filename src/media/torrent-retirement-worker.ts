import { randomUUID } from "node:crypto";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { stewardshipBindingPage } from "./torrent-stewardship";
import { privateRetirementDaemon } from "./torrent-retirement-transmission";
import { TorrentRetirement, type TorrentRetirementAdapters } from "./torrent-retirement";
import { createOrphanRetirementFence } from "./torrent-retirement-admission";

/** Bounded orphan-only hook; the concrete default claims only inside its own globally locked durable step. */
export async function processTorrentRetirements(ctx: WorkerContext, job: JobRow,
  batch: { source: "books" | "screen"; offset: number }, adapters: TorrentRetirementAdapters = {}): Promise<void> {
  if (!ctx.execution || adapters.daemon === null) return;
  ctx.execution.assertActive();
  const fence = adapters.fence ?? createOrphanRetirementFence(ctx.repo, { globalMaintenanceHeld: () => ctx.activeLocks?.has("*") === true,
    readIdentityDigest: adapters.readIdentityDigest });
  let daemon;
  try { daemon = adapters.daemon ?? privateRetirementDaemon(); } catch { return; }
  const service = new TorrentRetirement(ctx.repo, { ...adapters, fence, daemon });
  for (const pending of service.pending()) {
    await durableStep(ctx, `retirement:reconcile:${pending.operationId}`, () => service.execute(ctx, job, {
      operationId: pending.operationId, source: pending.source, clientId: pending.clientId, infoHash: pending.infoHash,
    }));
  }
  const current = stewardshipBindingPage(ctx.repo, batch.source, batch.offset, 50, daemon.clientId);
  const eligible = new Set<string>();
  for (let offset = 0; offset < current.scannedCount; offset += 20) {
    for (const item of service.preview({ source: batch.source, offset: batch.offset + offset, limit: Math.min(20, current.scannedCount - offset) }).items)
      if (item.eligible) eligible.add(item.bindingKey);
  }
  let attempts = 0;
  for (const binding of current.bindings) {
    if (attempts >= 2 || service.pending().length >= 5) break;
    if (!eligible.has(binding.key)) continue;
    const operationId = await durableStep(ctx, `retirement:operation:${binding.key}`, randomUUID);
    await durableStep(ctx, `retirement:execute:${binding.key}`, () => service.execute(ctx, job, {
      operationId, source: batch.source, clientId: binding.clientId, infoHash: binding.hash,
    }));
    attempts++;
  }
}
