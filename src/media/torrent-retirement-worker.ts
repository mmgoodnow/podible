import { randomUUID } from "node:crypto";
import type { JobRow } from "../app-types";
import { durableStep, workerLog, type WorkerContext } from "../worker/context";
import { stewardshipBindingPage } from "./torrent-stewardship";
import { privateRetirementDaemon } from "./torrent-retirement-transmission";
import { TorrentRetirement, type TorrentRetirementAdapters } from "./torrent-retirement";
import { createOrphanRetirementFence } from "./torrent-retirement-admission";
import { nextPackRetirementCandidate } from "../tv/pack-retirement-discovery";

/** Bounded discovery and reconciliation; all claims occur within globally locked durable steps. */
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
    try {
      await durableStep(ctx, `retirement:reconcile:${pending.operationId}`, () => service.execute(ctx, job, {
        operationId: pending.operationId, source: pending.source, clientId: pending.clientId, infoHash: pending.infoHash,
        ...(pending.packNomination && { packNomination: pending.packNomination }),
      }));
    } catch {
      ctx.execution.assertActive();
      workerLog(ctx, `[stewardship] Retirement ${pending.operationId} could not be reconciled; its admission fence remains closed.`);
    }
  }
  const pack = await durableStep(ctx, "retirement:pack-discovery", () => {
    const pending = service.pending();
    if (pending.length >= 5 || pending.filter(intent => intent.packNomination).length >= 2) return null;
    const candidate = nextPackRetirementCandidate(ctx.repo, daemon.clientId);
    return candidate ? { ...candidate, operationId: randomUUID() } : null;
  });
  if (pack) {
    try { await durableStep(ctx, `retirement:pack-execute:${pack.operationId}`, () => service.execute(ctx, job, pack)); }
    catch {
      ctx.execution.assertActive();
      workerLog(ctx, `[stewardship] Pack retirement ${pack.operationId} was deferred; no broader removal will be attempted.`);
    }
  }
  const current = stewardshipBindingPage(ctx.repo, batch.source, batch.offset, 50, daemon.clientId);
  const eligible = new Set<string>();
  for (let offset = 0; offset < current.scannedCount; offset += 20) {
    for (const item of service.preview({ source: batch.source, offset: batch.offset + offset, limit: Math.min(20, current.scannedCount - offset) }).items)
      if (item.eligible) eligible.add(item.bindingKey);
  }
  let attempts = pack ? 1 : 0;
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
