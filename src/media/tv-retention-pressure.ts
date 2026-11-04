import { randomUUID } from "node:crypto";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { observeRetentionDisk } from "./retention-filesystem";
import { TvRetentionService, type TvRetentionAdapters } from "./tv-retention";
import { observeRecoveryMount } from "./recovery-filesystem";
import type { TvRetentionAudit } from "./tv-retention-types";

/** Bounded pressure intake. Never searches releases or removes bytes itself. */
export async function runTvRetentionPressure(ctx: WorkerContext, _job: JobRow,
  adapters: TvRetentionAdapters & { observeDisk?: typeof observeRetentionDisk } = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("TV pressure handling requires durable execution");
  const active = () => ctx.execution!.assertActive(), settings = ctx.repo.readSettings();
  if (!settings?.tv.managedRoot || !settings.tv.plexViewPath || !settings.tv.plexSectionId) return "done";
  const root = settings.tv.managedRoot;
  const pressured = async () => {
    active();
    if (JSON.stringify(ctx.repo.readSettings()) !== JSON.stringify(settings) || observeRecoveryMount(ctx.repo, root, "tv").state !== "verified") return false;
    const disk = await (adapters.observeDisk ?? observeRetentionDisk)(root);
    active(); return disk.state === "known" && disk.availableBytes !== null && disk.availableBytes < settings.retention.minimumFreeBytes;
  };
  if (!await pressured()) return "done";
  const cursor = ctx.repo.getJsonState<number>("tv-retention-pressure-cursor") ?? 0;
  const page = ctx.repo.tvRetention.pressureCandidates(Number.isSafeInteger(cursor) && cursor >= 0 ? cursor : 0);
  const service = new TvRetentionService(ctx.repo, adapters);
  const audits: TvRetentionAudit[] = [];
  for (const scope of page.scopes) {
    active();
    if (settings.retention.protectedTvTargetIds.includes(scope.targetId)) continue;
    const audit = await service.audit(scope);
    active(); if (audit.eligible) audits.push(audit);
  }
  const plays = (audit: typeof audits[number]) => audit.watch.episodes.reduce((sum, ep) => sum + (ep.playCount ?? Infinity), 0);
  const latest = (audit: typeof audits[number]) => Math.max(...audit.watch.episodes.map(ep => Date.parse(ep.lastViewedAt ?? "9999-01-01")));
  audits.sort((a, b) => plays(a) - plays(b) || latest(a) - latest(b) || a.season.targetId - b.season.targetId);
  const chosen = audits[0];
  if (chosen && await pressured()) {
    const operationId = await durableStep(ctx, "tv-pressure:operation", randomUUID);
    await durableStep(ctx, "tv-pressure:queue", async () => {
      active(); if (!await pressured()) return null;
      return service.evict({ ...chosen.season, expectedRevision: chosen.revision, operationId,
        reason: "Automatic disk pressure: low-watch, old, verified whole-season target" });
    });
  }
  active();
  ctx.repo.setJsonState("tv-retention-pressure-cursor", page.hasMore ? page.scopes.at(-1)!.targetId : 0);
  return "done";
}
