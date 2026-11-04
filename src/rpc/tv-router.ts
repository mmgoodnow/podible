import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError, type RpcContext } from "./shared";
import { seerrInstance } from "../movies/seerr";
import { tvCatalogEntrySchema } from "../tv/types";
import { tvSeerrFeedKey } from "../tv/sync";
import { queueTvAcquisition } from "../tv/acquisition";
import { ensurePresentIntent } from "../media/retention-service";

const id = z.number().int().positive();
const empty = z.object({}).strict();
const view = z.object({
  requestId: id, targetId: id, externalRequestId: id.nullable(),
  entry: tvCatalogEntrySchema, requestedEpisodes: z.number().int().nonnegative(),
  availableEpisodes: z.number().int().nonnegative(), futureEpisodes: z.number().int().nonnegative(),
  withdrawn: z.boolean(), intentionallyAbsent: z.boolean(),
  acquisitions: z.array(z.object({ id, jobId: id.nullable(), status: z.string(), error: z.string().nullable(), reason: z.string(), decision: z.unknown() })),
  events: z.array(z.object({ stage: z.string(), message: z.string(), createdAt: z.string() })),
});
function requestView(ctx: RpcContext, requestId: number): z.input<typeof view> {
  const repo = ctx.repo.tv, request = repo.requestById(requestId);
  if (!request) throw new RpcError(-32602, "TV request not found");
  const target = repo.target(request.target_id)!;
  const entry = repo.entry(target.entry_id)!;
  const episodes = repo.requestedEpisodes(requestId);
  const active = new Set(repo.activeEpisodes(target.id).map((episode) => episode.episode_id));
  return { requestId, targetId: target.id,
    externalRequestId: repo.origins().find((origin) => origin.requestId === requestId)?.externalRequestId ?? null,
    entry, requestedEpisodes: episodes.length, availableEpisodes: episodes.filter((episode) => active.has(episode.id)).length,
    futureEpisodes: episodes.filter((episode) => !episode.airDate || Date.parse(episode.airDate) > Date.now()).length,
    withdrawn: request.withdrawn_at !== null,
    intentionallyAbsent: ctx.repo.retention.get({ namespace: "tv-target", id: String(target.id) })?.desiredPresence === "evicted",
    acquisitions: repo.acquisitions(target.id).map((acquisition) => {
      const job = acquisition.job_id ? ctx.repo.getJob(acquisition.job_id) : null;
      return { id: acquisition.id, jobId: acquisition.job_id, status: job?.status ?? "unknown", reason: acquisition.reason,
        error: job?.error ? "TV acquisition failed; inspect server diagnostics" : null,
        decision: acquisition.decision_json ? JSON.parse(acquisition.decision_json) as unknown : null };
    }), events: repo.events(target.id).map((event) => ({ stage: event.stage, message: event.message, createdAt: event.createdAt })),
  };
}
function sync(ctx: RpcContext, payload = {}) {
  const settings = ctx.repo.getSettings();
  if (!settings.movies.seerr.url || !settings.movies.seerr.apiKey || !settings.tv.sonarrUrl || !settings.tv.sonarrApiKey ||
      !settings.tv.managedRoot || !settings.tv.plexSectionId || !settings.tv.plexViewPath)
    throw new RpcError(-32602, "Configure Seerr, Sonarr and the isolated TV library first");
  return { jobId: ctx.repo.createJob({ type: "tv_sync", payload, idempotencyKey: `tv-sync:${randomUUID()}`, maxAttempts: 3 }).id };
}
export const tvRouter = defineRouter({
  status: defineMethod({ auth: "admin", readOnly: true, summary: "Inspect independent TV acquisition and its no-backfill activation boundary.",
    paramsSchema: empty,
    resultSchema: z.object({ configured: z.boolean(), plexSectionId: z.string(), baselineId: z.number().nullable(), cursor: z.string().nullable(),
      lastSync: z.object({ jobId: id, status: z.string() }).nullable() }),
    handler(ctx) {
      const settings = ctx.repo.getSettings(), tv = settings.tv;
      const feed = settings.movies.seerr.url ? ctx.repo.getJsonState<{ baselineId: number; cursor: string }>(tvSeerrFeedKey(seerrInstance(settings.movies.seerr.url))) : null;
      const job = ctx.repo.latestJobByType("tv_sync");
      return { configured: !!(tv.sonarrUrl && tv.sonarrApiKey && tv.managedRoot && tv.plexSectionId && tv.plexViewPath &&
        settings.movies.seerr.url && settings.movies.seerr.apiKey), plexSectionId: tv.plexSectionId,
        baselineId: feed?.baselineId ?? null, cursor: feed?.cursor ?? null, lastSync: job ? { jobId: job.id, status: job.status } : null };
    } }),
  list: defineMethod({ auth: "admin", readOnly: true, summary: "Read actual independent TV requests, episode coverage and acquisition evidence.",
    paramsSchema: z.object({ limit: z.number().int().min(1).max(100).default(25) }).strict(), resultSchema: z.object({ items: z.array(view) }),
    handler: (ctx, p) => ({ items: ctx.repo.tv.requests().slice(0, p.limit).map((request) => requestView(ctx, request.id)) }) }),
  get: defineMethod({ auth: "admin", readOnly: true, summary: "Inspect one independent TV request.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: view,
    handler: (ctx, p) => requestView(ctx, p.requestId) }),
  sync: defineMethod({ auth: "admin", summary: "Consume new approved Seerr TV requests; activation never downloads existing requests.",
    paramsSchema: empty, resultSchema: z.object({ jobId: id }), handler: (ctx) => sync(ctx) }),
  backfill: defineMethod({ auth: "admin", summary: "Explicitly acquire a bounded set of episodes from approved historical Seerr requests.",
    paramsSchema: z.object({ backfillRequestIds: z.array(id).min(1).max(5), episodeLimit: z.number().int().min(1).max(50).default(1) }).strict(),
    resultSchema: z.object({ jobId: id }), handler: (ctx, p) => sync(ctx, p) }),
  retry: defineMethod({ auth: "admin", summary: "Explicitly re-request/recover a TV target without deleting its current episodes or seed data.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: z.object({ jobId: id }),
    handler(ctx, p) {
      const request = ctx.repo.tv.requestById(p.requestId);
      if (!request || request.withdrawn_at) throw new RpcError(-32602, "Active TV request not found");
      return ctx.repo.transaction(() => {
        ensurePresentIntent(ctx.repo, { namespace: "tv-target", id: String(request.target_id) }, "Administrator explicitly re-requested TV episodes");
        return { jobId: queueTvAcquisition(ctx.repo, p.requestId, "recovery").acquisition.job_id! };
      });
    } }),
});
