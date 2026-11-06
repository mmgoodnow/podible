import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError, type RpcContext } from "./shared";
import { seerrInstance } from "../movies/seerr";
import { tvCatalogEntrySchema } from "../tv/types";
import { tvSeerrFeedKey } from "../tv/sync";
import { queueTvSeasons } from "../tv/acquisition";
import { ensurePresentIntent } from "../media/retention-service";
import { configuredTvCatalog, TvCatalogError, tvSearchResultSchema, tvSeriesDetailsSchema, type TvCatalog } from "../tv/catalog";
import { nativeTvRequestScope, requestTvSeasons, tvSeasonSelectionSchema, withdrawTvRequest } from "../tv/service";
import { tvRequestProgress, tvRequestProgressSchema } from "../tv/request-progress";

const id = z.number().int().positive();
const empty = z.object({}).strict();
const view = z.object({
  requestId: id, targetId: id, externalRequestId: id.nullable(),
  entry: tvCatalogEntrySchema, requestedEpisodes: z.number().int().nonnegative(),
  availableEpisodes: z.number().int().nonnegative(), futureEpisodes: z.number().int().nonnegative(),
  withdrawn: z.boolean(), intentionallyAbsent: z.boolean(),
  seasonNumbers: z.array(z.number().int().nonnegative()),
  progress: tvRequestProgressSchema,
  acquisitions: z.array(z.object({ id, jobId: id.nullable(), status: z.string(), error: z.string().nullable(), reason: z.string(), decision: z.unknown() })),
  events: z.array(z.object({ stage: z.string(), message: z.string(), createdAt: z.string() })),
});
function requestView(ctx: RpcContext, requestId: number, userSafe = false): z.input<typeof view> {
  const repo = ctx.repo.tv, request = repo.requestById(requestId);
  if (!request) throw new RpcError(-32602, "TV request not found");
  const target = repo.target(request.target_id)!;
  const entry = repo.entry(target.entry_id)!;
  const requestedIds = new Set(repo.requestedEpisodeIds(requestId));
  const episodes = entry.episodes.filter(episode=>requestedIds.has(episode.id));
  const active = new Set(repo.activeEpisodes(target.id).map((episode) => episode.episode_id));
  const acquisitions = repo.acquisitions(target.id).filter((acquisition) => !userSafe || acquisition.request_id === requestId);
  const acquisitionIds = new Set(acquisitions.map((a) => a.id));
  const events = repo.events(target.id);
  return { requestId, targetId: target.id,
    externalRequestId: userSafe ? null : repo.origins().find((origin) => origin.requestId === requestId)?.externalRequestId ?? null,
    entry, progress: tvRequestProgress(ctx.repo,{episodes,available:active,events,acquisitions:acquisitions.filter(a=>a.request_id===requestId)}), requestedEpisodes: episodes.length, availableEpisodes: episodes.filter((episode) => active.has(episode.id)).length,
    futureEpisodes: episodes.filter((episode) => !episode.airDate || Date.parse(episode.airDate) > Date.now()).length,
    withdrawn: request.withdrawn_at !== null,
    seasonNumbers: nativeTvRequestScope(request)?.seasonNumbers ?? [...new Set(episodes.flatMap((ep) => ep.numbering
      .filter((n) => n.scheme === "tvdb:aired" && n.season !== null).map((n) => n.season!)))].sort((a, b) => a - b),
    intentionallyAbsent: ctx.repo.retention.get({ namespace: "tv-target", id: String(target.id) })?.desiredPresence === "evicted",
    acquisitions: acquisitions.map((acquisition) => {
      const job = acquisition.job_id ? ctx.repo.getJob(acquisition.job_id) : null;
      return { id: acquisition.id, jobId: acquisition.job_id, status: job?.status ?? "unknown", reason: acquisition.reason,
        error: job?.error ? "TV acquisition failed; inspect server diagnostics" : null,
        decision: !userSafe && acquisition.decision_json ? JSON.parse(acquisition.decision_json) as unknown : null };
    }), events: events.filter((event) => !userSafe || (event.acquisitionId !== null && acquisitionIds.has(event.acquisitionId)))
      .map((event) => ({ stage: event.stage, message: userSafe ? `TV acquisition: ${event.stage}` : event.message, createdAt: event.createdAt })),
  };
}
function sync(ctx: RpcContext, payload = {}) {
  const settings = ctx.repo.getSettings();
  if (!settings.movies.seerr.url || !settings.movies.seerr.apiKey || !settings.tv.sonarrUrl || !settings.tv.sonarrApiKey ||
      !settings.tv.managedRoot || !settings.tv.plexSectionId || !settings.tv.plexViewPath)
    throw new RpcError(-32602, "Configure Seerr, Sonarr and the isolated TV library first");
  return { jobId: ctx.repo.createJob({ type: "tv_sync", payload, idempotencyKey: `tv-sync:${randomUUID()}`, maxAttempts: 3 }).id };
}
function owned(ctx: RpcContext, requestId: number) {
  const request = ctx.repo.tv.requestById(requestId);
  if (!request || request.user_id !== ctx.session!.user_id) throw new RpcError(-32003, "TV request not found");
  return request;
}
function catalogFailure(error: unknown): never {
  if (error instanceof TvCatalogError) throw new RpcError(-32602, error.message, { error: "tv_catalog_unavailable", reason: error.kind });
  throw error;
}
export function createTvRouter(services: { catalog?: TvCatalog } = {}) {
  const catalog = (ctx: RpcContext) => services.catalog ?? configuredTvCatalog(ctx.repo.getSettings());
  const configured = (ctx: RpcContext) => !!(services.catalog || ctx.repo.getSettings().movies.tmdbAccessToken.trim());
  return defineRouter({
  search: defineMethod({ auth: "user", readOnly: true, summary: "Discover TV works using the shared TMDB credential.",
    paramsSchema: z.object({ query: z.string().trim().min(1).max(200) }).strict(),
    resultSchema: z.object({ configured: z.boolean(), unavailableReason: z.string().nullable(), results: z.array(tvSearchResultSchema) }),
    async handler(ctx, p) {
      try { return { configured: configured(ctx), unavailableReason: null, results: await catalog(ctx).search(p.query) }; }
      catch (error) {
        if (!(error instanceof TvCatalogError)) throw error;
        return { configured: configured(ctx), unavailableReason: error.message, results: [] };
      }
    } }),
  details: defineMethod({ auth: "user", readOnly: true, summary: "Read a TV work and its explicitly mapped, requestable TVDB aired seasons.",
    paramsSchema: z.object({ tmdbId: id }).strict(),
    resultSchema: z.object({ configured: z.boolean(), unavailableReason: z.string().nullable(), series: tvSeriesDetailsSchema.nullable() }),
    async handler(ctx, p) {
      try {
        const series = await catalog(ctx).details(p.tmdbId);
        return { configured: configured(ctx), unavailableReason: series.seasons.find((s) => !s.requestable)?.unavailableReason ??
          (series.seasons.length ? null : "No canonical TVDB seasons are available for this work."), series };
      } catch (error) {
        if (!(error instanceof TvCatalogError)) throw error;
        return { configured: configured(ctx), unavailableReason: error.message, series: null };
      }
    } }),
  request: defineMethod({ auth: "user", summary: "Request complete canonical TVDB seasons, including future episode intent.",
    paramsSchema: z.object({ tmdbId: id, seasonNumbers: tvSeasonSelectionSchema }).strict(),
    resultSchema: z.object({ request: view }),
    async handler(ctx, p) {
      try {
        const requested = await requestTvSeasons(ctx.repo, catalog(ctx), ctx.session!.user_id, p);
        return { request: requestView(ctx, requested.requestId, true) };
      } catch (error) { return catalogFailure(error); }
    } }),
  myRequests: defineMethod({ auth: "user", readOnly: true, summary: "Read only the current user's TV requests and safe progress.",
    paramsSchema: empty, resultSchema: z.object({ requests: z.array(view) }),
    handler: (ctx) => ({ requests: ctx.repo.tv.requests(ctx.session!.user_id).map((request) => requestView(ctx, request.id, true)) }) }),
  myRequest: defineMethod({ auth: "user", readOnly: true, summary: "Read one of the current user's TV requests.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: z.object({ request: view }),
    handler(ctx, p) { owned(ctx, p.requestId); return { request: requestView(ctx, p.requestId, true) }; } }),
  withdraw: defineMethod({ auth: "user", summary: "Withdraw only the current user's request, preserving media and other demand.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: z.object({ request: view }),
    handler(ctx, p) { owned(ctx, p.requestId); withdrawTvRequest(ctx.repo, p.requestId); return { request: requestView(ctx, p.requestId, true) }; } }),
  retryMyRequest: defineMethod({ auth: "user", summary: "Search missing aired seasons for the current user's active request; share existing work.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: z.object({ request: view }),
    handler(ctx, p) {
      const request = owned(ctx, p.requestId);
      if (request.withdrawn_at) throw new RpcError(-32602, "This request has been withdrawn");
      return ctx.repo.transaction(() => {
        ensurePresentIntent(ctx.repo, { namespace: "tv-target", id: String(request.target_id) }, "User explicitly resumed their TV request");
        queueTvSeasons(ctx.repo, p.requestId, "recovery");
        return { request: requestView(ctx, p.requestId, true) };
      });
    } }),
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
  backfill: defineMethod({ auth: "admin", summary: "Explicitly acquire all episodes in approved seasons from historical Seerr TV requests, retaining future intent.",
    paramsSchema: z.object({ backfillRequestIds: z.array(id).min(1).max(5) }).strict(),
    resultSchema: z.object({ jobId: id }), handler: (ctx, p) => sync(ctx, p) }),
  retry: defineMethod({ auth: "admin", summary: "Explicitly re-request/recover TV seasons without deleting current episodes or seed data.",
    paramsSchema: z.object({ requestId: id }).strict(), resultSchema: z.object({ jobId: id }),
    handler(ctx, p) {
      const request = ctx.repo.tv.requestById(p.requestId);
      if (!request || request.withdrawn_at) throw new RpcError(-32602, "Active TV request not found");
      return ctx.repo.transaction(() => {
        ensurePresentIntent(ctx.repo, { namespace: "tv-target", id: String(request.target_id) }, "Administrator explicitly re-requested TV seasons");
        const queued = queueTvSeasons(ctx.repo, p.requestId, "recovery");
        if (!queued.length) throw new RpcError(-32602, "All aired requested episodes are already available");
        return { jobId: queued[0]!.acquisition.job_id! };
      });
    } }),
  });
}
export const tvRouter = createTvRouter();
