import { z } from "zod";
import type { JobRow } from "../app-types";
import { seerrInstance } from "../movies/seerr";
import { ensurePresentIntent } from "../media/retention-service";
import { durableStep, type WorkerContext } from "../worker/context";
import { queueTvAcquisition, tvUpstreamStep } from "./acquisition";
import { enrichAnimeEntry, type AnimeCatalogProjection } from "./anime";
import { SonarrCatalog } from "./sonarr";
import { approvedSeerrTv, selectedSeerrTvSeasons, SeerrTvClient, type SeerrTvRequest, type SeerrTvSource } from "./upstream";
import type { TvCatalogEntry } from "./types";

export const tvSyncPayloadSchema = z.object({
  backfillRequestIds: z.array(z.number().int().positive()).min(1).max(5).optional(),
  episodeLimit: z.number().int().min(1).max(50).optional(),
}).strict().refine((value) => !value.episodeLimit || !!value.backfillRequestIds, "Episode limit is only for explicit backfill");
const feedSchema = z.object({ baselineId: z.number().int().nonnegative(), cursor: z.string().datetime() });
export function tvSeerrFeedKey(instance: string) { return `tv-seerr-feed:${instance}`; }
export type TvSyncServices = {
  source: SeerrTvSource;
  catalog: Pick<SonarrCatalog, "resolveSeerr">;
  enrich: (entry: TvCatalogEntry) => Promise<AnimeCatalogProjection>;
};
export function configuredTvSyncServices(ctx: WorkerContext): TvSyncServices {
  const settings = ctx.getSettings();
  return { source: new SeerrTvClient(settings.movies.seerr),
    catalog: new SonarrCatalog({ url: settings.tv.sonarrUrl, apiKey: settings.tv.sonarrApiKey }), enrich: enrichAnimeEntry };
}
export async function syncTvRequests(ctx: WorkerContext, job: JobRow,
  services: TvSyncServices = ctx.tvSyncServices ?? configuredTvSyncServices(ctx)): Promise<"done"> {
  const input = tvSyncPayloadSchema.parse(JSON.parse(job.payload_json ?? "{}"));
  const settings = ctx.getSettings();
  const instance = seerrInstance(settings.movies.seerr.url);
  const configuration = JSON.stringify([settings.tv, settings.movies.seerr.url, settings.movies.qualityPolicy]);
  const expectedConfiguration = await durableStep(ctx, "tv-seerr:configuration", () => configuration);
  const active = () => {
    ctx.execution?.assertActive();
    const now = ctx.getSettings();
    if (JSON.stringify([now.tv, now.movies.seerr.url, now.movies.qualityPolicy]) !== expectedConfiguration)
      throw new Error("TV request configuration changed; start a new sync");
  };
  const savedFeed = ctx.repo.getJsonState(tvSeerrFeedKey(instance));
  const latest = savedFeed ? null : await tvUpstreamStep(ctx, "tv-seerr:latest", () => services.source.latest());
  const feed = await durableStep(ctx, "tv-seerr:activation", () => {
    const existing = ctx.repo.getJsonState(tvSeerrFeedKey(instance));
    if (existing) return feedSchema.parse(existing);
    active();
    const baseline = { baselineId: latest?.id ?? 0, cursor: new Date().toISOString() };
    // Persist activation before checkpointing; restart must not move the baseline.
    ctx.repo.setJsonState(tvSeerrFeedKey(instance), baseline);
    return baseline;
  });
  const changed = input.backfillRequestIds ? null : await tvUpstreamStep(ctx, "tv-seerr:changes", () => services.source.changes(feed.cursor));
  const requests: SeerrTvRequest[] = [];
  if (input.backfillRequestIds) {
    for (const id of [...new Set(input.backfillRequestIds)]) {
      const request = await tvUpstreamStep(ctx, `tv-seerr:request:${id}`, () => services.source.request(id));
      if (request.id !== id || !approvedSeerrTv(request)) throw new Error(`Seerr request ${id} is not approved TV`);
      requests.push(request);
    }
  } else for (const request of changed!.requests)
    if (request.id > feed.baselineId || ctx.repo.tv.origins(instance, request.id).length) requests.push(request);
  for (const request of requests) {
    active();
    if (!approvedSeerrTv(request)) continue;
    const catalog = await tvUpstreamStep(ctx, `tv-seerr:catalog:${request.id}`, () => services.catalog.resolveSeerr(request));
    const seasons = new Set(selectedSeerrTvSeasons(request));
    const selected = catalog.episodes.filter((episode) => episode.numbering.some((number) =>
      number.scheme === "tvdb:aired" && number.season !== null && seasons.has(number.season)));
    if (!selected.length) throw new Error(`Seerr request ${request.id} has no explicitly mapped episodes in its approved seasons`);
    const origins = ctx.repo.tv.origins(instance, request.id);
    const prior = origins[0];
    const priorLimit = z.object({ episodeLimit: z.number().int().positive().nullable() }).safeParse(prior?.source);
    const limit = priorLimit.success ? priorLimit.data.episodeLimit : input.episodeLimit ?? null;
    const pinned = limit ? origins.flatMap((origin) => ctx.repo.tv.requestedEpisodes(origin.requestId).map((episode) => episode.key)) : [];
    // A canary selects episode identities once, not a moving first-N provider sort order.
    const wantedKeys = new Set(pinned.length ? pinned : (limit ? selected.slice(0, limit) : selected).map((episode) => episode.key));
    if ([...wantedKeys].some((key) => !selected.some((episode) => episode.key === key)))
      throw new Error("Previously selected TV episodes no longer map to the approved seasons; retaining the existing scope");
    const projection = await durableStep(ctx, `tv-seerr:anime:${request.id}`, () => catalog.anime
      ? services.enrich(catalog) : Promise.resolve({ entries: [catalog], warnings: [], unmappedEpisodeKeys: [] }));
    await durableStep(ctx, `tv-seerr:import:${request.id}`, () => ctx.repo.transaction(() => {
      active();
      const actor = request.requestedBy;
      const principalId = `seerr:${instance}:${actor.id}`;
      const user = (actor.plexId ? ctx.repo.listUsers("plex").find((u) => u.provider_user_id === String(actor.plexId)) : null)
        ?? ctx.repo.listUsers("local").find((u) => u.provider_user_id === principalId)
        ?? ctx.repo.upsertUser({ provider: "local", providerUserId: principalId,
          username: `Seerr: ${actor.displayName ?? actor.username ?? actor.id}`, isAdmin: false });
      const base = settings.movies.qualityPolicy;
      const policy = request.is4k ? { ...base, tiers: base.tiers.some((tier) => tier.resolution === 2160)
        ? base.tiers.filter((tier) => tier.resolution === 2160)
        : [...new Map(base.tiers.map((tier) => [tier.source, { ...tier, resolution: 2160 as const }])).values()] } : base;
      const covered = new Set<string>();
      for (const entry of projection.entries) {
        const episodeKeys = entry.episodes.filter((episode) => wantedKeys.has(episode.key)).map((episode) => episode.key);
        if (!episodeKeys.length) continue;
        for (const key of episodeKeys) { if (covered.has(key)) throw new Error("Anime projection assigned an episode to multiple works"); covered.add(key); }
        const oldOrigin = ctx.repo.tv.origins(instance, request.id).find((origin) => ctx.repo.tv.entry(origin.entryId)?.key === entry.key);
        const previousEpisodes = new Set(oldOrigin ? ctx.repo.tv.requestedEpisodeIds(oldOrigin.requestId) : []);
        const native = ctx.repo.tv.requestApproved({ userId: user.id, entry, policy, episodeKeys,
          origin: { instance, externalRequestId: request.id, source: { request, episodeLimit: limit, warnings: projection.warnings } } });
        // Replaying an origin is not a new user re-request after intentional eviction.
        const key = { namespace: "tv-target", id: String(native.targetId) };
        if (!ctx.repo.retention.get(key)) ensurePresentIntent(ctx.repo, key, "Approved Seerr TV request");
        if (ctx.repo.retention.get(key)?.desiredPresence === "evicted") continue;
        if (ctx.repo.tv.missingEpisodes(native.requestId).some((episode) => !previousEpisodes.has(episode.id) && episode.airDate && Date.parse(episode.airDate) <= Date.now()))
          queueTvAcquisition(ctx.repo, native.requestId);
      }
      if (covered.size !== wantedKeys.size) throw new Error("Anime projection lost requested episode identities");
    }));
  }
  if (changed) await durableStep(ctx, "tv-seerr:cursor", () => {
    active(); const current = feedSchema.parse(ctx.repo.getJsonState(tvSeerrFeedKey(instance)));
    ctx.repo.setJsonState(tvSeerrFeedKey(instance), { baselineId: current.baselineId,
      cursor: current.cursor > changed.through ? current.cursor : changed.through });
  });
  return "done";
}
