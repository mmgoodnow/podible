import { z } from "zod";
import type { JobRow } from "../app-types";
import { seerrInstance } from "../movies/seerr";
import { ensurePresentIntent } from "../media/retention-service";
import { durableStep, workerLog, type WorkerContext } from "../worker/context";
import { queueTvSeasons, tvUpstreamStep } from "./acquisition";
import { enrichAnimeEntry, type AnimeCatalogProjection } from "./anime";
import { SonarrCatalog } from "./sonarr";
import { approvedSeerrTv, selectedSeerrTvSeasons, SeerrTvClient, TvUpstreamError, type SeerrTvRequest, type SeerrTvSource } from "./upstream";
import type { TvCatalogEntry } from "./types";
import type { BooksRepo } from "../repo";
import { configuredTvCatalog, type TvCatalog } from "./catalog";
import { nativeTvRequestScope, refreshNativeTvRequests } from "./service";

export const tvSyncPayloadSchema = z.object({
  backfillRequestIds: z.array(z.number().int().positive()).min(1).max(5).optional(),
}).strict();
const feedSchema = z.object({ baselineId: z.number().int().nonnegative(), cursor: z.string().datetime() });
const legacyOriginSourceSchema = z.object({ episodeLimit: z.number() });
export function tvSeerrFeedKey(instance: string) { return `tv-seerr-feed:${instance}`; }
export function tvSeerrMappingKey(instance: string) { return `tv-seerr-season-mapping:${instance}`; }
export const tvSeerrMappingIssueSchema = z.object({
  requestId: z.number().int().positive(), title: z.string(), tmdbId: z.number().int().positive(),
  tvdbId: z.number().int().positive().nullable(), approvedSeasons: z.array(z.number().int().nonnegative()),
  catalogSeasons: z.array(z.number().int().nonnegative()), reason: z.string(),
  firstSeenAt: z.iso.datetime(), lastAttemptAt: z.iso.datetime(),
});
/** Server diagnostics plus retry intent, separate from a feed cursor or acquisition scope. */
export function tvSeerrMappingIssues(repo: BooksRepo, instance: string) {
  return z.array(tvSeerrMappingIssueSchema).parse(repo.getJsonState(tvSeerrMappingKey(instance)) ?? []);
}
export type TvSyncServices = {
  source: SeerrTvSource;
  catalog: Pick<SonarrCatalog, "resolveSeerr"> & Partial<Pick<SonarrCatalog, "poster" | "instance">>;
  enrich: (entry: TvCatalogEntry) => Promise<AnimeCatalogProjection>;
};

/** Backfill old, unchanged works without loading episodes or doing HTTP on Library reads. */
export async function hydrateMissingTvArtwork(ctx: WorkerContext, supplied?: TvSyncServices["catalog"]) {
  const settings = ctx.getSettings();
  if (!settings.tv.sonarrUrl || !settings.tv.sonarrApiKey || (supplied && !supplied.poster)) return;
  let catalog: TvSyncServices["catalog"];
  try { catalog = supplied ?? new SonarrCatalog({ url: settings.tv.sonarrUrl, apiKey: settings.tv.sonarrApiKey }); }
  catch { workerLog(ctx, "[tv-artwork] Sonarr artwork configuration unavailable; existing artwork retained"); return; }
  if (!catalog.poster || !catalog.instance) return;
  const configuration = JSON.stringify([settings.tv.sonarrUrl, settings.tv.sonarrApiKey]);
  const active = () => {
    ctx.execution?.assertActive();
    const current = ctx.getSettings();
    if (JSON.stringify([current.tv.sonarrUrl, current.tv.sonarrApiKey]) !== configuration)
      throw new Error("TV artwork connection changed during refresh");
  };
  const cursorKey = `tv-artwork-cursor:${catalog.instance}`;
  const cursor = ctx.repo.getJsonState<number>(cursorKey) ?? 0;
  const batch = ctx.repo.tv.missingArtwork(cursor, 3);
  for (const entry of batch) {
    active();
    const refs = entry.refs.filter(ref => ref.provider === "sonarr" && ref.instance === catalog.instance && /^[1-9]\d*$/.test(ref.id));
    const fetched = await durableStep(ctx, `tv-artwork:read:${entry.id}`, async () => {
      active();
      if (refs.length !== 1 || (!entry.tvdbId && !entry.tmdbId)) return { entry, poster: null };
      try { return { entry, poster: await catalog.poster!({ sonarrSeriesId: Number(refs[0]!.id), tvdbId: entry.tvdbId, tmdbId: entry.tmdbId }) }; }
      catch { active(); workerLog(ctx, `[tv-artwork] metadata unavailable for entry=${entry.id}; existing artwork retained`); return { entry, poster: null }; }
    });
    await durableStep(ctx, `tv-artwork:write:${entry.id}`, () => ctx.repo.transaction(() => {
      active();
      if (fetched.poster) ctx.repo.tv.updateArtwork(fetched.entry, fetched.poster);
      ctx.repo.setJsonState(cursorKey, entry.id);
    }));
  }
}

export function configuredTvSyncServices(ctx: WorkerContext): TvSyncServices {
  const settings = ctx.getSettings();
  return { source: new SeerrTvClient(settings.movies.seerr),
    catalog: new SonarrCatalog({ url: settings.tv.sonarrUrl, apiKey: settings.tv.sonarrApiKey }), enrich: enrichAnimeEntry };
}
export async function syncTvRequests(ctx: WorkerContext, job: JobRow,
  services?: TvSyncServices, nativeCatalog?: TvCatalog): Promise<"done"> {
  const input = tvSyncPayloadSchema.parse(JSON.parse(job.payload_json ?? "{}"));
  const settings = ctx.getSettings();
  if (!input.backfillRequestIds && settings.tv.sonarrUrl && settings.tv.sonarrApiKey)
    await hydrateMissingTvArtwork(ctx, services?.catalog ?? ctx.tvSyncServices?.catalog);
  if (!input.backfillRequestIds && ctx.repo.tv.requests().some((request) => !request.withdrawn_at && nativeTvRequestScope(request))) {
    const nativeConfiguration = () => JSON.stringify([
      ctx.getSettings().movies.tmdbAccessToken, ctx.getSettings().tv.sonarrUrl, ctx.getSettings().tv.sonarrApiKey,
    ]);
    const expected = nativeConfiguration();
    const assertActive = () => {
      ctx.execution?.assertActive();
      if (nativeConfiguration() !== expected) throw new Error("Native TV metadata configuration changed; start a new sync");
    };
    const result = await durableStep(ctx, "tv-native:metadata-refresh", () =>
      refreshNativeTvRequests(ctx.repo, nativeCatalog ?? configuredTvCatalog(settings), { assertActive }));
    assertActive();
    for (const error of result.errors) workerLog(ctx, `[tv-native] request=${error.requestId} metadata unavailable: ${error.message}`);
  }
  // Native season metadata is independent of the optional Seerr ingestion bridge.
  const bridgeConfigured = services || ctx.tvSyncServices || (settings.tv.sonarrUrl && settings.tv.sonarrApiKey &&
    settings.tv.managedRoot && settings.tv.plexSectionId && settings.tv.plexViewPath);
  if (!settings.movies.seerr.url || !settings.movies.seerr.apiKey || !bridgeConfigured) {
    if (input.backfillRequestIds) throw new Error("Configure Seerr and the read-only TV bridge before requesting historical TV backfill");
    return "done";
  }
  services ??= ctx.tvSyncServices ?? configuredTvSyncServices(ctx);
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
  } else {
    for (const request of changed!.requests)
      if (request.id > feed.baselineId || ctx.repo.tv.origins(instance, request.id).length) requests.push(request);
    // Re-read only known legacy origins outside the modified window, never unrelated request history.
    // The old field identifies a one-time migration; its value never limits season membership.
    const seen = new Set(requests.map((request) => request.id));
    const legacyIds = [...new Set(ctx.repo.tv.origins(instance)
      .filter((origin) => legacyOriginSourceSchema.safeParse(origin.source).success).map((origin) => origin.externalRequestId))].sort((a, b) => a - b);
    for (const id of legacyIds) {
      if (seen.has(id)) continue;
      const request = await tvUpstreamStep(ctx, `tv-seerr:request:${id}`, () => services.source.request(id));
      if (request.id !== id) throw new Error(`Seerr request ${id} returned a different identity`);
      requests.push(request);
    }
  }
  if (!input.backfillRequestIds) {
    const seen = new Set(requests.map(request => request.id));
    // Metadata-only retries. Advancing the feed must not forget a genuinely unresolved approval.
    const pending = tvSeerrMappingIssues(ctx.repo, instance).filter(issue => !seen.has(issue.requestId))
      .sort((a, b) => a.lastAttemptAt.localeCompare(b.lastAttemptAt) || a.requestId - b.requestId).slice(0, 8);
    for (const issue of pending) {
      const outcome = await durableStep(ctx, `tv-seerr:mapping-read:${issue.requestId}`, async () => {
        try { return { request: await services.source.request(issue.requestId) }; }
        catch (error) {
          if (!(error instanceof TvUpstreamError)) throw error;
          return { request: null, status: error.status };
        }
      });
      if (!outcome.request) {
        await durableStep(ctx, `tv-seerr:mapping-read-unavailable:${issue.requestId}`, () => {
          active();
          ctx.repo.setJsonState(tvSeerrMappingKey(instance), tvSeerrMappingIssues(ctx.repo, instance).map(item =>
            item.requestId === issue.requestId ? { ...item, lastAttemptAt: new Date().toISOString() } : item));
          workerLog(ctx, `[tv-seerr] request=${issue.requestId} title=${JSON.stringify(issue.title)} metadata unavailable${outcome.status ? ` (HTTP ${outcome.status})` : ""}; approved season mapping remains pending`);
        });
        continue;
      }
      const request = outcome.request;
      if (request.id !== issue.requestId) throw new Error("Deferred Seerr request returned another identity");
      requests.push(request);
    }
  }
  for (const request of requests) {
    active();
    if (!approvedSeerrTv(request)) {
      await durableStep(ctx, `tv-seerr:mapping-revoked:${request.id}`, () => {
        active(); ctx.repo.setJsonState(tvSeerrMappingKey(instance), tvSeerrMappingIssues(ctx.repo, instance).filter(issue => issue.requestId !== request.id));
      });
      continue;
    }
    const catalog = await tvUpstreamStep(ctx, `tv-seerr:catalog:${request.id}`, () => services.catalog.resolveSeerr(request));
    const seasons = new Set(selectedSeerrTvSeasons(request));
    const catalogSeasons = [...new Set(catalog.episodes.flatMap(episode => episode.numbering
      .filter(number => number.scheme === "tvdb:aired" && number.season !== null).map(number => number.season!)))].sort((a, b) => a - b);
    if ([...seasons].some(season => !catalogSeasons.includes(season))) {
      await durableStep(ctx, `tv-seerr:mapping-unresolved:${request.id}`, () => ctx.repo.transaction(() => {
        active();
        const previous = tvSeerrMappingIssues(ctx.repo, instance), now = new Date().toISOString();
        const issue = tvSeerrMappingIssueSchema.parse({ requestId: request.id, title: catalog.title,
          tmdbId: request.media.tmdbId, tvdbId: catalog.tvdbId, approvedSeasons: [...seasons], catalogSeasons,
          firstSeenAt: previous.find(item => item.requestId === request.id)?.firstSeenAt ?? now, lastAttemptAt: now,
          reason: `Approved Seerr seasons ${[...seasons].join(", ")} do not all exist in TVDB aired seasons ${catalogSeasons.join(", ")}. Explicit provider episode mapping is required; approval retained, no ordinal remapping or download performed.` });
        ctx.repo.setJsonState(tvSeerrMappingKey(instance), [...previous.filter(item => item.requestId !== request.id), issue]);
        workerLog(ctx, `[tv-seerr] request=${request.id} title=${JSON.stringify(issue.title)} tmdb=${issue.tmdbId} tvdb=${issue.tvdbId}: ${issue.reason}`);
      }));
      continue;
    }
    const selected = catalog.episodes.filter((episode) => episode.numbering.some((number) =>
      number.scheme === "tvdb:aired" && number.season !== null && seasons.has(number.season)));
    if (!selected.length) throw new Error(`Seerr request ${request.id} has no explicitly mapped episodes in its approved seasons`);
    const origins = ctx.repo.tv.origins(instance, request.id);
    // Approval selects complete provider seasons, including future episodes, before work projection.
    const wantedKeys = new Set(selected.map((episode) => episode.key));
    const previousKeys = origins.flatMap((origin) => ctx.repo.tv.requestedEpisodes(origin.requestId).map((episode) => episode.key));
    if (previousKeys.some((key) => !wantedKeys.has(key)))
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
        const previouslyAired = new Set(oldOrigin ? ctx.repo.tv.requestedEpisodes(oldOrigin.requestId)
          .filter((episode) => episode.airDate && Date.parse(episode.airDate) <= Date.now()).map((episode) => episode.id) : []);
        const native = ctx.repo.tv.requestApproved({ userId: user.id, entry, policy, episodeKeys,
          origin: { instance, externalRequestId: request.id, source: { request, warnings: projection.warnings } } });
        // Replaying an origin is not a new user re-request after intentional eviction.
        const key = { namespace: "tv-target", id: String(native.targetId) };
        if (!ctx.repo.retention.get(key)) ensurePresentIntent(ctx.repo, key, "Approved Seerr TV request");
        if (ctx.repo.retention.get(key)?.desiredPresence === "evicted") continue;
        if (ctx.repo.tv.missingEpisodes(native.requestId).some((episode) => !previouslyAired.has(episode.id) && episode.airDate && Date.parse(episode.airDate) <= Date.now()))
          queueTvSeasons(ctx.repo, native.requestId);
      }
      if (covered.size !== wantedKeys.size) throw new Error("Anime projection lost requested episode identities");
    }));
    await durableStep(ctx, `tv-seerr:mapping-resolved:${request.id}`, () => {
      active(); ctx.repo.setJsonState(tvSeerrMappingKey(instance), tvSeerrMappingIssues(ctx.repo, instance).filter(issue => issue.requestId !== request.id));
    });
  }
  if (changed) await durableStep(ctx, "tv-seerr:cursor", () => {
    active(); const current = feedSchema.parse(ctx.repo.getJsonState(tvSeerrFeedKey(instance)));
    ctx.repo.setJsonState(tvSeerrFeedKey(instance), { baselineId: current.baselineId,
      cursor: current.cursor > changed.through ? current.cursor : changed.through });
  });
  return "done";
}
