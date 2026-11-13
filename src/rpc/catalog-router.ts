import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { catalogMovieSchema } from "../movies/types";
import { tvCatalogEntrySchema } from "../tv/types";
import { homeRecentSchema } from "../media/home-recent";
import { homeDiscovery, homeDiscoverySchema } from "../media/home-discovery";
import { backdropUrlSchema } from "../media/tmdb-artwork";
import { movieCatalog } from "../movies/runtime";
import { configuredTvCatalog } from "../tv/catalog";
import { prepareTvRequestProgress, tvRequestProgress, tvRequestProgressSchema } from "../tv/request-progress";
import { nativeTvRequestScope, nativeTvRequestSeasonNumbers } from "../tv/service";
import { adminTorrentKey, readAdminTorrents, type AdminTransmissionReader } from "./admin-torrents";
const id = z.number().int().positive();
export const createCatalogRouter = (transmission?: AdminTransmissionReader | null) => defineRouter({
  recent: defineMethod({ auth: "user", readOnly: true,
    summary: "Bounded shared library rails ordered by verified publication and actual episode air dates.",
    paramsSchema: z.object({ limit: z.number().int().min(1).max(20).default(12) }).strict(),
    resultSchema: homeRecentSchema,
    handler(ctx, input) { return ctx.repo.readHomeRecent({ ...input, userId: ctx.session!.user_id }); } }),
  discovery: defineMethod({ auth: "user", readOnly: true,
    summary: "Server-cached public trending lists using the existing TMDB configuration, without per-card lookups.",
    paramsSchema: z.object({}).strict(), resultSchema: homeDiscoverySchema,
    handler(ctx) { return homeDiscovery.get(ctx.repo.readSettings()?.movies.tmdbAccessToken); } }),
  backdrop: defineMethod({ auth: "user", readOnly: true,
    summary: "Read landscape artwork for the focused title without enriching the whole library.",
    paramsSchema: z.object({ kind: z.enum(["movie", "tv"]), tmdbId: id.safe() }).strict(),
    resultSchema: z.object({ backdropUrl: backdropUrlSchema }),
    async handler(ctx, { kind, tmdbId }) {
      const settings = ctx.repo.getSettings();
      if (kind === "movie") {
        const saved = ctx.repo.movies.movieByTmdb(tmdbId)?.backdropUrl;
        if (saved) return { backdropUrl: saved };
        if (!ctx.movieServices && !settings.movies.tmdbAccessToken && !(settings.movies.seerr.url && settings.movies.seerr.apiKey))
          return { backdropUrl: null };
        const catalog = ctx.movieServices?.catalog ?? movieCatalog(settings);
        const movie = await catalog.get(tmdbId);
        return { backdropUrl: movie.tmdbId === tmdbId ? movie.backdropUrl ?? null : null };
      }
      if (!settings.movies.tmdbAccessToken) return { backdropUrl: null };
      return { backdropUrl: await configuredTvCatalog(settings).backdrop?.(tmdbId) ?? null };
    } }),
  available: defineMethod({ auth: "user", readOnly: true,
    summary: "Search already-published shared movies/TV without waiting on remote catalogs or exposing anyone's requests.",
    paramsSchema: z.object({ query: z.string().trim().max(200).default(""), limit: z.number().int().min(1).max(100).default(50) }).strict(),
    resultSchema: z.object({ movies: z.array(catalogMovieSchema), tv: z.array(tvCatalogEntrySchema.pick({ key: true, title: true,
      year: true, tmdbId: true, posterUrl: true, backdropUrl: true }).extend({ entryId: id, availableEpisodes: z.number().int().nonnegative() })),
      movieTruncated: z.boolean(), tvTruncated: z.boolean() }),
    handler(ctx, input) {
      const movies = ctx.repo.movies.availableCatalog(input.query, input.limit + 1);
      const tv = ctx.repo.tv.availableCatalog(input.query, input.limit + 1);
      return { movies: movies.slice(0, input.limit), tv: tv.slice(0, input.limit), movieTruncated: movies.length > input.limit, tvTruncated: tv.length > input.limit };
    } }),
  tvEntry: defineMethod({ auth: "user", readOnly: true,
    summary: "Read one TV show with episode availability, acquisition progress and the viewer's request actions.",
    paramsSchema: z.object({ entryId: id }).strict(),
    resultSchema: z.object({ entry: tvCatalogEntrySchema, availableEpisodes: z.number().int().nonnegative(),
      progress: tvRequestProgressSchema,
      requests: z.array(z.object({ id, seasons: z.array(z.number()), searchable: z.boolean() })),
      crossSeeds: z.array(z.object({ title: z.string(), provider: z.string().nullable(),
        status: z.enum(["queued", "preparing", "registered"]), reusedBytes: z.number().nonnegative().nullable(),
        episodeIds: z.array(id) })),
    }),
    async handler(ctx, input) {
      const tv = ctx.repo.tv;
      const available = new Set(tv.publishedEpisodeIds(input.entryId));
      const requests = tv.showRequests(input.entryId, ctx.session!.user_id);
      const entry = available.size || requests.length ? tv.entry(input.entryId) : null;
      if (!entry) throw new RpcError(-32602, "TV show not found");
      const acquisitions = tv.showAcquisitions(entry.id), events = tv.showProgressEvents(entry.id);
      const seeds = ctx.repo.crossSeed.showRegistrations(entry.id);
      const jobStatuses = ctx.repo.getJobStatuses([
        ...acquisitions.flatMap(a => a.job_id === null ? [] : [a.job_id]),
        ...seeds.flatMap(s => s.registered ? [] : [s.jobId]),
      ]);
      const requested = tv.showRequestedEpisodeIds(entry.id, ctx.session!.user_id);
      const requestByEpisode = new Map<number, number>();
      for (const [requestId, episodes] of requested) for (const episodeId of episodes) requestByEpisode.set(episodeId, requestId);
      const prepared = prepareTvRequestProgress({ episodes: entry.episodes, available, events, acquisitions, jobStatuses,
        requestByEpisode,
        readTransfers: ids => tv.requestDownloadObservations(ids),
        readPublishingEpisodes: ids => tv.publishingEpisodeIds(ids) });
      const settings = prepared.bindings.length ? ctx.repo.readSettings() : null;
      const sources = prepared.bindings.map(row => ({ media: "tv" as const, binding: { ...row, id: row.torrentId } }));
      const live = settings ? (await readAdminTorrents(sources, settings, Date.now(), transmission)).observations : undefined;
      const progress = prepared.project(live && new Map(sources.flatMap(source => {
        const sample = live.get(adminTorrentKey(source));
        return sample ? [[source.binding.id, sample] as const] : [];
      })));
      // Only display provider-facing labels, never raw offers, client errors, or torrent identifiers.
      const label = (value: string | null) => value?.replace(/(?:https?:\/\/|magnet:\?)[^\s]+/gi, "").trim().slice(0, 350) || null;
      return { entry: { ...entry, episodes: [] }, availableEpisodes: available.size, progress,
        requests: requests.filter(r => r.withdrawn_at === null).map(r => {
          const scope = new Set(requested.get(r.id) ?? []);
          const episodes = entry.episodes.filter(ep => scope.has(ep.id));
          const own = tvRequestProgress({ episodes, available, events,
            acquisitions: acquisitions.filter(a => a.request_id === r.id), jobStatuses });
          return { id: r.id, seasons: nativeTvRequestSeasonNumbers(nativeTvRequestScope(r)) ?? own.seasons.map(s => s.seasonNumber),
            searchable: own.searchableAiredEpisodes > 0 };
        }),
        crossSeeds: seeds.flatMap(s => {
          const status: "registered" | "preparing" | "queued" | null = s.registered ? "registered" : jobStatuses.get(s.jobId) === "running" ? "preparing"
            : jobStatuses.get(s.jobId) === "queued" ? "queued" : null;
          return status ? [{ title: label(s.title) ?? "Cross-seed release", provider: label(s.provider),
            status, reusedBytes: s.reusedBytes, episodeIds: s.episodeIds }] : [];
        }) };
    } }),
});
export const catalogRouter = createCatalogRouter();
