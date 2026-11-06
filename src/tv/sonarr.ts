import { z } from "zod";
import { tvPosterFromImages, tvPosterUrl, type TvCatalogEntry } from "./types";
import {
  approvedSeerrTv,
  sonarrInstance,
  TvUpstreamError,
  TvUpstreamHttp,
  tvWire,
  type SeerrTvRequest,
  type TvFetcher,
} from "./upstream";

export interface SonarrSettings {
  url: string;
  apiKey: string;
  lookbackDays?: number;
}
export interface SonarrSeriesRef {
  tvdbId?: number | null;
  tmdbId?: number | null;
  sonarrSeriesId?: number | null;
}
const localId = z.number().int().positive();
const providerId = z.number().int().nonnegative().nullable().optional();
const coordinate = z.number().int().nonnegative();
const seriesSchema = z.object({
  id: localId,
  tvdbId: providerId,
  tmdbId: providerId,
  title: z.string().min(1),
  alternateTitles: z.array(z.object({ title: z.string() })).max(1000).nullable().optional(),
  year: z.number().int().nonnegative().nullable().optional(),
  seriesType: z.enum(["standard", "daily", "anime"]),
  images: z.unknown().optional(),
  remotePoster: z.unknown().optional(),
});
const episodeSchema = z.object({
  id: localId,
  seriesId: localId,
  tvdbId: providerId,
  title: z.string(),
  seasonNumber: coordinate,
  episodeNumber: coordinate,
  airDate: z.iso.date().nullable().optional(),
  airDateUtc: z.string().datetime({ offset: true }).nullable().optional(),
  runtime: z.number().int().nonnegative().nullable().optional(),
  absoluteEpisodeNumber: coordinate.nullable().optional(),
  sceneSeasonNumber: coordinate.nullable().optional(),
  sceneEpisodeNumber: coordinate.nullable().optional(),
  sceneAbsoluteEpisodeNumber: coordinate.nullable().optional(),
  unverifiedSceneNumbering: z.boolean().optional(),
});
type SeriesWire = z.infer<typeof seriesSchema>;
type EpisodeWire = z.infer<typeof episodeSchema>;
const known = (id: number | null | undefined): id is number => !!id && id > 0;

function seriesKey(series: SeriesWire, instance: string): string {
  return known(series.tvdbId) ? `tvdb:${series.tvdbId}`
    : known(series.tmdbId) ? `tmdb:${series.tmdbId}`
    : `sonarr:${instance}:${series.id}`;
}
function episodeKey(episode: EpisodeWire, instance: string): string {
  return known(episode.tvdbId) ? `tvdb:${episode.tvdbId}`
    : `sonarr:${instance}:${episode.id}`;
}

/** Sonarr v3 catalog reads; no search/add/monitor/command endpoint capability. */
export class SonarrCatalog {
  readonly instance: string;
  #http: TvUpstreamHttp;
  constructor(settings: SonarrSettings, fetcher: TvFetcher = fetch) {
    this.#http = new TvUpstreamHttp("sonarr", {
      url: settings.url, apiKey: settings.apiKey,
    }, fetcher);
    this.instance = sonarrInstance(settings.url);
  }

  async #series(id: number): Promise<SeriesWire> {
    tvWire("sonarr", localId, id);
    const series = tvWire("sonarr", seriesSchema, await this.#http.get(`series/${id}`));
    if (series.id !== id) throw new TvUpstreamError("sonarr", "different series identity");
    return series;
  }

  #entry(series: SeriesWire, episodes: EpisodeWire[]): TvCatalogEntry {
    const instance = this.instance;
    const source = `sonarr:${instance}`;
    const episodeKeys = new Set<string>();
    const localIds = new Set<number>();
    return {
      key: seriesKey(series, instance),
      tvdbId: series.tvdbId || null,
      tmdbId: series.tmdbId || null,
      title: this.#http.clean(series.title),
      aliases: [...new Set((series.alternateTitles ?? []).map((a) => this.#http.clean(a.title)).filter(Boolean))],
      year: series.year || null,
      posterUrl: tvPosterFromImages(series.images) ?? tvPosterUrl(series.remotePoster),
      anime: series.seriesType === "anime",
      source,
      refs: [
        { provider: "sonarr", id: String(series.id), instance, source: `${source}:api/v3/series` },
        ...(known(series.tvdbId) ? [{ provider: "tvdb" as const, id: String(series.tvdbId), instance: null, source }] : []),
        ...(known(series.tmdbId) ? [{ provider: "tmdb" as const, id: String(series.tmdbId), instance: null, source }] : []),
      ],
      episodes: episodes.map((episode) => {
        const key = episodeKey(episode, instance);
        if (episode.seriesId !== series.id || episodeKeys.has(key) || localIds.has(episode.id))
          throw new TvUpstreamError("sonarr", "conflicting episode identity");
        episodeKeys.add(key);
        localIds.add(episode.id);
        const mappingSource = `${source}:api/v3/episode`;
        const numbering: TvCatalogEntry["episodes"][number]["numbering"] = [];
        const add = (scheme: string, season: number | null, number: number | null, absolute: number | null) =>
          numbering.push({ scheme, season, episode: number, absolute, source: mappingSource });
        add(`${source}:aired`, episode.seasonNumber, episode.episodeNumber, null);
        if (known(episode.tvdbId)) add("tvdb:aired", episode.seasonNumber, episode.episodeNumber, null);
        if (known(episode.absoluteEpisodeNumber)) {
          add(`${source}:absolute`, null, null, episode.absoluteEpisodeNumber);
          if (known(episode.tvdbId)) add("tvdb:absolute", null, null, episode.absoluteEpisodeNumber);
        }
        // Unverified scene mappings must not become acquisition identities.
        if (!episode.unverifiedSceneNumbering) {
          if (episode.sceneSeasonNumber != null && known(episode.sceneEpisodeNumber))
            add(`${source}:scene`, episode.sceneSeasonNumber, episode.sceneEpisodeNumber, null);
          if (known(episode.sceneAbsoluteEpisodeNumber))
            add(`${source}:scene-absolute`, null, null, episode.sceneAbsoluteEpisodeNumber);
        }
        return {
          key,
          title: this.#http.clean(episode.title),
          airDate: episode.airDateUtc ?? episode.airDate ?? null,
          runtimeMinutes: episode.runtime || null,
          numbering,
          refs: [
            { provider: "sonarr" as const, id: String(episode.id), instance, source: mappingSource },
            ...(known(episode.tvdbId) ? [{ provider: "tvdb" as const, id: String(episode.tvdbId), instance: null, source: mappingSource }] : []),
          ],
          identity: { stability: known(episode.tvdbId) ? "provider" as const : "ephemeral" as const, source: mappingSource },
        };
      }),
      // Sonarr's title/alternate-title strings do not establish anime relations.
      relations: [],
    };
  }

  async #catalog(series: SeriesWire): Promise<TvCatalogEntry> {
    const episodes = tvWire("sonarr", z.array(episodeSchema).max(50_000),
      await this.#http.get(`episode?seriesId=${series.id}`));
    return this.#entry(series, episodes);
  }

  async #resolveSeries(ref: SonarrSeriesRef): Promise<SeriesWire> {
    const parsed = tvWire("sonarr", z.object({
      tvdbId: localId.nullable().optional(),
      tmdbId: localId.nullable().optional(),
      sonarrSeriesId: localId.nullable().optional(),
    }).refine((r) => r.tvdbId || r.tmdbId), ref);
    const matches = (series: SeriesWire) =>
      (known(parsed.tvdbId) ? series.tvdbId === parsed.tvdbId : series.tmdbId === parsed.tmdbId) &&
      !(known(parsed.tvdbId) && known(series.tvdbId) && parsed.tvdbId !== series.tvdbId) &&
      !(known(parsed.tmdbId) && known(series.tmdbId) && parsed.tmdbId !== series.tmdbId);
    if (parsed.sonarrSeriesId) {
      const series = await this.#series(parsed.sonarrSeriesId);
      if (!matches(series)) throw new TvUpstreamError("sonarr", "local series ID failed the external identity cross-check");
      return series;
    }
    const series = tvWire("sonarr", z.array(seriesSchema).max(10_000),
      await this.#http.get(known(parsed.tvdbId) ? `series?tvdbId=${parsed.tvdbId}` : "series"));
    const candidates = series.filter(matches);
    if (candidates.length !== 1)
      throw new TvUpstreamError("sonarr", candidates.length ? "ambiguous external series identity" : "series not found by external identity");
    return candidates[0]!;
  }

  async resolve(ref: SonarrSeriesRef): Promise<TvCatalogEntry> { return this.#catalog(await this.#resolveSeries(ref)); }

  /** Artwork-only intake: exact work cross-check, no episode/release search or external mutation. */
  async poster(ref: SonarrSeriesRef): Promise<string | null> {
    const series = await this.#resolveSeries(ref);
    return tvPosterFromImages(series.images) ?? tvPosterUrl(series.remotePoster);
  }

  async get(key: string): Promise<TvCatalogEntry> {
    const external = /^(tvdb|tmdb):([1-9]\d*)$/.exec(key);
    if (external) return this.resolve({ [external[1]! + "Id"]: Number(external[2]) });
    const local = /^sonarr:([a-f0-9]{20}):([1-9]\d*)$/.exec(key);
    if (!local || local[1] !== this.instance)
      throw new TvUpstreamError("sonarr", "invalid or foreign-instance series key");
    return this.#catalog(await this.#series(Number(local[2])));
  }

  async resolveSeerr(request: SeerrTvRequest): Promise<TvCatalogEntry> {
    if (!approvedSeerrTv(request)) throw new TvUpstreamError("seerr", "request has no explicitly approved seasons");
    const entry = await this.resolve({
      tvdbId: request.media.tvdbId,
      tmdbId: request.media.tmdbId,
      // Seerr keeps separate Sonarr associations for standard and 4K requests.
      sonarrSeriesId: request.is4k ? request.media.externalServiceId4k : request.media.externalServiceId,
    });
    // The approved request supplies an explicit work-level bridge even when Sonarr omits TMDB.
    if (entry.tmdbId === null && request.media.tvdbId === entry.tvdbId)
      return { ...entry, tmdbId: request.media.tmdbId, refs: [...(entry.refs ?? []), {
        provider: "tmdb", id: String(request.media.tmdbId), instance: null, source: `seerr:request:${request.id}:tvdb-work-link`,
      }] };
    return entry;
  }
}

const observationSchema = z.object({
  id: z.number().int(),
  seriesId: localId.nullable().optional(),
  episodeId: localId.nullable().optional(),
  series: seriesSchema.nullable().optional(),
  episode: episodeSchema.nullable().optional(),
  title: z.string().optional(),
  sourceTitle: z.string().optional(),
  eventType: z.string().optional(),
  date: z.string().datetime({ offset: true }).optional(),
  sizeleft: z.number().nonnegative().optional(),
  sizeLeft: z.number().nonnegative().optional(),
  outputPath: z.string().nullable().optional(),
  trackedDownloadState: z.string().nullable().optional(),
  errorMessage: z.string().nullable().optional(),
  statusMessages: z.array(z.object({
    title: z.string().optional(), messages: z.array(z.string()).optional(),
  })).nullable().optional(),
  data: z.object({
    droppedPath: z.string().optional(), importedPath: z.string().optional(),
  }).nullable().optional(),
});

export interface SonarrObservation {
  key: string;
  entry: TvCatalogEntry;
  episodeKeys: string[];
  releaseTitle: string;
  sourcePaths: string[];
  libraryPath: string | null;
  observedAt: string;
  sonarr: { outcome: "imported" | "blocked"; messages: string[] };
}

/** Comparison evidence only. Never adopts paths or fans out acquisition jobs. */
export class SonarrObserver {
  readonly instance: string;
  #http: TvUpstreamHttp;
  #catalog: SonarrCatalog;
  #lookbackDays: number;
  constructor(settings: SonarrSettings, fetcher: TvFetcher = fetch) {
    this.#http = new TvUpstreamHttp("sonarr", {
      url: settings.url, apiKey: settings.apiKey,
    }, fetcher);
    this.#catalog = new SonarrCatalog(settings, fetcher);
    this.instance = this.#catalog.instance;
    this.#lookbackDays = tvWire("sonarr", z.number().min(1).max(365), settings.lookbackDays ?? 7);
  }

  async observe(): Promise<SonarrObservation[]> {
    const observations = new Map<string, SonarrObservation>();
    const entries = new Map<number, TvCatalogEntry>();
    const cutoff = Date.now() - this.#lookbackDays * 86_400_000;
    for (const kind of ["queue", "history"] as const) {
      const pages = new Set<string>();
      let previousDate = Infinity;
      for (let page = 1; page <= 10; page++) {
        const result = tvWire("sonarr", z.object({
          page: z.number().int().positive().optional(),
          pageSize: z.number().int().positive().optional(),
          records: z.array(observationSchema).max(100),
          totalRecords: z.number().int().nonnegative(),
        }), await this.#http.get(`${kind}?page=${page}&pageSize=100&includeSeries=true&includeEpisode=true&sortKey=${kind === "history" ? "date" : "added"}&sortDirection=descending`));
        const signature = JSON.stringify(result.records.map((r) => r.id));
        if ((result.page != null && result.page !== page) ||
            (result.pageSize != null && result.pageSize !== 100) ||
            (result.records.length && pages.has(signature)) ||
            result.records.length !== Math.min(100, Math.max(0, result.totalRecords - (page - 1) * 100)))
          throw new TvUpstreamError("sonarr", "incomplete or repeated observation page");
        pages.add(signature);
        for (const row of result.records) {
          if (kind === "history") {
            if (!row.date || Date.parse(row.date) > previousDate)
              throw new TvUpstreamError("sonarr", "history is missing a timestamp or not date-descending");
            previousDate = Date.parse(row.date);
          }
          const imported = kind === "history" && row.eventType === "downloadFolderImported";
          const blocked = kind === "queue" &&
            ((row.sizeleft ?? row.sizeLeft) === 0 || ["importBlocked", "importPending"].includes(row.trackedDownloadState ?? ""));
          if (!imported && !blocked) continue;
          if (imported && !row.date) throw new TvUpstreamError("sonarr", "import observation is missing its timestamp");
          if (imported && Date.parse(row.date!) < cutoff) continue;
          const seriesId = row.seriesId ?? row.series?.id;
          const episodeId = row.episodeId ?? row.episode?.id;
          // An unassociated queue item/season pack is not evidence for any episode.
          if (!seriesId || !episodeId) {
            if (imported) throw new TvUpstreamError("sonarr", "import observation is missing its episode association");
            continue;
          }
          if ((row.series && row.series.id !== seriesId) ||
              (row.episode && (row.episode.id !== episodeId || row.episode.seriesId !== seriesId)))
            throw new TvUpstreamError("sonarr", "conflicting observation identity");
          let entry = entries.get(seriesId);
          if (!entry) {
            entry = await this.#catalog.get(`sonarr:${this.instance}:${seriesId}`);
            entries.set(seriesId, entry);
          }
          if (row.series && (
            (known(row.series.tvdbId) && row.series.tvdbId !== entry.tvdbId) ||
            (known(row.series.tmdbId) && row.series.tmdbId !== entry.tmdbId)
          )) throw new TvUpstreamError("sonarr", "observation series failed the external identity cross-check");
          const episode = entry.episodes.find((e) => e.refs?.some((r) =>
            r.provider === "sonarr" && r.instance === this.instance && r.id === String(episodeId)));
          if (!episode || (row.episode && known(row.episode.tvdbId) && episode.key !== `tvdb:${row.episode.tvdbId}`))
            throw new TvUpstreamError("sonarr", "observation episode not found in the catalog identity");
          const paths = imported ? [row.data?.droppedPath, row.data?.importedPath] : [row.outputPath];
          const key = `sonarr:${this.instance}:${kind}:${row.id}`;
          observations.set(key, {
            key, entry, episodeKeys: [episode.key],
            releaseTitle: this.#http.clean(row.sourceTitle ?? row.title ?? entry.title),
            sourcePaths: paths.filter((p): p is string => !!p).map((p) => this.#http.clean(p)),
            libraryPath: imported && row.data?.importedPath ? this.#http.clean(row.data.importedPath) : null,
            observedAt: new Date().toISOString(),
            sonarr: {
              outcome: imported ? "imported" : "blocked",
              messages: [row.errorMessage ?? "", ...(row.statusMessages ?? []).flatMap((s) => [s.title ?? "", ...(s.messages ?? [])])]
                .filter(Boolean).map((m) => this.#http.clean(m)),
            },
          });
        }
        const done = page * 100 >= result.totalRecords ||
          (kind === "history" && result.records.every((r) => r.date && Date.parse(r.date) < cutoff));
        if (done) break;
        if (page === 10) throw new TvUpstreamError("sonarr", "observations exceeded the bounded scan");
      }
    }
    return [...observations.values()];
  }
}
