import { createHash } from "node:crypto";
import { z } from "zod";
import type { AppSettings } from "../app-types";
import { SonarrCatalog, type SonarrSeriesRef } from "./sonarr";
import { tvCatalogEntrySchema, tvPosterFromImages, type TvCatalogEntry } from "./types";
import type { TvFetcher } from "./upstream";

const id = z.number().int().positive().safe();
export const tvSearchResultSchema = z.object({
  tmdbId: id, title: z.string(), originalTitle: z.string(), year: z.number().int().nullable(),
  overview: z.string(), posterUrl: z.string().nullable(),
});
export const tvSeasonSchema = z.object({
  seasonNumber: z.number().int().nonnegative(), title: z.string(), episodeCount: z.number().int().nonnegative(),
  airDate: z.string().nullable(), requestable: z.boolean(), unavailableReason: z.string().nullable(),
});
export const tvSeriesDetailsSchema = tvSearchResultSchema.extend({
  numberingScheme: z.literal("tvdb:aired"), seasons: z.array(tvSeasonSchema),
});
export type TvSearchResult = z.infer<typeof tvSearchResultSchema>;
export type TvSeriesDetails = z.infer<typeof tvSeriesDetailsSchema>;
export type TvCatalogDetails = TvSeriesDetails & { episodeKeysBySeason?: ReadonlyMap<number, string[]> };
export class TvCatalogError extends Error {
  constructor(message: string, readonly kind: "configuration" | "unavailable" | "unsupported" = "unavailable") {
    super(message);
  }
}
export interface TvCatalog {
  search(query: string): Promise<TvSearchResult[]>;
  details(tmdbId: number): Promise<TvCatalogDetails>;
  resolve(tmdbId: number): Promise<TvCatalogEntry>;
}

/** Identity is provider-sourced; ordinal equality is never an identity bridge. */
export function canonicalTvSeasons(input: TvCatalogEntry, tmdbId: number) {
  const parsed = tvCatalogEntrySchema.safeParse(input);
  if (!parsed.success || parsed.data.tmdbId !== tmdbId || !parsed.data.tvdbId || !parsed.data.episodes.length)
    throw new TvCatalogError("Canonical TVDB work/episode mapping is unavailable; configure a verified TV catalog or make the series metadata available in Sonarr.", "unsupported");
  const entry = parsed.data;
  if ((entry.key.startsWith("tvdb:") && entry.key !== `tvdb:${entry.tvdbId}`) ||
      (entry.key.startsWith("tmdb:") && entry.key !== `tmdb:${tmdbId}`) ||
      entry.refs.some((ref) => ref.provider === "tvdb" && ref.id !== String(entry.tvdbId)))
    throw new TvCatalogError("Conflicting canonical TV work identity; correct the provider mapping before requesting.", "unsupported");
  const coordinates = new Set<string>(), identities = new Set<string>();
  const seasons = new Map<number, typeof entry.episodes>();
  for (const episode of entry.episodes) {
    const aired = episode.numbering.filter((n) => n.scheme === "tvdb:aired");
    const number = aired[0];
    if (!/^tvdb:[1-9]\d*$/.test(episode.key) || episode.identity?.stability === "ephemeral" ||
        identities.has(episode.key) || aired.length !== 1 || number?.season == null || number.episode == null ||
        number.episode < 1 || episode.refs.some((r) => r.provider === "tvdb" && r.id !== episode.key.slice(5)))
      throw new TvCatalogError("Unsupported or conflicting canonical TVDB episode identity/numbering; refresh or correct the provider mapping before requesting seasons.", "unsupported");
    const coordinate = `${number.season}:${number.episode}`;
    if (coordinates.has(coordinate))
      throw new TvCatalogError("Ambiguous TVDB aired mapping; multiple episodes claim one season/episode coordinate.", "unsupported");
    identities.add(episode.key); coordinates.add(coordinate);
    const members = seasons.get(number.season) ?? [];
    members.push(episode); seasons.set(number.season, members);
  }
  return { entry, seasons };
}

const tmdbSeries = z.object({
  id, name: z.string().min(1), original_name: z.string(), first_air_date: z.string().optional(),
  overview: z.string().default(""), poster_path: z.string().nullable().optional(),
});
const tmdbDetails = tmdbSeries.extend({
  external_ids: z.object({ tvdb_id: z.number().int().nonnegative().nullable().optional() }),
  seasons: z.array(z.object({ season_number: z.number().int().nonnegative(), name: z.string(),
    episode_count: z.number().int().nonnegative(), air_date: z.string().nullable().optional() })).max(500),
});
const providerDate = z.union([z.iso.date(), z.literal("")]).nullable().optional();
const skyhookShowSchema = z.object({
  tvdbId: id, tmdbId: z.number().int().nonnegative().nullable().optional(), title: z.string().min(1),
  runtime: z.number().nonnegative().nullable().optional(),
  images: z.unknown().optional(),
  episodes: z.array(z.object({
    tvdbId: id, tvdbShowId: id.optional(), seasonNumber: z.number().int().nonnegative(), episodeNumber: id,
    title: z.string().default(""), airDate: providerDate,
    airDateUtc: z.iso.datetime({ offset: true }).nullable().optional(),
    runtime: z.number().nonnegative().nullable().optional(),
    absoluteEpisodeNumber: z.number().int().nonnegative().nullable().optional(),
  })).min(1).max(50_000),
});
function summary(raw: z.infer<typeof tmdbSeries>): TvSearchResult {
  const year = Number(raw.first_air_date?.slice(0, 4));
  return { tmdbId: raw.id, title: raw.name, originalTitle: raw.original_name,
    year: Number.isInteger(year) && year > 0 ? year : null, overview: raw.overview,
    posterUrl: raw.poster_path && /^\/[a-zA-Z0-9._-]+$/.test(raw.poster_path)
      ? `https://image.tmdb.org/t/p/w342${raw.poster_path}` : null };
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () => reject(new TvCatalogError("TV catalog request timed out; retry later"));
    if (signal.aborted) { abort(); return; }
    signal.addEventListener("abort", abort, { once: true });
    promise.then((value) => { signal.removeEventListener("abort", abort); resolve(value); },
      (error) => { signal.removeEventListener("abort", abort); reject(error); });
  });
}

/** Bounded, coalesced GET cache shared by both providers; no retry or mutation capability. */
class CatalogHttp {
  private cache = new Map<string, { text: string; bytes: number; until: number }>();
  private pending = new Map<string, Promise<Response>>();
  private bytes = 0;
  constructor(private fetcher: TvFetcher, private now: () => number, private timeoutMs: number) {}
  fetch: TvFetcher = async (input, init) => {
    if (init?.method && init.method !== "GET") throw new TvCatalogError("Catalog transport only supports GET");
    const key = createHash("sha256").update(String(input)).update(JSON.stringify([...new Headers(init?.headers)])).digest("hex");
    const cached = this.cache.get(key);
    if (cached && cached.until > this.now()) return new Response(cached.text, { headers: { "content-type": "application/json" } });
    const inFlight = this.pending.get(key);
    if (inFlight) return (await inFlight).clone() as Awaited<ReturnType<TvFetcher>>;
    if (this.pending.size >= 8) throw new TvCatalogError("TV catalog is busy; retry shortly");
    const request = this.read(input, init, key);
    this.pending.set(key, request);
    try { return (await request).clone() as Awaited<ReturnType<TvFetcher>>; }
    finally { this.pending.delete(key); }
  };
  private async read(input: Parameters<TvFetcher>[0], init: RequestInit | undefined, key: string) {
    const host = new URL(String(input)).hostname;
    const provider = host === "api.themoviedb.org" ? "TMDB" : host === "skyhook.sonarr.tv" ? "SkyHook" : "Sonarr";
    const signal = AbortSignal.timeout(this.timeoutMs);
    let response: Response;
    try { response = await abortable(this.fetcher(input, { ...init, method: "GET", redirect: "error", signal }), signal); }
    catch { throw new TvCatalogError(`${provider} metadata is unreachable; check provider availability/configuration, then retry`); }
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      throw new TvCatalogError(`${provider} metadata returned HTTP ${response.status}; check provider availability/configuration or retry later`);
    }
    const reader = response.body?.getReader();
    if (!reader) throw new TvCatalogError("TV catalog returned an empty response");
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    try {
      for (;;) {
        const chunk = await abortable(reader.read(), signal);
        if (chunk.done) break;
        bytes += chunk.value.byteLength;
        if (bytes > 8 * 1024 * 1024) throw new TvCatalogError("TV catalog response exceeds the supported metadata size");
        chunks.push(chunk.value);
      }
    } catch (error) {
      void reader.cancel().catch(() => {});
      if (error instanceof TvCatalogError) throw error;
      throw new TvCatalogError("TV catalog response was interrupted; retry later");
    }
    const text = Buffer.concat(chunks).toString("utf8");
    try { JSON.parse(text); }
    catch { throw new TvCatalogError("TV catalog returned invalid JSON; check provider configuration"); }
    const old = this.cache.get(key);
    if (old) { this.bytes -= old.bytes; this.cache.delete(key); }
    while (this.cache.size >= 64 || this.bytes + bytes > 32 * 1024 * 1024) {
      const first = this.cache.keys().next().value!;
      this.bytes -= this.cache.get(first)!.bytes; this.cache.delete(first);
    }
    this.cache.set(key, { text, bytes, until: this.now() + 5 * 60_000 }); this.bytes += bytes;
    return new Response(text, { headers: { "content-type": "application/json" } });
  }
}

export class TmdbTvCatalog implements TvCatalog {
  #token: string;
  private http: CatalogHttp;
  private canonical: Pick<SonarrCatalog, "resolve"> | null;
  private authoritativeCatalog: boolean;
  constructor(token: string, sonarr: { url: string; apiKey: string }, options: {
    fetcher?: TvFetcher; now?: () => number; timeoutMs?: number;
    canonical?: { resolve(ref: SonarrSeriesRef): Promise<TvCatalogEntry> };
  } = {}) {
    this.#token = token;
    this.authoritativeCatalog = !!options.canonical;
    this.http = new CatalogHttp(options.fetcher ?? fetch, options.now ?? Date.now,
      z.number().int().min(1).max(15_000).parse(options.timeoutMs ?? 15_000));
    this.canonical = options.canonical ?? null;
    if (!this.canonical && sonarr.url && sonarr.apiKey) {
      try { this.canonical = new SonarrCatalog(sonarr, this.http.fetch); }
      catch { /* Discovery can still use public, read-only SkyHook metadata. */ }
    }
  }
  private async json<T>(route: string, schema: z.ZodType<T>): Promise<T> {
    const token = this.#token.trim();
    if (!token) throw new TvCatalogError("Configure the shared movies.tmdbAccessToken credential to discover TV titles.", "configuration");
    const url = new URL(`https://api.themoviedb.org/3${route}`);
    const headers: Record<string, string> = { Accept: "application/json" };
    if (/^[a-f0-9]{32}$/i.test(token)) url.searchParams.set("api_key", token);
    else headers.Authorization = `Bearer ${token}`;
    const response = await this.http.fetch(url, { headers });
    const result = schema.safeParse(await response.json());
    if (!result.success) throw new TvCatalogError("TV catalog returned unsupported metadata; check provider configuration");
    return result.data;
  }
  async search(query: string) {
    const q = z.string().trim().min(1).max(200).parse(query);
    const params = new URLSearchParams({ query: q, include_adult: "false", language: "en-US" });
    const result = await this.json(`/search/tv?${params}`, z.object({ results: z.array(tmdbSeries).max(100) }));
    return result.results.map(summary);
  }
  private async metadata(tmdbId: number) {
    id.parse(tmdbId);
    const metadata = await this.json(`/tv/${tmdbId}?append_to_response=external_ids&language=en-US`, tmdbDetails);
    if (metadata.id !== tmdbId) throw new TvCatalogError("TV catalog returned a different work identity", "unsupported");
    return metadata;
  }
  private async mapped(metadata: z.infer<typeof tmdbDetails>) {
    if (!metadata.external_ids.tvdb_id) throw new TvCatalogError("TMDB does not supply a TVDB work mapping for this title; request is unsupported until a verified mapping is available.", "unsupported");
    let raw: TvCatalogEntry | undefined;
    if (this.canonical) {
      try { raw = await this.canonical.resolve({ tmdbId: metadata.id, tvdbId: metadata.external_ids.tvdb_id }); }
      catch {
        if (this.authoritativeCatalog)
          throw new TvCatalogError("Configured canonical catalog could not resolve this exact TMDB/TVDB work; correct its identity mapping before requesting.", "unsupported");
      }
    }
    raw ??= await this.skyhook(metadata);
    if (raw.tvdbId !== metadata.external_ids.tvdb_id)
      throw new TvCatalogError("TMDB and canonical catalog disagree on TVDB work identity; correct the mapping before requesting.", "unsupported");
    return canonicalTvSeasons({ ...raw, posterUrl: summary(metadata).posterUrl ?? raw.posterUrl ?? null }, metadata.id);
  }
  private async skyhook(metadata: z.infer<typeof tmdbDetails>): Promise<TvCatalogEntry> {
    const tvdbId = metadata.external_ids.tvdb_id!;
    // Sonarr main's SkyHookTvdb/GetSeriesInfo contract: provider IDs + TVDB aired coordinates.
    // GET /series/lookup discards the episode list, so it cannot supply this identity evidence.
    const source = `skyhook:v1:tvdb:aired:${tvdbId}`;
    let show: z.infer<typeof skyhookShowSchema>;
    try {
      const response = await this.http.fetch(`https://skyhook.sonarr.tv/v1/tvdb/shows/en/${tvdbId}`);
      const parsed = skyhookShowSchema.safeParse(await response.json());
      if (!parsed.success) throw new TvCatalogError("SkyHook returned unsupported TVDB work/episode metadata; a verified mapping is required before requesting.", "unsupported");
      show = parsed.data;
    } catch (error) {
      if (error instanceof TvCatalogError) throw error;
      throw new TvCatalogError("TVDB metadata is unavailable from Sonarr/SkyHook; retry later or configure a verified canonical catalog.");
    }
    if (show.tvdbId !== tvdbId || (show.tmdbId && show.tmdbId !== metadata.id) ||
        show.episodes.some((episode) => episode.tvdbShowId !== undefined && episode.tvdbShowId !== tvdbId))
      throw new TvCatalogError("TMDB/SkyHook work or episode ownership disagrees; correct the provider identity mapping before requesting.", "unsupported");
    return { key: `tvdb:${tvdbId}`, tvdbId, tmdbId: metadata.id, title: show.title,
      aliases: [...new Set([metadata.name, metadata.original_name].filter((name) => name !== show.title))],
      year: summary(metadata).year, posterUrl: summary(metadata).posterUrl ?? tvPosterFromImages(show.images), anime: false, source, relations: [],
      refs: [ { provider: "tvdb", id: String(tvdbId), source }, { provider: "tmdb", id: String(metadata.id), source: "tmdb:external_ids" } ],
      episodes: show.episodes.map((episode) => ({
        key: `tvdb:${episode.tvdbId}`, title: episode.title,
        airDate: episode.airDateUtc ?? (episode.airDate || null), runtimeMinutes: episode.runtime || show.runtime || null,
        identity: { stability: "provider", source },
        refs: [{ provider: "tvdb", id: String(episode.tvdbId), source }],
        numbering: [{ scheme: "tvdb:aired", season: episode.seasonNumber, episode: episode.episodeNumber, absolute: null, source },
          ...(episode.absoluteEpisodeNumber ? [{ scheme: "tvdb:absolute", season: null, episode: null,
            absolute: episode.absoluteEpisodeNumber, source }] : [])],
      })),
    };
  }
  async resolve(tmdbId: number) { return (await this.mapped(await this.metadata(tmdbId))).entry; }
  async details(tmdbId: number): Promise<TvCatalogDetails> {
    const metadata = await this.metadata(tmdbId);
    let seasons: TvSeriesDetails["seasons"];
    let episodeKeysBySeason: TvCatalogDetails["episodeKeysBySeason"];
    try {
      const mapped = await this.mapped(metadata);
      episodeKeysBySeason = new Map([...mapped.seasons].map(([season, episodes]) => [season, episodes.map(ep => ep.key)]));
      seasons = [...mapped.seasons].sort(([a], [b]) => a - b).map(([seasonNumber, episodes]) => ({
        seasonNumber, title: seasonNumber === 0 ? "Specials" : `Season ${seasonNumber}`, episodeCount: episodes.length,
        airDate: episodes.map((ep) => ep.airDate).filter((date): date is string => !!date).sort()[0] ?? null,
        requestable: true, unavailableReason: null,
      }));
    } catch (error) {
      if (!(error instanceof TvCatalogError)) throw error;
      // TMDB-only ordinals are display metadata, never a requestable TVDB mapping.
      seasons = metadata.seasons.map((s) => ({ seasonNumber: s.season_number, title: s.name,
        episodeCount: s.episode_count, airDate: s.air_date ?? null, requestable: false, unavailableReason: error.message }));
    }
    return { ...summary(metadata), numberingScheme: "tvdb:aired", seasons, episodeKeysBySeason };
  }
}

const catalogs = new Map<string, TvCatalog>();
export function configuredTvCatalog(settings: AppSettings): TvCatalog {
  const key = createHash("sha256").update(JSON.stringify([
    settings.movies.tmdbAccessToken, settings.tv.sonarrUrl, settings.tv.sonarrApiKey,
  ])).digest("hex");
  let catalog = catalogs.get(key);
  if (!catalog) {
    catalog = new TmdbTvCatalog(settings.movies.tmdbAccessToken, { url: settings.tv.sonarrUrl, apiKey: settings.tv.sonarrApiKey });
    if (catalogs.size >= 4) catalogs.delete(catalogs.keys().next().value!);
    catalogs.set(key, catalog);
  }
  return catalog;
}
