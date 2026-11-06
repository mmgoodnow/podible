import { createHash } from "node:crypto";
import { z } from "zod";
import {
  seerrInstance,
  seerrRequestSchema,
  seerrSettingsSchema,
} from "../movies/seerr";

export type TvProvider = "sonarr" | "seerr";
export type TvFetcher = (
  ...args: Parameters<typeof fetch>
) => ReturnType<typeof fetch>;

/** Retry data is for the caller's durable Reflow wait, never a fetch retry loop. */
export class TvUpstreamError extends Error {
  constructor(
    readonly provider: TvProvider,
    message: string,
    readonly status: number | null = null,
    readonly retryable = false,
    readonly retryAfterMs = 60_000,
  ) {
    super(`${provider === "sonarr" ? "Sonarr" : "Seerr"}: ${message}`);
    this.name = "TvUpstreamError";
  }
}

function providerUrl(value: string, provider: TvProvider): URL {
  try {
    const base = new URL(value.replace(/\/+$/, "") + "/");
    if (
      !["http:", "https:"].includes(base.protocol) ||
      base.username || base.password || base.search || base.hash
    ) throw new Error();
    return base;
  } catch {
    throw new TvUpstreamError(provider, "configure an HTTP(S) URL without embedded credentials, query or fragment");
  }
}

export function sonarrInstance(url: string): string {
  return createHash("sha256").update(providerUrl(url, "sonarr").href)
    .digest("hex").slice(0, 20);
}

function retryDelay(value: string | null): number {
  if (!value?.trim()) return 60_000;
  const delay = /^\d+(?:\.\d+)?$/.test(value.trim())
    ? Number(value) * 1000
    : Date.parse(value) - Date.now();
  return Number.isFinite(delay)
    ? Math.max(1000, Math.min(Number.MAX_SAFE_INTEGER, Math.ceil(delay)))
    : 60_000;
}

/** Server-only transport. ECMAScript private fields cannot serialize credentials. */
export class TvUpstreamHttp {
  #base: URL;
  #apiKey: string;
  #fetcher: TvFetcher;
  #timeoutMs: number;
  constructor(
    readonly provider: TvProvider,
    settings: { url: string; apiKey: string },
    fetcher: TvFetcher = fetch,
    timeoutMs = 30_000,
  ) {
    this.#base = providerUrl(settings.url, provider);
    if (!settings.apiKey.trim()) throw new TvUpstreamError(provider, "configure the server API key");
    this.#apiKey = settings.apiKey;
    this.#fetcher = fetcher;
    this.#timeoutMs = timeoutMs;
  }

  clean(value: string): string {
    return value.replaceAll(this.#apiKey, "[redacted]")
      .replace(/https?:\/\/[^\s<>"']+/gi, "[redacted URL]")
      .replace(/\b(api[-_]?key|passkey|token|authorization)\s*[:=]\s*[^\s,;]+/gi, "$1=[redacted]")
      .slice(0, 2000);
  }

  async get(resource: string): Promise<unknown> {
    // Only adapter-owned relative routes may receive the authentication header.
    if (!/^[a-z][a-z\d/-]*(?:\?[^#]*)?$/i.test(resource) || resource.includes(".."))
      throw new TvUpstreamError(this.provider, "invalid adapter resource");
    const signal = AbortSignal.timeout(this.#timeoutMs);
    let response: Response;
    try {
      response = await this.#fetcher(new URL(
        `${this.provider === "sonarr" ? "api/v3" : "api/v1"}/${resource}`,
        this.#base,
      ), {
        method: "GET",
        redirect: "manual",
        headers: { "X-Api-Key": this.#apiKey },
        signal,
      });
    } catch {
      throw new TvUpstreamError(this.provider, signal.aborted ? "request timed out" : "request unavailable", null, true);
    }
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      throw new TvUpstreamError(
        this.provider,
        response.status >= 300 && response.status < 400
          ? "API redirect refused; check the configured URL base"
          : `HTTP ${response.status}`,
        response.status,
        response.status === 429 || response.status === 408 || response.status >= 500,
        retryDelay(response.headers.get("retry-after")),
      );
    }
    try {
      return await response.json();
    } catch {
      throw new TvUpstreamError(this.provider, signal.aborted ? "response timed out" : "invalid JSON response", null, signal.aborted);
    }
  }
}

export function tvWire<T>(provider: TvProvider, schema: z.ZodType<T>, value: unknown): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new TvUpstreamError(provider, "invalid metadata; cursor was not advanced");
  return parsed.data;
}

const id = z.number().int().positive();
const timestamp = z.string().datetime({ offset: true });
const status = z.number().int().min(1).max(5);
export const seerrTvRequestSchema = seerrRequestSchema.extend({
  type: z.literal("tv"),
  media: seerrRequestSchema.shape.media.extend({
    tvdbId: id.nullable().optional(),
    externalServiceId: id.nullable().optional(),
    externalServiceId4k: id.nullable().optional(),
  }),
  seasons: z.array(z.object({
    seasonNumber: z.number().int().nonnegative(),
    status,
  })).min(1).superRefine((seasons, ctx) => {
    if (new Set(seasons.map((s) => s.seasonNumber)).size !== seasons.length)
      ctx.addIssue({ code: "custom", message: "Duplicate season request" });
  }),
});
export type SeerrTvRequest = z.infer<typeof seerrTvRequestSchema>;
export type SeerrTvSettings = z.infer<typeof seerrSettingsSchema>;
export type SeerrTvSeason = {
  tmdbId: number; seasonNumber: number;
  episodes: { id: number; seasonNumber: number; episodeNumber: number; title: string;
    airDate: string | null; runtimeMinutes: number | null; overview: string; tvdbId: number | null }[];
};
export interface SeerrTvSource {
  latest(): Promise<SeerrTvRequest | null>;
  changes(since: string): Promise<{ requests: SeerrTvRequest[]; through: string }>;
  request(id: number): Promise<SeerrTvRequest>;
  season?(tmdbId: number, seasonNumber: number): Promise<SeerrTvSeason>;
}

const approved = (status: number) => [2, 4, 5].includes(status);
/** Failed/completed are post-approval states, as in the movie feed. */
export function selectedSeerrTvSeasons(request: SeerrTvRequest): number[] {
  const parsed = tvWire("seerr", seerrTvRequestSchema, request);
  if (!approved(parsed.status)) return [];
  return parsed.seasons.filter((s) => approved(s.status))
    .map((s) => s.seasonNumber).sort((a, b) => a - b);
}
export function approvedSeerrTv(request: SeerrTvRequest): boolean {
  return selectedSeerrTvSeasons(request).length > 0;
}

/** A stateless feed: only the caller may persist a baseline or commit a cursor. */
export class SeerrTvClient implements SeerrTvSource {
  readonly instance: string;
  #http: TvUpstreamHttp;
  constructor(settings: SeerrTvSettings, fetcher: TvFetcher = fetch) {
    this.#http = new TvUpstreamHttp("seerr", settings, fetcher);
    this.instance = seerrInstance(settings.url);
  }
  async #page(sort: "added" | "modified", skip: number, take = 100) {
    const result = tvWire("seerr", z.object({
      results: z.array(seerrTvRequestSchema).max(take),
      pageInfo: z.object({ results: z.number().int().nonnegative() }),
    }), await this.#http.get(`request?take=${take}&skip=${skip}&sort=${sort}&sortDirection=desc&mediaType=tv`));
    if (result.results.length !== Math.min(take, Math.max(0, result.pageInfo.results - skip)))
      throw new TvUpstreamError("seerr", "incomplete request page; cursor was not advanced");
    return result;
  }
  async latest() {
    return (await this.#page("added", 0, 1)).results[0] ?? null;
  }
  async changes(since: string) {
    const cursor = new Date(tvWire("seerr", timestamp, since)).toISOString();
    const cutoff = Date.parse(cursor) - 120_000;
    const requests = new Map<number, SeerrTvRequest>();
    const pages = new Set<string>();
    let through = cursor;
    let previous = Infinity;
    for (let page = 0; page < 100; page++) {
      const result = await this.#page("modified", page * 100);
      const signature = JSON.stringify(result.results.map((r) => [r.id, r.updatedAt]));
      if (result.results.length && pages.has(signature))
        throw new TvUpstreamError("seerr", "repeated request page; cursor was not advanced");
      pages.add(signature);
      for (const request of result.results) {
        const modified = Date.parse(request.updatedAt);
        if (modified > previous)
          throw new TvUpstreamError("seerr", "request page is not modified-descending; cursor was not advanced");
        previous = modified;
        const prior = requests.get(request.id);
        if (modified >= cutoff && (!prior || modified >= Date.parse(prior.updatedAt)))
          requests.set(request.id, request);
        const date = new Date(modified).toISOString();
        if (date > through) through = date;
      }
      if (result.results.every((r) => Date.parse(r.updatedAt) < cutoff) ||
          (page + 1) * 100 >= result.pageInfo.results)
        return { requests: [...requests.values()], through };
    }
    throw new TvUpstreamError("seerr", "changes exceeded the bounded scan; cursor was not advanced");
  }
  async request(requestId: number) {
    tvWire("seerr", id, requestId);
    const result = tvWire("seerr", seerrTvRequestSchema, await this.#http.get(`request/${requestId}`));
    if (result.id !== requestId)
      throw new TvUpstreamError("seerr", "different request identity; cursor was not advanced");
    return result;
  }

  async season(tmdbId: number, seasonNumber: number): Promise<SeerrTvSeason> {
    tvWire("seerr", id, tmdbId);
    tvWire("seerr", z.number().int().nonnegative(), seasonNumber);
    const result = tvWire("seerr", z.object({
      seasonNumber: z.number().int().nonnegative(),
      episodes: z.array(z.object({
        id, showId: id.optional(), seasonNumber: z.number().int().nonnegative(), episodeNumber: id,
        name: z.string().max(2000), airDate: z.union([z.iso.date(), z.literal("")]).nullable().optional(),
        runtime: z.number().nonnegative().nullable().optional(), overview: z.string().default(""),
        externalIds: z.object({ tvdbId: id.nullable().optional() }).optional(),
      })).max(2000),
    }), await this.#http.get(`tv/${tmdbId}/season/${seasonNumber}`));
    if (result.seasonNumber !== seasonNumber || result.episodes.some(ep => ep.seasonNumber !== seasonNumber || (ep.showId !== undefined && ep.showId !== tmdbId)) ||
        new Set(result.episodes.map(ep => ep.id)).size !== result.episodes.length ||
        new Set(result.episodes.map(ep => ep.episodeNumber)).size !== result.episodes.length)
      throw new TvUpstreamError("seerr", "conflicting season/episode ownership or identity");
    return { tmdbId, seasonNumber, episodes: result.episodes.map(ep => ({
      id: ep.id, seasonNumber, episodeNumber: ep.episodeNumber, title: this.#http.clean(ep.name),
      airDate: ep.airDate || null, runtimeMinutes: ep.runtime || null, overview: this.#http.clean(ep.overview), tvdbId: ep.externalIds?.tvdbId ?? null,
    })) };
  }
}
