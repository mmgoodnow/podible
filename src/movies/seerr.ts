import { z } from "zod";
import { createHash } from "node:crypto";
import type { MovieCatalog } from "./catalog";
import type { CatalogMovie } from "./types";
function baseUrl(url: string): URL {
  try {
    const base = new URL(url.replace(/\/+$/, "") + "/");
    if (
      !["http:", "https:"].includes(base.protocol) ||
      base.username ||
      base.password ||
      base.search ||
      base.hash
    )
      throw new Error();
    return base;
  } catch {
    throw new Error(
      "Configure a valid Seerr URL without embedded credentials or query parameters",
    );
  }
}
export const seerrSettingsSchema = z
  .object({
    url: z
      .string()
      .refine((value) => {
        if (!value) return true;
        try {
          baseUrl(value);
          return true;
        } catch {
          return false;
        }
      }, "Invalid Seerr URL")
      .default(""),
    apiKey: z.string().default(""),
    pollIntervalMinutes: z.number().int().min(1).max(60).default(1),
  })
  .strict();
const timestamp = z.string().datetime({ offset: true });
export const seerrRequestSchema = z.object({
  id: z.number().int().positive(),
  status: z.number().int().min(1).max(5),
  type: z.enum(["movie", "tv"]),
  is4k: z.boolean(),
  createdAt: timestamp,
  updatedAt: timestamp,
  media: z.object({ tmdbId: z.number().int().positive() }),
  requestedBy: z.object({
    id: z.number().int().positive(),
    plexId: z.number().int().nonnegative().nullable().optional(),
    displayName: z.string().nullable().optional(),
    username: z.string().nullable().optional(),
  }),
});
export type SeerrRequest = z.infer<typeof seerrRequestSchema>;
export interface SeerrSource {
  latest(): Promise<SeerrRequest | null>;
  changes(
    since: string,
  ): Promise<{ requests: SeerrRequest[]; through: string }>;
  request(id: number): Promise<SeerrRequest>;
  movie(id: number): Promise<CatalogMovie>;
}
export function seerrInstance(url: string) {
  return createHash("sha256")
    .update(baseUrl(url).href)
    .digest("hex")
    .slice(0, 20);
}
export function approvedSeerrMovie(request: SeerrRequest): boolean {
  // Seerr request statuses: pending=1, approved=2, declined=3, failed=4, completed=5.
  return request.type === "movie" && [2, 4, 5].includes(request.status);
}
const movieWireSchema = z.object({
  id: z.number().int().positive(),
  title: z.string(),
  originalTitle: z.string().nullable().optional(),
  releaseDate: z.string().nullable().optional(),
  originalLanguage: z.string().nullable().optional(),
  overview: z.string().nullable().optional(),
  posterPath: z.string().nullable().optional(),
  runtime: z.number().nullable().optional(),
  imdbId: z.string().nullable().optional(),
  externalIds: z
    .object({ imdbId: z.string().nullable().optional() })
    .optional(),
});
function movie(raw: unknown): CatalogMovie {
  const m = movieWireSchema.parse(raw),
    year = Number(m.releaseDate?.slice(0, 4));
  return {
    tmdbId: m.id,
    title: m.title,
    originalTitle: m.originalTitle ?? m.title,
    year: Number.isInteger(year) && year > 0 ? year : null,
    originalLanguage: m.originalLanguage ?? "und",
    overview: m.overview ?? "",
    posterUrl:
      m.posterPath && /^\/[\w.-]+$/.test(m.posterPath)
        ? `https://image.tmdb.org/t/p/w342${m.posterPath}`
        : null,
    runtimeMinutes: m.runtime && m.runtime > 0 ? m.runtime : null,
    imdbId: m.imdbId ?? m.externalIds?.imdbId ?? null,
    ...(m.releaseDate && z.iso.date().safeParse(m.releaseDate).success ? { releaseDate: m.releaseDate } : {}),
  };
}

/** GET-only Seerr adapter: metadata and approved intent, never Radarr commands. */
export class SeerrClient implements SeerrSource, MovieCatalog {
  constructor(
    private settings: z.infer<typeof seerrSettingsSchema>,
    private fetcher: (
      ...args: Parameters<typeof fetch>
    ) => ReturnType<typeof fetch> = fetch,
  ) {}
  private async getJson(resource: string) {
    const base = baseUrl(this.settings.url);
    if (!this.settings.apiKey) throw new Error("Configure the Seerr API key");
    let response: Response;
    try {
      response = await this.fetcher(new URL(`api/v1/${resource}`, base), {
        method: "GET",
        headers: { "X-Api-Key": this.settings.apiKey },
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      });
    } catch {
      throw new Error("Seerr request feed is unavailable");
    }
    if (!response.ok)
      throw new Error(
        `Seerr returned HTTP ${response.status}; check connection, URL base and credentials`,
      );
    try {
      return await response.json();
    } catch {
      throw new Error("Seerr returned invalid JSON");
    }
  }
  private async page(sort: "added" | "modified", skip: number, take = 100) {
    return z
      .object({
        results: z.array(seerrRequestSchema),
        pageInfo: z.object({ results: z.number().int().nonnegative() }),
      })
      .parse(
        await this.getJson(
          `request?take=${take}&skip=${skip}&sort=${sort}&sortDirection=desc&mediaType=movie`,
        ),
      );
  }
  async latest() {
    return (await this.page("added", 0, 1)).results[0] ?? null;
  }
  async changes(since: string) {
    const cursor = new Date(timestamp.parse(since)).toISOString();
    const requests = new Map<number, SeerrRequest>();
    let through = cursor;
    const cutoff = Date.parse(cursor) - 120_000;
    for (let page = 0; page < 100; page++) {
      const result = await this.page("modified", page * 100);
      for (const request of result.results) {
        const date = new Date(request.updatedAt),
          prior = requests.get(request.id);
        if (
          date.getTime() >= cutoff &&
          (!prior || date.getTime() >= Date.parse(prior.updatedAt))
        )
          requests.set(request.id, request);
        if (date.toISOString() > through) through = date.toISOString();
      }
      if (
        result.results.every((r) => Date.parse(r.updatedAt) < cutoff) ||
        (page + 1) * 100 >= result.pageInfo.results
      )
        return { requests: [...requests.values()], through };
    }
    throw new Error(
      "Seerr changes exceeded the bounded scan; cursor was not advanced",
    );
  }
  async request(id: number) {
    z.number().int().positive().parse(id);
    const request = seerrRequestSchema.parse(
      await this.getJson(`request/${id}`),
    );
    if (request.id !== id)
      throw new Error("Seerr returned a different request identity");
    return request;
  }
  async movie(id: number) {
    z.number().int().positive().parse(id);
    const result = movie(await this.getJson(`movie/${id}`));
    if (result.tmdbId !== id)
      throw new Error("Seerr returned a different movie identity");
    return result;
  }
  get(id: number) {
    return this.movie(id);
  }
  async search(query: string) {
    if (!query.trim()) return [];
    const r = z
      .object({ results: z.array(z.record(z.string(), z.unknown())) })
      .parse(
        await this.getJson(
          `search?query=${encodeURIComponent(query.trim())}&page=1`,
        ),
      );
    return r.results.filter((m) => m.mediaType === "movie").map(movie);
  }
}
