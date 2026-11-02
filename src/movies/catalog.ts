import { z } from "zod";
import { type CatalogMovie } from "./types";

const entrySchema = z.object({
  id: z.number().int().positive(),
  title: z.string(),
  original_title: z.string(),
  release_date: z.string().optional(),
  overview: z.string().default(""),
  poster_path: z.string().nullable().optional(),
  original_language: z.string(),
  imdb_id: z.string().nullable().optional(),
  runtime: z.number().nullable().optional(),
});
function normalize(raw: z.infer<typeof entrySchema>): CatalogMovie {
  const year = Number(raw.release_date?.slice(0, 4));
  return {
    tmdbId: raw.id,
    title: raw.title,
    originalTitle: raw.original_title,
    year: Number.isInteger(year) && year > 0 ? year : null,
    overview: raw.overview,
    originalLanguage: raw.original_language,
    posterUrl:
      raw.poster_path && /^\/[a-zA-Z0-9._-]+$/.test(raw.poster_path)
        ? `https://image.tmdb.org/t/p/w342${raw.poster_path}`
        : null,
    imdbId: raw.imdb_id ?? null,
    runtimeMinutes: raw.runtime && raw.runtime > 0 ? raw.runtime : null,
  };
}
export class CatalogUnavailableError extends Error {
  constructor(
    message: string,
    readonly retryAfterSeconds: number | null = null,
  ) {
    super(message);
  }
}
export interface MovieCatalog {
  search(query: string): Promise<CatalogMovie[]>;
  get(tmdbId: number): Promise<CatalogMovie>;
}
/** The catalog owns upstream URLs. RPC callers supply IDs, never fetch URLs. */
export class TmdbCatalog implements MovieCatalog {
  private cache = new Map<string, { until: number; value: unknown }>();
  constructor(
    private token: string,
    private fetcher: (
      url: string,
      init?: RequestInit,
    ) => Promise<Response> = fetch,
  ) {}
  private async json(route: string): Promise<unknown> {
    if (!this.token.trim())
      throw new CatalogUnavailableError(
        "TMDB is not configured by the administrator",
      );
    const cached = this.cache.get(route);
    if (cached && cached.until > Date.now()) return cached.value;
    const response = await this.fetcher(
      `https://api.themoviedb.org/3${route}`,
      {
        headers: {
          Authorization: `Bearer ${this.token}`,
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(15_000),
        redirect: "error",
      },
    );
    if (!response.ok) {
      const raw = response.headers.get("retry-after");
      const delay = raw === null ? NaN : Number(raw);
      throw new CatalogUnavailableError(
        `TMDB returned ${response.status}`,
        Number.isFinite(delay) ? Math.max(0, delay) : null,
      );
    }
    const value: unknown = await response.json();
    if (this.cache.size >= 100)
      this.cache.delete(this.cache.keys().next().value!);
    this.cache.set(route, { until: Date.now() + 10 * 60_000, value });
    return value;
  }
  async search(query: string): Promise<CatalogMovie[]> {
    const q = query.trim();
    if (!q) return [];
    const params = new URLSearchParams({
      query: q,
      include_adult: "false",
      language: "en-US",
    });
    return z
      .object({ results: z.array(entrySchema) })
      .parse(await this.json(`/search/movie?${params}`))
      .results.map(normalize);
  }
  async get(tmdbId: number): Promise<CatalogMovie> {
    z.number().int().positive().parse(tmdbId);
    return normalize(
      entrySchema.parse(await this.json(`/movie/${tmdbId}?language=en-US`)),
    );
  }
}
