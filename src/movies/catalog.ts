import { z } from "zod";
import { type CatalogMovie, type MoviePageExtras } from "./types";

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
const imagePath = (value: string | null | undefined, size: "w185" | "w342") =>
  value && /^\/[a-zA-Z0-9._-]+$/.test(value) ? `https://image.tmdb.org/t/p/${size}${value}` : null;
function normalize(raw: z.infer<typeof entrySchema>): CatalogMovie {
  const year = Number(raw.release_date?.slice(0, 4));
  return {
    tmdbId: raw.id,
    title: raw.title,
    originalTitle: raw.original_title,
    year: Number.isInteger(year) && year > 0 ? year : null,
    overview: raw.overview,
    originalLanguage: raw.original_language,
    posterUrl: imagePath(raw.poster_path, "w342"),
    imdbId: raw.imdb_id ?? null,
    runtimeMinutes: raw.runtime && raw.runtime > 0 ? raw.runtime : null,
  };
}
const label = z.string().trim().min(1).max(300);
const person = z.object({ id: z.number().int().positive(), name: label });
const actor = person.extend({ character: z.string().trim().max(300).nullish(), profile_path: z.string().nullish() });
const relatedEntry = entrySchema.extend({ adult: z.boolean().optional() });
const creditsSchema = z.object({ cast: z.array(z.unknown()).optional(), crew: z.array(z.unknown()).optional() });
const relatedSchema = z.object({ results: z.array(z.unknown()) });
const extrasWireSchema = z.object({
  id: z.number().int().positive(),
  runtime: z.unknown(), genres: z.unknown(), credits: z.unknown(), recommendations: z.unknown(), similar: z.unknown(),
});
function validRows<S extends z.ZodType>(schema: S, rows: unknown[]) {
  return rows.flatMap(row => {
    const parsed = schema.safeParse(row);
    return parsed.success ? [parsed.data] : [];
  });
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
  pageExtras?(tmdbId: number): Promise<MoviePageExtras>;
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
    const credential = this.token.trim();
    const url = new URL(`https://api.themoviedb.org/3${route}`);
    const headers: Record<string, string> = { Accept: "application/json" };
    // TMDB issues both legacy v3 API keys and v4 bearer read-access tokens.
    if (/^[a-f0-9]{32}$/i.test(credential))
      url.searchParams.set("api_key", credential);
    else headers.Authorization = `Bearer ${credential}`;
    let response: Response;
    try {
      response = await this.fetcher(url.href, {
        headers,
        signal: AbortSignal.timeout(15_000),
        redirect: "error",
      });
    } catch {
      throw new CatalogUnavailableError("TMDB is temporarily unreachable");
    }
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
  async pageExtras(tmdbId: number): Promise<MoviePageExtras> {
    z.number().int().positive().parse(tmdbId);
    const raw = extrasWireSchema.parse(await this.json(
      `/movie/${tmdbId}?language=en-US&append_to_response=credits,recommendations,similar`,
    ));
    if (raw.id !== tmdbId) throw new CatalogUnavailableError("TMDB returned a different movie");
    const credits = creditsSchema.safeParse(raw.credits).data;
    const directors = validRows(person.extend({ job: z.literal("Director") }), credits?.crew ?? []);
    const cast = validRows(actor, credits?.cast ?? []);
    const seen = new Set([tmdbId]);
    const related = [raw.recommendations, raw.similar].flatMap(section => {
      const rows = relatedSchema.safeParse(section).data?.results ?? [];
      return validRows(relatedEntry, rows).flatMap(entry => {
        if (entry.adult || !entry.title.trim() || seen.has(entry.id)) return [];
        seen.add(entry.id);
        const { tmdbId, title, year, posterUrl } = normalize(entry);
        return [{ tmdbId, title, year, posterUrl }];
      });
    }).slice(0, 12);
    return {
      runtimeMinutes: z.number().int().positive().safeParse(raw.runtime).data ?? null,
      genres: [...new Set(validRows(z.object({ name: label }), Array.isArray(raw.genres) ? raw.genres : []).map(row => row.name))].slice(0, 8),
      directors: [...new Set(directors.map(row => row.name))].slice(0, 8),
      cast: cast.filter((row, i) => cast.findIndex(person => person.id === row.id) === i).slice(0, 12)
        .map(({ id, name, character, profile_path }) => ({ id, name, character: character ?? "", profileUrl: imagePath(profile_path, "w185") })),
      related,
    };
  }
}
