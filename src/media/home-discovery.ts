import { createHash } from "node:crypto";
import { z } from "zod";
import { backdropUrlSchema, tmdbImageUrl } from "./tmdb-artwork";

const item = z.object({ tmdbId: z.number().int().positive(), title: z.string(), year: z.number().nullable(),
  posterUrl: z.string().nullable(), backdropUrl: backdropUrlSchema });
const section = z.object({ items: z.array(item), unavailable: z.boolean() });
export const homeDiscoverySchema = z.object({ configured: z.boolean(), movies: section, tv: section });
type Section = z.infer<typeof section>;
const upstream = z.object({ results: z.array(z.object({ id: z.number().int().positive(), adult: z.boolean().optional(),
  title: z.string().optional(), name: z.string().optional(), release_date: z.string().optional(), first_air_date: z.string().optional(),
  poster_path: z.string().nullable().optional(), backdrop_path: z.string().nullable().optional().catch(null) })).max(100) });

/** Fixed public lists only. No per-title enrichment, user-specific cache entries or raw upstream errors. */
export class HomeDiscovery {
  private cache = new Map<string, { until: number; value: Promise<Section> }>();
  constructor(private fetcher: typeof fetch = fetch, private now: () => number = Date.now) {}
  async get(rawToken: string | null | undefined): Promise<z.infer<typeof homeDiscoverySchema>> {
    const token = rawToken?.trim();
    const empty = { items: [], unavailable: false };
    if (!token) return { configured: false, movies: empty, tv: empty };
    const [movies, tv] = await Promise.all([this.list(token, "movie"), this.list(token, "tv")]);
    return { configured: true, movies, tv };
  }
  private list(token: string, kind: "movie" | "tv"): Promise<Section> {
    const key = createHash("sha256").update(token).digest("hex") + kind;
    const cached = this.cache.get(key);
    if (cached && cached.until > this.now()) return cached.value;
    // Bound credential churn and concurrency without evicting an in-flight request.
    if (this.cache.size >= 16 && !this.cache.has(key)) {
      for (const [k, entry] of this.cache) if (entry.until <= this.now()) this.cache.delete(k);
      if (this.cache.size >= 16) return Promise.resolve({ items: [], unavailable: true });
    }
    const entry = { until: Infinity, value: Promise.resolve<Section>({ items: [], unavailable: true }) };
    entry.value = this.load(token, kind).then(value => {
      entry.until = this.now() + (value.unavailable ? 60_000 : 10 * 60_000);
      return value;
    });
    this.cache.set(key, entry);
    return entry.value;
  }
  private async load(token: string, kind: "movie" | "tv"): Promise<Section> {
    try {
      const url = new URL(`https://api.themoviedb.org/3/trending/${kind}/week`);
      const headers: Record<string, string> = { accept: "application/json" };
      if (/^[a-f0-9]{32}$/i.test(token)) url.searchParams.set("api_key", token);
      else headers.authorization = `Bearer ${token}`;
      const response = await this.fetcher(url, { headers, redirect: "error", signal: AbortSignal.timeout(8_000) });
      if (!response.ok) { await response.body?.cancel(); return { items: [], unavailable: true }; }
      const reader = response.body?.getReader();
      if (!reader) return { items: [], unavailable: true };
      const chunks: Uint8Array[] = []; let size = 0;
      try {
        while (true) {
          const next = await reader.read(); if (next.done) break;
          size += next.value.byteLength;
          if (size > 512 * 1024) { await reader.cancel(); return { items: [], unavailable: true }; }
          chunks.push(next.value);
        }
      } finally { reader.releaseLock(); }
      const bytes = new Uint8Array(size); let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
      const data = upstream.parse(JSON.parse(new TextDecoder().decode(bytes)));
      const seen = new Set<number>();
      return { unavailable: false, items: data.results.filter(row => !row.adult && (kind === "movie" ? row.title : row.name)?.trim())
        .filter(row => !seen.has(row.id) && Boolean(seen.add(row.id))).slice(0, 20).map(row => {
          const date = kind === "movie" ? row.release_date : row.first_air_date;
          return { tmdbId: row.id, title: (kind === "movie" ? row.title! : row.name!).trim(),
            year: date && /^\d{4}-\d{2}-\d{2}$/.test(date) ? Number(date.slice(0, 4)) : null,
            posterUrl: tmdbImageUrl(row.poster_path, "w342"), backdropUrl: tmdbImageUrl(row.backdrop_path, "w1280") };
        }) };
    } catch { return { items: [], unavailable: true }; }
  }
}
// Delay resolving global fetch until execution, allowing the existing fixture/injection boundary.
export const homeDiscovery = new HomeDiscovery(Object.assign((...args: Parameters<typeof fetch>) => fetch(...args), { preconnect: fetch.preconnect }));
