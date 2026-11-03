import { createHash } from "node:crypto";
import { ProwlarrDiscovery, type DiscoveredIndexer } from "../media/prowlarr";
import { parseTorznabSearch } from "../library/torznab";
import { inspectMetainfo } from "../media/metainfo";
import type { Movie } from "./types";

export type MovieOffer = {
  key: string;
  title: string;
  provider: string;
  sizeBytes: number | null;
  seeders: number | null;
  url: string;
  infoHash: string | null;
  attributes: Record<string, unknown>;
};
export type MovieSearch = {
  offers: MovieOffer[];
  warnings: string[];
  deferred?: { provider: string; retryAt: string }[];
};
export class MovieIndexerError extends Error {
  constructor(
    message: string,
    readonly retryAfterMs: number = 60_000,
    readonly retryable = true,
  ) {
    super(message);
  }
}
function retryDelay(value: string | null): number {
  if (!value) return 60_000;
  const seconds = Number(value);
  return Math.min(
    60 * 60_000,
    Math.max(
      1000,
      Number.isFinite(seconds)
        ? seconds * 1000
        : Date.parse(value) - Date.now() || 60_000,
    ),
  );
}
/** Shared per runtime: coalesce searches and honor indexer backoff across users. */
export class MovieIndexers {
  private backoff = new Map<string, number>();
  private cache = new Map<string, { until: number; result: MovieSearch }>();
  private inflight = new Map<string, Promise<MovieSearch>>();
  constructor(
    private sources: DiscoveredIndexer[],
    private fetcher: (
      url: string,
      init?: RequestInit,
    ) => Promise<Response> = fetch,
  ) {}
  updateSources(sources: DiscoveredIndexer[]) {
    if (JSON.stringify(sources) !== JSON.stringify(this.sources)) {
      this.sources = sources;
      this.cache.clear();
    }
  }
  async search(movie: Movie, query?: string): Promise<MovieSearch> {
    const q =
      query?.trim() || `${movie.title}${movie.year ? ` ${movie.year}` : ""}`;
    const cached = this.cache.get(q);
    if (cached && cached.until > Date.now()) return cached.result;
    const pending = this.inflight.get(q);
    if (pending) return pending;
    const action = this.perform(q);
    this.inflight.set(q, action);
    try {
      const result = await action;
      if (this.cache.size > 100) this.cache.clear();
      const deferredUntil =
        result.deferred
          ?.map((source) => Date.parse(source.retryAt))
          .filter(Number.isFinite) ?? [];
      // Partial results must expire when the first missing provider can retry.
      this.cache.set(q, {
        until: Math.min(Date.now() + 60_000, ...deferredUntil),
        result,
      });
      return result;
    } finally {
      this.inflight.delete(q);
    }
  }
  private async perform(query: string): Promise<MovieSearch> {
    if (!this.sources.length)
      throw new MovieIndexerError("No indexers are configured");
    const offers: MovieOffer[] = [];
    const warnings: string[] = [];
    const retryAt: number[] = [];
    const deferred: NonNullable<MovieSearch["deferred"]> = [];
    let successes = 0;
    for (const source of this.sources) {
      const key = source.baseUrl;
      const availableAt = Math.max(
        this.backoff.get(key) ?? 0,
        source.availableAfter ?? 0,
      );
      if (availableAt > Date.now()) {
        retryAt.push(availableAt);
        deferred.push({
          provider: source.name,
          retryAt: new Date(availableAt).toISOString(),
        });
        warnings.push(`${source.name}: waiting for indexer backoff`);
        continue;
      }
      try {
        const url = new URL(source.baseUrl);
        if (!url.pathname.endsWith("/api"))
          url.pathname = `${url.pathname.replace(/\/$/, "")}/api`;
        url.searchParams.set("t", "search");
        url.searchParams.set("q", query);
        url.searchParams.set("cat", "2000");
        url.searchParams.set("limit", "100");
        if (source.apiKey) url.searchParams.set("apikey", source.apiKey);
        const response = await this.fetcher(url.toString(), {
          redirect: "manual",
          signal: AbortSignal.timeout(20_000),
        });
        if (!response.ok) {
          const delay = retryDelay(response.headers.get("retry-after"));
          this.backoff.set(key, Date.now() + delay);
          throw new MovieIndexerError(
            `${source.name}: HTTP ${response.status}`,
            delay,
          );
        }
        const xml = await response.text();
        if (/<error(?:\s|\/|>)/i.test(xml))
          throw new MovieIndexerError(
            `${source.name}: indexer returned an error`,
          );
        if (!/<rss[\s>]/i.test(xml))
          throw new MovieIndexerError(
            `${source.name}: invalid Torznab response`,
          );
        successes++;
        for (const result of parseTorznabSearch(xml, source.name, "movie")) {
          const identity = `${source.name}:${result.guid ?? result.infoHash ?? result.url}`;
          const offerKey = createHash("sha256").update(identity).digest("hex");
          if (!offers.some((offer) => offer.key === offerKey))
            offers.push({
              key: offerKey,
              title: result.title,
              provider: source.name,
              sizeBytes: result.sizeBytes,
              seeders: result.seeders,
              url: result.url,
              infoHash: result.infoHash,
              attributes: result.raw,
            });
        }
      } catch (error) {
        const retry =
          Date.now() +
          (error instanceof MovieIndexerError ? error.retryAfterMs : 60_000);
        this.backoff.set(key, retry);
        retryAt.push(retry);
        deferred.push({
          provider: source.name,
          retryAt: new Date(retry).toISOString(),
        });
        // Provider URLs/passkeys must not escape through error strings.
        warnings.push(
          error instanceof MovieIndexerError
            ? error.message
            : `${source.name}: search unavailable`,
        );
      }
    }
    if (!successes)
      throw new MovieIndexerError(
        warnings.join("; ") || "All indexers are unavailable",
        retryAt.length
          ? Math.max(1000, Math.min(...retryAt) - Date.now())
          : 60_000,
      );
    return { offers, warnings, ...(deferred.length ? { deferred } : {}) };
  }
}

export class DiscoveredMovieIndexers {
  private searcher: MovieIndexers;
  constructor(
    private discovery: ProwlarrDiscovery,
    fetcher?: ConstructorParameters<typeof MovieIndexers>[1],
  ) {
    this.searcher = new MovieIndexers([], fetcher);
  }
  async search(movie: Movie, query?: string) {
    try {
      this.searcher.updateSources(await this.discovery.sources("movie"));
    } catch {
      throw new MovieIndexerError(
        "Prowlarr indexer discovery is temporarily unavailable",
        60_000,
      );
    }
    return this.searcher.search(movie, query);
  }
}

export async function fetchMovieMetainfo(
  offer: MovieOffer,
): Promise<Uint8Array> {
  const url = new URL(offer.url);
  if (!["http:", "https:"].includes(url.protocol))
    throw new Error("Unsupported metainfo retrieval protocol");
  const response = await fetch(url, { signal: AbortSignal.timeout(30_000) });
  if (!response.ok)
    throw new MovieIndexerError(
      `Metainfo retrieval returned HTTP ${response.status}`,
      retryDelay(response.headers.get("retry-after")),
    );
  if (!response.body) throw new Error("Metainfo response has no body");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > 8 * 1024 ** 2) throw new Error("Metainfo exceeds 8 MiB");
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  const bytes = new Uint8Array(Buffer.concat(chunks));
  const inspected = inspectMetainfo(bytes);
  if (offer.infoHash && inspected.infoHash !== offer.infoHash.toLowerCase())
    throw new Error(
      "Indexer metainfo hash does not match its advertised identity",
    );
  return bytes;
}
