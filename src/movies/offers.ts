import { ProwlarrDiscovery, type DiscoveredIndexer } from "../media/prowlarr";
import { IndexerGateway, indexerGateway, type SearchOptions } from "../media/indexer-gateway";
import { inspectMetainfo } from "../media/metainfo";
import type { BooksRepo } from "../repo";
import { getOrFetchCachedTorrentBytes, unpinnedMetainfoMaxAgeMs } from "../library/torrent-cache";
import type { TrafficPurpose } from "../media/indexer-traffic";
import type { Movie } from "./types";
import { releaseOfferFromResult as movieOfferFromResult, type ReleaseOffer as MovieOffer } from "../media/release-offer";
export { movieOfferFromResult, type MovieOffer };
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
/** Books, movies and RSS use the same transport, response bounds and provider cooldowns. */
export class MovieIndexers {
  private gateway: IndexerGateway;
  constructor(
    private sources: DiscoveredIndexer[],
    fetcher?: (url: string, init?: RequestInit) => Promise<Response>,
  ) {
    this.gateway = fetcher
      ? new IndexerGateway((url, init) => fetcher(url.href, init))
      : indexerGateway;
  }
  updateSources(sources: DiscoveredIndexer[]) {
    this.sources = sources;
  }
  async search(movie: Movie, query?: string, options?: SearchOptions): Promise<MovieSearch> {
    if (!this.sources.length)
      throw new MovieIndexerError("No indexers are configured", 60_000, false);
    const q = query?.trim() || (movie.title + (movie.year ? " " + movie.year : ""));
    const batch = await this.gateway.search(this.sources, q, "movie", { limit: 100, ...options });
    if (batch.failures.length === this.sources.length ||
        (!batch.results.length && batch.failures.some(f => f.kind === "configuration"))) {
      const times = batch.failures.flatMap(f => f.retryAt === null ? [] : [f.retryAt]);
      throw new MovieIndexerError(
        batch.failures.map(f => f.message).join("; "),
        times.length ? Math.max(1000, Math.min(...times) - Date.now()) : 60_000,
        batch.failures.some(f => f.kind === "unavailable"),
      );
    }
    const offers = [...new Map(batch.results.map(result => {
      const offer = movieOfferFromResult(result);
      return [offer.key, offer] as const;
    })).values()];
    const deferred = batch.failures.flatMap(f => f.retryAt === null ? [] : [{
      provider: f.provider, retryAt: new Date(f.retryAt).toISOString(),
    }]);
    return {
      offers,
      warnings: batch.failures.map(f => f.message),
      ...(deferred.length ? { deferred } : {}),
    };
  }
}

export class DiscoveredMovieIndexers {
  private searcher: MovieIndexers;
  constructor(
    private discovery: Pick<ProwlarrDiscovery, "sources">,
    fetcher?: ConstructorParameters<typeof MovieIndexers>[1],
  ) {
    this.searcher = new MovieIndexers([], fetcher);
  }
  async search(movie: Movie, query?: string, options?: SearchOptions) {
    try {
      this.searcher.updateSources(await this.discovery.sources("movie"));
    } catch {
      throw new MovieIndexerError(
        "Prowlarr indexer discovery is temporarily unavailable",
        60_000,
      );
    }
    return this.searcher.search(movie, query, options);
  }
}

async function fetchMetainfoResponse(downloadUrl: string): Promise<Uint8Array> {
  const url = new URL(downloadUrl);
  if (!["http:", "https:"].includes(url.protocol))
    throw new Error("Unsupported metainfo retrieval protocol");
  const response = await fetch(url, { signal: AbortSignal.timeout(30_000) });
  if (!response.ok)
    throw new MovieIndexerError(
      `Metainfo retrieval returned HTTP ${response.status}`,
      retryDelay(response.headers.get("retry-after")),
      response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500,
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
  return new Uint8Array(Buffer.concat(chunks));
}

function validateMetainfo(bytes: Uint8Array, advertisedHash: string | null) {
  const inspected = inspectMetainfo(bytes);
  if (advertisedHash && inspected.infoHash !== advertisedHash.toLowerCase())
    throw new Error(
      "Indexer metainfo hash does not match its advertised identity",
    );
}

export async function fetchMovieMetainfo(offer: MovieOffer): Promise<Uint8Array> {
  const bytes = await fetchMetainfoResponse(offer.url);
  validateMetainfo(bytes, offer.infoHash);
  return bytes;
}

export async function cachedMovieMetainfo(repo: BooksRepo, offer: MovieOffer, purpose: TrafficPurpose): Promise<Uint8Array> {
  const guid = offer.attributes.guid;
  const providerGuid = typeof guid === "string" ? guid : guid && typeof guid === "object" && "#text" in guid && typeof guid["#text"] === "string"
    ? guid["#text"] : null;
  return (await getOrFetchCachedTorrentBytes(repo, { provider: offer.provider, providerGuid, url: offer.url, infoHash: offer.infoHash }, {
    purpose,
    maxAgeMs: offer.infoHash ? undefined : unpinnedMetainfoMaxAgeMs,
    fetchBytes: fetchMetainfoResponse,
    validate: bytes => validateMetainfo(bytes, offer.infoHash),
  })).bytes;
}
