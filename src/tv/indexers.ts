import { indexerSources } from "../media/indexer-discovery";
import { indexerGateway } from "../media/indexer-gateway";
import type { AppSettings } from "../app-types";
import { movieOfferFromResult, MovieIndexerError, type MovieOffer, type MovieSearch } from "../movies/offers";
import type { TvCatalogEntry } from "./types";

/** TV shares discovery, rate limits and normalized release payloads with the other media. */
export async function searchTvReleases(settings: AppSettings, entry: Pick<TvCatalogEntry, "title">, query?: string): Promise<MovieSearch> {
  const sources = await indexerSources(settings, "tv");
  if (!sources.length) throw new MovieIndexerError("No TV-capable indexers are configured", 60_000, false);
  const result = await indexerGateway.search(sources, query?.trim() || entry.title, "tv", { limit: 100 });
  if (result.failures.length === sources.length) {
    const retryAt = Math.min(...result.failures.flatMap((failure) => failure.retryAt === null ? [] : [failure.retryAt]));
    throw new MovieIndexerError(result.failures.map((failure) => failure.message).join("; "),
      Number.isFinite(retryAt) ? Math.max(1000, retryAt - Date.now()) : 60_000,
      result.failures.some((failure) => failure.kind === "unavailable"));
  }
  const offers = new Map<string, MovieOffer>();
  for (const release of result.results) {
    // Offer identity is media-neutral; the receiving workflow owns episode semantics.
    const offer = movieOfferFromResult({ ...release, mediaType: "movie" });
    offers.set(offer.key, offer);
  }
  return { offers: [...offers.values()], warnings: result.failures.map((failure) => failure.message),
    deferred: result.failures.flatMap((failure) => failure.retryAt === null ? [] : [{ provider: failure.provider, retryAt: new Date(failure.retryAt).toISOString() }]) };
}
