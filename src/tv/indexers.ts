import { indexerSources } from "../media/indexer-discovery";
import { indexerGateway } from "../media/indexer-gateway";
import type { AppSettings } from "../app-types";
import { movieOfferFromResult, MovieIndexerError, type MovieOffer, type MovieSearch } from "../movies/offers";
import type { TvEntry, TvEpisode } from "./types";
import type { BooksRepo } from "../repo";
import type { ModelProvider } from "@openai/agents-core";
import { planIndexerQuery, tvSearchEvidence } from "../media/indexer-query-plan";
import { recordIndexerTraffic } from "../media/indexer-traffic";

/** TV shares discovery, rate limits and normalized release payloads with the other media. */
export async function searchTvReleases(settings: AppSettings, entry: Pick<TvEntry, "title"> & Partial<TvEntry>, query?: string,
  runtime: { repo?: BooksRepo; modelProvider?: ModelProvider; episodes?: TvEpisode[] } = {}): Promise<MovieSearch> {
  const sources = await indexerSources(settings, "tv");
  if (!sources.length) throw new MovieIndexerError("No TV-capable indexers are configured", 60_000, false);
  const { evidence, options } = tvSearchEvidence(entry, runtime.episodes);
  const plan = query?.trim() ? { primaryQuery: query.trim(), alternateQuery: null } : await planIndexerQuery(settings, evidence, runtime.modelProvider);
  const result = await indexerGateway.search(sources, plan.primaryQuery, "tv", { limit: 100, ...(query?.trim() ? {} : options),
    alternateQuery: plan.alternateQuery,
    onRequest: mode => { if (runtime.repo) recordIndexerTraffic(runtime.repo, "acquisition", mode === "id" ? "search_id" : "search_text"); } });
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
