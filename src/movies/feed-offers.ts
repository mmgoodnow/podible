import type { BooksRepo } from "../repo";
import { movieOfferFromResult, type MovieOffer } from "./offers";
import type { SearchResult } from "../media/indexer-gateway";

/** Inbox identity is immutable; expiring provider URLs and availability are not. */
export function refreshMovieFeedOffer(
  repo: BooksRepo,
  offer: MovieOffer,
): MovieOffer {
  if (offer.inboxId === undefined) return offer;
  const entry = repo.releaseInbox.entries([offer.inboxId])[0];
  if (!entry || entry.result.mediaType !== "movie")
    throw new Error("Movie RSS candidate is missing from the inbox");
  return {
    ...movieOfferFromResult(entry.result as SearchResult<"movie">),
    key: offer.key,
    inboxId: offer.inboxId,
  };
}
