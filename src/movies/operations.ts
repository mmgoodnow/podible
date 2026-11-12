import type { MovieCatalog } from "./catalog";
import type { MovieFacts, MovieQualityPolicy } from "./quality";
import type { Movie } from "./types";
import type { MovieOffer, MovieSearch } from "./offers";
import type { TorrentClient } from "../media/torrent-client";
import type { MovieLibrary } from "./publication";
import type { MovieProbe } from "./probe";

export type MovieSelection = {
  offerKey: string | null;
  facts: MovieFacts;
  reason: string;
  matchingOffers?: MovieOffer[];
};
export type MovieFileCandidate = {
  index: number;
  name: string;
  sizeBytes: number;
  probe: MovieProbe;
};
export interface MovieDecider {
  select(input: {
    movie: Movie;
    policy: MovieQualityPolicy;
    offers: MovieOffer[];
    current: MovieFacts | null;
    allowDecisions?: boolean;
  }): Promise<MovieSelection>;
  inspect(input: {
    movie: Movie;
    policy: MovieQualityPolicy;
    offer: MovieOffer;
    files: MovieFileCandidate[];
    selection: MovieSelection;
    allowDecisions?: boolean;
  }): Promise<{
    fileIndex: number | null;
    facts: Pick<MovieFacts, "identityVerified" | "source" | "cut" | "proper">;
    reason: string;
  }>;
}
/** Integration seams are server-only; never populated from RPC input. */
export type MovieServices = {
  catalog: MovieCatalog;
  search(movie: Movie, query?: string): Promise<MovieSearch>;
  decider: MovieDecider;
  client: TorrentClient;
  library: MovieLibrary;
  fetchMetainfo(offer: MovieOffer): Promise<Uint8Array>;
  probe(file: string): Promise<MovieProbe>;
  timing?: {
    pollMs?: number;
    stallMs?: number;
    plexAttempts?: number;
    plexRetryMs?: number;
  };
};
