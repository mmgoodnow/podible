import { createHash } from "node:crypto";
import { releaseIdentity, type SearchResult } from "./indexer-gateway";
import { AUTOBRR_ID_PREFIX } from "./autobrr";

export type ReleaseOffer = {
  inboxId?: number;
  key: string;
  title: string;
  provider: string;
  sizeBytes: number | null;
  seeders: number | null;
  url: string;
  infoHash: string | null;
  attributes: Record<string, unknown>;
};

/** Stable server-owned offer identity, independent of the acquisition's media type. */
export function releaseOfferFromResult(result: SearchResult<string>): ReleaseOffer {
  return {
    key: createHash("sha256").update(JSON.stringify([releaseIdentity(result), result.infoHash, result.sizeBytes, result.title])).digest("hex"),
    title: result.title, provider: result.provider, sizeBytes: result.sizeBytes,
    seeders: result.seeders, url: result.url, infoHash: result.infoHash,
    attributes: result.guid?.startsWith(AUTOBRR_ID_PREFIX)
      ? { ...result.raw, guid: result.guid }
      : result.raw,
  };
}
