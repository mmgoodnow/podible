import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import type { AppSettings } from "../app-types";
import type { IndexerMedia } from "./indexer-gateway";
import { ProwlarrDiscovery, type DiscoveredIndexer } from "./prowlarr";
import { INDEXER_RETRY_MS, IndexerError, indexerFailure, type IndexerFailure } from "./indexer-errors";

type DiscoveryEntry = { discovery: ProwlarrDiscovery; failure?: IndexerFailure; until?: number };
const discoveries = new Map<string, DiscoveryEntry>();

export async function indexerSources(settings: AppSettings, media: IndexerMedia, mode: "search" | "rss" = "search"): Promise<DiscoveredIndexer[]> {
  if (!settings.prowlarr?.url.trim()) return settings.torznab;
  try {
    const url = new URL(settings.prowlarr.url);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash || !settings.prowlarr.apiKey.trim()) throw new Error();
  } catch {
    throw new IndexerError(indexerFailure("Prowlarr discovery", 400, null));
  }
  const key = createHash("sha256").update(JSON.stringify(settings.prowlarr)).digest("hex");
  let entry = discoveries.get(key);
  if (!entry) {
    if (discoveries.size >= 8) discoveries.delete(discoveries.keys().next().value!);
    entry = { discovery: new ProwlarrDiscovery(settings.prowlarr, ((input, init) => fetch(input, init)) as typeof fetch) };
    discoveries.set(key, entry);
  }
  if (entry.failure && (entry.until ?? 0) > Date.now()) throw new IndexerError(entry.failure);
  try {
    const sources = await entry.discovery.sources(media, mode);
    entry.failure = undefined;
    return sources;
  } catch (error) {
    if (isRunControlError(error)) throw error;
    // The shared discovery API currently exposes HTTP status in its safe error, not a typed field.
    const status = error instanceof Error ? /\bHTTP (\d{3})\b/.exec(error.message)?.[1] : undefined;
    entry.until = Date.now() + INDEXER_RETRY_MS;
    entry.failure = indexerFailure("Prowlarr discovery", status ? Number(status) : null, entry.until);
    throw new IndexerError(entry.failure);
  }
}
