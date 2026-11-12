import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import type { AppSettings } from "../app-types";
import type { IndexerMedia } from "./indexer-gateway";
import { ProwlarrDiscovery, type DiscoveredIndexer } from "./prowlarr";
import { INDEXER_RETRY_MS, IndexerError, indexerFailure, type IndexerFailure } from "./indexer-errors";

type DiscoveryEntry = { discovery: ProwlarrDiscovery; failure?: IndexerFailure; until?: number };
const discoveries = new Map<string, DiscoveryEntry>();

function endpointIdentity(value: string): string | null {
  try {
    const url = new URL(value);
    return url.origin + url.pathname.replace(/\/+$/, "").replace(/\/api$/, "");
  } catch {
    return null;
  }
}

export async function indexerDiscoveryStatus(settings: AppSettings, sourcesFor = prowlarrSources) {
  const mediaTypes = ["movie", "tv", "audio", "ebook"] as const;
  const modes = ["search", "rss"] as const;
  type Row = { id: number; name: string; search: IndexerMedia[]; feeds: IndexerMedia[]; retryAt: number | null };
  if (!settings.prowlarr.url.trim())
    return { state: "not-configured" as const, indexers: [], duplicates: [] };
  try {
    const groups = await Promise.all(modes.flatMap(mode => mediaTypes.map(async media =>
      ({ mode, media, sources: await sourcesFor(settings, media, mode) }))));
    const rows = new Map<string, Row>();
    for (const { mode, media, sources } of groups) for (const source of sources) {
      const endpoint = endpointIdentity(source.baseUrl);
      if (!endpoint) continue;
      let row = rows.get(endpoint);
      if (!row) {
        const name = source.name.replaceAll(settings.prowlarr.apiKey, "[redacted]").replace(/https?:\/\/\S+/gi, "[URL]");
        row = { id: Number(endpoint.split("/").at(-1)), name, search: [], feeds: [], retryAt: null };
        rows.set(endpoint, row);
      }
      row[mode === "search" ? "search" : "feeds"].push(media);
      if (source.availableAfter && source.availableAfter > Date.now())
        row.retryAt = Math.max(row.retryAt ?? 0, source.availableAfter);
    }
    const duplicates = settings.torznab.flatMap((source, index) => {
      const match = rows.get(endpointIdentity(source.baseUrl) ?? "");
      return match ? [{ index, name: match.name }] : [];
    });
    return { state: "available" as const, indexers: [...rows.values()].sort((a, b) => a.name.localeCompare(b.name)), duplicates };
  } catch (error) {
    if (isRunControlError(error)) throw error;
    return { state: "unavailable" as const, indexers: [], duplicates: [] };
  }
}

export async function indexerSources(settings: AppSettings, media: IndexerMedia, mode: "search" | "rss" = "search",
  discover = prowlarrSources): Promise<DiscoveredIndexer[]> {
  const discovered = settings.prowlarr?.url.trim() ? await discover(settings, media, mode) : [];
  const seen = new Set<string>();
  return [...discovered, ...settings.torznab].filter(source => {
    const endpoint = endpointIdentity(source.baseUrl);
    // Keep malformed entries for the gateway to report as configuration errors.
    if (endpoint === null) return true;
    if (seen.has(endpoint)) return false;
    seen.add(endpoint);
    return true;
  });
}

async function prowlarrSources(settings: AppSettings, media: IndexerMedia, mode: "search" | "rss"): Promise<DiscoveredIndexer[]> {
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
