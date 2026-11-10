import bencode from "bencode";

import type { BooksRepo } from "../repo";
import { infoHashFromTorrentBytes, normalizeInfoHash } from "./torrent";
import { recordIndexerTraffic, type TrafficPurpose } from "../media/indexer-traffic";
import { AUTOBRR_ID_PREFIX } from "../media/autobrr";

type TorrentCacheIdentity = {
  provider?: string | null;
  providerGuid?: string | null;
  url: string;
  infoHash?: string | null;
};

type TorrentFileEntry = {
  path: string;
  size: number | null;
};

function decodeBytes(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Uint8Array) {
    return new TextDecoder().decode(value);
  }
  return String(value ?? "");
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function normalizeUrlForCache(value: string): string {
  const trimmed = value.trim();
  try {
    const parsed = new URL(trimmed);
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return trimmed;
  }
}

export function torrentCacheKeyFor(identity: TorrentCacheIdentity): string {
  const provider = (identity.provider ?? "").trim();
  const guid = (identity.providerGuid ?? "").trim();
  if (provider && guid) {
    return `pg:${provider}:${guid}`;
  }
  const normalizedUrl = normalizeUrlForCache(identity.url);
  return `url:${provider}:${normalizedUrl}`;
}

export async function fetchTorrentBytes(url: string): Promise<Uint8Array> {
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) {
    throw new Error(`Failed to download torrent: ${response.status}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

export async function fetchAutobrrTorrentBytes(url: string, fetcher: typeof fetch = fetch): Promise<Uint8Array> {
  // The webhook checks the initial origin; a redirect must not bypass that check.
  const response = await fetcher(url, { redirect: "manual", signal: AbortSignal.timeout(30_000) });
  if (!response.ok || !response.body) {
    await response.body?.cancel().catch(() => {});
    throw new Error("Autobrr metainfo retrieval returned HTTP " + response.status);
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > 8 * 1024 * 1024) throw new Error("Autobrr metainfo exceeds 8 MiB");
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  return Buffer.concat(chunks);
}

export function inspectTorrentFiles(bytes: Uint8Array): TorrentFileEntry[] {
  const decoded = bencode.decode(bytes) as unknown;
  const root = asRecord(decoded);
  const info = asRecord(root?.info);
  if (!info) {
    throw new Error("Torrent payload missing info dictionary");
  }

  const baseName = decodeBytes(info["name.utf-8"] ?? info.name ?? "");
  const files = Array.isArray(info.files) ? info.files : null;
  if (!files) {
    const singleLength = typeof info.length === "number" ? Math.trunc(info.length) : null;
    return [
      {
        path: baseName || "(unknown)",
        size: singleLength,
      },
    ];
  }

  const out: TorrentFileEntry[] = [];
  for (const rawFile of files) {
    const file = asRecord(rawFile);
    if (!file) continue;
    const parts = Array.isArray(file["path.utf-8"])
      ? (file["path.utf-8"] as unknown[])
      : Array.isArray(file.path)
        ? (file.path as unknown[])
        : [];
    const rel = parts.map((part) => decodeBytes(part)).filter(Boolean).join("/");
    const size = typeof file.length === "number" ? Math.trunc(file.length) : null;
    out.push({
      path: baseName && rel ? `${baseName}/${rel}` : rel || baseName || "(unknown)",
      size,
    });
  }
  return out;
}

const pendingByDatabase = new WeakMap<object, Map<string, Promise<Uint8Array>>>();
const unpinnedMetainfoMaxAgeMs = 30 * 60_000;

export async function getOrFetchCachedTorrentBytes(
  repo: BooksRepo,
  identity: TorrentCacheIdentity,
  options: { onLog?: (message: string) => void; revalidate?: boolean; purpose?: TrafficPurpose;
    fetchBytes?: (url: string) => Promise<Uint8Array>; validate?: (bytes: Uint8Array) => void } = {}
): Promise<{ bytes: Uint8Array; cacheKey: string; cacheHit: boolean }> {
  const cacheKey = torrentCacheKeyFor(identity);
  const cached = repo.getTorrentCache(cacheKey);
  const age = cached ? Date.now() - Date.parse(cached.updated_at) : Infinity;
  const fresh = identity.infoHash || (age >= 0 && age <= unpinnedMetainfoMaxAgeMs);
  let cachedHash: string | null = null;
  if (cached?.torrent_bytes && fresh && !options.revalidate) {
    try {
      options.validate?.(cached.torrent_bytes);
      cachedHash = infoHashFromTorrentBytes(cached.torrent_bytes);
    } catch { /* Refetch corrupt legacy cache entries. */ }
  }
  if (cached?.torrent_bytes && cachedHash && !options.revalidate && cached.url === identity.url &&
      (!identity.infoHash || cachedHash === normalizeInfoHash(identity.infoHash))) {
    options.onLog?.(`[torrent_cache] hit bytes=${cached.torrent_bytes.byteLength}`);
    recordIndexerTraffic(repo, options.purpose ?? "acquisition", "metainfo_cache_hit");
    return { bytes: cached.torrent_bytes, cacheKey, cacheHit: true };
  }
  const purpose = options.purpose ?? "acquisition";
  const namespace = repo.torrentCacheNamespace();
  let pending = pendingByDatabase.get(namespace);
  if (!pending) pendingByDatabase.set(namespace, pending = new Map());
  const requestKey = JSON.stringify([cacheKey, identity.url, identity.infoHash, Boolean(options.validate)]);
  const existing = pending.get(requestKey);
  if (existing) {
    recordIndexerTraffic(repo, purpose, "metainfo_coalesced");
    return { bytes: await existing, cacheKey, cacheHit: false };
  }
  options.onLog?.("[torrent_cache] miss");
  const request = (async () => {
    recordIndexerTraffic(repo, purpose, "metainfo_fetch");
    let bytes: Uint8Array, infoHash: string;
    try {
      const fetchBytes = identity.providerGuid?.startsWith(AUTOBRR_ID_PREFIX)
        ? fetchAutobrrTorrentBytes : options.fetchBytes ?? fetchTorrentBytes;
      bytes = await fetchBytes(identity.url);
      options.validate?.(bytes);
      infoHash = infoHashFromTorrentBytes(bytes);
    } catch (error) {
      recordIndexerTraffic(repo, purpose, "metainfo_failure");
      throw error;
    }
    return repo.putTorrentCache({ key: cacheKey, provider: identity.provider, providerGuid: identity.providerGuid,
      url: identity.url, infoHash, torrentBytes: bytes, observedFetch: true }).torrent_bytes!;
  })();
  pending.set(requestKey, request);
  let bytes: Uint8Array;
  try { bytes = await request; } finally { pending.delete(requestKey); }
  options.onLog?.(`[torrent_cache] stored bytes=${bytes.byteLength}`);
  return { bytes, cacheKey, cacheHit: false };
}

export type { TorrentFileEntry, TorrentCacheIdentity };
