import bencode from "bencode";

import type { BooksRepo } from "../repo";

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

export async function getOrFetchCachedTorrentBytes(
  repo: BooksRepo,
  identity: TorrentCacheIdentity,
  options: { onLog?: (message: string) => void } = {}
): Promise<{ bytes: Uint8Array; cacheKey: string; cacheHit: boolean }> {
  const cacheKey = torrentCacheKeyFor(identity);
  const cached = repo.getTorrentCache(cacheKey);
  if (cached?.torrent_bytes) {
    options.onLog?.(`[torrent_cache] hit key=${JSON.stringify(cacheKey)} bytes=${cached.torrent_bytes.byteLength}`);
    return { bytes: cached.torrent_bytes, cacheKey, cacheHit: true };
  }
  options.onLog?.(`[torrent_cache] miss key=${JSON.stringify(cacheKey)} fetch_url=${JSON.stringify(identity.url)}`);
  const bytes = await fetchTorrentBytes(identity.url);
  repo.putTorrentCache({
    key: cacheKey,
    provider: identity.provider ?? null,
    providerGuid: identity.providerGuid ?? null,
    url: identity.url,
    infoHash: identity.infoHash ?? null,
    torrentBytes: bytes,
  });
  options.onLog?.(`[torrent_cache] stored key=${JSON.stringify(cacheKey)} bytes=${bytes.byteLength}`);
  return { bytes, cacheKey, cacheHit: false };
}

export type { TorrentFileEntry, TorrentCacheIdentity };
