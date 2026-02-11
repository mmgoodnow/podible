import { XMLParser } from "fast-xml-parser";

import type { MediaType, TorznabSource } from "./types";
import { normalizeInfoHash } from "./torrent";

type TorznabResult = {
  title: string;
  provider: string;
  mediaType: MediaType;
  sizeBytes: number | null;
  url: string;
  infoHash: string;
  seeders: number | null;
  leechers: number | null;
  raw: Record<string, unknown>;
};

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  parseTagValue: false,
  textNodeName: "#text",
});

function toArray<T>(value: T | T[] | undefined): T[] {
  if (value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

function parseIntOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function textValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") {
    const text = (value as Record<string, unknown>)["#text"];
    if (typeof text === "string") return text;
  }
  return "";
}

function chooseDownloadUrl(item: Record<string, unknown>): string {
  const enclosure = item.enclosure as Record<string, unknown> | undefined;
  const comments = item.comments;
  if (enclosure && typeof enclosure.url === "string") return enclosure.url;
  if (typeof item.link === "string") return item.link;
  if (typeof comments === "string") return comments;
  return "";
}

function isSupportedTorrentUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function parseAttrMap(item: Record<string, unknown>): Record<string, string> {
  const attrs = toArray(item.attr as Record<string, unknown> | Record<string, unknown>[] | undefined);
  const out: Record<string, string> = {};
  for (const rawAttr of attrs) {
    const attr = rawAttr as Record<string, unknown>;
    const name = typeof attr.name === "string" ? attr.name.toLowerCase() : "";
    const value = typeof attr.value === "string" ? attr.value : "";
    if (!name || !value) continue;
    out[name] = value;
  }
  return out;
}

function inferInfoHash(attrMap: Record<string, string>): string | null {
  const candidate =
    attrMap.infohash ??
    attrMap["torrenthash"] ??
    attrMap["btih"];
  if (candidate) {
    try {
      return normalizeInfoHash(candidate);
    } catch {
      // fall through
    }
  }
  return null;
}

export function parseTorznabSearch(xml: string, provider: string, mediaType: MediaType): TorznabResult[] {
  const doc = parser.parse(xml) as Record<string, unknown>;
  const channel = (doc.rss as Record<string, unknown> | undefined)?.channel as
    | Record<string, unknown>
    | undefined;
  const items = toArray((channel?.item as Record<string, unknown> | Record<string, unknown>[] | undefined) ?? []);

  const out: TorznabResult[] = [];
  for (const item of items) {
      const title = textValue(item.title);
      const attrMap = parseAttrMap(item);
      const url = chooseDownloadUrl(item);
      const infoHash = inferInfoHash(attrMap);
      if (!title || !url || !isSupportedTorrentUrl(url) || !infoHash) {
        continue;
      }
      out.push({
        title,
        provider,
        mediaType,
        sizeBytes: parseIntOrNull(
          attrMap.size ??
            item.size ??
            (item.enclosure as Record<string, unknown> | undefined)?.length
        ),
        url,
        infoHash,
        seeders: parseIntOrNull(attrMap.seeders),
        leechers: parseIntOrNull(attrMap.leechers ?? attrMap.peers),
        raw: {
          ...item,
          provider,
        },
      });
  }
  return out;
}

function buildSearchUrl(source: TorznabSource, query: string, mediaType: MediaType): URL {
  const url = new URL(source.baseUrl);
  if (url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}api`;
  } else if (!url.pathname.endsWith("/api")) {
    url.pathname = `${url.pathname}/api`;
  }
  url.searchParams.set("t", "search");
  url.searchParams.set("q", query);
  const category = mediaType === "audio" ? source.categories?.audio : source.categories?.ebook;
  if (category) {
    url.searchParams.set("cat", category);
  }
  if (source.apiKey) {
    url.searchParams.set("apikey", source.apiKey);
  }
  return url;
}

export async function searchTorznab(
  sources: TorznabSource[],
  query: string,
  mediaType: MediaType
): Promise<TorznabResult[]> {
  const results: TorznabResult[] = [];
  for (const source of sources) {
    const url = buildSearchUrl(source, query, mediaType);
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      throw new Error(`Torznab source ${source.name} returned ${response.status}`);
    }
    const xml = await response.text();
    results.push(...parseTorznabSearch(xml, source.name, mediaType));
  }
  return results;
}

export type { TorznabResult };
