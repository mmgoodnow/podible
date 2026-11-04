import { XMLParser } from "fast-xml-parser";
import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";

import type { MediaType, TorznabSource } from "../app-types";
import { normalizeInfoHash } from "../library/torrent";
import { INDEXER_RETRY_MS, IndexerError, IndexerSearchError, indexerCooldown, indexerFailure, parseIndexerRetryAfter, type IndexerFailure } from "./indexer-errors";

export type IndexerMedia = MediaType | "movie" | "tv";
export type SearchResult<TMedia extends string> = {
  title: string;
  provider: string;
  mediaType: TMedia;
  sizeBytes: number | null;
  url: string;
  guid: string | null;
  infoHash: string | null;
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

function parseGuid(item: Record<string, unknown>): string | null {
  const value = textValue(item.guid);
  return value.trim() ? value.trim() : null;
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
  const candidate = attrMap.infohash ?? attrMap["torrenthash"] ?? attrMap["btih"];
  if (candidate) {
    try {
      return normalizeInfoHash(candidate);
    } catch {
      // fall through
    }
  }
  return null;
}

export function parseTorznabSearch<TMedia extends IndexerMedia>(xml: string, provider: string, mediaType: TMedia): SearchResult<TMedia>[] {
  const doc = parser.parse(xml) as Record<string, unknown>;
  const channel = (doc.rss as Record<string, unknown> | undefined)?.channel as Record<string, unknown> | undefined;
  const items = toArray((channel?.item as Record<string, unknown> | Record<string, unknown>[] | undefined) ?? []);

  const out: SearchResult<TMedia>[] = [];
  for (const item of items) {
    const title = textValue(item.title);
    const attrMap = parseAttrMap(item);
    const url = chooseDownloadUrl(item);
    if (!title || !url || !isSupportedTorrentUrl(url)) {
      continue;
    }
    out.push({
      title,
      provider,
      mediaType,
      sizeBytes: parseIntOrNull(attrMap.size ?? item.size ?? (item.enclosure as Record<string, unknown> | undefined)?.length),
      url,
      guid: parseGuid(item),
      infoHash: inferInfoHash(attrMap),
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

function buildSearchUrl(source: TorznabSource, request: IndexerRequest): URL {
  const url = new URL(source.baseUrl);
  if (url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}api`;
  } else if (!url.pathname.endsWith("/api")) {
    url.pathname = `${url.pathname}/api`;
  }
  url.searchParams.set("t", "search");
  if (request.query?.trim()) url.searchParams.set("q", request.query.trim());
  else url.searchParams.delete("q");
  const category = request.media === "movie" ? "2000" : request.media === "tv" ? "5000" : request.media === "audio" ? source.categories?.audio : source.categories?.ebook;
  if (category) {
    url.searchParams.set("cat", category);
  }
  if (source.apiKey) {
    url.searchParams.set("apikey", source.apiKey);
  }
  if (request.offset !== undefined) url.searchParams.set("offset", String(request.offset));
  if (request.limit !== undefined) url.searchParams.set("limit", String(request.limit));
  return url;
}

export type IndexerSearchBatch = { results: TorznabResult[]; failures: IndexerFailure[] };
export type SearchSource = TorznabSource & { availableAfter?: number; supportsPagination?: boolean };
export type IndexerRequest = { media: IndexerMedia; query?: string; offset?: number; limit?: number };
const MAX_ENTRIES = 128;
const MAX_XML_BYTES = 8 * 1024 * 1024;
const MAX_CACHE_BYTES = 32 * 1024 * 1024;

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function releaseIdentity(result: Pick<SearchResult<string>, "provider" | "guid" | "infoHash" | "url">): string {
  return digest(result.provider + ":" + (result.guid ?? result.infoHash ?? result.url));
}

/** Share raw responses, not media-tagged results: category-less audio/ebook URLs are identical. */
export class IndexerGateway {
  private cache = new Map<string, { until: number; xml: string; bytes: number }>();
  private cacheBytes = 0;
  private inflight = new Map<string, Promise<string>>();
  private backoff = new Map<string, { failure: IndexerFailure; count: number; until: number }>();

  constructor(
    private fetcher: (url: URL, init: RequestInit) => Promise<Response> = (url, init) => fetch(url, init),
    private now: () => number = () => Date.now(),
  ) {}

  private async readXml(response: Response): Promise<string> {
    if (!response.body) throw new Error("Missing indexer response");
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_XML_BYTES) throw new Error("Indexer response is too large");
        chunks.push(value);
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
    return Buffer.concat(chunks).toString("utf8");
  }

  private evictCache(key: string): void {
    const cached = this.cache.get(key);
    if (cached) this.cacheBytes -= cached.bytes;
    this.cache.delete(key);
  }

  private async xml(source: SearchSource, criteria: IndexerRequest): Promise<string> {
    let url: URL;
    try {
      url = buildSearchUrl(source, criteria);
      if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) throw new Error();
    } catch {
      throw new IndexerError(indexerFailure(source.name, 400, null));
    }
    url.searchParams.sort();
    const requestKey = digest(url.href);
    const identity = new URL(url);
    for (const key of ["q", "t", "cat", "offset", "limit"]) identity.searchParams.delete(key);
    const providerKey = digest(identity.href);
    const blocked = this.backoff.get(providerKey);
    const availableAt = Math.max(source.availableAfter ?? 0, blocked?.until ?? 0);
    if (availableAt > this.now()) {
      const failure = blocked && blocked.until >= (source.availableAfter ?? 0)
        ? { ...blocked.failure, provider: indexerFailure(source.name, null, availableAt).provider }
        : indexerCooldown(source.name, availableAt);
      throw new IndexerError(failure);
    }
    const cached = this.cache.get(requestKey);
    if (cached && cached.until > this.now()) return cached.xml;
    if (cached) this.evictCache(requestKey);
    const pending = this.inflight.get(requestKey);
    if (pending) return pending;
    if (this.inflight.size >= MAX_ENTRIES) {
      throw new IndexerError(indexerFailure(source.name, null, this.now() + INDEXER_RETRY_MS));
    }
    const request = (async () => {
      try {
        const response = await this.fetcher(url, { method: "GET", redirect: "manual", signal: AbortSignal.timeout(20_000) });
        if (!response.ok) {
          const delay = Math.max(INDEXER_RETRY_MS, parseIndexerRetryAfter(response.headers.get("retry-after"), this.now()) ?? 0);
          await response.body?.cancel().catch(() => {});
          throw new IndexerError(indexerFailure(source.name, response.status, this.now() + delay));
        }
        const xml = await this.readXml(response);
        const doc = parser.parse(xml) as Record<string, unknown>;
        if (doc.error) {
          const code = Number((doc.error as Record<string, unknown>).code);
          throw new IndexerError(indexerFailure(source.name, code >= 100 && code <= 102 ? 401 : null, this.now() + INDEXER_RETRY_MS));
        }
        if (!doc.rss || typeof doc.rss !== "object" || !Object.hasOwn(doc.rss, "channel")) throw new Error("Invalid Torznab response");
        // A concurrent healthy category must not erase a newer failure/cooldown.
        if (this.backoff.get(providerKey) === blocked) this.backoff.delete(providerKey);
        const bytes = Buffer.byteLength(xml);
        for (const [key, entry] of this.cache) if (entry.until <= this.now()) this.evictCache(key);
        while (this.cache.size >= MAX_ENTRIES || this.cacheBytes + bytes > MAX_CACHE_BYTES) {
          this.evictCache(this.cache.keys().next().value!);
        }
        this.cache.set(requestKey, { until: this.now() + 30_000, xml, bytes });
        this.cacheBytes += bytes;
        return xml;
      } catch (error) {
        if (isRunControlError(error)) throw error;
        const failure = error instanceof IndexerError ? error.failure : indexerFailure(source.name, null, this.now() + INDEXER_RETRY_MS);
        const count = (this.backoff.get(providerKey)?.count ?? 0) + 1;
        const until = failure.kind === "unavailable"
          ? Math.max(failure.retryAt ?? 0, this.now() + Math.min(30 * 60_000, INDEXER_RETRY_MS * 2 ** Math.min(count - 1, 5)))
          : this.now() + INDEXER_RETRY_MS;
        const safeFailure = { ...failure, retryAt: failure.kind === "unavailable" ? until : null };
        if (!this.backoff.has(providerKey) && this.backoff.size >= MAX_ENTRIES) this.backoff.delete(this.backoff.keys().next().value!);
        this.backoff.set(providerKey, { failure: safeFailure, count, until });
        throw new IndexerError(safeFailure);
      }
    })();
    this.inflight.set(requestKey, request);
    try {
      return await request;
    } finally {
      this.inflight.delete(requestKey);
    }
  }

  async search<T extends IndexerMedia>(sources: SearchSource[], query: string, media: T, options: { limit?: number } = {}): Promise<{ results: SearchResult<T>[]; failures: IndexerFailure[] }> {
    const batches = await Promise.all(sources.map(async (source): Promise<{ results: SearchResult<T>[]; failures: IndexerFailure[] }> => {
      try {
        const xml = await this.xml(source, { query, media, ...options });
        return { results: parseTorznabSearch(xml, source.name, media), failures: [] };
      } catch (error) {
        if (isRunControlError(error)) throw error;
        if (!(error instanceof IndexerError)) throw error;
        return { results: [], failures: [error.failure] };
      }
    }));
    return { results: batches.flatMap((batch) => batch.results), failures: batches.flatMap((batch) => batch.failures) };
  }

  async page<T extends IndexerMedia>(source: SearchSource, media: T, offset = 0, limit = 100) {
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 100)
      throw new Error("Invalid feed pagination");
    const xml = await this.xml(source, { media, offset, limit });
    const doc = parser.parse(xml) as { rss: { channel: { response?: { offset?: string; total?: string }; item?: unknown[] | unknown } } };
    const response = doc.rss.channel.response;
    const reportedOffset = parseIntOrNull(response?.offset);
    if (reportedOffset !== null && reportedOffset !== offset)
      throw new IndexerError(indexerFailure(source.name, null, this.now() + INDEXER_RETRY_MS));
    const total = parseIntOrNull(response?.total);
    const count = toArray(doc.rss.channel.item).length;
    return { results: parseTorznabSearch(xml, source.name, media), offset, total, count };
  }
}

export { IndexerGateway as TorznabSearchClient };
export const indexerGateway = new IndexerGateway();

export function searchTorznabBatch(sources: SearchSource[], query: string, media: MediaType): Promise<IndexerSearchBatch> {
  return indexerGateway.search(sources, query, media);
}

export async function searchTorznab(sources: TorznabSource[], query: string, mediaType: MediaType): Promise<TorznabResult[]> {
  const batch = await searchTorznabBatch(sources, query, mediaType);
  if (!batch.results.length && batch.failures.length) throw new IndexerSearchError(batch.failures);
  return batch.results;
}

type TorznabResult = SearchResult<MediaType>;
export type { TorznabResult };
