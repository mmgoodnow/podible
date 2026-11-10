import { createHash, createHmac } from "node:crypto";
import { z } from "zod";
import type { IndexerMedia, SearchResult } from "./indexer-gateway";

export const AUTOBRR_WEBHOOK_PATH = "/hooks/autobrr";
export const AUTOBRR_ID_PREFIX = "autobrr:";
const digest = (value: string) => createHash("sha256").update(value).digest("hex");

function httpUrl(value: string): URL | null {
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) && !url.username && !url.password && !url.hash ? url : null;
  } catch {
    return null;
  }
}

export type AutobrrConfig = {
  apiUrl: string;
  apiKey: string;
  callbackUrl: string;
  authorization: string;
  downloadOrigins: ReadonlySet<string>;
};

export function readAutobrrConfig(env: Record<string, string | undefined>): AutobrrConfig | null {
  const base = env.AUTOBRR_URL?.trim();
  if (!base) return null;
  const api = httpUrl(base), callback = httpUrl(env.AUTOBRR_PODIBLE_URL ?? "");
  const apiKey = env.AUTOBRR_API_KEY?.trim();
  const origins = (env.AUTOBRR_DOWNLOAD_ORIGINS ?? "").split(",").map(s => s.trim()).filter(Boolean);
  const downloadOrigins = new Set<string>();
  for (const origin of origins) {
    const url = httpUrl(origin);
    if (!url || url.pathname !== "/" || url.search)
      throw new Error("AUTOBRR_DOWNLOAD_ORIGINS must contain only HTTP(S) origins");
    downloadOrigins.add(url.origin);
  }
  if (!api || api.search || !callback || callback.search || !apiKey || !downloadOrigins.size)
    throw new Error("Set AUTOBRR_URL, AUTOBRR_API_KEY, AUTOBRR_PODIBLE_URL and AUTOBRR_DOWNLOAD_ORIGINS");
  api.pathname = api.pathname.replace(/\/+$/, "") + "/api/notification";
  callback.pathname = callback.pathname.replace(/\/+$/, "") + AUTOBRR_WEBHOOK_PATH;
  const token = createHmac("sha256", apiKey).update("podible/autobrr/webhook/v1\0" + callback.href).digest("hex");
  return { apiUrl: api.href, apiKey, callbackUrl: callback.href, authorization: "Bearer " + token, downloadOrigins };
}

const notificationSchema = z.object({
  id: z.number().int().positive(),
  name: z.string(),
  type: z.string(),
  enabled: z.boolean(),
  webhook: z.string(),
  events: z.array(z.string()).nullable(),
  headers: z.string().optional(),
  method: z.string().optional(),
});

/** List before writing, including after an ambiguous POST failure. Never adopt another callback. */
export async function reconcileAutobrrWebhook(config: AutobrrConfig, fetcher: typeof fetch = fetch, signal?: AbortSignal) {
  const desired = {
    name: "Podible release intake " + digest(config.callbackUrl).slice(0, 12),
    type: "WEBHOOK",
    enabled: true,
    webhook: config.callbackUrl,
    events: ["RELEASE_NEW"],
    method: "POST",
    headers: "Authorization=" + config.authorization,
  };
  const request = async (url: string, method: string, body?: object) => {
    const response = await fetcher(url, {
      method, redirect: "error",
      signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(10_000)]) : AbortSignal.timeout(10_000),
      headers: { "X-API-Token": config.apiKey, "Content-Type": "application/json" },
      body: body && JSON.stringify(body),
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      throw new Error("Autobrr notification API returned HTTP " + response.status);
    }
    return response;
  };
  const response = await request(config.apiUrl, "GET");
  const notifications = z.array(notificationSchema).nullable().parse(await response.json()) ?? [];
  const matches = notifications.filter(n => n.name === desired.name);
  const current = matches[0];
  if (matches.length > 1 || current && (current.type !== desired.type || current.webhook !== desired.webhook))
    throw new Error("Conflicting Podible notification in autobrr");
  if (current?.enabled && current.headers === desired.headers && current.method === desired.method &&
      current.events?.length === 1 && current.events[0] === "RELEASE_NEW") return;
  const written = current
    ? await request(config.apiUrl + "/" + current.id, "PUT", { ...desired, id: current.id })
    : await request(config.apiUrl, "POST", desired);
  await written.body?.cancel().catch(() => {});
}

const shortText = z.string().trim().min(1).max(256);
const count = z.number().int().nonnegative().safe().optional();
const eventSchema = z.object({
  event: z.literal("release.new"),
  timestamp: z.iso.datetime({ offset: true }).optional(),
  data: z.object({
    indexer: z.object({ name: shortText, identifier: shortText.optional() }),
    release: z.object({
      name: z.string().trim().min(1).max(1024),
      protocol: z.literal("torrent"),
      type: z.string().max(64).optional(),
      category: z.string().max(256).optional(),
      categories: z.array(z.string().max(256)).max(64).optional(),
      download_url: z.string().max(8192),
      external_id: shortText.optional(),
      size: count,
      seeders: count,
      leechers: count,
      meta_imdb: z.string().max(32).optional(),
      meta_tmdb: count,
      meta_tvdb: count,
      freeleech: z.boolean().optional(),
    }),
  }),
});

function releaseMedia(type: string): IndexerMedia | undefined {
  const value = type.trim().toLowerCase();
  if (value === "3030" || value === "audiobook" || value === "audio/audiobook") return "audio";
  if (value === "7020" || value === "ebook" || value === "e-book" || value === "books/ebook") return "ebook";
  if (value === "movie" || value === "movies" || /^2\d{3}$/.test(value)) return "movie";
  if (["tv", "series", "episode"].includes(value) || /^5\d{3}$/.test(value)) return "tv";
}

export function parseAutobrrRelease(config: AutobrrConfig, body: unknown) {
  const parsed = eventSchema.safeParse(body);
  if (!parsed.success) return null;
  const { release, indexer } = parsed.data.data;
  const url = httpUrl(release.download_url);
  if (!url || !config.downloadOrigins.has(url.origin)) return null;
  const categories = new Set([release.category ?? "", ...release.categories ?? []].map(releaseMedia).filter(Boolean));
  if (categories.size > 1) return null;
  const mediaType = [...categories][0] ?? releaseMedia(release.type ?? "");
  if (!mediaType) return null;
  const feedId = AUTOBRR_ID_PREFIX + digest(JSON.stringify([config.apiUrl, indexer.identifier ?? indexer.name, mediaType]));
  const attr: { name: string; value: string }[] = [];
  if (release.meta_imdb && /^tt\d+$/i.test(release.meta_imdb)) attr.push({ name: "imdbid", value: release.meta_imdb });
  if (release.meta_tmdb) attr.push({ name: "tmdbid", value: String(release.meta_tmdb) });
  if (release.meta_tvdb) attr.push({ name: "tvdbid", value: String(release.meta_tvdb) });
  if (release.freeleech) attr.push({ name: "freeleech", value: "1" });
  const result: SearchResult<IndexerMedia> = {
    mediaType, provider: indexer.name, title: release.name, url: url.href,
    guid: AUTOBRR_ID_PREFIX + digest(JSON.stringify([feedId, release.external_id ?? url.href])),
    sizeBytes: release.size ?? null, infoHash: null, seeders: release.seeders ?? null, leechers: release.leechers ?? null,
    raw: { attr, pubDate: parsed.data.timestamp },
  };
  return { feedId, result };
}
