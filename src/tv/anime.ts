import { XMLParser, XMLValidator } from "fast-xml-parser";
import { z } from "zod";
import type { TvCatalogEntry } from "./types";

export const ANIME_MAPPING_URL =
  "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-master.xml";
export const ANIME_LINKS_URL =
  "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json";
export const ANILIST_URL = "https://graphql.anilist.co";

const MAX_DATASET_BYTES = 16 * 1024 * 1024;
const MAX_ROWS = 100_000;
const MAX_RULES = 1_000;
const MAX_EPISODES = 2_000;
const MAX_WARNINGS = 1_000;
const MAX_RELATIONS = 200;

export interface AnimeWarning {
  code: "invalid-record" | "unsupported-mapping" | "conflicting-link" |
    "ambiguous-mapping" | "unmapped-episode" | "metadata-unavailable" | "missing-relations" | "request-limit";
  message: string;
  anidbId?: number;
  episodeKey?: string;
}
type Pair = { kind: "pair"; nativeSeason: number; nativeEpisode: number;
  tvdbSeason: number; tvdbEpisode: number };
type Range = { kind: "range"; nativeSeason: number; start: number; end: number;
  offset: number; tvdbSeason: number };
export interface AnimeMapping {
  anidbId: number;
  tvdbId: number;
  title: string;
  defaultTvdbSeason?: number;
  episodeOffset: number;
  rules: (Pair | Range)[];
  /** An unsupported TVDB rule makes this work unsafe to project, not a partial match. */
  unsupported: boolean;
  unsupportedDefault?: boolean;
}
export interface AnimeLink { anidbId: number; anilistId: number }
export interface AnimeTvdbEpisode {
  /** The original provider episode key, usually tvdb:<episode ID>. */
  key: string;
  title?: string | null;
  airDate?: string | null;
  runtimeMinutes?: number | null;
  season: number;
  episode: number;
  absolute?: number | null;
}
export interface AnimeNumbering {
  scheme: "tvdb:aired" | "anidb";
  season: number;
  episode: number;
  absolute?: number | null;
  source: string;
}
export interface AnimeRelation {
  key: string;
  title: string;
  relation: string;
  source: string;
}
export interface AnimeEntry {
  key: string;
  title: string;
  anime: true;
  anidbId: number;
  anilistId?: number;
  tvdbId: number;
  episodes: {
    key: string;
    sourceEpisodeKey: string;
    title: string | null;
    airDate: string | null;
    runtimeMinutes: number | null;
    numbering: AnimeNumbering[];
  }[];
  relations: AnimeRelation[];
}
export interface AniListMedia {
  id: number;
  title: string;
  episodeCount: number | null;
  relations: { anilistId: number; title: string; relation: string }[];
}
export interface AnimeProjection {
  entries: AnimeEntry[];
  warnings: AnimeWarning[];
  unmappedEpisodeKeys: string[];
}

const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
const array = (value: unknown): unknown[] => value === undefined ? [] :
  Array.isArray(value) ? value : [value];
function integer(value: unknown, min = 0): number | undefined {
  if (typeof value !== "string" && typeof value !== "number") return;
  if (typeof value === "string" && !/^-?\d+$/.test(value)) return;
  const n = Number(value);
  return Number.isSafeInteger(n) && n >= min && n <= 1_000_000_000 ? n : undefined;
}
function warn(warnings: AnimeWarning[], warning: AnimeWarning) {
  if (warnings.length < MAX_WARNINGS) warnings.push(warning);
}
function checkText(text: string) {
  if (Buffer.byteLength(text) > MAX_DATASET_BYTES) throw new Error("Anime dataset exceeds byte limit");
}

/** Only explicit TVDB IDs are indexed. Empty/unknown/movie/provider-specific IDs are not matches. */
export function parseAnimeListXml(xml: string): { records: AnimeMapping[]; warnings: AnimeWarning[] } {
  checkText(xml);
  if (/<!DOCTYPE|<!ENTITY/i.test(xml) || XMLValidator.validate(xml) !== true)
    throw new Error("Invalid or unsafe anime mapping XML");
  const parsed = record(new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_",
    parseAttributeValue: false, parseTagValue: false, trimValues: true }).parse(xml));
  if (!("anime-list" in parsed)) throw new Error("Missing anime-list root");
  const rows = array(record(parsed["anime-list"]).anime);
  if (rows.length > MAX_ROWS) throw new Error("Anime dataset exceeds row limit");
  const records: AnimeMapping[] = [];
  const warnings: AnimeWarning[] = [];
  const identities = new Map<number, AnimeMapping>();
  for (const value of rows) {
    const row = record(value);
    const tvdbId = integer(row["@_tvdbid"], 1);
    if (!tvdbId) continue;
    const anidbId = integer(row["@_anidbid"], 1);
    if (!anidbId) {
      warn(warnings, { code: "invalid-record", message: "TVDB mapping without a valid AniDB ID" });
      continue;
    }
    const season = integer(row["@_defaulttvdbseason"]);
    const offset = row["@_episodeoffset"] === "" || row["@_episodeoffset"] === undefined
      ? 0 : integer(row["@_episodeoffset"], -1_000_000);
    const item: AnimeMapping = { anidbId, tvdbId,
      title: typeof row.name === "string" ? row.name : `AniDB ${anidbId}`,
      defaultTvdbSeason: season, episodeOffset: offset ?? 0, rules: [], unsupported: offset === undefined,
      unsupportedDefault: season === undefined && Boolean(row["@_defaulttvdbseason"]) };
    if (season === undefined && row["@_defaulttvdbseason"])
      warn(warnings, { code: "unsupported-mapping", anidbId,
        message: "Default season is not numeric; absolute/letter season defaults are not inferred" });
    const rules = array(record(row["mapping-list"]).mapping);
    if (rules.length > MAX_RULES) throw new Error("Anime work exceeds mapping rule limit");
    for (const value of rules) {
      const rule = record(value);
      if (rule["@_tvdbseason"] === undefined) continue; // TMDB mappings are not TVDB evidence.
      const nativeSeason = integer(rule["@_anidbseason"]);
      const tvdbSeason = integer(rule["@_tvdbseason"]);
      if ((nativeSeason !== 0 && nativeSeason !== 1) || tvdbSeason === undefined) {
        item.unsupported = true;
        continue;
      }
      const text = rule["#text"];
      if (text !== undefined && text !== "") {
        if (typeof text !== "string" || text.length > 32_000) {
          item.unsupported = true;
          continue;
        }
        for (const token of text.split(";").map(t => t.trim()).filter(Boolean)) {
          const match = /^(\d+)-(\d+)$/.exec(token);
          const nativeEpisode = match ? integer(match[1], 1) : undefined;
          const tvdbEpisode = match ? integer(match[2]) : undefined;
          if (nativeEpisode === undefined || tvdbEpisode === undefined ||
            (nativeSeason === 0 && nativeEpisode >= 201)) {
            item.unsupported = true;
            continue;
          }
          item.rules.push({ kind: "pair", nativeSeason, nativeEpisode, tvdbSeason, tvdbEpisode });
        }
      }
      const hasRange = ["start", "end", "offset"].some(key => rule[`@_${key}`] !== undefined);
      if (hasRange) {
        const start = integer(rule["@_start"], 1);
        const end = integer(rule["@_end"], 1);
        const offset = rule["@_offset"] === undefined ? 0 : integer(rule["@_offset"], -1_000_000);
        if (start === undefined || end === undefined || end < start || offset === undefined ||
          (nativeSeason === 0 && end >= 201)) item.unsupported = true;
        else item.rules.push({ kind: "range", nativeSeason, start, end, offset, tvdbSeason });
      } else if (text === undefined || text === "") {
        item.unsupported = true;
      }
      if (item.rules.length > MAX_RULES) throw new Error("Anime work exceeds expanded rule limit");
    }
    const previous = identities.get(anidbId);
    if (previous) {
      item.unsupported = true;
      previous.unsupported = true;
    } else identities.set(anidbId, item);
    if (item.unsupported) warn(warnings, { code: "unsupported-mapping", anidbId,
      message: "Work has duplicate identity or unsupported TVDB rules; no episodes will be guessed" });
    records.push(item);
  }
  return { records, warnings };
}

/** Reject conflicting IDs in either direction instead of selecting the first row. */
export function parseAnimeLinksJson(json: string): { links: AnimeLink[]; warnings: AnimeWarning[] } {
  checkText(json);
  const rows: unknown = JSON.parse(json);
  if (!Array.isArray(rows) || rows.length > MAX_ROWS) throw new Error("Invalid anime ID crosswalk");
  const forward = new Map<number, Set<number>>();
  const reverse = new Map<number, Set<number>>();
  for (const value of rows) {
    const row = record(value);
    const a = integer(row.anidb_id, 1);
    const l = integer(row.anilist_id, 1);
    if (!a || !l) continue;
    if (!forward.has(a)) forward.set(a, new Set());
    if (!reverse.has(l)) reverse.set(l, new Set());
    forward.get(a)!.add(l);
    reverse.get(l)!.add(a);
  }
  const warnings: AnimeWarning[] = [];
  const links: AnimeLink[] = [];
  for (const [anidbId, candidates] of forward) {
    const [anilistId] = candidates;
    if (candidates.size !== 1 || reverse.get(anilistId!)!.size !== 1) {
      warn(warnings, { code: "conflicting-link", anidbId,
        message: "AniDB/AniList crosswalk is not unique; link withheld" });
    } else links.push({ anidbId, anilistId: anilistId! });
  }
  return { links, warnings };
}

const titleSchema = z.object({ english: z.string().nullable().optional(),
  romaji: z.string().nullable().optional(), native: z.string().nullable().optional() });
const relationSchema = z.enum(["ADAPTATION", "PREQUEL", "SEQUEL", "PARENT", "SIDE_STORY",
  "CHARACTER", "SUMMARY", "ALTERNATIVE", "SPIN_OFF", "OTHER", "SOURCE", "COMPILATION", "CONTAINS", "SAME_UNIVERSE"]);
const mediaSchema = z.object({ id: z.number().int().positive(), type: z.literal("ANIME"),
  title: titleSchema, episodes: z.number().int().positive().nullable(),
  relations: z.object({ edges: z.array(z.object({ relationType: relationSchema,
    node: z.object({ id: z.number().int().positive(), type: z.enum(["ANIME", "MANGA"]), title: titleSchema }) })) }) });
const displayTitle = (title: z.infer<typeof titleSchema>, id: number) =>
  title.english || title.romaji || title.native || `AniList ${id}`;

/** Accept only the exact requested anime ID; GraphQL partial/error results are not authoritative. */
export function parseAniListMedia(payload: unknown, requestedId: number): AniListMedia {
  const root = record(payload);
  if (array(root.errors).length) throw new Error("AniList GraphQL returned errors");
  const media = mediaSchema.parse(record(root.data).Media);
  if (media.id !== requestedId) throw new Error("AniList returned a different ID");
  if (media.relations.edges.length > MAX_RELATIONS) throw new Error("AniList relations exceed limit");
  return { id: media.id, title: displayTitle(media.title, media.id), episodeCount: media.episodes,
    relations: media.relations.edges.filter(edge => edge.node.type === "ANIME")
      .map(edge => ({ anilistId: edge.node.id, title: displayTitle(edge.node.title, edge.node.id),
        relation: edge.relationType })) };
}

type Native = { season: number; episode: number };
function natives(mapping: AnimeMapping, source: AnimeTvdbEpisode, count?: number | null): Native[] {
  if (mapping.unsupported) return [];
  const pairs = mapping.rules.filter((r): r is Pair => r.kind === "pair");
  const overridden = (season: number, episode: number) =>
    pairs.some(p => p.nativeSeason === season && p.nativeEpisode === episode);
  const result: Native[] = [];
  for (const rule of mapping.rules) {
    if (rule.tvdbSeason !== source.season) continue;
    if (rule.kind === "pair") {
      if (rule.tvdbEpisode > 0 && rule.tvdbEpisode === source.episode)
        result.push({ season: rule.nativeSeason, episode: rule.nativeEpisode });
    } else {
      const n = source.episode - rule.offset;
      if (n >= rule.start && n <= rule.end && !overridden(rule.nativeSeason, n))
        result.push({ season: rule.nativeSeason, episode: n });
    }
  }
  const n = source.episode - mapping.episodeOffset;
  // Special-season one-offs with explicit regular mappings are closed lists, not unbounded defaults.
  const closedSpecials = mapping.defaultTvdbSeason === 0 && mapping.rules.some(r => r.nativeSeason === 1);
  const explicitlyMapped = mapping.rules.some(r => r.nativeSeason === 1 &&
    (r.kind === "pair" ? r.nativeEpisode === n : n >= r.start && n <= r.end));
  if (!closedSpecials && mapping.defaultTvdbSeason === source.season && n >= 1 &&
    !explicitlyMapped && (count == null || n <= count)) result.push({ season: 1, episode: n });
  return [...new Map(result.map(n => [`${n.season}:${n.episode}`, n])).values()];
}

export interface ProjectAnimeOptions {
  tvdbId: number;
  episodes: readonly AnimeTvdbEpisode[];
  mappings: readonly AnimeMapping[];
  links: readonly AnimeLink[];
  media?: readonly AniListMedia[];
}
export function projectAnimeEntries(input: ProjectAnimeOptions): AnimeProjection {
  if (!integer(input.tvdbId, 1) || input.episodes.length > MAX_EPISODES)
    throw new Error("Invalid or oversized anime projection request");
  if (input.mappings.length > MAX_ROWS || input.links.length > MAX_ROWS || (input.media?.length ?? 0) > 200)
    throw new Error("Anime projection datasets exceed row limit");
  const warnings: AnimeWarning[] = [];
  const unmappedEpisodeKeys: string[] = [];
  const entries = new Map<number, AnimeEntry>();
  const mappings = input.mappings.filter(row => row.tvdbId === input.tvdbId);
  if (mappings.length > 200) throw new Error("Too many anime works for one TVDB series");
  // Also validate injected links: callers need not have used our JSON parser.
  const checkedLinks = parseAnimeLinksJson(JSON.stringify(input.links.map(l =>
    ({ anidb_id: l.anidbId, anilist_id: l.anilistId }))));
  warnings.push(...checkedLinks.warnings);
  const links = new Map(checkedLinks.links.map(l => [l.anidbId, l.anilistId]));
  const reverse = new Map(checkedLinks.links.map(l => [l.anilistId, l.anidbId]));
  const media = new Map<number, AniListMedia>();
  for (const item of input.media ?? []) {
    if (media.has(item.id)) throw new Error("Duplicate AniList media ID");
    media.set(item.id, item);
  }
  for (const mapping of mappings) if (mapping.unsupported || mapping.defaultTvdbSeason === undefined)
    warn(warnings, { code: "unsupported-mapping", anidbId: mapping.anidbId,
      message: "Unsupported mapping/default; only supported explicit coordinates can be used" });
  const keys = new Set<string>();
  for (const episode of input.episodes) {
    if (!episode.key || keys.has(episode.key) || integer(episode.season) === undefined ||
      integer(episode.episode, 1) === undefined) throw new Error("Invalid or duplicate TVDB episode");
    keys.add(episode.key);
    const candidates = mappings.flatMap(mapping => natives(mapping, episode,
      media.get(links.get(mapping.anidbId)!)?.episodeCount).map(native => ({ mapping, native })));
    const unsafeCompetitor = mappings.some(m => (m.unsupportedDefault &&
      !candidates.some(c => c.mapping === m)) || (m.unsupported &&
      (m.defaultTvdbSeason === undefined || m.defaultTvdbSeason === episode.season ||
        m.rules.some(r => r.tvdbSeason === episode.season))));
    if (candidates.length !== 1 || unsafeCompetitor) {
      unmappedEpisodeKeys.push(episode.key);
      warn(warnings, { code: unsafeCompetitor ? "unsupported-mapping" : candidates.length ? "ambiguous-mapping" : "unmapped-episode",
        episodeKey: episode.key, message: unsafeCompetitor
          ? "Unsupported competing work may cover these TVDB coordinates; identity withheld" : candidates.length
          ? "Multiple native episodes/works match TVDB coordinates; identity withheld"
          : "No supported explicit/default mapping for TVDB coordinates" });
      continue;
    }
    const { mapping, native } = candidates[0]!;
    let entry = entries.get(mapping.anidbId);
    if (!entry) {
      const anilistId = links.get(mapping.anidbId);
      const detail = media.get(anilistId!);
      entry = { key: `anidb:${mapping.anidbId}`, title: detail?.title ?? mapping.title,
        anime: true, anidbId: mapping.anidbId, anilistId, tvdbId: input.tvdbId, episodes: [],
        relations: detail?.relations.map(relation => ({
          key: reverse.has(relation.anilistId) ? `anidb:${reverse.get(relation.anilistId)}` : `anilist:${relation.anilistId}`,
          title: relation.title, relation: relation.relation, source: `${ANILIST_URL}#Media:${detail!.id}`,
        })) ?? [] };
      entries.set(mapping.anidbId, entry);
      if (!detail) warn(warnings, { code: "missing-relations", anidbId: mapping.anidbId,
        message: anilistId ? "Exact AniList metadata unavailable; relations are unknown, not absent"
          : "No unique AniList crosswalk; AniDB work is valid but relations are unknown" });
    }
    entry.episodes.push({ key: episode.key, sourceEpisodeKey: episode.key,
      title: episode.title ?? null, airDate: episode.airDate ?? null,
      runtimeMinutes: episode.runtimeMinutes ?? null,
      numbering: [{ scheme: "tvdb:aired", season: episode.season, episode: episode.episode, source: "tvdb",
        ...(episode.absolute !== undefined ? { absolute: episode.absolute } : {}) },
      { scheme: "anidb", season: native.season, episode: native.episode,
        source: `${ANIME_MAPPING_URL}#anidb:${mapping.anidbId}` }] });
  }
  return { entries: [...entries.values()], warnings, unmappedEpisodeKeys };
}

export class AnimeMetadataUnavailableError extends Error {
  constructor(message: string, readonly retryAt?: number) { super(message); }
}
export interface AnimeAdapterOptions {
  fetch?: (url: string, init?: RequestInit) => Promise<Response>;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  datasetTtlMs?: number;
  mediaTtlMs?: number;
  maxCacheEntries?: number;
  maxMediaRequests?: number;
  timeoutMs?: number;
  maxDatasetBytes?: number;
  maxMediaBytes?: number;
  minRequestIntervalMs?: number;
}
interface AnimeDatasets {
  mappings: AnimeMapping[];
  links: AnimeLink[];
  warnings: AnimeWarning[];
}
export interface AnimeCatalogProjection {
  entries: TvCatalogEntry[];
  warnings: AnimeWarning[];
  unmappedEpisodeKeys: string[];
}
export interface AnimeMetadataAdapter {
  loadDatasets(): Promise<AnimeDatasets>;
  getMedia(anilistId: number): Promise<AniListMedia>;
  enrich(input: { tvdbId: number; episodes: readonly AnimeTvdbEpisode[] }): Promise<AnimeProjection>;
  enrichAnimeEntry(entry: TvCatalogEntry): Promise<AnimeCatalogProjection>;
}
const RELATIONS_QUERY = `query AnimeSidecar($id: Int!) {
  Media(id: $id, type: ANIME) {
    id type episodes title { english romaji native }
    relations { edges { relationType(version: 3) node { id type title { english romaji native } } } }
  }
}`;
function option(value: number | undefined, fallback: number, min: number, max: number) {
  const n = value ?? fallback;
  if (!Number.isSafeInteger(n) || n < min || n > max) throw new Error("Invalid anime adapter limit");
  return n;
}

/** Optional sidecar: no DB, credentials, provider identity replacement, or recursive relation fetches. */
export function createAnimeMetadataAdapter(options: AnimeAdapterOptions = {}): AnimeMetadataAdapter {
  const fetcher = options.fetch ?? fetch;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const datasetTtl = option(options.datasetTtlMs, 24 * 60 * 60_000, 0, 7 * 24 * 60 * 60_000);
  const mediaTtl = option(options.mediaTtlMs, 6 * 60 * 60_000, 0, 7 * 24 * 60 * 60_000);
  const cacheLimit = option(options.maxCacheEntries, 64, 1, 128);
  const requestLimit = option(options.maxMediaRequests, 8, 0, 16);
  const timeout = option(options.timeoutMs, 15_000, 1, 30_000);
  const datasetBytes = option(options.maxDatasetBytes, MAX_DATASET_BYTES, 1, MAX_DATASET_BYTES);
  const mediaBytes = option(options.maxMediaBytes, 256 * 1024, 1, 1024 * 1024);
  const interval = option(options.minRequestIntervalMs, 2_100, 0, 10_000);
  let snapshot: { until: number; value: AnimeDatasets } | undefined;
  let loading: Promise<AnimeDatasets> | undefined;
  let busy = false;
  let nextAniListRequest = 0;
  const cooldown = new Map<string, number>();
  const cache = new Map<number, { until: number; value: AniListMedia }>();
  const pending = new Map<number, Promise<AniListMedia>>();

  async function request(url: string, maxBytes: number, init: RequestInit = {}): Promise<string> {
    const host = new URL(url).host;
    const retryAt = cooldown.get(host) ?? 0;
    if (retryAt > now()) throw new AnimeMetadataUnavailableError("Anime upstream is cooling down", retryAt);
    if (busy) throw new AnimeMetadataUnavailableError("Anime adapter is busy; retry later");
    busy = true;
    const controller = new AbortController();
    let reader: Pick<ReadableStreamDefaultReader<Uint8Array>, "read" | "cancel" | "releaseLock"> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      if (url === ANILIST_URL && nextAniListRequest > now())
        await sleep(Math.min(interval, nextAniListRequest - now()));
      const deadline = new Promise<never>((_, reject) => {
        timer = setTimeout(() => { controller.abort(); void reader?.cancel().catch(() => {});
          reject(new AnimeMetadataUnavailableError("Anime upstream timed out")); }, timeout);
      });
      const work = async () => {
        const response = await fetcher(url, { ...init, redirect: "error", signal: controller.signal });
        controller.signal.throwIfAborted();
        if (url === ANILIST_URL) nextAniListRequest = now() + interval;
        if (!response.ok || response.headers.get("x-ratelimit-remaining") === "0") {
          const raw = response.headers.get("retry-after");
          const seconds = raw !== null && /^\d+(?:\.\d+)?$/.test(raw) ? Number(raw) : NaN;
          const date = raw !== null && !Number.isFinite(seconds) ? Date.parse(raw) : NaN;
          const reset = Number(response.headers.get("x-ratelimit-reset")) * 1000;
          const exhausted = response.status === 429 || response.headers.get("x-ratelimit-remaining") === "0";
          const retry = Math.max(now() + (exhausted ? 60_000 : 1_000),
            Number.isFinite(seconds) ? now() + seconds * 1000 : 0,
            Number.isFinite(date) ? date : 0, Number.isFinite(reset) ? reset : 0);
          cooldown.set(host, retry);
          if (!response.ok) {
            await response.body?.cancel();
            throw new AnimeMetadataUnavailableError(`Anime upstream returned HTTP ${response.status}`, retry);
          }
        }
        const length = response.headers.get("content-length");
        if (length !== null && Number(length) > maxBytes) {
          await response.body?.cancel();
          throw new AnimeMetadataUnavailableError("Anime response exceeds byte limit");
        }
        reader = response.body?.getReader();
        if (!reader) throw new AnimeMetadataUnavailableError("Anime upstream returned an empty body");
        const bytes = Buffer.allocUnsafe(maxBytes);
        let size = 0;
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (size + value.byteLength > maxBytes) throw new AnimeMetadataUnavailableError("Anime response exceeds byte limit");
            bytes.set(value, size);
            size += value.byteLength;
          }
          return bytes.toString("utf8", 0, size);
        } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
      };
      return await Promise.race([work(), deadline]);
    } catch (error) {
      cooldown.set(host, Math.max(cooldown.get(host) ?? 0, now() + 1_000));
      if (error instanceof AnimeMetadataUnavailableError) throw error;
      throw new AnimeMetadataUnavailableError("Anime upstream is unavailable or invalid");
    } finally { if (timer) clearTimeout(timer); controller.abort(); busy = false; }
  }
  async function loadDatasets(): Promise<AnimeDatasets> {
    if (snapshot && snapshot.until > now()) return structuredClone(snapshot.value);
    if (loading) return structuredClone(await loading);
    loading = (async () => {
      const mappings = parseAnimeListXml(await request(ANIME_MAPPING_URL, datasetBytes));
      const links = parseAnimeLinksJson(await request(ANIME_LINKS_URL, datasetBytes));
      const value = { mappings: mappings.records, links: links.links,
        warnings: [...mappings.warnings, ...links.warnings].slice(0, MAX_WARNINGS) };
      snapshot = { until: now() + datasetTtl, value };
      return value;
    })();
    try { return structuredClone(await loading); }
    catch (error) {
      const host = new URL(ANIME_MAPPING_URL).host;
      cooldown.set(host, Math.max(cooldown.get(host) ?? 0, now() + 1_000));
      throw error;
    } finally { loading = undefined; }
  }
  async function getMedia(anilistId: number): Promise<AniListMedia> {
    if (!integer(anilistId, 1)) throw new Error("Invalid AniList ID");
    const cached = cache.get(anilistId);
    if (cached && cached.until > now()) {
      cache.delete(anilistId); cache.set(anilistId, cached);
      return structuredClone(cached.value);
    }
    const existing = pending.get(anilistId);
    if (existing) return structuredClone(await existing);
    if (pending.size >= 16) throw new AnimeMetadataUnavailableError("Too many pending AniList requests");
    const loading = (async () => {
      const text = await request(ANILIST_URL, mediaBytes, { method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ query: RELATIONS_QUERY, variables: { id: anilistId } }) });
      const payload: unknown = JSON.parse(text);
      if (array(record(payload).errors).some(e => integer(record(e).status) === 429))
        throw new AnimeMetadataUnavailableError("AniList GraphQL is rate limited", now() + 60_000);
      const value = parseAniListMedia(payload, anilistId);
      cache.delete(anilistId);
      while (cache.size >= cacheLimit) cache.delete(cache.keys().next().value!);
      cache.set(anilistId, { until: now() + mediaTtl, value });
      return value;
    })();
    pending.set(anilistId, loading);
    try { return structuredClone(await loading); }
    catch (error) {
      const host = new URL(ANILIST_URL).host;
      cooldown.set(host, Math.max(cooldown.get(host) ?? 0, now() + 1_000,
        error instanceof AnimeMetadataUnavailableError ? error.retryAt ?? 0 : 0));
      throw error;
    } finally { pending.delete(anilistId); }
  }
  async function enrich(input: { tvdbId: number; episodes: readonly AnimeTvdbEpisode[] }): Promise<AnimeProjection> {
    // Validate before any network I/O.
    projectAnimeEntries({ ...input, mappings: [], links: [] });
    let datasets: AnimeDatasets;
    try { datasets = await loadDatasets(); }
    catch { return { entries: [], unmappedEpisodeKeys: input.episodes.map(e => e.key),
      warnings: [{ code: "metadata-unavailable", message: "Anime datasets unavailable; retain base TV catalog" }] }; }
    const mappings = datasets.mappings.filter(m => m.tvdbId === input.tvdbId);
    if (mappings.length > 200) return { entries: [], unmappedEpisodeKeys: input.episodes.map(e => e.key),
      warnings: [{ code: "request-limit", message: "Aggregate exceeds anime work limit; retain base TV catalog" }] };
    const ids = [...new Set(mappings.filter(m => !m.unsupported).flatMap(m =>
      datasets.links.filter(l => l.anidbId === m.anidbId).map(l => l.anilistId)))];
    const warnings: AnimeWarning[] = datasets.warnings.filter(w =>
      w.anidbId === undefined || mappings.some(m => m.anidbId === w.anidbId));
    const media: AniListMedia[] = [];
    for (const id of ids.slice(0, requestLimit)) {
      try { media.push(await getMedia(id)); }
      catch { warn(warnings, { code: "metadata-unavailable",
        message: `Exact AniList ${id} unavailable; no relation/title inference` }); }
    }
    if (ids.length > requestLimit) warn(warnings, { code: "request-limit",
      message: "AniList work request limit reached; remaining works use mapping evidence only" });
    const result = projectAnimeEntries({ ...input, mappings, links: datasets.links, media });
    result.warnings = [...warnings, ...result.warnings].slice(0, MAX_WARNINGS);
    return result;
  }
  async function enrichAnimeEntry(entry: TvCatalogEntry): Promise<AnimeCatalogProjection> {
    if (entry.episodes.length > MAX_EPISODES) throw new Error("Anime entry exceeds episode limit");
    if (!entry.tvdbId) return { entries: [structuredClone(entry)], unmappedEpisodeKeys: entry.episodes.map(e => e.key),
      warnings: [{ code: "unmapped-episode", message: "No explicit aggregate TVDB ID; anime split not attempted" }] };
    const coordinates: AnimeTvdbEpisode[] = [];
    const warnings: AnimeWarning[] = [];
    const keys = new Set<string>();
    for (const episode of entry.episodes) {
      if (keys.has(episode.key)) throw new Error("Duplicate source episode key");
      keys.add(episode.key);
      const maps = episode.numbering.filter(n => n.scheme === "tvdb:aired" &&
        integer(n.season) !== undefined && integer(n.episode, 1) !== undefined);
      const distinct = new Map(maps.map(n => [`${n.season}:${n.episode}`, n]));
      if (distinct.size !== 1) {
        warn(warnings, { code: "unmapped-episode", episodeKey: episode.key,
          message: "Missing or conflicting TVDB aired numbering; retain provider episode" });
        continue;
      }
      const map = [...distinct.values()][0]!;
      coordinates.push({ key: episode.key, title: episode.title, airDate: episode.airDate,
        runtimeMinutes: episode.runtimeMinutes, season: map.season!, episode: map.episode!, absolute: map.absolute });
    }
    const projected = coordinates.length ? await enrich({ tvdbId: entry.tvdbId, episodes: coordinates })
      : { entries: [], warnings: [], unmappedEpisodeKeys: [] };
    const originals = new Map(entry.episodes.map(e => [e.key, e]));
    const mapped = new Set(projected.entries.flatMap(e => e.episodes.map(ep => ep.key)));
    const entries: TvCatalogEntry[] = projected.entries.map(work => {
      const source = `${ANIME_MAPPING_URL}#anidb:${work.anidbId}`;
      return { key: work.key, tvdbId: work.tvdbId, tmdbId: entry.tmdbId,
        // Neither aggregate year nor an arbitrary requested episode establishes work premiere year.
        title: work.title, anime: true, year: null, aliases: [], source,
        refs: [...(entry.refs ?? []), { provider: "anidb", id: String(work.anidbId), instance: null, source },
          ...(work.anilistId ? [{ provider: "anilist" as const, id: String(work.anilistId), instance: null,
            source: ANIME_LINKS_URL }] : [])], relations: work.relations,
        episodes: work.episodes.map(ep => ({ ...structuredClone(originals.get(ep.key)!),
          numbering: [...structuredClone(originals.get(ep.key)!.numbering),
            ...ep.numbering.filter(n => n.scheme === "anidb").map(n => ({ ...n, absolute: null }))] })) };
    });
    const unmatched = entry.episodes.filter(ep => !mapped.has(ep.key));
    if (unmatched.length || !entries.length) entries.push({ ...structuredClone(entry), episodes: structuredClone(unmatched) });
    return { entries, warnings: [...warnings, ...projected.warnings].slice(0, MAX_WARNINGS),
      unmappedEpisodeKeys: unmatched.map(e => e.key) };
  }
  return { loadDatasets, getMedia, enrich, enrichAnimeEntry };
}

let defaultAdapter: AnimeMetadataAdapter | undefined;
/** Reuse a factory-created adapter for injected fetch/clock/cache boundaries. */
export async function enrichAnimeEntry(entry: TvCatalogEntry,
  adapter: AnimeMetadataAdapter = defaultAdapter ??= createAnimeMetadataAdapter()): Promise<AnimeCatalogProjection> {
  return adapter.enrichAnimeEntry(entry);
}
