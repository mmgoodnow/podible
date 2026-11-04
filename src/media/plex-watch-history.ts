import { createHash } from "node:crypto";
import { z } from "zod";

export type PlexWatchHistoryInput = {
  tmdbId: number; imdbId: string | null; plexUrl: string; plexToken: string; plexSectionId: string;
};
export type PlexWatchHistoryObservation = {
  state: "watched" | "unwatched" | "unknown";
  coverage: "global-all-users" | "owner-only" | "unknown";
  playCount: number | null; lastViewedAt: string | null; observedAt: string;
  activePlayback: "active" | "none" | "unknown";
};

const count = z.number().int().nonnegative().safe();
const id = z.union([z.number().int().positive().safe(), z.string().regex(/^[1-9]\d*$/)])
  .transform(String).refine((value) => Number.isSafeInteger(Number(value)));
const inputSchema = z.object({
  tmdbId: z.number().int().positive().safe(), imdbId: z.string().regex(/^tt\d+$/).nullable(),
  plexUrl: z.string().min(1), plexToken: z.string().trim().min(1), plexSectionId: id,
}).strict();
const guids = z.array(z.object({ id: z.string() })).default([]);
const movieSchema = z.object({
  ratingKey: id, librarySectionID: id, type: z.literal("movie"), guid: z.string().optional(), Guid: guids,
});
type Movie = z.output<typeof movieSchema>;
const historySchema = z.object({
  historyKey: z.string().regex(/^\/status\/sessions\/history\/[1-9]\d*$/),
  ratingKey: id, librarySectionID: id, type: z.string(), accountID: id,
  viewedAt: z.number().int().positive().safe(),
});
type History = z.output<typeof historySchema>;
const sessionSchema = z.object({ type: z.string(), ratingKey: id.optional(), guid: z.string().optional(), Guid: guids });
const pageSize = 200, maxRows = 10_000, maxBytes = 2 * 1024 ** 2;
const excludedElements = "Media,Role,Director,Writer,Genre,Country,Collection,Image,Rating,UltraBlurColors";
const digest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const plexGuid = (value: string | undefined) => value?.match(/^plex:\/\/movie\/[a-z\d_-]+$/i)?.[0] ?? null;

function externalIds(item: { guid?: string; Guid: { id: string }[] }) {
  const tmdb = new Set<string>(), imdb = new Set<string>();
  for (const value of [item.guid, ...item.Guid.map((guid) => guid.id)]) {
    if (!value) continue;
    const match = /^(tmdb|themoviedb|imdb|com\.plexapp\.agents\.(?:imdb|themoviedb)):\/\/([^/?#]+)(?:[/?#].*)?$/.exec(value);
    if (!match) continue;
    if (match[1]!.endsWith("imdb") && /^tt\d+$/.test(match[2]!)) imdb.add(match[2]!);
    else if (/^[1-9]\d*$/.test(match[2]!)) tmdb.add(match[2]!);
  }
  return { tmdb, imdb };
}
function identity(item: { guid?: string; Guid: { id: string }[] }, input: PlexWatchHistoryInput) {
  const ids = externalIds(item);
  const matches = ids.tmdb.has(String(input.tmdbId)) || (input.imdbId !== null && ids.imdb.has(input.imdbId));
  const conflict = [...ids.tmdb].some((id) => id !== String(input.tmdbId)) ||
    (input.imdbId !== null && [...ids.imdb].some((id) => id !== input.imdbId));
  return { matches, conflict };
}

/** Server-only seam. No token, URL, account identity or upstream error is returned or logged. */
export function createPlexWatchHistoryReader(options: {
  fetcher?: typeof fetch; now?: () => number; timeoutMs?: number;
  /** Use zero for action-time evidence. Default read-model facts have a bounded TTL. */
  cacheTtlMs?: number;
} = {}) {
  const fetcher = options.fetcher ?? fetch, now = options.now ?? Date.now;
  const timeoutMs = Math.max(1, Math.min(20_000, options.timeoutMs ?? 20_000));
  const cacheTtlMs = Math.max(0, Math.min(60_000, options.cacheTtlMs ?? 60_000));
  const cache = new Map<string, { expiresAt: number; promise: Promise<unknown> }>();
  const cooldown = new Map<string, number>();
  const running = new Map<string, Promise<PlexWatchHistoryObservation>>();
  const unknown = (): PlexWatchHistoryObservation => ({ state: "unknown", coverage: "unknown",
    playCount: null, lastViewedAt: null, observedAt: new Date(now()).toISOString(), activePlayback: "unknown" });

  async function cached<T>(key: string, ttl: number, action: () => Promise<T>): Promise<T> {
    const existing = cache.get(key);
    if (existing && existing.expiresAt > now()) {
      cache.delete(key); cache.set(key, existing);
      return existing.promise as Promise<T>;
    }
    const entry = { expiresAt: Infinity, promise: Promise.resolve().then(action) as Promise<unknown> };
    cache.set(key, entry);
    while (cache.size > 128) cache.delete(cache.keys().next().value!);
    try { const result = await entry.promise as T; entry.expiresAt = now() + ttl; return result; }
    catch (error) { if (cache.get(key) === entry) cache.delete(key); throw error; }
  }

  async function observe(input: PlexWatchHistoryInput): Promise<PlexWatchHistoryObservation> {
    const parsed = inputSchema.parse(input);
    const base = new URL(parsed.plexUrl.replace(/\/+$/, "") + "/");
    if (!["http:", "https:"].includes(base.protocol) || base.username || base.password || base.search || base.hash)
      throw new Error("Invalid Plex configuration");
    const context = digest([base.href, parsed.plexToken]), deadline = now() + timeoutMs;
    if ((cooldown.get(context) ?? 0) > now()) return unknown();
    const pause = (milliseconds: number) => {
      cooldown.set(context, now() + Math.max(1000, Math.min(60_000, milliseconds)));
      while (cooldown.size > 16) cooldown.delete(cooldown.keys().next().value!);
    };
    async function get(route: string, query: Record<string, string> = {}): Promise<unknown> {
      const remaining = Math.min(5000, deadline - now());
      if (remaining <= 0) throw new Error("Plex observation budget exhausted");
      const url = new URL(route, base);
      for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
      const controller = new AbortController();
      let cancelBody: (() => void) | undefined, timer: ReturnType<typeof setTimeout> | undefined;
      const expiry = new Promise<never>((_, reject) => { timer = setTimeout(() => {
        controller.abort(); cancelBody?.(); reject(new Error("Plex request timed out"));
      }, remaining); });
      const request = async () => {
        const response = await fetcher(url, { method: "GET", redirect: "error", signal: controller.signal,
          headers: { Accept: "application/json", "X-Plex-Token": parsed.plexToken, "X-Plex-Product": "Podible retention audit" } });
        if (controller.signal.aborted) { void response.body?.cancel().catch(() => {}); throw new Error("Plex request expired"); }
        if (!response.ok) {
          void response.body?.cancel().catch(() => {});
          const retry = response.headers.get("retry-after");
          const delay = retry && /^\d+$/.test(retry) ? Number(retry) * 1000 : retry ? Date.parse(retry) - now() : 0;
          pause(response.status === 429 ? Number.isFinite(delay) && delay > 0 ? delay : 30_000 : 2000);
          throw new Error("Plex request unavailable");
        }
        if (Number(response.headers.get("content-length")) > maxBytes || !response.body) {
          void response.body?.cancel().catch(() => {}); throw new Error("Plex response exceeds read budget");
        }
        const reader = response.body.getReader();
        cancelBody = () => { void reader.cancel().catch(() => {}); };
        const chunks: Uint8Array[] = []; let bytes = 0;
        try {
          for (;;) {
            const part = await reader.read();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > maxBytes) { void reader.cancel().catch(() => {}); throw new Error("Plex response exceeds read budget"); }
            chunks.push(part.value);
          }
          return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
        } finally { reader.releaseLock(); }
      };
      try { return await Promise.race([request(), expiry]); }
      finally { if (timer !== undefined) clearTimeout(timer); }
    }
    async function page<T>(route: string, query: Record<string, string>, start: number, schema: z.ZodType<T>) {
      const body = z.object({ MediaContainer: z.object({ size: count, offset: count, totalSize: count,
        Metadata: z.array(schema).max(pageSize).default([]) }) }).parse(await get(route, { ...query,
          "X-Plex-Container-Start": String(start), "X-Plex-Container-Size": String(pageSize) })).MediaContainer;
      if (body.offset !== start || body.size !== body.Metadata.length || body.totalSize > maxRows ||
          body.size !== Math.min(pageSize, Math.max(0, body.totalSize - start))) throw new Error("Incomplete Plex page");
      return body;
    }
    async function all<T>(route: string, query: Record<string, string>, schema: z.ZodType<T>, key: (item: T) => string) {
      const first = await page(route, query, 0, schema), items = [...first.Metadata];
      for (let start = pageSize; start < first.totalSize; start += pageSize) {
        const next = await page(route, query, start, schema);
        if (next.totalSize !== first.totalSize) throw new Error("Plex collection changed during pagination");
        items.push(...next.Metadata);
      }
      if (new Set(items.map(key)).size !== items.length) throw new Error("Repeated Plex page or identity");
      const check = await page(route, query, 0, schema);
      if (check.totalSize !== first.totalSize || digest(check.Metadata) !== digest(first.Metadata))
        throw new Error("Plex collection changed during observation");
      return items;
    }
    try {
      // https://developer.plex.tv/pms/: admins see all users, others only their own. Multiple account IDs in an
      // unfiltered request prove global authority; one/zero IDs do NOT prove owner/admin scope.
      // This witness is not used to infer a play count from a partial global-history page.
      const global = await cached(context + ":authority", cacheTtlMs, async () => {
        const body = z.object({ MediaContainer: z.object({ size: count, offset: count, totalSize: count,
          Metadata: z.array(historySchema).max(pageSize).default([]) }) }).parse(await get("status/sessions/history/all", {
            sort: "viewedAt:desc,accountID", "X-Plex-Container-Start": "0", "X-Plex-Container-Size": String(pageSize),
          })).MediaContainer;
        if (body.offset !== 0 || body.size !== body.Metadata.length || body.size !== Math.min(body.totalSize, pageSize))
          throw new Error("Incomplete Plex authority witness");
        return new Set(body.Metadata.map((item) => item.accountID)).size > 1;
      });
      if (!global) return unknown();
      const catalog = await cached(context + ":catalog:" + parsed.plexSectionId, cacheTtlMs, async () => {
        const sections = z.object({ MediaContainer: z.object({ size: count,
          Directory: z.array(z.object({ key: id, type: z.string() })).max(100) }) }).parse(await get("library/sections")).MediaContainer;
        if (sections.size !== sections.Directory.length || !sections.Directory.some((s) => s.key === parsed.plexSectionId && s.type === "movie"))
          throw new Error("Configured movie section is unavailable");
        return all("library/all", { type: "1", includeGuids: "1", excludeElements: excludedElements }, movieSchema, (item) => item.ratingKey);
      });
      const exact = catalog.filter((item) => identity(item, parsed).matches);
      const canonicalGuids = new Set(exact.map((item) => plexGuid(item.guid)).filter((guid) => guid !== null));
      const copies = catalog.filter((item) => identity(item, parsed).matches || (plexGuid(item.guid) !== null && canonicalGuids.has(plexGuid(item.guid)!)));
      if (!copies.length || copies.length > 20 || copies.some((item) => identity(item, parsed).conflict)) return unknown();
      const history = await cached(context + ":history:" + digest([parsed.tmdbId, parsed.imdbId, copies]), Math.min(cacheTtlMs, 30_000), async () => {
        const events: History[] = [];
        for (const copy of copies) {
          const entries = await all("status/sessions/history/all", { metadataItemID: copy.ratingKey, sort: "viewedAt:desc,accountID" }, historySchema, (item) => item.historyKey);
          if (entries.some((item) => item.type !== "movie" || item.ratingKey !== copy.ratingKey || item.librarySectionID !== copy.librarySectionID ||
              item.viewedAt * 1000 > now() + 300_000 || !Number.isFinite(new Date(item.viewedAt * 1000).getTime())))
            throw new Error("History is not scoped to the verified movie identity");
          events.push(...entries);
          if (events.length > maxRows) throw new Error("Movie history exceeds the bounded read budget");
        }
        if (new Set(events.map((item) => item.historyKey)).size !== events.length) throw new Error("Ambiguous movie history identity");
        return { count: events.length, last: events.length ? Math.max(...events.map((event) => event.viewedAt)) * 1000 : null, observedAt: now() };
      });
      // Playback is deliberately fresh, never a cached 'none' that could authorize deleting a live stream.
      const sessions = z.object({ MediaContainer: z.object({ size: count,
        Metadata: z.array(sessionSchema).max(1000).default([]) }) }).parse(await get("status/sessions")).MediaContainer;
      if (sessions.size !== sessions.Metadata.length) throw new Error("Incomplete Plex session observation");
      let activePlayback: PlexWatchHistoryObservation["activePlayback"] = "none";
      for (const session of sessions.Metadata) {
        if (session.type !== "movie") continue;
        if (copies.some((copy) => copy.ratingKey === session.ratingKey) ||
            (plexGuid(session.guid) !== null && canonicalGuids.has(plexGuid(session.guid)!)) || identity(session, parsed).matches) {
          activePlayback = "active"; break;
        }
        if (!catalog.some((item) => item.ratingKey === session.ratingKey)) activePlayback = "unknown";
      }
      return { state: activePlayback !== "none" ? "unknown" : history.count ? "watched" : "unwatched",
        coverage: "global-all-users", playCount: history.count, lastViewedAt: history.last === null ? null : new Date(history.last).toISOString(),
        observedAt: new Date(history.observedAt).toISOString(), activePlayback };
    } catch { pause(Math.max(2000, (cooldown.get(context) ?? 0) - now())); return unknown(); }
  }
  return async (input: PlexWatchHistoryInput): Promise<PlexWatchHistoryObservation> => {
    const key = digest(input);
    const existing = running.get(key);
    if (existing) return { ...await existing };
    if (running.size >= 8) return unknown();
    const result = observe(input).catch(() => unknown());
    running.set(key, result);
    try { return { ...await result }; } finally { if (running.get(key) === result) running.delete(key); }
  };
}

/** GET-only retained PMS history, not cloud history or a claim that every user has watched. */
export const readPlexWatchHistory = createPlexWatchHistoryReader();
