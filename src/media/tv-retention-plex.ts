import { z } from "zod";
import { TV_RETENTION_MAX_EPISODES } from "./tv-retention-types";

export type TvRetentionWatchInput = {
  tvdbId: number; episodes: { id: number; key: string; providerId: string }[];
  plexUrl: string; plexToken: string; plexSectionId: string;
};
export type TvRetentionWatch = {
  state: "watched" | "unwatched" | "unknown"; coverage: "global-all-users" | "unknown";
  activePlayback: "active" | "none" | "unknown";
  episodes: { episodeId: number; episodeKey: string; state: "watched" | "unwatched" | "unknown";
    playCount: number | null; lastViewedAt: string | null }[];
};
export const unknownTvRetentionWatch = (input: Pick<TvRetentionWatchInput, "episodes">): TvRetentionWatch => ({
  state: "unknown", coverage: "unknown", activePlayback: "unknown",
  episodes: input.episodes.map((episode) => ({ episodeId: episode.id, episodeKey: episode.key, state: "unknown", playCount: null, lastViewedAt: null })),
});
const count = z.number().int().nonnegative().safe();
const id = z.union([z.string().regex(/^[1-9]\d*$/), z.number().int().positive().safe()]).transform(String)
  .refine((value) => Number.isSafeInteger(Number(value)));
const guids = z.array(z.object({ id: z.string() })).max(20).default([]);
const showSchema = z.object({ ratingKey: id, librarySectionID: id, type: z.literal("show"), guid: z.string().optional(), Guid: guids });
const episodeSchema = z.object({ ratingKey: id, librarySectionID: id, grandparentRatingKey: id, type: z.literal("episode"), Guid: guids });
const historySchema = z.object({ historyKey: z.string().regex(/^\/status\/sessions\/history\/[1-9]\d*$/),
  ratingKey: id, librarySectionID: id, type: z.string(), accountID: id, viewedAt: z.number().int().positive().safe() });
const sessionSchema = z.object({ type: z.string(), ratingKey: id.optional(), grandparentRatingKey: id.optional(), Guid: guids });
const external = (item: { Guid: { id: string }[] }) => [...new Set(item.Guid.map((guid) => /^tvdb:\/\/([1-9]\d*)$/.exec(guid.id)?.[1]).filter((id): id is string => !!id))];

/** GET-only, fresh global history/playback. Never treats aired ordinals or owner counters as episode identity/history. */
export function createTvRetentionPlexReader(options: {
  fetcher?: (input: Parameters<typeof fetch>[0], init?: RequestInit) => Promise<Response>;
  timeoutMs?: number; now?: () => number;
} = {}) {
  const fetcher = options.fetcher ?? fetch, now = options.now ?? Date.now;
  return async (input: TvRetentionWatchInput): Promise<TvRetentionWatch> => {
    const unknown = () => unknownTvRetentionWatch(input);
    const deadline = performance.now() + Math.max(1, Math.min(20_000, options.timeoutMs ?? 20_000));
    let requests = 0, totalBytes = 0;
    try {
      if (!input.episodes.length || input.episodes.length > TV_RETENTION_MAX_EPISODES ||
          new Set(input.episodes.map((episode) => episode.providerId)).size !== input.episodes.length ||
          input.episodes.some((episode) => !/^[1-9]\d*$/.test(episode.providerId)) ||
          !Number.isSafeInteger(input.tvdbId) || input.tvdbId < 1 || !input.plexToken.trim() || !/^[1-9]\d*$/.test(input.plexSectionId)) return unknown();
      const base = new URL(input.plexUrl.replace(/\/+$/, "") + "/");
      if (!["http:", "https:"].includes(base.protocol) || base.username || base.password || base.search || base.hash) return unknown();
      async function get(route: string, query: Record<string, string> = {}) {
        const milliseconds = Math.min(5000, deadline - performance.now());
        if (++requests > 160 || milliseconds <= 0) throw new Error("Plex audit budget exhausted");
        const url = new URL(route, base);
        for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
        const controller = new AbortController();
        let cancelBody: (() => void) | undefined, timer: ReturnType<typeof setTimeout> | undefined;
        const expired = new Promise<never>((_, reject) => { timer = setTimeout(() => {
          controller.abort(); cancelBody?.(); reject(new Error("Plex audit timed out"));
        }, milliseconds); });
        const request = async () => {
          const response = await fetcher(url, { method: "GET", redirect: "error", signal: controller.signal,
            headers: { Accept: "application/json", "X-Plex-Token": input.plexToken, "X-Plex-Product": "Podible whole-season retention audit" } });
          if (!response.ok || !response.body || controller.signal.aborted || Number(response.headers.get("content-length")) > 2 * 1024 ** 2) {
            void response.body?.cancel().catch(() => {}); throw new Error("Plex audit unavailable");
          }
          const reader = response.body.getReader();
          cancelBody = () => { void reader.cancel().catch(() => {}); };
          const chunks: Uint8Array[] = []; let bytes = 0;
          try {
            for (;;) {
              const part = await reader.read(); if (part.done) break;
              bytes += part.value.byteLength; totalBytes += part.value.byteLength;
              if (bytes > 2 * 1024 ** 2 || totalBytes > 8 * 1024 ** 2) { void reader.cancel().catch(() => {}); throw new Error("Plex audit size budget"); }
              chunks.push(part.value);
            }
            return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
          } finally { reader.releaseLock(); }
        };
        try { return await Promise.race([request(), expired]); }
        finally { if (timer !== undefined) clearTimeout(timer); }
      }
      async function page<T>(route: string, query: Record<string, string>, start: number, schema: z.ZodType<T>) {
        const value = z.object({ MediaContainer: z.object({ size: count, offset: count, totalSize: count,
          Metadata: z.array(schema).max(200).default([]) }) }).parse(await get(route, { ...query,
            "X-Plex-Container-Start": String(start), "X-Plex-Container-Size": "200" })).MediaContainer;
        if (value.offset !== start || value.size !== value.Metadata.length || value.totalSize > 2000 ||
            value.size !== Math.min(200, Math.max(0, value.totalSize - start))) throw new Error("Incomplete Plex page");
        return value;
      }
      async function all<T>(route: string, query: Record<string, string>, schema: z.ZodType<T>, key: (item: T) => string) {
        const first = await page(route, query, 0, schema), values = [...first.Metadata];
        for (let start = 200; start < first.totalSize; start += 200) {
          const next = await page(route, query, start, schema);
          if (next.totalSize !== first.totalSize) throw new Error("Plex membership changed");
          values.push(...next.Metadata);
        }
        const again = await page(route, query, 0, schema);
        if (again.totalSize !== first.totalSize || JSON.stringify(again.Metadata) !== JSON.stringify(first.Metadata) ||
            new Set(values.map(key)).size !== values.length) throw new Error("Plex observation changed");
        return values;
      }
      // Same conservative authority witness as movie retention: one/zero accounts is not admin/global proof.
      const witness = z.object({ MediaContainer: z.object({ size: count, offset: count, totalSize: count,
        Metadata: z.array(historySchema).max(200) }) }).parse(await get("status/sessions/history/all", {
        "X-Plex-Container-Start": "0", "X-Plex-Container-Size": "200", sort: "viewedAt:desc,accountID",
      })).MediaContainer;
      if (witness.offset !== 0 || witness.size !== witness.Metadata.length || witness.size !== Math.min(200, witness.totalSize) ||
          new Set(witness.Metadata.map((row) => row.accountID)).size < 2) return unknown();
      const sections = z.object({ MediaContainer: z.object({ size: count,
        Directory: z.array(z.object({ key: id, type: z.string() })).max(100) }) }).parse(await get("library/sections")).MediaContainer;
      if (sections.size !== sections.Directory.length || !sections.Directory.some((section) => section.key === input.plexSectionId && section.type === "show")) return unknown();
      const catalog = await all("library/all", { type: "2", includeGuids: "1" }, showSchema, (show) => show.ratingKey);
      const exact = catalog.filter((show) => external(show).includes(String(input.tvdbId)));
      const canonical = new Set(exact.map((show) => show.guid).filter((guid) => guid?.startsWith("plex://show/")));
      const copies = catalog.filter((show) => external(show).includes(String(input.tvdbId)) || (show.guid && canonical.has(show.guid)));
      if (!copies.length || copies.length > 10 || copies.some((show) => external(show).some((id) => id !== String(input.tvdbId)))) return unknown();
      const members = new Map(input.episodes.map((episode) => [episode.providerId, episode]));
      const history = new Map<number, { count: number; last: number | null }>();
      const leaves = new Map<string, string>();
      const historyKeys = new Set<string>();
      for (const copy of copies) {
        const episodes = await all(`library/metadata/${copy.ratingKey}/allLeaves`, { includeGuids: "1" }, episodeSchema, (episode) => episode.ratingKey);
        for (const episode of episodes) {
          if (episode.grandparentRatingKey !== copy.ratingKey || episode.librarySectionID !== copy.librarySectionID) return unknown();
          const keys = external(episode), member = keys.length === 1 ? members.get(keys[0]!) : undefined;
          if (keys.some((key) => members.has(key)) && keys.length !== 1) return unknown();
          if (keys.length === 1) leaves.set(episode.ratingKey, keys[0]!);
          if (!member) continue;
          const events = await all("status/sessions/history/all", { metadataItemID: episode.ratingKey, sort: "viewedAt:desc,accountID" }, historySchema, (event) => event.historyKey);
          const result = history.get(member.id) ?? { count: 0, last: null };
          for (const event of events) {
            if (event.ratingKey !== episode.ratingKey || event.librarySectionID !== copy.librarySectionID || event.type !== "episode" ||
                event.viewedAt * 1000 > now() + 300_000 || !Number.isFinite(new Date(event.viewedAt * 1000).getTime()) || historyKeys.has(event.historyKey)) return unknown();
            historyKeys.add(event.historyKey); if (historyKeys.size > 5000) return unknown();
            result.count++; result.last = Math.max(result.last ?? 0, event.viewedAt * 1000);
          }
          history.set(member.id, result);
        }
      }
      if (input.episodes.some((episode) => !history.has(episode.id))) return unknown();
      // This is always fetched after history, never cached, including on repeated audits.
      const sessions = z.object({ MediaContainer: z.object({ size: count, Metadata: z.array(sessionSchema).max(1000).default([]) }) })
        .parse(await get("status/sessions")).MediaContainer;
      if (sessions.size !== sessions.Metadata.length) return unknown();
      let activePlayback: TvRetentionWatch["activePlayback"] = "none";
      for (const session of sessions.Metadata) {
        if (session.type !== "episode") continue;
        const keys = external(session);
        if (keys.some((key) => members.has(key)) || (session.ratingKey && members.has(leaves.get(session.ratingKey) ?? ""))) { activePlayback = "active"; break; }
        if (!session.ratingKey || !leaves.has(session.ratingKey)) activePlayback = "unknown";
      }
      const observations = input.episodes.map((episode) => {
        const item = history.get(episode.id)!;
        return { episodeId: episode.id, episodeKey: episode.key, state: item.count ? "watched" as const : "unwatched" as const,
          playCount: item.count, lastViewedAt: item.last === null ? null : new Date(item.last).toISOString() };
      });
      return { state: activePlayback !== "none" ? "unknown" : observations.every((episode) => episode.state === "watched") ? "watched" : "unwatched",
        coverage: "global-all-users", activePlayback, episodes: observations };
    } catch { return unknown(); }
  };
}
