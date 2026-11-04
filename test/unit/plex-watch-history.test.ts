import { expect, test } from "bun:test";
import { createPlexWatchHistoryReader, type PlexWatchHistoryInput } from "../../src/media/plex-watch-history";

const input: PlexWatchHistoryInput = { tmdbId: 42, imdbId: "tt4242", plexUrl: "http://plex.invalid/pms",
  plexToken: "private-plex-token", plexSectionId: "16" };
const timestamp = Date.parse("2026-11-03T00:00:00.000Z");
const movie = (ratingKey: string, section = "1", tmdb = "42", imdb = "tt4242") => ({
  ratingKey, librarySectionID: section, type: "movie", guid: `plex://movie/${tmdb}`,
  Guid: [{ id: `tmdb://${tmdb}` }, { id: `imdb://${imdb}` }], title: "Same title", viewCount: 999,
});
const event = (number: number, ratingKey = "10", section = "1", accountID = 2) => ({
  historyKey: `/status/sessions/history/${number}`, ratingKey, librarySectionID: section, type: "movie", accountID,
  viewedAt: Math.floor(timestamp / 1000) - number,
});

function fixture() {
  let clock = timestamp;
  const calls: { url: URL; headers: Headers; signal?: AbortSignal | null }[] = [];
  const catalog = [movie("10"), movie("20", "16"), movie("30", "1", "99", "tt9999")];
  const histories = new Map<string, ReturnType<typeof event>[]>([
    ["10", [event(1), event(2, "10", "1", 3)]], ["20", [event(3, "20", "16", 1)]], ["30", []],
  ]);
  let global = [event(100, "999", "2", 1), event(101, "999", "2", 2)];
  let sessions: Record<string, unknown>[] = [];
  let override: ((url: URL, headers: Headers) => Response | undefined | Promise<Response | undefined>) | undefined;
  const fetcher = (async (raw, init) => {
    const url = new URL(raw instanceof Request ? raw.url : String(raw)), headers = new Headers(init?.headers);
    calls.push({ url, headers, signal: init?.signal });
    expect(init?.method).toBe("GET"); expect(init?.redirect).toBe("error");
    expect(url.href).not.toContain(input.plexToken);
    expect(url.searchParams.has("X-Plex-Token")).toBe(false);
    const overridden = await override?.(url, headers);
    if (overridden) return overridden;
    if (url.pathname === "/pms/library/sections") return Response.json({ MediaContainer: { size: 2,
      Directory: [{ key: "1", type: "movie" }, { key: "16", type: "movie" }] } });
    if (url.pathname === "/pms/status/sessions") return Response.json({ MediaContainer: { size: sessions.length, Metadata: sessions } });
    const rows = url.pathname === "/pms/library/all" ? catalog : url.pathname === "/pms/status/sessions/history/all"
      ? url.searchParams.has("metadataItemID") ? histories.get(url.searchParams.get("metadataItemID")!) ?? [] : global
      : (() => { throw new Error("Unexpected outbound route"); })();
    const start = Number(url.searchParams.get("X-Plex-Container-Start"));
    const size = Number(url.searchParams.get("X-Plex-Container-Size"));
    const page = rows.slice(start, start + size);
    return Response.json({ MediaContainer: { offset: start, size: page.length, totalSize: rows.length, Metadata: page } });
  }) as typeof fetch;
  const read = createPlexWatchHistoryReader({ fetcher, now: () => clock });
  return { calls, catalog, histories, fetcher, read,
    setGlobal: (next: typeof global) => { global = next; },
    setSessions: (next: typeof sessions) => { sessions = next; },
    setOverride: (next: typeof override) => { override = next; },
    advance: (milliseconds: number) => { clock += milliseconds; },
  };
}
const isHistory = (url: URL, id?: string) => url.pathname.endsWith("/status/sessions/history/all") &&
  (id === undefined || url.searchParams.get("metadataItemID") === id);
const expectUnknown = (result: Awaited<ReturnType<ReturnType<typeof createPlexWatchHistoryReader>>>) => {
  expect(result.state).toBe("unknown"); expect(result.playCount).toBeNull(); expect(result.lastViewedAt).toBeNull();
  expect(result.activePlayback).toBe("unknown");
  expect(JSON.stringify(result)).not.toContain("private-plex-token");
  expect(JSON.stringify(result)).not.toContain("private.invalid");
};

test("complete global history joins stable movie identity across main and isolated copies and multiple users", async () => {
  const f = fixture();
  f.catalog[1]!.Guid = []; // Same canonical Plex GUID still joins an alternate copy.
  const result = await f.read(input);
  expect(result).toEqual({ state: "watched", coverage: "global-all-users", playCount: 3,
    lastViewedAt: new Date(timestamp - 1000).toISOString(), observedAt: new Date(timestamp).toISOString(), activePlayback: "none" });
  expect(f.calls.every((call) => call.headers.get("X-Plex-Token") === input.plexToken)).toBe(true);
  expect(f.calls.filter((call) => isHistory(call.url, "10"))).toHaveLength(2);
  expect(f.calls.filter((call) => isHistory(call.url, "20"))).toHaveLength(2);
  for (const { url } of f.calls.filter((call) => isHistory(call.url))) {
    expect(url.searchParams.has("accountID")).toBe(false);
    expect(url.searchParams.has("librarySectionID")).toBe(false);
    expect(url.searchParams.has("viewedAt")).toBe(false);
  }
});

test("verified complete zero history is unwatched; owner viewCount is never a play count", async () => {
  const f = fixture(); f.histories.set("10", []); f.histories.set("20", []);
  expect(await f.read(input)).toMatchObject({ state: "unwatched", coverage: "global-all-users", playCount: 0,
    lastViewedAt: null, activePlayback: "none" });
});

test("single-account, empty or malformed global history cannot prove all-user visibility", async () => {
  for (const global of [[event(1)], []]) {
    const f = fixture(); f.setGlobal(global);
    const result = await f.read(input); expectUnknown(result); expect(result.coverage).toBe("unknown");
    expect(f.calls).toHaveLength(1);
  }
  const f = fixture();
  f.setOverride((url) => isHistory(url) ? Response.json({ MediaContainer: { offset: 0, size: 1, totalSize: 1,
    Metadata: [{ ...event(1), accountID: undefined }] } }) : undefined);
  expectUnknown(await f.read(input));
});

test("GUID mismatch, conflicting provider identities, or absent configured movie section stay unknown", async () => {
  for (const conflict of [false, true]) {
    const f = fixture();
    f.catalog.splice(0, 2, movie("10", "1", conflict ? "42" : "420", "tt9999"));
    expectUnknown(await f.read(input));
    expect(f.calls.some((call) => call.url.searchParams.has("metadataItemID"))).toBe(false);
  }
  const f = fixture(); expectUnknown(await f.read({ ...input, plexSectionId: "999" }));
});

test("all pages are consumed and validated before a movie play count is known", async () => {
  const f = fixture(); f.histories.set("10", Array.from({ length: 201 }, (_, i) => event(i + 1)));
  f.histories.set("20", []);
  expect(await f.read(input)).toMatchObject({ state: "watched", coverage: "global-all-users", playCount: 201 });
  expect(f.calls.some((call) => isHistory(call.url, "10") && call.url.searchParams.get("X-Plex-Container-Start") === "200")).toBe(true);
});

test("partial, repeated, changing, unscoped or malformed history pages never become a partial count or zero", async () => {
  for (const failure of ["partial", "missing-total", "wrong-movie", "repeated", "changed-total", "missing-account"] as const) {
    const f = fixture(); let requests = 0;
    f.setOverride((url) => {
      if (!isHistory(url, "10")) return undefined;
      requests++;
      if (failure === "partial") return Response.json({ MediaContainer: { size: 1, offset: 0, totalSize: 201, Metadata: [event(1)] } });
      if (failure === "missing-total") return Response.json({ MediaContainer: { size: 0, offset: 0 } });
      if (failure === "wrong-movie") return Response.json({ MediaContainer: { size: 1, offset: 0, totalSize: 1, Metadata: [event(1, "30")] } });
      if (failure === "missing-account") return Response.json({ MediaContainer: { size: 1, offset: 0, totalSize: 1, Metadata: [{ ...event(1), accountID: undefined }] } });
      if (failure === "changed-total") return requests > 1 ? Response.json({ MediaContainer: { size: 0, offset: 0, totalSize: 0 } }) : undefined;
      const start = Number(url.searchParams.get("X-Plex-Container-Start"));
      const rows = start === 0 ? Array.from({ length: 200 }, (_, i) => event(i + 1)) : [event(1)];
      return Response.json({ MediaContainer: { size: rows.length, offset: start, totalSize: 201, Metadata: rows } });
    });
    expectUnknown(await f.read(input));
  }
});

test("HTTP auth/errors/redirects are sanitized, bounded and cooled down rather than retried or served stale", async () => {
  for (const status of [401, 403, 302, 500]) {
    const f = fixture();
    f.setOverride(() => new Response("https://private.invalid/?token=private-plex-token", { status, headers: { Location: "https://private.invalid/" } }));
    expectUnknown(await f.read(input)); expectUnknown(await f.read(input)); expect(f.calls).toHaveLength(1);
  }
  const f = fixture(); f.setOverride(() => new Response(null, { status: 429, headers: { "Retry-After": "60" } }));
  expectUnknown(await f.read(input)); f.advance(59_000); expectUnknown(await f.read(input)); expect(f.calls).toHaveLength(1);
  f.advance(2000); expectUnknown(await f.read(input)); expect(f.calls).toHaveLength(2);
  const bad = fixture();
  expectUnknown(await bad.read({ ...input, plexUrl: "https://user:secret@private.invalid" }));
  expect(bad.calls).toHaveLength(0);
});

test("metadata/history cache coalesces reads but current playback is always freshly observed", async () => {
  const f = fixture();
  const [first, second] = await Promise.all([f.read(input), f.read(input)]);
  expect(first.state).toBe("watched"); first.playCount = 999; expect(second.playCount).toBe(3);
  const oldCalls = f.calls.length;
  f.setSessions([{ type: "movie", ratingKey: "20", guid: "plex://movie/42" }]);
  expect(await f.read(input)).toMatchObject({ state: "unknown", playCount: 3, activePlayback: "active" });
  expect(f.calls.length - oldCalls).toBe(1);
  expect(f.calls.at(-1)!.url.pathname).toBe("/pms/status/sessions");
  f.setSessions([]); f.advance(31_000);
  expect((await f.read(input)).state).toBe("watched");
  expect(f.calls.filter((call) => isHistory(call.url, "10"))).toHaveLength(4);
});

test("unknown/incomplete live playback blocks while unrelated known movies and TV do not", async () => {
  const f = fixture(); f.setSessions([{ type: "movie", ratingKey: "9999" }]);
  expect(await f.read(input)).toMatchObject({ state: "unknown", coverage: "global-all-users", activePlayback: "unknown" });
  f.setSessions([{ type: "movie", ratingKey: "30" }, { type: "episode", ratingKey: "7777" }]);
  expect(await f.read(input)).toMatchObject({ state: "watched", activePlayback: "none" });
  f.setOverride((url) => url.pathname.endsWith("/status/sessions") ? Response.json({ MediaContainer: { size: 1, Metadata: [] } }) : undefined);
  expectUnknown(await f.read(input));
});

test("credential changes never reuse another token's catalog, authority or history", async () => {
  const f = fixture(); expect((await f.read(input)).state).toBe("watched");
  f.setOverride((_url, headers) => headers.get("X-Plex-Token") === "another-private-token" ? new Response(null, { status: 403 }) : undefined);
  expectUnknown(await f.read({ ...input, plexToken: "another-private-token" }));
  expect(f.calls.at(-1)!.headers.get("X-Plex-Token")).toBe("another-private-token");
});

test("action-time readers can bypass all cached evidence, including cross-section identity and latest play time", async () => {
  const f = fixture();
  const read = createPlexWatchHistoryReader({ fetcher: f.fetcher, now: () => timestamp, cacheTtlMs: 0 });
  expect((await read(input)).playCount).toBe(3);
  f.catalog.push(movie("21", "16")); f.histories.set("21", [event(4, "21", "16", 9)]);
  expect(await read(input)).toMatchObject({ state: "watched", playCount: 4, activePlayback: "none" });
  expect(f.calls.filter((call) => call.url.pathname.endsWith("/library/sections"))).toHaveLength(2);
  expect(f.calls.filter((call) => isHistory(call.url) && !call.url.searchParams.has("metadataItemID"))).toHaveLength(2);
});

test("invalid JSON, secret-bearing transport exceptions and oversized catalogs stay sanitized unknown", async () => {
  for (const failure of ["json", "exception", "catalog"] as const) {
    const f = fixture();
    f.setOverride((url) => {
      if (failure === "exception") throw new Error("https://private.invalid/?token=private-plex-token");
      if (failure === "json") return new Response("invalid private-plex-token JSON");
      return url.pathname.endsWith("/library/all") ? Response.json({ MediaContainer: {
        size: 0, totalSize: 10_001, offset: 0, Metadata: [],
      } }) : undefined;
    });
    expectUnknown(await f.read(input));
  }
});

test("declared/streamed body budgets cancel responses without exposing upstream content", async () => {
  for (const declared of [true, false]) {
    const f = fixture(); let cancelled = false;
    f.setOverride(() => new Response(new ReadableStream({ pull(controller) { controller.enqueue(new Uint8Array(2 * 1024 ** 2 + 1)); },
      cancel() { cancelled = true; } }), { headers: declared ? { "Content-Length": String(2 * 1024 ** 2 + 1) } : {} }));
    expectUnknown(await f.read(input)); expect(cancelled).toBe(true);
  }
});

test("a stalled response body is aborted and cancelled within the observation budget", async () => {
  const f = fixture(); let cancelled = false;
  f.setOverride(() => new Response(new ReadableStream({ start() {}, cancel() { cancelled = true; } })));
  const read = createPlexWatchHistoryReader({ fetcher: f.fetcher, now: () => timestamp, timeoutMs: 10 });
  const started = performance.now(); expectUnknown(await read(input));
  expect(performance.now() - started).toBeLessThan(1000);
  expect(cancelled).toBe(true); expect(f.calls[0]!.signal?.aborted).toBe(true);
});
