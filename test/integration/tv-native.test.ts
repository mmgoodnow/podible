import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { initTRPC } from "@trpc/server";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { createTvRouter, tvRouter } from "../../src/rpc/tv-router";
import { executeMethod, type RpcMethodDefinition } from "../../src/rpc/framework";
import { RpcError, type RpcContext } from "../../src/rpc/shared";
import { TmdbTvCatalog, TvCatalogError } from "../../src/tv/catalog";
import { nativeTvRequestScope, refreshNativeTvRequests, requestTvSeasons } from "../../src/tv/service";
import { syncTvRequests, type TvSyncServices } from "../../src/tv/sync";
import type { WorkerContext } from "../../src/worker/context";
import { runWorker } from "../../src/worker";
import { fixtureTvCatalog } from "../fixtures/tv-catalog";

async function setup() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-tv-"));
  const file = path.join(root, "app.sqlite");
  const db = new Database(file); runMigrations(db); finalizeJobJournalMigration(db); db.exec("PRAGMA foreign_keys=ON");
  const repo = new BooksRepo(db); repo.bindJobJournalReader({ read: () => new Map() }); repo.ensureSettings();
  for (const username of ["one", "two", "admin"]) {
    const user = repo.upsertUser({ provider: "local", providerUserId: username, username, isAdmin: username === "admin" });
    repo.createSession(user.id, hashSessionToken(username), null, "browser");
  }
  const fixture = fixtureTvCatalog();
  const router = createTvRouter({ catalog: fixture.catalog });
  function ctx(token = "one"): RpcContext {
    return { repo, startTime: Date.now(), request: new Request("http://localhost/rpc"), session: repo.getSessionByTokenHash(hashSessionToken(token)) };
  }
  async function call<K extends keyof typeof router.routes>(method: K, input: Record<string, unknown> = {}, token = "one") {
    return executeMethod(router.routes[method] as RpcMethodDefinition, ctx(token), input);
  }
  function count(table: string) { return (db.query(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n; }
  const outbox = () => db.query("SELECT * FROM job_commands WHERE kind='enqueue'").all();
  async function close() { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  return { root, file, db, repo, fixture, router, ctx, call, count, outbox, close };
}

test("USER TV catalog/request methods authenticate before provider access and leave administrative audit methods unchanged", async () => {
  const s = await setup();
  try {
    for (const [method, input] of [["search", { query: "show" }], ["details", { tmdbId: 70 }],
      ["request", { tmdbId: 70, seasonNumbers: [1] }], ["myRequests", {}], ["myRequest", { requestId: 1 }], ["withdraw", { requestId: 1 }]] as const) {
      await expect(s.call(method, input, "invalid")).rejects.toMatchObject({ code: -32001 });
      expect(s.router.routes[method].auth).toBe("user");
    }
    expect(s.fixture.calls).toEqual({ search: 0, details: 0, resolve: 0 });
    expect(s.count("tv_requests")).toBe(0);
    for (const method of ["status", "list", "get", "sync", "backfill", "retry"] as const) {
      expect(tvRouter.routes[method].auth).toBe("admin");
      await expect(s.call(method, {}, "one")).rejects.toMatchObject({ code: -32003 });
    }
    expect(await s.call("search", { query: "work" })).toMatchObject({ configured: true, unavailableReason: null, results: [{ tmdbId: 70 }] });
    expect(await s.call("details", { tmdbId: 70 })).toMatchObject({ series: { numberingScheme: "tvdb:aired", seasons: [
      { seasonNumber: 0, requestable: true }, { seasonNumber: 1, requestable: true }, { seasonNumber: 2, requestable: true },
    ] } });
  } finally { await s.close(); }
});

test("strict SEASON selection has no episode-level surface and unsupported mappings roll back all intent", async () => {
  const s = await setup();
  try {
    for (const input of [{ tmdbId: 70, seasonNumbers: [] }, { tmdbId: 70, seasonNumbers: [1, 1] },
      { tmdbId: 70, seasonNumbers: [-1] }, { tmdbId: 70, seasonNumbers: [1.5] }, { tmdbId: 70, seasonNumbers: ["1"] },
      { tmdbId: 70, seasonNumbers: [1], episodeIds: [7001] }, { tmdbId: 70, seasonNumbers: [1], episodeKeys: ["tvdb:7001"] },
      { tmdbId: 70, seasonNumbers: [1], episodeLimit: 1 }, { tmdbId: 70, seasonNumbers: [1], url: "https://wrong.invalid" }])
      await expect(s.call("request", input)).rejects.toMatchObject({ code: -32602 });
    expect(s.fixture.calls.resolve).toBe(0);
    await expect(s.call("request", { tmdbId: 70, seasonNumbers: [1, 99] })).rejects.toMatchObject({ code: -32602 });
    for (const mutate of [
      (entry: ReturnType<typeof s.fixture.entry>) => { entry.episodes[1]!.key = "ephemeral:tvdb:700:1:1"; },
      (entry: ReturnType<typeof s.fixture.entry>) => { entry.tmdbId = 71; },
      (entry: ReturnType<typeof s.fixture.entry>) => { entry.episodes[1]!.numbering = []; },
    ]) {
      const entry = s.fixture.entry(); mutate(entry); s.fixture.setEntry(entry);
      await expect(s.call("request", { tmdbId: 70, seasonNumbers: [1] })).rejects.toMatchObject({ code: -32602 });
    }
    for (const table of ["tv_entries", "tv_requests", "tv_request_episodes", "tv_targets", "tv_acquisitions", "retention_resources"])
      expect(s.count(table)).toBe(0);
    expect(s.outbox()).toEqual([]);
  } finally { await s.close(); }
});

test("native requests select all known season episodes including future/undated, exclude other seasons, and deduplicate concurrent submissions", async () => {
  const s = await setup();
  try {
    const [first, second] = await Promise.all([s.call("request", { tmdbId: 70, seasonNumbers: [1] }), s.call("request", { tmdbId: 70, seasonNumbers: [1] })]) as any[];
    const request = first.request;
    expect(second.request.requestId).toBe(request.requestId);
    expect(request.seasonNumbers).toEqual([1]); expect(request.requestedEpisodes).toBe(3); expect(request.futureEpisodes).toBe(2);
    expect(s.repo.tv.requestedEpisodes(request.requestId).map((ep) => ep.key)).toEqual(["tvdb:7001", "tvdb:7002", "tvdb:7003"]);
    expect(s.count("tv_requests")).toBe(1); expect(s.count("tv_acquisitions")).toBe(1); expect(s.outbox()).toHaveLength(1);
    expect(s.repo.tv.origins()).toEqual([]);
    expect(s.repo.retention.get({ namespace: "tv-target", id: String(request.targetId) })?.desiredPresence).toBe("present");
    const other = await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any;
    expect(other.request.requestId).not.toBe(request.requestId);
    expect(other.request.targetId).toBe(request.targetId); expect(other.request.requestedEpisodes).toBe(1);
    expect(other.request.acquisitions).toEqual([]); expect(s.outbox()).toHaveLength(1);
    const ordered = await s.call("request", { tmdbId: 70, seasonNumbers: [2, 0] }) as any;
    const reordered = await s.call("request", { tmdbId: 70, seasonNumbers: [0, 2] }) as any;
    expect(ordered.request.requestId).toBe(reordered.request.requestId);
    expect(ordered.request.seasonNumbers).toEqual([0, 2]);
    expect(s.repo.tv.requestedEpisodes(ordered.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:7000", "tvdb:7004"]);
    expect(s.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
  } finally { await s.close(); }
});

test("native outbox failure atomically rolls back catalog identity, requests, retention and jobs", async () => {
  const s = await setup();
  try {
    s.db.exec(`CREATE TRIGGER fail_native_tv_outbox BEFORE INSERT ON job_commands
      WHEN NEW.kind='enqueue' AND (SELECT type FROM jobs WHERE id=NEW.job_id)='tv_acquire'
      BEGIN SELECT RAISE(ABORT,'injected native TV outbox failure'); END`);
    await expect(s.call("request", { tmdbId: 70, seasonNumbers: [1] })).rejects.toThrow("outbox failure");
    for (const table of ["tv_entries", "tv_episodes", "tv_entry_episodes", "tv_requests", "tv_request_episodes", "tv_targets", "tv_acquisitions", "retention_resources", "jobs", "job_commands"])
      expect(s.count(table)).toBe(0);
    s.db.exec("DROP TRIGGER fail_native_tv_outbox");
    await s.call("request", { tmdbId: 70, seasonNumbers: [1] });
    expect(s.outbox()).toHaveLength(1);
  } finally { await s.close(); }
});

test("own request RPC hides other users' scope and diagnostics; withdrawal preserves shared demand and durable cancellation", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [1] }) as any).request;
    const second = (await s.call("request", { tmdbId: 70, seasonNumbers: [0] }, "two") as any).request;
    expect(first.targetId).toBe(second.targetId);
    const acquisition = s.repo.tv.acquisitions(first.targetId).find((a) => a.request_id === first.requestId)!;
    s.repo.tv.decision(acquisition.id, { url: "https://private.invalid/?token=secret", otherRequest: second.requestId });
    s.repo.tv.event(first.targetId, acquisition.id, "private-native-event", "blocked", "Private diagnostic token=secret");
    const handler = createPodibleFetchHandler(s.repo, Date.now(), null);
    async function rpc(method: string, params = {}, token = "one") {
      return (await (await handler(new Request("http://localhost/rpc", { method: "POST", headers: {
        cookie: `podible_session=${token}`, "Content-Type": "application/json",
      }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: `tv.${method}`, params }) }))).json()) as any;
    }
    expect((await rpc("myRequests", {}, "invalid")).error.code).toBe(-32001);
    const listed = (await rpc("myRequests")).result.requests;
    expect(listed).toHaveLength(1); expect(listed[0].requestId).toBe(first.requestId);
    expect(listed[0].acquisitions).toHaveLength(1); expect(listed[0].acquisitions[0].decision).toBeNull();
    expect(JSON.stringify(listed)).not.toContain("secret"); expect(JSON.stringify(listed)).not.toContain("private.invalid");
    expect((await rpc("myRequest", { requestId: second.requestId })).error.code).toBe(-32003);
    expect((await rpc("withdraw", { requestId: second.requestId })).error.code).toBe(-32003);
    expect((await rpc("withdraw", { requestId: second.requestId }, "admin")).error.code).toBe(-32003);
    expect((await rpc("myRequest", { requestId: 99999 })).error.code).toBe(-32003);
    expect((await rpc("withdraw", { requestId: first.requestId })).result.request.withdrawn).toBe(true);
    expect(s.repo.isJobCancellationRequested(first.acquisitions[0].jobId)).toBe(true);
    expect(s.db.query("SELECT * FROM job_commands WHERE kind='cancel' AND job_id=?").all(first.acquisitions[0].jobId)).toHaveLength(1);
    expect(s.repo.getJob(second.acquisitions[0].jobId)!.status).toBe("queued");
    expect(s.repo.tv.activeRequestCount(first.targetId)).toBe(1);
    expect(s.count("tv_episodes")).toBe(5);
    expect(s.repo.retention.get({ namespace: "tv-target", id: String(first.targetId) })?.desiredPresence).toBe("present");
    const commands = s.count("job_commands");
    await rpc("withdraw", { requestId: first.requestId }); expect(s.count("job_commands")).toBe(commands);
    const restored = (await s.call("request", { tmdbId: 70, seasonNumbers: [1] }) as any).request;
    expect(restored.requestId).toBe(first.requestId); expect(restored.withdrawn).toBe(false);
    expect(restored.acquisitions[0].jobId).not.toBe(first.acquisitions[0].jobId);
  } finally { await s.close(); }
});

test("native future-only season intent survives reopening, metadata expansion, and air-date changes without premature acquisition", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const before = s.repo.tv.requestedEpisodeIds(first.requestId);
    expect(s.outbox()).toEqual([]);
    const entry = s.fixture.entry();
    entry.episodes.push({ key: "tvdb:7005", title: "Announced later", airDate: null, runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 2, episode: 2, absolute: null, source: "verified-catalog" }] });
    entry.episodes[4]!.airDate = "2020-01-01";
    s.fixture.setEntry(entry);
    const reopenedDb = new Database(s.file), reopened = new BooksRepo(reopenedDb);
    try {
      expect(nativeTvRequestScope(reopened.tv.requestById(first.requestId)!)).toEqual({ tmdbId: 70, seasonNumbers: [2] });
      expect(await refreshNativeTvRequests(reopened, s.fixture.catalog)).toEqual({ refreshed: 1, errors: [] });
      expect(reopened.tv.requestedEpisodes(first.requestId).map((ep) => ep.key)).toEqual(["tvdb:7004", "tvdb:7005"]);
      expect(reopened.tv.requestedEpisodeIds(first.requestId)[0]).toBe(before[0]);
      expect(reopened.tv.requestedEpisodes(first.requestId)[0]!.airDate).toBe("2020-01-01");
      expect(s.outbox()).toEqual([]); // Feed arrival, not metadata refresh, initiates acquisition.
      expect((await refreshNativeTvRequests(reopened, s.fixture.catalog)).refreshed).toBe(1);
      expect(reopened.tv.requests()).toHaveLength(1);
    } finally { reopenedDb.close(); }
  } finally { await s.close(); }
});

test("metadata refresh preserves withdrawal and eviction fences and rejects lost or conflicting work/episode identities", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const key = { namespace: "tv-target", id: String(first.targetId) };
    s.repo.retention.evict(key, { expectedRevision: s.repo.retention.get(key)!.revision, reason: "Explicit eviction" });
    const evicted = s.repo.retention.get(key)!;
    expect((await refreshNativeTvRequests(s.repo, s.fixture.catalog)).refreshed).toBe(1);
    expect(s.repo.retention.get(key)).toEqual(evicted); expect(s.outbox()).toEqual([]);
    const entry = s.fixture.entry(); entry.key = "anilist:999"; s.fixture.setEntry(entry);
    expect((await refreshNativeTvRequests(s.repo, s.fixture.catalog)).errors[0]!.message).toContain("work or episode");
    expect(s.repo.tv.entries()).toHaveLength(1);
    entry.key = "tvdb:700"; entry.episodes[4]!.key = "tvdb:99999"; s.fixture.setEntry(entry);
    expect((await refreshNativeTvRequests(s.repo, s.fixture.catalog)).errors).toHaveLength(1);
    expect(s.repo.tv.requestedEpisodes(first.requestId)[0]!.key).toBe("tvdb:7004");
    await s.call("withdraw", { requestId: first.requestId });
    const calls = s.fixture.calls.resolve;
    expect(await refreshNativeTvRequests(s.repo, s.fixture.catalog)).toEqual({ refreshed: 0, errors: [] });
    expect(s.fixture.calls.resolve).toBe(calls);
    expect(s.repo.tv.requestById(first.requestId)!.withdrawn_at).not.toBeNull();
  } finally { await s.close(); }
});

test("typed tRPC convention consumes exactly the same USER schemas/handlers and never infers anime work structure", async () => {
  const s = await setup();
  try {
    const t = initTRPC.context<RpcContext>().create(), routes = s.router.routes;
    const router = t.router({ tv: t.router({
      details: t.procedure.input(routes.details.paramsSchema).output(routes.details.resultSchema).query(({ ctx, input }) => executeMethod(routes.details, ctx, input)),
      request: t.procedure.input(routes.request.paramsSchema).output(routes.request.resultSchema).mutation(({ ctx, input }) => executeMethod(routes.request, ctx, input)),
      myRequest: t.procedure.input(routes.myRequest.paramsSchema).output(routes.myRequest.resultSchema).query(({ ctx, input }) => executeMethod(routes.myRequest, ctx, input)),
    }) });
    const client = router.createCaller(s.ctx());
    const entry = s.fixture.entry(); entry.key = "anilist:800"; entry.anime = false; s.fixture.setEntry(entry);
    const requested = await client.tv.request({ tmdbId: 70, seasonNumbers: [2] });
    expect(requested.request.entry.key).toBe("anilist:800");
    expect((await client.tv.myRequest({ requestId: requested.request.requestId })).request.seasonNumbers).toEqual([2]);
    expect((await client.tv.details({ tmdbId: 70 })).series!.seasons).toHaveLength(3);
    await expect(router.createCaller(s.ctx("two")).tv.myRequest({ requestId: requested.request.requestId })).rejects.toThrow("TV request not found");
    await expect(router.createCaller(s.ctx("invalid")).tv.request({ tmdbId: 70, seasonNumbers: [1] })).rejects.toThrow("Unauthorized");
    expect(s.repo.tv.origins()).toEqual([]);
  } finally { await s.close(); }
});

test("unconfigured and temporarily unavailable catalogs are not disguised as successful empty discoveries", async () => {
  const s = await setup();
  try {
    expect(await executeMethod(tvRouter.routes.search, s.ctx(), { query: "show" })).toMatchObject({ configured: false, unavailableReason: expect.stringContaining("movies.tmdbAccessToken"), results: [] });
    const router = createTvRouter({ catalog: { ...s.fixture.catalog,
      async search() { throw new TvCatalogError("TMDB unavailable; retry later"); },
      async details() { throw new TvCatalogError("TMDB unavailable; retry later"); },
      async resolve() { throw new TvCatalogError("Canonical mapping unavailable; check Sonarr", "unsupported"); },
    } });
    expect(await executeMethod(router.routes.search, s.ctx(), { query: "show" })).toMatchObject({ configured: true, unavailableReason: "TMDB unavailable; retry later", results: [] });
    expect(await executeMethod(router.routes.details, s.ctx(), { tmdbId: 70 })).toMatchObject({ configured: true, unavailableReason: "TMDB unavailable; retry later", series: null });
    await expect(executeMethod(router.routes.request, s.ctx(), { tmdbId: 70, seasonNumbers: [1] })).rejects.toBeInstanceOf(RpcError);
    expect(s.count("tv_requests")).toBe(0);
  } finally { await s.close(); }
});

test("native season scope is complete beyond fifty episodes and catalog reads occur before the request transaction", async () => {
  const s = await setup();
  try {
    const entry = s.fixture.entry();
    entry.episodes = Array.from({ length: 64 }, (_, index) => ({ key: `tvdb:${9000 + index}`, title: "Episode", runtimeMinutes: 24,
      airDate: index < 60 ? "2020-01-01" : null,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: index + 1, absolute: null, source: "verified-catalog" }],
    }));
    s.fixture.setEntry(entry);
    const result = await requestTvSeasons(s.repo, { ...s.fixture.catalog, async resolve(tmdbId) {
      expect(s.db.inTransaction).toBe(false); return s.fixture.catalog.resolve(tmdbId);
    } }, s.ctx().session!.user_id, { tmdbId: 70, seasonNumbers: [1] });
    expect(s.repo.tv.requestedEpisodes(result.requestId)).toHaveLength(64);
    expect(s.outbox()).toHaveLength(1);
  } finally { await s.close(); }
});

test("a new show not added to Sonarr is requestable via actual SkyHook provider IDs/numbering without any Sonarr or Seerr mutation", async () => {
  const s = await setup();
  try {
    const calls: { url: URL; init?: RequestInit }[] = [];
    const catalog = new TmdbTvCatalog("shared-token", { url: "", apiKey: "" }, { fetcher: async (input, init) => {
      const url = new URL(String(input)); calls.push({ url, init });
      if (url.hostname === "api.themoviedb.org") return Response.json({ id: 70, name: "A Work", original_name: "A Work",
        first_air_date: "2020-01-01", external_ids: { tvdb_id: 700 }, seasons: [{ season_number: 1, name: "TMDB season", episode_count: 2 }] });
      if (url.href === "https://skyhook.sonarr.tv/v1/tvdb/shows/en/700") return Response.json({
        tvdbId: 700, tmdbId: 70, title: "A Work", episodes: [
          { tvdbShowId: 700, tvdbId: 7001, title: "Aired", seasonNumber: 2, episodeNumber: 1, airDate: "2020-01-01" },
          { tvdbShowId: 700, tvdbId: 7002, title: "Future", seasonNumber: 2, episodeNumber: 2, airDate: "2099-01-01" },
        ],
      });
      throw new Error("Unexpected provider call");
    } });
    const router = createTvRouter({ catalog });
    const details = await executeMethod(router.routes.details, s.ctx(), { tmdbId: 70 });
    expect(details.series!.seasons).toMatchObject([{ seasonNumber: 2, requestable: true }]);
    const requested = await executeMethod(router.routes.request, s.ctx(), { tmdbId: 70, seasonNumbers: [2] });
    expect(requested.request.requestedEpisodes).toBe(2); expect(requested.request.futureEpisodes).toBe(1);
    expect(s.repo.tv.requestedEpisodes(requested.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:7001", "tvdb:7002"]);
    expect(s.repo.tv.requestedEpisodes(requested.request.requestId)[0]!.numbering[0]!.source).toBe("skyhook:v1:tvdb:aired:700");
    expect(s.outbox()).toHaveLength(1); expect(s.repo.tv.origins()).toEqual([]);
    expect(calls).toHaveLength(2); expect(calls.every((call) => call.init?.method === "GET")).toBe(true);
    expect([...new Headers(calls[1]!.init?.headers)]).toEqual([]);
  } finally { await s.close(); }
});

test("normal tv_sync refreshes native season intent without Seerr and checkpoints metadata intake without acquisition", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const entry = s.fixture.entry();
    entry.episodes.push({ key: "tvdb:7005", title: "Newly announced", airDate: "2099-02-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 2, episode: 2, absolute: null, source: "verified-catalog" }] });
    s.fixture.setEntry(entry);
    const steps = new Map<string, unknown>();
    const ctx: WorkerContext = { repo: s.repo, getSettings: () => s.repo.getSettings(), onLog() {}, execution: {
      key: "native-metadata", assertActive() {}, async sleep() {},
      async run<T>(name: string, action: () => T | Promise<T>) {
        if (steps.has(name)) return steps.get(name) as T;
        const result = await action(); steps.set(name, result); return result;
      },
    } };
    const bridge = new Proxy({} as TvSyncServices, { get() { throw new Error("Seerr bridge must not be touched"); } });
    const job = s.repo.createJob({ type: "tv_sync" });
    expect(await syncTvRequests(ctx, job, bridge, s.fixture.catalog)).toBe("done");
    expect([...steps.keys()]).toEqual(["tv-native:metadata-refresh"]);
    expect(s.repo.tv.requestedEpisodes(first.requestId).map((ep) => ep.key)).toEqual(["tvdb:7004", "tvdb:7005"]);
    const resolved = s.fixture.calls.resolve;
    expect(await syncTvRequests(ctx, job, bridge, s.fixture.catalog)).toBe("done");
    expect(s.fixture.calls.resolve).toBe(resolved); expect(s.repo.tv.requests()).toHaveLength(1);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
    expect(s.repo.tv.origins()).toEqual([]);
    expect(s.db.query("SELECT * FROM app_state WHERE key LIKE 'tv-seerr-feed:%'").all()).toEqual([]);
    const settings = s.repo.getSettings();
    settings.movies.seerr.url = "https://seerr.invalid"; settings.movies.seerr.apiKey = "movie-bridge-key";
    s.repo.updateSettings(settings);
    // A movie-only Seerr configuration must not activate an unconfigured TV bridge.
    expect(await syncTvRequests({ ...ctx, execution: undefined }, job, undefined, s.fixture.catalog)).toBe("done");
    expect(s.db.query("SELECT * FROM app_state WHERE key LIKE 'tv-seerr-feed:%'").all()).toEqual([]);
  } finally { await s.close(); }
});

test("native metadata intake logs unavailable mapping and fences configuration changes before persisting episode membership", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const logs: string[] = [];
    const ctx = { repo: s.repo, getSettings: () => s.repo.getSettings(), onLog: (line: string) => { logs.push(line); } };
    const job = s.repo.createJob({ type: "tv_sync" });
    expect(await syncTvRequests(ctx, job, undefined, { ...s.fixture.catalog,
      async resolve() { throw new TvCatalogError("SkyHook unavailable; retry later"); },
    })).toBe("done");
    expect(logs[0]).toContain(`request=${first.requestId} metadata unavailable: SkyHook unavailable`);
    expect(s.repo.tv.requestedEpisodes(first.requestId)).toHaveLength(1);
    await expect(syncTvRequests(ctx, job, undefined, { ...s.fixture.catalog, async resolve(tmdbId) {
      const settings = s.repo.getSettings(); settings.movies.tmdbAccessToken = "rotated"; s.repo.updateSettings(settings);
      return s.fixture.catalog.resolve(tmdbId);
    } })).rejects.toThrow("configuration changed");
    expect(s.repo.tv.requestedEpisodes(first.requestId)).toHaveLength(1);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
  } finally { await s.close(); }
});

test("real application tRPC wiring exposes USER TV discovery and own-request reads while preserving admin audit authorization", async () => {
  const s = await setup();
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const handler = createPodibleFetchHandler(s.repo, Date.now(), null);
    const query = (name: string, input: unknown, token = "one") => handler(new Request(
      `http://localhost/api/trpc/tv.${name}?input=${encodeURIComponent(JSON.stringify(input))}`,
      { headers: { cookie: `podible_session=${token}` } },
    ));
    expect((await query("search", { query: "show" }, "invalid")).status).toBe(401);
    const unavailable = await query("search", { query: "show" });
    expect(unavailable.status).toBe(200);
    expect(await unavailable.json()).toMatchObject({ result: { data: { configured: false, unavailableReason: expect.any(String), results: [] } } });
    expect((await query("myRequest", { requestId: first.requestId })).status).toBe(200);
    expect((await query("myRequest", { requestId: first.requestId }, "two")).status).toBe(403);
    expect((await query("list", {})).status).toBe(403);
    expect((await query("list", {}, "admin")).status).toBe(200);
  } finally { await s.close(); }
});

test("normal scheduler refreshes active native seasons without Sonarr or Seerr, not through title searches or acquisition", async () => {
  const s = await setup();
  let stop = false;
  let worker: Promise<void> | undefined;
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const settings = s.repo.getSettings(); settings.movies.tmdbAccessToken = "shared-fixture-token"; s.repo.updateSettings(settings);
    const entry = s.fixture.entry();
    entry.episodes.push({ key: "tvdb:7005", title: "New episode", airDate: "2099-02-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 2, episode: 2, absolute: null, source: "verified-catalog" }] });
    s.fixture.setEntry(entry);
    worker = runWorker({ repo: s.repo, getSettings: () => s.repo.getSettings(), shouldStop: () => stop, onLog() {} }, {
      databasePath: path.join(s.root, "execution.sqlite"), retryDelayMs: 1,
      processJob: (ctx, job) => {
        expect(job.type).toBe("tv_sync");
        return syncTvRequests(ctx, job, undefined, s.fixture.catalog);
      },
    });
    const deadline = Date.now() + 5000;
    while (s.repo.listJobsByType("tv_sync")[0]?.status !== "succeeded") {
      if (Date.now() > deadline) throw new Error("Native metadata scheduling timed out");
      await Bun.sleep(20);
    }
    await Bun.sleep(220);
    expect(s.repo.listJobsByType("tv_sync")).toHaveLength(1);
    expect(s.repo.listJobsByType("tv_shadow_sync")).toEqual([]);
    expect(s.repo.listJobsByType("movie_dual_write_sync")).toEqual([]);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
    expect(s.repo.tv.requestedEpisodes(first.requestId).map((ep) => ep.key)).toEqual(["tvdb:7004", "tvdb:7005"]);
    expect(s.fixture.calls.search).toBe(0); expect(s.fixture.calls.resolve).toBe(2);
  } finally { stop = true; await worker; await s.close(); }
});

test.each(["withdrawn", "unconfigured"] as const)("normal scheduler skips %s native season demand and keeps shadow Sonarr-only", async (condition) => {
  const s = await setup();
  let stop = false;
  let worker: Promise<void> | undefined;
  try {
    const first = (await s.call("request", { tmdbId: 70, seasonNumbers: [2] }) as any).request;
    const settings = s.repo.getSettings();
    settings.tv.sonarrUrl = "https://sonarr.invalid"; settings.tv.sonarrApiKey = "fixture";
    settings.movies.tmdbAccessToken = condition === "withdrawn" ? "shared-fixture-token" : "";
    s.repo.updateSettings(settings);
    if (condition === "withdrawn") await s.call("withdraw", { requestId: first.requestId });
    worker = runWorker({ repo: s.repo, getSettings: () => s.repo.getSettings(), shouldStop: () => stop, onLog() {} }, {
      databasePath: path.join(s.root, "execution.sqlite"), retryDelayMs: 1,
      processJob: async (_ctx, job) => { expect(job.type).toBe("tv_shadow_sync"); return "done"; },
    });
    const deadline = Date.now() + 5000;
    while (s.repo.listJobsByType("tv_shadow_sync")[0]?.status !== "succeeded") {
      if (Date.now() > deadline) throw new Error("TV shadow scheduling timed out");
      await Bun.sleep(20);
    }
    await Bun.sleep(120);
    expect(s.repo.listJobsByType("tv_sync")).toEqual([]);
    expect(s.repo.listJobsByType("tv_shadow_sync")).toHaveLength(1);
    expect(s.fixture.calls.resolve).toBe(1);
  } finally { stop = true; await worker; await s.close(); }
});
