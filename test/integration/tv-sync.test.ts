import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { seerrInstance } from "../../src/movies/seerr";
import { BooksRepo } from "../../src/repo";
import { syncTvRequests, tvSeerrFeedKey, type TvSyncServices } from "../../src/tv/sync";
import type { TvCatalogEntry } from "../../src/tv/types";
import type { SeerrTvRequest } from "../../src/tv/upstream";
import type { WorkerContext } from "../../src/worker/context";
import { createJobExecutionRuntime } from "../../src/worker/execution";

function sourceRequest(id: number, overrides: Partial<SeerrTvRequest> = {}): SeerrTvRequest {
  return {
    id, type: "tv", status: 2, is4k: false,
    createdAt: new Date(Date.now() + id * 1000).toISOString(), updatedAt: new Date(Date.now() + id * 1000).toISOString(),
    media: { tmdbId: 101, tvdbId: 307375 }, requestedBy: { id: 42, plexId: 999, displayName: "Viewer" },
    seasons: [{ seasonNumber: 1, status: 2 }], ...overrides,
  };
}
function catalog(futureOnly = false): TvCatalogEntry {
  return {
    key: "tvdb:307375", tvdbId: 307375, tmdbId: 101, title: "Anime Provider Aggregate", aliases: [], year: 2016,
    anime: true, source: "sonarr:fixture:series:1", relations: [],
    episodes: [101, 102, 201, 202, 301].map((id) => ({
      key: `tvdb:${id}`, title: "Episode", airDate: futureOnly || id === 202 ? "2099-01-01" : "2016-01-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: Math.floor(id / 100), episode: id % 100, absolute: id % 100 + (Math.floor(id / 100) - 1) * 12,
        source: "tvdb:307375:aired" }],
    })),
  };
}

async function setup() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-sync-")));
  const appPath = path.join(root, "app.sqlite");
  let db = new Database(appPath);
  runMigrations(db);
  finalizeJobJournalMigration(db);
  let repo = new BooksRepo(db);
  repo.bindJobJournalReader({ read: () => new Map() });
  const settings = repo.ensureSettings();
  settings.agents.apiKey = "";
  settings.movies.seerr = { url: "https://seerr.invalid/base", apiKey: "seerr-fixture-private", pollIntervalMinutes: 1 };
  settings.movies.plexUrl = "http://plex.invalid";
  settings.movies.plexToken = "plex-fixture-private";
  settings.tv = { ...settings.tv, sonarrUrl: "https://sonarr.invalid/base", sonarrApiKey: "sonarr-fixture-private",
    managedRoot: root, plexSectionId: "9", plexViewPath: "/data/views/plex/tv" };
  repo.updateSettings(settings);
  const records = new Map([[100, sourceRequest(100)]]);
  let rawCatalog = catalog();
  const calls = { latest: 0, changes: [] as string[], requests: [] as number[], catalogs: [] as number[], enrich: 0 };
  const services: TvSyncServices = {
    source: {
      latest: async () => { calls.latest++; return structuredClone([...records.values()].sort((a, b) => b.id - a.id)[0] ?? null); },
      changes: async (since) => {
        calls.changes.push(since);
        // Deliberately return overlap, including already consumed request IDs.
        const requests = structuredClone([...records.values()]);
        return { requests, through: new Date(Math.max(Date.parse(since), ...requests.map((request) => Date.parse(request.updatedAt)))).toISOString() };
      },
      request: async (id) => { calls.requests.push(id); const request = records.get(id); if (!request) throw new Error("Unknown fixture request"); return structuredClone(request); },
    },
    catalog: { resolveSeerr: async (request) => { calls.catalogs.push(request.id); return structuredClone(rawCatalog); } },
    enrich: async (input) => {
      calls.enrich++;
      const entries = [1, 2, 3].map((season) => ({ ...input, key: `anidb:${1000 + season}`, title: "Same anime title",
        episodes: input.episodes.filter((ep) => ep.numbering.some((n) => n.scheme === "tvdb:aired" && n.season === season)),
        relations: season === 1 ? [{ key: "anidb:1002", title: "Same anime title", relation: "SEQUEL", source: "anilist:exact-ID:fixture" }] : [],
      })).filter((entry) => entry.episodes.length);
      return { entries, warnings: [], unmappedEpisodeKeys: [] };
    },
  };
  const context: WorkerContext = { get repo() { return repo; }, getSettings: () => repo.getSettings(), onLog() {}, tvSyncServices: services };
  const instance = seerrInstance(settings.movies.seerr.url), feedKey = tvSeerrFeedKey(instance);
  const sync = async (payload: Record<string, unknown> = {}) => {
    const job = repo.createJob({ type: "tv_sync", payload, maxAttempts: 1 });
    await syncTvRequests(context, job, services);
    return job;
  };
  const nativeOutbox = () => db.query(`SELECT c.id,c.job_id FROM job_commands c JOIN jobs j ON j.id=c.job_id
    WHERE c.kind='enqueue' AND j.type='tv_acquire' ORDER BY c.id`).all();
  const count = (table: string) => (db.query(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n;
  return {
    root, records, calls, services, context, instance, feedKey, sync, count, nativeOutbox,
    setCatalog: (next: TvCatalogEntry) => { rawCatalog = structuredClone(next); },
    get repo() { return repo; }, get db() { return db; },
    reopen: () => { db.close(); db = new Database(appPath); repo = new BooksRepo(db); },
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

test("TV sync activation never backfills history and future-only approved season intent is retained without acquisition", async () => {
  const s = await setup();
  try {
    await s.sync();
    const activated = s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)!;
    expect(activated.baselineId).toBe(100);
    expect(s.repo.tv.requests()).toEqual([]);
    expect(s.calls.catalogs).toEqual([]);
    s.records.set(101, sourceRequest(101, { status: 1 }));
    await s.sync();
    expect(s.repo.tv.requests()).toEqual([]);
    s.setCatalog(catalog(true));
    s.records.set(101, sourceRequest(101, { seasons: [{ seasonNumber: 2, status: 2 }, { seasonNumber: 3, status: 1 }] }));
    await s.sync();
    await s.sync();
    const origins = s.repo.tv.origins(s.instance, 101);
    expect(origins).toHaveLength(1);
    expect(s.repo.tv.entry(origins[0]!.entryId)!.key).toBe("anidb:1002");
    expect(s.repo.tv.requestedEpisodes(origins[0]!.requestId).map((ep) => ep.key)).toEqual(["tvdb:201", "tvdb:202"]);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
    expect(s.nativeOutbox()).toEqual([]);
    expect(s.calls.latest).toBe(1);
    expect(s.calls.requests).toEqual([]);
    expect(s.repo.getJsonState<{ baselineId: number }>(s.feedKey)!.baselineId).toBe(100);
  } finally { await s.close(); }
});

test("selected TVDB seasons fan out to separate anime origins with stable IDs and no overlapping acquisition repeats", async () => {
  const s = await setup();
  try {
    await s.sync();
    s.records.set(101, sourceRequest(101, { seasons: [
      { seasonNumber: 1, status: 2 }, { seasonNumber: 2, status: 2 }, { seasonNumber: 3, status: 1 },
    ] }));
    await s.sync();
    const origins = s.repo.tv.origins(s.instance, 101);
    expect(origins).toHaveLength(2);
    expect(origins.map((origin) => s.repo.tv.entry(origin.entryId)!.key)).toEqual(["anidb:1001", "anidb:1002"]);
    expect(origins.map((origin) => s.repo.tv.entry(origin.entryId)!.tvdbId)).toEqual([307375, 307375]);
    const episodeIds = origins.map((origin) => s.repo.tv.requestedEpisodeIds(origin.requestId));
    expect(origins.flatMap((origin) => s.repo.tv.requestedEpisodes(origin.requestId).map((ep) => ep.key))).toEqual(["tvdb:101", "tvdb:102", "tvdb:201", "tvdb:202"]);
    expect(origins.every((origin) => origin.externalRequestId === 101 && origin.instance === s.instance)).toBe(true);
    expect(origins[0]!.source).toMatchObject({ episodeLimit: null, request: { id: 101, media: { tvdbId: 307375 } } });
    expect(s.repo.tv.entry(origins[0]!.entryId)!.relations[0]!.source).toBe("anilist:exact-ID:fixture");
    expect(s.repo.listJobsByType("tv_acquire")).toHaveLength(2);
    expect(s.nativeOutbox()).toHaveLength(2);
    for (const origin of origins)
      for (const acquisition of s.repo.tv.acquisitions(s.repo.tv.requestById(origin.requestId)!.target_id))
        s.repo.tv.finishAcquisition(acquisition.id);
    await s.sync();
    await s.sync();
    expect(s.repo.tv.origins(s.instance, 101)).toEqual(origins);
    expect(origins.map((origin) => s.repo.tv.requestedEpisodeIds(origin.requestId))).toEqual(episodeIds);
    expect(s.repo.listJobsByType("tv_acquire")).toHaveLength(2);
    expect(s.nativeOutbox()).toHaveLength(2);
    expect(s.count("books")).toBe(0);
    expect(s.count("sessions")).toBe(0);
  } finally { await s.close(); }
});

test("Seerr principal mapping uses exact Plex ID, never a matching display name or requester privileges", async () => {
  const s = await setup();
  try {
    const owner = s.repo.upsertUser({ provider: "plex", providerUserId: "12345", username: "owner", isAdmin: true });
    await s.sync();
    s.records.set(101, sourceRequest(101, { requestedBy: { id: 10, plexId: 12345, displayName: "Not owner" } }));
    s.records.set(102, sourceRequest(102, { requestedBy: { id: 11, plexId: 88888, displayName: "owner", username: "owner" } }));
    await s.sync();
    const linked = s.repo.tv.origins(s.instance, 101)[0]!;
    const unlinked = s.repo.tv.origins(s.instance, 102)[0]!;
    expect(s.repo.tv.requestById(linked.requestId)!.user_id).toBe(owner.id);
    const fallback = s.repo.getUserById(s.repo.tv.requestById(unlinked.requestId)!.user_id)!;
    expect(fallback.id).not.toBe(owner.id);
    expect(fallback.provider).toBe("local");
    expect(fallback.provider_user_id).toBe(`seerr:${s.instance}:11`);
    expect(fallback.is_admin).toBe(0);
    expect(s.repo.getUserById(owner.id)!.is_admin).toBe(1);
    expect(s.count("sessions")).toBe(0);
  } finally { await s.close(); }
});

test("a second anime fanout outbox failure rolls back both origins, memberships, principals, jobs and retention atomically", async () => {
  const s = await setup();
  try {
    await s.sync();
    const before = s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey);
    s.records.set(101, sourceRequest(101, { seasons: [{ seasonNumber: 1, status: 2 }, { seasonNumber: 2, status: 2 }] }));
    s.db.exec(`CREATE TRIGGER fail_tv_outbox BEFORE INSERT ON job_commands
      WHEN NEW.kind='enqueue' AND (SELECT type FROM jobs WHERE id=NEW.job_id)='tv_acquire'
        AND (SELECT COUNT(*) FROM tv_acquisitions)=2
      BEGIN SELECT RAISE(ABORT,'injected second TV outbox failure'); END`);
    await expect(s.sync()).rejects.toThrow("second TV outbox failure");
    for (const table of ["tv_entries", "tv_episodes", "tv_entry_episodes", "tv_request_origins", "tv_requests", "tv_targets", "tv_acquisitions", "retention_resources", "users"])
      expect(s.count(table)).toBe(0);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
    expect(s.nativeOutbox()).toEqual([]);
    expect(s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)).toEqual(before);
    s.db.exec("DROP TRIGGER fail_tv_outbox");
    await s.sync();
    expect(s.repo.tv.origins(s.instance, 101)).toHaveLength(2);
    expect(s.nativeOutbox()).toHaveLength(2);
    expect(s.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
  } finally { await s.close(); }
});

test("real Reflow retries a failed durable TV cursor after fanout without refetching metadata or duplicating future requests", async () => {
  const s = await setup();
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  try {
    const before = { baselineId: 100, cursor: new Date().toISOString() };
    s.repo.setJsonState(s.feedKey, before);
    s.setCatalog(catalog(true));
    s.records.set(101, sourceRequest(101, { seasons: [{ seasonNumber: 1, status: 2 }, { seasonNumber: 2, status: 2 }] }));
    s.db.exec(`CREATE TRIGGER fail_tv_cursor BEFORE UPDATE ON app_state WHEN NEW.key='${s.feedKey}'
      BEGIN SELECT RAISE(ABORT,'injected TV cursor failure'); END`);
    const job = s.repo.createJob({ type: "tv_sync", maxAttempts: 1 });
    const executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    const until = async (status: "failed" | "succeeded") => {
      const deadline = Date.now() + 7000;
      while (s.repo.getJob(job.id)?.status !== status) {
        await runtime!.reconcile();
        if (Date.now() > deadline) throw new Error(`TV sync did not become ${status}: ${JSON.stringify(s.repo.getJob(job.id))}`);
        await Bun.sleep(15);
      }
    };
    await until("failed");
    expect(s.repo.getJob(job.id)!.error).toContain("TV cursor failure");
    const runId = s.repo.getJobExecution(job.id)!.run_id;
    const origins = s.repo.tv.origins(s.instance, 101);
    expect(origins).toHaveLength(2);
    expect(s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)).toEqual(before);
    expect(s.calls.catalogs).toEqual([101]);
    s.db.exec("DROP TRIGGER fail_tv_cursor");
    await runtime.stop(); runtime = undefined;
    s.reopen();
    s.repo.retryJob(job.id);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until("succeeded");
    expect(s.repo.getJobExecution(job.id)!.run_id).toBe(runId);
    expect(s.repo.tv.origins(s.instance, 101)).toEqual(origins);
    expect(s.calls.catalogs).toEqual([101]);
    expect(s.calls.enrich).toBe(1);
    expect(s.repo.tv.requests()).toHaveLength(2);
    expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
    expect(s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)!.cursor > before.cursor).toBe(true);
    expect(s.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("explicit one-episode canary pins stable episode identity when later overlap changes provider episode ordering", async () => {
  const s = await setup();
  try {
    await s.sync({ backfillRequestIds: [100], episodeLimit: 1 });
    const origin = s.repo.tv.origins(s.instance, 100)[0]!;
    const selectedIds = s.repo.tv.requestedEpisodeIds(origin.requestId);
    expect(selectedIds).toHaveLength(1);
    expect(s.repo.tv.requestedEpisodes(origin.requestId)[0]!.key).toBe("tvdb:101");
    for (const acquisition of s.repo.tv.acquisitions(s.repo.tv.requestById(origin.requestId)!.target_id)) s.repo.tv.finishAcquisition(acquisition.id);
    const reordered = catalog();
    [reordered.episodes[0], reordered.episodes[1]] = [reordered.episodes[1]!, reordered.episodes[0]!];
    s.setCatalog(reordered);
    await s.sync();
    await s.sync();
    expect(s.repo.tv.requestedEpisodeIds(origin.requestId)).toEqual(selectedIds);
    expect(s.repo.tv.origins(s.instance, 100)).toHaveLength(1);
    expect(s.repo.listJobsByType("tv_acquire")).toHaveLength(1);
    expect(s.nativeOutbox()).toHaveLength(1);
    expect(s.repo.tv.origins(s.instance, 100)[0]!.source).toMatchObject({ episodeLimit: 1 });
  } finally { await s.close(); }
});

test("future-only canary membership loss preserves pinned intent and cursor without widening or enqueueing acquisition", async () => {
  for (const failure of ["missing-provider-episode", "unapproved-season"] as const) {
    const s = await setup();
    try {
      s.setCatalog(catalog(true));
      await s.sync({ backfillRequestIds: [100], episodeLimit: 1 });
      const origin = s.repo.tv.origins(s.instance, 100)[0]!;
      const selectedIds = s.repo.tv.requestedEpisodeIds(origin.requestId);
      const before = s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)!;
      expect(s.repo.tv.requestedEpisodes(origin.requestId).map((ep) => ep.key)).toEqual(["tvdb:101"]);
      if (failure === "missing-provider-episode") {
        const missing = catalog(true);
        missing.episodes = missing.episodes.filter((ep) => ep.key !== "tvdb:101");
        s.setCatalog(missing);
      } else {
        s.records.set(100, sourceRequest(100, { seasons: [{ seasonNumber: 2, status: 2 }] }));
      }
      await expect(s.sync()).rejects.toThrow("retaining the existing scope");
      expect(s.repo.getJsonState<{ baselineId: number; cursor: string }>(s.feedKey)).toEqual(before);
      expect(s.repo.tv.requestedEpisodeIds(origin.requestId)).toEqual(selectedIds);
      expect(s.repo.tv.origins(s.instance, 100)).toEqual([origin]);
      expect(s.repo.tv.entries()).toHaveLength(1);
      expect(s.calls.enrich).toBe(1);
      expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
      expect(s.nativeOutbox()).toEqual([]);
      s.setCatalog(catalog(true));
      s.records.set(100, sourceRequest(100));
      await s.sync();
      expect(s.repo.tv.requestedEpisodeIds(origin.requestId)).toEqual(selectedIds);
      expect(s.repo.tv.missingEpisodes(origin.requestId)).toHaveLength(1);
      expect(s.repo.listJobsByType("tv_acquire")).toEqual([]);
      expect(s.repo.tv.origins(s.instance, 100)[0]!.source).toMatchObject({ episodeLimit: 1 });
    } finally { await s.close(); }
  }
});

test("intentionally evicted origins accept new approved membership without restoring presence or enqueueing acquisition", async () => {
  const s = await setup();
  try {
    await s.sync();
    const first = catalog();
    first.episodes = first.episodes.filter((ep) => ep.key !== "tvdb:102");
    s.setCatalog(first);
    s.records.set(101, sourceRequest(101));
    await s.sync();
    const origin = s.repo.tv.origins(s.instance, 101)[0]!;
    const targetId = s.repo.tv.requestById(origin.requestId)!.target_id;
    for (const acquisition of s.repo.tv.acquisitions(targetId)) s.repo.tv.finishAcquisition(acquisition.id);
    const key = { namespace: "tv-target", id: String(targetId) };
    s.repo.retention.evict(key, { expectedRevision: s.repo.retention.get(key)!.revision, reason: "Intentionally remove requested TV" });
    s.setCatalog(catalog());
    await s.sync();
    await s.sync();
    expect(s.repo.tv.requestedEpisodes(origin.requestId).map((ep) => ep.key)).toEqual(["tvdb:101", "tvdb:102"]);
    expect(s.repo.retention.get(key)!.desiredPresence).toBe("evicted");
    expect(s.repo.listJobsByType("tv_acquire")).toHaveLength(1);
    expect(s.nativeOutbox()).toHaveLength(1);
  } finally { await s.close(); }
});

test("TV JSON-RPC and tRPC are admin-only, validate canary bounds and omit upstream credentials", async () => {
  const s = await setup();
  try {
    await s.sync({ backfillRequestIds: [100], episodeLimit: 1 });
    for (const [token, isAdmin] of [["admin", true], ["viewer", false]] as const) {
      const user = s.repo.upsertUser({ provider: "local", providerUserId: token, username: token, isAdmin });
      s.repo.createSession(user.id, hashSessionToken(token), null, "browser");
    }
    const handler = createPodibleFetchHandler(s.repo, Date.now(), null);
    type RpcReply = { result?: Record<string, any>; error?: { code: number; message: string } };
    const rpc = async (method: string, params: Record<string, unknown> = {}, token = "admin"): Promise<RpcReply> =>
      (await (await handler(new Request("http://localhost/rpc", { method: "POST",
        headers: { cookie: `podible_session=${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: `tv.${method}`, params }),
      }))).json()) as RpcReply;
    const requestId = s.repo.tv.requests()[0]!.id;
    const methods = [
      ["status", {}], ["list", {}], ["get", { requestId }], ["sync", {}],
      ["backfill", { backfillRequestIds: [100], episodeLimit: 1 }], ["retry", { requestId }],
    ] as const;
    const jobsBeforeDenials = s.count("jobs");
    for (const [method, params] of methods) {
      expect((await rpc(method, params, "viewer")).error?.code).toBe(-32003);
      expect((await rpc(method, params, "invalid")).error?.code).toBe(-32001);
    }
    expect(s.count("jobs")).toBe(jobsBeforeDenials);
    for (const params of [{ backfillRequestIds: [] }, { backfillRequestIds: [1, 2, 3, 4, 5, 6] },
      { backfillRequestIds: [100], episodeLimit: 0 }, { backfillRequestIds: [100], episodeLimit: 51 },
      { backfillRequestIds: [100], episodeLimit: 1, untrustedUrl: "https://wrong.invalid" }])
      expect((await rpc("backfill", params)).error?.code).toBe(-32602);
    const status = await rpc("status"), list = await rpc("list"), detail = await rpc("get", { requestId });
    expect(status.result?.configured).toBe(true);
    expect(status.result?.baselineId).toBe(100);
    expect(detail.result?.requestedEpisodes).toBe(1);
    expect(detail.result?.entry.key).toBe("anidb:1001");
    expect(list.result?.items).toHaveLength(1);
    const encoded = JSON.stringify([status, list, detail]);
    for (const secret of ["seerr-fixture-private", "sonarr-fixture-private", "plex-fixture-private", "seerr.invalid", "sonarr.invalid"])
      expect(encoded).not.toContain(secret);
    expect((await rpc("sync")).result?.jobId).toBeGreaterThan(0);
    const backfill = await rpc("backfill", { backfillRequestIds: [100], episodeLimit: 1 });
    expect(JSON.parse(s.repo.getJob(backfill.result!.jobId)!.payload_json!)).toEqual({ backfillRequestIds: [100], episodeLimit: 1 });
    const trpc = (token: string) => handler(new Request("http://localhost/api/trpc/tv.list?input=%7B%7D", { headers: { cookie: `podible_session=${token}` } }));
    expect((await trpc("viewer")).status).toBe(403);
    expect((await trpc("admin")).status).toBe(200);
    const crossOrigin = await handler(new Request("http://localhost/api/trpc/tv.sync", { method: "POST",
      headers: { origin: "https://untrusted.invalid", cookie: "podible_session=admin", "Content-Type": "application/json" }, body: "{}" }));
    expect(crossOrigin.status).toBe(403);
  } finally { await s.close(); }
});
