import { Database } from "bun:sqlite";
import { test, expect } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import bencode from "bencode";
import { createHash, randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { crossSeedCommandSchema } from "../../src/media/cross-seed-state";
import { queueCrossSeed, processCrossSeed, crossSeedPlan, type CrossSeedAdapters } from "../../src/media/cross-seed";
import { discoverCrossSeeds } from "../../src/media/cross-seed-feed";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { jobLockKeys, lockSetsConflict } from "../../src/worker/locks";
import { inspectMetainfo } from "../../src/media/metainfo";
import type { TorrentClient, TorrentRef, DownloadLocation } from "../../src/media/torrent-client";
import type { WorkerContext } from "../../src/worker/context";

async function fixture(original = "abcdefgh", desired = "abcdefgh", sidecar = "!!") {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "podible-cross-seed-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.movies.managedRoot = root; repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "reuse", username: "reuse", isAdmin: true });
  const request = repo.movies.request(user.id, fixtureMovie, settings.movies.qualityPolicy);
  const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "movie_acquire" }).id).acquisition;
  await prepareMovieStorage(repo, path.join(root, "objects"));
  const fileId = randomUUID(), relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`;
  const canonical = path.join(root, "objects", relativePath);
  await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, original, { mode: 0o444 });
  const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id: fileId, relativePath, sizeBytes: original.length, probe: {} }]);
  repo.movies.selectVersion(request.targetId, version, repo.movies.target(request.targetId)!.revision);
  repo.movies.finishAcquisition(acquisition.id);
  const files = [{ path: "Pack/renamed.mkv", bytes: Buffer.from(desired) }, ...(sidecar ? [{ path: "Pack/tracker.nfo", bytes: Buffer.from(sidecar) }] : [])];
  const data = Buffer.concat(files.map(file => file.bytes)), pieces: Buffer[] = [];
  for (let i = 0; i < data.length; i += 4) pieces.push(createHash("sha1").update(data.subarray(i, i + 4)).digest());
  const metainfo = new Uint8Array(bencode.encode({ info: { name: "Pack", "piece length": 4, pieces: Buffer.concat(pieces),
    files: files.map(file => ({ path: [path.basename(file.path)], length: file.bytes.length })) } }));
  const metadata = inspectMetainfo(metainfo);
  const feed = repo.releaseInbox.ensureFeed("reuse-fixture", "Other tracker", "movie", new Date().toISOString());
  const [releaseId] = repo.releaseInbox.ingest(feed.id, [{ mediaType: "movie", title: "A Fixture Film other tracker", provider: "Other tracker",
    url: "https://fixture.invalid/metainfo", guid: "candidate", infoHash: metadata.infoHash, sizeBytes: data.length, seeders: 1, leechers: 0, raw: {} }], new Date().toISOString());
  const scope = { media: "movie" as const, targetId: request.targetId };
  let location: DownloadLocation, checking = 0, downloaded = false, bytesDone = 0, adds = 0, resumes = 0, stops = 0;
  const client: TorrentClient = {
    id: `transmission:${randomUUID()}`, capabilities: { pausedAdd: true, recheck: true, derivedLayouts: true, metainfoVersions: [1] },
    async ensureAdded(input) { adds++; location = input.location; return { clientId: this.id, nativeId: metadata.infoHash }; },
    async inspect(ref) { return { ref, location, state: checking-- > 0 ? "checking" : downloaded ? "seeding" : "inactive",
      complete: bytesDone === data.length, sizeBytes: data.length, bytesDone, message: null, observedAt: new Date().toISOString(), downloadBytesPerSecond: 0 }; },
    async files() { return files.map((file, index) => ({ index, path: path.join(root, "downloads", location.relativePath, file.path) })); },
    async recheck() {
      checking = 1; bytesDone = 0;
      const staged = Buffer.concat(await Promise.all(files.map(file => fs.readFile(path.join(root, "downloads", location.relativePath, file.path)).catch(() => Buffer.alloc(file.bytes.length)))));
      for (let i = 0; i < data.length; i += 4) if (staged.subarray(i, i + 4).equals(data.subarray(i, i + 4))) bytesDone += Math.min(4, data.length - i);
    },
    async resume() { resumes++; for (const file of files) await fs.writeFile(path.join(root, "downloads", location.relativePath, file.path), file.bytes); bytesDone = data.length; downloaded = true; },
    async quiesce() { stops++; }, async removeRecord() { throw new Error("Reuse must never remove old records"); },
  };
  // Explicit test adapter supplies independent writable storage on Linux too.
  // Native clone correctness/unsupported behavior is covered by reuse-layout tests.
  const materialize: NonNullable<CrossSeedAdapters["materialize"]> = async ({ plan, root: destination }) => {
    const dataPath = path.join(destination.path, plan.id, "data"); await fs.mkdir(dataPath, { recursive: true });
    for (const file of plan.files) {
      const target = path.join(dataPath, file.candidatePath); await fs.mkdir(path.dirname(target), { recursive: true });
      if (file.source) {
        try { await fs.writeFile(target, await fs.readFile(file.source.path), { flag: "wx" }); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      }
    }
    return { status: "ready", dataPath, layoutPath: path.dirname(dataPath), plan, replay: false };
  };
  const adapters: CrossSeedAdapters = { client: () => client, fetchMetainfo: async () => metainfo, materialize, pollMs: 1, maxPolls: 20 };
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings(), crossSeedAdapters: adapters };
  const command = crossSeedCommandSchema.parse({ operationId: randomUUID(), scope, releaseId,
    policy: { minimumReuseRatio: 0.4, maximumExtraBytes: 8 } });
  return { root, db, repo, ctx, command, canonical, metadata, version, adapters, metainfo,
    counts: () => ({ adds, resumes, stops }), cleanup: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

async function execute(f: Awaited<ReturnType<typeof fixture>>, receipt = queueCrossSeed(f.repo, f.command), wrap = (ctx: WorkerContext) => ctx) {
  const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
    retryDelayMs: 1,
    processJob: (ctx, job) => job.type === "torrent_reuse" ? processCrossSeed(wrap(ctx), job) : Promise.resolve("done" as const) });
  try {
    await runtime.reconcile();
    const end = Date.now() + 10_000;
    while (!["succeeded", "failed"].includes(f.repo.getJob(receipt.jobId)?.status ?? "")) {
      if (Date.now() > end) throw new Error("Reuse runtime timed out"); await Bun.sleep(10);
    }
    expect(f.repo.getJob(receipt.jobId)?.error).toBeNull();
    expect(f.repo.getJob(receipt.jobId)?.status).toBe("succeeded");
    return f.repo.crossSeed.receipt(f.command.operationId)!;
  } finally { await runtime.stop(); }
}

test("cross-seed durable pipeline reuses renamed media, fetches private sidecar and never changes library selection", async () => {
  const f = await fixture();
  try {
    const first = queueCrossSeed(f.repo, f.command);
    expect(queueCrossSeed(f.repo, f.command).jobId).toBe(first.jobId);
    expect(() => queueCrossSeed(f.repo, { ...f.command, policy: { ...f.command.policy, maximumExtraBytes: 2 } })).toThrow("another command");
    const result = await execute(f, first);
    expect(result.outcome).toMatchObject({ kind: "seeding", reusedBytes: 8, downloadedBytes: 2, totalBytes: 10 });
    expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgh");
    expect(f.repo.movies.target(f.command.scope.targetId)!.active_version_id).toBe(f.version);
    expect(f.counts()).toEqual({ adds: 1, resumes: 1, stops: 0 });
    expect(f.repo.reuseTorrent({ clientId: f.adapters.client!(f.command.scope).id, nativeId: f.metadata.infoHash })?.ownership_tag).toBe("Podible Movies");
  } finally { await f.cleanup(); }
});

test("same-length tail mismatch repairs only an isolated variant, retaining the original source", async () => {
  const f = await fixture("abcdefgX", "abcdefgh", "");
  try {
    const result = await execute(f);
    expect(result.outcome).toMatchObject({ kind: "seeding", reusedBytes: 4, downloadedBytes: 4 });
    expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgX");
    expect(f.repo.movies.target(f.command.scope.targetId)!.active_version_id).toBe(f.version);
  } finally { await f.cleanup(); }
});

test("a lost add checkpoint reconciles the same owned record and never repeats publication", async () => {
  const f = await fixture(); let lost = false;
  try {
    const result = await execute(f, queueCrossSeed(f.repo, f.command), ctx => {
      const execution = ctx.execution!;
      return { ...ctx, execution: { ...execution, run: <T>(name: string, action: () => T | Promise<T>): Promise<T> =>
        execution.run(name, async () => {
          const value = await action();
          if (name === "reuse:add" && !lost) { lost = true; throw new Error("lost add checkpoint"); }
          return value;
        }) } };
    });
    expect(lost).toBe(true); expect(result.outcome?.kind).toBe("seeding");
    expect(f.counts()).toEqual({ adds: 2, resumes: 1, stops: 0 });
    expect((f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get() as { n: number }).n).toBe(1);
    expect(f.repo.movies.target(f.command.scope.targetId)!.active_version_id).toBe(f.version);
    expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgh");
  } finally { await f.cleanup(); }
});

test("daemon completion with wrong bytes is stopped rather than accepted as a successful seed", async () => {
  const f = await fixture();
  try {
    const client = f.adapters.client!(f.command.scope), resume = client.resume.bind(client);
    client.resume = async ref => {
      await resume(ref);
      const [file] = await client.files(ref);
      await fs.writeFile(file!.path, "WRONGBYT");
    };
    expect((await execute(f)).outcome).toMatchObject({ kind: "stalled", reason: "Engine completion did not match candidate piece hashes" });
    expect(f.counts().stops).toBe(1);
    expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgh");
  } finally { await f.cleanup(); }
});

test("a warning from one tracker does not stop an otherwise active candidate", async () => {
  const f = await fixture();
  try {
    const client = f.adapters.client!(f.command.scope), inspect = client.inspect.bind(client);
    let warned = false;
    client.inspect = async ref => {
      const snapshot = await inspect(ref);
      if (f.counts().resumes && !warned) {
        warned = true;
        return { ...snapshot, complete: false, state: "downloading", message: "One tracker unavailable" };
      }
      return snapshot;
    };
    expect((await execute(f)).outcome?.kind).toBe("seeding");
    expect(warned).toBe(true); expect(f.counts().stops).toBe(0);
  } finally { await f.cleanup(); }
});

test("reuse holds its target lane, not a global lock that blocks unrelated acquisitions", async () => {
  const f = await fixture();
  try {
    const queued = queueCrossSeed(f.repo, f.command);
    const keys = jobLockKeys(f.repo, f.repo.getJob(queued.jobId)!);
    expect(keys).toEqual(["torrent-reuse", `movie-target:${f.command.scope.targetId}`]);
    expect(lockSetsConflict(new Set(keys), ["tv-target:99"])).toBe(false);
    expect(lockSetsConflict(new Set(keys), [`movie-target:${f.command.scope.targetId}`])).toBe(true);
    expect(lockSetsConflict(new Set(keys), ["*"])).toBe(true);
  } finally { await f.cleanup(); }
});

test("an unsupported CoW filesystem has an explicit no-action outcome, not a full-copy fallback", async () => {
  const f = await fixture();
  try {
    f.adapters.materialize = async () => ({ status: "unsupported", reason: "CoW unavailable" });
    expect((await execute(f)).outcome?.kind).toBe("unsupported");
    expect(f.counts().adds).toBe(0); expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgh");
  } finally { await f.cleanup(); }
});

test("eviction fencing and immutable inbox identity reject commands without starting torrents", async () => {
  const f = await fixture();
  try {
    expect(() => queueCrossSeed(f.repo, { ...f.command, releaseId: 9999 })).toThrow("retained");
    const intent = f.repo.retention.get({ namespace: "movie-target", id: String(f.command.scope.targetId) })!;
    f.repo.retention.evict({ namespace: "movie-target", id: String(f.command.scope.targetId) }, { expectedRevision: intent.revision, reason: "fixture" });
    expect(() => queueCrossSeed(f.repo, f.command)).toThrow("evicted");
    expect(f.counts().adds).toBe(0);
  } finally { await f.cleanup(); }
});

test("completed targets are considered by RSS without per-title searches or waiting for a quality upgrade", async () => {
  const f = await fixture();
  try {
    const job = f.repo.createJob({ type: "torrent_reuse_feed", payload: { releaseIds: [f.command.releaseId] } });
    const ctx = { ...f.ctx, movieFeedMatcher: async () => [{ targetId: f.command.scope.targetId, releaseIndexes: [0], reason: "Same movie, another tracker" }] };
    await discoverCrossSeeds(ctx, job); await discoverCrossSeeds(ctx, job);
    const operations = f.repo.crossSeed.receipts();
    expect(operations).toHaveLength(1); expect(operations[0]!.command.scope).toEqual(f.command.scope);
    expect(f.counts().adds).toBe(0);
  } finally { await f.cleanup(); }
});

test("reuse RPC is admin-only, accepts only retained release IDs, and reads status without effects", async () => {
  const f = await fixture();
  try {
    for (const name of ["admin", "reader"]) {
      const user = f.repo.upsertUser({ provider: "local", providerUserId: name, username: name, isAdmin: name === "admin" });
      f.repo.createSession(user.id, hashSessionToken(name), null, "browser");
    }
    const handler = createPodibleFetchHandler(f.repo, Date.now());
    const rpc = async (method: string, token: string, params: unknown = {}) => {
      const response = await handler(new Request("http://localhost/rpc", { method: "POST",
        headers: { cookie: `podible_session=${token}`, "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }));
      return await response.json() as any;
    };
    for (const method of ["crossSeed.create", "crossSeed.preview", "crossSeed.status", "crossSeed.reportNuked"]) {
      expect((await rpc(method, "missing")).error.code).toBe(-32001);
      expect((await rpc(method, "reader")).error.code).toBe(-32003);
    }
    expect((await rpc("crossSeed.create", "admin", { ...f.command, url: "https://arbitrary.invalid/torrent" })).error).toBeTruthy();
    const first = (await rpc("crossSeed.create", "admin", f.command)).result;
    expect(first.jobId).toBeGreaterThan(0);
    expect((await rpc("crossSeed.create", "admin", f.command)).result.jobId).toBe(first.jobId);
    const status = (await rpc("crossSeed.status", "admin", { operationId: f.command.operationId })).result;
    expect(status.operations).toHaveLength(1);
    expect(JSON.stringify(status)).not.toContain("fixture.invalid");
    expect(f.counts()).toEqual({ adds: 0, resumes: 0, stops: 0 });
  } finally { await f.cleanup(); }
});

test("RSS reuse queues every routed release rather than silently dropping the fifth match", async () => {
  const f = await fixture();
  try {
    const now = new Date().toISOString();
    const entry = f.repo.releaseInbox.entries([f.command.releaseId])[0]!;
    const ids = f.repo.releaseInbox.ingest(entry.feed_id, Array.from({ length: 6 }, (_, i) => ({
      ...entry.result, guid: `extra-${i}`, infoHash: null,
    })), now);
    const job = f.repo.createJob({ type: "torrent_reuse_feed", payload: { releaseIds: ids } });
    await discoverCrossSeeds({ ...f.ctx, movieFeedMatcher: async () => [{ targetId: f.command.scope.targetId,
      releaseIndexes: ids.map((_, i) => i), reason: "Same work" }] }, job);
    expect(f.repo.crossSeed.receipts()).toHaveLength(6);
  } finally { await f.cleanup(); }
});

test("TV reuse reads its own canonical pool and leaves episode publication unchanged", async () => {
  const f = await fixture();
  try {
    const settings = f.repo.getSettings(); settings.tv.managedRoot = f.root; f.repo.updateSettings(settings);
    const user = f.repo.upsertUser({ provider: "local", providerUserId: "tv-reuse", username: "tv-reuse", isAdmin: true });
    const request = f.repo.tv.request(user.id, { key: "tvdb:123", tvdbId: 123, tmdbId: null, title: "Fixture TV", year: 2020,
      aliases: [], anime: false, relations: [], episodes: [{ key: "tvdb:1234", title: "Pilot", airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: 1 }] }] }, settings.movies.qualityPolicy, ["tvdb:1234"]);
    const acquisition = f.repo.tv.startAcquisition(request.targetId, "initial", id => f.repo.createJob({
      type: "tv_acquire", payload: { acquisitionId: id } }).id, request.requestId).acquisition;
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`;
    const source = path.join(f.root, "objects", relativePath);
    await fs.mkdir(path.dirname(source), { recursive: true }); await fs.writeFile(source, "abcdefgh", { mode: 0o444 });
    const version = f.repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id, relativePath, sizeBytes: 8,
      probe: {}, episodeIds: request.episodeIds }]);
    const revision = f.repo.tv.target(request.targetId)!.revision;
    f.repo.tv.stagePublication(request.targetId, version, revision);
    f.repo.tv.promotePublication(request.targetId, version, revision);
    f.repo.tv.clearPublication(request.targetId, version); f.repo.tv.finishAcquisition(acquisition.id);
    const published = f.repo.tv.activeEpisodes(request.targetId);
    const entry = f.repo.releaseInbox.entries([f.command.releaseId])[0]!;
    const now = new Date().toISOString(), feed = f.repo.releaseInbox.ensureFeed("tv-reuse", "Other tracker", "tv", now);
    const [releaseId] = f.repo.releaseInbox.ingest(feed.id, [{ ...entry.result, mediaType: "tv", guid: "tv-candidate" }], now);
    f.command.scope = { media: "tv", targetId: request.targetId }; f.command.releaseId = releaseId!;
    const plan = await crossSeedPlan(f.repo, f.repo.getSettings(), f.command, f.metainfo);
    expect(plan.files[0]!.source?.id).toBe(id);
    expect((await execute(f)).outcome?.kind).toBe("seeding");
    expect(f.repo.tv.activeEpisodes(request.targetId)).toEqual(published);
    expect(await fs.readFile(source, "utf8")).toBe("abcdefgh");
  } finally { await f.cleanup(); }
});
