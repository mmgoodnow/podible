import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { openDatabase } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import type { WorkerContext } from "../../src/worker/context";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { startCrossSeedBackfill, processCrossSeedBackfill, crossSeedBackfillStatus, latestCrossSeedBackfill, cancelCrossSeedBackfill,
  type CrossSeedBackfillAdapters } from "../../src/media/cross-seed-backfill";
import type { IndexerGateway, IndexerMedia, SearchResult } from "../../src/media/indexer-gateway";
import { IndexerError, indexerFailure, type IndexerFailure } from "../../src/media/indexer-errors";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { queueCrossSeed } from "../../src/media/cross-seed";
import { crossSeedRegistrationKey, crossSeedStateKey } from "../../src/media/cross-seed-state";
import { feedIdentity } from "../../src/media/release-inbox";

const source = { name: "Fixture tracker", baseUrl: "https://fixture.invalid/1/api", apiKey: "private-secret" };
function result(media: IndexerMedia, guid = "candidate"): SearchResult<IndexerMedia> {
  return { mediaType: media, title: "Retained work", provider: source.name, guid, url: `https://fixture.invalid/download/${guid}?secret=hidden`,
    infoHash: null, sizeBytes: 8, seeders: 1, leechers: 0, raw: {} };
}
function gateway(handler: (sources: { name: string }[], query: string, media: IndexerMedia, limit: number) => Promise<{ results: SearchResult<IndexerMedia>[]; failures: IndexerFailure[] }>): Pick<IndexerGateway, "search"> {
  return { search: ((sources, query, media, options) => handler(sources, query, media, options?.limit ?? 0)) as IndexerGateway["search"] };
}
async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-backfill-")), db = openDatabase(path.join(root, "app.sqlite"));
  const repo = new BooksRepo(db); repo.ensureSettings();
  const user = repo.upsertUser({ provider: "local", providerUserId: "admin", username: "admin", isAdmin: true });
  const book = (media: "audio" | "ebook", title = "Retained work") => {
    const book = repo.createBook({ title, author: "Fixture Author" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: media });
    repo.addAsset({ bookId: book.id, manifestationId: manifestation.id, kind: "single", mime: media === "audio" ? "audio/mp4" : "application/epub+zip", totalSize: 8,
      files: [{ path: `/fixture/${book.id}/${media}`, size: 8, start: 0, end: 8, durationMs: media === "audio" ? 1000 : 0 }] });
    return book;
  };
  const movie = () => {
    const request = repo.movies.request(user.id, fixtureMovie, repo.getSettings().movies.qualityPolicy);
    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "movie_acquire" }).id).acquisition;
    const id = randomUUID();
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 8, probe: {} }]);
    repo.movies.selectVersion(request.targetId, version, repo.movies.target(request.targetId)!.revision); repo.movies.finishAcquisition(acquisition.id);
  };
  const tv = (key: number) => {
    const request = repo.tv.request(user.id, { key: `tvdb:${key}`, tvdbId: key, tmdbId: null, title: "Same Series", year: 2020,
      aliases: [], anime: false, relations: [], episodes: [{ key: `tvdb:${key}1`, title: "Pilot", airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: 1 }] }] }, repo.getSettings().movies.qualityPolicy, [`tvdb:${key}1`]);
    const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "tv_acquire" }).id, request.requestId).acquisition;
    const id = randomUUID(), version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 8, probe: {}, episodeIds: request.episodeIds }]);
    const revision = repo.tv.target(request.targetId)!.revision;
    repo.tv.stagePublication(request.targetId, version, revision); repo.tv.promotePublication(request.targetId, version, revision);
    repo.tv.clearPublication(request.targetId, version); repo.tv.finishAcquisition(acquisition.id);
    return request.targetId;
  };
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings(), onLog() {},
    bookFeedMatcher: async ({ targets, releases }) => targets.map(target => ({ bookId: target.bookId, media: target.media, releaseIndexes: releases.map((_, i) => i), reason: "Same work" })),
    movieFeedMatcher: async ({ targets, offers }) => targets.map(target => ({ targetId: target.targetId, releaseIndexes: offers.map((_, i) => i), reason: "Same work" })),
    tvFeedMatcher: async ({ targets, offers }) => targets.map(target => ({ targetId: target.targetId, releaseIndexes: offers.map((_, i) => i), reason: "Same work" })) };
  const checkpoints = new Map<string, unknown>();
  let lostRecord = false, injectLostRecord = false;
  ctx.execution = { key: randomUUID(), assertActive() {},
    async run(name, handler) {
      if (checkpoints.has(name)) return structuredClone(checkpoints.get(name)) as Awaited<ReturnType<typeof handler>>;
      const value = await handler();
      if (injectLostRecord && !lostRecord && name.startsWith("backfill:search:") && name.endsWith(":record")) { lostRecord = true; throw new Error("Lost progress checkpoint"); }
      checkpoints.set(name, structuredClone(value)); return value;
    }, async sleep() { throw new Error("Fixture suspended"); } };
  const run = (requestId: string, adapters: CrossSeedBackfillAdapters) => processCrossSeedBackfill(ctx, repo.getJob(startCrossSeedBackfill(repo, { requestId }).jobId)!, adapters);
  return { root, db, repo, user, ctx, book, movie, tv, run, loseRecord() { injectLostRecord = true; },
    async close() { db.close(); await rm(root, { recursive: true, force: true }); } };
}

test("backfill snapshots all formats, shares identical TV searches, and exposes no credentials", async () => {
  const f = await fixture();
  try {
    f.book("audio"); f.book("ebook"); f.movie(); const first = f.tv(123), second = f.tv(124);
    const requests: string[] = [], id = randomUUID();
    await f.run(id, { sources: async () => [source], gateway: gateway(async (_, query, media, limit) => {
      requests.push(`${media}:${query}`); expect(limit).toBe(150); return { results: [result(media)], failures: [] };
    }) });
    const status = crossSeedBackfillStatus(f.repo, { requestId: id });
    expect(status).toMatchObject({ status: "completed", total: 5, scanned: 5, queued: 5, errors: 0 });
    expect(requests.filter(request => request === "tv:Same Series")).toHaveLength(1);
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(5);
    expect(new Set(f.repo.crossSeed.receipts().filter(r => r.command.scope.media === "tv").map(r => r.command.scope.targetId))).toEqual(new Set([first, second]));
    expect(JSON.stringify(status)).not.toContain("private-secret"); expect(JSON.stringify(status)).not.toContain("fixture.invalid");
  } finally { await f.close(); }
});

test("explicit backfill reconsiders terminal attempts but skips pending and currently registered seeds", async () => {
  const f = await fixture();
  try {
    const books = [0, 1, 2, 3].map(index => f.book("audio", `Work ${index}`));
    const now = new Date().toISOString(), feedId = feedIdentity(source, "audio");
    f.repo.releaseInbox.ensureFeed(feedId, source.name, "audio", now);
    const [releaseId] = f.repo.releaseInbox.ingest(feedId, [result("audio")], now);
    for (const [index, book] of books.entries()) {
      const receipt = queueCrossSeed(f.repo, { operationId: randomUUID(), scope: { media: "audio", targetId: book.id }, releaseId: releaseId! });
      if (index === 1) continue;
      f.db.query("UPDATE jobs SET status='succeeded' WHERE id=?").run(receipt.jobId);
      receipt.outcome = { kind: index === 0 ? "skipped" : "seeding", reason: "Historical result", at: now };
      if (index === 2) {
        const clientId = `transmission:${randomUUID()}`, hash = "c".repeat(40);
        const torrent = f.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,?) RETURNING id")
          .get(clientId, hash, JSON.stringify({ rootId: "books", relativePath: hash }), "fixture") as { id: number };
        receipt.outcome.torrentId = torrent.id;
        f.repo.setJsonState(crossSeedRegistrationKey({ clientId, nativeId: hash }), { operationId: receipt.command.operationId });
      }
      f.repo.setJsonState(crossSeedStateKey(receipt.command.operationId), receipt);
    }
    const requestId = randomUUID();
    await f.run(requestId, { sources: async () => [source], gateway: gateway(async () => ({ results: [], failures: [] })) });
    expect(crossSeedBackfillStatus(f.repo, { requestId })).toMatchObject({ total: 4, queued: 2, skipped: 2 });
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(6);
  } finally { await f.close(); }
});

test("entire library beyond 100 targets is included with paginated progress", async () => {
  const f = await fixture();
  try {
    for (let i = 0; i < 103; i++) f.book("audio", `Book ${i}`);
    f.repo.createBook({ title: "No completed files", author: "Fixture" });
    const id = randomUUID();
    await f.run(id, { sources: async () => [source], gateway: gateway(async () => ({ results: [], failures: [] })) });
    const first = crossSeedBackfillStatus(f.repo, { requestId: id, limit: 100 });
    expect(first.total).toBe(103); expect(first.scanned).toBe(103); expect(first.targets).toHaveLength(100); expect(first.nextOffset).toBe(100);
    expect(crossSeedBackfillStatus(f.repo, { requestId: id, offset: 100 }).targets).toHaveLength(3);
  } finally { await f.close(); }
});

test("retained pages survive holes and cursor state stays unchanged; missing sources are visible errors", async () => {
  const f = await fixture();
  try {
    f.book("audio"); const now = new Date().toISOString();
    const feed = f.repo.releaseInbox.ensureFeed("retained", source.name, "audio", now);
    const ids = f.repo.releaseInbox.ingest(feed.id, Array.from({ length: 31 }, (_, i) => result("audio", `retained-${i}`)), now);
    f.repo.releaseInbox.succeed(feed.id, "unchanged-head", null, now);
    f.db.query("DELETE FROM release_inbox WHERE id=?").run(ids[5]!);
    const before = f.repo.releaseInbox.feed(feed.id), id = randomUUID();
    await expect(f.run(id, { sources: async () => [], gateway: gateway(async () => { throw new Error("No source may be searched"); }) })).rejects.toThrow("provider errors");
    const status = crossSeedBackfillStatus(f.repo, { requestId: id });
    expect(status).toMatchObject({ total: 1, scanned: 1, queued: 30, errors: 1 });
    expect(status.targets[0]!.providers[0]).toMatchObject({ status: "error", provider: "No configured audio indexers" });
    expect(f.repo.releaseInbox.feed(feed.id)).toEqual(before);
  } finally { await f.close(); }
});

test("lost app progress checkpoint does not double-count or queue duplicate children", async () => {
  const f = await fixture();
  try {
    f.book("audio"); f.loseRecord(); const id = randomUUID(); let calls = 0;
    const adapters = { sources: async () => [source], gateway: gateway(async () => { calls++; return { results: [result("audio")], failures: [] }; }) };
    await expect(f.run(id, adapters)).rejects.toThrow("Lost progress checkpoint");
    await f.run(id, adapters);
    expect(crossSeedBackfillStatus(f.repo, { requestId: id })).toMatchObject({ total: 1, scanned: 1, queued: 1, skipped: 0, errors: 0 });
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(1); expect(calls).toBe(1);
  } finally { await f.close(); }
});

test("discovery outage in one format never blocks retained routing or healthy-format searches", async () => {
  const f = await fixture();
  try {
    f.book("audio"); f.book("ebook"); const now = new Date().toISOString();
    for (const media of ["audio", "ebook"] as const) {
      const feed = f.repo.releaseInbox.ensureFeed(`retained-${media}`, source.name, media, now);
      f.repo.releaseInbox.ingest(feed.id, [result(media, `retained-${media}`)], now);
    }
    const original = f.ctx.execution!.run;
    let insideHandler = false;
    f.ctx.execution!.run = (name, handler) => original(name, async () => {
      insideHandler = true; try { return await handler(); } finally { insideHandler = false; }
    });
    f.ctx.execution!.sleep = async () => { expect(insideHandler).toBe(false); throw new Error("Fixture suspended"); };
    const searched: string[] = [], id = randomUUID();
    await expect(f.run(id, { sources: async media => {
      if (media === "audio") throw new IndexerError(indexerFailure("Audio discovery", 429, Date.now() + 60_000));
      return [source];
    }, gateway: gateway(async (_, _query, media) => { searched.push(media); return { results: [result(media, "new-ebook")], failures: [] }; }) }))
      .rejects.toThrow("Fixture suspended");
    const status = crossSeedBackfillStatus(f.repo, { requestId: id });
    expect(status).toMatchObject({ status: "waiting", total: 2, scanned: 1, queued: 3, errors: 0 });
    expect(status.targets.every(target => target.retainedDone)).toBe(true);
    expect(status.targets.find(target => target.scope.media === "audio")!.providers[0]!.status).toBe("waiting");
    expect(searched).toEqual(["ebook"]);
  } finally { await f.close(); }
});

test("start coalesces active clicks and scoped cancellation survives reload", async () => {
  const f = await fixture();
  try {
    const id = randomUUID(), first = startCrossSeedBackfill(f.repo, { requestId: id });
    expect(startCrossSeedBackfill(f.repo, { requestId: randomUUID() }).jobId).toBe(first.jobId);
    expect(latestCrossSeedBackfill(f.repo).backfill?.requestId).toBe(id);
    expect(cancelCrossSeedBackfill(f.repo, id).status).toBe("cancelled");
    expect(latestCrossSeedBackfill(f.repo).backfill?.status).toBe("cancelled");
    f.repo.retryJob(first.jobId);
    expect(latestCrossSeedBackfill(f.repo).backfill?.status).toBe("queued");
    expect(f.repo.listJobsByType("torrent_reuse_backfill")).toHaveLength(1);
  } finally { await f.close(); }
});

test("cancel/retry keeps the original scope snapshot and refreshes failed searches in the new generation", async () => {
  const f = await fixture();
  try {
    const original = f.book("audio"), id = randomUUID(); let fixed = false, calls = 0;
    const adapters: CrossSeedBackfillAdapters = { sources: async () => [source], gateway: gateway(async () => {
      calls++;
      return fixed ? { results: [result("audio")], failures: [] } : { results: [], failures: [indexerFailure(source.name, 401, null)] };
    }) };
    await expect(f.run(id, adapters)).rejects.toThrow("provider errors");
    f.book("audio", "Added after snapshot");
    const request = cancelCrossSeedBackfill(f.repo, id); f.repo.retryJob(request.jobId); fixed = true;
    expect(f.repo.jobExecutionGeneration(request.jobId)).toBe(1);
    await f.run(id, adapters);
    const status = crossSeedBackfillStatus(f.repo, { requestId: id });
    expect(status).toMatchObject({ total: 1, scanned: 1, queued: 1, errors: 0, status: "completed" });
    expect(status.targets.map(target => target.scope.targetId)).toEqual([original.id]); expect(calls).toBe(2);
  } finally { await f.close(); }
});

test("backfill RPCs require admin and never accept client-authored download URLs", async () => {
  const f = await fixture();
  try {
    f.repo.createSession(f.user.id, hashSessionToken("admin"), null, "browser");
    const reader = f.repo.upsertUser({ provider: "local", providerUserId: "reader", username: "reader", isAdmin: false });
    f.repo.createSession(reader.id, hashSessionToken("reader"), null, "browser");
    const handler = createPodibleFetchHandler(f.repo, Date.now());
    const rpc = async (method: string, token: string, params: unknown = {}) => {
      const response = await handler(new Request("http://localhost/rpc", { method: "POST", headers: { cookie: `podible_session=${token}`, "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }));
      return await response.json() as { error?: { code: number }; result?: unknown };
    };
    for (const method of ["startBackfill", "backfillStatus", "backfillLatest", "cancelBackfill"]) {
      expect((await rpc(`crossSeed.${method}`, "missing")).error?.code).toBe(-32001);
      expect((await rpc(`crossSeed.${method}`, "reader")).error?.code).toBe(-32003);
    }
    expect((await rpc("crossSeed.startBackfill", "admin", { requestId: randomUUID(), url: "https://arbitrary.invalid/torrent" })).error).toBeTruthy();
    expect((await rpc("crossSeed.backfillLatest", "admin")).result).toEqual({ backfill: null });
  } finally { await f.close(); }
});

test("real Reflow restart preserves provider cooldown progress, isolates failures and retries only failed searches", async () => {
  const f = await fixture(); let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  try {
    f.book("audio"); const id = randomUUID(), request = startCrossSeedBackfill(f.repo, { requestId: id });
    let cooled = false, fixed = false, healthyCalls = 0, brokenCalls = 0;
    const broken = { ...source, name: "Broken tracker", baseUrl: "https://fixture.invalid/2/api" };
    const adapters: CrossSeedBackfillAdapters = { sources: async () => [source, broken], gateway: gateway(async sources => {
      if (sources[0]!.name === source.name) {
        healthyCalls++;
        if (!cooled) { cooled = true; return { results: [], failures: [indexerFailure(source.name, 429, Date.now() + 300)] }; }
        return { results: [result("audio")], failures: [] };
      }
      brokenCalls++;
      return fixed ? { results: [], failures: [] } : { results: [], failures: [indexerFailure(broken.name, 401, null)] };
    }) };
    const start = () => createJobExecutionRuntime({ ...f.ctx, execution: undefined }, f.repo.executionDatabasePath(), { retryDelayMs: 1,
      processJob: (ctx, job) => job.type === "torrent_reuse_backfill" ? processCrossSeedBackfill(ctx, job, adapters) : Promise.resolve("done" as const) });
    const until = async (check: () => boolean | Promise<boolean>) => {
      const deadline = Date.now() + 8000;
      while (!await check()) {
        if (Date.now() > deadline) {
          const link = f.repo.getJobExecution(request.jobId), execution = link ? await runtime!.engine.getRunStatus(link.run_id) : null;
          throw new Error("Backfill runtime timed out: " + JSON.stringify({ job: f.repo.getJob(request.jobId), progress: crossSeedBackfillStatus(f.repo, { requestId: id }),
            tail: execution?.steps.slice(-5).map(step => ({ name: step.name, status: step.status, error: step.error })) }));
        }
        await runtime!.reconcile(); await Bun.sleep(10);
      }
    };
    runtime = await start(); await runtime.reconcile();
    await until(async () => {
      const link = f.repo.getJobExecution(request.jobId);
      return crossSeedBackfillStatus(f.repo, { requestId: id }).status === "waiting" && !!link &&
        (await runtime!.engine.getRunStatus(link.run_id))?.run.status === "sleeping";
    });
    const link = f.repo.getJobExecution(request.jobId)!;
    expect(brokenCalls).toBe(1);
    await runtime.stop(); runtime = await start(); await runtime.reconcile();
    await until(() => f.repo.getJobStatus(request.jobId) === "failed");
    expect(f.repo.getJobExecution(request.jobId)?.run_id).toBe(link.run_id);
    expect(crossSeedBackfillStatus(f.repo, { requestId: id })).toMatchObject({ queued: 1, errors: 1, status: "failed" });
    const healthyBeforeRetry = healthyCalls;
    fixed = true; f.repo.retryJob(request.jobId);
    expect(latestCrossSeedBackfill(f.repo).backfill?.status).toBe("queued");
    await runtime.reconcile(); await until(() => f.repo.getJobStatus(request.jobId) === "succeeded");
    expect(crossSeedBackfillStatus(f.repo, { requestId: id })).toMatchObject({ total: 1, scanned: 1, queued: 1, errors: 0, status: "completed" });
    expect(healthyCalls).toBe(healthyBeforeRetry); expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(1);
  } finally { await runtime?.stop(); await f.close(); }
}, 15_000);
