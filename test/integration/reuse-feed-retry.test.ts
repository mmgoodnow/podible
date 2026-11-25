import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { APIError, APIUserAbortError } from "openai";
import { LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { discoverCrossSeeds } from "../../src/media/cross-seed-feed";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import type { WorkerContext } from "../../src/worker/context";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";

const unavailable = () => new APIError(503, { message: "SECRET_PROVIDER_BODY" }, undefined, new Headers());

async function fixture(beforeMatch: () => void = () => {}, media: "movie" | "tv" | "ebook" = "ebook") {
  const root = await mkdtemp(path.join(os.tmpdir(), "reuse-feed-retry-"));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db); repo.ensureSettings();
  const book = repo.createBook({ title: "Retained work", author: "Fixture Author" });
  const manifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
  repo.addAsset({ bookId: book.id, manifestationId: manifestation.id, kind: "single", mime: "application/epub+zip", totalSize: 8,
    files: [{ path: "/fixture/book.epub", size: 8, start: 0, end: 8, durationMs: 0 }] });
  const user = repo.upsertUser({ provider: "local", providerUserId: "fixture", username: "fixture", isAdmin: true });
  const id = randomUUID(), file = { id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 8, probe: {} };
  if (media === "movie") {
    const request = repo.movies.request(user.id, fixtureMovie, repo.getSettings().movies.qualityPolicy);
    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "movie_acquire" }).id).acquisition;
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [file]);
    repo.movies.selectVersion(request.targetId, version, repo.movies.target(request.targetId)!.revision);
    repo.movies.finishAcquisition(acquisition.id);
  } else if (media === "tv") {
    const request = repo.tv.request(user.id, { key: "tvdb:123", tvdbId: 123, tmdbId: null, title: "Fixture TV", year: 2020,
      aliases: [], anime: false, relations: [], episodes: [{ key: "tvdb:1234", title: "Pilot", airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: 1 }] }] }, repo.getSettings().movies.qualityPolicy, ["tvdb:1234"]);
    const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "tv_acquire" }).id, request.requestId).acquisition;
    const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ ...file, episodeIds: request.episodeIds }]);
    const revision = repo.tv.target(request.targetId)!.revision;
    repo.tv.stagePublication(request.targetId, version, revision); repo.tv.promotePublication(request.targetId, version, revision);
    repo.tv.clearPublication(request.targetId, version); repo.tv.finishAcquisition(acquisition.id);
  }
  const now = new Date().toISOString();
  repo.releaseInbox.ensureFeed("fixture", "Fixture", media, now);
  const releaseIds = repo.releaseInbox.ingest("fixture", [{ title: book.title, provider: "Fixture", mediaType: media,
    guid: "candidate", url: "https://fixture.invalid/book", infoHash: null, sizeBytes: 8, seeders: 1, leechers: 0, raw: {} }], now);
  const job = repo.createJob({ type: "torrent_reuse_feed", payload: { releaseIds }, maxAttempts: 3 });
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings(), onLog() {}, bookFeedMatcher: async ({ targets }) => {
    beforeMatch();
    return targets.map(target => ({ bookId: target.bookId, media: target.media, releaseIndexes: [0], reason: "Same work" }));
  }, movieFeedMatcher: async ({ targets }) => {
    beforeMatch(); return targets.map(target => ({ targetId: target.targetId, releaseIndexes: [0], reason: "Same work" }));
  }, tvFeedMatcher: async ({ targets }) => {
    beforeMatch(); return targets.map(target => ({ targetId: target.targetId, releaseIndexes: [0], reason: "Same work" }));
  } };
  const checkpoints = new Map<string, unknown>(), sleeps: number[] = [];
  const execution: NonNullable<WorkerContext["execution"]> = { key: "fixture", assertActive() {},
    async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
      if (checkpoints.has(name)) return structuredClone(checkpoints.get(name)) as T;
      const value = await handler(); checkpoints.set(name, structuredClone(value)); return value;
    },
    async sleep(_name, ms) { sleeps.push(ms); },
  };
  return { root, db, repo, ctx, execution, job, releaseIds, book, checkpoints, sleeps,
    async close() { db.close(); await rm(root, { recursive: true, force: true }); } };
}

test("reuse matcher outages outlive the job retry budget, back off durably, and queue once", async () => {
  let calls = 0;
  const f = await fixture(() => { if (++calls <= 8) throw calls === 3 ? new APIUserAbortError() : unavailable(); });
  try {
    const ctx = { ...f.ctx, execution: f.execution };
    await discoverCrossSeeds(ctx, f.job);
    expect(calls).toBe(9);
    expect(f.sleeps).toEqual([60_000, 120_000, 240_000, 480_000, 960_000, 1_800_000, 1_800_000, 1_800_000]);
    expect(f.checkpoints.get("reuse-feed:match:0")).toEqual({ retry: true });
    expect(JSON.stringify([...f.checkpoints])).not.toContain("SECRET_PROVIDER_BODY");
    await discoverCrossSeeds(ctx, f.job);
    expect(calls).toBe(9);
    expect(f.repo.crossSeed.receipts()).toHaveLength(1);
    expect(f.repo.crossSeed.receipts()[0]!.command.releaseId).toBe(f.releaseIds[0]);
  } finally { await f.close(); }
});

test("old successful matcher and queue receipts replay without another model call or reuse job", async () => {
  const f = await fixture(() => { throw new Error("Completed matcher must not run again"); });
  try {
    // Pre-cooldown checkpoints stored the bare match array under this exact name.
    f.checkpoints.set("reuse-feed:match:0", [{ bookId: f.book.id, media: "ebook", releaseIndexes: [0], reason: "Old match" }]);
    const ctx = { ...f.ctx, execution: f.execution };
    await discoverCrossSeeds(ctx, f.job);
    const receipt = f.repo.crossSeed.receipts()[0]!;
    // An old operation receipt also deduplicates a queue whose checkpoint was lost.
    f.checkpoints.delete(`reuse-feed:queue:${f.book.id}:${f.releaseIds[0]}`);
    await discoverCrossSeeds(ctx, f.job);
    expect(f.repo.crossSeed.receipts()).toEqual([receipt]);
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(1);
    expect(f.sleeps).toEqual([]);
  } finally { await f.close(); }
});

test("permanent failures, run controls, and lost ownership never become provider cooldowns", async () => {
  for (const error of [new APIError(401, {}, "Not authorized", new Headers()), new Error("Invalid matcher output"),
    new RunCancelledError("fixture"), new LeaseExpiredError("fixture")]) {
    const f = await fixture(() => { throw error; });
    try {
      await expect(discoverCrossSeeds({ ...f.ctx, execution: f.execution }, f.job)).rejects.toBe(error);
      expect(f.sleeps).toEqual([]);
      expect(f.repo.crossSeed.receipts()).toEqual([]);
    } finally { await f.close(); }
  }
  const f = await fixture(() => { throw new APIUserAbortError(); });
  try {
    const lost = new LeaseExpiredError("fixture");
    f.execution.assertActive = () => { throw lost; };
    await expect(discoverCrossSeeds({ ...f.ctx, execution: f.execution }, f.job)).rejects.toBe(lost);
    expect(f.sleeps).toEqual([]);
    await expect(discoverCrossSeeds(f.ctx, f.job)).rejects.toBeInstanceOf(APIUserAbortError);
  } finally { await f.close(); }
});

for (const media of ["movie", "tv", "ebook"] as const) test(`${media} Reflow restart resumes a persisted cooldown without replaying failures or duplicating dispatch`, async () => {
  let calls = 0;
  const f = await fixture(() => { if (++calls <= 4) throw calls === 3 ? new APIUserAbortError() : unavailable(); }, media);
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  const start = () => createJobExecutionRuntime(f.ctx, f.repo.executionDatabasePath(), { retryDelayMs: 1,
    processJob: (ctx, job) => {
      if (job.id !== f.job.id) return Promise.resolve("done" as const);
      const execution = ctx.execution!;
      return discoverCrossSeeds({ ...ctx, execution: { ...execution,
        sleep: (name, _ms) => execution.sleep(name, name.endsWith(":0") ? 500 : 1),
      } }, job);
    },
  });
  const until = async (check: () => Promise<boolean>) => {
    const deadline = Date.now() + 8000;
    while (!await check()) {
      if (Date.now() > deadline) throw new Error("Reuse feed regression timed out");
      await runtime!.reconcile(); await Bun.sleep(10);
    }
  };
  try {
    runtime = await start(); await runtime.reconcile();
    await until(async () => {
      const link = f.repo.getJobExecution(f.job.id);
      return !!link && ["sleeping", "failed"].includes((await runtime!.engine.getRunStatus(link.run_id))!.run.status);
    });
    const link = f.repo.getJobExecution(f.job.id)!;
    expect(f.repo.getJobStatus(f.job.id)).toBe("queued");
    expect(calls).toBe(1);
    await runtime.stop(); runtime = undefined;
    using journal = new Database(f.repo.executionDatabasePath(), { readonly: true });
    expect(journal.query("SELECT output FROM workflow_steps WHERE run_id=? AND name='reuse-feed:match:0'").get(link.run_id))
      .toEqual({ output: '{"retry":true}' });
    runtime = await start(); await runtime.reconcile();
    await until(async () => ["succeeded", "failed"].includes(f.repo.getJobStatus(f.job.id) ?? ""));
    expect(f.repo.getJobStatus(f.job.id)).toBe("succeeded");
    expect(f.repo.getJobExecution(f.job.id)?.run_id).toBe(link.run_id);
    expect(calls).toBe(5);
    expect(f.repo.crossSeed.receipts()).toHaveLength(1);
    const steps = journal.query<{ name: string; attempts: number; output: string | null; error: string | null }, [string]>(
      "SELECT name,attempts,output,error FROM workflow_steps WHERE run_id=? AND name LIKE 'reuse-feed:match:%'")
      .all(link.run_id);
    const matches = steps.filter(step => !step.name.includes("retry-wait"));
    expect(matches).toHaveLength(5);
    expect(matches.every(step => step.attempts === 1 && step.error === null)).toBe(true);
    expect(JSON.stringify(steps)).not.toContain("SECRET_PROVIDER_BODY");
  } finally { await runtime?.stop(); await f.close(); }
}, 15_000);
