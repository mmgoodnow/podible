import { expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { reconcileMovieView } from "../../src/movies/publication";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import type { WorkerContext } from "../../src/worker/context";
import { DEFAULT_RETENTION_POLICY, retentionPreviewSchema } from "../../src/media/retention-policy-types";
import { RETENTION_EXECUTION_SCHEMA_SQL } from "../../src/media/retention-execution-schema";
import { inspectRetentionCandidate, previewRetention, queueMovieEviction, type RetentionAdapters } from "../../src/media/retention-policy";
import { observeRetentionDisk } from "../../src/media/retention-filesystem";
import { runRetentionCleanup } from "../../src/media/retention-execution";
import { RetentionRepo } from "../../src/media/retention-repo";
import { RetentionService } from "../../src/media/retention-service";

const clientId = "transmission:11111111-1111-4111-8111-111111111111";
const facts = { identityVerified: true, cut: "theatrical", source: "web" as const, width: 1920, height: 1080,
  videoCodec: "h264", dynamicRange: "sdr" as const, audioLanguages: ["en"], proper: false };

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "retention-execution-")));
  const databasePath = path.join(root, "app.sqlite");
  let db = new Database(databasePath);
  runMigrations(db);
  if (!db.query("SELECT 1 FROM sqlite_master WHERE name='retention_evictions'").get()) db.exec(RETENTION_EXECUTION_SCHEMA_SQL);
  let repo = new BooksRepo(db);
  const settings = repo.getSettings();
  settings.movies.managedRoot = path.join(root, "movies");
  settings.movies.plexViewPath = "/fixture/movies";
  settings.movies.plexUrl = "https://plex.invalid";
  settings.movies.plexToken = "private-plex-secret";
  settings.movies.plexSectionId = "4";
  repo.updateSettings(settings);
  const contentRoot = path.join(settings.movies.managedRoot, "objects");
  await prepareMovieStorage(repo, contentRoot);
  const roots = { contentRoot, viewRoot: path.join(settings.movies.managedRoot, "views", "plex", "movies"), serverViewRoot: settings.movies.plexViewPath };
  const user = repo.upsertUser({ provider: "local", providerUserId: "retention-test", username: "viewer", isAdmin: true });
  const watches = new Map<number, { state: "watched" | "unwatched" | "unknown"; coverage: "global-all-users" | "unknown";
    playCount: number | null; lastViewedAt: string | null; activePlayback: "active" | "none" | "unknown" }>();
  const torrents = new Map<string, Record<string, unknown>>();
  let available = 0, watchCalls = 0;
  const rpcCalls: string[] = [];
  const adapters: RetentionAdapters = {
    watchHistory: async ({ tmdbId }) => { watchCalls++; return watches.get(tmdbId)!; },
    reclaimableAllocation: async (file) => file.allocatedBytes,
    observeDisk: async (directory) => ({ ...await observeRetentionDisk(directory), availableBytes: available }),
    transmission: { clientId, rpc: { call: async (method, args) => {
      rpcCalls.push(method); expect(method).toBe("torrent-get");
      return { torrents: (args!.ids as string[]).map((hash) => torrents.get(hash)).filter(Boolean) };
    } } },
  };
  const context: WorkerContext = { get repo() { return repo; }, getSettings: () => repo.getSettings(), onLog() {} };
  const saved = new Map<string, unknown>();
  const execution: NonNullable<WorkerContext["execution"]> = { key: "retention-fixture", assertActive() {}, sleep: async () => {},
    run: async <T>(name: string, action: () => T | Promise<T>) => {
      if (saved.has(name)) return structuredClone(saved.get(name)) as T;
      const result = await action(); saved.set(name, structuredClone(result)); return result;
    } };
  const add = async (number: number, playCount = 1, viewedDaysAgo = 180, parts = 1) => {
    const movie = { tmdbId: number, title: `Owned movie ${number}`, originalTitle: `Owned movie ${number}`, year: 2020,
      overview: "Fixture", originalLanguage: "en", posterUrl: null, imdbId: null };
    const requested = repo.movies.request(user.id, movie, defaultMovieQualityPolicy);
    const acquisition = repo.movies.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
    const files = [];
    for (let index = 0; index < parts; index++) {
      const fileId = randomUUID(), relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`, canonical = path.join(contentRoot, relativePath);
      await fs.mkdir(path.dirname(canonical), { recursive: true });
      await fs.writeFile(canonical, Buffer.alloc(5000, number + index));
      files.push({ fileId, relativePath, canonical });
    }
    const { fileId, relativePath, canonical } = files[0]!;
    const versionId = repo.movies.recordVersion(acquisition.id, facts,
      files.map((file) => ({ id: file.fileId, relativePath: file.relativePath, sizeBytes: 5000, probe: {} })));
    repo.movies.selectVersion(requested.targetId, versionId, 0); repo.movies.finishAcquisition(acquisition.id);
    db.query("UPDATE jobs SET status='succeeded' WHERE id=?").run(acquisition.job_id);
    const key = { namespace: "movie-target", id: String(requested.targetId) };
    const old = new Date(Date.now() - 90 * 86_400_000).toISOString();
    db.query("UPDATE retention_events SET created_at=? WHERE namespace=? AND resource_id=?").run(old, key.namespace, key.id);
    db.query("UPDATE movie_requests SET created_at=? WHERE target_id=?").run(old, requested.targetId);
    const hash = number.toString(16).padStart(40, "0"), seedRoot = path.join(settings.movies.managedRoot, "downloads", hash);
    await fs.mkdir(seedRoot, { recursive: true });
    const seedFiles = [];
    for (const [index, file] of files.entries()) {
      const name = index ? `movie-${index}.mkv` : "movie.mkv";
      await fs.copyFile(file.canonical, path.join(seedRoot, name));
      seedFiles.push({ name, length: 5000, bytesCompleted: 5000 });
    }
    repo.movies.bindDownload(acquisition.id, { clientId, nativeId: hash }, { rootId: "movies", relativePath: hash }, `offer-${number}`);
    torrents.set(hash, { hashString: hash, downloadDir: seedRoot, labels: ["Podible Movies"], files: seedFiles,
      leftUntilDone: 0, secondsSeeding: 0, error: 0, status: 6 });
    watches.set(number, { state: "watched", coverage: "global-all-users", playCount,
      lastViewedAt: new Date(Date.now() - viewedDaysAgo * 86_400_000).toISOString(), activePlayback: "none" });
    await reconcileMovieView(repo.movies, roots);
    return { requested, acquisition, fileId, relativePath, canonical, versionId, key, seed: path.join(seedRoot, "movie.mkv"), hash, movie, files };
  };
  return { root, roots, contentRoot, settings, user, context, execution, adapters, add, watches, torrents, rpcCalls, saved,
    get repo() { return repo; }, get db() { return db; }, get watchCalls() { return watchCalls; },
    setAvailable(value: number) { available = value; },
    async reopen() { db.close(); db = new Database(databasePath); repo = new BooksRepo(db); },
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

test("retention preview is dry-run and healthy pressure loops do no candidate or provider work", async () => {
  await using f = await fixture();
  const movie = await f.add(1), before = f.db.query("SELECT total_changes() AS n").get();
  const preview = await previewRetention(f.repo, f.adapters);
  expect(retentionPreviewSchema.parse(preview)).toEqual(preview);
  expect(preview.candidates[0]).toMatchObject({ eligible: true, targetId: movie.requested.targetId });
  expect(preview.candidates[0]!.reclaimableBytes).toBe((await fs.stat(movie.canonical)).blocks * 512);
  expect(preview.candidates[0]!.reclaimableBytes).not.toBe(5000);
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
  f.setAvailable(DEFAULT_RETENTION_POLICY.minimumFreeBytes + 1);
  const job = f.repo.createJob({ type: "retention_cleanup", payload: {} });
  const calls = f.watchCalls, rpc = f.rpcCalls.length, changes = f.db.query("SELECT total_changes() AS n").get();
  await runRetentionCleanup({ ...f.context, execution: f.execution }, job, f.adapters);
  expect(f.watchCalls).toBe(calls); expect(f.rpcCalls).toHaveLength(rpc);
  expect(f.repo.retention.evictions(job.id)).toEqual([]);
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(changes);
  expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
});

test("eviction intent, selection retirement and outbox roll back together on enqueue failure", async () => {
  await using f = await fixture(); const movie = await f.add(1);
  const runtime = await createJobExecutionRuntime(f.context, path.join(f.root, "execution.sqlite"), { processJob: async () => "done" });
  await runtime.stop();
  f.db.exec("CREATE TRIGGER reject_retention_job BEFORE INSERT ON job_commands BEGIN SELECT RAISE(ABORT,'fixture outbox unavailable'); END");
  await expect(queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Operator cleanup" }, f.adapters)).rejects.toThrow("outbox unavailable");
  expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", revision: 1 });
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
  expect(f.db.query("SELECT COUNT(*) AS n FROM retention_evictions").get()).toEqual({ n: 0 });
  expect((await fs.stat(movie.canonical)).size).toBe(5000);
});

test("pressure ranks inspected low-watch/old-watch targets, preserves seeds and measures free space before stopping", async () => {
  await using f = await fixture();
  const popular = await f.add(1, 8, 300), newer = await f.add(2, 1, 90), oldest = await f.add(3, 1, 300);
  f.adapters.afterCheckpoint = async (name) => { if (name === "file-unlinked") f.setAvailable(DEFAULT_RETENTION_POLICY.minimumFreeBytes + 123); };
  const job = f.repo.createJob({ type: "retention_cleanup", payload: {} });
  await runRetentionCleanup({ ...f.context, execution: f.execution }, job, f.adapters);
  const plans = f.repo.retention.evictions(job.id);
  expect(plans).toHaveLength(1); expect(plans[0]!.plan.targetId).toBe(oldest.requested.targetId);
  expect(plans[0]!.result).toMatchObject({ observedAvailableDeltaBytes: DEFAULT_RETENTION_POLICY.minimumFreeBytes + 123, attributedFreedBytes: null });
  await expect(fs.lstat(oldest.canonical)).rejects.toMatchObject({ code: "ENOENT" });
  expect(f.repo.retention.get(oldest.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "missing" });
  expect(f.repo.movies.target(oldest.requested.targetId)!.active_version_id).toBeNull();
  expect(f.repo.movies.version(oldest.versionId)).not.toBeNull();
  for (const movie of [popular, newer, oldest]) expect((await fs.stat(movie.seed)).size).toBe(5000);
  for (const movie of [popular, newer]) expect((await fs.stat(movie.canonical)).size).toBe(5000);
  expect(f.rpcCalls.every((method) => method === "torrent-get")).toBe(true);
});

test.each(["fresh", "untracked", "unwatched", "unknown-watch", "active-playback", "unknown-playback", "unknown-allocation", "seed-unverified", "hardlink", "active-acquisition"])("%s evidence blocks both preview eligibility and operator deletion", async (kind) => {
  await using f = await fixture(); const movie = await f.add(1);
  const watch = f.watches.get(1)!;
  if (kind === "fresh") f.db.query("UPDATE retention_events SET created_at=? WHERE namespace='movie-target' AND resource_id=?").run(new Date().toISOString(), movie.key.id);
  if (kind === "untracked") { f.db.query("DELETE FROM retention_events WHERE resource_id=?").run(movie.key.id); f.db.query("DELETE FROM retention_resources WHERE resource_id=?").run(movie.key.id); }
  if (kind === "unwatched") { watch.state = "unwatched"; watch.playCount = 0; }
  if (kind === "unknown-watch") watch.coverage = "unknown";
  if (kind === "active-playback") watch.activePlayback = "active";
  if (kind === "unknown-playback") watch.activePlayback = "unknown";
  if (kind === "unknown-allocation") f.adapters.reclaimableAllocation = async () => null;
  if (kind === "seed-unverified") f.torrents.clear();
  if (kind === "hardlink") await fs.link(movie.canonical, path.join(f.root, "foreign-link.mkv"));
  if (kind === "active-acquisition") f.repo.movies.startAcquisition(movie.requested.targetId, "upgrade", () => f.repo.createJob({ type: "metadata_hydration" }).id);
  const inspected = await inspectRetentionCandidate(f.repo, movie.requested.targetId, f.adapters);
  expect(inspected.candidate.eligible).toBe(false);
  await expect(queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Operator cleanup" }, f.adapters)).rejects.toThrow("blocked");
  expect((await fs.stat(movie.canonical)).size).toBe(5000);
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
});

test.each(["re-request", "replacement", "sharing", "playback"])("fresh %s after intent retirement fences execution without claiming missing bytes", async (kind) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Operator cleanup" }, f.adapters);
  expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "unknown" });
  f.adapters.afterCheckpoint = async (name) => {
    if (name !== "view-retired") return;
    if (kind === "re-request") f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    if (kind === "replacement") { await fs.unlink(movie.canonical); await fs.writeFile(movie.canonical, Buffer.alloc(5000, 99)); }
    if (kind === "sharing") await fs.link(movie.canonical, path.join(f.root, "new-shared-link.mkv"));
    if (kind === "playback") f.watches.get(1)!.activePlayback = "active";
  };
  const run = runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters);
  if (kind === "re-request") await expect(run).resolves.toBe("done");
  else await expect(run).rejects.toThrow();
  const plan = f.repo.retention.eviction(queued.evictionId)!;
  if (kind === "re-request") {
    expect(plan.result).toMatchObject({ outcome: "superseded", selectionRestored: true });
    expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
  } else expect(plan.finishedAt).toBeNull();
  expect(f.repo.retention.get(movie.key)!.actualPresence).not.toBe("missing");
  expect((await fs.stat(movie.seed)).size).toBe(5000);
});

test.each(["staging-created", "staging-initialized", "view-retired", "file-staged", "file-unlinked"])("real Reflow/SQLite restart recovers eviction after %s without deleting seed storage or replaying intent", async (checkpoint) => {
  await using f = await fixture(); const movie = await f.add(1, 1, 180, 2);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Crash acceptance" }, f.adapters);
  f.db.query("UPDATE jobs SET max_attempts=1 WHERE id=?").run(queued.jobId);
  let once = true;
  f.adapters.afterCheckpoint = async (name) => {
    expect(f.context.activeLocks?.has("*")).toBe(true);
    if (once && name === checkpoint) { once = false; throw new Error("fixture execution interrupted"); }
  };
  const executionPath = path.join(f.root, "execution.sqlite");
  const start = () => createJobExecutionRuntime(f.context, executionPath, { retryDelayMs: 1, processJob: (ctx, job) => runRetentionCleanup(ctx, job, f.adapters) });
  let runtime = await start();
  const until = async (status: "failed" | "succeeded") => {
    const deadline = Date.now() + 8000;
    while (f.repo.getJob(queued.jobId)?.status !== status && Date.now() < deadline) { await runtime.reconcile(); await Bun.sleep(10); }
    expect(f.repo.getJob(queued.jobId)?.status).toBe(status);
  };
  try {
    await until("failed");
    expect(f.repo.getJob(queued.jobId)?.error).toContain("fixture execution interrupted");
    expect(f.repo.retention.get(movie.key)!.actualPresence).not.toBe("missing");
    const runId = f.repo.getJobExecution(queued.jobId)!.run_id;
    const interrupted = await runtime.engine.getRunStatus(runId);
    for (const file of movie.files)
      expect(interrupted!.steps.find((step) => step.name === `retention:${queued.evictionId}:proof:${file.fileId}`)?.status).toBe("completed");
    if (checkpoint.startsWith("staging-")) {
      await expect(fs.lstat(path.join(f.contentRoot, ".podible-retention"))).rejects.toMatchObject({ code: "ENOENT" });
      expect((await fs.readdir(f.contentRoot)).some((name) => name.startsWith(".podible-retention-init-"))).toBe(true);
    }
    await runtime.stop(); await f.reopen();
    f.repo.retryJob(queued.jobId); runtime = await start(); await runtime.reconcile();
    await until("succeeded");
    expect(f.repo.retention.eviction(queued.evictionId)!.finishedAt).not.toBeNull();
    expect(f.repo.retention.evictions(queued.jobId)).toHaveLength(1);
    expect(f.repo.retention.events(movie.key).filter((event) => event.action === "evicted")).toHaveLength(1);
    expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "missing" });
    for (const file of movie.files) await expect(fs.lstat(file.canonical)).rejects.toMatchObject({ code: "ENOENT" });
    const completed = await runtime.engine.getRunStatus(runId);
    for (const file of movie.files)
      expect(completed!.steps.find((step) => step.name === `retention:${queued.evictionId}:proof:${file.fileId}`)?.attempts).toBe(1);
    expect((await fs.stat(movie.seed)).size).toBe(5000);
    expect(f.repo.movies.version(movie.versionId)).not.toBeNull();
  } finally { await runtime.stop(); }
}, 20_000);

test.each(["file-staged", "file-unlinked"])("new demand at %s restores proven bytes or preserves honest missing recovery", async (checkpoint) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Demand race" }, f.adapters);
  f.adapters.afterCheckpoint = async (name) => {
    if (name === checkpoint) f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
  };
  await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).resolves.toBe("done");
  const restored = checkpoint === "file-staged", result = f.repo.retention.eviction(queued.evictionId)!;
  expect(result.finishedAt).not.toBeNull();
  expect(result.result).toMatchObject({ outcome: "superseded", selectionRestored: restored,
    verifiedPresentObjectIds: restored ? [movie.fileId] : [], verifiedAbsentObjectIds: restored ? [] : [movie.fileId] });
  expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", actualPresence: restored ? "present" : "missing" });
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(restored ? movie.versionId : null);
  expect(f.repo.retention.retiredFileIds(movie.requested.targetId)).toEqual(restored ? [] : [movie.fileId]);
  if (restored) expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
  else await expect(fs.lstat(movie.canonical)).rejects.toMatchObject({ code: "ENOENT" });
  await expect(fs.lstat(path.join(f.contentRoot, ".podible-retention", `${queued.evictionId}-${movie.fileId}.mkv`))).rejects.toMatchObject({ code: "ENOENT" });
});

test.each(["before-start", "between-proofs"])("new demand %s restores selection without deleting still-canonical members", async (boundary) => {
  await using f = await fixture(); const movie = await f.add(1, 1, 180, 2);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Proof demand race" }, f.adapters);
  if (boundary === "before-start") f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
  const execution: NonNullable<WorkerContext["execution"]> = { ...f.execution, run: async <T>(name: string, action: () => T | Promise<T>) => {
    const result = await f.execution.run(name, action);
    if (boundary === "between-proofs" && name === `retention:${queued.evictionId}:proof:${movie.files[0]!.fileId}`)
      f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    return result;
  } };
  await expect(runRetentionCleanup({ ...f.context, execution }, f.repo.getJob(queued.jobId)!, f.adapters)).resolves.toBe("done");
  expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", actualPresence: "present" });
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
  expect(f.repo.retention.eviction(queued.evictionId)!.result).toMatchObject({ outcome: "superseded", verifiedAbsentObjectIds: [] });
  for (const file of movie.files) expect((await fs.stat(file.canonical)).size).toBe(5000);
});

test("cleanup without durable execution cannot commit eviction intent", async () => {
  await using f = await fixture(); const movie = await f.add(1);
  const job = f.repo.createJob({ type: "retention_cleanup", payload: {} }), before = f.db.query("SELECT total_changes() AS n").get();
  await expect(runRetentionCleanup(f.context, job, f.adapters)).rejects.toThrow("requires durable execution");
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", revision: 1 });
  expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(movie.versionId);
});

test.each(["file-staged", "file-unlinked", "file-restored-linked", "demand-superseded"])("real Reflow restart settles newer demand after %s", async (checkpoint) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Demand restart" }, f.adapters);
  f.db.query("UPDATE jobs SET max_attempts=1 WHERE id=?").run(queued.jobId);
  let interrupted = false;
  f.adapters.afterCheckpoint = async (name) => {
    if (checkpoint === "file-restored-linked" || checkpoint === "demand-superseded") {
      if (name === "file-staged") f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    }
    if (!interrupted && name === checkpoint) { interrupted = true; throw new Error("demand restart interrupted"); }
  };
  const start = () => createJobExecutionRuntime(f.context, path.join(f.root, "execution.sqlite"), {
    retryDelayMs: 1, processJob: (ctx, job) => runRetentionCleanup(ctx, job, f.adapters),
  });
  let runtime = await start();
  const until = async (status: "failed" | "succeeded") => {
    const deadline = Date.now() + 8000;
    while (f.repo.getJob(queued.jobId)?.status !== status && Date.now() < deadline) { await runtime.reconcile(); await Bun.sleep(10); }
    expect(f.repo.getJob(queued.jobId)?.status).toBe(status);
  };
  try {
    await until("failed");
    expect(f.repo.getJob(queued.jobId)!.error).toContain("demand restart interrupted");
    await runtime.stop(); await f.reopen();
    f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    f.repo.retryJob(queued.jobId); runtime = await start(); await until("succeeded");
    const present = checkpoint !== "file-unlinked";
    expect(f.repo.retention.eviction(queued.evictionId)!.result).toMatchObject({ outcome: "superseded" });
    expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", actualPresence: present ? "present" : "missing" });
    expect(f.repo.movies.target(movie.requested.targetId)!.active_version_id).toBe(present ? movie.versionId : null);
    if (present) {
      expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
      expect((await fs.stat(movie.canonical)).nlink).toBe(1);
      const manifest = JSON.parse(await fs.readFile(path.join(f.roots.viewRoot, ".podible-view.json"), "utf8"));
      expect(JSON.stringify(manifest)).toContain(movie.relativePath);
    }
    await expect(fs.lstat(path.join(f.contentRoot, ".podible-retention", `${queued.evictionId}-${movie.fileId}.mkv`))).rejects.toMatchObject({ code: "ENOENT" });
  } finally { await runtime.stop(); }
}, 20_000);

test.each(["file-staged", "file-unlinked", "demand-superseded"])("pressure job restart resumes its existing plan after %s", async (checkpoint) => {
  await using f = await fixture(); const movie = await f.add(1);
  const job = f.repo.createJob({ type: "retention_cleanup", payload: {}, maxAttempts: 1 });
  let interrupted = false;
  f.adapters.afterCheckpoint = async (name) => {
    if (name !== "intent-committed") expect(f.context.activeLocks?.has("*")).toBe(true);
    if (checkpoint === "demand-superseded" && name === "file-staged") f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    if (!interrupted && name === checkpoint) { interrupted = true; throw new Error("pressure restart interrupted"); }
  };
  const start = () => createJobExecutionRuntime(f.context, path.join(f.root, "execution.sqlite"), {
    retryDelayMs: 1, processJob: (ctx, row) => runRetentionCleanup(ctx, row, f.adapters),
  });
  let runtime = await start();
  const until = async (status: "failed" | "succeeded") => {
    const deadline = Date.now() + 8000;
    while (f.repo.getJob(job.id)?.status !== status && Date.now() < deadline) { await runtime.reconcile(); await Bun.sleep(10); }
    expect(f.repo.getJob(job.id)?.status).toBe(status);
  };
  try {
    await until("failed");
    expect(f.repo.getJob(job.id)!.error).toContain("pressure restart interrupted");
    const plan = f.repo.retention.evictions(job.id)[0]!;
    expect(plan).toBeDefined();
    await runtime.stop(); await f.reopen();
    f.repo.retryJob(job.id); runtime = await start(); await until("succeeded");
    expect(f.repo.retention.evictions(job.id)).toHaveLength(1);
    expect(f.repo.retention.eviction(plan.id)!.finishedAt).not.toBeNull();
    expect(f.repo.retention.events(movie.key).filter((event) => event.action === "evicted")).toHaveLength(1);
    if (checkpoint === "demand-superseded") {
      expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", actualPresence: "present" });
      expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
      expect(await fs.readFile(path.join(f.roots.viewRoot, ".podible-view.json"), "utf8")).toContain(movie.relativePath);
    } else {
      expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "missing" });
      await expect(fs.lstat(movie.canonical)).rejects.toMatchObject({ code: "ENOENT" });
    }
    expect((await fs.stat(movie.seed)).size).toBe(5000);
  } finally { await runtime.stop(); }
}, 20_000);

test.each(["bytes", "inode", "hardlink", "proof", "missing-proof", "mount", "seed"])("staged %s corruption is blocked without unlink or false completion", async (kind) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Corruption fence" }, f.adapters);
  f.adapters.afterCheckpoint = async (name) => { if (name === "file-staged") throw new Error("staged fixture interrupted"); };
  await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).rejects.toThrow("staged fixture interrupted");
  const staged = path.join(f.contentRoot, ".podible-retention", `${queued.evictionId}-${movie.fileId}.mkv`);
  if (kind === "bytes") await fs.writeFile(staged, Buffer.alloc(5000, 99));
  if (kind === "inode") { const original = await fs.readFile(staged); await fs.rename(staged, `${staged}.original`); await fs.writeFile(staged, original); }
  if (kind === "hardlink") await fs.link(staged, path.join(f.root, "foreign-link.mkv"));
  if (kind === "proof") f.saved.set(`retention:${queued.evictionId}:proof:${movie.fileId}`, { sha256: "0".repeat(64) });
  if (kind === "missing-proof") f.saved.delete(`retention:${queued.evictionId}:proof:${movie.fileId}`);
  if (kind === "mount") await fs.writeFile(path.join(f.contentRoot, ".podible-content.json"), JSON.stringify({ id: randomUUID() }));
  if (kind === "seed") await fs.writeFile(movie.seed, Buffer.alloc(5000, 99));
  delete f.adapters.afterCheckpoint;
  await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).rejects.toThrow();
  expect((await fs.stat(staged)).size).toBe(5000);
  expect((await fs.stat(movie.seed)).size).toBe(5000);
  expect(f.repo.retention.eviction(queued.evictionId)!.finishedAt).toBeNull();
  expect(f.repo.retention.get(movie.key)!.actualPresence).not.toBe("missing");
});

test("supersession never overwrites a new canonical file or restores corrupted staging", async () => {
  for (const kind of ["canonical", "staged"]) {
    await using f = await fixture(); const movie = await f.add(1);
    const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Restore conflict" }, f.adapters);
    const staged = path.join(f.contentRoot, ".podible-retention", `${queued.evictionId}-${movie.fileId}.mkv`);
    f.adapters.afterCheckpoint = async (name) => {
      if (name !== "file-staged") return;
      await fs.writeFile(kind === "canonical" ? movie.canonical : staged, Buffer.alloc(5000, 99));
      f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    };
    await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).rejects.toThrow();
    expect((await fs.readFile(kind === "canonical" ? movie.canonical : staged))[0]).toBe(99);
    expect((await fs.stat(staged)).size).toBe(5000);
    expect(f.repo.retention.get(movie.key)!.desiredPresence).toBe("present");
    expect(f.repo.retention.eviction(queued.evictionId)!.finishedAt).toBeNull();
  }
});

test.each(["unmarked", "wrong-owner", "symlink"])("foreign %s staging is never adopted or removed", async (kind) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Staging ownership" }, f.adapters);
  const staging = path.join(f.contentRoot, ".podible-retention");
  if (kind === "symlink") await fs.symlink(f.root, staging);
  else {
    await fs.mkdir(staging);
    if (kind === "wrong-owner") await fs.writeFile(path.join(staging, ".owner.json"), JSON.stringify({ contentId: randomUUID() }));
  }
  await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).rejects.toThrow();
  expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
  expect(f.repo.retention.eviction(queued.evictionId)!.finishedAt).toBeNull();
  expect((await fs.lstat(staging)).isSymbolicLink()).toBe(kind === "symlink");
});

test.each([1, 2])("request immediately before filesystem fence %d prevents stale destructive intent", async (boundary) => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Writer fence" }, f.adapters);
  const original = RetentionRepo.prototype.withEvictionFence;
  let calls = 0;
  const fence = spyOn(RetentionRepo.prototype, "withEvictionFence").mockImplementation(function <T>(this: RetentionRepo, id: string, effect: () => T): T {
    if (++calls === boundary) f.repo.movies.request(f.user.id, movie.movie, defaultMovieQualityPolicy);
    return original.call(this, id, effect) as T;
  });
  try {
    await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).resolves.toBe("done");
    expect(await fs.readFile(movie.canonical)).toEqual(await fs.readFile(movie.seed));
    expect(f.repo.retention.get(movie.key)).toMatchObject({ desiredPresence: "present", actualPresence: "present" });
    expect(f.repo.retention.eviction(queued.evictionId)!.result).toMatchObject({ outcome: "superseded" });
  } finally { fence.mockRestore(); }
});

test("playback starting during final seed hashing is re-observed before unlink", async () => {
  await using f = await fixture(); const movie = await f.add(1);
  const queued = await queueMovieEviction(f.repo, { targetId: movie.requested.targetId, expectedRevision: 1, reason: "Playback freshness" }, f.adapters);
  let staged = false;
  f.adapters.afterCheckpoint = async (name) => { if (name === "file-staged") staged = true; };
  const original = RetentionService.prototype.seedEvidence;
  const hashing = spyOn(RetentionService.prototype, "seedEvidence").mockImplementation(async function (this: RetentionService, targetId, root, objects) {
    const result = await original.call(this, targetId, root, objects);
    if (staged && objects.some((object) => object.sha256)) f.watches.get(1)!.activePlayback = "active";
    return result;
  });
  try {
    await expect(runRetentionCleanup({ ...f.context, execution: f.execution }, f.repo.getJob(queued.jobId)!, f.adapters)).rejects.toThrow("Fresh post-hash watch or playback");
    expect((await fs.stat(path.join(f.contentRoot, ".podible-retention", `${queued.evictionId}-${movie.fileId}.mkv`))).size).toBe(5000);
    expect(f.repo.retention.eviction(queued.evictionId)!.finishedAt).toBeNull();
    expect(f.repo.retention.get(movie.key)!.actualPresence).not.toBe("missing");
    expect((await fs.stat(movie.seed)).size).toBe(5000);
  } finally { hashing.mockRestore(); }
});
