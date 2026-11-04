import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { prepareMovieStorage } from "../../src/movies/storage";
import { reconcileTvView, tvRoots, tvViewEntries } from "../../src/tv/publication";
import { TvRetentionService, type TvRetentionAdapters } from "../../src/media/tv-retention";
import { tvRetentionAuditSchema } from "../../src/media/tv-retention-types";
import { createTvRetentionPlexReader } from "../../src/media/tv-retention-plex";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createTvRetentionRouter } from "../../src/rpc/tv-retention-router";
import { executeMethod } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";

const clientId = `transmission:${"b".repeat(8)}-${"b".repeat(4)}-${"b".repeat(4)}-${"b".repeat(4)}-${"b".repeat(12)}`;
const torrentHash = "a".repeat(40);
const catalog = { key: "tvdb:700", tvdbId: 700, tmdbId: 1700, title: "Whole Season", aliases: [], year: 2020, anime: false, relations: [],
  episodes: [1, 2, 3, 4].map((id) => ({ key: `tvdb:${7000 + id}`, title: `Episode ${id}`, airDate: "2020-01-01", runtimeMinutes: 24,
    numbering: [{ scheme: "tvdb:aired", season: id === 4 ? 2 : 1, episode: id === 4 ? 1 : id, absolute: null }],
    refs: [{ provider: "tvdb" as const, id: String(7000 + id), source: "fixture" }] })),
};

async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 8000;
  while (!(await check())) { if (Date.now() > deadline) throw new Error("TV retention acceptance timed out"); await Bun.sleep(10); }
}

async function fixture(options: { identicalMembers?: boolean } = {}) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-retention-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.managedRoot = path.join(root, "tv"); settings.tv.plexViewPath = "/server/tv"; settings.tv.plexSectionId = "3";
  settings.movies.plexUrl = "http://plex.fixture.invalid"; settings.movies.plexToken = "private-plex-token"; repo.updateSettings(settings);
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath); await prepareMovieStorage(repo, roots.contentRoot);
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-retention", username: "admin", isAdmin: true });
  const request = repo.tv.request(user.id, catalog, defaultMovieQualityPolicy, catalog.episodes.slice(0, 3).map((episode) => episode.key));
  const key = { namespace: "tv-target", id: String(request.targetId) };
  repo.retention.request(key, { expectedRevision: 0, reason: "Explicit complete season request" });
  const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id, request.requestId).acquisition;
  const seedDir = path.join(settings.tv.managedRoot, "downloads", "season-pack"); await fs.mkdir(seedDir, { recursive: true });
  const files: { id: string; relativePath: string; sizeBytes: number; probe: Record<string, unknown>; episodeIds: number[];
    canonical: string; seed: string; name: string; bytes: Buffer<ArrayBuffer> }[] = [];
  for (const [index, episodeId] of request.episodeIds.entries()) {
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`, canonical = path.join(roots.contentRoot, relativePath);
    const bytes = Buffer.alloc(8192, options.identicalMembers ? 65 : index + 65), name = `member-${index + 1}.mkv`, seed = path.join(seedDir, name);
    await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, bytes); await fs.copyFile(canonical, seed);
    files.push({ id, relativePath, sizeBytes: bytes.length, probe: {}, episodeIds: [episodeId], canonical, seed, name, bytes });
  }
  const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, files.map(({ id, relativePath, sizeBytes, probe, episodeIds }) => ({ id, relativePath, sizeBytes, probe, episodeIds })));
  repo.tv.stagePublication(request.targetId, versionId, 0); repo.tv.promotePublication(request.targetId, versionId, 0); repo.tv.clearPublication(request.targetId, versionId);
  repo.tv.finishAcquisition(acquisition.id);
  repo.tv.bindDownload(acquisition.id, { clientId, nativeId: torrentHash }, { rootId: "tv", relativePath: "season-pack" }, "verified-season-pack");
  await reconcileTvView(repo.tv, roots);
  const views = tvViewEntries(repo.tv, roots).map((entry) => path.join(roots.viewRoot, entry.relativePath));
  const old = new Date(Date.now() - 90 * 86_400_000).toISOString();
  db.query("UPDATE tv_requests SET created_at=?").run(old); db.query("UPDATE retention_events SET created_at=?").run(old);
  const scope = { targetId: request.targetId, scheme: "tvdb:aired", seasonNumber: 1 };
  const requests: string[] = [], rpcCalls: string[] = [], logs: string[] = [];
  const playback: { active: boolean; unwatched: Set<string>; wrongIds: boolean } = { active: false, unwatched: new Set(), wrongIds: false };
  const page = (Metadata: unknown[]) => Response.json({ MediaContainer: { size: Metadata.length, offset: 0, totalSize: Metadata.length, Metadata } });
  const event = (ratingKey: string, id: number, accountID = "1") => ({ historyKey: `/status/sessions/history/${id}`, ratingKey, librarySectionID: "3", type: "episode", accountID, viewedAt: 1_700_000_000 });
  const watch = createTvRetentionPlexReader({ fetcher: (async (input, init) => {
    expect(init?.method).toBe("GET"); expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("private-plex-token");
    const url = new URL(String(input)); requests.push(url.pathname);
    if (url.pathname === "/library/sections") return Response.json({ MediaContainer: { size: 1, Directory: [{ key: "3", type: "show" }] } });
    if (url.pathname === "/library/all") return page([{ ratingKey: "10", librarySectionID: "3", type: "show", guid: "plex://show/exact", Guid: [{ id: "tvdb://700" }] }]);
    if (url.pathname === "/library/metadata/10/allLeaves") return page([1, 2, 3].map((id) => ({ ratingKey: String(100 + id), grandparentRatingKey: "10", librarySectionID: "3",
      type: "episode", parentIndex: 1, index: id, Guid: [{ id: `tvdb://${playback.wrongIds ? 9900 + id : 7000 + id}` }] })));
    if (url.pathname === "/status/sessions/history/all") {
      const ratingKey = url.searchParams.get("metadataItemID");
      return ratingKey ? page(playback.unwatched.has(ratingKey) ? [] : [event(ratingKey, Number(ratingKey) + 1000)]) : page([event("101", 1001), event("102", 1002, "2")]);
    }
    if (url.pathname === "/status/sessions") return Response.json({ MediaContainer: { size: playback.active ? 1 : 0,
      Metadata: playback.active ? [{ type: "episode", ratingKey: "101", grandparentRatingKey: "10", Guid: [{ id: "tvdb://7001" }] }] : [] } });
    throw new Error("Unexpected Plex route");
  }) as typeof fetch });
  const engine = { label: "Podible TV", complete: true, seconds: 0, hash: torrentHash };
  const adapters: TvRetentionAdapters = {
    membership: async () => ({ complete: true, fullyAired: true, workKey: catalog.key, source: "fixture:complete-season",
      episodeKeys: catalog.episodes.slice(0, 3).map((episode) => episode.key) }),
    watchHistory: watch, reclaimableAllocation: async (file) => file.allocatedBytes,
    transmission: { clientId, rpc: { async call(method, args) {
      rpcCalls.push(method); expect(method).toBe("torrent-get"); expect(args?.ids).toEqual([torrentHash]); expect(args?.fields).not.toContain("addedDate");
      return { torrents: [{ hashString: engine.hash, downloadDir: seedDir, labels: [engine.label],
        files: files.map((file) => ({ name: file.name, length: file.sizeBytes, bytesCompleted: engine.complete ? file.sizeBytes : 0 })),
        leftUntilDone: engine.complete ? 0 : 1, secondsSeeding: engine.seconds, error: 0, status: 6 }] };
    } } },
  };
  const service = () => new TvRetentionService(repo, adapters);
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  const start = async () => runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), tvRetentionAdapters: adapters,
    onLog: (message) => { logs.push(message); } }, path.join(root, "execution.sqlite"), { retryDelayMs: 1 });
  const stop = async () => { await runtime?.stop(); runtime = undefined; };
  const queue = async (operationId = randomUUID()) => {
    const preview = await service().audit(scope);
    expect(preview.blockers).toEqual([]); expect(preview.executionBlockers).toEqual([]); expect(preview.eligible).toBe(true);
    return service().evict({ ...scope, expectedRevision: preview.revision, operationId, reason: "Explicit whole-season fixture eviction" });
  };
  const wait = async (jobId: number, status: "succeeded" | "failed") => until(async () => {
    await runtime!.reconcile(); return repo.getJob(jobId)?.status === status;
  });
  return { root, db, repo, settings, roots, user, request, key, files, views, scope, adapters, engine, playback, rpcCalls, requests, logs, service, queue, start, stop, wait,
    async [Symbol.asyncDispose]() { await stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}
const codes = (result: { blockers: { code: string }[] }) => result.blockers.map((blocker) => blocker.code);

test("whole-season audit uses exact global episode history and real independent seed bytes without writing", async () => {
  await using f = await fixture(); const changes = f.db.query("SELECT total_changes() AS n").get();
  const result = await f.service().audit(f.scope);
  expect(tvRetentionAuditSchema.parse(result)).toEqual(result);
  expect(result.blockers).toEqual([]); expect(result.executionBlockers).toEqual([]); expect(result.eligible).toBe(true);
  expect(result.watch.episodes).toHaveLength(3); expect(result.watch.coverage).toBe("global-all-users");
  expect(result.seeding.state).toBe("verified-independent-seed-store"); expect(result.seeding.obligations[0]!.verifiedSecondsSeeded).toBe(0);
  expect(result.planReclaimableBytes).toBeGreaterThan(0); expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(changes);
  expect(JSON.stringify(result)).not.toContain("private-plex-token"); expect(f.rpcCalls.every((method) => method === "torrent-get")).toBe(true);
});

test("protected TV pins block explicit audit/eviction and the queued plan fence independently", async () => {
  await using f = await fixture();
  const settings = f.repo.readSettings()!; settings.retention.protectedTvTargetIds = [f.scope.targetId]; f.repo.updateSettings(settings);
  const audit = await f.service().audit(f.scope);
  expect(audit.eligible).toBe(false); expect(codes(audit)).toContain("protected-resource");
  const blocked = await f.service().evict({ ...f.scope, expectedRevision: audit.revision, operationId: randomUUID(), reason: "Pins must win" });
  expect(blocked.status).toBe("blocked"); expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toHaveLength(3);
  settings.retention.protectedTvTargetIds = []; f.repo.updateSettings(settings);
  const queued = await f.queue();
  settings.retention.protectedTvTargetIds = [f.scope.targetId]; f.repo.updateSettings(settings);
  expect(() => f.repo.tvRetention.assert(queued.evictionId!)).toThrow("protected");
  for (const file of f.files) expect(await fs.readFile(file.canonical)).toEqual(file.bytes);
});

for (const mode of ["watched-subset", "wrong-episode-ids", "playback", "bad-seed-label", "wrong-seed-hash", "incomplete-seed", "corrupt-seed", "hash-budget", "loss", "missing-mount", "partial-request", "multi-season"] as const) {
  test(`whole-season removal blocks ${mode} without changing requests, selection or bytes`, async () => {
    await using f = await fixture();
    if (mode === "watched-subset") f.playback.unwatched.add("103");
    if (mode === "wrong-episode-ids") f.playback.wrongIds = true;
    if (mode === "playback") f.playback.active = true;
    if (mode === "bad-seed-label") f.engine.label = "Podible TV foreign";
    if (mode === "wrong-seed-hash") f.engine.hash = "c".repeat(40);
    if (mode === "incomplete-seed") f.engine.complete = false;
    if (mode === "corrupt-seed") await fs.writeFile(f.files[0]!.seed, Buffer.alloc(8192, 90));
    if (mode === "hash-budget") f.adapters.maxHashBytes = 0;
    if (mode === "loss") f.db.query("INSERT INTO media_content_losses(file_id,namespace,target_id,operation_id,observed_at) VALUES(?,'tv-target',?,?,?)")
      .run(f.files[0]!.id, f.scope.targetId, randomUUID(), new Date().toISOString());
    if (mode === "missing-mount") await fs.unlink(path.join(f.roots.contentRoot, ".podible-content.json"));
    if (mode === "partial-request") f.db.query("DELETE FROM tv_request_episodes WHERE request_id=? AND episode_id=?").run(f.request.requestId, f.request.episodeIds[2]!);
    if (mode === "multi-season") f.repo.tv.request(f.user.id, catalog, defaultMovieQualityPolicy, [catalog.episodes[3]!.key]);
    const selected = f.repo.tv.activeEpisodes(f.scope.targetId), before = f.repo.retention.get(f.key), result = await f.service().audit(f.scope);
    expect(result.eligible).toBe(false);
    const evicted = await f.service().evict({ ...f.scope, operationId: randomUUID(), expectedRevision: result.revision, reason: "Must be blocked" });
    expect(evicted.status).toBe("blocked"); expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toEqual(selected); expect(f.repo.retention.get(f.key)).toEqual(before);
    expect(f.repo.listJobsByType("tv_retention_cleanup")).toHaveLength(0);
    for (const file of f.files) expect(await fs.readFile(file.canonical)).toEqual(file.bytes);
    if (mode === "watched-subset") expect(codes(result)).toContain("whole-season-watch-unverified");
    if (mode === "loss") expect(codes(result)).toContain("content-loss");
    if (mode === "multi-season") expect(result.executionBlockers.map((blocker) => blocker.code)).toContain("multi-season-target-unsupported");
  });
}

test("durable TV queue rolls back intent, selection, job/outbox and plan atomically, then exact retry deduplicates", async () => {
  await using f = await fixture(); await f.start(); await f.stop();
  const preview = await f.service().audit(f.scope), selected = f.repo.tv.activeEpisodes(f.scope.targetId), intent = f.repo.retention.get(f.key);
  const command = { ...f.scope, expectedRevision: preview.revision, operationId: randomUUID(), reason: "Atomic fixture eviction" };
  f.db.exec("CREATE TRIGGER reject_tv_eviction BEFORE INSERT ON job_commands BEGIN SELECT RAISE(ABORT,'injected TV outbox outage'); END");
  await expect(f.service().evict(command)).rejects.toThrow("injected TV outbox outage");
  expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toEqual(selected); expect(f.repo.retention.get(f.key)).toEqual(intent);
  expect(f.repo.tvRetention.eviction(command.operationId)).toBeNull(); expect(f.repo.listJobsByType("tv_retention_cleanup")).toHaveLength(0);
  f.db.exec("DROP TRIGGER reject_tv_eviction");
  const queued = await f.service().evict(command);
  expect(queued.status).toBe("queued"); expect(await f.service().evict(command)).toEqual(queued);
  expect(f.repo.listJobsByType("tv_retention_cleanup")).toHaveLength(1); expect(f.repo.listJobCommands()).toHaveLength(1);
  expect(f.repo.retention.get(f.key)!.desiredPresence).toBe("evicted"); expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toEqual([]);
});

for (const checkpoint of ["audit-complete", "queue-file-inspected"] as const) {
  for (const change of ["re-request", "another-season"] as const) {
    test(`${change} after ${checkpoint} cannot be blessed by fresh queue revisions`, async () => {
      await using f = await fixture();
      const preview = await f.service().audit(f.scope), selection = f.repo.tv.activeEpisodes(f.scope.targetId);
      let changed = false, desiredRevision = f.repo.retention.get(f.key)!.revision;
      f.adapters.afterCheckpoint = (name) => {
        if (name !== checkpoint || changed) return;
        changed = true;
        if (change === "re-request") desiredRevision = f.repo.retention.request(f.key, {
          expectedRevision: desiredRevision, reason: "Explicit re-request while eviction was preparing" }).revision;
        else f.repo.tv.request(f.user.id, catalog, defaultMovieQualityPolicy, [catalog.episodes[3]!.key]);
      };
      const result = await f.service().evict({ ...f.scope, expectedRevision: preview.revision, operationId: randomUUID(), reason: "Stale audit must not retire new demand" });
      expect(changed).toBe(true); expect(result.status).toBe("blocked"); expect(result.blockers.map((blocker) => blocker.code)).toContain("stale-revision");
      expect(f.repo.retention.get(f.key)).toMatchObject({ desiredPresence: "present", revision: desiredRevision });
      expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toEqual(selection); expect(f.repo.listJobsByType("tv_retention_cleanup")).toHaveLength(0);
      for (const file of f.files) expect(await fs.readFile(file.canonical)).toEqual(file.bytes);
    });
  }
}

test("real Reflow hashes only the current file's exact seed members: linear bytes, not whole-season rehashes per file", async () => {
  await using f = await fixture(); const bytes = new Map<string, number>(); let reads = 0;
  f.adapters.onHashBytes = (file, count) => { bytes.set(file, (bytes.get(file) ?? 0) + count); reads++; };
  await f.start(); const queued = await f.queue(); bytes.clear(); reads = 0;
  await f.wait(queued.jobId!, "succeeded");
  const size = f.files.reduce((sum, file) => sum + file.sizeBytes, 0);
  expect([...bytes.values()].reduce((sum, count) => sum + count, 0)).toBe(size * 4);
  expect(reads).toBe(f.files.length * 4);
  for (const file of f.files) { expect(bytes.get(file.seed)).toBe(file.sizeBytes * 2); expect(bytes.get(file.canonical)).toBe(file.sizeBytes); }
}, 15_000);

test("identical-sized identical-byte pack members still persist only one exact seed mapping per object/binding", async () => {
  await using f = await fixture({ identicalMembers: true }); let bytes = 0;
  f.adapters.onHashBytes = (_file, count) => { bytes += count; };
  await f.start(); const queued = await f.queue(); bytes = 0;
  expect(f.repo.tvRetention.eviction(queued.evictionId!)!.plan.seedProofs).toHaveLength(f.files.length);
  await f.wait(queued.jobId!, "succeeded");
  expect(bytes).toBe(f.files.reduce((sum, file) => sum + file.sizeBytes, 0) * 4);
}, 15_000);

test("real Reflow removes the complete single-season target while retaining requests, seed bytes and registrations", async () => {
  await using f = await fixture(); await f.start();
  const queued = await f.queue(); expect(queued.status).toBe("queued");
  await f.wait(queued.jobId!, "succeeded");
  const record = f.repo.tvRetention.eviction(queued.evictionId!)!;
  expect(record.finishedAt).not.toBeNull(); expect(record.result).toMatchObject({ outcome: "evicted", attributedFreedBytes: null });
  expect((record.result as { verifiedAbsentFileIds: string[] }).verifiedAbsentFileIds.sort()).toEqual(f.files.map((file) => file.id).sort());
  expect(f.repo.tv.requestById(f.request.requestId)).toMatchObject({ id: f.request.requestId, withdrawn_at: null });
  expect(f.repo.retention.get(f.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "missing" });
  for (const file of f.files) { await expect(fs.lstat(file.canonical)).rejects.toMatchObject({ code: "ENOENT" }); expect(await fs.readFile(file.seed)).toEqual(file.bytes); }
  for (const view of f.views) await expect(fs.lstat(view)).rejects.toMatchObject({ code: "ENOENT" });
  expect(f.repo.tv.downloads(f.scope.targetId)).toHaveLength(1); expect(f.rpcCalls.every((method) => method === "torrent-get")).toBe(true);
  expect(f.logs.join("\n")).not.toContain("private-plex-token");
}, 15_000);

for (const checkpoint of ["view-retired", "staging-created", "file-staged", "file-unlinked"] as const) {
  test(`real Reflow restart resumes whole-season eviction after ${checkpoint} with original durable proofs`, async () => {
    await using f = await fixture(); let interrupted = false;
    f.adapters.afterCheckpoint = (name) => { if (name === checkpoint && !interrupted) { interrupted = true; throw new Error("Injected TV retention interruption"); } };
    const runtime = await f.start(), queued = await f.queue();
    f.db.query("UPDATE jobs SET max_attempts=1 WHERE id=?").run(queued.jobId!);
    await f.wait(queued.jobId!, "failed"); expect(interrupted).toBe(true);
    const runId = f.repo.getJobExecution(queued.jobId!)!.run_id;
    const before = (await runtime.engine.getRunStatus(runId))!;
    expect(before.steps.filter((step) => step.name.includes(":proof:")).every((step) => step.status === "completed" && step.attempts === 1)).toBe(true);
    await f.stop(); delete f.adapters.afterCheckpoint; await f.start(); f.repo.retryJob(queued.jobId!);
    await f.wait(queued.jobId!, "succeeded");
    expect(f.repo.getJobExecution(queued.jobId!)!.run_id).toBe(runId); expect(f.repo.tvRetention.eviction(queued.evictionId!)!.finishedAt).not.toBeNull();
    for (const file of f.files) { await expect(fs.lstat(file.canonical)).rejects.toMatchObject({ code: "ENOENT" }); expect(await fs.readFile(file.seed)).toEqual(file.bytes); }
    expect(f.repo.listJobsByType("tv_retention_cleanup")).toHaveLength(1);
  }, 15_000);
}

for (const checkpoint of ["file-staged", "file-unlinked"] as const) {
  test(`new demand after ${checkpoint} supersedes eviction and restores every surviving healthy episode`, async () => {
    await using f = await fixture(); let requested = false;
    f.adapters.afterCheckpoint = (name) => { if (name === checkpoint && !requested) {
      requested = true; const intent = f.repo.retention.get(f.key)!;
      f.repo.retention.request(f.key, { expectedRevision: intent.revision, reason: "Explicit new user demand" });
    } };
    await f.start(); const queued = await f.queue(); await f.wait(queued.jobId!, "succeeded");
    expect(requested).toBe(true); expect(f.repo.tvRetention.eviction(queued.evictionId!)!.result).toMatchObject({ outcome: "superseded" });
    expect(f.repo.retention.get(f.key)!.desiredPresence).toBe("present");
    expect(f.repo.tv.missingEpisodes(f.request.requestId)).toHaveLength(checkpoint === "file-unlinked" ? 1 : 0);
    for (const file of f.files) expect(await fs.readFile(file.seed)).toEqual(file.bytes);
  }, 15_000);
}

test("fresh playback beginning after staging blocks unlink and leaves owned staged bytes for safe retry", async () => {
  await using f = await fixture(); f.adapters.afterCheckpoint = (name) => { if (name === "file-staged") f.playback.active = true; };
  await f.start(); const queued = await f.queue(); f.db.query("UPDATE jobs SET max_attempts=1 WHERE id=?").run(queued.jobId!);
  await f.wait(queued.jobId!, "failed"); expect(f.repo.tvRetention.eviction(queued.evictionId!)!.finishedAt).toBeNull();
  const staged = await fs.readdir(path.join(f.roots.contentRoot, ".podible-tv-retention"));
  expect(staged.some((name) => name.endsWith(".mkv"))).toBe(true);
  for (const file of f.files) expect(await fs.readFile(file.seed)).toEqual(file.bytes);
});

test("changed staging ownership after rename blocks unlink rather than adopting a foreign directory", async () => {
  await using f = await fixture();
  f.adapters.afterCheckpoint = async (name) => { if (name === "file-staged") await fs.writeFile(path.join(f.roots.contentRoot, ".podible-tv-retention", ".owner.json"), JSON.stringify({ contentId: randomUUID() })); };
  await f.start(); const queued = await f.queue(); f.db.query("UPDATE jobs SET max_attempts=1 WHERE id=?").run(queued.jobId!);
  await f.wait(queued.jobId!, "failed"); expect(f.repo.tvRetention.eviction(queued.evictionId!)!.finishedAt).toBeNull();
  expect((await fs.readdir(path.join(f.roots.contentRoot, ".podible-tv-retention"))).filter((name) => name.endsWith(".mkv"))).toHaveLength(1);
  for (const file of f.files) expect(await fs.readFile(file.seed)).toEqual(file.bytes);
});

test("TV retention router is administrator-only, strict and bounded; preview methods remain read-only", async () => {
  await using f = await fixture(); const router = createTvRetentionRouter(f.adapters);
  const ctx: RpcContext = { repo: f.repo, startTime: 0, request: new Request("http://podible.invalid/rpc"), session: null };
  for (const method of Object.values(router.routes)) await expect(executeMethod<any, any>(method, ctx, {})).rejects.toMatchObject({ code: -32001 });
  ctx.session = { user_id: f.user.id, is_admin: 0 } as RpcContext["session"];
  await expect(executeMethod(router.routes.audit, ctx, f.scope)).rejects.toMatchObject({ code: -32003 });
  ctx.session = { user_id: f.user.id, is_admin: 1 } as RpcContext["session"];
  expect(router.routes.audit.readOnly).toBe(true); expect(router.routes.preview.readOnly).toBe(true); expect(router.routes.evict.readOnly).toBe(false);
  for (const extra of [{ force: true }, { watched: true }, { filePath: "/foreign" }, { seedSeconds: 9_999_999 }])
    await expect(executeMethod(router.routes.audit, ctx, { ...f.scope, ...extra })).rejects.toMatchObject({ code: -32602 });
  await expect(executeMethod(router.routes.preview, ctx, { seasons: Array.from({ length: 4 }, (_, i) => ({ ...f.scope, seasonNumber: i })) })).rejects.toMatchObject({ code: -32602 });
});
