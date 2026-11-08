import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { publishTvVersion, tvRoots } from "../../src/tv/publication";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { jobAdmissionBlocked } from "../../src/worker/locks";
import { reconcileTv } from "../../src/tv/maintenance";
import { createFixtureMovieServices } from "../fixtures/movie-services";

async function until(check: () => boolean | Promise<boolean>) {
  const end = Date.now() + 4000;
  while (!await check()) { if (Date.now() > end) throw new Error("Publication admission timed out"); await Bun.sleep(10); }
}

test.each([[false, false], [true, false], [false, true]])("staged TV owner takes next admission; in-flight=%s selected=%s", async (inFlight, selected) => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-publication-admission-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.managedRoot = root; settings.tv.plexViewPath = "/server/tv"; repo.updateSettings(settings);
  const roots = tvRoots(root, settings.tv.plexViewPath); await prepareMovieStorage(repo, roots.contentRoot);
  const user = repo.upsertUser({ provider: "local", providerUserId: "admission", username: "admission", isAdmin: true });
  const catalog = { key: "tvdb:701", tvdbId: 701, tmdbId: null, title: "Admission fixture", aliases: [], year: 2020,
    anime: false, relations: [], episodes: [1, 2, 3].map(id => ({ key: `tvdb:${70100 + id}`, title: `Episode ${id}`,
      airDate: "2020-01-01", runtimeMinutes: 24, refs: [{ provider: "tvdb" as const, id: String(70100 + id), source: "fixture" }],
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: id, absolute: id }] })) };
  const request = repo.tv.request(user.id, catalog, settings.movies.qualityPolicy, catalog.episodes.map(ep => ep.key));
  const enqueue = (episodeId: number) => repo.tv.startAcquisition(request.targetId, "initial", id => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId: id }, maxAttempts: 1
  }).id, request.requestId, [episodeId]).acquisition;
  let release!: () => void; const hold = new Promise<void>(resolve => { release = resolve; });
  let held = false, afterRead = 0, published = false, ownerId = 0, versionId = 0;
  const runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), onLog() {} },
    path.join(root, "execution.sqlite"), { processJob: async (ctx, job) => {
      if (job.id === ownerId) await ctx.execution!.run("publish", () => publishTvVersion({ repo: ctx.repo.tv, roots,
        targetId: request.targetId, versionId, assertActive: () => ctx.execution!.assertActive(), library: {
          scan: async () => {}, verify: async (_entry, files) => {
            expect(afterRead).toBe(0);
            expect(files[0]!.episodes[0]!.refs.some(ref => ref.id === "70101")).toBe(true);
            const local = path.join(roots.viewRoot, path.posix.relative(roots.serverViewRoot, files[0]!.serverPath));
            expect(await fs.readFile(local, "utf8")).toBe("verified episode");
            published = true; return true;
          }
        } }));
      else {
        await ctx.execution!.run("long-read", async () => { if (!published) { held = true; await hold; } });
        await ctx.execution!.run("after-read", () => { expect(published).toBe(true); afterRead++; });
      }
      return "done";
    } });
  const first = enqueue(request.episodeIds[1]!);
  try {
    if (inFlight) { await runtime.reconcile(); await until(() => held); }
    const second = enqueue(request.episodeIds[2]!);
    const owner = enqueue(request.episodeIds[0]!); ownerId = owner.job_id!;
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`;
    await fs.mkdir(path.dirname(path.join(roots.contentRoot, relativePath)), { recursive: true });
    await fs.writeFile(path.join(roots.contentRoot, relativePath), "verified episode", { mode: 0o444 });
    versionId = repo.tv.recordVersion(owner.id, { identityVerified: true }, [{ id, relativePath, sizeBytes: 16,
      probe: {}, episodeIds: [request.episodeIds[0]!] }]);
    if (selected) db.query("UPDATE tv_targets SET revision=17 WHERE id=?").run(request.targetId);
    repo.tv.stagePublication(request.targetId, versionId);
    if (selected) {
      repo.tv.promotePublication(request.targetId, versionId, 17);
      expect(repo.tv.publications()[0]!.expected_revision).toBe(17);
      expect(repo.tv.target(request.targetId)!.revision).toBe(18);
    }
    expect(jobAdmissionBlocked(repo, repo.getJob(ownerId)!)).toBe(false);
    expect(jobAdmissionBlocked(repo, repo.getJob(first.job_id!)!)).toBe(true);
    expect(jobAdmissionBlocked(repo, { ...repo.getJob(ownerId)!, type: "tv_reconcile" })).toBe(false);
    await runtime.reconcile();
    if (inFlight) {
      await until(async () => (await runtime.engine.getRunStatus(repo.getJobExecution(second.job_id!)!.run_id))?.run.status === "sleeping");
      expect(published).toBe(false); expect(afterRead).toBe(0);
      expect(runtime.diagnostics().effects.some(effect => effect.jobId === first.job_id && effect.stepName === "long-read")).toBe(true);
      release();
    }
    await until(() => repo.getJobStatus(ownerId) === "succeeded").catch(async error => {
      throw new Error(`${error.message}: ${JSON.stringify({ jobs: [first, second, owner].map(acq => repo.getJob(acq.job_id!)),
        diagnostics: runtime.diagnostics(), runs: await Promise.all([first, second, owner].map(acq =>
          runtime.engine.getRunStatus(repo.getJobExecution(acq.job_id!)!.run_id))) })}`);
    });
    expect(published).toBe(true);
    if (!inFlight) { expect(held).toBe(false); release(); }
    await until(() => [first, second].every(acq => repo.getJobStatus(acq.job_id!) === "succeeded"));
    expect(afterRead).toBe(2);
    expect(repo.tv.publications()).toEqual([]);
    expect(jobAdmissionBlocked(repo, repo.getJob(first.job_id!)!)).toBe(false);
    expect(repo.tv.activeEpisodes(request.targetId)).toEqual([{ episode_id: request.episodeIds[0]!, version_id: versionId }]);
    expect(repo.tv.target(request.targetId)!.revision).toBe(selected ? 18 : 1);
  } finally { release(); await runtime.stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); }
}, 15000);

test.each([false, true])("unfenced reconciliation clears an abandoned publication; selected=%s", async selected => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-abandoned-publication-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.managedRoot = root; settings.tv.plexViewPath = "/server/tv"; repo.updateSettings(settings);
  const roots = tvRoots(root, settings.tv.plexViewPath); await prepareMovieStorage(repo, roots.contentRoot);
  const user = repo.upsertUser({ provider: "local", providerUserId: "abandoned", username: "abandoned", isAdmin: true });
  const catalog = { key: "tvdb:702", tvdbId: 702, tmdbId: null, title: "Abandoned fixture", aliases: [], year: 2020,
    anime: false, relations: [], episodes: [1, 2].map(id => ({ key: `tvdb:${70200 + id}`, title: `Episode ${id}`,
      airDate: "2020-01-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: id, absolute: id }] })) };
  const request = repo.tv.request(user.id, catalog, settings.movies.qualityPolicy, catalog.episodes.map(ep => ep.key));
  const enqueue = (episodeId: number) => repo.tv.startAcquisition(request.targetId, "initial", id => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId: id }, maxAttempts: 1
  }).id, request.requestId, [episodeId]).acquisition;
  const fixture = createFixtureMovieServices(root);
  let scans = 0, verifications = 0, entered = false;
  const runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), onLog() {}, tvServices: {
    ...fixture.services, search: async () => { throw new Error("Reconciliation must not search"); },
    decider: () => { throw new Error("Reconciliation must not invoke an agent"); },
    library: { scan: async () => { scans++; }, verify: async () => { verifications++; return true; } }
  } }, path.join(root, "execution.sqlite"), { processJob: async (ctx, job) => {
    if (job.type === "tv_reconcile") return reconcileTv(ctx);
    await ctx.execution!.run("contender", () => { expect(repo.tv.publications()).toEqual([]); entered = true; });
    return "done";
  } });
  try {
    const owner = enqueue(request.episodeIds[0]!);
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`;
    await fs.mkdir(path.dirname(path.join(roots.contentRoot, relativePath)), { recursive: true });
    await fs.writeFile(path.join(roots.contentRoot, relativePath), "episode", { mode: 0o444 });
    const versionId = repo.tv.recordVersion(owner.id, { identityVerified: true }, [{ id, relativePath, sizeBytes: 7,
      probe: {}, episodeIds: [request.episodeIds[0]!] }]);
    repo.tv.stagePublication(request.targetId, versionId);
    if (selected) repo.tv.promotePublication(request.targetId, versionId, 0);
    repo.markJobCancelled(owner.job_id!);
    const contender = enqueue(request.episodeIds[1]!);
    await runtime.reconcile();
    await until(async () => (await runtime.engine.getRunStatus(repo.getJobExecution(contender.job_id!)!.run_id))?.run.status === "sleeping");
    expect(entered).toBe(false);
    const maintenance = repo.createJob({ type: "tv_reconcile" });
    expect(jobAdmissionBlocked(repo, maintenance)).toBe(false);
    await runtime.reconcile();
    await until(() => repo.getJobStatus(maintenance.id) === "succeeded");
    await until(() => repo.getJobStatus(contender.job_id!) === "succeeded");
    expect(repo.tv.publications()).toEqual([]); expect(entered).toBe(true);
    expect(scans).toBeGreaterThan(0); expect(verifications).toBe(selected ? 1 : 0);
    expect(repo.tv.activeEpisodes(request.targetId)).toEqual(selected ? [{ episode_id: request.episodeIds[0]!, version_id: versionId }] : []);
    expect(await fs.readFile(path.join(roots.contentRoot, relativePath), "utf8")).toBe("episode");
  } finally { await runtime.stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); }
}, 15000);
