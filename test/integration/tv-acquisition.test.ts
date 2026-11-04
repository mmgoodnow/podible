import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { inspectMetainfo } from "../../src/media/metainfo";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { acquireTv } from "../../src/tv/acquisition";
import { PlexTvLibrary } from "../../src/tv/plex";
import { tvRoots, tvViewEntries, type TvPublishedFile } from "../../src/tv/publication";
import type { TvServices } from "../../src/tv/runtime";
import type { TvCatalogEntry } from "../../src/tv/types";
import type { WorkerContext } from "../../src/worker/context";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createFixtureMovieServices, fixtureMovie } from "../fixtures/movie-services";

function catalog(): TvCatalogEntry {
  return {
    key: "anidb:11826", tvdbId: 307375, tmdbId: null, title: "Anime Work Two", aliases: ["Anime Season 2"], year: 2019,
    anime: true, source: "fixture:explicit-anime-work", relations: [],
    episodes: [201, 202, 203, 204].map((id, index) => ({
      key: `tvdb:${id}`, title: `Episode ${index + 1}`, airDate: index === 3 ? "2099-01-01" : "2019-01-01", runtimeMinutes: 24,
      numbering: [
        { scheme: "anidb:episode", season: 1, episode: index + 1, absolute: index + 1, source: "anidb:11826:explicit-map" },
        { scheme: "tvdb:aired", season: 2, episode: index + 1, absolute: 13 + index, source: "tvdb:307375:aired" },
      ],
    })),
  };
}

function fixtureServices(root: string) {
  const base = createFixtureMovieServices(root);
  const coverage = new Map<string, string[]>();
  const rejected = new Set<string>();
  const selections: { ids: number[]; keys: string[]; absolute: (number | null)[] }[] = [];
  let sourceAdds = 0;
  const services: TvServices = {
    search: () => base.services.search(fixtureMovie),
    client: { ...base.services.client, ensureAdded: async (input) => {
      expect(input.location.rootId).toBe("tv");
      sourceAdds++;
      return base.services.client.ensureAdded(input);
    } },
    library: { scan: () => base.services.library.scan(),
      verify: (_entry, files) => base.services.library.verify(fixtureMovie, files.map((file) => file.serverPath)) },
    fetchMetainfo: base.services.fetchMetainfo,
    probe: base.services.probe,
    timing: { pollMs: 1, stallMs: 1500, plexAttempts: 2, plexRetryMs: 1 },
    decider: () => ({
      select: async ({ episodes, offers }) => {
        selections.push({ ids: episodes.map((ep) => ep.id), keys: episodes.map((ep) => ep.key),
          absolute: episodes.map((ep) => ep.numbering.find((n) => n.scheme === "tvdb:aired")!.absolute) });
        return { offer: offers.find((offer) => coverage.get(offer.key)?.some((key) => episodes.some((ep) => ep.key === key))) ?? null,
          reason: "Fixture selection by explicit stable episode-key membership" };
      },
      inspect: async ({ offer, episodes, files }) => {
        const episodeIds = episodes.filter((ep) => coverage.get(offer.key)?.includes(ep.key)).map((ep) => ep.id);
        return { files: !files.length || rejected.has(offer.key) || !episodeIds.length ? [] : [{
          fileIndex: files[0]!.index, episodeIds,
          facts: { identityVerified: true, cut: "theatrical", source: "web", proper: false },
          reason: "Explicit synthetic complete-episode coverage",
        }], reason: rejected.has(offer.key) ? "Downloaded content is not the requested episodes" : "Fixture episode identity reviewed" };
      },
    }),
  };
  return {
    ...base, services, selections, get sourceAdds() { return sourceAdds; },
    addRelease: (key: string, episodeKeys: string[], options: { stalled?: boolean; invalid?: boolean } = {}) => {
      coverage.set(key, episodeKeys);
      if (options.invalid) rejected.add(key);
      return base.addOffer(key, 1280, options.stalled);
    },
  };
}

async function setup(keys = ["tvdb:201", "tvdb:202", "tvdb:203", "tvdb:204"]) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-acquire-")));
  const appPath = path.join(root, "app.sqlite");
  let db = new Database(appPath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  const settings = defaultSettings();
  settings.agents.apiKey = "";
  settings.tv = { ...settings.tv, managedRoot: root, plexViewPath: "/data/views/plex/tv" };
  repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-acquisition", username: "viewer", isAdmin: false });
  const request = repo.tv.request(user.id, catalog(), defaultMovieQualityPolicy, keys);
  const fixture = fixtureServices(root);
  const context: WorkerContext = { get repo() { return repo; }, getSettings: () => repo.getSettings(), onLog() {}, tvServices: fixture.services };
  const enqueue = (reason: "initial" | "recovery" = "initial") => repo.tv.startAcquisition(request.targetId, reason, (id) =>
    repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id }, maxAttempts: 1 }).id, request.requestId).acquisition;
  return {
    root, request, fixture, context, enqueue, roots: tvRoots(root, settings.tv.plexViewPath),
    get repo() { return repo; }, get db() { return db; },
    reopen: () => { db.close(); db = new Database(appPath); repo = new BooksRepo(db); },
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

type Runtime = Awaited<ReturnType<typeof createJobExecutionRuntime>>;
async function until(runtime: Runtime, predicate: () => boolean, description: () => string) {
  const deadline = Date.now() + 8000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`TV Reflow did not finish: ${description()}`);
    await runtime.reconcile();
    await Bun.sleep(15);
  }
}
async function assertOwnedViews(s: Awaited<ReturnType<typeof setup>>) {
  const views = tvViewEntries(s.repo.tv, s.roots);
  for (const view of views) {
    const link = path.join(s.roots.viewRoot, view.relativePath);
    expect((await fs.lstat(link)).isSymbolicLink()).toBe(true);
    expect(await fs.realpath(link)).toBe(view.contentPath);
    expect(view.contentPath.startsWith(`${s.roots.contentRoot}/files/`)).toBe(true);
    expect(await fs.readFile(link)).toEqual(await fs.readFile(view.contentPath));
  }
  return views;
}

test("native Reflow publishes anime partial packs by stable IDs, continues only missing aired episodes and preserves future intent", async () => {
  const s = await setup();
  let runtime: Runtime | undefined;
  try {
    const firstOffer = s.fixture.addRelease("absolute-13-14", ["tvdb:201", "tvdb:202"]);
    s.fixture.addRelease("absolute-15", ["tvdb:203"]);
    const initial = s.enqueue();
    runtime = await createJobExecutionRuntime(s.context, path.join(s.root, "execution.sqlite"), { retryDelayMs: 1 });
    await until(runtime, () => {
      const failed = s.repo.tv.acquisitions(s.request.targetId).find((a) => a.job_id && s.repo.getJob(a.job_id)?.status === "failed");
      if (failed) throw new Error(`Native TV job failed: ${s.repo.getJob(failed.job_id!)?.error}`);
      return s.repo.tv.missingEpisodes(s.request.requestId).length === 1 &&
        s.repo.tv.acquisitions(s.request.targetId).every((a) => a.job_id && s.repo.getJob(a.job_id)?.status === "succeeded");
    }, () => JSON.stringify(s.repo.tv.acquisitions(s.request.targetId).map((a) => s.repo.getJob(a.job_id!))));
    expect(s.repo.getJob(initial.job_id!)!.status).toBe("succeeded");
    expect(s.fixture.selections.map((selection) => selection.keys)).toEqual([["tvdb:201", "tvdb:202", "tvdb:203"], ["tvdb:203"]]);
    expect(s.fixture.selections[0]!.absolute).toEqual([13, 14, 15]);
    expect(s.fixture.selections[0]!.ids).toEqual(s.request.episodeIds.slice(0, 3));
    expect(s.repo.tv.requestedEpisodeIds(s.request.requestId)).toEqual(s.request.episodeIds);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:204"]);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(3);
    expect(s.repo.tv.acquisitions(s.request.targetId)).toHaveLength(2);
    expect(s.fixture.sourceAdds).toBe(2);
    const views = await assertOwnedViews(s);
    expect(views.map((view) => view.relativePath).some((name) => name.includes("S02E01-E02"))).toBe(true);
    expect(views.map((view) => view.relativePath).some((name) => name.includes("S02E03"))).toBe(true);
    const metainfo = await s.fixture.services.fetchMetainfo(firstOffer);
    const metadata = inspectMetainfo(metainfo);
    const download = s.fixture.downloads.get(metadata.infoHash)!;
    const source = await fs.readFile(path.join(s.root, "downloads", download.location.relativePath, metadata.name));
    const decoded = bencode.decode(Buffer.from(metainfo)) as { info: { pieces: Buffer } };
    expect(source.length).toBe(metadata.size);
    expect(createHash("sha1").update(source).digest()).toEqual(Buffer.from(decoded.info.pieces));
    expect(await fs.readFile(path.join(s.roots.contentRoot, s.repo.tv.version(s.repo.tv.activeEpisodes(s.request.targetId)[0]!.version_id)!.files[0]!.relativePath))).toEqual(source);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("invalid TV import and stalled source are retained while a different complete release is reacquired", async () => {
  const s = await setup(["tvdb:201", "tvdb:202"]);
  try {
    s.fixture.addRelease("wrong-episodes", ["tvdb:201", "tvdb:202"], { invalid: true });
    s.fixture.addRelease("stalled-pack", ["tvdb:201", "tvdb:202"], { stalled: true });
    s.fixture.addRelease("valid-pack", ["tvdb:201", "tvdb:202"]);
    const acquisition = s.enqueue();
    await acquireTv(s.context, s.repo.getJob(acquisition.job_id!)!, s.fixture.services);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    expect(s.fixture.sourceAdds).toBe(3);
    expect(s.fixture.stats.quiesces).toBe(1);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.fixture.downloads.size).toBe(3);
    const invalid = [...s.fixture.downloads.values()][0]!;
    expect(await fs.readFile(path.join(s.root, "downloads", invalid.location.relativePath, "wrong-episodes.mkv"), "utf8")).toContain("wrong-episodes");
    const views = await assertOwnedViews(s);
    expect(await fs.readFile(views[0]!.contentPath, "utf8")).toContain("valid-pack");
    const events = s.repo.tv.events(s.request.targetId);
    expect(events.some((event) => event.stage === "replacing" && event.message.includes("not the requested"))).toBe(true);
    expect(events.some((event) => event.stage === "replacing" && event.message.includes("stalled"))).toBe(true);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
  } finally { await s.close(); }
});

test("wrong Plex episode GUID blocks activation despite correct aired ordinals and retains old playable links", async () => {
  const s = await setup(["tvdb:201", "tvdb:202"]);
  try {
    s.fixture.addRelease("old-first-episode", ["tvdb:201"]);
    await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    const before = s.repo.tv.activeEpisodes(s.request.targetId);
    const original = (await assertOwnedViews(s))[0]!;
    const oldLink = path.join(s.roots.viewRoot, original.relativePath);
    const bytes = await fs.readFile(oldLink);
    s.fixture.addRelease("new-second-episode", ["tvdb:202"]);
    let stagedFiles: TvPublishedFile[] = [];
    const plex = new PlexTvLibrary({ url: "http://plex.invalid", token: "fixture-only", sectionId: "9" }, (async (input) => {
      const url = new URL(String(input));
      if (url.pathname === "/library/sections") return Response.json({ MediaContainer: { Directory: [{ key: "9", type: "show" }] } });
      if (url.pathname.endsWith("/refresh")) return new Response("{}");
      if (url.pathname === "/library/sections/9/all")
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: "show", Guid: [{ id: "tvdb://307375" }] }] } });
      if (url.pathname === "/library/metadata/show/allLeaves")
        return Response.json({ MediaContainer: { Metadata: stagedFiles.flatMap((file) => file.episodes.map((ep) => ({
          ratingKey: `wrong-${ep.id}`, parentIndex: 2, index: 2,
          Guid: [{ id: "tvdb://999999" }], Media: [{ Part: [{ file: file.serverPath }] }],
        }))) } });
      throw new Error(`Unexpected Plex request ${url.pathname}`);
    }) as typeof fetch);
    s.fixture.services.library = { scan: () => plex.scan(), verify: (entry, files) => { stagedFiles = files; return plex.verify(entry, files); } };
    const recovery = s.enqueue("recovery");
    await expect(acquireTv(s.context, s.repo.getJob(recovery.job_id!)!, s.fixture.services)).rejects.toThrow("Plex did not verify");
    expect(stagedFiles.flatMap((file) => file.episodes.map((ep) => ep.key))).toEqual(["tvdb:202"]);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual(before);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:202"]);
    expect(s.repo.tv.publications()).toHaveLength(1);
    expect(await fs.readFile(oldLink)).toEqual(bytes);
    expect(await fs.realpath(oldLink)).toBe(original.contentPath);
    expect(s.fixture.sourceAdds).toBe(2);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("real Reflow restart replays a checkpointed TV snatch without extra source adds or content versions", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    s.fixture.addRelease("replayed", ["tvdb:201"]);
    const acquisition = s.enqueue();
    const jobId = acquisition.job_id!;
    const inspect = s.fixture.services.client.inspect;
    let interrupted = false;
    s.fixture.services.client.inspect = async (ref) => {
      if (!interrupted) { interrupted = true; throw new Error("fixture process lost after saved snatch"); }
      return inspect(ref);
    };
    const executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(jobId)?.status === "failed", () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJob(jobId)!.error).toContain("fixture process lost");
    const runId = s.repo.getJobExecution(jobId)!.run_id;
    expect(JSON.stringify((await runtime.engine.getRunStatus(runId))?.steps)).toContain("tv:attempt:0:snatch");
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 0 });
    await runtime.stop(); runtime = undefined;
    s.reopen();
    s.repo.retryJob(jobId);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => {
      const current = s.repo.getJob(jobId);
      if (current?.status === "failed") throw new Error(`Replayed native TV job failed: ${current.error}`);
      return current?.status === "succeeded";
    }, () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJobExecution(jobId)!.run_id).toBe(runId);
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
    expect(s.db.query("SELECT COUNT(*) AS n FROM managed_content_files").get()).toEqual({ n: 1 });
    await assertOwnedViews(s);
    await runtime.stop(); runtime = undefined;
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("real Reflow retry rechecks caught-up Plex using the same staged TV version without another source add", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    s.fixture.addRelease("plex-catching-up", ["tvdb:201"]);
    s.fixture.setPlexVerified(false);
    const acquisition = s.enqueue();
    const jobId = acquisition.job_id!;
    const executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(jobId)?.status === "failed", () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJob(jobId)!.error).toContain("Plex did not verify");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([]);
    const staged = s.repo.tv.publications()[0]!;
    expect(staged).toBeDefined();
    const runId = s.repo.getJobExecution(jobId)!.run_id;
    const scansBeforeRetry = s.fixture.stats.scans;
    await runtime.stop(); runtime = undefined;
    s.reopen();
    s.fixture.setPlexVerified(true);
    s.repo.retryJob(jobId);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => {
      const current = s.repo.getJob(jobId);
      if (current?.status === "failed")
        throw new Error(`Caught-up Plex was not rechecked (scans ${scansBeforeRetry} -> ${s.fixture.stats.scans}): ${current.error}`);
      return current?.status === "succeeded";
    }, () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.fixture.stats.scans).toBeGreaterThan(scansBeforeRetry);
    expect(s.repo.getJobExecution(jobId)!.run_id).toBe(runId);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([{ episode_id: s.request.episodeIds[0]!, version_id: staged.version_id }]);
    expect(s.repo.tv.publications()).toEqual([]);
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
    await assertOwnedViews(s);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("withdrawal and intentional eviction fence queued recovery without repairing missing links or snatching again", async () => {
  for (const intent of ["withdrawn", "evicted"] as const) {
    const s = await setup(["tvdb:201", "tvdb:202"]);
    try {
      s.fixture.addRelease(`initial-${intent}`, ["tvdb:201"]);
      await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
      const view = (await assertOwnedViews(s))[0]!;
      const before = s.repo.tv.activeEpisodes(s.request.targetId);
      const recovery = s.enqueue("recovery");
      await fs.unlink(path.join(s.roots.viewRoot, view.relativePath));
      if (intent === "withdrawn") s.repo.tv.withdrawRequest(s.request.requestId);
      else s.repo.retention.evict({ namespace: "tv-target", id: String(s.request.targetId) }, { expectedRevision: 0, reason: "User intentionally evicted TV content" });
      s.fixture.addRelease(`repair-${intent}`, ["tvdb:202"]);
      await expect(acquireTv(s.context, s.repo.getJob(recovery.job_id!)!, s.fixture.services)).rejects.toThrow(intent === "withdrawn" ? "withdrawn" : "intentionally evicted");
      expect(s.fixture.sourceAdds).toBe(1);
      expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual(before);
      await expect(fs.lstat(path.join(s.roots.viewRoot, view.relativePath))).rejects.toMatchObject({ code: "ENOENT" });
      expect((await fs.stat(view.contentPath)).isFile()).toBe(true);
      expect(s.fixture.stats.removals).toBe(0);
    } finally { await s.close(); }
  }
});
