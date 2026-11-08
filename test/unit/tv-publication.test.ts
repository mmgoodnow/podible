import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { adoptContentFile } from "../../src/media/content-store";
import { prepareMovieStorage } from "../../src/movies/storage";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { PlexTvLibrary } from "../../src/tv/plex";
import { publishTvVersion, reconcileTvView, tvRoots, tvViewEntries, type TvPublishedFile } from "../../src/tv/publication";
import type { TvCatalogEntry } from "../../src/tv/types";
import { executionDiagnostics, withExecutionEffects } from "../../src/runtime/execution-effects";

const catalog: TvCatalogEntry = {
  key: "anidb:11826", tvdbId: 307375, tmdbId: null, title: "Anime / Sequel", aliases: [], year: 2019, anime: true,
  source: "fixture:explicit-work-map", relations: [],
  episodes: [201, 202, 203, 204].map((id, index) => ({
    key: `tvdb:${id}`, title: `Anime episode ${index + 1}`, airDate: "2019-01-01", runtimeMinutes: 24,
    numbering: [
      { scheme: "anidb:episode", season: 1, episode: index + 1, absolute: index + 1, source: "anidb:11826:mapping" },
      { scheme: "tvdb:aired", season: index === 3 ? 3 : 2, episode: index === 3 ? 1 : index + 1, absolute: 13 + index, source: "tvdb:307375:aired" },
    ],
  })),
};

async function setup() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-publication-")));
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-publication", username: "viewer", isAdmin: false });
  const request = repo.tv.request(user.id, catalog, defaultMovieQualityPolicy, catalog.episodes.map((ep) => ep.key));
  const roots = tvRoots(root, "/data/views/plex/tv");
  await prepareMovieStorage(repo, roots.contentRoot);
  const sourceRoot = path.join(root, "downloads");
  await fs.mkdir(sourceRoot);
  const makeVersion = async (coverage: number[][]) => {
    const acquisition = repo.tv.startAcquisition(request.targetId, "initial", (id) =>
      repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id } }).id, request.requestId).acquisition;
    const files = [];
    for (const episodeIds of coverage) {
      const fileId = randomUUID();
      const bytes = Buffer.from(`verified TV content ${fileId}`);
      const sourcePath = path.join(sourceRoot, `${fileId}.mkv`);
      await fs.writeFile(sourcePath, bytes);
      const stored = await adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot, sourcePath, fileId, expectedSize: bytes.length });
      files.push({ ...stored, probe: { verified: true }, episodeIds });
    }
    const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, files);
    repo.tv.finishAcquisition(acquisition.id);
    return versionId;
  };
  return { root, roots, db, repo, request, makeVersion, close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

const verifiedLibrary = { scan: async () => {}, verify: async () => true };

test("publication diagnostics distinguish a real Plex verification wait from view reconciliation", async () => {
  const s = await setup();
  let release!: () => void, entered!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const verifying = new Promise<void>(resolve => { entered = resolve; });
  let pending: Promise<void> | undefined;
  try {
    const versionId = await s.makeVersion([[s.request.episodeIds[0]!]]);
    pending = withExecutionEffects({ jobId: 1, runId: "publication-fixture", stepName: "publish", signal: new AbortController().signal }, async () => {
      await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId,
        library: { scan: async () => {}, verify: async () => { entered(); await hold; return true; } } });
      expect(executionDiagnostics().find(effect => effect.runId === "publication-fixture")?.phases.at(-1)?.name).toBe("publication:complete");
    });
    await verifying;
    const phases = executionDiagnostics().find(effect => effect.runId === "publication-fixture")!.phases;
    expect(phases.map(phase => phase.name)).toEqual(["publication:mutex-wait", "publication:stage", "publication:reconcile", "publication:scan", "publication:verify"]);
    expect(phases.slice(0, -1).every(phase => phase.finishedAt !== undefined)).toBe(true);
    expect(phases.at(-1)?.finishedAt).toBeUndefined();
    release(); await pending;
  } finally { release(); await pending; await s.close(); }
});

test("Plex TV requires exact episode GUID plus path, not matching anime/absolute or aired ordinals", async () => {
  const s = await setup();
  try {
    const entry = s.repo.tv.entry(s.request.entryId)!;
    const episodes = entry.episodes.slice(0, 2);
    const serverPath = "/data/views/plex/tv/Anime/Season 02/Anime - S02E01-E02.mkv";
    let leaves: unknown[] = [];
    const routes: string[] = [];
    const plex = new PlexTvLibrary({ url: "http://plex.invalid", token: "fixture-only", sectionId: "3" }, (async (input) => {
      const url = new URL(String(input));
      routes.push(url.pathname);
      if (url.pathname === "/library/sections/3/all")
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: "show", Guid: [{ id: "tvdb://307375" }] }] } });
      if (url.pathname === "/library/metadata/show/allLeaves")
        return Response.json({ MediaContainer: { Metadata: leaves } });
      throw new Error(`Unexpected Plex GET ${url.pathname}`);
    }) as typeof fetch);
    const leaf = (id: string, file = serverPath, parentIndex = 1, index = 13) => ({
      ratingKey: id, parentIndex, index, Guid: [{ id: `tvdb://${id}` }], Media: [{ Part: [{ file }] }],
    });
    const files: TvPublishedFile[] = [{ serverPath, episodes }];
    leaves = [leaf("201"), leaf("202", serverPath, 1, 14)];
    expect(await plex.verify(entry, files)).toBe(true);
    leaves = [leaf("999", serverPath, 2, 1), leaf("202", serverPath, 2, 2)];
    expect(await plex.verify(entry, files)).toBe(false);
    leaves = [leaf("201", `${serverPath}.wrong`), leaf("202")];
    expect(await plex.verify(entry, files)).toBe(false);
    leaves = [leaf("201")];
    expect(await plex.verify(entry, files)).toBe(false);
    expect(routes.every((route) => route === "/library/sections/3/all" || route === "/library/metadata/show/allLeaves")).toBe(true);
  } finally { await s.close(); }
});

test("publication replay after promotion keeps owned bytes and finishes exactly one available event", async () => {
  const s = await setup();
  try {
    const [one] = s.request.episodeIds;
    const old = await s.makeVersion([[one!]]);
    await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: old, library: verifiedLibrary });
    const oldView = tvViewEntries(s.repo.tv, s.roots)[0]!;
    const oldLink = path.join(s.roots.viewRoot, oldView.relativePath);
    const oldBytes = await fs.readFile(oldView.contentPath);
    const next = await s.makeVersion([[one!]]);
    let interrupted = false;
    await expect(publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: next, library: verifiedLibrary,
      assertActive: () => {
        if (!interrupted && s.repo.tv.activeEpisodes(s.request.targetId).some((ep) => ep.version_id === next)) {
          interrupted = true;
          throw new Error("crash after TV promotion");
        }
      },
    })).rejects.toThrow("crash after TV promotion");
    expect(s.repo.tv.publications()).toHaveLength(1);
    expect(await fs.readFile(oldLink)).toEqual(oldBytes);
    await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: next, library: verifiedLibrary });
    expect(s.repo.tv.publications()).toEqual([]);
    expect(s.repo.tv.target(s.request.targetId)!.revision).toBe(2);
    const current = tvViewEntries(s.repo.tv, s.roots)[0]!;
    expect((await fs.lstat(path.join(s.roots.viewRoot, current.relativePath))).isSymbolicLink()).toBe(true);
    expect(await fs.realpath(path.join(s.roots.viewRoot, current.relativePath))).toBe(current.contentPath);
    await expect(fs.lstat(oldLink)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.readFile(oldView.contentPath)).toEqual(oldBytes);
    expect(s.repo.tv.events(s.request.targetId).filter((event) => event.stage === "available")).toHaveLength(2);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 2 });
  } finally { await s.close(); }
});

test("multi-episode view rejects partial replacement and safely replaces complete file coverage", async () => {
  const s = await setup();
  try {
    const [one, two] = s.request.episodeIds as [number, number];
    const old = await s.makeVersion([[one, two]]);
    await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: old, library: verifiedLibrary });
    const original = tvViewEntries(s.repo.tv, s.roots)[0]!;
    expect(original.relativePath).toContain("Season 02/");
    expect(original.relativePath).toContain("S02E01-E02");
    const oldLink = path.join(s.roots.viewRoot, original.relativePath);
    const partial = await s.makeVersion([[one]]);
    await expect(publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: partial, library: verifiedLibrary }))
      .rejects.toThrow("partially invalidate");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([{ episode_id: one, version_id: old }, { episode_id: two, version_id: old }]);
    expect(s.repo.tv.target(s.request.targetId)!.revision).toBe(1);
    expect(await fs.realpath(oldLink)).toBe(original.contentPath);
    s.repo.tv.abortPublication(s.request.targetId, partial);
    await reconcileTvView(s.repo.tv, s.roots);
    expect(tvViewEntries(s.repo.tv, s.roots)).toHaveLength(1);
    const complete = await s.makeVersion([[one], [two]]);
    await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: complete, library: verifiedLibrary });
    expect(s.repo.tv.activeEpisodes(s.request.targetId).every((ep) => ep.version_id === complete)).toBe(true);
    expect(tvViewEntries(s.repo.tv, s.roots)).toHaveLength(2);
    await expect(fs.lstat(oldLink)).rejects.toMatchObject({ code: "ENOENT" });
    expect((await fs.stat(original.contentPath)).isFile()).toBe(true);
  } finally { await s.close(); }
});

test("noncontiguous and cross-season multi-episode projections fail closed before Plex or symlink changes", async () => {
  const s = await setup();
  try {
    const [one, , three, four] = s.request.episodeIds as [number, number, number, number];
    const old = await s.makeVersion([[one]]);
    await publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: old, library: verifiedLibrary });
    const before = tvViewEntries(s.repo.tv, s.roots);
    let scans = 0;
    const library = { scan: async () => { scans++; }, verify: async () => true };
    for (const coverage of [[one, three], [one, four]]) {
      const id = await s.makeVersion([coverage]);
      await expect(publishTvVersion({ repo: s.repo.tv, roots: s.roots, targetId: s.request.targetId, versionId: id, library }))
        .rejects.toThrow("contiguous provider episodes");
      expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([{ episode_id: one, version_id: old }]);
      s.repo.tv.abortPublication(s.request.targetId, id);
    }
    expect(scans).toBe(0);
    expect(tvViewEntries(s.repo.tv, s.roots)).toEqual(before);
    expect(await fs.realpath(path.join(s.roots.viewRoot, before[0]!.relativePath))).toBe(before[0]!.contentPath);
  } finally { await s.close(); }
});
