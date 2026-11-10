import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { executeMethod } from "../../src/rpc/framework";
import { moviesRouter } from "../../src/rpc/movies-router";
import type { RpcContext } from "../../src/rpc/shared";
import { acquireMovie, movieRoots } from "../../src/movies/acquisition";
import { movieViewEntries } from "../../src/movies/publication";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { movieVersionSpecSchema, policyForMovieVersion } from "../../src/movies/version-target";
import { createFixtureMovieServices, fixtureMovie } from "../fixtures/movie-services";

test("independent 1080p and 4K movie targets publish together; compatible requests reuse canonical bytes without snatching", async () => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "movie-versions-")));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.getSettings();
  settings.movies.managedRoot = root;
  settings.movies.plexViewPath = "/data/view";
  repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "versions", username: "requester", isAdmin: false });
  const fixture = createFixtureMovieServices(root);
  fixture.addOffer("1080p", 1920);
  fixture.addOffer("4k", 3840);
  const ctx: RpcContext = { repo, startTime: Date.now(), request: new Request("https://podible.invalid/rpc"),
    session: { user_id: user.id, is_admin: 0 } as RpcContext["session"], movieServices: fixture.services };
  const request = (version?: { resolution?: 1080 | 2160; audioLanguage?: string }) =>
    executeMethod(moviesRouter.routes.request, ctx, { tmdbId: fixtureMovie.tmdbId, ...(version ? { version } : {}) });
  const work = async (jobId: number) => acquireMovie({ repo, getSettings: () => settings, onLog() {}, movieServices: fixture.services }, repo.getJob(jobId)!, fixture.services);
  try {
    const hd = (await request({ resolution: 1080 })).request;
    const uhd = (await request({ resolution: 2160 })).request;
    expect(hd.targetId).not.toBe(uhd.targetId);
    expect(hd.job!.id).not.toBe(uhd.job!.id);
    expect((await request({ resolution: 2160 })).request.id).toBe(uhd.id);
    expect(hd.versionLabel).toBe("1080p");
    expect(uhd.versionLabel).toBe("4K");
    await work(hd.job!.id);
    const originalVersion = repo.movies.target(hd.targetId)!.active_version_id;
    await work(uhd.job!.id);
    expect(repo.movies.target(hd.targetId)!.active_version_id).toBe(originalVersion);
    expect(repo.movies.target(uhd.targetId)!.active_version_id).not.toBe(originalVersion);
    const roots = movieRoots(root, settings.movies.plexViewPath);
    const entries = movieViewEntries(repo.movies, roots);
    expect(entries).toHaveLength(2);
    expect(new Set(entries.map(entry => entry.relativePath)).size).toBe(2);
    for (const entry of entries) {
      const view = path.join(roots.viewRoot, entry.relativePath);
      expect((await fs.lstat(view)).isSymbolicLink()).toBe(true);
      expect(await fs.realpath(view)).toBe(entry.contentPath);
    }
    const before = { ...fixture.stats };
    const english = (await request({ resolution: 1080, audioLanguage: "en" })).request;
    await work(english.job!.id);
    expect(repo.movies.target(english.targetId)!.active_version_id).toBe(originalVersion);
    expect(fixture.stats.snatches).toBe(before.snatches);
    expect(fixture.stats.searches).toBe(before.searches);
    expect(fixture.stats.probes).toBe(before.probes);
    expect(fixture.stats.scans).toBeGreaterThan(before.scans);
    const page = await executeMethod(moviesRouter.routes.get, ctx, { tmdbId: fixtureMovie.tmdbId });
    expect(page.requests).toHaveLength(3);
    expect(page.requests.every(request => request.available)).toBe(true);
    expect(new Set(movieViewEntries(repo.movies, roots).map(entry => entry.contentPath)).size).toBe(2);
    const corrupt = entries.find(entry => entry.relativePath.includes(`t${hd.targetId}v`))!.contentPath;
    await fs.chmod(corrupt, 0o600);
    await fs.writeFile(corrupt, "truncated");
    const sdr = (await executeMethod(moviesRouter.routes.request, ctx, { tmdbId: fixtureMovie.tmdbId,
      version: { resolution: 1080, sdrOnly: true } })).request;
    await expect(work(sdr.job!.id)).rejects.toThrow("no longer matches");
    expect(repo.movies.target(sdr.targetId)!.active_version_id).toBeNull();
    expect(fixture.stats.snatches).toBe(before.snatches);
  } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
});

test("version specs preserve unrelated policy and reject missing or unrecognized requirements", () => {
  for (const input of [{}, { resolution: 1440 }, { audioLanguage: "not-a-language" }, { cut: " " }, { sdrOnly: false }])
    expect(movieVersionSpecSchema.safeParse(input).success).toBe(false);
  const spec = movieVersionSpecSchema.parse({ resolution: 2160, cut: " Extended ", audioLanguage: "ja-JP", sdrOnly: true });
  const policy = policyForMovieVersion(defaultMovieQualityPolicy, spec);
  expect(policy.tiers.every(tier => tier.resolution === 2160)).toBe(true);
  expect(policy).toMatchObject({ cut: "extended", audioLanguages: ["ja"], allowHdr: false,
    allowedCodecs: defaultMovieQualityPolicy.allowedCodecs, minimumPreferenceGain: defaultMovieQualityPolicy.minimumPreferenceGain });
  expect(defaultMovieQualityPolicy.cut).toBeNull();
});
