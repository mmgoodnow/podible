import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import type { JobRow } from "../../src/app-types";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { acquireMovie } from "../../src/movies/acquisition";
import { matchMovieFeed } from "../../src/movies/feed";
import type { MovieOffer } from "../../src/movies/offers";
import { defaultMovieQualityPolicy, type MovieQualityPolicy } from "../../src/movies/quality";
import {
  assessConfirmedNukeReplacement, canReplaceConfirmedNuke, reportConfirmedReleaseNuke,
} from "../../src/media/release-health";
import { createFixtureMovieServices, fixtureMovie, fixtureFacts } from "../fixtures/movie-services";

async function setup(policy: MovieQualityPolicy = defaultMovieQualityPolicy) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(tmpdir(), "release-health-")));
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = defaultSettings();
  Object.assign(settings.movies, { managedRoot: root, plexViewPath: "/data/views/plex/movies" });
  repo.updateSettings(settings);
  const admin = repo.upsertUser({ provider: "local", providerUserId: "admin", username: "admin", isAdmin: true });
  const user = repo.upsertUser({ provider: "local", providerUserId: "reader", username: "reader", isAdmin: false });
  const request = repo.movies.request(user.id, fixtureMovie, policy);
  const fixture = createFixtureMovieServices(root);
  const context = { repo, getSettings: () => repo.getSettings(), onLog: () => {}, movieServices: fixture.services };
  const run = async (jobId: number) => acquireMovie(context, repo.getJob(jobId)!, fixture.services);
  const initial = async (width = 1920) => {
    const offer = fixture.addOffer("original", width);
    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", (id) =>
      repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id }, maxAttempts: 1 }).id).acquisition;
    await run(acquisition.job_id!);
    const versionId = repo.movies.target(request.targetId)!.active_version_id!;
    const file = repo.movies.version(versionId)!.files[0];
    const source = repo.movies.downloads(request.targetId).find((entry) => entry.offer_key === "original")!;
    return { versionId, source, offer, file: path.join(root, "objects", file.relativePath) };
  };
  const report = (sourceTorrentId: number, reason = "Trusted tracker release report: corrupt final scene", reportId = randomUUID()) =>
    reportConfirmedReleaseNuke(repo, { reportId, sourceTorrentId, reason }, admin.id);
  const links = async (directory = path.join(root, "views/plex/movies")): Promise<string[]> => {
    const result: string[] = [];
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) result.push(...await links(file));
      else if (entry.isSymbolicLink()) result.push(await fs.realpath(file));
    }
    return result;
  };
  return { root, db, repo, admin, user, request, fixture, context, initial, report, run, links,
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

async function publishUnrelated(s: Awaited<ReturnType<typeof setup>>) {
  const request = s.repo.movies.request(s.user.id,
    { ...fixtureMovie, tmdbId: fixtureMovie.tmdbId + 1, title: "Unrelated Healthy Film" }, defaultMovieQualityPolicy);
  s.fixture.clearOffers();
  s.fixture.addOffer("unrelated-healthy", 1920);
  const acquisition = s.repo.movies.startAcquisition(request.targetId, "initial", (id) =>
    s.repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id } }).id).acquisition;
  await s.run(acquisition.job_id!);
  const versionId = s.repo.movies.target(request.targetId)!.active_version_id!;
  const file = path.join(s.root, "objects", s.repo.movies.version(versionId)!.files[0].relativePath);
  return { request, versionId, file };
}

function ingestRepairOffer(s: Awaited<ReturnType<typeof setup>>, offer: MovieOffer) {
  const now = new Date().toISOString();
  s.repo.releaseInbox.ensureFeed("nuke-repair-rss", "Fixture", "movie", now);
  return s.repo.releaseInbox.ingest("nuke-repair-rss", [{
    title: offer.title, provider: offer.provider, mediaType: "movie", sizeBytes: offer.sizeBytes,
    url: offer.url, guid: offer.key, infoHash: offer.infoHash, seeders: offer.seeders, leechers: 0, raw: offer.attributes,
  }], now)[0];
}

async function routeRepairOffer(s: Awaited<ReturnType<typeof setup>>, releaseId: number, seen: number[][], matchTargetId?: number, replay?: JobRow) {
  const job = replay ?? s.repo.createJob({ type: "movie_feed_match", payload: { releaseIds: [releaseId] } });
  await matchMovieFeed({ ...s.context, movieFeedMatcher: async ({ targets }) => {
    seen.push(targets.map((target) => target.targetId));
    return targets.filter(target => matchTargetId === undefined || target.targetId === matchTargetId)
      .map((target) => ({ targetId: target.targetId, releaseIndexes: [0], reason: "Fixture identity routing" }));
  } }, job);
  return job;
}

test("RSS repairs an unresolved nuke after an empty search without touching unrelated movies", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    const unrelated = await publishUnrelated(s);
    s.fixture.clearOffers();
    const report = s.report(old.source.id);
    await s.run(report.replacements[0].jobId);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(s.repo.releaseHealth.currentMovieNuke(s.request.targetId)).not.toBeNull();
    expect(s.repo.movies.acquisition(report.replacements[0].acquisitionId)?.finished_at).not.toBeNull();
    const searches = s.fixture.stats.searches;
    const probes = s.fixture.stats.probes;
    const unrelatedAcquisitions = s.repo.movies.acquisitions(unrelated.request.targetId).length;
    const replacement = s.fixture.addOffer("late-rss-repair", 1920);
    const releaseId = ingestRepairOffer(s, replacement);
    const seen: number[][] = [];
    const matchJob = await routeRepairOffer(s, releaseId, seen, s.request.targetId);
    expect(seen[0]).toContain(unrelated.request.targetId);
    const recovery = s.repo.movies.acquisitions(s.request.targetId)[0];
    expect(recovery.reason).toBe("recovery");
    expect(recovery.id).not.toBe(report.replacements[0].acquisitionId);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(await s.links()).toContain(old.file);
    s.fixture.services.library.verify = async () => {
      expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
      expect(await s.links()).toContain(old.file);
      return true;
    };
    await s.run(recovery.job_id!);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).not.toBe(old.versionId);
    expect(s.repo.releaseHealth.currentMovieNuke(s.request.targetId)).toBeNull();
    expect(s.repo.movies.target(unrelated.request.targetId)?.active_version_id).toBe(unrelated.versionId);
    expect(s.repo.movies.acquisitions(unrelated.request.targetId)).toHaveLength(unrelatedAcquisitions);
    expect(s.fixture.stats.searches).toBe(searches);
    expect(s.fixture.stats.probes).toBe(probes + 1);
    expect(s.fixture.stats.rechecks).toBe(0);
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
    expect(await fs.readFile(unrelated.file, "utf8")).toContain("unrelated-healthy");
    const count = s.repo.movies.acquisitions(s.request.targetId).length;
    await routeRepairOffer(s, releaseId, seen, s.request.targetId, matchJob);
    expect(s.repo.movies.acquisitions(s.request.targetId)).toHaveLength(count);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("nuke repair invalidates only the affected target's prior RSS quality consideration", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    const unrelated = await publishUnrelated(s);
    s.fixture.clearOffers();
    const replacement = s.fixture.addOffer("previously-equal-rss", 1920);
    const releaseId = ingestRepairOffer(s, replacement);
    const seen: number[][] = [];
    await routeRepairOffer(s, releaseId, seen);
    for (const targetId of [s.request.targetId, unrelated.request.targetId])
      await s.run(s.repo.movies.acquisitions(targetId)[0].job_id!);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    const unrelatedCount = s.repo.movies.acquisitions(unrelated.request.targetId).length;
    s.fixture.clearOffers();
    const report = s.report(old.source.id);
    await s.run(report.replacements[0].jobId);
    await routeRepairOffer(s, releaseId, seen);
    const repair = s.repo.movies.acquisitions(s.request.targetId)[0];
    expect(repair.reason).toBe("recovery");
    expect(repair.id).not.toBe(report.replacements[0].acquisitionId);
    expect(s.repo.movies.acquisitions(unrelated.request.targetId)).toHaveLength(unrelatedCount);
    const considered = (targetId: number) => s.db.query(`SELECT target_revision FROM release_considerations
      WHERE release_id=? AND target_id=? ORDER BY source_job_id`).all(releaseId, targetId) as { target_revision: string }[];
    expect(considered(s.request.targetId)).toHaveLength(2);
    expect(considered(s.request.targetId)[1].target_revision).toContain(`:nuke:${old.source.id}`);
    expect(considered(unrelated.request.targetId)).toHaveLength(1);
    expect(considered(unrelated.request.targetId)[0].target_revision).not.toContain(":nuke:");
    await s.run(repair.job_id!);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).not.toBe(old.versionId);
    expect(s.repo.movies.target(unrelated.request.targetId)?.active_version_id).toBe(unrelated.versionId);
  } finally { await s.close(); }
});

test("trusted nuke evidence and autonomous recovery are atomic, authorized and idempotent", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    const command = { reportId: randomUUID(), sourceTorrentId: old.source.id, reason: "Confirmed release defect" };
    await expect(Promise.resolve().then(() => reportConfirmedReleaseNuke(s.repo, command, s.user.id))).rejects.toThrow("administrators");
    expect(s.repo.releaseHealth.evidence(old.source.id)).toBeNull();
    expect(() => s.report(999999)).toThrow("Unknown source");
    const before = s.repo.movies.acquisitions(s.request.targetId).length;
    const result = reportConfirmedReleaseNuke(s.repo, command, s.admin.id);
    expect(result).toMatchObject({ idempotent: false, evidence: { status: "confirmed-nuked", source: { id: old.source.id } } });
    expect(result.replacements).toHaveLength(1);
    expect(s.repo.movies.acquisition(result.replacements[0].acquisitionId)?.reason).toBe("recovery");
    expect(s.repo.getJob(result.replacements[0].jobId)?.type).toBe("movie_acquire");
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(reportConfirmedReleaseNuke(s.repo, command, s.admin.id)).toMatchObject({ ...result, idempotent: true });
    expect(s.repo.movies.acquisitions(s.request.targetId)).toHaveLength(before + 1);
    expect(() => reportConfirmedReleaseNuke(s.repo, { ...command, reason: "Different report" }, s.admin.id)).toThrow("another command");
    expect(() => reportConfirmedReleaseNuke(s.repo, { ...command, url: "https://arbitrary.invalid" } as typeof command, s.admin.id)).toThrow();
    expect(s.repo.releaseHealth.isContentFileBlocked(s.repo.movies.version(old.versionId)!.files[0].id)).toBe(true);
    expect(await s.links()).toContain(old.file);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("same-tier verified replacement excludes the nuked source and leaves original bytes and seed record intact", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    const bytes = await fs.readFile(old.file);
    const replacement = s.fixture.addOffer("replacement", 1920);
    const search = s.fixture.services.search;
    s.fixture.services.search = async (...args) => {
      const result = await search(...args);
      return { ...result, offers: [{ ...old.offer, key: "another-provider-alias" }, ...result.offers] };
    };
    const select = s.fixture.services.decider.select;
    const seen: string[][] = [];
    s.fixture.services.decider.select = async (input) => { seen.push(input.offers.map((entry) => entry.key)); return select(input); };
    const report = s.report(old.source.id);
    await s.run(report.replacements[0].jobId);
    const next = s.repo.movies.target(s.request.targetId)!.active_version_id!;
    expect(next).not.toBe(old.versionId);
    expect(s.repo.movies.version(next)!.facts.width).toBe(1920);
    expect(seen.flat()).toEqual([replacement.key]);
    expect(await fs.readFile(old.file)).toEqual(bytes);
    expect(s.repo.movies.downloads(s.request.targetId).some((entry) => entry.id === old.source.id)).toBe(true);
    expect(s.repo.releaseHealth.evidence(old.source.id)?.status).toBe("confirmed-nuked");
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.fixture.stats.quiesces).toBe(0);
    expect(await s.links()).not.toContain(old.file);
  } finally { await s.close(); }
});

test("failed replacement import retains old selection, old view and durable nuke evidence", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.fixture.addOffer("broken", 1920);
    s.fixture.services.decider.inspect = async () => ({ fileIndex: null,
      facts: { identityVerified: false, cut: null, source: "web", proper: false }, reason: "Not the requested feature" });
    const report = s.report(old.source.id);
    await s.run(report.replacements[0].jobId);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(await s.links()).toEqual([old.file]);
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
    expect(s.repo.releaseHealth.evidence(old.source.id)?.reports).toHaveLength(1);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.fixture.stats.snatches).toBe(2);
  } finally { await s.close(); }
});

test("same-tier replacement cannot retire the old selected movie before exact Plex verification", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.fixture.addOffer("replacement", 1920);
    s.fixture.setPlexVerified(false);
    const report = s.report(old.source.id);
    await expect(s.run(report.replacements[0].jobId)).rejects.toThrow("Plex did not verify");
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(await s.links()).toContain(old.file);
    expect(s.repo.movies.publications()).toHaveLength(1);
    expect(s.repo.releaseHealth.evidence(old.source.id)).not.toBeNull();
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("a candidate reported nuked during Plex verification is fenced at final promotion", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.fixture.addOffer("replacement", 1920);
    const report = s.report(old.source.id);
    let reported = false;
    s.fixture.services.library.verify = async () => {
      if (!reported) {
        reported = true;
        const replacement = s.repo.movies.downloads(s.request.targetId).find((source) => source.offer_key === "replacement")!;
        s.report(replacement.id, "Replacement itself confirmed defective during publication");
      }
      return true;
    };
    await expect(s.run(report.replacements[0].jobId)).rejects.toThrow();
    expect(reported).toBe(true);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(await s.links()).toContain(old.file);
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("a nuke after promotion queues a follow-up instead of attaching repair to the finishing job", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.fixture.addOffer("replacement", 1920);
    const firstReport = s.report(old.source.id);
    let followup: ReturnType<typeof s.report> | null = null;
    const context = { ...s.context, execution: {
      key: "nuke-after-promotion", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>): Promise<T> => {
        if (name === "movie:finish") {
          const source = s.repo.movies.downloads(s.request.targetId).find((entry) => entry.offer_key === "replacement")!;
          followup = s.report(source.id, "Defect confirmed just after promotion");
        }
        return action();
      },
    } };
    await acquireMovie(context, s.repo.getJob(firstReport.replacements[0].jobId)!, s.fixture.services);
    expect(followup).not.toBeNull();
    const repair = (followup! as ReturnType<typeof s.report>).replacements[0];
    expect(repair.jobId).not.toBe(firstReport.replacements[0].jobId);
    expect(repair.shared).toBe(false);
    expect(s.repo.movies.acquisition(repair.acquisitionId)?.finished_at).toBeNull();
    s.fixture.addOffer("healthy-followup", 1920);
    await s.run(repair.jobId);
    const selected = s.repo.movies.version(s.repo.movies.target(s.request.targetId)!.active_version_id!)!;
    expect(await fs.readFile(path.join(s.root, "objects", selected.files[0].relativePath), "utf8")).toContain("healthy-followup");
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("cached selection of a newly nuked hash alias is skipped on replay instead of consuming the recovery", async () => {
  const s = await setup();
  try {
    const stalled = s.fixture.addOffer("stalled-before-original", 1920, true);
    const old = await s.initial(1280);
    const bad = s.repo.movies.downloads(s.request.targetId).find((entry) => entry.offer_key === stalled.key)!;
    s.fixture.clearOffers();
    const healthy = s.fixture.addOffer("healthy-upgrade", 1920);
    s.fixture.services.search = async () => ({ offers: [{ ...stalled, key: "new-provider-alias" }, healthy], warnings: [] });
    const acquisition = s.repo.movies.startAcquisition(s.request.targetId, "upgrade", (id) =>
      s.repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id } }).id).acquisition;
    const checkpoints = new Map<string, unknown>();
    let interrupted = false;
    const context = { ...s.context, execution: {
      key: "nuked-alias-replay", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>): Promise<T> => {
        if (checkpoints.has(name)) return checkpoints.get(name) as T;
        const result = await action();
        checkpoints.set(name, structuredClone(result));
        if (name === "movie:attempt:0:select" && !interrupted) {
          interrupted = true;
          s.report(bad.id, "Stalled candidate now confirmed defective");
          throw new Error("Process died after selection checkpoint");
        }
        return result;
      },
    } };
    const job = s.repo.getJob(acquisition.job_id!)!;
    await expect(acquireMovie(context, job, s.fixture.services)).rejects.toThrow("Process died");
    await acquireMovie(context, job, s.fixture.services);
    const selected = s.repo.movies.version(s.repo.movies.target(s.request.targetId)!.active_version_id!)!;
    expect(await fs.readFile(path.join(s.root, "objects", selected.files[0].relativePath), "utf8")).toContain("healthy-upgrade");
    expect(s.fixture.stats.snatches).toBe(3);
    expect(s.repo.releaseHealth.evidence(bad.id)).not.toBeNull();
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
  } finally { await s.close(); }
});

test("a trusted report invalidates a cached no-upgrade decision in a shared in-flight acquisition", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.fixture.addOffer("equal-repair", 1920);
    const acquisition = s.repo.movies.startAcquisition(s.request.targetId, "upgrade", (id) =>
      s.repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id } }).id).acquisition;
    const checkpoints = new Map<string, unknown>();
    let interrupted = false;
    const context = { ...s.context, execution: {
      key: "nuke-invalidates-rejection", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>): Promise<T> => {
        if (checkpoints.has(name)) return checkpoints.get(name) as T;
        const result = await action();
        checkpoints.set(name, structuredClone(result));
        if (name === "movie:attempt:0:select" && !interrupted) {
          interrupted = true;
          expect((result as { offerKey: string | null }).offerKey).toBeNull();
          const report = s.report(old.source.id);
          expect(report.replacements[0]).toMatchObject({ acquisitionId: acquisition.id, shared: true });
          throw new Error("Process died after cached no-upgrade decision");
        }
        return result;
      },
    } };
    const job = s.repo.getJob(acquisition.job_id!)!;
    await expect(acquireMovie(context, job, s.fixture.services)).rejects.toThrow("Process died");
    await acquireMovie(context, job, s.fixture.services);
    const next = s.repo.movies.target(s.request.targetId)!.active_version_id!;
    expect(next).not.toBe(old.versionId);
    expect(checkpoints.has(`movie:attempt:0:select:nuke:${old.versionId}`)).toBe(true);
    expect(await fs.readFile(old.file, "utf8")).toContain("original");
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("confirmed nuke recovery does not download a lower-tier candidate", async () => {
  const s = await setup();
  try {
    const old = await s.initial(1920);
    s.fixture.addOffer("lower-quality", 1280);
    const report = s.report(old.source.id);
    await s.run(report.replacements[0].jobId);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
    expect(await s.links()).toEqual([old.file]);
    expect(s.repo.releaseHealth.evidence(old.source.id)).not.toBeNull();
  } finally { await s.close(); }
});

for (const failure of ["identity", "cut", "language"] as const) {
  test(`nuke replacement still enforces actual ${failure} requirements`, async () => {
    const s = await setup({ ...defaultMovieQualityPolicy, cut: "theatrical", audioLanguages: ["en"] });
    try {
      const old = await s.initial();
      s.fixture.addOffer("replacement", 1920);
      const inspect = s.fixture.services.decider.inspect;
      s.fixture.services.decider.inspect = async (input) => {
        const result = await inspect(input);
        return { ...result, facts: { ...result.facts,
          ...(failure === "identity" ? { identityVerified: false } : {}),
          ...(failure === "cut" ? { cut: "extended" } : {}) } };
      };
      const probe = s.fixture.services.probe;
      s.fixture.services.probe = async (file) => {
        const result = await probe(file);
        if (failure === "language") for (const stream of result.streams)
          if (stream.codec_type === "audio") stream.tags = { language: "spa" };
        return result;
      };
      const report = s.report(old.source.id);
      await s.run(report.replacements[0].jobId);
      expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
      expect(await s.links()).toEqual([old.file]);
      expect(s.fixture.stats.removals).toBe(0);
    } finally { await s.close(); }
  });
}

test("unregistered tracker telemetry is not a confirmed nuke or an equal-tier publication exception", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.db.query("UPDATE media_torrents SET observation_json=? WHERE id=?")
      .run(JSON.stringify({ state: "seeding", message: "Unregistered torrent" }), old.source.id);
    expect(s.repo.releaseHealth.currentMovieNuke(s.request.targetId)).toBeNull();
    expect(s.repo.releaseHealth.isOfferBlocked(old.offer)).toBe(false);
    expect(canReplaceConfirmedNuke(s.db, s.request.targetId, old.versionId)).toBe(false);
    const before = s.fixture.stats.snatches;
    s.fixture.addOffer("equal", 1920);
    const acquisition = s.repo.movies.startAcquisition(s.request.targetId, "upgrade", (id) =>
      s.repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id } }).id).acquisition;
    await s.run(acquisition.job_id!);
    expect(s.fixture.stats.snatches).toBe(before);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
  } finally { await s.close(); }
});

test("reporting a failed earlier attempt does not misidentify the successfully imported source", async () => {
  const s = await setup();
  try {
    s.fixture.addOffer("stalled-before-original", 1920, true);
    const old = await s.initial(1280);
    const failed = s.repo.movies.downloads(s.request.targetId).find((entry) => entry.offer_key === "stalled-before-original")!;
    expect(failed).toBeDefined();
    expect(s.report(failed.id).replacements).toEqual([]);
    expect(s.repo.releaseHealth.currentMovieNuke(s.request.targetId)).toBeNull();
    expect(s.repo.releaseHealth.isContentFileBlocked(s.repo.movies.version(old.versionId)!.files[0].id)).toBe(false);
    expect(s.report(old.source.id).replacements).toHaveLength(1);
  } finally { await s.close(); }
});

test("version source is pinned independently of mutable acquisition decisions", async () => {
  const s = await setup();
  try {
    s.fixture.addOffer("stalled-before-original", 1920, true);
    const old = await s.initial(1280);
    const acquisition = s.repo.movies.acquisitions(s.request.targetId)[0];
    const failed = s.repo.movies.downloads(s.request.targetId).find((entry) => entry.offer_key === "stalled-before-original")!;
    s.repo.movies.decision(acquisition.id, { selection: { offerKey: failed.offer_key } });
    expect(s.repo.releaseHealth.versionSources(old.versionId).map((source) => source.id)).toEqual([old.source.id]);
    expect(() => s.repo.releaseHealth.bindMovieVersionSource(old.versionId, failed.id)).toThrow("another pinned source");
    expect(s.report(failed.id).replacements).toEqual([]);
    expect(s.report(old.source.id).replacements).toHaveLength(1);
  } finally { await s.close(); }
});

test("ambiguous legacy lineage cannot reuse possibly nuked bytes or grant an equal-tier exception", async () => {
  const s = await setup();
  try {
    s.fixture.addOffer("stalled-before-original", 1920, true);
    const old = await s.initial(1280);
    const acquisition = s.repo.movies.acquisitions(s.request.targetId)[0];
    const failed = s.repo.movies.downloads(s.request.targetId).find((entry) => entry.offer_key === "stalled-before-original")!;
    s.db.query("DELETE FROM app_state WHERE key=?").run(`release-health:movie-version-source:${old.versionId}`);
    s.repo.movies.decision(acquisition.id, {});
    expect(s.report(failed.id).replacements).toEqual([]);
    expect(s.repo.releaseHealth.currentMovieNuke(s.request.targetId)).toBeNull();
    expect(s.repo.releaseHealth.isContentFileBlocked(s.repo.movies.version(old.versionId)!.files[0].id)).toBe(true);
    expect(() => canReplaceConfirmedNuke(s.db, s.request.targetId, old.versionId)).toThrow("nuked release");
  } finally { await s.close(); }
});

test("a trusted report records evidence without reviving withdrawn demand", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    s.repo.movies.withdrawRequest(s.request.requestId);
    const before = s.repo.movies.acquisitions(s.request.targetId).length;
    expect(s.report(old.source.id).replacements).toEqual([]);
    expect(s.repo.releaseHealth.evidence(old.source.id)).not.toBeNull();
    expect(s.repo.movies.acquisitions(s.request.targetId)).toHaveLength(before);
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
  } finally { await s.close(); }
});

test("enqueue failure rolls back trusted evidence and report receipt rather than losing replacement intent", async () => {
  const s = await setup();
  try {
    const old = await s.initial();
    const command = { reportId: randomUUID(), sourceTorrentId: old.source.id, reason: "Confirmed defective release" };
    s.repo.createJob = () => { throw new Error("Outbox unavailable"); };
    expect(() => reportConfirmedReleaseNuke(s.repo, command, s.admin.id)).toThrow("Outbox unavailable");
    expect(s.repo.releaseHealth.evidence(old.source.id)).toBeNull();
    expect(s.repo.getJsonState(`release-health:report:${command.reportId}`)).toBeNull();
    expect(s.repo.movies.target(s.request.targetId)?.active_version_id).toBe(old.versionId);
  } finally { await s.close(); }
});

test("replacement policy does not use nuke evidence to waive quality or identity constraints", () => {
  const policy = { ...defaultMovieQualityPolicy, cut: "theatrical", audioLanguages: ["en"] };
  const current = fixtureFacts(1920);
  expect(assessConfirmedNukeReplacement(policy, current, fixtureFacts(1920)).allowed).toBe(true);
  expect(assessConfirmedNukeReplacement(policy, current, fixtureFacts(1280)).allowed).toBe(false);
  expect(assessConfirmedNukeReplacement(policy, current, { ...current, identityVerified: false }).allowed).toBe(false);
  expect(assessConfirmedNukeReplacement(policy, current, { ...current, cut: "extended" }).allowed).toBe(false);
  expect(assessConfirmedNukeReplacement(policy, current, { ...current, audioLanguages: ["spa"] }).allowed).toBe(false);
  expect(assessConfirmedNukeReplacement(policy, { ...current, proper: true }, current).allowed).toBe(false);
});
