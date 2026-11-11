import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { TvRetentionService } from "../../src/media/tv-retention";
import { runTvRetentionPressure } from "../../src/media/tv-retention-pressure";
import type { TvRetentionAudit } from "../../src/media/tv-retention-types";
import type { RetentionDisk } from "../../src/media/retention-policy-types";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-pressure-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.managedRoot = root; settings.tv.plexViewPath = "/fixture/tv"; settings.tv.plexSectionId = "3";
  repo.updateSettings(settings); await prepareMovieStorage(repo, path.join(root, "objects"));
  const user = repo.upsertUser({ provider: "local", providerUserId: "pressure", username: "pressure", isAdmin: true });
  for (let number = 1; number <= 4; number++) {
    const requested = repo.tv.request(user.id, { key: `tvdb:${number}`, title: `Season ${number}`, tvdbId: number, tmdbId: number,
      aliases: [], year: 2020, anime: false, relations: [], episodes: [{ key: `tvdb:${number}001`, title: "Episode", airDate: "2020-01-01",
        runtimeMinutes: 20, numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null }] }] }, settings.movies.qualityPolicy, [`tvdb:${number}001`]);
    const a = repo.tv.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id, requested.requestId).acquisition;
    const id = randomUUID(), version = repo.tv.recordVersion(a.id, { identityVerified: true }, [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 1, probe: {}, episodeIds: requested.episodeIds }]);
    repo.tv.stagePublication(requested.targetId, version, 0); repo.tv.promotePublication(requested.targetId, version, 0); repo.tv.clearPublication(requested.targetId, version); repo.tv.finishAcquisition(a.id);
  }
  const ctx = { repo, getSettings: () => repo.getSettings(), execution: { key: "pressure-test", assertActive() {},
    async run<T>(_name: string, action: () => T | Promise<T>) { return action(); }, async sleep() {} } };
  const job = repo.createJob({ type: "tv_retention_pressure" });
  const disk = (available: number | null): RetentionDisk => ({ state: available === null ? "unknown" : "known", filesystem: "fixture", device: "1",
    totalBytes: 100 * 1024 ** 3, availableBytes: available, observedAt: new Date().toISOString(), reason: "Fixture space" });
  return { db, root, repo, settings, ctx, job, disk, async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("healthy/unknown TV space is a read-only no-op without metadata audits or writes", async () => {
  await using f = await fixture();
  const audit = spyOn(TvRetentionService.prototype, "audit").mockImplementation(async () => { throw new Error("No audit expected"); });
  try {
    for (const free of [100 * 1024 ** 3, null]) {
      const before = f.db.query("SELECT total_changes() AS n").get();
      await runTvRetentionPressure(f.ctx, f.job, { observeDisk: async () => f.disk(free) });
      expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    }
    expect(audit).not.toHaveBeenCalled();
  } finally { audit.mockRestore(); }
});

test("TV pressure scans bounded whole targets, ranks low plays/old watch, and rechecks space before queueing", async () => {
  await using f = await fixture();
  // Evidence and destructive execution are covered by the real whole-season suite; this tests intake/ranking only.
  const audit = spyOn(TvRetentionService.prototype, "audit").mockImplementation(async scope => ({ season: scope,
    eligible: true, revision: "a".repeat(64), watch: { episodes: [{ playCount: scope.targetId === 3 ? 1 : 5, lastViewedAt: "2020-01-01" }] },
  } as unknown as TvRetentionAudit));
  const evict = spyOn(TvRetentionService.prototype, "evict").mockResolvedValue({ status: "queued", evictionId: randomUUID(), jobId: 99, blockers: [] });
  try {
    await runTvRetentionPressure(f.ctx, f.job, { observeDisk: async () => f.disk(1) });
    expect(audit.mock.calls.map(([scope]) => scope.targetId)).toEqual([1, 2, 3]);
    expect(evict.mock.calls[0]![0].targetId).toBe(3);
    expect(f.repo.getJsonState<number>("tv-retention-pressure-cursor")).toBe(3);
    evict.mockClear(); f.repo.setJsonState("tv-retention-pressure-cursor", 0);
    let reads = 0;
    await runTvRetentionPressure(f.ctx, f.job, { observeDisk: async () => f.disk(++reads === 3 ? 100 * 1024 ** 3 : 1) });
    expect(reads).toBe(3);
    expect(evict).not.toHaveBeenCalled();
    f.repo.setJsonState("tv-retention-pressure-cursor", 0);
    const mount = path.join(f.root, "objects", ".podible-content.json"); await fs.unlink(mount);
    audit.mockClear();
    await runTvRetentionPressure(f.ctx, f.job, { observeDisk: async () => { throw new Error("Missing mount must short circuit"); } });
    expect(audit).not.toHaveBeenCalled();
  } finally { audit.mockRestore(); evict.mockRestore(); }
});
