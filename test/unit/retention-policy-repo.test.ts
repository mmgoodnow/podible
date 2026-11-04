import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { prepareMovieStorage } from "../../src/movies/storage";
import { retentionEvictionPlanSchema } from "../../src/media/retention-policy-types";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "retention-fence-")));
  const filename = path.join(root, "app.sqlite"), db = new Database(filename);
  db.exec("PRAGMA journal_mode=WAL"); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.getSettings();
  settings.movies.managedRoot = root; settings.movies.plexViewPath = "/fixture/movies"; repo.updateSettings(settings);
  const contentRoot = path.join(root, "objects"); await prepareMovieStorage(repo, contentRoot);
  const user = repo.upsertUser({ provider: "local", providerUserId: "fence", username: "viewer", isAdmin: true });
  const movie = { tmdbId: 1, title: "Owned movie", originalTitle: "Owned movie", year: 2020, overview: "Fixture",
    originalLanguage: "en", posterUrl: null, imdbId: null };
  const request = repo.movies.request(user.id, movie, defaultMovieQualityPolicy);
  const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
  const fileId = randomUUID(), relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`, canonical = path.join(contentRoot, relativePath);
  await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, Buffer.alloc(5000, 65));
  const facts = { identityVerified: true, source: "web" as const, cut: null, proper: false, width: 1280, height: 720,
    videoCodec: "h264", dynamicRange: "sdr" as const, audioLanguages: ["en"] };
  const versionId = repo.movies.recordVersion(acquisition.id, facts, [{ id: fileId, relativePath, sizeBytes: 5000, probe: {} }]);
  repo.movies.selectVersion(request.targetId, versionId, 0); repo.movies.finishAcquisition(acquisition.id);
  const old = new Date(Date.now() - 90 * 86_400_000).toISOString();
  db.query("UPDATE movie_requests SET created_at=?").run(old); db.query("UPDATE retention_events SET created_at=?").run(old);
  const stat = await fs.stat(canonical, { bigint: true });
  const plan = retentionEvictionPlanSchema.parse({ id: randomUUID(), targetId: request.targetId, mode: "operator", reason: "Fixture eviction",
    expectedRevision: 1, expectedTargetRevision: 1, activeVersionId: versionId, intentRevision: 2, targetRevision: 2,
    settingsHash: createHash("sha256").update(JSON.stringify(repo.readSettings())).digest("hex"),
    contentId: JSON.parse(await fs.readFile(path.join(contentRoot, ".podible-content.json"), "utf8")).id, device: String(stat.dev),
    files: [{ id: fileId, relativePath, sizeBytes: 5000, device: String(stat.dev), inode: String(stat.ino),
      mtimeNs: String(stat.mtimeNs), ctimeNs: String(stat.ctimeNs), allocatedBytes: Number(stat.blocks * 512n), reclaimableBytes: Number(stat.blocks * 512n) }],
    createdAt: new Date().toISOString() });
  const eviction = repo.retention.queueEviction(plan, (evictionId) => repo.createJob({ type: "retention_cleanup", payload: { evictionId } }).id);
  return { db, repo, root, filename, user, movie, request, fileId, versionId, canonical, facts, eviction,
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("immediate eviction fence excludes another SQLite writer through the short synchronous effect", async () => {
  await using f = await fixture(); const other = new Database(f.filename);
  other.exec("PRAGMA busy_timeout=0");
  try {
    const result = f.repo.retention.withEvictionFence(f.eviction.id, () => {
      expect(() => other.query("UPDATE movie_requests SET created_at=created_at WHERE target_id=?").run(f.request.targetId)).toThrow(/locked|busy/i);
      return 42;
    });
    expect(result).toBe(42);
    expect(() => other.query("UPDATE movie_requests SET created_at=created_at WHERE target_id=?").run(f.request.targetId)).not.toThrow();
  } finally { other.close(); }
});

test("eviction fence rejects async and thenable effects and does not run after a newer request", async () => {
  await using f = await fixture(); let called = false;
  expect(() => f.repo.retention.withEvictionFence(f.eviction.id, async () => { called = true; })).toThrow("synchronous");
  expect(called).toBe(false);
  expect(() => f.repo.retention.withEvictionFence(f.eviction.id, () => ({ then() {} }))).toThrow("thenable");
  f.repo.movies.request(f.user.id, f.movie, defaultMovieQualityPolicy);
  expect(() => f.repo.retention.withEvictionFence(f.eviction.id, () => { called = true; })).toThrow("superseded");
  expect(called).toBe(false);
  expect((await fs.stat(f.canonical)).size).toBe(5000);
});

test("superseded eviction restores a proven complete selection without retiring restored canonical identities", async () => {
  await using f = await fixture();
  f.repo.movies.request(f.user.id, f.movie, defaultMovieQualityPolicy);
  const completed = f.repo.retention.supersedeEviction(f.eviction.id, { verifiedPresentObjectIds: [f.fileId], verifiedAbsentObjectIds: [] });
  expect(completed.finishedAt).not.toBeNull(); expect(completed.result).toMatchObject({ outcome: "superseded", selectionRestored: true });
  expect(f.repo.retention.get({ namespace: "movie-target", id: String(f.request.targetId) })).toMatchObject({ desiredPresence: "present", actualPresence: "present" });
  expect(f.repo.movies.target(f.request.targetId)!.active_version_id).toBe(f.versionId);
  expect(f.repo.retention.retiredFileIds(f.request.targetId)).toEqual([]);
  const events = f.repo.retention.events({ namespace: "movie-target", id: String(f.request.targetId) });
  expect(f.repo.retention.supersedeEviction(f.eviction.id, { verifiedPresentObjectIds: [f.fileId], verifiedAbsentObjectIds: [] })).toEqual(completed);
  expect(f.repo.retention.events({ namespace: "movie-target", id: String(f.request.targetId) })).toEqual(events);
});

test("supersession retains only verified absence tombstones and never overwrites a newer selected version", async () => {
  await using f = await fixture();
  f.repo.movies.request(f.user.id, f.movie, defaultMovieQualityPolicy);
  const acquisition = f.repo.movies.startAcquisition(f.request.targetId, "recovery", () => f.repo.createJob({ type: "metadata_hydration" }).id).acquisition;
  const id = randomUUID();
  const newer = f.repo.movies.recordVersion(acquisition.id, f.facts, [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 5000, probe: {} }]);
  f.repo.movies.selectVersion(f.request.targetId, newer, 2); f.repo.movies.finishAcquisition(acquisition.id);
  await fs.unlink(f.canonical);
  f.repo.retention.supersedeEviction(f.eviction.id, { verifiedPresentObjectIds: [], verifiedAbsentObjectIds: [f.fileId] });
  expect(f.repo.movies.target(f.request.targetId)!.active_version_id).toBe(newer);
  expect(f.repo.retention.retiredFileIds(f.request.targetId)).toEqual([f.fileId]);
  expect(f.repo.retention.get({ namespace: "movie-target", id: String(f.request.targetId) })).toMatchObject({ desiredPresence: "present", actualPresence: "unknown" });
});

test("supersession cannot bless absent identities outside the immutable plan", async () => {
  await using f = await fixture(); f.repo.movies.request(f.user.id, f.movie, defaultMovieQualityPolicy);
  expect(() => f.repo.retention.supersedeEviction(f.eviction.id, { verifiedPresentObjectIds: [], verifiedAbsentObjectIds: [randomUUID()] })).toThrow("partition");
  expect(f.repo.retention.eviction(f.eviction.id)!.finishedAt).toBeNull();
});

test("seed bindings are scoped to exact removal-file acquisitions, retaining all affected sibling bindings", async () => {
  await using f = await fixture();
  const first = f.repo.movies.acquisitions(f.request.targetId)[0]!;
  f.repo.movies.bindDownload(first.id, { clientId: "fixture-client", nativeId: "a".repeat(40) }, { rootId: "movies", relativePath: "old-seed" }, "old-release");
  f.repo.movies.request(f.user.id, f.movie, defaultMovieQualityPolicy);
  const next = f.repo.movies.startAcquisition(f.request.targetId, "recovery", () => f.repo.createJob({ type: "metadata_hydration" }).id).acquisition;
  const id = randomUUID();
  f.repo.movies.recordVersion(next.id, f.facts, [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 5000, probe: {} }]);
  f.repo.movies.finishAcquisition(next.id);
  for (const hash of ["b", "c"]) f.repo.movies.bindDownload(next.id, { clientId: "fixture-client", nativeId: hash.repeat(40) },
    { rootId: "movies", relativePath: `new-seed-${hash}` }, `new-release-${hash}`);
  expect(f.repo.retention.seedDownloads(f.request.targetId, [id]).map((row) => row.native_id).sort()).toEqual(["b".repeat(40), "c".repeat(40)]);
  expect(f.repo.retention.seedDownloads(f.request.targetId, [id, id])).toHaveLength(2);
  expect(f.repo.retention.seedDownloads(999, [id])).toEqual([]);
  expect(f.repo.retention.seedDownloads(f.request.targetId, [f.fileId]).map((row) => row.native_id)).toEqual(["a".repeat(40)]);
});
