import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { adoptContentFile } from "../../src/media/content-store";
import { reuseDigest } from "../../src/media/reuse-plan";
import { prepareMovieStorage } from "../../src/movies/storage";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { tvPackRetirementNominationKey } from "../../src/tv/pack-retirement-nominations";
import { publishTvVersion, tvRoots, type TvPublicationGuard } from "../../src/tv/publication";
import type { TvSource } from "../../src/tv/source-reuse";
import type { TvCatalogEntry } from "../../src/tv/types";

const clientId = `transmission:${randomUUID()}`;
const fingerprint = "a".repeat(64);
const catalog: TvCatalogEntry = {
  key: "tvdb:100", tvdbId: 100, tmdbId: 200, title: "Fixture Season", aliases: [], year: 2020, anime: false, relations: [],
  episodes: [301, 302, 303, 304].map((id, index) => ({ key: `tvdb:${id}`, title: `Episode ${index + 1}`,
    airDate: "2020-01-01", runtimeMinutes: 42, numbering: [{ scheme: "tvdb:aired", season: index < 3 ? 1 : 2, episode: index < 3 ? index + 1 : 1, absolute: index + 1 }],
  })),
};

const library = { scan: async () => {}, verify: async () => true };
type SourceFault = "legacy" | "unbound" | "wrong-location" | "extra-member" | "wrong-index" | "foreign-client";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-pack-nomination-")));
  const dbPath = path.join(root, "app.sqlite");
  const db = new Database(dbPath);
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-pack-nomination", username: "viewer", isAdmin: false });
  const request = repo.tv.request(user.id, catalog, defaultMovieQualityPolicy, catalog.episodes.map(ep => ep.key));
  const { targetId, requestId, episodeIds } = request;
  const roots = tvRoots(root, "/data/views/plex/tv");
  await prepareMovieStorage(repo, roots.contentRoot);

  async function makeVersion(coverage: number[][], options: { pack?: true; fault?: SourceFault } = {}) {
    const sourceRoot = path.join(root, randomUUID());
    await fs.mkdir(sourceRoot);
    const acquisition = repo.tv.startAcquisition(targetId, options.pack ? "upgrade" : "initial", id =>
      repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id } }).id, requestId).acquisition;
    if (options.pack) repo.tv.decision(acquisition.id, { seasonPromotion: { season: 1, revision: revision(), fingerprint, episodeIds: coverage.flat() } });
    const ref = { clientId: options.fault === "foreign-client" ? `transmission:${randomUUID()}` : clientId,
      nativeId: createHash("sha1").update(sourceRoot).digest("hex") };
    const offerKey = `offer:${ref.nativeId}`;
    const location = { rootId: "tv" as const, relativePath: path.basename(sourceRoot) };
    // This attempt is never the recorded source, even when it is the only binding.
    repo.tv.bindDownload(acquisition.id, { clientId, nativeId: createHash("sha1").update(`${sourceRoot}-sibling`).digest("hex") },
      { ...location, relativePath: `${location.relativePath}-sibling` }, "sibling-offer");
    const torrentId = options.fault === "unbound" ? null : repo.tv.bindDownload(acquisition.id, ref, location, offerKey);
    const files = [], manifestFiles: TvSource["manifest"]["files"] = [];
    for (const [index, episodeIds] of coverage.entries()) {
      const name = `episode-${index + 1}.mkv`, sourcePath = path.join(sourceRoot, name);
      const bytes = Buffer.from(`fixture episode ${episodeIds.join(",")}`);
      await fs.writeFile(sourcePath, bytes);
      const stored = await adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot, sourcePath, fileId: randomUUID(), expectedSize: bytes.length });
      const { size, mtimeMs, ctimeMs, dev: device, ino: inode } = await fs.stat(sourcePath);
      files.push({ ...stored, episodeIds, probe: {} });
      manifestFiles.push({ path: name, size, identity: { mtimeMs, ctimeMs, device, inode } });
    }
    if (options.fault === "extra-member") manifestFiles.push({ ...manifestFiles[0]!, path: "extra.mkv" });
    const source: TvSource = { ref, location: options.fault === "wrong-location" ? { ...location, relativePath: "wrong-layout" } : location,
      offerKey, title: "Fixture", provider: "fixture", manifest: { infoHash: ref.nativeId, name: "Fixture",
        size: manifestFiles.reduce((sum, file) => sum + file.size, 0), files: manifestFiles } };
    const plan = { files: coverage.map((episodeIds, index) => ({ fileIndex: options.fault === "wrong-index" ? 99 : index, episodeIds })) };
    const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true, plan,
      ...(options.fault === "legacy" ? {} : { source }) }, files);
    repo.tv.finishAcquisition(acquisition.id);
    return { versionId, acquisitionId: acquisition.id, torrentId, source, files };
  }

  const revision = () => repo.tv.target(targetId)!.revision;
  const publish = (versionId: number, opts: Partial<Pick<Parameters<typeof publishTvVersion>[0], "guard" | "library" | "assertActive" | "pendingIsExpected">> = {}) =>
    publishTvVersion({ repo: repo.tv, targetId, roots, library, versionId, ...opts });
  const guard = (): TvPublicationGuard => ({ expectedRevision: revision(), validate: () => {}, verifyRoster: async () => true });
  const read = (versionId: number) => repo.tv.packRetirementNomination(targetId, versionId);
  const previous = async (fault?: SourceFault) => {
    const old: Awaited<ReturnType<typeof makeVersion>>[] = [];
    for (const [index, episodeId] of episodeIds.entries()) {
      const version = await makeVersion([[episodeId]], { fault: index === 0 ? fault : undefined });
      await publish(version.versionId);
      old.push(version);
    }
    return old;
  };
  return { dbPath, db, repo, targetId, requestId, roots, episodeIds, revision, makeVersion, publish, guard, read, previous,
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("verified pack records exact pre-promotion provenance, survives post-commit interruption and replays once", async () => {
  const s = await fixture();
  try {
    const old = await s.previous();
    const pack = await s.makeVersion([[s.episodeIds[0]!, s.episodeIds[1]!], [s.episodeIds[2]!]], { pack: true });
    const guard = s.guard();
    const before = s.repo.tv.downloads(s.targetId);
    const payloads = await Promise.all([...old, pack].flatMap(v => v.files).map(async file => {
      const absolute = path.join(s.roots.contentRoot, file.relativePath);
      return { absolute, bytes: await fs.readFile(absolute), inode: (await fs.stat(absolute)).ino };
    }));
    await expect(s.publish(pack.versionId, { guard, assertActive: () => {
      if (s.read(pack.versionId) && !s.repo.tv.publications().length) throw new Error("interrupted after commit");
    } })).rejects.toThrow("interrupted after commit");
    expect(s.revision()).toBe(guard.expectedRevision + 1);

    const proof = (v: typeof pack) => ({ versionId: v.versionId, acquisitionId: v.acquisitionId, torrentId: v.torrentId!,
      clientId, infoHash: v.source.ref.nativeId, location: v.source.location, manifestDigest: reuseDigest(v.source.manifest),
      members: v.files.map((file, fileIndex) => ({ fileIndex, contentFileId: file.id, episodeIds: file.episodeIds })) });
    const expected = { targetId: s.targetId, requestId: s.requestId, publishedRevision: s.revision(), season: 1, promotionFingerprint: fingerprint,
      episodeIds: s.episodeIds.slice(0, 3), pack: proof(pack), superseded: old.slice(0, 3).map(proof) };
    const key = tvPackRetirementNominationKey(s.targetId, pack.versionId);
    const state = () => s.db.query("SELECT * FROM app_state WHERE key=?").get(key);
    const originalReceipt = state();
    const reopened = new Database(s.dbPath, { readonly: true });
    try { expect(new BooksRepo(reopened).tv.packRetirementNomination(s.targetId, pack.versionId)).toEqual(expected); }
    finally { reopened.close(); }
    await expect(s.publish(pack.versionId, { guard: { ...guard, verifyRoster: async () => { throw new Error("unexpected replay verify"); } } })).resolves.toBe(true);
    expect(state()).toEqual(originalReceipt);
    expect(s.repo.tv.downloads(s.targetId)).toEqual(before);
    expect(s.repo.tv.activeEpisodes(s.targetId).map(row => row.version_id)).toEqual([pack.versionId, pack.versionId, pack.versionId, old[3]!.versionId]);
    for (const payload of payloads) {
      expect(await fs.readFile(payload.absolute)).toEqual(payload.bytes);
      expect((await fs.stat(payload.absolute)).ino).toBe(payload.inode);
    }
  } finally { await s.close(); }
});

test("pending Plex, rejected roster, and a transaction rollback create no nomination", async () => {
  const s = await fixture();
  try {
    await s.previous();
    const selected = s.repo.tv.activeEpisodes(s.targetId);
    const pack = await s.makeVersion(s.episodeIds.slice(0, 3).map(id => [id]), { pack: true });
    const guard = s.guard();
    await expect(s.publish(pack.versionId, { guard, pendingIsExpected: true, library: { ...library, verify: async () => false } })).resolves.toBe(false);
    expect(s.read(pack.versionId)).toBeNull();
    await expect(s.publish(pack.versionId, { guard: { ...guard, verifyRoster: async () => false } })).rejects.toThrow("roster changed");
    expect(s.read(pack.versionId)).toBeNull();
    await expect(s.publish(pack.versionId, { guard, assertActive: () => {
      if (s.read(pack.versionId)) throw new Error("interrupted before commit");
    } })).rejects.toThrow("interrupted before commit");
    expect(s.revision()).toBe(guard.expectedRevision);
    expect(s.repo.tv.activeEpisodes(s.targetId)).toEqual(selected);
    expect(s.repo.tv.publications()).toEqual([]);
    expect(s.read(pack.versionId)).toBeNull();
    await expect(s.publish(pack.versionId, { guard })).resolves.toBe(true);
    expect(s.read(pack.versionId)?.superseded).toHaveLength(3);
  } finally { await s.close(); }
});

test("invalid legacy source receipts and bindings do not block playback promotion", async () => {
  const s = await fixture();
  try {
    const old = await s.previous();
    s.db.query("UPDATE media_torrents SET location_json=? WHERE id=?").run("{invalid", old[0]!.torrentId);
    s.db.query("UPDATE tv_versions SET facts_json=json_set(facts_json,'$.source.manifest.files',NULL) WHERE id=?").run(old[1]!.versionId);
    const pack = await s.makeVersion(s.episodeIds.slice(0, 3).map(id => [id]), { pack: true });
    await expect(s.publish(pack.versionId, { guard: s.guard() })).resolves.toBe(true);
    expect(s.read(pack.versionId)?.superseded.map(source => source.versionId)).toEqual([old[2]!.versionId]);
    expect(s.repo.tv.activeEpisodes(s.targetId).slice(0, 3).map(row => row.version_id)).toEqual(Array(3).fill(pack.versionId));
  } finally { await s.close(); }
});

test("a real nomination write error rolls back selection and surfaces to the caller", async () => {
  const s = await fixture();
  try {
    await s.previous();
    const selected = s.repo.tv.activeEpisodes(s.targetId);
    const pack = await s.makeVersion(s.episodeIds.slice(0, 3).map(id => [id]), { pack: true });
    s.db.exec(`CREATE TEMP TRIGGER fail_nomination BEFORE INSERT ON app_state
      WHEN NEW.key LIKE 'tv-pack-retirement:nomination:v1:%' BEGIN SELECT RAISE(ABORT, 'fixture nomination write error'); END`);
    await expect(s.publish(pack.versionId, { guard: s.guard() })).rejects.toThrow("fixture nomination write error");
    expect(s.repo.tv.activeEpisodes(s.targetId)).toEqual(selected);
    expect(s.read(pack.versionId)).toBeNull();
  } finally { await s.close(); }
});

for (const fault of ["legacy", "unbound", "wrong-location", "extra-member", "wrong-index", "foreign-client"] as const) {
  test(`guarded publication skips a displaced source with ${fault} provenance`, async () => {
    const s = await fixture();
    try {
      const old = await s.previous(fault);
      const pack = await s.makeVersion(s.episodeIds.slice(0, 3).map(id => [id]), { pack: true });
      await expect(s.publish(pack.versionId, { guard: s.guard() })).resolves.toBe(true);
      expect(s.read(pack.versionId)?.superseded.map(candidate => candidate.torrentId)).toEqual(old.slice(1, 3).map(v => v.torrentId!));
    } finally { await s.close(); }
  });
}

for (const mode of ["unguarded", "unbound-pack", "partial-roster", "invalid-receipt"] as const) {
  test(`${mode} publication does not nominate any displaced sources`, async () => {
    const s = await fixture();
    try {
      await s.previous();
      const pack = await s.makeVersion(s.episodeIds.slice(0, mode === "partial-roster" ? 2 : 3).map(id => [id]),
        { pack: true, fault: mode === "unbound-pack" ? "unbound" : undefined });
      if (mode === "invalid-receipt") s.repo.setJsonState(tvPackRetirementNominationKey(s.targetId, pack.versionId), { invalid: true });
      const guard = s.guard();
      await expect(s.publish(pack.versionId, mode === "unguarded" ? {} : { guard })).resolves.toBe(true);
      expect(s.read(pack.versionId)).toBeNull();
      if (mode === "unguarded") {
        await expect(s.publish(pack.versionId, { guard })).resolves.toBe(true);
        expect(s.read(pack.versionId)).toBeNull();
      }
    } finally { await s.close(); }
  });
}
