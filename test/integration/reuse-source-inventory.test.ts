import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { crossSeedPlan } from "../../src/media/cross-seed";
import { crossSeedCommandSchema } from "../../src/media/cross-seed-state";
import { prepareCanonicalDownload, sealCanonicalDownload, finalizeCanonicalDownloadFile } from "../../src/media/canonical-download";
import { completedCanonicalReuseSources } from "../../src/media/reuse-source-inventory";
import { captureReuseRoot, verifyContentReuse } from "../../src/media/reuse-layout";
import type { TorrentClient } from "../../src/media/torrent-client";
import type { TvCatalogEntry } from "../../src/tv/types";

async function fixture(options: { sealed?: boolean; published?: boolean; journal?: boolean } = {}) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "reuse-source-inventory-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.managedRoot = root; repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "inventory", username: "inventory", isAdmin: true });
  const entry: TvCatalogEntry = { key: "tvdb:123", tvdbId: 123, tmdbId: 456, title: "Complete Pack", aliases: [],
    year: 2020, anime: false, source: "fixture", relations: [], episodes: [1, 2, 3].map(i => ({
      key: `tvdb:${i}`, title: `Chapter ${i}`, airDate: "2020-01-01", runtimeMinutes: 20,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: i, absolute: i, source: "fixture" }],
    })) };
  const requested = repo.tv.request(user.id, entry, settings.movies.qualityPolicy, entry.episodes.map(ep => ep.key));
  const acquisition = repo.tv.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "tv_acquire" }).id, requested.requestId).acquisition;
  const contentRoot = path.join(root, "objects"), downloadRoot = path.join(root, "downloads"), sourceRoot = path.join(downloadRoot, "pack");
  await prepareMovieStorage(repo, contentRoot); await fs.mkdir(sourceRoot, { recursive: true });
  const payloads = [Buffer.from("AAAA"), Buffer.from("BBBBB"), Buffer.from("CCCCCC")];
  const data = Buffer.concat(payloads), pieces: Buffer[] = [];
  for (let i = 0; i < data.length; i += 4) pieces.push(createHash("sha1").update(data.subarray(i, i + 4)).digest());
  const metainfo = bencode.encode({ info: { name: "Pack", "piece length": 4, pieces: Buffer.concat(pieces),
    files: payloads.map((bytes, i) => ({ path: [`episode-${i + 1}.mkv`], length: bytes.length })) } });
  const clientId = `transmission:${randomUUID()}`;
  const plan = (await prepareCanonicalDownload({ contentRoot, sourceRoot, clientId, metainfo, allowCreate: true }))!;
  for (const [i, file] of plan.files.entries()) await fs.writeFile(path.join(sourceRoot, file.path), payloads[i]!);
  const ref = { clientId, nativeId: plan.infoHash }, location = { rootId: "tv", relativePath: "pack" };
  if (options.sealed !== false) await sealCanonicalDownload(contentRoot, sourceRoot, { id: clientId, sealDownload: async () => {} } as unknown as TorrentClient, ref);
  const torrentId = repo.tv.bindDownload(acquisition.id, ref, location, "primary-offer");
  repo.tv.observeDownload(torrentId, { ref, location, state: "inactive", complete: true, bytesDone: data.length,
    sizeBytes: data.length, message: null, downloadBytesPerSecond: 0, observedAt: new Date().toISOString() });
  const source = { ref, location, offerKey: "primary-offer", title: "Complete Pack", provider: "Original tracker",
    manifest: { infoHash: plan.infoHash, name: "Pack", size: data.length, files: await Promise.all(plan.files.map(async file => {
      const current = await fs.stat(path.join(contentRoot, file.relativePath!));
      return { path: file.path, size: file.size, identity: { device: current.dev, inode: current.ino, mtimeMs: current.mtimeMs, ctimeMs: current.ctimeMs } };
    })) } };
  if (options.published !== false) {
    const file = plan.files[0]!;
    const stored = await finalizeCanonicalDownloadFile(contentRoot, sourceRoot, path.join(sourceRoot, file.path), file.size);
    const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true, source }, [{ ...stored!, probe: {}, episodeIds: [requested.episodeIds[0]!] }]);
    repo.tv.stagePublication(requested.targetId, version, 0); repo.tv.promotePublication(requested.targetId, version, 0);
  }
  if (options.journal) {
    const journal = new SQLiteStorage(repo.executionDatabasePath()); await journal.initialize();
    const runId = randomUUID(), now = Date.now();
    await journal.createRun({ id: runId, workflow: "media-tv_acquire-v1", input: { jobId: acquisition.job_id! }, status: "completed",
      createdAt: now, updatedAt: now, startedAt: now, finishedAt: now, output: null, error: null, wakeAt: null, waitingFor: null, idempotencyKey: null });
    await journal.saveStepResult({ id: randomUUID(), runId, name: "tv:attempt:0:source-evidence", status: "completed",
      output: source, error: null, attempts: 1, createdAt: now, updatedAt: now, startedAt: now, history: [], kind: "step" });
    repo.attachJobExecution(acquisition.job_id!, runId, 0); await journal.close();
  }
  const scope = { media: "tv" as const, targetId: requested.targetId };
  const command = crossSeedCommandSchema.parse({ operationId: randomUUID(), scope, releaseId: 1,
    policy: { minimumReuseRatio: 1, maximumExtraBytes: 0 } });
  return { root, db, repo, contentRoot, downloadRoot, sourceRoot, source, plan, torrentId, metainfo, command, requested, scope,
    inventory: async () => completedCanonicalReuseSources({ repo, scope, root: await captureReuseRoot({ rootPath: contentRoot, markerName: ".podible-content.json" }),
      downloadRoot, sizes: payloads.map(bytes => bytes.length) }),
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("complete canonical pack with only one published episode supplies all members and passes exact piece verification", async () => {
  const f = await fixture();
  try {
    expect(f.repo.tv.activeEpisodes(f.scope.targetId)).toHaveLength(1);
    expect(f.repo.crossSeed.files(f.scope, [4, 5, 6])).toHaveLength(1);
    expect(await f.inventory()).toHaveLength(3);
    const plan = await crossSeedPlan(f.repo, f.repo.getSettings(), f.command, f.metainfo);
    expect(plan.summary.likelyReuseBytes).toBe(15);
    expect(plan.sources).toHaveLength(3);
    const proof = await verifyContentReuse(plan);
    expect(proof.status).toBe("ready"); expect(proof.summary.verifiedReuseBytes).toBe(15);
    expect(f.repo.crossSeed.files(f.scope, [4, 5, 6])).toHaveLength(1); // Inventory does not publish or adopt.
  } finally { await f.close(); }
});

test("completed pre-publication pack uses bounded durable source evidence without a version", async () => {
  const f = await fixture({ published: false, journal: true });
  try {
    expect(f.repo.crossSeed.files(f.scope, [4, 5, 6])).toHaveLength(0);
    expect(await f.inventory()).toHaveLength(3);
  } finally { await f.close(); }
});

test("incomplete or unsealed canonical downloads cannot enter the source pool", async () => {
  const f = await fixture({ sealed: false, published: false });
  try { expect(await f.inventory()).toHaveLength(0); }
  finally { await f.close(); }
  const complete = await fixture();
  try {
    const snapshot = { ...complete.source, state: "downloading", complete: false, bytesDone: 3, sizeBytes: 15, message: null };
    complete.db.query("UPDATE media_torrents SET observation_json=? WHERE id=?").run(JSON.stringify(snapshot), complete.torrentId);
    expect(await complete.inventory()).toHaveLength(0);
  } finally { await complete.close(); }
});

test("same-size mutation and replaced inodes of unpublished members are not trusted", async () => {
  const f = await fixture();
  try {
    await fs.writeFile(path.join(f.contentRoot, f.plan.files[1]!.relativePath!), "xxxxx");
    expect((await f.inventory()).map(file => file.size)).toEqual([4, 6]);
    const replacement = path.join(f.contentRoot, f.plan.files[2]!.relativePath!);
    await fs.unlink(replacement); await fs.writeFile(replacement, "CCCCCC");
    await expect(f.inventory()).rejects.toThrow("canonical regular object");
  } finally { await f.close(); }
});

test("foreign ownership, other targets, changed client identities and escaped roots cannot contribute files", async () => {
  const f = await fixture();
  try {
    f.db.query("UPDATE media_torrents SET ownership_tag='Podible Movies' WHERE id=?").run(f.torrentId);
    expect(await f.inventory()).toHaveLength(0);
    f.db.query("UPDATE media_torrents SET ownership_tag='Podible TV',client_id=? WHERE id=?").run(`transmission:${randomUUID()}`, f.torrentId);
    expect(await f.inventory()).toHaveLength(0);
    f.db.query("UPDATE media_torrents SET client_id=?,location_json=? WHERE id=?")
      .run(f.source.ref.clientId, JSON.stringify({ rootId: "tv", relativePath: "../foreign" }), f.torrentId);
    await expect(f.inventory()).rejects.toThrow("escaped");
    expect(f.repo.crossSeed.sourceDownloads({ media: "tv", targetId: f.scope.targetId + 1 })).toEqual([]);
  } finally { await f.close(); }
});

test("sealed objects without finalization or source identity proof and legacy regular members remain excluded", async () => {
  const f = await fixture({ published: false });
  try {
    expect(await f.inventory()).toHaveLength(0);
    await fs.rm(path.join(f.contentRoot, ".podible-downloads"), { recursive: true });
    for (const file of f.plan.files) {
      const view = path.join(f.sourceRoot, file.path);
      await fs.unlink(view); await fs.writeFile(view, Buffer.alloc(file.size));
    }
    expect(await f.inventory()).toHaveLength(0);
  } finally { await f.close(); }
});

test("canonical mount identity and source metainfo identity cannot be substituted", async () => {
  const f = await fixture();
  try {
    const planDirectory = path.join(f.contentRoot, ".podible-downloads");
    const [name] = await fs.readdir(planDirectory), manifestPath = path.join(planDirectory, name!);
    const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
    await fs.writeFile(manifestPath, JSON.stringify({ ...manifest, infoHash: "f".repeat(40) }));
    await expect(f.inventory()).rejects.toThrow("completed registration");
    await fs.writeFile(manifestPath, JSON.stringify(manifest));
    await fs.writeFile(path.join(f.contentRoot, ".podible-content.json"), JSON.stringify({ id: randomUUID() }));
    await expect(f.inventory()).rejects.toThrow("intent identity changed");
  } finally { await f.close(); }
});
