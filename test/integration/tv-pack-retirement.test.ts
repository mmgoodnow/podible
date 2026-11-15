import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareCanonicalDownload, sealCanonicalDownload } from "../../src/media/canonical-download";
import { adoptContentFile } from "../../src/media/content-store";
import { inspectMetainfo } from "../../src/media/metainfo";
import { admitTorrentUse, assertTorrentDatabaseWritable } from "../../src/media/torrent-retirement-admission";
import { retirementDigest } from "../../src/media/torrent-retirement-evidence";
import { privateRetirementDaemon, type RetirementDaemonSnapshot } from "../../src/media/torrent-retirement-transmission";
import { TorrentRetirement, type TorrentRetirementAdapters } from "../../src/media/torrent-retirement";
import { MINIMUM_SEED_SECONDS } from "../../src/media/torrent-stewardship-types";
import type { TorrentClient } from "../../src/media/torrent-client";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { prepareMovieStorage } from "../../src/movies/storage";
import { publishTvVersion, tvRoots } from "../../src/tv/publication";
import { tvSourceFiles } from "../../src/tv/source-reuse";
import { processJob } from "../../src/worker/dispatcher";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import type { WorkerContext } from "../../src/worker/context";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-pack-retirement-")));
  const dbPath = path.join(root, "domain.sqlite"), db = new Database(dbPath);
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.tv.managedRoot = root; settings.tv.plexViewPath = "/plex/tv";
  repo.updateSettings(settings);
  const roots = tvRoots(root, settings.tv.plexViewPath);
  await prepareMovieStorage(repo, roots.contentRoot);
  const user = repo.upsertUser({ provider: "local", providerUserId: "pack-retirement", username: "viewer", isAdmin: false });
  const entry = { key: "tvdb:100", tvdbId: 100, tmdbId: 200, title: "Fixture", aliases: [], year: 2020, anime: false, relations: [],
    episodes: [301, 302].map((id, index) => ({ key: `tvdb:${id}`, title: `Episode ${index + 1}`, airDate: "2020-01-01", runtimeMinutes: 42,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: index + 1, absolute: index + 1 }] })) };
  const { targetId, requestId, episodeIds } = repo.tv.request(user.id, entry, defaultMovieQualityPolicy, entry.episodes.map(ep => ep.key));
  const identity = { id: randomUUID(), username: "fixture", password: "fixture" }, clientId = `transmission:${identity.id}`;
  const records = new Map<string, NonNullable<RetirementDaemonSnapshot["record"]>>();
  const revision = () => repo.tv.target(targetId)!.revision;
  const trackerStats = [{ id: 1, announce: "https://tracker.invalid/announce", hasAnnounced: true,
    lastAnnounceSucceeded: false, lastAnnounceTimedOut: false, lastAnnounceTime: Math.floor(Date.now() / 1000), lastAnnounceResult: "Unregistered torrent" }];
  const publishLibrary = { scan: async () => {}, verify: async () => true };
  async function version(coverage: number[][], replacement = false) {
    const name = randomUUID(), location = { rootId: "tv" as const, relativePath: name };
    const directory = path.join(root, "downloads", name);
    await fs.mkdir(directory, { recursive: true });
    const payloads = [...coverage.map((ids, index) => ({ name: `${index + 1}.mkv`, data: Buffer.from(`video ${name} ${ids}`) })),
      { name: "readme.nfo", data: Buffer.from(`sidecar ${name}`) }];
    const metainfo = bencode.encode({ info: { name, files: payloads.map(file => ({ length: file.data.length, path: [file.name] })),
      "piece length": 16384, pieces: createHash("sha1").update(Buffer.concat(payloads.map(file => file.data))).digest() } });
    const manifest = inspectMetainfo(metainfo), ref = { clientId, nativeId: manifest.infoHash }, offerKey = `offer:${name}`;
    const { acquisition } = repo.tv.startAcquisition(targetId, replacement ? "upgrade" : "initial",
      id => repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id } }).id, requestId);
    const expectedRevision = revision();
    if (replacement) repo.tv.decision(acquisition.id, { seasonPromotion: { season: 1, revision: expectedRevision, fingerprint: "a".repeat(64), episodeIds } });
    const torrentId = repo.tv.bindDownload(acquisition.id, ref, location, offerKey);
    await prepareCanonicalDownload({ contentRoot: roots.contentRoot, sourceRoot: directory, metainfo, clientId, allowCreate: true });
    for (const file of payloads) await fs.writeFile(path.join(directory, name, file.name), file.data);
    const client = { id: clientId, sealDownload: async () => {}, files: async () => manifest.files.map((file, index) => ({ index, path: path.join(directory, file.path) })) } as unknown as TorrentClient;
    await sealCanonicalDownload(roots.contentRoot, directory, client, ref);
    const prepared = await tvSourceFiles({ ref, location, offerKey, title: name, provider: "fixture", manifest }, client, root);
    const files = await Promise.all(coverage.map(async (episodeIds, index) => ({ episodeIds, probe: {}, ...(await adoptContentFile({
      contentRoot: roots.contentRoot, sourceRoot: directory, sourcePath: prepared.files[index]!.path, fileId: randomUUID(), expectedSize: manifest.files[index]!.size,
    })) })));
    const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true, source: prepared.source,
      plan: { files: coverage.map((episodeIds, fileIndex) => ({ fileIndex, episodeIds })) } }, files);
    await publishTvVersion({ repo: repo.tv, targetId, versionId, roots, library: publishLibrary,
      ...(replacement && { guard: { expectedRevision, validate() {}, verifyRoster: async () => true } }) });
    repo.tv.finishAcquisition(acquisition.id);
    db.query("UPDATE jobs SET status='succeeded' WHERE id=?").run(acquisition.job_id);
    const record = { id: torrentId + 100, hashString: ref.nativeId, downloadDir: directory, labels: ["Podible TV", "Podible Seed Only"],
      status: 6, leftUntilDone: 0, secondsSeeding: MINIMUM_SEED_SECONDS, totalSize: manifest.size, sizeWhenDone: manifest.size,
      haveValid: manifest.size, haveUnchecked: 0, error: 2, trackerStats,
      files: manifest.files.map(file => ({ name: file.path, length: file.size, bytesCompleted: file.size })),
      fileStats: manifest.files.map(() => ({ wanted: false })) };
    records.set(ref.nativeId, record);
    return { versionId, acquisitionId: acquisition.id, torrentId, ref, location, offerKey, files, record };
  }
  const old = [await version([[episodeIds[0]!]]), await version([[episodeIds[1]!]])];
  const pack = await version(episodeIds.map(id => [id]), true);
  expect(repo.tv.packRetirementNomination(targetId, pack.versionId)?.superseded).toHaveLength(2);
  const pinnedPaths = [
    ...[...old, pack].flatMap(item => item.files.map(file => path.join(roots.contentRoot, file.relativePath))),
    ...[...old, pack].flatMap(item => item.record.files.map(file => path.join(item.record.downloadDir, file.name))),
    ...(await fs.readdir(roots.viewRoot, { recursive: true })).filter(file => file.endsWith(".mkv")).map(file => path.join(roots.viewRoot, file)),
  ];
  const payloads = () => Promise.all(pinnedPaths.map(async file => ({ inode: (await fs.stat(file)).ino, bytes: await fs.readFile(file, "utf8") })));
  const pinned = await payloads();
  let plexVerified = true, plexCalls = 0, acknowledged = true, removeTakesEffect = true;
  let beforeRead: ((hash: string) => void) | undefined;
  let session = "fixture-daemon-session-at-start";
  const removes: unknown[] = [];
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => {
    if (db.inTransaction) throw new Error("Daemon I/O inside application transaction");
    const { method, arguments: args } = JSON.parse(String(init?.body));
    if (method === "session-get") return Response.json({ result: "success", arguments: { "session-id": session } });
    if (method === "torrent-get") {
      expect(args.ids).toHaveLength(1);
      beforeRead?.(args.ids[0]);
      const row = records.get(args.ids[0]);
      return Response.json({ result: "success", arguments: { torrents: row ? [row] : [] } });
    }
    if (method !== "torrent-remove") throw new Error(`Unexpected RPC ${method}`);
    removes.push(args);
    expect(new Headers(init?.headers).get("X-Transmission-Session-Id")).toBe(session);
    expect(args).toEqual({ ids: [args.ids[0]], "delete-local-data": false });
    const row = [...records.values()].find(row => row.id === args.ids[0]);
    expect(old.some(item => item.ref.nativeId === row?.hashString)).toBe(true);
    if (removeTakesEffect && row) records.delete(row.hashString);
    return acknowledged ? Response.json({ result: "success" }) : new Response(null, { status: 502 });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const adapters = { daemon: privateRetirementDaemon({ readIdentity: () => identity, fetcher }), readIdentityDigest: () => retirementDigest(identity),
    packLibrary: { verify: async (_entry: unknown, files: { serverPath: string }[]) => {
      expect(db.inTransaction).toBe(false);
      expect(files).toHaveLength(2);
      expect(files.every(file => file.serverPath.startsWith("/plex/tv/") && file.serverPath.endsWith(`v${pack.versionId}.mkv`))).toBe(true);
      plexCalls++;
      return plexVerified;
    } } };
  const job = repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 });
  const context = (owner = repo): WorkerContext => ({ repo: owner, getSettings: () => owner.getSettings(), onLog() {}, activeLocks: new Set(["*"]), execution: {
    key: randomUUID(), assertActive() {}, async run(_name, action) { return action(); }, async sleep() {},
  } });
  const locator = (index = 0) => ({ source: "screen" as const, clientId, infoHash: old[index]!.ref.nativeId,
    packNomination: { targetId, packVersionId: pack.versionId }, operationId: randomUUID() });
  const execute = (options: TorrentRetirementAdapters = {}, input = locator()) =>
    new TorrentRetirement(repo, { ...adapters, ...options }).execute(context(), job, input);
  return { root, dbPath, db, repo, roots, old, pack, job, context, locator, execute, adapters, records, targetId, requestId, episodeIds, removes,
    plex: (verified: boolean) => { plexVerified = verified; }, plexCalls: () => plexCalls,
    restart: () => { session = `fixture-new-session-${randomUUID()}`; },
    loseAck: (effect: boolean) => { acknowledged = false; removeTakesEffect = effect; },
    beforeRead: (hook: (hash: string) => void) => { beforeRead = hook; },
    unchanged: async () => expect(await payloads()).toEqual(pinned),
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("at 30 actual seed days, retire only the exact displaced daemon record and retain files, selected views and history", async () => {
  const s = await fixture();
  try {
    const bindings = s.repo.tv.downloads(s.targetId), selected = s.repo.tv.activeEpisodes(s.targetId);
    expect(await s.execute()).toMatchObject({ status: "retired", intent: { recordOnly: true, filesDeleted: false } });
    expect(s.removes).toEqual([{ ids: [s.old[0]!.record.id], "delete-local-data": false }]);
    expect(s.records.has(s.pack.ref.nativeId)).toBe(true);
    expect(s.repo.tv.downloads(s.targetId)).toEqual(bindings);
    expect(s.repo.tv.activeEpisodes(s.targetId)).toEqual(selected);
    expect(() => assertTorrentDatabaseWritable(s.db, { clientId: s.old[0]!.ref.clientId, infoHash: s.old[0]!.ref.nativeId })).toThrow("retiring or retired");
    await s.unchanged();
  } finally { await s.close(); }
});

test.each(["underage", "seed unknown", "candidate membership", "pack partial", "Plex missing", "selection changed", "source relinked",
  "admission occupied", "other acquisition", "album asset", "pending publication", "pack writer"])("fail closed: %s", async fault => {
  const s = await fixture();
  try {
    const one = s.old[0]!;
    if (fault === "underage") one.record.secondsSeeding = MINIMUM_SEED_SECONDS - 1;
    if (fault === "seed unknown") Object.assign(one.record, { secondsSeeding: null });
    if (fault === "candidate membership") one.record.files[0]!.name = "not-the-recorded-episode.mkv";
    if (fault === "pack partial") s.pack.record.haveValid--;
    if (fault === "Plex missing") s.plex(false);
    if (fault === "selection changed") s.db.query("UPDATE tv_active_episodes SET version_id=? WHERE episode_id=?").run(one.versionId, s.episodeIds[0]!);
    if (fault === "source relinked") {
      const link = path.join(one.record.downloadDir, one.record.files[0]!.name);
      await fs.unlink(link); await fs.symlink(path.join(s.roots.contentRoot, s.pack.files[0]!.relativePath), link);
    }
    if (fault === "admission occupied") admitTorrentUse(s.repo, { clientId: one.ref.clientId, infoHash: one.ref.nativeId }, randomUUID());
    if (fault === "other acquisition") s.repo.tv.bindDownload(s.pack.acquisitionId, one.ref, one.location, one.offerKey);
    if (fault === "album asset") s.db.query("INSERT INTO book_torrent_bindings(torrent_id,book_id,created_at) VALUES(?,?,?)")
      .run(one.torrentId, 123, new Date().toISOString());
    if (fault === "pending publication") s.repo.tv.stagePublication(s.targetId, one.versionId);
    if (fault === "pack writer") s.pack.record.fileStats[0]!.wanted = true;
    if (fault === "seed unknown") await expect(s.execute()).rejects.toThrow();
    else expect(await s.execute()).toMatchObject({ status: "blocked" });
    expect(s.removes).toEqual([]);
    expect(new TorrentRetirement(s.repo).pending()).toEqual([]);
  } finally { await s.close(); }
}, 20_000);

test("post-intent Plex loss and final Transmission seed regression are both blocked", async () => {
  const s = await fixture();
  try {
    expect(await s.execute({ afterCheckpoint: async stage => { if (stage === "dispatch-committed") s.plex(false); } })).toMatchObject({ status: "aborted" });
    expect(s.removes).toEqual([]);
    s.plex(true);
    const hash = s.old[0]!.ref.nativeId;
    let armed = false;
    s.beforeRead(id => { if (armed && id === hash) s.old[0]!.record.secondsSeeding--; });
    expect(await s.execute({ afterCheckpoint: async stage => { if (stage === "dispatch-committed") armed = true; } })).toMatchObject({ status: "pending" });
    expect(s.removes).toEqual([]);
    await s.unchanged();
  } finally { await s.close(); }
});

test.each([true, false])("lost acknowledgement across process and daemon restart (remove took effect: %s)", async effect => {
  const s = await fixture();
  try {
    const input = s.locator(), ref = { clientId: input.clientId, infoHash: input.infoHash };
    s.loseAck(effect);
    expect(await s.execute({}, input)).toMatchObject({ status: "pending", intent: { phase: "uncertain" } });
    expect(() => assertTorrentDatabaseWritable(s.db, ref)).toThrow();
    s.restart(); s.plex(false); // Absence reconciliation must not depend on the now-unavailable pack proof.
    const db = new Database(s.dbPath), repo = new BooksRepo(db);
    try {
      const service = new TorrentRetirement(repo, s.adapters);
      if (effect) expect(await service.execute(s.context(repo), s.job, input)).toMatchObject({ status: "retired" });
      else {
        await expect(service.execute(s.context(repo), s.job, input)).rejects.toThrow("session/record changed");
        expect(service.intent(input.operationId)?.phase).toBe("uncertain");
      }
      expect(() => assertTorrentDatabaseWritable(db, ref)).toThrow();
      expect(s.removes).toHaveLength(1);
      await s.unchanged();
    } finally { db.close(); }
  } finally { await s.close(); }
});

test("dispatcher discovers an eligible later candidate on a books observation page after runtime restart", async () => {
  const s = await fixture();
  try {
    s.old[0]!.record.secondsSeeding = 0;
    const context = { repo: s.repo, getSettings: () => s.repo.getSettings(), onLog() {},
      stewardshipAdapters: { connection: null, retirement: s.adapters, optimistic: { client: null } } };
    for (const index of [0, 1]) {
      const job = index === 0 ? s.job : s.repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 });
      const runtime = await createJobExecutionRuntime(context, path.join(s.root, "execution.sqlite"), { processJob });
      try {
        const until = Date.now() + 6000;
        while (s.repo.getJob(job.id)?.status !== "succeeded") {
          await runtime.reconcile();
          if (Date.now() > until) throw new Error(`Maintenance job did not finish: ${s.repo.getJob(job.id)?.error}`);
          await Bun.sleep(20);
        }
      } finally { await runtime.stop(); }
      expect(s.removes).toHaveLength(index);
    }
    expect(s.removes).toEqual([{ ids: [s.old[1]!.record.id], "delete-local-data": false }]);
    await s.unchanged();
  } finally { await s.close(); }
}, 15_000);
