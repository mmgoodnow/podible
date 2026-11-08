import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomUUID, createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import type { PersistedValue } from "reflow-ts";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { inspectMetainfo } from "../../src/media/metainfo";
import type { TorrentClient, TorrentSnapshot } from "../../src/media/torrent-client";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { completeTvSource, recordedLegacyTvSource, reusableTvSources, tvSourceFiles, tvSourceSchema } from "../../src/tv/source-reuse";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-source-proof-")));
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "source-proof", username: "reader", isAdmin: false });
  const request = repo.tv.request(user.id, {
    key: "tvdb:1", title: "Fixture season", tvdbId: 1, tmdbId: null, year: 2020, aliases: [], anime: false, relations: [],
    episodes: [1, 2].map((episode) => ({ key: `tvdb:${episode}`, title: `Episode ${episode}`, airDate: "2020-01-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode, absolute: episode }] })),
  }, defaultMovieQualityPolicy, ["tvdb:1", "tvdb:2"]);
  const acquisition = repo.tv.startAcquisition(request.targetId, "initial", (id) => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId: id }, maxAttempts: 1,
  }).id, request.requestId).acquisition;
  const metainfo = new Uint8Array(bencode.encode({ announce: "https://tracker.invalid/private-passkey",
    info: { name: "season", files: [1, 2].map((episode) => ({ length: 1, path: [`episode-${episode}.mkv`] })),
      "piece length": 16384, pieces: createHash("sha1").update("ab").digest() } }));
  const source = tvSourceSchema.parse({ ref: { clientId: "fixture-client", nativeId: inspectMetainfo(metainfo).infoHash },
    location: { rootId: "tv", relativePath: "owned-pack" }, offerKey: "verified-season", title: "Fixture season pack", provider: "Fixture",
    manifest: inspectMetainfo(metainfo) });
  const bind = (hash: string, offerKey: string) => repo.tv.bindDownload(acquisition.id,
    { clientId: "fixture-client", nativeId: hash }, { rootId: "tv", relativePath: "owned-pack" }, offerKey);
  bind(source.ref.nativeId, source.offerKey);
  const record = (withSource: boolean, indexes = [0]) => repo.tv.recordVersion(acquisition.id, {
    identityVerified: true, releaseTitle: source.title, provider: source.provider,
    plan: { files: indexes.map((index) => ({ fileIndex: index, episodeIds: [request.episodeIds[index]] })) },
    ...(withSource ? { source } : {}),
  }, indexes.map((index) => ({ id: `verified-${index}`, relativePath: `files/${index}.mkv`, sizeBytes: 1, probe: {}, episodeIds: [request.episodeIds[index]!] })));
  return { root, db, repo, request, acquisition, source, metainfo, bind, record,
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("complete TV sources tolerate explicit tracker failures, never storage faults or incomplete ownership evidence", async () => {
  const f = await fixture();
  try {
    const snapshot: TorrentSnapshot = { ref: f.source.ref, location: f.source.location,
      state: "seeding", complete: true, bytesDone: f.source.manifest.size, sizeBytes: f.source.manifest.size,
      downloadBytesPerSecond: 0, message: null, observedAt: new Date().toISOString() };
    expect(completeTvSource(f.source, snapshot)).toBe(true);
    const tracker: TorrentSnapshot = { ...snapshot, errorKind: "tracker", message: "Tracker unavailable" };
    expect(completeTvSource(f.source, tracker)).toBe(true);
    for (const errorKind of ["storage", "unknown"] as const) {
      expect(completeTvSource(f.source, { ...snapshot, errorKind, message: "Engine error" })).toBe(false);
      expect(completeTvSource(f.source, { ...snapshot, errorKind })).toBe(false);
    }
    expect(completeTvSource(f.source, { ...snapshot, message: "Legacy unclassified error" })).toBe(false);
    for (const partial of [
      { complete: false }, { state: "checking" as const }, { bytesDone: 1 }, { sizeBytes: 1 },
      { bytesDone: null }, { sizeBytes: null },
      { ref: { ...snapshot.ref, clientId: "foreign" } }, { ref: { ...snapshot.ref, nativeId: "a".repeat(40) } },
      { location: { ...snapshot.location, rootId: "foreign" } },
      { location: { ...snapshot.location, relativePath: "foreign" } },
    ]) expect(completeTvSource(f.source, { ...tracker, ...partial })).toBe(false);
  } finally { await f.close(); }
});

test("reusable TV source provenance never chooses failed sibling torrent bindings", async () => {
  const f = await fixture();
  try {
    f.bind("a".repeat(40), "failed-import");
    f.bind("b".repeat(40), "stalled-download");
    f.record(true);
    f.repo.tv.finishAcquisition(f.acquisition.id);
    const result = await reusableTvSources(f.repo, f.request.targetId, 999, [f.request.episodeIds[1]!]);
    const sources = result.sources.map((item) => item.source);
    expect(sources.map((source) => source.ref.nativeId)).toEqual([f.source.ref.nativeId]);
    expect(sources[0]!.offerKey).toBe("verified-season");
    expect(result.sources[0]!.skipFileIndexes).toEqual([0]);
    expect(await reusableTvSources(f.repo, f.request.targetId, f.acquisition.id, f.request.episodeIds)).toEqual({ sources: [], consumed: 0 });
    expect(await reusableTvSources(f.repo, 999, 999, f.request.episodeIds)).toEqual({ sources: [], consumed: 0 });
    expect(JSON.stringify(sources)).not.toContain("private-passkey");
  } finally { await f.close(); }
});

test("legacy pack provenance joins only the matching successful version attempt's exact receipts", async () => {
  const f = await fixture();
  const storage = new SQLiteStorage(f.repo.executionDatabasePath());
  try {
    f.bind("a".repeat(40), "failed-import");
    const versionId = f.record(false);
    f.repo.tv.finishAcquisition(f.acquisition.id);
    await storage.initialize();
    const runId = randomUUID(), now = Date.now();
    await storage.createRun({ id: runId, workflow: "media-tv_acquire-v1", input: { jobId: f.acquisition.job_id! }, idempotencyKey: null,
      status: "completed", createdAt: now, updatedAt: now, startedAt: now, finishedAt: now,
      output: null, error: null, wakeAt: null, waitingFor: null });
    f.repo.attachJobExecution(f.acquisition.job_id!, runId);
    const save = async (name: string, output: PersistedValue) => storage.saveStepResult({ id: randomUUID(), runId, name, status: "completed", output,
      error: null, attempts: 1, createdAt: now, updatedAt: now, startedAt: now, history: [], kind: "step" });
    await save("tv:attempt:0:snatch", { clientId: "fixture-client", nativeId: "a".repeat(40) });
    await save("tv:attempt:0:version", versionId + 100);
    await save("tv:attempt:1:snatch", f.source.ref);
    await save("tv:attempt:1:download-intent", f.source.location);
    await save("tv:attempt:1:metainfo:request:0", { ok: false, delay: 1000 });
    await save("tv:attempt:1:metainfo:request:1", { ok: true, value: Buffer.from(f.metainfo).toString("base64") });
    await save("tv:attempt:1:version", versionId);
    const recovered = await reusableTvSources(f.repo, f.request.targetId, 999, [f.request.episodeIds[1]!]);
    expect(recovered).toEqual({ sources: [{ source: f.source, skipFileIndexes: [0] }], consumed: 0 });
    expect(JSON.stringify(recovered)).not.toContain("private-passkey");
    expect(await recordedLegacyTvSource(f.repo, f.request.targetId, f.acquisition.id)).toBeNull();
    const sha256 = createHash("sha256").update("a").digest("hex");
    await save("tv:attempt:1:adopt:0", { id: "verified-0", relativePath: "files/0.mkv", sizeBytes: 1, sha256 });
    expect((await recordedLegacyTvSource(f.repo, f.request.targetId, f.acquisition.id))?.adoptions)
      .toEqual([{ fileId: "verified-0", fileIndex: 0, sha256 }]);
    await save("tv:attempt:2:version", versionId);
    expect(await reusableTvSources(f.repo, f.request.targetId, 999, f.request.episodeIds)).toEqual({ sources: [], consumed: 0 });
  } finally { await storage.close(); await f.close(); }
});

test("fully accounted sources are skipped, but missing previously mapped episodes can still be repaired", async () => {
  const f = await fixture();
  try {
    f.record(true, [0, 1]);
    f.repo.tv.finishAcquisition(f.acquisition.id);
    expect(await reusableTvSources(f.repo, f.request.targetId, 999, [])).toEqual({ sources: [], consumed: 1 });
    const repair = await reusableTvSources(f.repo, f.request.targetId, 999, [f.request.episodeIds[1]!]);
    expect(repair.sources).toHaveLength(1);
    expect(repair.sources[0]!.skipFileIndexes).toEqual([0]);
  } finally { await f.close(); }
});

test("completed pack evidence rejects same-size source replacement and changed file layouts", async () => {
  const f = await fixture();
  try {
    const directory = path.join(f.root, "downloads", "owned-pack", "season");
    await fs.mkdir(directory, { recursive: true });
    for (const episode of [1, 2]) await fs.writeFile(path.join(directory, `episode-${episode}.mkv`), episode === 1 ? "a" : "b");
    let wrongLayout = false;
    const unused = async () => { throw new Error("Source evidence must not control or add torrents"); };
    const client: TorrentClient = { id: "fixture-client",
      capabilities: { pausedAdd: true, recheck: true, derivedLayouts: false, metainfoVersions: [1] },
      ensureAdded: unused, inspect: unused, quiesce: unused, recheck: unused, resume: unused, removeRecord: unused,
      files: async () => [1, 2].map((episode) => ({ index: episode - 1,
        path: path.join(directory, wrongLayout && episode === 2 ? "renamed.mkv" : `episode-${episode}.mkv`) })),
    };
    const captured = await tvSourceFiles(f.source, client, f.root);
    expect(captured.source.manifest.files.every((file) => file.identity)).toBe(true);
    await fs.writeFile(path.join(directory, "episode-2.mkv"), "c");
    await expect(tvSourceFiles(captured.source, client, f.root)).rejects.toThrow("changed");
    wrongLayout = true;
    await expect(tvSourceFiles(f.source, client, f.root)).rejects.toThrow("layout");
  } finally { await f.close(); }
});
