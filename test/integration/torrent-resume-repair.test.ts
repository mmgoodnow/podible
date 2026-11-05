import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import bencode from "bencode";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import type { PersistedValue } from "reflow-ts";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { inspectMetainfo } from "../../src/media/metainfo";
import { TransmissionAdapter, type TransmissionRpc } from "../../src/media/transmission-client";
import { repairTorrentResume } from "../../src/media/torrent-resume-repair";
import { jobJournalReader } from "../../src/worker/job-journal";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "resume-repair-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db); const repo = new BooksRepo(db);
  const settings = defaultSettings(); settings.tv.managedRoot = path.join(root, "tv"); repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "repair", username: "operator", isAdmin: true });
  const request = repo.tv.request(user.id, { key: "tvdb:81797", tvdbId: 81797, tmdbId: 37854,
    title: "One Piece fixture", aliases: [], anime: true, year: 1999, relations: [], episodes: [{ key: "tvdb:1", title: "Fixture",
      airDate: "2000-01-01", runtimeMinutes: 25, numbering: [{ scheme: "tvdb:aired", season: 2, episode: 1, absolute: 9 }] }] },
    defaultMovieQualityPolicy, ["tvdb:1"]);
  repo.retention.request({ namespace: "tv-target", id: String(request.targetId) }, { expectedRevision: 0, reason: "Explicit canary" });
  const acquisition = repo.tv.startAcquisition(request.targetId, "initial", id => repo.createJob({ type: "tv_acquire",
    payload: { acquisitionId: id } }).id, request.requestId).acquisition;
  const metainfo = new Uint8Array(bencode.encode({ announce: "https://tracker.invalid/private-secret", info: { name: "fixture-season",
    files: [{ length: 2, path: ["episode.mkv"] }], "piece length": 16384, pieces: createHash("sha1").update("ab").digest() } }));
  const metadata = inspectMetainfo(metainfo), hash = metadata.infoHash, clientId = `transmission:${randomUUID()}`;
  const location = { rootId: "tv", relativePath: "acquisition-fixture" };
  const torrentId = repo.tv.bindDownload(acquisition.id, { clientId, nativeId: hash }, location, "fixture-release");
  repo.tv.observeDownload(torrentId, { ref: { clientId, nativeId: hash }, location, state: "downloading", complete: false,
    bytesDone: 1, sizeBytes: 2, downloadBytesPerSecond: 1, message: null, observedAt: new Date().toISOString() });
  const owned = path.join(settings.tv.managedRoot, "downloads", location.relativePath);
  const source = path.join(owned, metadata.files[0]!.path); await fs.mkdir(path.dirname(source), { recursive: true }); await fs.writeFile(source, "ab");
  const engineDirectory = path.join(root, "engine"); await fs.mkdir(path.join(engineDirectory, "torrents"), { recursive: true });
  await fs.mkdir(path.join(engineDirectory, "resume")); await fs.writeFile(path.join(engineDirectory, "torrents", `${hash}.torrent`), metainfo);
  const defaultDirectory = path.join(root, "default"); await fs.mkdir(defaultDirectory);
  const journal = new SQLiteStorage(repo.executionDatabasePath()); await journal.initialize();
  const runId = randomUUID(), now = Date.now();
  await journal.createRun({ id: runId, workflow: "media-tv_acquire-v1", input: { jobId: acquisition.job_id! },
    idempotencyKey: null, status: "failed", createdAt: now, updatedAt: now, startedAt: now, finishedAt: now,
    output: null, error: { stepName: "poll", message: "Refusing to control an unmanaged torrent", code: null }, wakeAt: null, waitingFor: null });
  repo.attachJobExecution(acquisition.job_id!, runId); repo.finishJobJournalMigration();
  repo.bindJobJournalReader(jobJournalReader(repo.executionDatabasePath()));
  const save = async (name: string, output: PersistedValue) => journal.saveStepResult({ id: randomUUID(), runId, name,
    status: "completed", output, error: null, attempts: 1, createdAt: now, updatedAt: now, startedAt: now, history: [], kind: "step" });
  await save("tv:attempt:0:snatch", { clientId, nativeId: hash }); await save("tv:attempt:0:download-intent", location);
  await save("tv:attempt:0:metainfo:request:0", { ok: true, value: Buffer.from(metainfo).toString("base64") });
  const row = { hashString: hash, downloadDir: defaultDirectory, labels: [] as string[], status: 0,
    totalSize: 2, haveValid: 0, haveUnchecked: 0, files: metadata.files.map(f => ({ name: f.path, length: f.size })) };
  const controls: string[] = []; let retryCount = 0, recoveredBytes = 1, failLabelOnce = false;
  const rpc = { async call(method: string, args: Record<string, unknown> = {}) {
    if (method === "torrent-get") return { torrents: [structuredClone(row)].map(r => ({ ...r,
      leftUntilDone: r.totalSize - r.haveValid, sizeWhenDone: r.totalSize, rateDownload: 0, error: 0, errorString: "" })) };
    controls.push(method);
    expect(args.ids).toEqual([hash]);
    if (method === "torrent-set-location") { expect(args.move).toBe(false); row.downloadDir = String(args.location); }
    else if (method === "torrent-set") { if (failLabelOnce) { failLabelOnce = false; throw new Error("Injected lost label acknowledgement"); } row.labels = args.labels as string[]; }
    else if (method === "torrent-verify") row.haveValid = recoveredBytes;
    else if (method === "torrent-start") { row.status = 4; row.haveValid = 2; }
    else if (method === "torrent-stop") row.status = 0;
    else throw new Error("Forbidden control " + method);
    return {};
  } } as Pick<TransmissionRpc, "call">;
  const client = new TransmissionAdapter({ clientId, roots: { tv: path.join(settings.tv.managedRoot, "downloads") }, ownershipTag: "Podible TV", rpc: rpc as TransmissionRpc });
  const input = { scope: { hash, requestId: request.requestId, targetId: request.targetId, acquisitionId: acquisition.id,
    jobId: acquisition.job_id!, torrentId, entryKey: "tvdb:81797", relativePath: location.relativePath }, repo, journal, client, rpc,
    engineDirectory, defaultDirectory, reportPath: path.join(root, "repairs", "report.json"),
    build: "a".repeat(40), requiredBuild: "a".repeat(40), pollMs: 1, timeoutMs: 1500,
    async retryOriginalJob() { retryCount++; repo.retryJob(acquisition.job_id!); } };
  return { input, row, source, owned, controls, runId, hash, save, get retryCount() { return retryCount; },
    noValidPieces: () => { recoveredBytes = 0; }, loseLabelAck: () => { failLabelOnce = true; },
    close: async () => { await journal.close(); db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("exact resume repair dry-run reads real registry/journal/files and never writes or leaks metainfo", async () => {
  const f = await fixture(); try {
    const before = await fs.readFile(f.source), report = await repairTorrentResume(f.input);
    expect(report.mode).toBe("dry-run"); expect(report.resumeState).toBe("missing"); expect(f.controls).toEqual([]);
    expect(await fs.readFile(f.source)).toEqual(before); expect(await fs.exists(f.input.reportPath)).toBe(false);
    expect(JSON.stringify(report)).not.toContain("private-secret"); expect(f.retryCount).toBe(0);
  } finally { await f.close(); }
});

test("exact repair validates damaged resume, keeps bytes, rechecks and retries the original execution only", async () => {
  const f = await fixture(); try {
    await fs.writeFile(path.join(f.input.engineDirectory, "resume", `${f.hash}.resume`), bencode.encode({
      destination: f.input.defaultDirectory, labels: [], paused: 1, name: "fixture-season" }));
    const before = await fs.stat(f.source), result = await repairTorrentResume(f.input, true);
    expect(result.phase).toBe("retry-queued"); expect(f.controls).toEqual(["torrent-set-location", "torrent-set", "torrent-verify", "torrent-start"]);
    expect(f.retryCount).toBe(1); expect(f.input.repo.getJobExecution(f.input.scope.jobId)?.run_id).toBe(f.runId);
    expect(f.input.repo.tv.requests()).toHaveLength(1); expect(f.input.repo.tv.acquisitions(f.input.scope.targetId)).toHaveLength(1);
    expect((await fs.stat(f.source)).ino).toBe(before.ino); expect(await fs.readFile(f.source, "utf8")).toBe("ab");
    expect(f.input.repo.listJobCommands().filter(c => c.kind === "retry").map(c => c.job_id)).toEqual([f.input.scope.jobId]);
    await repairTorrentResume(f.input, true); expect(f.retryCount).toBe(1);
  } finally { await f.close(); }
});

test("durable repair intent allows safe replay after directory restoration but before label acknowledgement", async () => {
  const f = await fixture(); try {
    f.loseLabelAck(); await expect(repairTorrentResume(f.input, true)).rejects.toThrow("Injected");
    expect(f.row.downloadDir).toBe(f.owned); expect(f.row.labels).toEqual([]);
    expect((await repairTorrentResume(f.input, true)).phase).toBe("retry-queued"); expect(f.retryCount).toBe(1);
  } finally { await f.close(); }
});

for (const violation of ["foreign-label", "active-record", "wrong-manifest", "wrong-registry", "symlink", "default-data", "wrong-resume", "wrong-build", "missing-receipt"] as const)
  test(`resume repair refuses ${violation} before any daemon control`, async () => {
    const f = await fixture(); try {
      if (violation === "foreign-label") f.row.labels = ["Someone else"];
      if (violation === "active-record") f.row.status = 4;
      if (violation === "wrong-manifest") f.row.files[0]!.length++;
      if (violation === "wrong-registry") f.input.scope.torrentId++;
      if (violation === "symlink") { await fs.rename(f.source, f.source + ".original"); await fs.symlink(f.source + ".original", f.source); }
      if (violation === "default-data") { const other = path.join(f.input.defaultDirectory, f.row.files[0]!.name); await fs.mkdir(path.dirname(other), { recursive: true }); await fs.writeFile(other, "foreign"); }
      if (violation === "wrong-resume") await fs.writeFile(path.join(f.input.engineDirectory, "resume", `${f.hash}.resume`), bencode.encode({ destination: "/foreign", labels: [], paused: 1 }));
      if (violation === "wrong-build") f.input.requiredBuild = "b".repeat(40);
      if (violation === "missing-receipt") await f.save("tv:attempt:0:snatch", { clientId: f.input.client.id, nativeId: "f".repeat(40) });
      const expected = { "foreign-label": "record is not", "active-record": "record is not", "wrong-manifest": "manifest mismatch",
        "wrong-registry": "registry ownership", symlink: "unsafe file", "default-data": "default directory", "wrong-resume": "resume metadata",
        "wrong-build": "approved shutdown-fixed", "missing-receipt": "execution receipt" }[violation];
      await expect(repairTorrentResume(f.input, true)).rejects.toThrow(expected); expect(f.controls).toEqual([]); expect(f.retryCount).toBe(0);
    } finally { await f.close(); }
  });

test("zero recovered hash-valid pieces prevents resume and original-job retry", async () => {
  const f = await fixture(); try {
    f.noValidPieces(); await expect(repairTorrentResume(f.input, true)).rejects.toThrow("no valid pieces");
    expect(f.controls).not.toContain("torrent-start"); expect(f.retryCount).toBe(0);
  } finally { await f.close(); }
});
