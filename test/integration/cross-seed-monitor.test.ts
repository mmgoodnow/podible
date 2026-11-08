import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { crossSeedCommandSchema, crossSeedCommandHash, crossSeedRegistrationKey, crossSeedStateKey } from "../../src/media/cross-seed-state";
import { planContentReuse, reuseDigest } from "../../src/media/reuse-plan";
import { captureReuseFile, captureReuseRoot, materializeReuseLayout, prepareReuseLayoutRoot } from "../../src/media/reuse-layout";
import { prepareMovieStorage } from "../../src/movies/storage";
import { fixtureMovie } from "../fixtures/movie-services";
import { TorrentNotFoundError, type TorrentClient } from "../../src/media/torrent-client";
import { processStewardshipCollection } from "../../src/media/torrent-stewardship-worker";
import { createJobExecutionRuntime } from "../../src/worker/execution";

async function fixture(mode: "optimistic" | "verified" | "clone" = "optimistic") {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "optimistic-monitor-")));
  const app = path.join(root, "app.sqlite"), journal = path.join(root, "execution.sqlite");
  let db = new Database(app); runMigrations(db); let repo = new BooksRepo(db);
  const settings = repo.ensureSettings(); settings.movies.managedRoot = root; repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "monitor", username: "monitor", isAdmin: true });
  const request = repo.movies.request(user.id, fixtureMovie, settings.movies.qualityPolicy);
  await prepareMovieStorage(repo, path.join(root, "objects"));
  const source = path.join(root, "objects", "source.mkv"), bytes = Buffer.from("synthetic media");
  await fs.writeFile(source, bytes, { mode: 0o444 });
  const owned = await captureReuseRoot({ rootPath: path.join(root, "objects"), markerName: ".podible-content.json" });
  const captured = await captureReuseFile({ root: owned, sourceId: "source", sourcePath: source });
  const hash = "a".repeat(40), clientId = `transmission:${randomUUID()}`;
  const plan = planContentReuse({ candidate: { identity: { v1: hash }, files: [{ path: "episode.mkv", size: bytes.length }],
    v1: { pieceLength: bytes.length, pieceHashes: [createHash("sha1").update(bytes).digest("hex")] } }, sources: [captured],
    policy: { version: 1, minReuseRatio: 1, maxExtraDownloadBytes: 0 } });
  await fs.mkdir(path.join(root, "downloads"));
  const layoutRoot = await prepareReuseLayoutRoot({ rootPath: path.join(root, "downloads", "reuse"), ownerId: clientId });
  const layout = await materializeReuseLayout({ plan, root: layoutRoot, capabilities: { cow: "unavailable", symlinkMode: "optimistic-read-only" }, verification: "optimistic" });
  if (layout.status !== "ready") throw new Error("Fixture layout failed");
  if (mode !== "optimistic") {
    const data = await fs.realpath(layout.dataPath), marker = path.join(path.dirname(data), ".podible-reuse-layout.json");
    const saved = JSON.parse(await fs.readFile(marker, "utf8"));
    saved.capabilities = mode === "verified" ? { cow: "unavailable", symlinkMode: "verified-read-only" }
      : { cow: "clone-force", symlinkMode: "never" };
    saved.definitionDigest = reuseDigest({ id: plan.id, root: layoutRoot, capabilities: saved.capabilities });
    if (mode === "clone") {
      const output = path.join(data, "episode.mkv");
      await fs.unlink(output); await fs.writeFile(output, bytes);
      const stat = await fs.lstat(output, { bigint: true });
      saved.files[0] = { path: "episode.mkv", kind: "clone", device: String(stat.dev), inode: String(stat.ino) };
    }
    await fs.writeFile(marker, JSON.stringify(saved));
  }
  const location = { rootId: "movies", relativePath: path.relative(path.join(root, "downloads"), await fs.realpath(layout.dataPath)) };
  const command = crossSeedCommandSchema.parse({ operationId: randomUUID(), scope: { media: "movie", targetId: request.targetId }, releaseId: 1 });
  const job = repo.createJob({ type: "torrent_reuse", status: "succeeded", payload: { operationId: command.operationId } });
  const ref = { clientId, nativeId: hash };
  const receipt = { command, commandHash: crossSeedCommandHash(command), jobId: job.id, createdAt: new Date().toISOString() };
  repo.setJsonState(crossSeedStateKey(command.operationId), receipt);
  const record = repo.reserveReuseTorrent(ref, location, "fixture", command.scope, command.operationId);
  repo.crossSeed.recordOptimistic(command.operationId, { planId: plan.id, ref, location, plan });
  repo.setJsonState(crossSeedStateKey(command.operationId), { ...receipt, outcome: { kind: "seeding", at: new Date().toISOString(),
    reason: "Assumed candidate", torrentId: record.id, infoHash: hash, planId: plan.id, verification: "assumed" } });
  let removed = false, removals = 0, stops = 0, mismatch = true, tracker = false, lostAck = false;
  const client: TorrentClient = { id: clientId, capabilities: { pausedAdd: true, recheck: true, derivedLayouts: true,
    metainfoVersions: [1], readOnlySeeding: true, optimisticSeeding: true },
    ensureAdded: async () => { throw new Error("Monitor must not add"); },
    inspect: async () => {
      if (removed) throw new TorrentNotFoundError();
      return { ref, location, seedOnly: true, state: "inactive", complete: true, bytesDone: bytes.length, sizeBytes: bytes.length,
        downloadBytesPerSecond: 0, observedAt: new Date().toISOString(), message: tracker ? "Tracker unavailable" : mismatch ? "Local error" : null,
        errorKind: tracker ? "tracker" : mismatch ? "storage" : null, integrityError: mismatch && !tracker ? "piece-mismatch" : null };
    },
    files: async () => { throw new Error("Monitor must not enumerate payloads"); },
    quiesce: async () => { stops++; }, recheck: async () => { throw new Error("Monitor must not hash"); },
    resume: async () => { throw new Error("Monitor must not resume"); },
    removeRecord: async () => { removals++; removed = true; if (lostAck) { lostAck = false; throw new Error("Lost remove acknowledgement"); } }
  };
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let inFlightJobId = 0;
  const collect = async () => {
    runtime ??= await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), onLog() {} }, journal, {
      processJob: async (ctx, input) => {
        if (input.id === inFlightJobId) { await ctx.execution!.sleep("unfinished-source-job", 600_000); return "done"; }
        return processStewardshipCollection(ctx, input, { connection: null, retirement: { daemon: null }, optimistic: { client: () => client } });
      }
    });
    const next = repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 }); await runtime.reconcile();
    const end = Date.now() + 5000;
    while (!["succeeded", "failed"].includes(repo.getJobStatus(next.id) ?? "")) {
      if (Date.now() > end) throw new Error("Monitor runtime timed out"); await Bun.sleep(10);
    }
    expect(repo.getJob(next.id)?.error).toBeNull(); expect(repo.getJobStatus(next.id)).toBe("succeeded");
  };
  return { root, source, bytes, command, ref, location, plan, job, record, client, collect,
    get repo() { return repo; }, get db() { return db; }, counts: () => ({ removals, stops }),
    tracker: () => { tracker = true; }, loseAck: () => { lostAck = true; },
    inFlight: () => {
      inFlightJobId = repo.createJob({ type: "torrent_reuse", payload: { operationId: command.operationId } }).id;
      repo.setJsonState(crossSeedStateKey(command.operationId), { ...repo.crossSeed.receipt(command.operationId)!, jobId: inFlightJobId });
    },
    restart: async () => { await runtime?.stop(); runtime = undefined; db.close(); db = new Database(app); repo = new BooksRepo(db); },
    close: async () => { await runtime?.stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("scheduled monitoring blacklists an exact candidate/plan before record-only removal and preserves sources", async () => {
  const f = await fixture();
  let payloadReads = 0;
  const open = fs.open.bind(fs), guarded = spyOn(fs, "open").mockImplementation(async (...args) => {
    const handle = await open(...args);
    return args[0] === f.source ? new Proxy(handle, { get(target, key) {
      if (key === "read" || key === "readFile") return () => { payloadReads++; throw new Error("Payload reads forbidden"); };
      const value = Reflect.get(target, key, target); return typeof value === "function" ? value.bind(target) : value;
    } }) : handle;
  });
  try {
    const remove = f.client.removeRecord;
    f.client.removeRecord = async ref => {
      expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(true);
      expect(f.repo.crossSeed.receipt(f.command.operationId)!.outcome?.kind).toBe("incompatible");
      await remove(ref);
    };
    await f.collect(); await f.collect();
    expect(f.counts()).toEqual({ stops: 1, removals: 1 });
    expect(f.repo.reuseTorrent(f.ref)).toBeNull();
    expect(f.repo.crossSeed.optimisticRegistration(f.command.operationId)!.status).toBe("removed");
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, "b".repeat(64))).toBe(false);
    expect(payloadReads).toBe(0);
    guarded.mockRestore();
    expect(await fs.readFile(f.source)).toEqual(f.bytes);
  } finally { guarded.mockRestore(); await f.close(); }
});

test("tracker errors never blacklist or remove optimistic candidates", async () => {
  const f = await fixture();
  try {
    f.tracker(); await f.collect();
    expect(f.counts()).toEqual({ stops: 0, removals: 0 });
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(false);
  } finally { await f.close(); }
});

test.each(["verified", "clone"] as const)("monitor reconciles replayed %s layouts without source payload reads", async mode => {
  const f = await fixture(mode);
  const open = fs.open.bind(fs), guarded = spyOn(fs, "open").mockImplementation(async (...args) => {
    const handle = await open(...args);
    if (args[0] !== f.source) return handle;
    return new Proxy(handle, { get(target, key) {
      if (key === "read" || key === "readFile") return () => { throw new Error("Source payload reads forbidden"); };
      const value = Reflect.get(target, key, target); return typeof value === "function" ? value.bind(target) : value;
    } });
  });
  try {
    await f.collect();
    expect(f.counts()).toEqual({ stops: 1, removals: 1 });
    expect(f.repo.crossSeed.optimisticRegistration(f.command.operationId)!.status).toBe("removed");
  } finally { guarded.mockRestore(); await f.close(); }
});

test("monitor refuses a replaced legacy clone even when its length is unchanged", async () => {
  const f = await fixture("clone");
  try {
    const output = path.join(f.root, "downloads", f.location.relativePath, "episode.mkv");
    await fs.rename(output, output + ".old"); await fs.writeFile(output, f.bytes);
    await f.collect();
    expect(f.counts()).toEqual({ stops: 0, removals: 0 });
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(false);
  } finally { await f.close(); }
});

test("cross-seed mutations use the execution repository beforeWrite guard", async () => {
  const f = await fixture();
  try {
    const fenced = f.repo.forExecution(() => { throw new Error("Execution no longer owns writes"); });
    expect(() => fenced.crossSeed.markIncompatible(f.ref.nativeId, f.plan.id, "Mismatch")).toThrow("Execution no longer owns writes");
    expect(() => fenced.crossSeed.recordOptimistic(f.command.operationId,
      { planId: f.plan.id, plan: f.plan, ref: f.ref, location: f.location })).toThrow("Execution no longer owns writes");
    expect(() => fenced.crossSeed.completeOptimisticRemoval(f.command.operationId)).toThrow("Execution no longer owns writes");
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(false);
  } finally { await f.close(); }
});

test("restart reconciles a lost remove acknowledgement without another removal", async () => {
  const f = await fixture();
  try {
    f.loseAck(); await f.collect();
    expect(f.repo.crossSeed.optimisticRegistration(f.command.operationId)!.status).toBe("removing");
    await f.restart(); await f.collect();
    expect(f.counts().removals).toBe(1);
    expect(f.repo.crossSeed.optimisticRegistration(f.command.operationId)!.status).toBe("removed");
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(true);
    expect(await fs.readFile(f.source)).toEqual(f.bytes);
  } finally { await f.close(); }
});

test.each(["foreign-owner", "changed-source", "primary-binding", "active-job"])("monitor refuses unsafe control: %s", async failure => {
  const f = await fixture();
  try {
    if (failure === "foreign-owner") f.repo.setJsonState(crossSeedRegistrationKey(f.ref), { operationId: randomUUID() });
    if (failure === "changed-source") await fs.chmod(f.source, 0o644);
    if (failure === "active-job") f.inFlight();
    if (failure === "primary-binding") {
      const acquisition = f.repo.movies.startAcquisition(f.command.scope.targetId, "initial", () => f.repo.createJob({ type: "metadata_hydration" }).id).acquisition;
      f.db.query("INSERT INTO movie_acquisition_downloads(acquisition_id,torrent_id) VALUES(?,?)").run(acquisition.id, f.record.id);
    }
    await f.collect(); expect(f.counts()).toEqual({ stops: 0, removals: 0 });
    expect(f.repo.crossSeed.isIncompatible(f.ref.nativeId, f.plan.id)).toBe(false);
  } finally { await f.close(); }
});
