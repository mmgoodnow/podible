import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hasTorrentStewardshipBindings, TorrentStewardship, type StewardshipConnection } from "../../src/media/torrent-stewardship";
import { MINIMUM_SEED_SECONDS, STEWARDSHIP_INTERVAL_MS } from "../../src/media/torrent-stewardship-types";
import { createStewardshipRouter } from "../../src/rpc/stewardship-router";
import { executeMethod } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";
import { hashSessionToken } from "../../src/auth";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { processStewardshipCollection } from "../../src/media/torrent-stewardship-worker";

const privateId = "transmission:11111111-1111-4111-8111-111111111111";
const foreignId = "transmission:22222222-2222-4222-8222-222222222222";
const hashes = { books: "a".repeat(40), movies: "b".repeat(40), tv: "c".repeat(40) };
const secret = "sensitive-passkey-should-never-persist";
const nowStart = Date.parse("2026-11-04T12:00:00Z");
type DaemonRow = { hashString: string; downloadDir: string; labels: string[]; status: number; leftUntilDone: number;
  secondsSeeding?: number | null; error: number; trackerStats?: unknown };
function tracker(now: number, result = "Success", succeeded = true) {
  return { id: 1, announce: `https://tracker.invalid/${secret}/announce`, host: `https://tracker.invalid/${secret}`,
    isBackup: false, hasAnnounced: true, lastAnnounceSucceeded: succeeded, lastAnnounceTimedOut: false,
    lastAnnounceTime: Math.floor(now / 1000), lastAnnounceResult: result };
}
async function setup() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "torrent-stewardship-")));
  const db = new Database(path.join(root, "domain.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.libraryRoot = path.join(root, "library"); settings.movies.managedRoot = path.join(root, "movies"); settings.tv.managedRoot = path.join(root, "tv");
  repo.updateSettings(settings);
  const book = repo.createBook({ title: "A book", author: "Author" });
  const bookRoot = path.join(settings.libraryRoot, ".podible", "downloads"), token = randomUUID();
  await fs.mkdir(path.join(bookRoot, hashes.books), { recursive: true });
  await fs.writeFile(path.join(bookRoot, ".podible-content.json"), JSON.stringify({ id: token }));
  await fs.writeFile(path.join(bookRoot, hashes.books, ".podible-book-torrent.json"), JSON.stringify({ rootToken: token, clientId: privateId, infoHash: hashes.books }));
  repo.bookTorrents.reserve({ infoHash: hashes.books, bookId: book.id, clientId: privateId, rootPath: bookRoot, rootToken: token });
  for (const media of ["movies", "tv"] as const) {
    const directory = path.join(settings[media === "movies" ? "movies" : "tv"].managedRoot, "downloads", hashes[media]);
    await fs.mkdir(directory, { recursive: true });
    db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,?)")
      .run(privateId, hashes[media], JSON.stringify({ rootId: media, relativePath: hashes[media] }), "private-source");
  }
  let now = nowStart;
  const rows: DaemonRow[] = (["books", "movies", "tv"] as const).map((media) => ({
    hashString: hashes[media], downloadDir: media === "books" ? path.join(bookRoot, hashes.books) : path.join(settings[media].managedRoot, "downloads", hashes[media]),
    labels: [`Podible ${media === "books" ? "Books" : media === "movies" ? "Movies" : "TV"}`], status: 6, leftUntilDone: 0,
    secondsSeeding: 10 * 86_400, error: 0, trackerStats: [tracker(now)],
  }));
  const calls: { ids: string[]; fields: readonly string[] }[] = [];
  const engine: StewardshipConnection = { clientId: privateId, async get(ids, fields) {
    calls.push({ ids, fields }); return { torrents: structuredClone(rows.filter((row) => ids.includes(row.hashString))) };
  } };
  const service = new TorrentStewardship(repo, { connection: engine, now: () => now });
  const collect = () => service.collect({ operationId: randomUUID() });
  async function collectBooks() { let result = await collect(); if (result.source !== "books") result = await collect(); return result; }
  const bookObservation = () => service.status({ source: "books" }).items[0]!.observation!;
  return { root, db, repo, engine, rows, calls, service, collect, collectBooks, bookObservation, bookRoot, token,
    now: () => now, advance: (ms = 3 * STEWARDSHIP_INTERVAL_MS) => { now += ms; },
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("status is bounded and read-only: never queries the daemon, initializes settings, or writes observations", async () => {
  const s = await setup();
  try {
    const before = s.db.query("SELECT * FROM app_state").all();
    expect(s.service.status({ source: "books" }).items[0]).toMatchObject({ media: "books", observation: null, stale: true, retirementAllowed: false });
    expect(s.service.status({ source: "screen" }).items.map((item) => item.media)).toEqual(["movies", "tv"]);
    expect(s.calls).toEqual([]); expect(s.db.query("SELECT * FROM app_state").all()).toEqual(before);
    const empty = new Database(":memory:"); runMigrations(empty);
    try {
      new TorrentStewardship(new BooksRepo(empty), { connection: null }).status();
      expect((empty.query("SELECT COUNT(*) AS n FROM settings").get() as { n: number }).n).toBe(0);
    } finally { empty.close(); }
  } finally { await s.close(); }
});

test("collection rotates bounded books/movie/TV batches and persists safe actual seed observations across repository recreation", async () => {
  const s = await setup();
  try {
    expect(await s.collect()).toMatchObject({ source: "books", observedCount: 1 });
    expect(await s.collect()).toMatchObject({ source: "screen", observedCount: 2 });
    expect(s.calls.map((call) => call.ids)).toEqual([[hashes.books], [hashes.movies, hashes.tv]]);
    for (const call of s.calls) {
      expect(call.ids.length).toBeLessThanOrEqual(50);
      for (const forbidden of ["metainfo", "torrentFile", "magnetLink", "trackerList", "files", "name"]) expect(call.fields).not.toContain(forbidden);
    }
    const reopened = new Database(path.join(s.root, "domain.sqlite"));
    try {
      const persisted = new TorrentStewardship(new BooksRepo(reopened), { connection: s.engine, now: s.now }).status();
      expect(persisted.items[0]!.observation).toMatchObject({ ownership: "verified-private-binding", trackerState: "healthy", state: "seeding",
        actualSeedSeconds: 10 * 86_400, seedProtection: "protected", remainingSeedSeconds: 20 * 86_400 });
      expect(persisted.destructiveActionsEnabled).toBe(false);
    } finally { reopened.close(); }
    expect(JSON.stringify(s.db.query("SELECT * FROM app_state").all())).not.toContain(secret);
    expect(JSON.stringify(s.service.status())).not.toContain("tracker.invalid");
  } finally { await s.close(); }
});

test("only the current private client bindings are queried, never foreign/rTorrent rows or empty ids", async () => {
  const s = await setup();
  try {
    s.db.query("UPDATE book_torrents SET client_id=?").run(foreignId);
    s.db.query("UPDATE media_torrents SET client_id=? WHERE native_id=?").run("rtorrent:external", hashes.movies);
    expect(await s.collect()).toMatchObject({ source: "books", observedCount: 0, excludedCount: 1 });
    expect(s.calls).toEqual([]);
    expect(await s.collect()).toMatchObject({ source: "screen", observedCount: 1, excludedCount: 1 });
    expect(s.calls[0]!.ids).toEqual([hashes.tv]);
    expect(s.service.status().items).toEqual([]);
  } finally { await s.close(); }
});

test("paused incomplete data and unknown daemon seed counters stay protected; torrent age is not seed time", async () => {
  const s = await setup();
  try {
    Object.assign(s.rows[0]!, { status: 0, leftUntilDone: 123, secondsSeeding: null, addedDate: 1, doneDate: 1 });
    await s.collectBooks();
    expect(s.bookObservation()).toMatchObject({ state: "paused-incomplete", complete: false, bytesRemaining: 123,
      actualSeedSeconds: null, seedProtection: "unknown", remainingSeedSeconds: null });
    expect(s.service.status().items[0]!.retirementAllowed).toBe(false);
    s.rows[0]!.secondsSeeding = MINIMUM_SEED_SECONDS - 1;
    await s.collectBooks(); expect(s.bookObservation()).toMatchObject({ seedProtection: "protected", remainingSeedSeconds: 1 });
    s.rows[0]!.secondsSeeding = MINIMUM_SEED_SECONDS;
    await s.collectBooks(); expect(s.bookObservation()).toMatchObject({ seedProtection: "minimum-met", remainingSeedSeconds: 0 });
    expect(s.service.status().items[0]!.retirementAllowed).toBe(false);
    s.rows[0]!.secondsSeeding = -1;
    await s.collectBooks(); expect(s.bookObservation().seedProtection).toBe("unknown");
  } finally { await s.close(); }
});

test("tracker timeouts, generic transport errors, stale results and missing fields are unknown, not confirmed failures", async () => {
  const s = await setup();
  try {
    for (const stats of [undefined, [], [{ ...tracker(s.now(), "unregistered torrent", false), lastAnnounceTimedOut: true }],
      [{ ...tracker(s.now(), "unregistered torrent", false), hasAnnounced: false }],
      [{ ...tracker(s.now(), "Could not connect to tracker", false) }],
      [{ ...tracker(s.now(), `HTTP 500 body=${secret}`, false) }],
      [{ ...tracker(s.now() - 25 * 3600_000, "unregistered torrent", false) }], [{ lastAnnounceResult: "unregistered torrent" }]]) {
      s.rows[0]!.trackerStats = stats;
      await s.collectBooks(); expect(s.bookObservation().trackerState).toBe("unknown");
      expect(s.bookObservation().repeatedUnregistered).toBe(false);
    }
    s.rows[0]!.trackerStats = [tracker(s.now(), "Invalid passkey", false)];
    await s.collectBooks(); expect(s.bookObservation().trackerState).toBe("confirmed-error");
    expect(JSON.stringify(s.db.query("SELECT * FROM app_state").all())).not.toContain(secret);
  } finally { await s.close(); }
});

test("three distinct consistent spaced announcements establish repeated unregistered evidence, never retirement authorization", async () => {
  const s = await setup();
  try {
    s.rows[0]!.secondsSeeding = MINIMUM_SEED_SECONDS;
    s.rows[0]!.trackerStats = [tracker(s.now(), "Unregistered torrent", false)];
    await s.collectBooks(); expect(s.bookObservation().consistentUnregisteredAnnouncements).toBe(1);
    await s.collectBooks(); await s.collectBooks(); expect(s.bookObservation().consistentUnregisteredAnnouncements).toBe(1);
    s.advance(); s.rows[0]!.trackerStats = [tracker(s.now(), "Torrent not registered", false)];
    await s.collectBooks(); expect(s.bookObservation().consistentUnregisteredAnnouncements).toBe(2);
    s.advance(); s.rows[0]!.trackerStats = [tracker(s.now(), "Torrent not found", false)];
    await s.collectBooks(); expect(s.bookObservation()).toMatchObject({ consistentUnregisteredAnnouncements: 3, repeatedUnregistered: true, seedProtection: "minimum-met" });
    expect(s.service.status().items[0]!.retirementAllowed).toBe(false);
    expect(s.db.query("SELECT * FROM media_torrents").all()).toHaveLength(2);
    expect(await fs.stat(path.join(s.bookRoot, hashes.books))).toBeDefined();
    s.rows[0]!.secondsSeeding = 1;
    s.advance(); s.rows[0]!.trackerStats = [tracker(s.now(), "Unregistered torrent", false)];
    await s.collectBooks(); expect(s.bookObservation()).toMatchObject({ seedCounterRegressed: true, consistentUnregisteredAnnouncements: 1, seedProtection: "protected" });
  } finally { await s.close(); }
});

test("mixed healthy/error trackers do not establish record-wide unregistration; tracker-set changes reset history", async () => {
  const s = await setup();
  try {
    s.rows[0]!.trackerStats = [tracker(s.now(), "Unregistered torrent", false), { ...tracker(s.now()), id: 2 }];
    await s.collectBooks(); expect(s.bookObservation()).toMatchObject({ trackerState: "confirmed-error", repeatedUnregistered: false });
    s.rows[0]!.trackerStats = [tracker(s.now(), "Unregistered torrent", false)]; await s.collectBooks();
    s.advance(); s.rows[0]!.trackerStats = [{ ...tracker(s.now(), "Unregistered torrent", false), announce: `https://changed.invalid/${secret}` }];
    await s.collectBooks(); expect(s.bookObservation().consistentUnregisteredAnnouncements).toBe(1);
  } finally { await s.close(); }
});

test("label, exact directory, missing records, and book root markers fail closed without exposing daemon errors", async () => {
  const s = await setup();
  try {
    const original = structuredClone(s.rows[0]!);
    for (const update of [{ labels: [] }, { downloadDir: path.join(s.root, "outside") }]) {
      Object.assign(s.rows[0]!, original, update); await s.collectBooks();
      expect(s.bookObservation().ownership).toBe("binding-unverified"); expect(s.bookObservation().actualSeedSeconds).toBeNull();
    }
    Object.assign(s.rows[0]!, original);
    await fs.writeFile(path.join(s.bookRoot, ".podible-content.json"), JSON.stringify({ id: randomUUID() }));
    await s.collectBooks(); expect(s.bookObservation().ownership).toBe("binding-unverified");
    s.rows[0]!.hashString = "d".repeat(40); await s.collectBooks();
    expect(s.bookObservation().ownership).toBe("missing-record");
  } finally { await s.close(); }
});

test("replayed collections survive process recreation without another query, and failed storage rolls back observations, cursor and receipt", async () => {
  const s = await setup();
  try {
    const operationId = randomUUID();
    const receipt = await s.service.collect({ operationId });
    const calls = s.calls.length;
    expect(await new TorrentStewardship(new BooksRepo(s.db), { connection: s.engine, now: s.now }).collect({ operationId })).toEqual(receipt);
    expect(s.calls).toHaveLength(calls);
    const before = s.db.query("SELECT * FROM app_state ORDER BY key").all();
    s.db.exec(`CREATE TRIGGER fail_stewardship BEFORE INSERT ON app_state WHEN NEW.key LIKE 'torrent-stewardship:v1:observation:%'
      BEGIN SELECT RAISE(ABORT,'injected stewardship failure'); END`);
    await expect(s.collect()).rejects.toThrow("injected stewardship failure");
    expect(s.db.query("SELECT * FROM app_state ORDER BY key").all()).toEqual(before);
    s.db.exec("DROP TRIGGER fail_stewardship");
    expect(await s.collect()).toMatchObject({ source: "screen", observedCount: 2 });
  } finally { await s.close(); }
});

test("binding changes during daemon I/O cannot attribute observations to a rebound torrent", async () => {
  const s = await setup();
  try {
    const service = new TorrentStewardship(s.repo, { now: s.now, connection: { clientId: privateId, async get(ids, fields) {
      const value = await s.engine.get(ids, fields);
      s.db.query("UPDATE book_torrents SET book_id=book_id+1").run();
      return value;
    } } });
    expect(await service.collect({ operationId: randomUUID() })).toMatchObject({ observedCount: 0 });
    expect(service.status().items[0]!.observation).toBeNull();
  } finally { await s.close(); }
});

test("unavailable identities and time-bounded daemon failures persist safe status without controls or stale health claims", async () => {
  const s = await setup();
  try {
    const absent = new TorrentStewardship(s.repo, { connection: null, now: s.now });
    expect(absent.status()).toMatchObject({ engineConfigured: false, items: [] });
    expect(await absent.collect({ operationId: randomUUID() })).toMatchObject({ observedCount: 0, unavailableReason: expect.any(String) });
    const timedOut = new TorrentStewardship(s.repo, { connection: { clientId: privateId, get: async () => new Promise(() => {}) }, deadlineMs: 1, now: s.now });
    expect(await timedOut.collect({ operationId: randomUUID() })).toMatchObject({ observedCount: 1, unavailableReason: expect.any(String) });
    expect(timedOut.status().items[0]!.observation).toMatchObject({ ownership: "engine-unavailable", state: "unknown", seedProtection: "unknown" });
    s.advance(2 * 3600_000); expect(timedOut.status().items[0]!.stale).toBe(true);
  } finally { await s.close(); }
});

test("ADMIN API enforces auth before all I/O, exposes read-only status and explicit strict collection, and has no retirement/control method", async () => {
  const s = await setup();
  try {
    for (const name of ["user", "admin"]) {
      const user = s.repo.upsertUser({ provider: "local", providerUserId: name, username: name, isAdmin: name === "admin" });
      s.repo.createSession(user.id, hashSessionToken(name), null, "browser");
    }
    const ctx = (token: string): RpcContext => ({ repo: s.repo, startTime: 0, request: new Request("http://localhost/rpc"),
      session: s.repo.getSessionByTokenHash(hashSessionToken(token)) });
    const router = createStewardshipRouter({ connection: s.engine, now: s.now });
    expect(Object.keys(router.routes)).toEqual(["status", "collect"]);
    expect(router.routes.status.readOnly).toBe(true); expect(router.routes.collect.readOnly).toBe(false);
    for (const token of ["user", "invalid"]) {
      await expect(executeMethod(router.routes.status, ctx(token), {})).rejects.toMatchObject({ code: token === "user" ? -32003 : -32001 });
      await expect(executeMethod(router.routes.collect, ctx(token), { operationId: randomUUID() })).rejects.toMatchObject({ code: token === "user" ? -32003 : -32001 });
    }
    expect(s.calls).toEqual([]);
    for (const input of [{ operationId: randomUUID(), deleteLocalData: true }, { operationId: randomUUID(), ids: [hashes.books] }, { operationId: randomUUID(), url: "http://other.invalid" }])
      await expect(executeMethod(router.routes.collect, ctx("admin"), input)).rejects.toMatchObject({ code: -32602 });
    expect(await executeMethod(router.routes.collect, ctx("admin"), { operationId: randomUUID() })).toMatchObject({ observedCount: 1 });
    const result = await executeMethod(router.routes.status, ctx("admin"), { source: "books", limit: 1 });
    expect(JSON.stringify(result)).not.toContain(secret); expect(result.destructiveActionsEnabled).toBe(false);
  } finally { await s.close(); }
});

test("routine acquisition telemetry updates do not invalidate the binding, while storage errors stay separate from tracker evidence", async () => {
  const s = await setup();
  try {
    await s.collect(); await s.collect();
    const before = s.service.status({ source: "screen" }).items[0]!.observation;
    s.db.query("UPDATE media_torrents SET observation_json=? WHERE native_id=?").run(JSON.stringify({ complete: true }), hashes.movies);
    expect(s.service.status({ source: "screen" }).items[0]!.observation).toEqual(before);
    s.rows[0]!.error = 3;
    await s.collectBooks();
    expect(s.bookObservation()).toMatchObject({ daemonError: "storage-error", trackerState: "healthy" });
    expect(s.bookObservation().reason).toContain("local storage error");
  } finally { await s.close(); }
});

test("pagination and collection rotate past fifty rows, and malformed cursors self-recover without a control action", async () => {
  const s = await setup();
  try {
    for (let index = 1; index <= 55; index++) {
      const hash = index.toString(16).padStart(40, "0");
      s.repo.bookTorrents.reserve({ infoHash: hash, bookId: 1, clientId: foreignId, rootPath: s.bookRoot, rootToken: s.token });
    }
    expect(s.service.status({ limit: 25 })).toMatchObject({ scannedCount: 25, nextOffset: 25, sourceRows: 56 });
    expect(await s.collect()).toMatchObject({ source: "books", offset: 0 });
    expect(await s.collect()).toMatchObject({ source: "books", offset: 50, excludedCount: 6 });
    expect(await s.collect()).toMatchObject({ source: "screen", offset: 0, observedCount: 2 });
    s.repo.setJsonState("torrent-stewardship:v1:cursor", { broken: true });
    expect(await s.collect()).toMatchObject({ source: "books", offset: 0 });
    expect(s.repo.getJsonState<{ source: string; offset: number }>("torrent-stewardship:v1:cursor")).toEqual({ source: "books", offset: 50 });
  } finally { await s.close(); }
});

test("real Reflow restart replays a lost collection checkpoint through its durable receipt without resampling or controls", async () => {
  const s = await setup();
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let loseCheckpoint = true;
  try {
    const job = s.repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 });
    const ctx = { repo: s.repo, getSettings: () => s.repo.getSettings(), onLog() {} };
    const executionPath = path.join(s.root, "execution.sqlite");
    const process: NonNullable<Parameters<typeof createJobExecutionRuntime>[2]>["processJob"] = (owned, input) => {
      const execution = owned.execution!;
      return processStewardshipCollection({ ...owned, execution: { ...execution,
        run<T>(name: string, action: () => T | Promise<T>) {
          return execution.run(name, async () => {
            const value = await action();
            if (name === "stewardship:collect" && loseCheckpoint) {
              loseCheckpoint = false; throw new Error("Lost stewardship collection checkpoint");
            }
            return value;
          });
        },
      } }, input, { connection: s.engine, now: s.now });
    };
    const until = async (status: string) => {
      const deadline = Date.now() + 7000;
      while (s.repo.getJob(job.id)?.status !== status) {
        await runtime!.reconcile();
        if (Date.now() > deadline) throw new Error(`Stewardship job did not become ${status}`);
        await Bun.sleep(15);
      }
    };
    runtime = await createJobExecutionRuntime(ctx, executionPath, { retryDelayMs: 1, processJob: process });
    await until("failed");
    expect(s.repo.getJob(job.id)!.error).toContain("Lost stewardship collection checkpoint");
    expect(s.calls).toHaveLength(1);
    const stateBefore = s.db.query("SELECT * FROM app_state WHERE key LIKE 'torrent-stewardship:%' ORDER BY key").all();
    expect(s.bookObservation().ownership).toBe("verified-private-binding");
    await runtime.stop(); runtime = undefined;
    s.repo.retryJob(job.id);
    runtime = await createJobExecutionRuntime(ctx, executionPath, { retryDelayMs: 1, processJob: process });
    await until("succeeded");
    expect(s.calls).toHaveLength(1);
    expect(s.db.query("SELECT * FROM app_state WHERE key LIKE 'torrent-stewardship:%' ORDER BY key").all()).toEqual(stateBefore);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("real Reflow cancellation during daemon I/O fences all observation/receipt writes and leaves owned records untouched", async () => {
  const s = await setup();
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  let release = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let started = false;
  try {
    const job = s.repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 });
    runtime = await createJobExecutionRuntime({ repo: s.repo, getSettings: () => s.repo.getSettings(), onLog() {} },
      path.join(s.root, "execution.sqlite"), { retryDelayMs: 1, processJob: (ctx, input) => processStewardshipCollection(ctx, input, {
        connection: { clientId: privateId, async get(ids, fields) { started = true; await gate; return s.engine.get(ids, fields); } }, now: s.now,
      }) });
    const deadline = Date.now() + 7000;
    while (!started) {
      await runtime.reconcile(); if (Date.now() > deadline) throw new Error("Stewardship observation did not start"); await Bun.sleep(15);
    }
    s.repo.markJobCancelled(job.id, "Fixture cancellation"); await runtime.reconcile(); release();
    while (s.repo.getJob(job.id)?.status !== "cancelled") {
      await runtime.reconcile(); if (Date.now() > deadline) throw new Error("Stewardship did not cancel"); await Bun.sleep(15);
    }
    await Bun.sleep(30);
    expect(s.db.query("SELECT * FROM app_state WHERE key LIKE 'torrent-stewardship:%'").all()).toEqual([]);
    expect(s.db.query("SELECT * FROM book_torrents").all()).toHaveLength(1);
    expect(s.db.query("SELECT * FROM media_torrents").all()).toHaveLength(2);
    expect(s.service.status().items[0]!.observation).toBeNull();
  } finally { release(); await runtime?.stop(); await s.close(); }
}, 15_000);

test("scheduler eligibility retains orphaned book seed bindings instead of depending on current books or request targets", async () => {
  const s = await setup();
  try {
    s.db.query("DELETE FROM books").run(); s.db.query("DELETE FROM media_torrents").run();
    expect(s.repo.hasBooks()).toBe(false); expect(s.repo.movies.hasTargets()).toBe(false); expect(s.repo.tv.hasTargets()).toBe(false);
    expect(hasTorrentStewardshipBindings(s.repo)).toBe(true);
    s.db.query("DELETE FROM book_torrents").run();
    expect(hasTorrentStewardshipBindings(s.repo)).toBe(false);
  } finally { await s.close(); }
});
