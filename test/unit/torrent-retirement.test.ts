import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { retirementFixture, retirementFixtureClient, retirementFixtureHash, retirementFixtureSecret } from "../fixtures/torrent-retirement";
import { TorrentRetirement } from "../../src/media/torrent-retirement";
import { admitTorrentUse, assertTorrentDatabaseWritable, assertNoPendingTorrentRetirements, createOrphanRetirementFence, finishTorrentUse } from "../../src/media/torrent-retirement-admission";
import { allTrackerRetirementEvidence, qualifiedRetirementEvidence, readRetirementEvidence, recordRetirementEvidence, retirementDigest } from "../../src/media/torrent-retirement-evidence";
import { privateRetirementDaemon } from "../../src/media/torrent-retirement-transmission";
import { withTorrentUse } from "../../src/media/torrent-use";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createTorrentRetirementRouter } from "../../src/rpc/torrent-retirement-router";
import { executeMethod } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";
import { readConsumerCount } from "../../src/media/torrent-retirement-query";
import { stewardshipBinding } from "../../src/media/torrent-stewardship";
import { hashSessionToken } from "../../src/auth";

test("default orphan fence retires only the daemon record, with durable intent before I/O and a non-expiring re-add tombstone", async () => {
  const s = await retirementFixture();
  try {
    expect(s.service.preview()).toMatchObject({ automaticConfigured: true, items: [{ evidenceQualified: true, eligible: true }] });
    const operationId = randomUUID();
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "retired", intent: { recordOnly: true, filesDeleted: false } });
    expect(s.removeCount()).toBe(1); expect(await fs.readFile(s.retainedFile, "utf8")).toBe("seed data must survive");
    expect(s.repo.bookTorrents.get(retirementFixtureHash)).not.toBeNull();
    expect(readRetirementEvidence(s.repo, s.binding.key)?.samples).toHaveLength(3);
    expect(() => admitTorrentUse(s.repo, s.ref, randomUUID())).toThrow("retired");
    expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).toThrow("retired");
    expect(() => assertNoPendingTorrentRetirements(s.repo)).not.toThrow();
    const reads = s.readCount();
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "retired" });
    expect(s.readCount()).toBe(reads); expect(s.removeCount()).toBe(1);
    const persisted = JSON.stringify(s.db.query("SELECT * FROM app_state").all());
    expect(persisted).not.toContain(retirementFixtureSecret); expect(persisted).not.toContain("tracker0.invalid");
  } finally { await s.close(); }
});

test("all current trackers including backups must supply three individually advanced spaced announcements", async () => {
  const s = await retirementFixture();
  try {
    const raw = structuredClone(s.record()!.trackerStats) as Record<string, any>[];
    raw[1]!.hasAnnounced = false;
    expect(allTrackerRetirementEvidence(raw, s.now())).toBeNull();
    raw[1]!.hasAnnounced = true; raw[1]!.lastAnnounceSucceeded = true;
    expect(allTrackerRetirementEvidence(raw, s.now())).toBeNull();
    const base = structuredClone(s.record()!.trackerStats) as Record<string, any>[];
    // A cached far-ahead backup must not let changes in the minimum timestamp forge three tracker events.
    const start = s.now(), bindingKey = retirementDigest("independent vector fixture");
    base[0]!.lastAnnounceTime = Math.floor(start / 1000) - 3600;
    let evidence;
    for (let index = 0; index < 3; index++) {
      if (index) base[0]!.lastAnnounceTime += 900;
      evidence = recordRetirementEvidence(s.repo, { bindingKey, bindingDigest: s.binding.bindingDigest, observedAt: new Date(start).toISOString(),
        ownershipVerified: true, complete: true, actualSeedSeconds: 3_000_000, trackerStats: base });
    }
    expect(evidence!.samples).toHaveLength(1);
    expect(qualifiedRetirementEvidence(evidence!, s.binding.bindingDigest, s.now())).toBe(false);
  } finally { await s.close(); }
});

test("orphan checks reject attached books, admitted async producers, source-file references and missing global execution", async () => {
  const s = await retirementFixture();
  try {
    s.ctx.activeLocks!.clear();
    await expect(s.service.execute(s.ctx, s.job, { ...s.locator, operationId: randomUUID() })).rejects.toThrow("global maintenance");
    s.ctx.activeLocks!.add("*");
    const ref = { clientId: retirementFixtureClient, infoHash: retirementFixtureHash };
    const token = admitTorrentUse(s.repo, ref, randomUUID());
    expect(s.service.preview().items[0]!.eligible).toBe(false);
    await expect(s.service.execute(s.ctx, s.job, { ...s.locator, operationId: randomUUID() })).rejects.toThrow("consumer inventory");
    finishTorrentUse(s.repo, ref, token);
    expect(s.service.preview().items[0]!.eligible).toBe(true);
    const book = s.repo.createBook({ title: "Current owner", author: "Fixture" });
    s.db.query("UPDATE book_torrents SET book_id=?").run(book.id);
    expect(s.service.preview().items[0]!.eligible).toBe(false);
    expect(s.removeCount()).toBe(0);
  } finally { await s.close(); }
});

test("durable claim excludes the real producer wrapper and settings writer; safe pre-dispatch abort releases only the claim", async () => {
  const s = await retirementFixture();
  try {
    const operationId = randomUUID();
    const service = new TorrentRetirement(s.repo, { ...s.adapters, afterCheckpoint: async stage => {
      if (stage !== "intent-committed") return;
      await expect(withTorrentUse(s.repo, s.ref, async () => { throw new Error("Producer must never enter I/O"); })).rejects.toThrow("retiring");
      expect(() => s.repo.updateSettings(s.repo.getSettings())).toThrow("retirement");
      throw new Error("Crash before dispatch");
    } });
    await expect(service.execute(s.ctx, s.job, { ...s.locator, operationId })).rejects.toThrow("Crash before dispatch");
    expect(service.intent(operationId)?.phase).toBe("planned"); expect(s.removeCount()).toBe(0);
    s.repo.markJobCancelled(s.job.id, "Fixture cancellation");
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "aborted" });
    expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).not.toThrow();
    expect(s.repo.bookTorrents.get(retirementFixtureHash)).not.toBeNull();
  } finally { await s.close(); }
});

test("lost removal acknowledgement reconciles exact absence after reopening the service without another remove", async () => {
  const s = await retirementFixture();
  try {
    const operationId = randomUUID(); s.loseAck();
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "pending", intent: { phase: "uncertain" } });
    expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).toThrow("retiring");
    const reopened = new TorrentRetirement(s.repo, s.adapters);
    expect(await reopened.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "retired" });
    expect(s.removeCount()).toBe(1); expect(await fs.readFile(s.retainedFile, "utf8")).toBe("seed data must survive");
  } finally { await s.close(); }
});

test("a planned-only daemon restart safely aborts the claim; an uncertain dispatch stays fenced on the same session change", async () => {
  for (const phase of ["planned", "uncertain"] as const) {
    const s = await retirementFixture();
    try {
      const operationId = randomUUID();
      if (phase === "planned") {
        const preparing = new TorrentRetirement(s.repo, { ...s.adapters,
          afterCheckpoint: async stage => { if (stage === "intent-committed") throw new Error("Planned interruption"); } });
        await expect(preparing.execute(s.ctx, s.job, { ...s.locator, operationId })).rejects.toThrow("Planned interruption");
      } else { s.rejectBeforeEffect(); await s.service.execute(s.ctx, s.job, { ...s.locator, operationId }); }
      s.identities.sessionDigest = retirementDigest("new private daemon session");
      if (phase === "planned") {
        expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "aborted" });
        expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).not.toThrow();
        const token = admitTorrentUse(s.repo, s.ref, randomUUID()); finishTorrentUse(s.repo, s.ref, token);
        expect(s.removeCount()).toBe(0);
      } else {
        await expect(s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).rejects.toThrow("session/record changed");
        expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).toThrow("retiring");
        expect(s.removeCount()).toBe(1);
      }
      expect(s.record()).not.toBeNull(); expect(await fs.readFile(s.retainedFile, "utf8")).toBe("seed data must survive");
    } finally { await s.close(); }
  }
});

test("unknown pre-effect acknowledgement retains admission closure and safely retries only the same fenced record", async () => {
  const s = await retirementFixture();
  try {
    const operationId = randomUUID(); s.rejectBeforeEffect();
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "pending" });
    expect(s.record()).not.toBeNull(); expect(() => assertNoPendingTorrentRetirements(s.repo)).toThrow();
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "retired" });
    expect(s.removeCount()).toBe(2);
  } finally { await s.close(); }
});

test("crash/cancellation after dispatch intent never opens admission or resends under a cancelled origin", async () => {
  const s = await retirementFixture();
  try {
    const operationId = randomUUID();
    const crashing = new TorrentRetirement(s.repo, { ...s.adapters, afterCheckpoint: async stage => { if (stage === "dispatch-committed") throw new Error("Crash before RPC"); } });
    await expect(crashing.execute(s.ctx, s.job, { ...s.locator, operationId })).rejects.toThrow("Crash before RPC");
    expect(s.removeCount()).toBe(0); expect(crashing.intent(operationId)?.phase).toBe("dispatching");
    s.repo.markJobCancelled(s.job.id, "Cancel uncertain dispatch");
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "pending" });
    expect(s.removeCount()).toBe(0); expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).toThrow("retiring");
    s.setRecord(null); // A previously issued command may still complete after cancellation.
    expect(await s.service.execute(s.ctx, s.job, { ...s.locator, operationId })).toMatchObject({ status: "retired" });
  } finally { await s.close(); }
});

test("binding, daemon session, seed-time and all-tracker evidence changes cannot authorize a broader replay", async () => {
  for (const change of ["binding", "session", "seed", "tracker"] as const) {
    const s = await retirementFixture();
    try {
      const operationId = randomUUID(); s.rejectBeforeEffect();
      await s.service.execute(s.ctx, s.job, { ...s.locator, operationId });
      if (change === "binding") s.db.query("UPDATE book_torrents SET root_token=?").run(randomUUID());
      if (change === "session") s.identities.sessionDigest = retirementDigest("replacement daemon session");
      if (change === "seed") s.record()!.secondsSeeding = 1;
      if (change === "tracker") (s.record()!.trackerStats as Record<string, any>[])[1]!.lastAnnounceSucceeded = true;
      try { await s.service.execute(s.ctx, s.job, { ...s.locator, operationId }); } catch { /* Changed evidence may be surfaced as a failed durable gate. */ }
      expect(s.removeCount()).toBe(1); expect(s.record()).not.toBeNull();
      expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).toThrow("retiring");
    } finally { await s.close(); }
  }
});

test("intent persistence failure rolls back the claim and never performs removal under a SQLite writer", async () => {
  const s = await retirementFixture();
  try {
    s.db.exec(`CREATE TRIGGER reject_retirement_intent BEFORE INSERT ON app_state WHEN NEW.key LIKE 'torrent-retirement:intent:%'
      BEGIN SELECT RAISE(ABORT,'intent persistence rejected'); END`);
    await expect(s.service.execute(s.ctx, s.job, { ...s.locator, operationId: randomUUID() })).rejects.toThrow("intent persistence rejected");
    expect(s.removeCount()).toBe(0); expect(s.service.pending()).toEqual([]);
    expect(() => assertNoPendingTorrentRetirements(s.repo)).not.toThrow();
    expect(() => assertTorrentDatabaseWritable(s.db, s.ref)).not.toThrow();
  } finally { await s.close(); }
});

test("real default dispatcher/Reflow retirement hook runs inside its own global step, not after the collector lock has released", async () => {
  const s = await retirementFixture();
  let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  try {
    const ctx: Parameters<typeof createJobExecutionRuntime>[0] = { repo: s.repo, getSettings: () => s.repo.getSettings(), onLog() {},
      stewardshipAdapters: { connection: s.connection, now: s.now, retirement: s.adapters } };
    runtime = await createJobExecutionRuntime(ctx, path.join(s.root, "execution.sqlite"), { retryDelayMs: 1 });
    const deadline = Date.now() + 7000;
    while (s.repo.getJob(s.job.id)?.status !== "succeeded") {
      await runtime.reconcile();
      if (Date.now() > deadline) throw new Error(`Default retirement hook did not complete: ${s.repo.getJob(s.job.id)?.error}`);
      await Bun.sleep(15);
    }
    expect(s.removeCount()).toBe(1); expect(s.record()).toBeNull(); expect(ctx.activeLocks!.has("*")).toBe(false);
    expect(await fs.readFile(s.retainedFile, "utf8")).toBe("seed data must survive");
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("admin retirement inspection is read-only and does not expose a direct HTTP removal mutation", async () => {
  const s = await retirementFixture();
  try {
    const router = createTorrentRetirementRouter(s.adapters);
    expect(Object.keys(router.routes)).toEqual(["preview", "get"]);
    const context: RpcContext = { repo: s.repo, startTime: 0, request: new Request("http://localhost/rpc"), session: null };
    await expect(executeMethod(router.routes.preview, context, {})).rejects.toMatchObject({ code: -32001 });
    expect(s.readCount()).toBe(0); expect(s.removeCount()).toBe(0);
    const before = s.db.query("SELECT * FROM app_state ORDER BY key").all();
    const admin = s.repo.upsertUser({ provider: "local", providerUserId: "retirement-admin", username: "admin", isAdmin: true });
    s.repo.createSession(admin.id, hashSessionToken("retirement-admin"), null, "browser");
    context.session = s.repo.getSessionByTokenHash(hashSessionToken("retirement-admin"));
    // No injected fence or held maintenance lane: the real RPC factory must install its own read-only fence.
    expect(await executeMethod(router.routes.preview, context, {})).toMatchObject({
      automaticConfigured: true, items: [{ evidenceQualified: true, eligible: true }],
    });
    expect(s.db.query("SELECT * FROM app_state ORDER BY key").all()).toEqual(before);
    expect(s.readCount()).toBe(0); expect(s.removeCount()).toBe(0);
  } finally { await s.close(); }
});

test("targeted orphan consumer SQL ignores more than 2000 unrelated links/assets but finds exact source and publication dependencies", async () => {
  const s = await retirementFixture();
  try {
    const settings = s.repo.getSettings(); settings.movies.managedRoot = path.join(s.root, "movies"); s.repo.updateSettings(settings);
    const owner = s.repo.createBook({ title: "Unrelated live book", author: "Fixture" });
    const stamp = new Date(s.now()).toISOString();
    const asset = s.db.query("INSERT INTO assets(book_id,kind,mime,total_size,created_at,updated_at) VALUES(?,'single','audio/mp4',1,?,?) RETURNING id")
      .get(owner.id, stamp, stamp) as { id: number };
    const insertFile = s.db.query("INSERT INTO asset_files(asset_id,path,source_path,size,start,end,duration_ms,updated_at) VALUES(?,?,?,1,0,1,1,?)");
    s.db.transaction(() => { for (let index = 0; index < 2005; index++) insertFile.run(asset.id, `/unrelated/content/${index}`, `/unrelated/source/${index}`, stamp); })();
    const movie = s.db.query("INSERT INTO movies(tmdb_id,metadata_json,updated_at) VALUES(99001,'{}',?) RETURNING id").get(stamp) as { id: number };
    const target = s.db.query("INSERT INTO movie_targets(movie_id,policy_key,policy_json,created_at) VALUES(?,'fixture','{}',?) RETURNING id").get(movie.id, stamp) as { id: number };
    const acquisition = s.db.query("INSERT INTO movie_acquisitions(target_id,reason,created_at) VALUES(?,'initial',?) RETURNING id").get(target.id, stamp) as { id: number };
    const insertTorrent = s.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES('fixture:unrelated',?,'{}','fixture') RETURNING id");
    const insertLink = s.db.query("INSERT INTO movie_acquisition_downloads(acquisition_id,torrent_id) VALUES(?,?)");
    s.db.transaction(() => { for (let index = 0; index < 2005; index++) {
      const row = insertTorrent.get(index.toString(16).padStart(40, "0")) as { id: number }; insertLink.run(acquisition.id, row.id);
    } })();
    expect(readConsumerCount(s.db, s.binding)).toMatchObject({ complete: true, count: 0, activePublications: 0 });
    expect(s.service.preview().items[0]).toMatchObject({ eligible: true });
    const hash = "f".repeat(40);
    const row = s.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,'orphan') RETURNING *")
      .get(retirementFixtureClient, hash, JSON.stringify({ rootId: "movies", relativePath: hash })) as Record<string, unknown>;
    const screen = stewardshipBinding(s.repo, "screen", row, retirementFixtureClient)!;
    expect(readConsumerCount(s.db, screen)).toMatchObject({ complete: true, count: 0 });
    insertFile.run(asset.id, "/unrelated/consumer", path.join(screen.directory, "wanted.mkv"), stamp);
    expect(readConsumerCount(s.db, screen).count).toBe(1);
    const version = s.db.query("INSERT INTO movie_versions(movie_id,acquisition_id,facts_json,created_at) VALUES(?,?,'{}',?) RETURNING id")
      .get(movie.id, acquisition.id, stamp) as { id: number };
    s.db.query("INSERT INTO movie_publications(target_id,version_id,expected_revision,created_at) VALUES(?,?,0,?)").run(target.id, version.id, stamp);
    expect(readConsumerCount(s.db, screen).activePublications).toBe(0); // Unrelated publication is not a consumer.
    insertLink.run(acquisition.id, Number(row.id));
    expect(readConsumerCount(s.db, screen)).toMatchObject({ complete: true, activePublications: 1 });
    expect(readConsumerCount(s.db, screen).count).toBeGreaterThan(1);
  } finally { await s.close(); }
});

test.each(["normal", "changed-session", "replacement-record"] as const)("private record-only transport pins session and numeric identity: %s", async mode => {
  const s = await retirementFixture();
  try {
    let row = structuredClone(s.record()), session = "s".repeat(32), removes = 0;
    const methods: string[] = [];
    const identity = { id: retirementFixtureClient.slice("transmission:".length), username: "fixture", password: "p".repeat(32) };
    const fetcher: typeof fetch = Object.assign(async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { method: string; arguments: Record<string, unknown> };
      methods.push(body.method);
      if (body.method === "torrent-remove") {
        removes++;
        expect(body.arguments).toEqual({ ids: [7], "delete-local-data": false });
        if (mode === "changed-session") session = "r".repeat(32);
      }
      if (new Headers(init?.headers).get("x-transmission-session-id") !== session)
        return new Response(null, { status: 409, headers: { "X-Transmission-Session-Id": session } });
      if (body.method === "session-get") return Response.json({ result: "success", arguments: { "session-id": session } });
      if (body.method === "torrent-get") return Response.json({ result: "success", arguments: { torrents: row ? [row] : [] } });
      if (body.method === "torrent-remove") { row = null; return Response.json({ result: "success", arguments: {} }); }
      throw new Error("Forbidden daemon control");
    }, { preconnect: fetch.preconnect });
    const daemon = privateRetirementDaemon({ readIdentity: () => identity, fetcher, now: s.now });
    const snapshot = await daemon.read(s.binding);
    if (mode === "replacement-record") row!.id++;
    if (mode === "normal") { await daemon.removeRecord(s.binding, snapshot, () => {}); expect(row).toBeNull(); expect(removes).toBe(1); }
    else { await expect(daemon.removeRecord(s.binding, snapshot, () => {})).rejects.toThrow(); expect(row).not.toBeNull(); expect(removes).toBe(mode === "changed-session" ? 1 : 0); }
    expect(methods.every(method => ["session-get", "torrent-get", "torrent-remove"].includes(method))).toBe(true);
  } finally { await s.close(); }
});
