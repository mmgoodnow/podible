import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { collectTransferMetrics } from "../../src/media/transfer-metrics-collect";
import { crossSeedCommandSchema, crossSeedStateKey } from "../../src/media/cross-seed-state";
import { prepareReuseLayoutRoot } from "../../src/media/reuse-layout";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { processJob } from "../../src/worker/dispatcher";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "transfer-metrics-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db); const repo = new BooksRepo(db);
  const settings = repo.ensureSettings(); settings.movies.managedRoot = root; settings.libraryRoot = path.join(root, "library"); repo.updateSettings(settings);
  const clientId = `transmission:${randomUUID()}`;
  let now = Date.parse("2026-11-05T12:00:00Z"), calls = 0;
  const rows: Record<string, unknown>[] = [];
  const tracker = (host: string) => ({ announce: `https://user:secret@${host}/PRIVATE-PASSKEY/announce?auth=SECRET`,
    hasAnnounced: true, lastAnnounceTime: Math.floor(now / 1000), lastAnnounceSucceeded: true, lastAnnounceTimedOut: false,
    lastAnnounceResult: "PRIVATE ERROR WITH SECRET" });
  async function add(n: number, hosts = ["tracker.example"], crossSeed = false, book = false) {
    const hash = n.toString(16).padStart(40, "0"), relativePath = book ? "reuse/layout/data" : `fixture-${n}`;
    const downloads = book ? path.join(settings.libraryRoot, ".podible/downloads") : path.join(root, "downloads");
    await fs.mkdir(path.join(downloads, relativePath), { recursive: true });
    if (book) await prepareReuseLayoutRoot({ rootPath: path.join(downloads, "reuse"), ownerId: clientId }).catch(async () => {
      await fs.writeFile(path.join(downloads, "reuse", ".podible-reuse-root.json"), JSON.stringify({ version: 1, id: clientId }));
    });
    const bookRow = book ? repo.createBook({ title: "Owned audiobook", author: "Writer" }) : null;
    const scope = { media: book ? "audio" as const : "movie" as const, targetId: bookRow?.id ?? n };
    const ref = { clientId, nativeId: hash }, location = { rootId: book ? "books" : "movies", relativePath };
    const operationId = randomUUID();
    const record = repo.reserveReuseTorrent(ref, location, `offer-${n}`, scope, operationId);
    if (crossSeed) {
      const command = crossSeedCommandSchema.parse({ operationId, scope, releaseId: 1 });
      const job = repo.createJob({ type: "torrent_reuse" });
      repo.setJsonState(crossSeedStateKey(operationId), { command, jobId: job.id, commandHash: "fixture", createdAt: new Date(now).toISOString(),
        outcome: { kind: "seeding", at: new Date(now).toISOString(), torrentId: record.id, infoHash: hash,
          reusedBytes: 96, downloadedBytes: 4, totalBytes: 100, accountingVersion: 1, sidecarDownloadBytes: 2, contentRepairBytes: 2, contentFileCount: 1 } });
    }
    const row = { hashString: hash, downloadDir: path.join(downloads, relativePath), labels: [book ? "Podible Books" : "Podible Movies"],
      status: 6, totalSize: 100, haveValid: 100, haveUnchecked: 0, uploadedEver: 1000, downloadedEver: 100,
      rateUpload: 20, rateDownload: 0, secondsSeeding: 200, error: 0, trackerStats: hosts.map(tracker) };
    rows.push(row); return { row, record, ref, scope };
  }
  const adapters = { now: () => now, connection: { clientId, get: async (ids: string[], fields: readonly string[]) => {
    calls++; expect(fields).not.toContain("peers"); return { torrents: rows.filter(row => ids.includes(String(row.hashString))) };
  } } };
  return { db, repo, root, rows, add, adapters, advance: (seconds = 60) => { now += seconds * 1000; },
    now: () => now, calls: () => calls, close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("collector persists all registered movie/book reuse telemetry, sanitizes trackers, and attributes multi-tracker traffic once", async () => {
  const f = await fixture();
  try {
    const first = await f.add(1), second = await f.add(2, ["tracker.example", "other.example"], true), book = await f.add(3, ["book.example"], true, true);
    expect(f.repo.transferMetrics.status().summary).toBeNull();
    await collectTransferMetrics(f.repo, f.adapters);
    const status = f.repo.transferMetrics.status({}, f.now());
    expect(status.stale).toBe(false);
    expect(status.summary).toMatchObject({ seedBytes: 300, seedingTorrents: 3, uploadedBytes: 3000,
      downloadedBytes: 300, uploadBytesPerSecond: 60, crossSeedUploadedBytes: 2000, seedSeconds: 600, contentUnits: 1, averageCopies: 1 });
    expect(status.reuse).toMatchObject({ attempts: 2, successful: 2, matchRate: 1, verifiedReusedBytes: 192, sidecarDownloadBytes: 4, contentRepairBytes: 4 });
    expect(status.trackers.find(t => t.host === "tracker.example")!.uploadedBytes).toBeNull();
    expect(status.trackers.find(t => t.host === "tracker.example")!.uploadBytesPerSecond).toBeNull();
    expect(status.trackers.find(t => t.host === "book.example")!.uploadedBytes).toBe(1000);
    expect(status.trackers.find(t => t.host === "book.example")!.uploadBytesPerSecond).toBe(20);
    expect(f.repo.transferMetrics.record(book.record.id)?.verified).toBe(true);
    const persisted = JSON.stringify(f.db.query("SELECT * FROM transfer_metric_records").all()) + JSON.stringify(status);
    for (const secret of ["PRIVATE-PASSKEY", "SECRET", "user:secret", "announce?", "PRIVATE ERROR"]) expect(persisted).not.toContain(secret);
    f.repo.transferMetrics.inferTracker("tracker.example", "Friendly tracker", new Date(f.now()).toISOString());
    expect(f.repo.transferMetrics.status({}, f.now()).trackers.some(t => t.name === "Friendly tracker")).toBe(true);
    f.advance(); first.row.uploadedEver = 1200; second.row.uploadedEver = 50; book.row.uploadedEver = 1100;
    await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.uploadedBytes).toBe(3350);
    const count = f.calls(); f.repo.transferMetrics.status(); expect(f.calls()).toBe(count);
    f.advance(); f.rows.splice(0, 1);
    await collectTransferMetrics(f.repo, f.adapters);
    const partial = f.repo.transferMetrics.status({}, f.now());
    expect(partial.stale).toBe(true); expect(partial.summary!.seedingTorrents).toBeNull();
    expect(partial.summary!.uploadBytesPerSecond).toBeNull(); expect(partial.summary!.uploadedBytes).toBe(3350);
    expect(partial.history).toHaveLength(3);
    expect(partial.trackers.every(tracker => tracker.uploadBytesPerSecond === null)).toBe(true);
  } finally { await f.close(); }
});

test("ownership mismatches and selected-zero byte telemetry never produce seeding metrics; snapshots age without zeroing", async () => {
  const f = await fixture();
  try {
    const good = await f.add(1), bad = await f.add(2);
    bad.row.labels = ["Unmanaged"]; good.row.haveValid = 50;
    await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.record(bad.record.id)?.verified).toBe(false);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.seedBytes).toBeNull();
    bad.row.labels = ["Podible Movies"]; good.row.haveValid = 100; f.advance();
    await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.seedBytes).toBe(200);
    f.advance(181);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.seedBytes).toBeNull();
    await collectTransferMetrics(f.repo, { ...f.adapters, connection: null });
    expect(f.repo.transferMetrics.status({}, f.now()).unavailableReason).toContain("unavailable");
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.uploadedBytes).toBe(2000);
  } finally { await f.close(); }
});

test("metrics collection executes durably without torrent controls and preserves schema56 data across migration replay", async () => {
  const f = await fixture();
  try {
    await f.add(1);
    const job = f.repo.createJob({ type: "torrent_metrics_collect" });
    const runtime = await createJobExecutionRuntime({ repo: f.repo, getSettings: () => f.repo.getSettings(), transferMetricsAdapters: f.adapters },
      path.join(f.root, "execution.sqlite"), { processJob });
    try {
      await runtime.reconcile(); const end = Date.now() + 5000;
      while (!["succeeded", "failed"].includes(f.repo.getJob(job.id)!.status)) {
        if (Date.now() > end) throw new Error("Metrics execution timed out"); await Bun.sleep(10);
      }
      expect(f.repo.getJob(job.id)).toMatchObject({ status: "succeeded", error: null });
    } finally { await runtime.stop(); }
    runMigrations(f.db);
    expect(f.db.query("SELECT id FROM schema_migrations WHERE id=56").get()).toBeDefined();
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.seedingTorrents).toBe(1);
  } finally { await f.close(); }
});

test("tracker changes lose traffic attribution, retirement preserves totals, and raw/hourly history is bounded", async () => {
  const f = await fixture();
  try {
    const torrent = await f.add(1);
    await collectTransferMetrics(f.repo, f.adapters);
    f.repo.transferMetrics.inferTracker("tracker.example", "Configured provider", new Date(f.now()).toISOString());
    expect(f.repo.transferMetrics.status({}, f.now()).mappings[0]).toMatchObject({ name: "Configured provider", source: "inferred" });
    // Legacy overrides cannot affect automatic tracker identity.
    f.db.query("UPDATE transfer_tracker_mappings SET manual_name='Manual name' WHERE hostname='tracker.example'").run();
    expect(f.repo.transferMetrics.status({}, f.now()).mappings[0]).toMatchObject({ name: "Configured provider", source: "inferred" });
    f.repo.transferMetrics.inferTracker("tracker.example", "Changed source", new Date(f.now()).toISOString());
    expect(f.repo.transferMetrics.status({}, f.now()).mappings[0]?.source).toBe("unknown");
    torrent.row.trackerStats = [{ ...torrent.row.trackerStats[0]!, announce: "https://different.example/private" }];
    torrent.row.uploadedEver = 1100; f.advance(); await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).trackers.find(t => t.host === "different.example")!.uploadedBytes).toBeNull();
    f.db.query("DELETE FROM media_torrents WHERE id=?").run(torrent.record.id);
    f.advance(); await collectTransferMetrics(f.repo, f.adapters);
    const retired = f.repo.transferMetrics.status({}, f.now());
    expect(retired.summary!.seedingTorrents).toBe(0); expect(retired.summary!.uploadedBytes).toBe(1100);
    expect(retired.trackers.find(t => t.host === "different.example")!.seedingTorrents).toBe(0);
    const since = retired.trackingSince;
    f.advance(91 * 24 * 3600); await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).trackingSince).toBe(since);
    expect(f.db.query("SELECT resolution,COUNT(*) AS n FROM transfer_metric_samples GROUP BY resolution ORDER BY resolution").all())
      .toEqual([{ resolution: "hourly", n: 1 }, { resolution: "raw", n: 1 }]);
  } finally { await f.close(); }
});

test("paused copies do not inflate seeding coverage; seed time survives resets, outages and retirement", async () => {
  const f = await fixture();
  try {
    const book = await f.add(1, ["book.example"], true, true);
    await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.averageCopies).toBe(1);
    book.row.status = 0; book.row.secondsSeeding = 250; f.advance();
    await collectTransferMetrics(f.repo, f.adapters);
    const paused = f.repo.transferMetrics.status({}, f.now());
    expect(paused.summary!.averageCopies).toBe(0); expect(paused.summary!.seedSeconds).toBe(250);
    expect(paused.history.at(-1)!.trackers[0]!.seedingTorrents).toBe(0);
    book.row.status = 6; book.row.secondsSeeding = 20; f.advance();
    await collectTransferMetrics(f.repo, f.adapters);
    expect(f.repo.transferMetrics.status({}, f.now()).summary!.seedSeconds).toBe(270);
    f.advance(); await collectTransferMetrics(f.repo, { ...f.adapters, connection: null });
    const missing = f.repo.transferMetrics.status({}, f.now());
    expect(missing.summary!.seedSeconds).toBe(270);
    expect(missing.history.at(-1)!.trackers).toEqual([]);
    f.db.query("DELETE FROM media_torrents WHERE id=?").run(book.record.id);
    f.advance(); await collectTransferMetrics(f.repo, f.adapters);
    const retired = f.repo.transferMetrics.status({}, f.now());
    expect(retired.summary!.seedSeconds).toBe(270); expect(retired.summary!.seedingTorrents).toBe(0);
  } finally { await f.close(); }
});

test("old reuse receipts retain verified payload but cannot claim unknown sidecar or repair accounting", async () => {
  const f = await fixture();
  try {
    await f.add(1, ["tracker.example"], true);
    const receipt = f.repo.transferMetrics.receipts()[0]!;
    delete receipt.outcome!.accountingVersion; delete receipt.outcome!.sidecarDownloadBytes;
    f.repo.setJsonState(crossSeedStateKey(receipt.command.operationId), receipt);
    await collectTransferMetrics(f.repo, f.adapters);
    const status = f.repo.transferMetrics.status({}, f.now());
    expect(status.reuse.verifiedReusedBytes).toBe(96);
    expect(status.reuse.sidecarDownloadBytes).toBeNull(); expect(status.reuse.contentRepairBytes).toBeNull();
  } finally { await f.close(); }
});
