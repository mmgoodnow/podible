import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { stewardshipBinding, type StewardshipConnection } from "../../src/media/torrent-stewardship";
import { MINIMUM_SEED_SECONDS } from "../../src/media/torrent-stewardship-types";
import { recordRetirementEvidence, retirementDigest } from "../../src/media/torrent-retirement-evidence";
import type { RetirementDaemon, RetirementDaemonSnapshot } from "../../src/media/torrent-retirement-transmission";
import { TorrentRetirement } from "../../src/media/torrent-retirement";
import type { WorkerContext } from "../../src/worker/context";

export const retirementFixtureSecret = "must-not-persist-private-tracker-passkey";
export const retirementFixtureClient = "transmission:11111111-1111-4111-8111-111111111111";
export const retirementFixtureHash = "a".repeat(40);
export async function retirementFixture(options: { frozenSeedSeconds?: number } = {}) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "torrent-retirement-")));
  const db = new Database(path.join(root, "domain.sqlite")); runMigrations(db); finalizeJobJournalMigration(db);
  const repo = new BooksRepo(db); repo.bindJobJournalReader({ read: () => new Map() });
  const settings = repo.ensureSettings(); settings.libraryRoot = root; repo.updateSettings(settings);
  const book = repo.createBook({ title: "Deleted source owner", author: "Fixture" });
  const downloadRoot = path.join(root, ".podible", "downloads"), directory = path.join(downloadRoot, retirementFixtureHash);
  const token = randomUUID(); await fs.mkdir(directory, { recursive: true });
  await fs.writeFile(path.join(downloadRoot, ".podible-content.json"), JSON.stringify({ id: token }));
  await fs.writeFile(path.join(directory, ".podible-book-torrent.json"), JSON.stringify({ rootToken: token, clientId: retirementFixtureClient, infoHash: retirementFixtureHash }));
  const retainedFile = path.join(directory, "keep.m4b"); await fs.writeFile(retainedFile, "seed data must survive");
  repo.bookTorrents.reserve({ infoHash: retirementFixtureHash, bookId: book.id, clientId: retirementFixtureClient, rootPath: downloadRoot, rootToken: token });
  db.query("DELETE FROM books WHERE id=?").run(book.id);
  const binding = stewardshipBinding(repo, "books", { ...repo.bookTorrents.get(retirementFixtureHash)! }, retirementFixtureClient)!;
  let now = Date.parse("2026-11-04T12:00:00Z");
  const stats = () => [0, 1].map((index) => ({ id: index + 1, announce: `https://tracker${index}.invalid/${retirementFixtureSecret}/announce`,
    isBackup: index === 1, hasAnnounced: true, lastAnnounceSucceeded: false, lastAnnounceTimedOut: false,
    lastAnnounceTime: Math.floor(now / 1000), lastAnnounceResult: "Unregistered torrent" }));
  let record: RetirementDaemonSnapshot["record"] = { id: 7, hashString: retirementFixtureHash, downloadDir: directory, labels: ["Podible Books"],
    status: 6, leftUntilDone: 0, secondsSeeding: options.frozenSeedSeconds ?? MINIMUM_SEED_SECONDS, totalSize: 22, sizeWhenDone: 22, haveValid: 22, haveUnchecked: 0,
    error: 2, trackerStats: stats() };
  for (let index = 0; index < 3; index++) {
    if (index) now += 15 * 60_000;
    record.trackerStats = stats(); record.secondsSeeding = options.frozenSeedSeconds ?? MINIMUM_SEED_SECONDS + index * 900;
    recordRetirementEvidence(repo, { bindingKey: binding.key, bindingDigest: binding.bindingDigest,
      observedAt: new Date(now).toISOString(), ownershipVerified: true, complete: true, actualSeedSeconds: record.secondsSeeding, trackerStats: record.trackerStats });
  }
  let active = true, loseAck = false, rejectBeforeEffect = false, readHook: (() => void) | undefined;
  let reads = 0, removes = 0;
  const identities = { identityDigest: retirementDigest("private fixture identity"), sessionDigest: retirementDigest("private fixture session") };
  const daemon: RetirementDaemon = { clientId: retirementFixtureClient,
    async read() { if (db.inTransaction) throw new Error("Network read under SQLite writer"); reads++; readHook?.(); return { clientId: retirementFixtureClient, ...identities, record: structuredClone(record) }; },
    async removeRecord(_binding, _snapshot, assertActive) {
      if (db.inTransaction) throw new Error("Network removal under SQLite writer");
      assertActive(); removes++;
      const intent = db.query("SELECT value_json FROM app_state WHERE key LIKE 'torrent-retirement:intent:%'").get() as { value_json: string };
      if (JSON.parse(intent.value_json).phase !== "dispatching") throw new Error("Intent was not durable before removal");
      if (rejectBeforeEffect) { rejectBeforeEffect = false; throw new Error("No acknowledgement before effect"); }
      record = null;
      if (loseAck) { loseAck = false; throw new Error("Lost acknowledgement after effect"); }
    },
  };
  const adapters = { daemon, now: () => now, readIdentityDigest: () => identities.identityDigest };
  const service = new TorrentRetirement(repo, adapters);
  const job = repo.createJob({ type: "torrent_stewardship_collect", maxAttempts: 1 });
  const ctx: WorkerContext = { repo, getSettings: () => repo.getSettings(), onLog() {}, activeLocks: new Set(["*"]), execution: {
    key: randomUUID(), assertActive() { if (!active) throw new Error("Fixture execution cancelled"); },
    async run(_name, action) { return action(); }, async sleep() {},
  } };
  const locator = { source: "books" as const, clientId: retirementFixtureClient, infoHash: retirementFixtureHash };
  const connection: StewardshipConnection = { clientId: retirementFixtureClient, async get(ids) {
    if (db.inTransaction) throw new Error("Collector read under SQLite writer");
    return { torrents: record && ids.includes(record.hashString) ? [structuredClone(record)] : [] };
  } };
  return { root, db, repo, binding, retainedFile, daemon, connection, adapters, service, job, ctx, locator,
    ref: { clientId: retirementFixtureClient, infoHash: retirementFixtureHash },
    readCount: () => reads, removeCount: () => removes, now: () => now, record: () => record,
    loseAck: () => { loseAck = true; }, rejectBeforeEffect: () => { rejectBeforeEffect = true; },
    setRecord: (value: typeof record) => { record = value; }, identities,
    setReadHook: (value: typeof readHook) => { readHook = value; }, setActive: (value: boolean) => { active = value; },
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}
