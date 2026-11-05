import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { TorrentStewardship, stewardshipBinding, stewardshipBindingPage } from "../../src/media/torrent-stewardship";
import { stewardshipHealth, type StewardshipObservation } from "../../src/media/torrent-stewardship-types";
import { backfillStewardshipStatusIndex } from "../../src/media/stewardship-status-index";
import { stewardshipIndexKey } from "../../src/media/stewardship-binding";

const clientId = "transmission:11111111-1111-4111-8111-111111111111", token = "11111111-1111-4111-8111-111111111111";
const now = Date.parse("2026-11-04T12:00:00.123Z");
function fixture() {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.libraryRoot = "/fixture/books"; settings.movies.managedRoot = "/fixture/movies"; settings.tv.managedRoot = "/fixture/tv"; repo.updateSettings(settings);
  const service = new TorrentStewardship(repo, { now: () => now, connection: { clientId, async get() { throw new Error("Read-only status must never contact daemon"); } } });
  function book(n: number, override: Partial<StewardshipObservation> = {}) {
    const book = repo.createBook({ title: `Real book ${n}`, author: "Author" }), hash = n.toString(16).padStart(40, "0");
    const row = repo.bookTorrents.reserve({ infoHash: hash, bookId: book.id, clientId, rootPath: "/fixture/books/.podible/downloads", rootToken: token });
    const binding = stewardshipBinding(repo, "books", { ...row }, clientId)!;
    const observation: StewardshipObservation = { observedAt: new Date(now).toISOString(), bindingDigest: binding.bindingDigest,
      ownership: "verified-private-binding", state: "seeding", complete: true, bytesRemaining: 0, daemonError: "none", trackerState: "healthy",
      trackerCount: 1, trackerErrorCount: 0, trackerUnknownCount: 0, trackerSetDigest: null, announcementTime: null,
      actualSeedSeconds: 0, seedCounterRegressed: false, seedProtection: "protected", remainingSeedSeconds: 2592000,
      consistentUnregisteredAnnouncements: 0, repeatedUnregistered: false, reason: "Sanitized observation", ...override };
    const key = `torrent-stewardship:v1:observation:${binding.key}`;
    repo.setJsonState(key, { latest: observation, history: [observation] });
    return { hash, book, binding, key, observation };
  }
  return { db, repo, service, book };
}

test("SQL attention pagination reaches a problem beyond 25/50 healthy bindings and counts all scoped healthy rows", () => {
  const f = fixture();
  try {
    for (let n = 1; n <= 51; n++) f.book(n);
    f.book(52, { state: "paused-incomplete" });
    f.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,'fixture')")
      .run(clientId, "a".repeat(40), JSON.stringify({ rootId: "tv", relativePath: "unknown" }));
    const legacy = f.db.query("SELECT key,value_json FROM app_state WHERE key LIKE 'torrent-stewardship:v1:observation:%' ORDER BY key").all();
    expect(backfillStewardshipStatusIndex(f.db)).toEqual({ scannedCount: 53, indexedCount: 52 });
    expect(f.db.query("SELECT key,value_json FROM app_state WHERE key LIKE 'torrent-stewardship:v1:observation:%' ORDER BY key").all()).toEqual(legacy);
    const before = f.db.query("SELECT total_changes() AS n").get(), queries = spyOn(f.db, "query");
    const first = f.service.status({ view: "attention", limit: 1 });
    expect(queries.mock.calls.length).toBeLessThan(15); queries.mockRestore();
    expect(first.items.map(item => item.label)).toEqual(["Real book 52"]);
    expect(first.summary).toEqual({ scope: "current-private-bindings", total: 53, healthy: 51, attention: 2, excluded: 0 });
    expect(first.nextOffset).toBe(1); expect(first.scannedCount).toBe(1);
    const second = f.service.status({ view: "attention", limit: 1, offset: 1 });
    expect(second.items.map(item => item.media)).toEqual(["tv"]); expect(second.items[0]!.observation).toBeNull(); expect(second.nextOffset).toBeNull();
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.service.status({ view: "all", limit: 25 }).nextOffset).toBe(25);
    expect(stewardshipBindingPage(f.repo, "books", 0, 50, clientId).bindings).toHaveLength(50);
    expect(f.service.status({ source: "books", view: "attention" }).summary?.attention).toBe(1);
  } finally { f.db.close(); }
});

test("all-healthy attention inventory returns no items and no Next even beyond several raw pages", () => {
  const f = fixture();
  try {
    for (let n = 1; n <= 76; n++) f.book(n);
    backfillStewardshipStatusIndex(f.db);
    const status = f.service.status({ view: "attention" });
    expect(status.items).toEqual([]); expect(status.nextOffset).toBeNull();
    expect(status.summary).toMatchObject({ total: 76, healthy: 76, attention: 0 });
  } finally { f.db.close(); }
});

test("unverified rows cannot bury a confirmed problem on later attention pages", () => {
  const f = fixture();
  try {
    for (let n = 1; n <= 30; n++) f.book(n, { trackerState: "unknown" });
    f.book(31, { daemonError: "storage-error" });
    backfillStewardshipStatusIndex(f.db);
    const page = f.service.status({ view: "attention", limit: 1 });
    expect(page.items.map(item => item.label)).toEqual(["Real book 31"]);
    expect(stewardshipHealth(page.items[0]!)).toBe("problem");
    expect(page.nextOffset).toBe(1);
  } finally { f.db.close(); }
});

test("SQL filtering matches the shared JS classifier for freshness, future time, ownership, errors and seed time zero", () => {
  const f = fixture();
  try {
    const entry = f.book(1);
    const cases: Partial<StewardshipObservation>[] = [{}, { actualSeedSeconds: 0 }, { state: "paused-complete" },
      { observedAt: new Date(now + 1).toISOString() }, { observedAt: new Date(now - 3600000).toISOString() },
      { observedAt: new Date(now - 3600001).toISOString() }, { ownership: "engine-unavailable" }, { ownership: "missing-record" },
      { ownership: "binding-unverified" }, { daemonError: "storage-error" }, { daemonError: "tracker-warning" },
      { daemonError: "tracker-error" }, { daemonError: "unknown" }, { trackerState: "confirmed-unregistered" },
      { trackerState: "confirmed-error" }, { trackerState: "unknown" }, { state: "paused-incomplete" }, { state: "unknown" },
      { actualSeedSeconds: null }, { seedProtection: "unknown" }, { seedCounterRegressed: true }];
    for (const override of cases) {
      const latest = { ...entry.observation, ...override };
      f.repo.setJsonState(entry.key, { latest, history: [latest] }); backfillStewardshipStatusIndex(f.db);
      const stale = now < Date.parse(latest.observedAt) || now - Date.parse(latest.observedAt) > 3600000;
      const expected = stewardshipHealth({ observation: latest, stale });
      const actual = f.service.status({ view: "attention" });
      expect({ override, healthy: actual.summary?.healthy, items: actual.items.length }).toEqual({ override, healthy: expected === "healthy" ? 1 : 0, items: expected === "healthy" ? 0 : 1 });
      if (actual.items.length) expect(stewardshipHealth(actual.items[0]!)).toBe(expected);
    }
  } finally { f.db.close(); }
});

test("missing, invalid or changed projection/context is unknown, with no status writes or automatic reconciliation", () => {
  const f = fixture();
  try {
    const entry = f.book(1);
    const before = f.db.query("SELECT total_changes() AS n").get();
    expect(f.service.status({ view: "attention" }).items[0]!.observation).toBeNull();
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    backfillStewardshipStatusIndex(f.db);
    expect(f.service.status({ view: "attention" }).items).toEqual([]);
    const settings = f.repo.getSettings(); settings.libraryRoot = "/changed/books"; f.repo.updateSettings(settings);
    expect(f.service.status({ view: "attention" }).items[0]!.observation).toBeNull();
    settings.libraryRoot = "/fixture/books"; f.repo.updateSettings(settings);
    f.repo.setJsonState(stewardshipIndexKey("books", clientId, entry.hash), { version: 1, contextJson: "{}", bindingDigest: entry.binding.bindingDigest });
    expect(f.service.status({ view: "attention" }).items[0]!.observation).toBeNull();
    backfillStewardshipStatusIndex(f.db);
    f.repo.setJsonState(entry.key, { latest: { ...entry.observation, bindingDigest: "0".repeat(64) }, history: [] });
    const changed = f.service.status({ view: "attention" });
    expect(changed.summary?.healthy).toBe(0); expect(changed.items[0]!.observation).toBeNull();
  } finally { f.db.close(); }
});

test("an index redirected to another healthy observation cannot hide the current binding's problem", () => {
  const f = fixture();
  try {
    const problem = f.book(1, { state: "paused-incomplete" }), healthy = f.book(2);
    backfillStewardshipStatusIndex(f.db);
    const ownKey = stewardshipIndexKey("books", clientId, problem.hash), otherKey = stewardshipIndexKey("books", clientId, healthy.hash);
    const own = f.repo.getJsonState<Record<string, unknown>>(ownKey)!, other = f.repo.getJsonState<Record<string, unknown>>(otherKey)!;
    f.repo.setJsonState(ownKey, { ...other, contextJson: own.contextJson });
    const status = f.service.status({ view: "attention" });
    expect(status.summary).toMatchObject({ total: 2, healthy: 1, attention: 1 });
    expect(status.items.map(item => item.label)).toEqual(["Real book 1"]);
    expect(status.items[0]!.observation).toBeNull();
    expect(stewardshipHealth(f.service.status().items[0]!)).toBe("problem");
  } finally { f.db.close(); }
});

test("malformed UUID groups are excluded before pagination just as they are by the binding validator", () => {
  const f = fixture();
  try {
    const entry = f.book(1);
    f.db.query("UPDATE media_torrents SET root_token=? WHERE native_id=?").run("1111111--1111-4111-8111-111111111111", entry.hash);
    const status = f.service.status({ view: "attention" });
    expect(status.items).toEqual([]);
    expect(status.summary).toEqual({ scope: "current-private-bindings", total: 0, healthy: 0, attention: 0, excluded: 1 });
    expect(status.nextOffset).toBeNull();
  } finally { f.db.close(); }
});

test("migration 55 backfills existing healthy observations without altering history or repeating writes", () => {
  const f = fixture();
  try {
    f.book(1); f.book(2, { trackerState: "confirmed-error" });
    const history = () => f.db.query("SELECT key,value_json,updated_at FROM app_state WHERE key LIKE 'torrent-stewardship:v1:observation:%' ORDER BY key").all();
    const before = history();
    f.db.exec("DELETE FROM schema_migrations WHERE id=55; ALTER TABLE media_torrents DROP COLUMN stewardship_binding_json");
    runMigrations(f.db);
    expect(history()).toEqual(before);
    expect(f.db.query("SELECT id FROM schema_migrations WHERE id=55").get()).toEqual({ id: 55 });
    const status = f.service.status({ view: "attention" });
    expect(status.summary).toMatchObject({ total: 2, healthy: 1, attention: 1 });
    expect(status.items[0]!.observation?.trackerState).toBe("confirmed-error");
    const changes = f.db.query("SELECT total_changes() AS n").get();
    runMigrations(f.db);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(changes);
  } finally { f.db.close(); }
});
