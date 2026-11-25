import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { TvShadowRepo } from "../../src/tv/shadow-repo";
import type { SyncTransactionCallback } from "../../src/db-transaction";

test("TV shadow reserves the WAL writer before execution-guard reads can establish a stale snapshot", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tv-shadow-wal-"));
  const filename = path.join(root, "app.sqlite"), db = new Database(filename);
  db.exec("PRAGMA journal_mode=WAL; CREATE TABLE evidence(id INTEGER PRIMARY KEY)");
  const writer = new Database(filename);
  writer.exec("PRAGMA busy_timeout=1");
  let competingCode: string | undefined, guards = 0;
  const repo = new TvShadowRepo(db, () => {
    guards++;
    db.query("SELECT count(*) FROM evidence").get();
    try { writer.query("INSERT INTO evidence VALUES(1)").run(); }
    catch (error) { competingCode = (error as { code?: string }).code; }
  }, () => null);
  try {
    repo.transaction(() => db.query("INSERT INTO evidence VALUES(2)").run());
    expect(competingCode).toBe("SQLITE_BUSY");
    expect(guards).toBe(1);
    expect(db.query("SELECT id FROM evidence").all()).toEqual([{ id: 2 }]);
    expect(db.inTransaction).toBe(false);
    writer.query("INSERT INTO evidence VALUES(3)").run();
  } finally { writer.close(); db.close(); await rm(root, { recursive: true, force: true }); }
});

test("TV shadow transactions reject thenables and retain nested rollback and guard fences", () => {
  const db = new Database(":memory:");
  db.exec("CREATE TABLE evidence(id INTEGER PRIMARY KEY)");
  let stale = false;
  const repo = new TvShadowRepo(db, () => { if (stale) throw new Error("stale execution"); }, () => null);
  try {
    repo.transaction(() => {
      db.query("INSERT INTO evidence VALUES(1)").run();
      expect(() => repo.transaction(() => {
        db.query("INSERT INTO evidence VALUES(2)").run();
        throw new Error("nested failure");
      })).toThrow("nested failure");
    });
    expect(db.query("SELECT id FROM evidence").all()).toEqual([{ id: 1 }]);
    const invalid = (() => {
      db.query("INSERT INTO evidence VALUES(3)").run();
      return { then() {} };
    }) as unknown as SyncTransactionCallback<number>;
    expect(() => repo.transaction(invalid)).toThrow("thenables");
    expect(db.query("SELECT id FROM evidence").all()).toEqual([{ id: 1 }]);
    stale = true;
    expect(() => repo.transaction(() => db.query("INSERT INTO evidence VALUES(4)").run())).toThrow("stale execution");
    expect(db.inTransaction).toBe(false);
  } finally { db.close(); }
});
