import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { openDatabase } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { MovieRepo } from "../../src/movies/repo";
import { TvRepo } from "../../src/tv/repo";
import { withDatabaseTorrentUse } from "../../src/media/torrent-use";
import type { SyncTransactionCallback } from "../../src/db-transaction";

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-shared-transaction-"));
  const filename = path.join(root, "app.sqlite"), db = openDatabase(filename), writer = new Database(filename);
  writer.exec("PRAGMA busy_timeout=1");
  return { db, writer, async close() { writer.close(); db.close(); await rm(root, { recursive: true, force: true }); } };
}

for (const domain of ["books", "movies", "tv"] as const) test(`${domain} transaction helper reserves writer before reads and rejects thenables`, async () => {
  const f = await fixture(); let guards = 0;
  try {
    const guard = () => { guards++; };
    const repo = domain === "books" ? new BooksRepo(f.db, guard) : domain === "movies" ? new MovieRepo(f.db, guard, () => null) : new TvRepo(f.db, guard, () => null);
    repo.transaction(() => {
      f.db.query("SELECT count(*) FROM app_state").get();
      expect(() => f.writer.query("INSERT INTO app_state VALUES('competitor','true','fixture')").run()).toThrow("database is locked");
      f.db.query("INSERT INTO app_state VALUES('owner','true','fixture')").run();
    });
    expect(guards).toBe(1);
    const invalid = (() => {
      f.db.query("INSERT INTO app_state VALUES('rollback','true','fixture')").run();
      return { then() {} };
    }) as unknown as SyncTransactionCallback<number>;
    expect(() => repo.transaction(invalid)).toThrow("thenables");
    expect(f.db.query("SELECT 1 FROM app_state WHERE key='rollback'").get()).toBeNull();
    f.writer.query("INSERT INTO app_state VALUES('after-commit','true','fixture')").run();
    expect(f.db.inTransaction).toBe(false);
  } finally { await f.close(); }
});

test("raw torrent-use admission uses IMMEDIATE but releases SQL ownership before asynchronous I/O", async () => {
  const f = await fixture(); let guards = 0, blocked = 0;
  try {
    await withDatabaseTorrentUse(f.db, () => {
      if (++guards % 2 !== 0) return;
      try { f.writer.query("INSERT OR REPLACE INTO app_state VALUES('competitor','true','fixture')").run(); }
      catch (error) { if ((error as { code?: string }).code !== "SQLITE_BUSY") throw error; blocked++; }
    }, { clientId: `transmission:${randomUUID()}`, infoHash: "a".repeat(40) }, async () => {
      expect(f.db.inTransaction).toBe(false);
      await Bun.sleep(1);
      f.writer.query("INSERT INTO app_state VALUES('outside-sql','true','fixture')").run();
    });
    expect(blocked).toBe(2);
    expect(f.db.query("SELECT 1 FROM app_state WHERE key='competitor'").get()).toBeNull();
    expect(f.db.inTransaction).toBe(false);
  } finally { await f.close(); }
});
