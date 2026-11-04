import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { CompiledQuery, sql } from "kysely";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { dbSql, typedAll, typedGet, typedRun } from "../../src/db/typed";

function database() {
  const db = new Database(":memory:");
  runMigrations(db);
  finalizeJobJournalMigration(db);
  return db;
}

const insertSetting = (value: string) => dbSql.insertInto("settings").values({ id: 1, value_json: value });
const settings = () => dbSql.selectFrom("settings").selectAll();

describe("synchronous typed SQLite", () => {
  test("compiles SQLite placeholders and preserves aliases, nulls and missing rows", () => {
    using db = database();
    const query = dbSql.selectFrom("books").select(["id", "cover_path as cover"]).where("title", "=", "O'Brien");
    expect(query.compile().sql).toContain('"title" = ?');
    expect(query.compile().parameters).toEqual(["O'Brien"]);
    expect(typedGet(db, query)).toBeNull();
    const result = typedRun(db, dbSql.insertInto("books").values({
      title: "O'Brien", author: "Author", added_at: "now", updated_at: "now",
    }));
    expect(result).toEqual({ changes: 1, lastInsertRowid: 1 });
    expect(typedGet(db, query)).toEqual({ id: 1, cover: null });
    expect(typedAll(db, query.compile())).toEqual([{ id: 1, cover: null }]);
  });

  test("insert/update/delete RETURNING uses inferred native rows", () => {
    using db = database();
    expect(typedGet(db, insertSetting("first").returning(["id", "value_json"]))).toEqual({ id: 1, value_json: "first" });
    expect(typedAll(db, dbSql.updateTable("settings").set({ value_json: "second" }).where("id", "=", 1).returningAll()))
      .toEqual([{ id: 1, value_json: "second" }]);
    expect(typedGet(db, dbSql.deleteFrom("settings").where("id", "=", 1).returning("id"))).toEqual({ id: 1 });
    expect(typedAll(db, settings())).toEqual([]);
  });

  test("does not coerce JSON, timestamps, integers, floats, bigint bindings, null or blobs", () => {
    using db = database();
    const blob = new Uint8Array([0, 128, 255]);
    const result = typedGet(db, sql<{
      json: string; timestamp: string; flag: number; fraction: number; integer: number; empty: null; blob: Uint8Array;
    }>`select ${'{"enabled":true}'} as json, ${"2026-11-03T00:00:00Z"} as timestamp,
      ${1} as flag, ${1.25} as fraction, ${42n} as integer, ${null} as empty, ${blob} as blob`.compile(dbSql));
    expect(result).toEqual({
      json: '{"enabled":true}', timestamp: "2026-11-03T00:00:00Z", flag: 1, fraction: 1.25,
      integer: 42, empty: null, blob,
    });
    expect(result?.blob).toBeInstanceOf(Uint8Array);
  });

  test("does not override the caller's safeIntegers connection option", () => {
    using db = new Database(":memory:", { safeIntegers: true });
    expect(typedGet(db, sql<{ value: bigint }>`select ${2n ** 63n - 1n} as value`.compile(dbSql)))
      .toEqual({ value: 2n ** 63n - 1n });
    expect(typedGet(db, sql<{ value: bigint }>`select ${-(2n ** 63n)} as value`.compile(dbSql)))
      .toEqual({ value: -(2n ** 63n) });
  });

  test.each([
    undefined, true, false, {}, [], new Date(0), new ArrayBuffer(1), new DataView(new ArrayBuffer(1)),
    Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1,
    2n ** 63n, -(2n ** 63n) - 1n, Symbol("invalid"), () => 1,
  ].map((value) => ({ value })))("rejects unsupported/lossy parameter %# without executing", ({ value }) => {
    using db = database();
    expect(() => typedRun(db, CompiledQuery.raw("insert into settings (id, value_json) values (1, ?)", [value])))
      .toThrow("Invalid SQLite parameter at index 0");
    expect(typedAll(db, settings())).toEqual([]);
  });

  test("validates all bindings before preparing SQL", () => {
    using db = database();
    const query = CompiledQuery.raw("insert into nonexistent values (?, ?)", ["secret", undefined]);
    expect(() => typedRun(db, query)).toThrow("Invalid SQLite parameter at index 1");
  });

  test("rejects missing/extra bindings instead of reusing cached statement values", () => {
    using db = database();
    const query = dbSql.selectFrom("settings").selectAll().where("id", "=", 1).compile();
    typedRun(db, insertSetting("first"));
    expect(typedGet(db, query)).toEqual({ id: 1, value_json: "first" });
    expect(() => typedGet(db, { ...query, parameters: [] })).toThrow("parameter count mismatch");
    expect(() => typedAll(db, { ...query, parameters: [1, 2] })).toThrow("parameter count mismatch");
    expect(() => typedRun(db, CompiledQuery.raw("delete from settings where id = ?", [])))
      .toThrow("parameter count mismatch");
    expect(typedGet(db, query)).toEqual({ id: 1, value_json: "first" });
  });

  test("shares the existing transaction with native SQL and rolls all writes back", () => {
    using db = database();
    expect(() => db.transaction(() => {
      typedRun(db, insertSetting("typed"));
      expect(db.inTransaction).toBe(true);
      expect(db.query("select value_json from settings").get()).toEqual({ value_json: "typed" });
      db.query("update settings set value_json = ?").run("native");
      expect(typedGet(db, settings())?.value_json).toBe("native");
      throw new Error("rollback");
    })()).toThrow("rollback");
    expect(db.inTransaction).toBe(false);
    expect(typedAll(db, settings())).toEqual([]);
  });

  test("uses Bun's existing nested transaction/savepoint behavior", () => {
    using db = database();
    db.transaction(() => {
      typedRun(db, insertSetting("outer"));
      expect(() => db.transaction(() => {
        typedRun(db, dbSql.updateTable("settings").set({ value_json: "inner" }));
        expect(typedGet(db, settings())?.value_json).toBe("inner");
        throw new Error("inner rollback");
      })()).toThrow("inner rollback");
      expect(db.inTransaction).toBe(true);
      expect(typedGet(db, settings())?.value_json).toBe("outer");
    })();
    expect(typedGet(db, settings())?.value_json).toBe("outer");
    expect(() => db.transaction(() => {
      db.transaction(() => typedRun(db, dbSql.updateTable("settings").set({ value_json: "nested commit" })))();
      throw new Error("outer rollback");
    })()).toThrow("outer rollback");
    expect(typedGet(db, settings())?.value_json).toBe("outer");
  });

  test("compiler refuses accidental asynchronous Kysely execution", async () => {
    await expect(settings().execute()).rejects.toThrow("dbSql only compiles SQL");
  });
});
