import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { immediateTransaction, type SyncTransactionCallback } from "../../src/db-transaction";

function fixture() {
  const db = new Database(":memory:");
  db.exec("CREATE TABLE values_written(value TEXT)");
  return { db, write: (value: string) => db.query("INSERT INTO values_written VALUES (?)").run(value),
    values: () => db.query("SELECT value FROM values_written ORDER BY rowid").all() };
}

test("synchronous transactions preserve results, guard order and nested savepoints", () => {
  const f = fixture();
  const events: string[] = [];
  try {
    const result = immediateTransaction(f.db, () => { events.push("guard"); }, () => {
      events.push("body"); f.write("outer");
      expect(() => immediateTransaction(f.db, () => {}, () => {
        f.write("inner"); throw new Error("inner rollback");
      })).toThrow("inner rollback");
      f.write("after inner");
      return { count: 2 };
    });
    expect(events).toEqual(["guard", "body"]);
    expect(result).toEqual({ count: 2 });
    expect(f.values()).toEqual([{ value: "outer" }, { value: "after inner" }]);
    expect(() => immediateTransaction(f.db, () => {}, () => {
      immediateTransaction(f.db, () => {}, () => f.write("nested committed"));
      throw new Error("outer rollback");
    })).toThrow("outer rollback");
    expect(f.values()).toHaveLength(2);
    expect(f.db.inTransaction).toBe(false);
  } finally { f.db.close(); }
});

test("async bodies and guards are rejected before invoking their side effects", () => {
  const f = fixture(); let entered = false;
  try {
    const body = async () => { entered = true; f.write("async body"); return 1; };
    expect(() => immediateTransaction(f.db, () => {}, body as unknown as SyncTransactionCallback<number>)).toThrow("must be synchronous");
    const guard = async () => { entered = true; f.write("async guard"); };
    expect(() => immediateTransaction(f.db, guard, () => f.write("body"))).toThrow("must be synchronous");
    expect(entered).toBe(false); expect(f.values()).toEqual([]);
    expect(f.db.inTransaction).toBe(false);
  } finally { f.db.close(); }
});

test("promise and custom thenable results roll back before commit without invoking then", () => {
  const f = fixture(); let thenCalled = false;
  try {
    for (const value of [Promise.resolve(1), { then() { thenCalled = true; } }, Object.assign(() => {}, { then() { thenCalled = true; } })]) {
      const body = (() => { f.write("must roll back"); return value; }) as unknown as SyncTransactionCallback<number>;
      expect(() => immediateTransaction(f.db, () => {}, body)).toThrow("thenables");
      expect(f.values()).toEqual([]); expect(f.db.inTransaction).toBe(false);
    }
    expect(thenCalled).toBe(false);
    const guard = (() => { f.write("guard rollback"); return Promise.resolve(); }) as () => void;
    expect(() => immediateTransaction(f.db, guard, () => f.write("body"))).toThrow("thenables");
    expect(f.values()).toEqual([]);
    expect(() => immediateTransaction(f.db, () => { throw new Error("ownership lost"); }, () => f.write("fenced")))
      .toThrow("ownership lost");
    expect(f.values()).toEqual([]); expect(f.db.inTransaction).toBe(false);
  } finally { f.db.close(); }
});
