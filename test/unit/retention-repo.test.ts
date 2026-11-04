import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { RetentionRepo, RetentionRevisionConflictError } from "../../src/media/retention-repo";
import { RETENTION_SCHEMA_SQL } from "../../src/media/retention-schema";
import { recoveryDecision, type ResourceKey } from "../../src/media/retention";

const key: ResourceKey = { namespace: "movie-target", id: "7" };
function database() {
  const db = new Database(":memory:");
  db.exec("PRAGMA foreign_keys=ON");
  db.exec(RETENTION_SCHEMA_SQL);
  return db;
}
const change = (expectedRevision: number, reason = "User intent") => ({ expectedRevision, reason });

describe("retention lifecycle repository", () => {
  test("eviction then missing never recovers until an explicit re-request", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    expect(repo.get(key)).toBeNull();
    expect(repo.request(key, change(0))).toMatchObject({ desiredPresence: "present", actualPresence: "unknown", revision: 1 });
    repo.observe(key, { ...change(1, "Verified manifest on disk"), actualPresence: "present" });
    const evicted = repo.evict(key, change(2, "Operator chose eviction"));
    expect(evicted).toMatchObject({ desiredPresence: "evicted", actualPresence: "present", revision: 3 });
    expect(recoveryDecision(evicted).action).toBe("none");
    const missing = repo.observe(key, { ...change(3, "Verified missing on disk"), actualPresence: "missing" });
    expect(recoveryDecision(missing).action).toBe("none");
    const requested = repo.request(key, change(4, "User re-requested"));
    expect(requested).toMatchObject({ desiredPresence: "present", actualPresence: "missing", revision: 5 });
    expect(recoveryDecision(requested).action).toBe("recover");
    expect(repo.events(key).map((event) => [event.revision, event.action, event.reason])).toEqual([
      [1, "requested", "User intent"], [2, "observed", "Verified manifest on disk"],
      [3, "evicted", "Operator chose eviction"], [4, "observed", "Verified missing on disk"],
      [5, "requested", "User re-requested"],
    ]);
    expect(repo.events(key)[2]).toMatchObject({ key, desiredPresence: "evicted", actualPresence: "present" });
    expect(repo.events(key)[4]!.createdAt).toBe(requested.updatedAt);
  });

  test("identical missing observations distinguish intentional eviction from accidental loss", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    const other = { namespace: "tv-request", id: key.id };
    repo.evict(key, change(0, "Imported intentional eviction"));
    repo.request(other, change(0));
    const intentional = repo.observe(key, { ...change(1), actualPresence: "missing" });
    const accidental = repo.observe(other, { ...change(1), actualPresence: "missing" });
    expect(recoveryDecision(intentional).action).toBe("none");
    expect(recoveryDecision(accidental).action).toBe("recover");
    expect(repo.events(key)).toHaveLength(2);
    expect(repo.events(other)).toHaveLength(2);
  });

  test("withdrawal preserves desired state and complete history, including after re-request", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    repo.request(key, change(0));
    expect(repo.withdraw(key, change(1, "One request withdrawn"))).toMatchObject({ desiredPresence: "present", revision: 2 });
    repo.evict(key, change(2));
    expect(repo.withdraw(key, change(3, "Last request withdrawn"))).toMatchObject({ desiredPresence: "evicted", revision: 4 });
    repo.request(key, change(4));
    expect(repo.events(key).map((event) => event.action)).toEqual(["requested", "withdrawn", "evicted", "withdrawn", "requested"]);
    expect(db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    expect(() => db.query("DELETE FROM retention_resources").run()).toThrow();
    expect(repo.events(key)).toHaveLength(5);
  });

  test("every mutation is CAS guarded, including repeated intent and observation", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    expect(() => repo.request(key, change(1))).toThrow(RetentionRevisionConflictError);
    expect(repo.events(key)).toEqual([]);
    const first = repo.request(key, change(0));
    for (const mutation of [
      () => repo.request(key, change(0)),
      () => repo.evict(key, change(0)),
      () => repo.observe(key, { ...change(0), actualPresence: "missing" as const }),
      () => repo.withdraw(key, change(0)),
    ]) expect(mutation).toThrow(RetentionRevisionConflictError);
    expect(repo.get(key)).toEqual(first);
    expect(repo.events(key)).toHaveLength(1);
    try {
      repo.evict(key, change(0));
      throw new Error("Expected conflict");
    } catch (error) {
      expect(error).toMatchObject({ name: "RetentionRevisionConflictError", key, expectedRevision: 0, actualRevision: 1 });
    }
    expect(repo.request(key, change(1, "Repeated request still records its reason")).revision).toBe(2);
  });

  test("failed atomic intent transaction rolls back request/outbox, state, and event on the same connection", () => {
    using db = database();
    db.exec("CREATE TABLE request_intents(id TEXT PRIMARY KEY); CREATE TABLE outbox(id TEXT PRIMARY KEY)");
    const repo = new RetentionRepo(db);
    repo.evict(key, change(0));
    expect(() => repo.transaction(() => {
      db.query("INSERT INTO request_intents VALUES (?)").run("new-request");
      repo.request(key, change(1, "Re-request within parent transaction"));
      expect(db.inTransaction).toBe(true);
      db.query("INSERT INTO outbox VALUES (?)").run("acquire");
      throw new Error("Failed enqueue intent");
    })).toThrow("Failed enqueue intent");
    expect(repo.get(key)).toMatchObject({ desiredPresence: "evicted", revision: 1 });
    expect(repo.events(key)).toHaveLength(1);
    expect(db.query("SELECT * FROM request_intents").all()).toEqual([]);
    expect(db.query("SELECT * FROM outbox").all()).toEqual([]);
    expect(db.inTransaction).toBe(false);

    expect(() => db.transaction(() => {
      repo.request(key, change(1));
      throw new Error("Outer caller failed");
    })()).toThrow("Outer caller failed");
    expect(repo.get(key)!.revision).toBe(1);
    repo.transaction(() => {
      db.query("INSERT INTO request_intents VALUES (?)").run("new-request");
      repo.request(key, change(1));
      db.query("INSERT INTO outbox VALUES (?)").run("acquire");
    });
    expect(repo.get(key)).toMatchObject({ desiredPresence: "present", revision: 2 });
    expect(db.query("SELECT * FROM outbox").all()).toEqual([{ id: "acquire" }]);
  });

  test("event append failure cannot commit an unjournaled state or initial resource", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    repo.request(key, change(0));
    db.exec(`CREATE TRIGGER fail_retention_event BEFORE INSERT ON retention_events
      BEGIN SELECT RAISE(ABORT, 'journal unavailable'); END`);
    expect(() => repo.evict(key, change(1))).toThrow("journal unavailable");
    expect(repo.get(key)).toMatchObject({ desiredPresence: "present", revision: 1 });
    expect(repo.events(key)).toHaveLength(1);
    const other = { namespace: "book-manifestation", id: "manifestation:9" };
    expect(() => repo.request(other, change(0))).toThrow("journal unavailable");
    expect(repo.get(other)).toBeNull();
  });

  test("guard fences every write/transaction, but reads remain available", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    repo.request(key, change(0));
    let guarded = 0;
    const fenced = new RetentionRepo(db, () => { guarded++; throw new Error("Stale execution"); });
    for (const mutation of [
      () => fenced.request(key, change(1)), () => fenced.evict(key, change(1)),
      () => fenced.observe(key, { ...change(1), actualPresence: "missing" as const }),
      () => fenced.withdraw(key, change(1)), () => fenced.transaction(() => { throw new Error("Never called"); }),
    ]) expect(mutation).toThrow("Stale execution");
    expect(guarded).toBe(5);
    expect(fenced.get(key)!.revision).toBe(1);
    expect(fenced.events(key)).toHaveLength(1);
  });

  test("intent creation never guesses desired state from an observation or withdrawal", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    expect(() => repo.observe(key, { ...change(0), actualPresence: "missing" })).toThrow("Record request or eviction intent");
    expect(() => repo.withdraw(key, change(0))).toThrow("Record request or eviction intent");
    expect(repo.get(key)).toBeNull();
    expect(repo.events(key)).toEqual([]);
  });

  test("validated keys are open namespaces and opaque string IDs, without coercion", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    for (const namespace of ["movie-target", "tv-request", "book-manifestation", "future-resource-v2"])
      expect(repo.request({ namespace, id: "opaque/id:1" }, change(0)).key).toEqual({ namespace, id: "opaque/id:1" });
    for (const invalid of [
      { namespace: "", id: "7" }, { namespace: "Movie", id: "7" }, { namespace: "a_thing", id: "7" },
      { namespace: "a".repeat(65), id: "7" }, { ...key, id: "" }, { ...key, id: " 7" },
      { ...key, id: "7\n8" }, { ...key, id: "x".repeat(257) },
    ]) {
      expect(() => repo.request(invalid, change(0))).toThrow("Invalid retention resource key");
      expect(() => repo.get(invalid)).toThrow("Invalid retention resource key");
      expect(() => repo.events(invalid)).toThrow("Invalid retention resource key");
    }
    for (const revision of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER])
      expect(() => repo.request(key, change(revision))).toThrow("Invalid retention expected revision");
    for (const reason of ["", "   ", "x\n", "x".repeat(1001)])
      expect(() => repo.request(key, change(0, reason))).toThrow("Invalid retention event reason");
    expect(() => repo.observe(key, { ...change(0), actualPresence: "bogus" as "present" })).toThrow("Invalid retention actual presence");
    expect(repo.get(key)).toBeNull();
  });

  test("synchronous transaction contract rejects a thenable and rolls back its synchronous writes", () => {
    using db = database();
    const repo = new RetentionRepo(db);
    expect(() => repo.transaction(() => {
      repo.request(key, change(0));
      return Promise.resolve("not a synchronous result");
    })).toThrow("must be synchronous");
    expect(repo.get(key)).toBeNull();
    expect(repo.events(key)).toEqual([]);
  });

  test("intent, revisions and event history survive closing and reopening the database", () => {
    const directory = mkdtempSync(path.join(tmpdir(), "podible-retention-"));
    try {
      const filename = path.join(directory, "retention.sqlite");
      {
        using db = new Database(filename);
        db.exec(RETENTION_SCHEMA_SQL);
        const repo = new RetentionRepo(db);
        repo.request(key, change(0));
        repo.evict(key, change(1, "Durable intentional eviction"));
      }
      using db = new Database(filename);
      const repo = new RetentionRepo(db);
      expect(repo.get(key)).toMatchObject({ desiredPresence: "evicted", revision: 2 });
      expect(repo.events(key).map((event) => event.action)).toEqual(["requested", "evicted"]);
      expect(() => repo.request(key, change(1))).toThrow(RetentionRevisionConflictError);
      expect(repo.request(key, change(2)).revision).toBe(3);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
