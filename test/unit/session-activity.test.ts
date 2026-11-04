import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { Hono } from "hono";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createRequestContextMiddleware, requireAuthenticatedRequest, type HttpEnv } from "../../src/http/middleware";

function fixture(db = new Database(":memory:")) {
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "activity-test", username: "reader", isAdmin: false });
  const token = "activity-test-session";
  const session = repo.createSession(user.id, hashSessionToken(token), new Date(Date.now() + 60_000).toISOString());
  const app = new Hono<HttpEnv>();
  app.use("*", createRequestContextMiddleware(repo));
  app.get("/", requireAuthenticatedRequest, (c) => c.json({ ok: true }));
  return {
    db, repo, session,
    age: () => db.query("UPDATE sessions SET last_seen_at=? WHERE id=?").run(new Date(Date.now() - 120_000).toISOString(), session.id),
    request: () => app.request("https://example.test/", { headers: { Cookie: `podible_session=${token}` } }),
    [Symbol.dispose]: () => db.close(),
  };
}

test("a page's authenticated image/API burst updates activity at most once per minute", async () => {
  using f = fixture();
  let touches = 0;
  const touch = f.repo.touchSession.bind(f.repo);
  f.repo.touchSession = (id) => { touches++; return touch(id); };
  expect((await f.request()).status).toBe(200);
  expect(touches).toBe(0);
  f.age();
  for (let i = 0; i < 5; i++) expect((await f.request()).status).toBe(200);
  expect(touches).toBe(1);
});

test("busy activity writes cannot turn authenticated reads into HTTP 500s", async () => {
  using f = fixture();
  f.age();
  let attempts = 0;
  f.repo.touchSession = () => {
    attempts++;
    throw Object.assign(new Error("database is locked"), { code: "SQLITE_BUSY" });
  };
  const response = await f.request();
  expect(response.status).toBe(200);
  expect(response.headers.get("set-cookie")).toContain("podible_session=");
  for (let i = 0; i < 5; i++) expect((await f.request()).status).toBe(200);
  expect(attempts).toBe(1);
  f.repo.deleteSession(f.session.id);
  expect((await f.request()).status).toBe(401);
});

test("throttled activity never bypasses session expiry checks", async () => {
  using f = fixture();
  expect((await f.request()).status).toBe(200);
  f.db.query("UPDATE sessions SET expires_at=? WHERE id=?").run(new Date(Date.now() - 1000).toISOString(), f.session.id);
  expect((await f.request()).status).toBe(401);
});

test("unexpected activity storage failures are not hidden as lock contention", async () => {
  using f = fixture();
  f.db.exec("PRAGMA busy_timeout=2000");
  f.age();
  f.repo.touchSession = () => { throw Object.assign(new Error("unexpected activity failure"), { code: "SQLITE_CORRUPT" }); };
  expect((await f.request()).status).toBe(500);
  expect(f.db.query("PRAGMA busy_timeout").get()).toEqual({ timeout: 2000 });
});

test("optional activity never waits on an independent WAL writer or changes the durable write timeout", async () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "session-activity-contention-"));
  try {
    const filename = path.join(root, "app.sqlite");
    const db = new Database(filename);
    db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=2000;");
    using f = fixture(db);
    using writer = new Database(filename);
    f.age();
    writer.exec("BEGIN IMMEDIATE");
    try {
      const start = performance.now();
      expect((await f.request()).status).toBe(200);
      const elapsedMs = performance.now() - start;
      console.log(JSON.stringify({ scenario: "authenticated read during independent WAL write", elapsedMs: Math.round(elapsedMs) }));
      expect(elapsedMs).toBeLessThan(500);
      expect(f.db.query("PRAGMA busy_timeout").get()).toEqual({ timeout: 2000 });
      const lastSeen = f.repo.getSessionByTokenHash(hashSessionToken("activity-test-session"))!.last_seen_at;
      expect(Date.parse(lastSeen)).toBeLessThan(Date.now() - 60_000);
    } finally { writer.exec("ROLLBACK"); }
    expect((await f.request()).status).toBe(200);
    expect(f.repo.touchSessionWithoutWaiting(f.session.id)?.id).toBe(f.session.id);
    expect(f.db.query("PRAGMA busy_timeout").get()).toEqual({ timeout: 2000 });
    f.repo.deleteSession(f.session.id);
    expect((await f.request()).status).toBe(401);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
