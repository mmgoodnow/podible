import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { Hono } from "hono";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createRequestContextMiddleware, requireAuthenticatedRequest, type HttpEnv } from "../../src/http/middleware";

function fixture() {
  const db = new Database(":memory:");
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
  f.age();
  f.repo.touchSession = () => { throw Object.assign(new Error("unexpected activity failure"), { code: "SQLITE_CORRUPT" }); };
  expect((await f.request()).status).toBe(500);
});
