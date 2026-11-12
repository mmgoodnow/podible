import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createSessionToken, hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { BooksRepo } from "../../src/repo";
import type { authRouter } from "../../src/rpc/auth-router";
import type { z } from "zod";

test("Apple TV can exchange one phone approval for a separate revocable session", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "plex", providerUserId: "phone", username: "viewer" });
  const phone = createSessionToken();
  const browser = repo.createSession(user.id, hashSessionToken(phone));
  const handle = createPodibleFetchHandler(repo, Date.now());
  const origin = "https://podible.example";
  const rpc = async <M extends "beginDeviceLogin" | "exchangeDeviceLogin" | "logout">(
    method: `auth.${M}`, params: object = {}, accessToken?: string,
  ) => {
    const response = await handle(new Request(`${origin}/rpc`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}) },
      body: JSON.stringify({ jsonrpc: "2.0", id: "pairing", method, params }),
    }));
    expect(response.headers.get("cache-control")).toBe("no-store");
    return await response.json() as { result: z.output<(typeof authRouter.routes)[M]["resultSchema"]> };
  };
  const { result: grant } = await rpc("auth.beginDeviceLogin");
  expect(grant.authorizePath).toBe(`/auth/device?id=${grant.id}`);
  expect(grant.authorizePath).not.toContain(grant.verifier);
  expect(grant.code).toMatch(/^\d{6}$/);
  expect(grant.authorizePath).not.toContain("code=");
  const machine = "a".repeat(40);
  repo.stageDeviceLoginPlexAccess(grant.id, browser.id, machine, "viewer-test-token");
  const params = { id: grant.id, verifier: grant.verifier };
  expect((await rpc("auth.exchangeDeviceLogin", params)).result).toEqual({ status: "pending", accessToken: null });
  const link = origin + grant.authorizePath;
  const anonymous = await handle(new Request(link));
  expect(anonymous.status).toBe(303);
  expect(new URL(anonymous.headers.get("location")!, origin).searchParams.get("redirectTo")).toBe(grant.authorizePath);
  const cookie = `podible_session=${phone}`;
  const form = await handle(new Request(link, { headers: { cookie } }));
  const markup = await form.text();
  expect(markup).toContain("Approve sign-in");
  expect(markup).not.toContain("viewer-test-token");
  expect((await rpc("auth.exchangeDeviceLogin", params)).result.status).toBe("pending");
  const approve = (code: string, requestOrigin = origin) => handle(new Request(link, {
    method: "POST", headers: { cookie, origin: requestOrigin, "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ code }),
  }));
  expect((await approve(grant.code, "https://other.example")).status).toBe(403);
  expect((await approve(grant.code === "000000" ? "111111" : "000000")).status).toBe(400);
  expect((await approve(grant.code)).status).toBe(200);
  expect((await rpc("auth.exchangeDeviceLogin", { ...params, verifier: createSessionToken() })).result.status).toBe("unavailable");
  const exchanges = await Promise.all(Array.from({ length: 2 }, () => rpc("auth.exchangeDeviceLogin", params)));
  expect(exchanges.map(response => response.result.status).sort()).toEqual(["approved", "unavailable"]);
  const token = exchanges.find(response => response.result.status === "approved")!.result.accessToken!;
  const tv = repo.getSessionByTokenHash(hashSessionToken(token))!;
  expect(tv).toMatchObject({ user_id: user.id, kind: "app" });
  expect(tv.id).not.toBe(browser.id);
  expect(repo.getViewerPlexTokenForSession(tv.id, machine)).toBe("viewer-test-token");
  expect(repo.getViewerPlexTokenForSession(tv.id, "other-machine")).toBeNull();
  expect(repo.getViewerPlexTokenForSession(browser.id, machine)).toBeNull();
  expect((await rpc("auth.logout", {}, token)).result).toEqual({ ok: true });
  expect(repo.getSessionByTokenHash(hashSessionToken(token))).toBeNull();
  expect(repo.getViewerPlexTokenForSession(tv.id, machine)).toBeNull();
  expect(repo.getSessionByTokenHash(hashSessionToken(phone))?.id).toBe(browser.id);
});

test("pairing grants expire and reject approval after five incorrect codes", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "plex", providerUserId: "phone", username: "viewer" });
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const browser = repo.createSession(user.id, "browser-token");
  expect(repo.createDeviceLoginGrant("locked", "verifier", "123456", expiry)).toBe(true);
  for (let i = 0; i < 5; i++) expect(repo.approveDeviceLogin("locked", "000000", user.id, browser.id)).toBe(false);
  expect(repo.deviceLoginNeedsApproval("locked")).toBe(false);
  expect(repo.approveDeviceLogin("locked", "123456", user.id, browser.id)).toBe(false);
  expect(repo.exchangeDeviceLogin("locked", "verifier", "token")).toBe("unavailable");
  repo.createDeviceLoginGrant("expired", "verifier", "123456", new Date(0).toISOString());
  expect(repo.approveDeviceLogin("expired", "123456", user.id, browser.id)).toBe(false);
  expect(repo.exchangeDeviceLogin("expired", "verifier", "token")).toBe("unavailable");
});

test("pairing cannot inherit Plex access staged by another browser session", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const viewer = repo.upsertUser({ provider: "plex", providerUserId: "viewer", username: "viewer" });
  const first = repo.createSession(viewer.id, "first");
  const second = repo.createSession(viewer.id, "second");
  repo.createDeviceLoginGrant("grant", "verifier", "123456", new Date(Date.now() + 60_000).toISOString());
  repo.stageDeviceLoginPlexAccess("grant", first.id, "machine", "other-session-token");
  expect(repo.approveDeviceLogin("grant", "123456", viewer.id, second.id)).toBe(true);
  expect(repo.exchangeDeviceLogin("grant", "verifier", "tv")).toBe("approved");
  const session = repo.getSessionByTokenHash("tv")!;
  expect(repo.getViewerPlexTokenForSession(session.id, "machine")).toBeNull();
});

test("Plex phone sign-in stages only the viewer credential for the configured server", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  repo.updateSettings({ ...settings, auth: { ...settings.auth, mode: "plex", plex: {
    ...settings.auth.plex, machineId: "family-server", ownerToken: "owner-fixture-token",
  } } });
  repo.upsertUser({ provider: "plex", providerUserId: "owner", username: "owner", isAdmin: true });
  const grant = "d".repeat(32);
  repo.createDeviceLoginGrant(grant, "verifier", "123456", new Date(Date.now() + 60_000).toISOString());
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const url = new URL(String(input));
    if (url.origin !== "https://plex.tv") throw new Error("Unexpected upstream");
    const token = new Headers(init?.headers).get("X-Plex-Token");
    if (url.pathname === "/api/v2/pins") return Response.json({ id: 123, code: "fixture-pin" });
    if (url.pathname === "/api/v2/pins/123") return Response.json({ id: 123, code: "fixture-pin", authToken: "phone-fixture-token" });
    if (url.pathname === "/api/v2/user") return Response.json({ id: "viewer", username: "viewer" });
    if (url.pathname === "/api/users" && token === "owner-fixture-token")
      return new Response('<MediaContainer><User id="viewer"><Server machineIdentifier="family-server" /></User></MediaContainer>');
    if (url.pathname === "/api/resources" && token === "phone-fixture-token")
      return new Response('<MediaContainer><Device clientIdentifier="other-server" provides="server" accessToken="other-fixture-token" /><Device clientIdentifier="family-server" provides="server" accessToken="shared-fixture-token" /></MediaContainer>');
    throw new Error("Unexpected Plex call");
  }) as typeof fetch;
  try {
    const origin = "https://podible.example";
    const handler = createPodibleFetchHandler(repo, Date.now());
    const redirect = encodeURIComponent(`/auth/device?id=${grant}`);
    expect((await handler(new Request(`${origin}/login/plex/start?redirectTo=${redirect}`, { method: "POST" }))).status).toBe(200);
    const complete = await handler(new Request(`${origin}/login/plex/complete?pinId=123&redirectTo=${redirect}`));
    const cookie = complete.headers.get("set-cookie")?.split(";")[0];
    expect(cookie).toStartWith("podible_session=");
    const result = await handler(new Request(`${origin}/auth/device?id=${grant}`, {
      method: "POST", headers: { cookie: cookie!, origin, "content-type": "application/x-www-form-urlencoded" },
      body: "code=123456",
    }));
    expect(result.status).toBe(200);
    expect(repo.exchangeDeviceLogin(grant, "verifier", "tv")).toBe("approved");
    const tv = repo.getSessionByTokenHash("tv")!;
    expect(tv.is_admin).toBe(0);
    expect(repo.getViewerPlexTokenForSession(tv.id, "family-server")).toBe("shared-fixture-token");
    expect(repo.getViewerPlexTokenForSession(tv.id, "other-server")).toBeNull();
  } finally { globalThis.fetch = originalFetch; }
});
