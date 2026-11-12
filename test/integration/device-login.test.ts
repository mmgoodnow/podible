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
  const params = { id: grant.id, verifier: grant.verifier };
  expect((await rpc("auth.exchangeDeviceLogin", params)).result).toEqual({ status: "pending", accessToken: null });
  const link = origin + grant.authorizePath;
  const anonymous = await handle(new Request(link));
  expect(anonymous.status).toBe(303);
  expect(new URL(anonymous.headers.get("location")!, origin).searchParams.get("redirectTo")).toBe(grant.authorizePath);
  const cookie = `podible_session=${phone}`;
  const form = await handle(new Request(link, { headers: { cookie } }));
  expect(await form.text()).toContain("Approve sign-in");
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
  expect((await rpc("auth.logout", {}, token)).result).toEqual({ ok: true });
  expect(repo.getSessionByTokenHash(hashSessionToken(token))).toBeNull();
  expect(repo.getSessionByTokenHash(hashSessionToken(phone))?.id).toBe(browser.id);
});

test("pairing grants expire and reject approval after five incorrect codes", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "plex", providerUserId: "phone", username: "viewer" });
  const expiry = new Date(Date.now() + 60_000).toISOString();
  expect(repo.createDeviceLoginGrant("locked", "verifier", "123456", expiry)).toBe(true);
  for (let i = 0; i < 5; i++) expect(repo.approveDeviceLogin("locked", "000000", user.id)).toBe(false);
  expect(repo.deviceLoginNeedsApproval("locked")).toBe(false);
  expect(repo.approveDeviceLogin("locked", "123456", user.id)).toBe(false);
  expect(repo.exchangeDeviceLogin("locked", "verifier", "token")).toBe("unavailable");
  repo.createDeviceLoginGrant("expired", "verifier", "123456", new Date(0).toISOString());
  expect(repo.approveDeviceLogin("expired", "123456", user.id)).toBe(false);
  expect(repo.exchangeDeviceLogin("expired", "verifier", "token")).toBe("unavailable");
});
