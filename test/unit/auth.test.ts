import { describe, expect, test } from "bun:test";

import {
  buildSessionCookie,
  clearSessionCookie,
  hashSessionToken,
  resolveBearerSessionFromRequest,
  resolveBrowserSessionFromRequest,
  resolveSessionFromRequest,
  SESSION_COOKIE_NAME,
} from "../../src/books/auth";
import type { SessionWithUserRow } from "../../src/books/types";

function makeSession(token: string, overrides: Partial<SessionWithUserRow> = {}): SessionWithUserRow {
  const tokenHash = hashSessionToken(token);
  return {
    id: 1,
    user_id: 1,
    kind: "browser" as const,
    token_hash: tokenHash,
    expires_at: new Date(Date.now() + 60_000).toISOString(),
    created_at: new Date().toISOString(),
    last_seen_at: new Date().toISOString(),
    provider: "plex" as const,
    provider_user_id: "plex-1",
    username: "user",
    display_name: "User",
    thumb_url: null,
    is_admin: 1,
    ...overrides,
  };
}

describe("auth", () => {
  test("resolves a browser session from the session cookie", () => {
    const token = "session-token";
    const request = new Request("http://example.com/library", {
      headers: { Cookie: `${SESSION_COOKIE_NAME}=${token}` },
    });

    const session = resolveBrowserSessionFromRequest(request, (tokenHash) =>
      tokenHash === hashSessionToken(token) ? makeSession(token) : null
    );

    expect(session?.user_id).toBe(1);
    expect(session?.username).toBe("user");
  });

  test("resolves a bearer session from the authorization header", () => {
    const token = "app-token";
    const request = new Request("http://example.com/rpc", {
      headers: { Authorization: `Bearer ${token}` },
    });

    const session = resolveBearerSessionFromRequest(request, (tokenHash) =>
      tokenHash === hashSessionToken(token) ? makeSession(token, { kind: "app" }) : null
    );

    expect(session?.kind).toBe("app");
    expect(session?.username).toBe("user");
  });

  test("prefers bearer auth over the browser cookie when both are present", () => {
    const cookieToken = "browser-token";
    const bearerToken = "bearer-token";
    const request = new Request("http://example.com/rpc", {
      headers: {
        Cookie: `${SESSION_COOKIE_NAME}=${cookieToken}`,
        Authorization: `Bearer ${bearerToken}`,
      },
    });

    const session = resolveSessionFromRequest(request, (tokenHash) => {
      if (tokenHash === hashSessionToken(bearerToken)) {
        return makeSession(bearerToken, { id: 2, kind: "app" });
      }
      if (tokenHash === hashSessionToken(cookieToken)) {
        return makeSession(cookieToken, { id: 3, kind: "browser" });
      }
      return null;
    });

    expect(session?.id).toBe(2);
    expect(session?.kind).toBe("app");
  });

  test("ignores expired sessions", () => {
    const token = "expired-token";
    const request = new Request("http://example.com/library", {
      headers: { Cookie: `${SESSION_COOKIE_NAME}=${token}` },
    });

    const session = resolveBrowserSessionFromRequest(request, (tokenHash) =>
      tokenHash === hashSessionToken(token)
        ? makeSession(token, { expires_at: new Date(Date.now() - 60_000).toISOString() })
        : null
    );

    expect(session).toBeNull();
  });

  test("marks session cookies secure for forwarded https requests", () => {
    const request = new Request("http://podible.internal/login", {
      headers: { "X-Forwarded-Proto": "https" },
    });

    const cookie = buildSessionCookie("token", request);
    expect(cookie).toContain("Secure");

    const cleared = clearSessionCookie(request);
    expect(cleared).toContain("Secure");
  });
});
