import { describe, expect, test } from "bun:test";

import { authorizeRequest, hashSessionToken, SESSION_COOKIE_NAME } from "../../src/books/auth";
import { defaultSettings } from "../../src/books/settings";

describe("auth", () => {
  test("allows localhost bypass outside production", () => {
    const settings = defaultSettings({ auth: { mode: "apikey", key: "abc123" } });
    const previousNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "development";

    try {
      const localReq = new Request("http://localhost/health");
      const remoteReq = new Request("http://example.com/health");

      expect(authorizeRequest(localReq, settings)).toBe(true);
      expect(authorizeRequest(remoteReq, settings)).toBe(false);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });

  test("does not allow localhost bypass in production without credentials", () => {
    const settings = defaultSettings({ auth: { mode: "apikey", key: "abc123" } });
    const previousNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";

    try {
      const localReq = new Request("http://localhost/health");
      expect(authorizeRequest(localReq, settings)).toBe(false);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });

  test("accepts query, bearer, and x-api-key tokens", () => {
    const settings = defaultSettings({ auth: { mode: "apikey", key: "abc123" } });

    const queryReq = new Request("http://localhost/health?api_key=abc123");
    const bearerReq = new Request("http://localhost/health", {
      headers: { Authorization: "Bearer abc123" },
    });
    const headerReq = new Request("http://localhost/health", {
      headers: { "X-API-Key": "abc123" },
    });

    expect(authorizeRequest(queryReq, settings)).toBe(true);
    expect(authorizeRequest(bearerReq, settings)).toBe(true);
    expect(authorizeRequest(headerReq, settings)).toBe(true);
  });

  test("allows localhost bypass in local mode", () => {
    const settings = defaultSettings({ auth: { mode: "local", key: "ignored" } });
    const previousNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    const localReq = new Request("http://localhost/health");
    const remoteReq = new Request("http://example.com/health");

    try {
      expect(authorizeRequest(localReq, settings)).toBe(true);
      expect(authorizeRequest(remoteReq, settings)).toBe(false);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });

  test("accepts a valid session cookie", () => {
    const settings = defaultSettings({ auth: { mode: "apikey", key: "abc123" } });
    const previousNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    try {
      const token = "session-token";
      const req = new Request("http://example.com/library", {
        headers: { Cookie: `${SESSION_COOKIE_NAME}=${token}` },
      });
      expect(
        authorizeRequest(req, settings, (tokenHash) =>
          tokenHash === hashSessionToken(token)
            ? ({
                id: 1,
                user_id: 1,
                token_hash: tokenHash,
                expires_at: new Date(Date.now() + 60_000).toISOString(),
                created_at: new Date().toISOString(),
                last_seen_at: new Date().toISOString(),
                provider: "plex",
                provider_user_id: "u1",
                username: "user",
                display_name: "User",
                thumb_url: null,
                is_admin: 1,
              } as const)
            : null
        )
      ).toBe(true);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });
});
