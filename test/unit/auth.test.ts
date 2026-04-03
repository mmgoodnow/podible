import { describe, expect, test } from "bun:test";

import { authorizeRequest } from "../../src/books/auth";
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
});
