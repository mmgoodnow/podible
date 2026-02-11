import { describe, expect, test } from "bun:test";

import { authorizeRequest } from "../../src/kindling/auth";
import { defaultSettings } from "../../src/kindling/settings";

describe("auth", () => {
  test("accepts bearer and x-api-key tokens", () => {
    const settings = defaultSettings({ auth: { mode: "apikey", key: "abc123" } });

    const bearerReq = new Request("http://localhost/health", {
      headers: { Authorization: "Bearer abc123" },
    });
    const headerReq = new Request("http://localhost/health", {
      headers: { "X-API-Key": "abc123" },
    });

    expect(authorizeRequest(bearerReq, settings)).toBe(true);
    expect(authorizeRequest(headerReq, settings)).toBe(true);
  });

  test("allows localhost bypass in local mode", () => {
    const settings = defaultSettings({ auth: { mode: "local", key: "ignored" } });
    const localReq = new Request("http://localhost/health");
    const remoteReq = new Request("http://example.com/health");

    expect(authorizeRequest(localReq, settings)).toBe(true);
    expect(authorizeRequest(remoteReq, settings)).toBe(false);
  });
});
