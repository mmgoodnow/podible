import { describe, expect, test } from "bun:test";

import { decodePlexTokenExpiry } from "../../src/plex";

function makeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "EdDSA", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  // Signature is irrelevant — we never verify, just decode the payload.
  return `${header}.${body}.fake-signature`;
}

describe("decodePlexTokenExpiry", () => {
  test("returns unknown for missing token", () => {
    const result = decodePlexTokenExpiry("");
    expect(result.expSeconds).toBeNull();
    expect(result.expired).toBe(false);
    expect(result.expiresInMs).toBeNull();
  });

  test("returns unknown for non-JWT (legacy) token", () => {
    const result = decodePlexTokenExpiry("not-a-jwt-just-a-random-string");
    expect(result.expSeconds).toBeNull();
    expect(result.expired).toBe(false);
  });

  test("decodes valid JWT and reports time remaining", () => {
    const nowMs = 1_775_000_000_000;
    const exp = Math.floor(nowMs / 1000) + 86_400; // 1 day from now
    const result = decodePlexTokenExpiry(makeJwt({ exp }), nowMs);
    expect(result.expSeconds).toBe(exp);
    expect(result.expired).toBe(false);
    expect(result.expiresInMs).toBe(86_400_000);
  });

  test("flags expired tokens", () => {
    const nowMs = 1_775_000_000_000;
    const exp = Math.floor(nowMs / 1000) - 3600; // 1 hour ago
    const result = decodePlexTokenExpiry(makeJwt({ exp }), nowMs);
    expect(result.expired).toBe(true);
    expect((result.expiresInMs ?? 0) < 0).toBe(true);
  });

  test("returns unknown when payload has no exp claim", () => {
    const result = decodePlexTokenExpiry(makeJwt({ user: { id: 123 } }));
    expect(result.expSeconds).toBeNull();
    expect(result.expired).toBe(false);
  });

  test("returns unknown for malformed JWT payload", () => {
    const result = decodePlexTokenExpiry("a.bm90LWpzb24.b");
    expect(result.expSeconds).toBeNull();
    expect(result.expired).toBe(false);
  });

  test("matches the real-world stale token from cyprus (April 11 expiry)", () => {
    // The actual token from podible's settings on cyprus when annie's friend
    // got the 401. Recorded here as a regression fixture.
    const stale =
      "eyJraWQiOiI5d3RJb0tNYXV2R3lYU3pkVUFybVlzRXl4SnVQamJfOFZrWkduX3otQmNvIiwidHlwIjoiSldUIiwiYWxnIjoiRWREU0EifQ" +
      ".eyJub25jZSI6IjNkMTk1OGY0LTUyZTUtNGVhMS1iYTA2LWZmYjUzM2Y0ZmI5YiIsInRodW1icHJpbnQiOiJwb2RpYmxlLXBsZXgtYzJhYzgxNGVjNTM3OGZjNSIsImlzcyI6InBsZXgudHYiLCJhdWQiOlsicGxleC50diIsInBvZGlibGUtNzc3N2U2ZDMxZDY1N2JjYzliOWI0ZjNkIl0sImlhdCI6MTc3NTMzNDk5MSwiZXhwIjoxNzc1OTM5NzkxLCJ1c2VyIjp7ImlkIjoxMDMzMjExNCwidXVpZCI6ImIyNmE3MDBkYjM0YzJjMTYifX0" +
      ".sig";
    // Pretend "now" is April 25 2026 (when we discovered the bug).
    const result = decodePlexTokenExpiry(stale, Date.UTC(2026, 3, 25, 14, 0, 0));
    expect(result.expSeconds).toBe(1_775_939_791);
    expect(result.expired).toBe(true);
  });
});
