import { describe, expect, test } from "bun:test";

import { normalizeInfoHash } from "../../src/kindling/torrent";

describe("torrent infohash utilities", () => {
  test("normalizes 40-char hex hashes", () => {
    expect(normalizeInfoHash("0123456789abcdef0123456789abcdef01234567")).toBe(
      "0123456789abcdef0123456789abcdef01234567"
    );

    expect(() => normalizeInfoHash("AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH")).toThrow(
      "Unsupported info hash format"
    );
  });
});
