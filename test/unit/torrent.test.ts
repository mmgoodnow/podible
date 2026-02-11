import { describe, expect, test } from "bun:test";

import { infoHashFromMagnet, infoHashFromTorrentBytes, normalizeInfoHash } from "../../src/kindling/torrent";

describe("torrent infohash utilities", () => {
  test("normalizes hex and base32 hashes", () => {
    expect(normalizeInfoHash("0123456789abcdef0123456789abcdef01234567")).toBe(
      "0123456789abcdef0123456789abcdef01234567"
    );

    expect(normalizeInfoHash("AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH")).toBe(
      "0123456789abcdef0123456789abcdef01234567"
    );
  });

  test("extracts hash from magnet", () => {
    expect(infoHashFromMagnet("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")).toBe(
      "0123456789abcdef0123456789abcdef01234567"
    );
    expect(infoHashFromMagnet("https://example.com")).toBeNull();
  });

  test("computes info hash from torrent bytes", () => {
    const payload = Buffer.from("d8:announce13:http://t/4:infod4:name4:test12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee", "ascii");
    expect(infoHashFromTorrentBytes(payload)).toBe("800f59e21e8f1129643a478cd402b4180f410435");
  });
});
