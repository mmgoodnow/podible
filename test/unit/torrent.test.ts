import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";

import { infoHashFromTorrentBytes, normalizeInfoHash } from "../../src/library/torrent";

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce15:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

describe("torrent infohash utilities", () => {
  test("normalizes 40-char hex hashes", () => {
    expect(normalizeInfoHash("0123456789abcdef0123456789abcdef01234567")).toBe(
      "0123456789abcdef0123456789abcdef01234567"
    );

    expect(() => normalizeInfoHash("AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH")).toThrow(
      "Unsupported info hash format"
    );
  });

  test("derives info hash from torrent bytes", () => {
    const name = "dune-audio";
    const bytes = makeTorrentBytes(name);
    const infoDict = Buffer.from(
      `d4:name${Buffer.byteLength(name)}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890e`,
      "ascii"
    );
    const expected = createHash("sha1").update(infoDict).digest("hex");
    expect(infoHashFromTorrentBytes(bytes)).toBe(expected);
  });
});
