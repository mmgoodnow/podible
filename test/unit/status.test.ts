import { describe, expect, test } from "bun:test";

import { deriveBookStatus, deriveMediaStatus, MediaStatus } from "../../src/kindling/status";

describe("status derivation", () => {
  test("media status precedence follows plan", () => {
    expect(deriveMediaStatus({ mediaType: "audio", releases: ["snatched"], hasAsset: false })).toBe("snatched");
    expect(deriveMediaStatus({ mediaType: "audio", releases: ["snatched", "downloaded"], hasAsset: false })).toBe(
      "downloaded"
    );
    expect(
      deriveMediaStatus({ mediaType: "audio", releases: ["snatched", "downloading"], hasAsset: false })
    ).toBe("downloading");
    expect(deriveMediaStatus({ mediaType: "ebook", releases: ["failed", "failed"], hasAsset: false })).toBe("error");
    expect(deriveMediaStatus({ mediaType: "ebook", releases: [], hasAsset: false })).toBe("wanted");
    expect(deriveMediaStatus({ mediaType: "audio", releases: ["failed"], hasAsset: true })).toBe("imported");
  });

  test("overall book status emits partial when one media imported", () => {
    expect(deriveBookStatus("imported", "wanted")).toBe("partial");
    expect(deriveBookStatus("wanted", "imported")).toBe("partial");
    expect(deriveBookStatus("imported", "imported")).toBe("imported");
  });

  test("overall status picks highest non-imported state", () => {
    const cases: Array<[MediaStatus, MediaStatus, MediaStatus]> = [
      ["wanted", "snatched", "snatched"],
      ["error", "snatched", "snatched"],
      ["downloaded", "snatched", "downloaded"],
      ["downloaded", "downloading", "downloading"],
    ];

    for (const [a, b, expected] of cases) {
      expect(deriveBookStatus(a, b)).toBe(expected);
    }
  });
});
