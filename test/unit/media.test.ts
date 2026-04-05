import { describe, expect, test } from "bun:test";

import { selectPreferredAudioAsset } from "../../src/media";
import type { AssetRow } from "../../src/app-types";

function asset(overrides: Partial<AssetRow>): AssetRow {
  return {
    id: 1,
    book_id: 1,
    kind: "single",
    mime: "audio/mpeg",
    total_size: 100,
    duration_ms: 1000,
    source_release_id: null,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

describe("media asset selection", () => {
  test("prefers single m4b-style audio over multi mp3", () => {
    const chosen = selectPreferredAudioAsset([
      asset({ id: 2, kind: "multi", mime: "audio/mpeg", duration_ms: 5000 }),
      asset({ id: 3, kind: "single", mime: "audio/mp4", duration_ms: 4000 }),
    ]);
    expect(chosen?.id).toBe(3);
  });

  test("returns null when only ebook assets exist", () => {
    const chosen = selectPreferredAudioAsset([
      asset({ id: 4, kind: "ebook", mime: "application/epub+zip", duration_ms: null }),
    ]);
    expect(chosen).toBeNull();
  });
});
