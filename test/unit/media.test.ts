import { describe, expect, test } from "bun:test";

import { isGenericChapterLabel, pickTranscriptLabelForWindow, selectPreferredAudioAsset } from "../../src/library/media";
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

describe("chapter label heuristics", () => {
  test("recognizes generic chapter labels", () => {
    expect(isGenericChapterLabel("Chapter 1")).toBe(true);
    expect(isGenericChapterLabel("Chapter One")).toBe(true);
    expect(isGenericChapterLabel("chapter 42")).toBe(true);
    expect(isGenericChapterLabel("Prologue")).toBe(false);
    expect(isGenericChapterLabel("Chapter 1: The Beginning")).toBe(false);
    expect(isGenericChapterLabel("")).toBe(false);
  });

  test("picks first utterance whose midpoint falls in the window", () => {
    const utterances = [
      { startMs: 0, endMs: 1500, text: "This is Audible." },
      { startMs: 4500, endMs: 7000, text: "Corina Press and Harper Audio present" },
      { startMs: 15_000, endMs: 20_000, text: "The Long Game includes mentions and descriptions of suicide and depression." },
      { startMs: 24_000, endMs: 28_000, text: "This book is for the Shane and Ilya fans." },
      { startMs: 32_000, endMs: 34_000, text: "Chapter 1 July" },
    ];
    expect(pickTranscriptLabelForWindow(utterances, 0, 15_700)).toBe("This is Audible");
    expect(pickTranscriptLabelForWindow(utterances, 15_700, 24_100)).toBe(
      "The Long Game includes mentions and descriptions of suicide…"
    );
    expect(pickTranscriptLabelForWindow(utterances, 24_100, 32_100)).toBe("This book is for the Shane and Ilya fans");
    expect(pickTranscriptLabelForWindow(utterances, 32_100, 840_800)).toBe("Chapter 1 July");
  });

  test("returns null when no utterance falls in the window", () => {
    const utterances = [{ startMs: 0, endMs: 1000, text: "Intro" }];
    expect(pickTranscriptLabelForWindow(utterances, 5000, 10_000)).toBeNull();
  });
});
