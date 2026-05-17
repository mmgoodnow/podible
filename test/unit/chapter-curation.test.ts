import { describe, expect, test } from "bun:test";

import {
  getEmbeddedAudioChapters,
  getEpubStructure,
  fuzzySearchTranscript,
  estimateTimestampFromEpubPosition,
  rgSearchTranscript,
  type ChapterCurationContext,
  type ChapterCurationTiming,
} from "../../src/library/chapter-curation";
import type { AppSettings, AssetFileRow, AssetRow, BookRow, ManifestationRow } from "../../src/app-types";
import type { EpubChapterEntry, StoredTranscriptPayload } from "../../src/library/chapter-analysis";
import { defaultSettings } from "../../src/settings";

function word(text: string) {
  return { text, token: text.toLowerCase() };
}

function book(overrides: Partial<BookRow> = {}): BookRow {
  return {
    id: 1,
    title: "Test Book",
    author: "Test Author",
    cover_path: null,
    duration_ms: null,
    word_count: null,
    added_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    published_at: null,
    description: null,
    description_html: null,
    language: "eng",
    identifiers_json: null,
    ...overrides,
  };
}

function manifestation(overrides: Partial<ManifestationRow> = {}): ManifestationRow {
  return {
    id: 10,
    book_id: 1,
    kind: "audio",
    label: null,
    edition_note: null,
    selection_note: null,
    duration_ms: 120_000,
    total_size: 1000,
    preferred_score: 0,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function asset(overrides: Partial<AssetRow> = {}): AssetRow {
  return {
    id: 20,
    book_id: 1,
    kind: "single",
    mime: "audio/mp4",
    total_size: 1000,
    duration_ms: 120_000,
    source_release_id: null,
    manifestation_id: 10,
    sequence_in_manifestation: 0,
    import_note: null,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function file(overrides: Partial<AssetFileRow> = {}): AssetFileRow {
  return {
    id: 30,
    asset_id: 20,
    path: "/tmp/test.m4b",
    source_path: null,
    size: 1000,
    duration_ms: 120_000,
    start: 0,
    end: 999,
    title: null,
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function epubEntry(overrides: Partial<EpubChapterEntry>): EpubChapterEntry {
  const words = (overrides.words ?? [word("Once"), word("upon"), word("a"), word("time")]) as EpubChapterEntry["words"];
  return {
    id: "chapter-1",
    title: "Chapter 1",
    href: "chapter1.xhtml",
    text: words.map((item) => item.text).join(" "),
    words,
    tokens: words.map((item) => item.token),
    wordCount: words.length,
    cumulativeWords: words.length,
    cumulativeRatio: 1,
    ...overrides,
  };
}

function transcript(): StoredTranscriptPayload {
  return {
    version: "test",
    text: "Chapter one. Once upon a time.",
    words: [],
    utterances: [
      { startMs: 0, endMs: 1200, text: "Chapter one." },
      { startMs: 1200, endMs: 3000, text: "Once upon a time." },
    ],
  };
}

function ctx(overrides: Partial<ChapterCurationContext> = {}): ChapterCurationContext {
  const audio = asset();
  return {
    book: book(),
    manifestation: manifestation(),
    containers: [{ asset: audio, files: [file({ asset_id: audio.id })] }],
    settings: defaultSettings() as AppSettings,
    durationMs: 120_000,
    epubEntries: [
      epubEntry({
        id: "front",
        title: "Prologue",
        href: "front.xhtml",
        wordCount: 4,
        cumulativeWords: 4,
        cumulativeRatio: 0.25,
      }),
      epubEntry({
        id: "chapter-1",
        title: "Chapter 1",
        href: "chapter1.xhtml",
        wordCount: 12,
        cumulativeWords: 16,
        cumulativeRatio: 1,
        words: [word("The"), word("first"), word("real"), word("chapter")],
      }),
    ],
    transcript: transcript(),
    embeddedChapters: [
      { id: "ch0", title: "Chapter 1", startMs: 0, endMs: 60_000 },
      { id: "ch1", title: "Chapter 2", startMs: 60_000, endMs: 120_000 },
    ] satisfies ChapterCurationTiming[],
    ...overrides,
  };
}

describe("chapter curation tools", () => {
  test("getEpubStructure exposes ordered EPUB nodes with rough position data", () => {
    const result = getEpubStructure(ctx());
    expect(result.book.title).toBe("Test Book");
    expect(result.totalWordCount).toBe(16);
    expect(result.nodes).toHaveLength(2);
    expect(result.nodes[0]).toMatchObject({
      id: "front",
      title: "Prologue",
      startRatio: 0,
      endRatio: 0.25,
    });
    expect(result.nodes[1]).toMatchObject({
      id: "chapter-1",
      title: "Chapter 1",
      startRatio: 0.25,
      endRatio: 1,
      firstWords: "The first real chapter",
    });
  });

  test("getEmbeddedAudioChapters flags generic evenly-divided embedded markers", () => {
    const result = getEmbeddedAudioChapters(
      ctx({
        durationMs: 600_000,
        embeddedChapters: Array.from({ length: 6 }, (_, index) => ({
          id: `ch${index}`,
          title: `Chapter ${index + 1}`,
          startMs: index * 100_000,
          endMs: (index + 1) * 100_000,
        })),
      })
    );
    expect(result.chapters).toHaveLength(6);
    expect(result.diagnostics.labelQuality).toBe("generic");
    expect(result.diagnostics.durationPattern).toBe("suspiciously_even");
    expect(result.diagnostics.boundaryDensity).toBe("dense");
  });

  test("getEmbeddedAudioChapters recognizes named uneven boundaries as useful evidence", () => {
    const result = getEmbeddedAudioChapters(
      ctx({
        durationMs: 7_200_000,
        embeddedChapters: [
          { id: "ch0", title: "Prologue", startMs: 0, endMs: 120_000 },
          { id: "ch1", title: "The Crossing", startMs: 120_000, endMs: 2_600_000 },
          { id: "ch2", title: "Epilogue", startMs: 2_600_000, endMs: 7_200_000 },
        ],
      })
    );
    expect(result.diagnostics.labelQuality).toBe("named");
    expect(result.diagnostics.durationPattern).toBe("varied");
    expect(result.diagnostics.boundaryDensity).toBe("plausible");
  });

  test("rgSearchTranscript searches timestamped transcript utterances without shell access", async () => {
    const result = await rgSearchTranscript(ctx(), {
      pattern: "Once upon",
      regex: false,
      afterSeconds: 5,
    });
    expect(result.matches).toHaveLength(1);
    expect(result.matches[0]).toMatchObject({
      index: 1,
      startTime: 1.2,
      endTime: 3,
      text: "Once upon a time.",
    });
    expect(result.matches[0]?.after.text).toContain("Once upon a time");
  });

  test("rgSearchTranscript respects a scoped time window", async () => {
    const result = await rgSearchTranscript(ctx(), {
      pattern: "Chapter",
      regex: false,
      scope: { startTime: 2 },
    });
    expect(result.matches).toHaveLength(0);
  });

  test("fuzzySearchTranscript finds close utterance matches", async () => {
    const result = await fuzzySearchTranscript(ctx(), {
      query: "once upon tim",
      limit: 1,
    });
    expect(result.matches).toHaveLength(1);
    expect(result.matches[0]).toMatchObject({
      index: 1,
      text: "Once upon a time.",
    });
  });

  test("estimateTimestampFromEpubPosition maps EPUB word position onto duration", () => {
    const result = estimateTimestampFromEpubPosition(ctx(), { epubNodeId: "chapter-1" });
    expect(result).toMatchObject({
      epubNodeId: "chapter-1",
      title: "Chapter 1",
      estimatedStartTime: 30,
      estimatedEndTime: 120,
      confidence: "low",
      basis: {
        startRatio: 0.25,
        endRatio: 1,
      },
    });
  });
});
