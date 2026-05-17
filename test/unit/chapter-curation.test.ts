import { describe, expect, test } from "bun:test";

import {
  getEmbeddedAudioChapters,
  getEpubStructure,
  findEpubChapterEvidence,
  fuzzySearchTranscript,
  estimateTimestampFromEpubPosition,
  getTranscriptWindow,
  rgSearchTranscript,
  chapterCuratorToolUseBehavior,
  createRootCurationSpan,
  resolveRecursiveChapterSpans,
  submitChapterPlan,
  validateFulcrumSplit,
  validateLeafChapterPlan,
  type ChapterCurationContext,
  type ChapterCurationTiming,
  type RecursiveSpanDecision,
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
      { startMs: 30_000, endMs: 32_000, text: "The first real chapter begins." },
    ],
  };
}

function transcriptWith(text: string, startMs: number, endMs: number): StoredTranscriptPayload {
  return {
    version: "test",
    text,
    words: text.split(/\s+/).map((text, index) => ({ text, token: text.toLowerCase().replace(/[^a-z0-9]+/g, ""), startMs: startMs + index * 250, endMs: startMs + index * 250 + 200 })),
    utterances: [{ startMs, endMs, text }],
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

  test("findEpubChapterEvidence returns transcript anchors for EPUB nodes", async () => {
    const result = await findEpubChapterEvidence(ctx(), {
      nodeIds: ["chapter-1"],
      searchRadiusSeconds: 120,
      limitPerNode: 2,
    });
    expect(result.nodes).toHaveLength(1);
    expect(result.nodes[0]).toMatchObject({
      epubNodeId: "chapter-1",
      title: "Chapter 1",
      query: "first real",
    });
    expect(result.nodes[0]?.matches[0]).toMatchObject({
      startTime: 30,
      quality: "medium",
    });
    expect(result.nodes[0]?.matches[0]?.tokenOverlap).toEqual(["first", "real"]);
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

  test("getTranscriptWindow returns transcript context around a timestamp", () => {
    const result = getTranscriptWindow(ctx(), { startTime: 1, radiusSeconds: 1 });
    expect(result.startMs).toBe(0);
    expect(result.endMs).toBe(2000);
    expect(result.utterances).toHaveLength(2);
    expect(result.text).toContain("Chapter one");
    expect(result.text).toContain("Once upon a time");
  });

  test("submitChapterPlan rejects non-monotonic chapter starts with audit feedback", () => {
    const result = submitChapterPlan(ctx(), {
      manifestationId: 10,
      strategy: "test",
      chapters: [
        { title: "Prologue", startTime: 10, epubNodeId: "front" },
        { title: "Chapter 1", startTime: 5, epubNodeId: "chapter-1" },
      ],
    });
    expect(result.accepted).toBe(false);
    if (result.accepted) throw new Error("expected rejection");
    expect(result.errors.join("\n")).toContain("strictly greater");
    expect(result.audit).toHaveLength(2);
    expect(result.audit[0]?.claimedEpubHeading?.title).toBe("Prologue");
  });

  test("submitChapterPlan accepts a sane plan and returns normalized chapters", () => {
    const result = submitChapterPlan(ctx(), {
      manifestationId: 10,
      strategy: "epub plus transcript",
      notes: "Synthetic fixture",
      chapters: [
        { title: "Prologue", startTime: 0, epubNodeId: "front" },
        { title: "Chapter 1", startTime: 30, epubNodeId: "chapter-1" },
      ],
    });
    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.chapters).toEqual([
      { title: "Prologue", startTime: 0 },
      { title: "Chapter 1", startTime: 30 },
    ]);
    expect(result.audit[1]?.nearestEmbeddedBoundary?.startTime).toBe(0);
  });

  test("submitChapterPlan rejects plans copied from suspicious equal embedded divisions", () => {
    const result = submitChapterPlan(
      ctx({
        durationMs: 600_000,
        embeddedChapters: Array.from({ length: 6 }, (_, index) => ({
          id: `ch${index}`,
          title: `Chapter ${index + 1}`,
          startMs: index * 100_000,
          endMs: (index + 1) * 100_000,
        })),
      }),
      {
        manifestationId: 10,
        strategy: "embedded markers",
        chapters: Array.from({ length: 6 }, (_, index) => ({
          title: `Chapter ${index + 1}`,
          startTime: index * 100,
        })),
      }
    );
    expect(result.accepted).toBe(false);
    if (result.accepted) throw new Error("expected rejection");
    expect(result.errors.join("\n")).toContain("suspicious evenly-divided");
  });

  test("chapterCuratorToolUseBehavior only terminates on accepted submitChapterPlan output", () => {
    const rejected = chapterCuratorToolUseBehavior(undefined, [
      {
        type: "function_output",
        tool: { name: "submitChapterPlan" },
        output: { accepted: false, errors: ["bad"], warnings: [], audit: [], instruction: "retry" },
        runItem: {} as never,
      } as never,
    ]);
    expect(rejected.isFinalOutput).toBe(false);

    const accepted = chapterCuratorToolUseBehavior(undefined, [
      {
        type: "function_output",
        tool: { name: "submitChapterPlan" },
        output: { accepted: true, strategy: "test", notes: null, chapters: [], warnings: [], audit: [] },
        runItem: {} as never,
      } as never,
    ]);
    expect(accepted.isFinalOutput).toBe(true);

    const naturalJson = chapterCuratorToolUseBehavior(undefined, [
      {
        type: "function_output",
        tool: { name: "submitChapterPlan" },
        output: JSON.stringify({ chapters: [{ title: "Chapter 1", startTime: 0 }] }),
        runItem: {} as never,
      } as never,
    ]);
    expect(naturalJson.isFinalOutput).toBe(false);
  });

  test("validateFulcrumSplit accepts a transcript-backed internal boundary", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1: Helldiver",
          cumulativeRatio: 0.6,
          cumulativeWords: 16,
          words: [word("Helldiver"), word("The"), word("first"), word("thing"), word("you"), word("should"), word("know")],
        }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
      transcript: transcriptWith("Chapter 1 Helldiver The first thing you should know about me", 180_000, 186_000),
    });
    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-1",
      title: "Chapter 1: Helldiver",
      startTime: 180,
    });
    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.epubIndex).toBe(1);
    expect(result.audit.proseMatchedTokens.length).toBeGreaterThan(0);
  });

  test("validateFulcrumSplit rejects title-only evidence", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1: Helldiver Gold Mars",
          cumulativeRatio: 0.6,
          cumulativeWords: 16,
          words: [word("Helldiver"), word("The"), word("first"), word("thing"), word("you"), word("should"), word("know")],
        }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
      transcript: transcriptWith("Chapter 1 Helldiver Gold Mars", 180_000, 183_000),
    });
    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-1",
      title: "Chapter 1: Helldiver Gold Mars",
      startTime: 180,
    });
    expect(result.accepted).toBe(false);
    if (result.accepted) throw new Error("expected rejection");
    expect(result.errors.join("\n")).toContain("prose token");
  });

  test("validateFulcrumSplit rejects timestamps too close to span edges", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.6, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
      transcript: transcriptWith("Chapter one Once upon a time", 30_000, 34_000),
    });
    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-1",
      title: "Chapter 1",
      startTime: 30,
    });
    expect(result.accepted).toBe(false);
    if (result.accepted) throw new Error("expected rejection");
    expect(result.errors.join("\n")).toContain("too close");
  });

  test("validateLeafChapterPlan rejects broad non-forced spans so the agent keeps splitting", () => {
    const entries = Array.from({ length: 10 }, (_, index) =>
      epubEntry({
        id: `chapter-${index + 1}`,
        title: `Chapter ${index + 1}`,
        href: `chapter${index + 1}.xhtml`,
        wordCount: 10,
        cumulativeWords: (index + 1) * 10,
        cumulativeRatio: (index + 1) / 10,
        words: [word("Once"), word("upon"), word("time"), word(String(index + 1))],
      })
    );
    const context = ctx({
      durationMs: 3 * 60 * 60_000,
      manifestation: manifestation({ duration_ms: 3 * 60 * 60_000 }),
      epubEntries: entries,
    });

    const result = validateLeafChapterPlan(context, createRootCurationSpan(context), {
      spanPath: "root",
      strategy: "single chapter",
      chapters: [{ title: "Chapter 1", startTime: 0, epubNodeId: "chapter-1" }],
      notes: "",
    });

    expect(result.accepted).toBe(false);
    if (result.accepted) throw new Error("expected broad leaf rejection");
    expect(result.errors.join("\n")).toContain("Span is too broad for a leaf plan");
  });

  test("validateLeafChapterPlan allows broad spans when recursion is forced to produce a leaf", () => {
    const context = ctx({
      durationMs: 3 * 60 * 60_000,
      manifestation: manifestation({ duration_ms: 3 * 60 * 60_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          href: "chapter1.xhtml",
          wordCount: 10,
          cumulativeWords: 10,
          cumulativeRatio: 1,
          words: [word("Once"), word("upon"), word("time")],
        }),
      ],
    });

    const result = validateLeafChapterPlan(
      context,
      createRootCurationSpan(context),
      {
        spanPath: "root",
        strategy: "forced leaf",
        chapters: [{ title: "Chapter 1", startTime: 0, epubNodeId: "chapter-1" }],
        notes: "",
      },
      { forceLeaf: true }
    );

    expect(result.accepted).toBe(true);
  });

  test("resolveRecursiveChapterSpans returns a leaf-only plan", async () => {
    const context = ctx();
    const chapters = await resolveRecursiveChapterSpans(context, async () => ({
      kind: "leaf",
      chapters: [{ title: "Prologue", startTime: 0, epubNodeId: "front" }],
    }));
    expect(chapters).toEqual([{ title: "Prologue", startTime: 0, epubNodeId: "front" }]);
  });

  test("resolveRecursiveChapterSpans splits and merges child leaves in order", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.6, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
    });
    const decisions: string[] = [];
    const chapters = await resolveRecursiveChapterSpans(context, async (span): Promise<RecursiveSpanDecision> => {
      decisions.push(span.path);
      if (span.path === "root") {
        return {
          kind: "split",
          split: {
            accepted: true,
            kind: "split",
            spanPath: "root",
            epubNodeId: "chapter-1",
            epubIndex: 1,
            title: "Chapter 1",
            startTime: 120,
            notes: null,
            audit: {
              epubNodeId: "chapter-1",
              title: "Chapter 1",
              startTime: 120,
              expectedTokens: [],
              proseTokens: [],
              matchedTokens: [],
              proseMatchedTokens: [],
              overlapRatio: 1,
              transcriptWindow: "",
              candidates: [],
            },
          },
        };
      }
      return {
        kind: "leaf",
        chapters:
          span.path === "L"
            ? [{ title: "Prologue", startTime: 0, epubNodeId: "front" }]
            : [
                { title: "Chapter 1", startTime: 120, epubNodeId: "chapter-1" },
                { title: "Chapter 2", startTime: 220, epubNodeId: "chapter-2" },
              ],
      };
    });
    expect(decisions).toEqual(["root", "L", "R"]);
    expect(chapters?.map((chapter) => chapter.title)).toEqual(["Prologue", "Chapter 1", "Chapter 2"]);
  });

  test("resolveRecursiveChapterSpans runs sibling spans concurrently within the configured limit", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.6, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
    });
    let active = 0;
    let maxActive = 0;
    const chapters = await resolveRecursiveChapterSpans(
      context,
      async (span): Promise<RecursiveSpanDecision> => {
        active++;
        maxActive = Math.max(maxActive, active);
        try {
          if (span.path !== "root") {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
          if (span.path === "root") {
            return {
              kind: "split",
              split: {
                accepted: true,
                kind: "split",
                spanPath: "root",
                epubNodeId: "chapter-1",
                epubIndex: 1,
                title: "Chapter 1",
                startTime: 120,
                notes: null,
                audit: {
                  epubNodeId: "chapter-1",
                  title: "Chapter 1",
                  startTime: 120,
                  expectedTokens: [],
                  proseTokens: [],
                  matchedTokens: [],
                  proseMatchedTokens: [],
                  overlapRatio: 1,
                  transcriptWindow: "",
                  candidates: [],
                },
              },
            };
          }
          return {
            kind: "leaf",
            chapters:
              span.path === "L"
                ? [{ title: "Prologue", startTime: 0, epubNodeId: "front" }]
                : [{ title: "Chapter 1", startTime: 120, epubNodeId: "chapter-1" }],
          };
        } finally {
          active--;
        }
      },
      { maxConcurrency: 2 }
    );

    expect(chapters?.map((chapter) => chapter.title)).toEqual(["Prologue", "Chapter 1"]);
    expect(maxActive).toBe(2);
  });

  test("resolveRecursiveChapterSpans does not recurse after a failed decision", async () => {
    const reports: Array<{ outcome: string }> = [];
    const chapters = await resolveRecursiveChapterSpans(ctx(), async () => null, { reports: reports as never });
    expect(chapters).toBeNull();
    expect(reports[0]?.outcome).toBe("failed");
  });

  test("resolveRecursiveChapterSpans respects call limits", async () => {
    const context = ctx({
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.6, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
    });
    const chapters = await resolveRecursiveChapterSpans(
      context,
      async () => ({
        kind: "split",
        split: {
          accepted: true,
          kind: "split",
          spanPath: "root",
          epubNodeId: "chapter-1",
          epubIndex: 1,
          title: "Chapter 1",
          startTime: 60,
          notes: null,
          audit: {
            epubNodeId: "chapter-1",
            title: "Chapter 1",
            startTime: 60,
            expectedTokens: [],
            proseTokens: [],
            matchedTokens: [],
            proseMatchedTokens: [],
            overlapRatio: 1,
            transcriptWindow: "",
            candidates: [],
          },
        },
      }),
      { maxCalls: 1 }
    );
    expect(chapters).toBeNull();
  });
});
