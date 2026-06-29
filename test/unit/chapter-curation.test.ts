import { describe, expect, test } from "bun:test";

import {
  assessEmbeddedAudioChaptersForCuration,
  getEmbeddedAudioChapters,
  getEpubNodeText,
  getEpubStructure,
  getTranscriptWindowFromContext,
  findFulcrumCandidates,
  findSpokenHeadingBoundaryCandidate,
  fuzzySearchTranscript,
  fulcrumJudgeToolUseBehavior,
  nodeBoundaryToolUseBehavior,
  estimateTimestampFromEpubPosition,
  getTranscriptWindow,
  nodeBoundaryTargets,
  rgSearchTranscript,
  applyAudibleEpubNodeSelection,
  chooseResearchBoundaryCandidate,
  createRootCurationSpan,
  rankTargetBoundaries,
  resolveNodeBoundaryChapters,
  resolveRecursiveChapterSpans,
  researchEpubBoundary,
  searchEpubText,
  submitChapterPlan,
  validateNodeBoundary,
  validateFulcrumSplit,
  type ChapterCurationContext,
  type ChapterCurationSpan,
  type ChapterCurationTargetBoundary,
  type ChapterCurationTiming,
  type NodeBoundaryCurationReport,
  type NodeBoundaryDecision,
  type ResearchEpubBoundaryResult,
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
    added_by_user_id: null,
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

function transcriptFromUtterances(utterances: Array<{ startMs: number; endMs: number; text: string }>): StoredTranscriptPayload {
  const words = utterances.flatMap((utterance) =>
    utterance.text.split(/\s+/).map((text, index) => ({
      text,
      token: text.toLowerCase().replace(/[^a-z0-9]+/g, ""),
      startMs: utterance.startMs + index * 250,
      endMs: utterance.startMs + index * 250 + 200,
    }))
  );
  return {
    version: "test",
    text: utterances.map((utterance) => utterance.text).join(" "),
    words,
    utterances,
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

  test("applyAudibleEpubNodeSelection filters non-audio EPUB front matter", () => {
    const context = ctx({
      epubEntries: [
        epubEntry({ id: "copyright", title: "Copyright Page", wordCount: 100, cumulativeWords: 100, cumulativeRatio: 0.05 }),
        epubEntry({ id: "toc", title: "Contents", wordCount: 200, cumulativeWords: 300, cumulativeRatio: 0.15 }),
        epubEntry({ id: "prologue", title: "Prologue", wordCount: 500, cumulativeWords: 800, cumulativeRatio: 0.4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", wordCount: 1200, cumulativeWords: 2000, cumulativeRatio: 1 }),
      ],
    });

    const filtered = applyAudibleEpubNodeSelection(context, {
      audibleNodeIds: ["prologue", "chapter-1"],
      excludedNodes: [
        { epubNodeId: "copyright", reason: "copyright", notes: "Not represented in the audiobook." },
        { epubNodeId: "toc", reason: "toc", notes: "Navigation table of contents." },
      ],
      audioOnlyIntervals: [],
    });

    expect(filtered.epubEntries.map((entry) => entry.id)).toEqual(["prologue", "chapter-1"]);
    expect(createRootCurationSpan(filtered)).toMatchObject({
      epubStartIndex: 0,
      epubEndIndex: 1,
    });
  });

  test("applyAudibleEpubNodeSelection falls back when classifier excludes everything", () => {
    const context = ctx();

    const filtered = applyAudibleEpubNodeSelection(context, {
      audibleNodeIds: ["missing"],
      excludedNodes: [{ epubNodeId: "front", reason: "front_matter", notes: "Bad classifier output." }],
      audioOnlyIntervals: [{ startTime: 60, endTime: 75, kind: "part_bumper", notes: "Part 2 intro." }],
    });

    expect(filtered.epubEntries).toBe(context.epubEntries);
    expect(filtered.audioOnlyIntervals).toEqual([{ startTime: 60, endTime: 75, kind: "part_bumper", notes: "Part 2 intro." }]);
  });

  test("getTranscriptWindowFromContext annotates overlapping audio-only intervals", () => {
    const context = ctx({
      audioOnlyIntervals: [{ startTime: 58, endTime: 65, kind: "part_bumper", notes: "Part 2 intro." }],
    });

    const result = getTranscriptWindowFromContext(context, 60_000, 5_000);

    expect(result.audioOnlyIntervals).toEqual([{ startTime: 58, endTime: 65, kind: "part_bumper", notes: "Part 2 intro." }]);
  });

  test("getEpubNodeText returns a bounded word window with same-node phrase variants", () => {
    const context = ctx({
      epubEntries: [
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          href: "chapter2.xhtml",
          words: "Chapter Two The mud is dark and cold beneath the laurel roots and the passage narrows into black stone"
            .split(/\s+/)
            .map(word),
          wordCount: 18,
          cumulativeWords: 18,
          cumulativeRatio: 1,
        }),
      ],
    });

    const result = getEpubNodeText(context, { epubNodeId: "chapter-2", startWord: 0, wordCount: 12 });

    expect(result).toMatchObject({
      id: "chapter-2",
      title: "Chapter 2",
      startWord: 0,
      endWord: 12,
      wordCount: 12,
    });
    expect(result?.text).toBe("Chapter Two The mud is dark and cold beneath the laurel roots");
    expect(result?.phraseVariants.map((variant) => variant.text)).toContain("The mud is dark and cold");
    expect(result?.phraseVariants.every((variant) => variant.startWord >= 2)).toBe(true);
  });

  test("searchEpubText reverse-locates transcript phrases relative to a target node", () => {
    const context = ctx({
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          href: "chapter1.xhtml",
          words: "The prior scene will find me and I could use the little help"
            .split(/\s+/)
            .map(word),
          wordCount: 12,
          cumulativeWords: 12,
          cumulativeRatio: 0.5,
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          href: "chapter2.xhtml",
          words: "In order to have an army I must be able to feed it"
            .split(/\s+/)
            .map(word),
          wordCount: 12,
          cumulativeWords: 24,
          cumulativeRatio: 1,
        }),
      ],
    });

    const preRoll = searchEpubText(context, { query: "will find me and I could use", targetNodeId: "chapter-2" });
    const opener = searchEpubText(context, { query: "In order to have an army", targetNodeId: "chapter-2" });

    expect(preRoll.matches[0]).toMatchObject({
      epubNodeId: "chapter-1",
      targetNodeDistance: -1,
      relationToTarget: "pre_target",
    });
    expect(preRoll.matches[0]?.targetWordOffset).toBeLessThan(0);
    expect(opener.matches[0]).toMatchObject({
      epubNodeId: "chapter-2",
      wordOffset: 0,
      targetNodeDistance: 0,
      targetWordOffset: 0,
      relationToTarget: "opener",
    });
  });

  test("searchEpubText normalizes punctuation and classifies interior matches", () => {
    const entryWords = [
      ..."Chapter Two In order to have an army".split(/\s+/),
      ...Array.from({ length: 60 }, (_, index) => `filler${index}`),
      ..."I could use the little bastard's help after the ovens".split(/\s+/),
    ].map(word);
    const context = ctx({
      epubEntries: [
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          href: "chapter2.xhtml",
          words: entryWords,
          wordCount: entryWords.length,
          cumulativeWords: entryWords.length,
          cumulativeRatio: 1,
        }),
      ],
    });

    const result = searchEpubText(context, {
      query: "could use the little bastards help",
      targetNodeId: "chapter-2",
    });

    expect(result.matches[0]).toMatchObject({
      epubNodeId: "chapter-2",
      relationToTarget: "interior",
    });
    expect(Object.keys(result.matches[0] ?? {})).not.toContain(["ordered", "Match", "Ratio"].join(""));
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

  test("assessEmbeddedAudioChaptersForCuration identifies short-circuit candidates", () => {
    const result = assessEmbeddedAudioChaptersForCuration(
      ctx({
        durationMs: 7_200_000,
        epubEntries: [
          epubEntry({ id: "prologue", title: "Prologue", wordCount: 100, cumulativeWords: 100, cumulativeRatio: 0.33 }),
          epubEntry({ id: "crossing", title: "The Crossing", wordCount: 100, cumulativeWords: 200, cumulativeRatio: 0.66 }),
          epubEntry({ id: "epilogue", title: "Epilogue", wordCount: 100, cumulativeWords: 300, cumulativeRatio: 1 }),
        ],
        embeddedChapters: [
          { id: "ch0", title: "Prologue", startMs: 0, endMs: 120_000 },
          { id: "ch1", title: "The Crossing", startMs: 120_000, endMs: 2_600_000 },
          { id: "ch2", title: "Epilogue", startMs: 2_600_000, endMs: 7_200_000 },
        ],
      })
    );

    expect(result).toMatchObject({
      action: "short_circuit_candidate",
      confidence: "high",
      matchedEpubNodeIds: ["prologue", "crossing", "epilogue"],
    });
  });

  test("assessEmbeddedAudioChaptersForCuration treats generic varied markers as priors", () => {
    const result = assessEmbeddedAudioChaptersForCuration(
      ctx({
        durationMs: 7_200_000,
        epubEntries: Array.from({ length: 4 }, (_, index) =>
          epubEntry({
            id: `chapter-${index + 1}`,
            title: `Chapter ${index + 1}`,
            wordCount: 100,
            cumulativeWords: (index + 1) * 100,
            cumulativeRatio: (index + 1) / 4,
          })
        ),
        embeddedChapters: [
          { id: "ch0", title: "Chapter 1", startMs: 0, endMs: 1_000_000 },
          { id: "ch1", title: "Chapter 2", startMs: 1_000_000, endMs: 2_700_000 },
          { id: "ch2", title: "Chapter 3", startMs: 2_700_000, endMs: 5_100_000 },
          { id: "ch3", title: "Chapter 4", startMs: 5_100_000, endMs: 7_200_000 },
        ],
      })
    );

    expect(result.action).toBe("seed_boundaries");
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

  test("rgSearchTranscript can match fixed prose across utterance boundaries", async () => {
    const context = ctx({
      transcript: {
        version: "test",
        text: "I vomit as I wake. A second fist strikes my full stomach.",
        words: [
          ...transcriptWith("I vomit as I wake", 10_000, 12_000).words,
          ...transcriptWith("A second fist strikes my full stomach", 12_500, 15_000).words,
        ],
        utterances: [
          { startMs: 10_000, endMs: 12_000, text: "I vomit as I wake." },
          { startMs: 12_500, endMs: 15_000, text: "A second fist strikes my full stomach." },
        ],
      },
    });

    const result = await rgSearchTranscript(context, {
      pattern: "I vomit as I wake A second fist strikes my full stomach",
      regex: false,
      limit: 1,
    });

    expect(result.matches).toHaveLength(1);
    expect(result.matches[0]).toMatchObject({
      startTime: 10,
      text: "I vomit as I wake A second fist strikes my full stomach",
    });
  });

  test("rgSearchTranscript fallback normalizes apostrophes and punctuation", async () => {
    const context = ctx({
      transcript: {
        version: "test",
        text: "I could use the little bastard's help.",
        words: transcriptWith("I could use the little bastard's help", 10_000, 14_000).words,
        utterances: [{ startMs: 10_000, endMs: 14_000, text: "I could use the little bastard's help." }],
      },
    });

    const result = await rgSearchTranscript(context, {
      pattern: "could use the little bastards help",
      regex: false,
      limit: 1,
    });

    expect(result.matches).toHaveLength(1);
    expect(result.matches[0]).toMatchObject({
      startTime: 10.25,
      text: "could use the little bastard's help",
    });
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

  test("findFulcrumCandidates searches middle EPUB opener prose and ranks by position", () => {
    const entries = Array.from({ length: 5 }, (_, index) =>
      epubEntry({
        id: `chapter-${index + 1}`,
        title: `Chapter ${index + 1}`,
        href: `chapter${index + 1}.xhtml`,
        wordCount: 10,
        cumulativeWords: (index + 1) * 10,
        cumulativeRatio: (index + 1) / 5,
        words:
          index === 2
            ? [word("Helldiver"), word("opened"), word("beneath"), word("Mars"), word("with"), word("furnace"), word("smoke")]
            : [word("Ordinary"), word("chapter"), word(String(index + 1))],
      })
    );
    const context = ctx({
      durationMs: 500_000,
      manifestation: manifestation({ duration_ms: 500_000 }),
      epubEntries: entries,
      transcript: transcriptWith("Helldiver opened beneath Mars with furnace smoke and iron rain", 205_000, 212_000),
    });

    const result = findFulcrumCandidates(context, createRootCurationSpan(context), {
      candidateNodeCount: 3,
      searchRadiusSeconds: 120,
    });

    expect(result.candidates[0]).toMatchObject({
      epubNodeId: "chapter-3",
      title: "Chapter 3",
    });
    expect(result.candidates[0]?.startTime).toBeGreaterThanOrEqual(205);
    expect(result.candidates[0]?.ratioDistance).toBeLessThan(0.05);
    expect(result.candidates[0]?.boundaryScore).toBeGreaterThanOrEqual(0.35);
  });

  test("findFulcrumCandidates rejects windows that continue opener tokens from pre-roll", () => {
    const entries = Array.from({ length: 5 }, (_, index) =>
      epubEntry({
        id: `chapter-${index + 1}`,
        title: `Chapter ${index + 1}`,
        href: `chapter${index + 1}.xhtml`,
        wordCount: 10,
        cumulativeWords: (index + 1) * 10,
        cumulativeRatio: (index + 1) / 5,
        words:
          index === 2
            ? [word("Helldiver"), word("opened"), word("beneath"), word("Mars"), word("with"), word("furnace"), word("smoke")]
            : [word("Ordinary"), word("chapter"), word(String(index + 1))],
      })
    );
    const context = ctx({
      durationMs: 500_000,
      manifestation: manifestation({ duration_ms: 500_000 }),
      epubEntries: entries,
      transcript: {
        version: "test",
        text: "Helldiver opened beneath Mars with furnace smoke. opened beneath Mars with furnace smoke and iron rain",
        words: [
          ...transcriptWith("Helldiver opened beneath Mars with furnace smoke", 198_000, 203_000).words,
          ...transcriptWith("opened beneath Mars with furnace smoke and iron rain", 205_000, 212_000).words,
        ],
        utterances: [],
      },
    });

    const result = findFulcrumCandidates(context, createRootCurationSpan(context), {
      nodeIds: ["chapter-3"],
      searchRadiusSeconds: 120,
    });

    expect(result.candidates[0]?.startTime).toBeLessThan(205);
    expect(result.candidates.every((candidate) => candidate.startTime !== 205)).toBe(true);
  });

  test("researchEpubBoundary returns rare opener phrases with reverse-checked transcript hits", async () => {
    const context = ctx({
      durationMs: 120_000,
      manifestation: manifestation({ duration_ms: 120_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-target",
          title: "Chapter Target",
          href: "target.xhtml",
          words: "The mud is dark and cold beneath the laurel ridge when Dancer opens the sealed door".split(/\s+/).map(word),
          cumulativeRatio: 1,
          cumulativeWords: 16,
        }),
      ],
      transcript: transcriptWith("The mud is dark and cold beneath the laurel ridge when Dancer opens the sealed door", 50_000, 58_000),
    });

    const result = await researchEpubBoundary(context, {
      epubNodeId: "chapter-target",
      expectedTime: 52,
      searchRadiusSeconds: 20,
      phraseLimit: 5,
      hitLimitPerPhrase: 3,
    });

    expect(result?.anchorPhrases.length).toBeGreaterThan(0);
    expect(result?.bestCandidates.length).toBeGreaterThan(0);
    expect(result?.bestCandidates[0]?.reverseEpubRelation).toBe("opener");
  });

  test("chooseResearchBoundaryCandidate only widens near-opener fallback for opening nodes", () => {
    const research: ResearchEpubBoundaryResult = {
      epubNodeId: "prologue",
      epubIndex: 0,
      title: "Prologue",
      expectedStartTime: 0,
      searchScope: { startTime: 0, endTime: 3_600 },
      anchorPhrases: [],
      bestCandidates: [
        {
          phrase: "giant gymnosperms while stratocumulus towered nine kilometers high",
          phraseStartWord: 53,
          phraseWordCount: 8,
          startTime: 60,
          endTime: 68,
          distanceFromExpectedSeconds: 60,
          transcriptText: "The forest of giant gymnosperms while stratocumulus towered nine kilometers high",
          transcriptWindow: "The forest of giant gymnosperms while stratocumulus towered nine kilometers high in a violent sky.",
          reverseEpubRelation: "near_opener",
          boundaryUse: "supporting_context",
        },
      ],
    };
    const span = createRootCurationSpan(ctx({ durationMs: 3_600_000 }));

    expect(chooseResearchBoundaryCandidate(research, span)).toBeNull();

    const openingCandidate = chooseResearchBoundaryCandidate(research, span, { allowOpeningNearOpenerFallback: true });
    expect(openingCandidate).toMatchObject({
      startTime: 60,
      source: "near_opener_fallback",
      phraseStartWord: 53,
      reverseEpubRelation: "near_opener",
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

  test("submitChapterPlan accepts structural plans without transcript token evidence", () => {
    const context = ctx({
      transcript: {
        version: "test",
        text: "Graphic Audio intro. Interior body only.",
        words: [],
        utterances: [
          { startMs: 0, endMs: 2_000, text: "Graphic Audio intro." },
          { startMs: 30_000, endMs: 32_000, text: "Interior body only." },
        ],
      },
    });
    const plan = {
      manifestationId: 10,
      strategy: "recursive merge",
      chapters: [
        { title: "Prologue", startTime: 0, epubNodeId: "front" },
        { title: "Chapter 1", startTime: 30, epubNodeId: "chapter-1" },
      ],
    };

    const result = submitChapterPlan(context, plan);
    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
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

  test("fulcrumJudgeToolUseBehavior terminates on a structured judgment", () => {
    const accepted = fulcrumJudgeToolUseBehavior(undefined, [
      {
        type: "function_output",
        tool: { name: "submitBoundaryJudgment" },
        output: {
          accepted: false,
          confidence: "high",
          finding: "window_starts_before_opener_evidence",
          openerEvidenceAtTimestamp: "offset",
          reason: "Candidate starts in the previous chapter.",
          concerns: ["better candidate is 700s earlier"],
        },
        runItem: {} as never,
      } as never,
    ]);
    expect(accepted.isFinalOutput).toBe(true);

    const malformed = fulcrumJudgeToolUseBehavior(undefined, [
      {
        type: "function_output",
        tool: { name: "submitBoundaryJudgment" },
        output: { accepted: false },
        runItem: {} as never,
      } as never,
    ]);
    expect(malformed.isFinalOutput).toBe(false);
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
    expect(result.audit.boundaryComparison.targetEpub.headText).toContain("Helldiver");
  });

  test("validateFulcrumSplit treats unspoken EPUB chapter titles as optional", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.45,
          cumulativeWords: 8,
          words: "tail tail tail tail".split(/\s+/).map(word),
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2: Antonia",
          cumulativeRatio: 0.75,
          cumulativeWords: 20,
          words: "Antonia I passed this test The interminable war with House Minerva is done".split(/\s+/).map(word),
        }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 1, cumulativeWords: 28 }),
      ],
      transcript: {
        version: "test",
        text: "tail tail tail tail I passed this test The interminable war with House Minerva is done",
        utterances: [
          { startMs: 170_000, endMs: 174_000, text: "tail tail tail tail" },
          { startMs: 180_000, endMs: 186_000, text: "I passed this test The interminable war with House Minerva is done" },
        ],
        words: [
          { text: "tail", token: "tail", startMs: 170_000, endMs: 170_200 },
          { text: "tail", token: "tail", startMs: 170_200, endMs: 170_400 },
          { text: "tail", token: "tail", startMs: 170_400, endMs: 170_600 },
          { text: "tail", token: "tail", startMs: 170_600, endMs: 170_800 },
          { text: "I", token: "i", startMs: 180_000, endMs: 180_100 },
          { text: "passed", token: "passed", startMs: 180_100, endMs: 180_400 },
          { text: "this", token: "this", startMs: 180_400, endMs: 180_600 },
          { text: "test", token: "test", startMs: 180_600, endMs: 180_900 },
          { text: "The", token: "the", startMs: 180_900, endMs: 181_100 },
          { text: "interminable", token: "interminable", startMs: 181_100, endMs: 181_600 },
          { text: "war", token: "war", startMs: 181_600, endMs: 181_800 },
          { text: "with", token: "with", startMs: 181_800, endMs: 182_000 },
          { text: "House", token: "house", startMs: 182_000, endMs: 182_300 },
          { text: "Minerva", token: "minerva", startMs: 182_300, endMs: 182_800 },
          { text: "is", token: "is", startMs: 182_800, endMs: 183_000 },
          { text: "done", token: "done", startMs: 183_000, endMs: 183_300 },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2: Antonia",
      startTime: 180,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.targetEpub.optionalHeadingText).toBe("Antonia");
    expect(result.audit.boundaryComparison.targetEpub.bodyHeadText?.startsWith("I passed this test")).toBe(true);
    expect(result.audit.boundaryComparison.transcriptAfter.startsWith("I passed this test")).toBe(true);
  });

  test("validateFulcrumSplit accepts body opener prefix despite later ASR drift", async () => {
    const context = ctx({
      durationMs: 3_000_000,
      manifestation: manifestation({ duration_ms: 3_000_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.25,
          cumulativeWords: 20,
          words: "previous previous previous previous close".split(/\s+/).map(word),
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2: The Township",
          cumulativeRatio: 0.65,
          cumulativeWords: 80,
          words: [
            { text: "The", token: "the", kind: "heading" },
            { text: "Township", token: "township", kind: "heading" },
            ..."My suit can t handle the heat down here The outer layer is nearly melted through Soon the second layer will go Then the scanner blinks silver and I ve got what I came for".split(
              /\s+/
            ).map((text) => ({ text, token: text.toLowerCase(), kind: "body" as const })),
          ],
        }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 1, cumulativeWords: 120 }),
      ],
      transcript: {
        version: "test",
        text: "previous previous previous close My suit can't handle the heat down here The outer layer is nearly melting through Soon the second layer will go Then the scanner blinks silver and I've got what I came for extra ASR words that drift away from the exact EPUB opener",
        utterances: [
          { startMs: 1_170_000, endMs: 1_181_000, text: "previous previous previous close" },
          {
            startMs: 1_182_080,
            endMs: 1_196_000,
            text: "My suit can't handle the heat down here. The outer layer is nearly melting through. Soon, the second layer will go. Then, the scanner blinks silver, and I've got what I came for. Extra ASR words that drift away from the exact EPUB opener.",
          },
        ],
        words: [
          { text: "previous", token: "previous", startMs: 1_170_000, endMs: 1_170_200 },
          { text: "previous", token: "previous", startMs: 1_170_200, endMs: 1_170_400 },
          { text: "previous", token: "previous", startMs: 1_170_400, endMs: 1_170_600 },
          { text: "close", token: "close", startMs: 1_170_600, endMs: 1_170_800 },
          { text: "My", token: "my", startMs: 1_182_080, endMs: 1_182_200 },
          { text: "suit", token: "suit", startMs: 1_182_200, endMs: 1_182_400 },
          { text: "can't", token: "cant", startMs: 1_182_400, endMs: 1_182_700 },
          { text: "handle", token: "handle", startMs: 1_182_700, endMs: 1_183_000 },
          { text: "the", token: "the", startMs: 1_183_000, endMs: 1_183_200 },
          { text: "heat", token: "heat", startMs: 1_183_200, endMs: 1_183_500 },
          { text: "down", token: "down", startMs: 1_183_500, endMs: 1_183_800 },
          { text: "here", token: "here", startMs: 1_183_800, endMs: 1_184_000 },
          { text: "The", token: "the", startMs: 1_184_560, endMs: 1_184_700 },
          { text: "outer", token: "outer", startMs: 1_184_700, endMs: 1_185_000 },
          { text: "layer", token: "layer", startMs: 1_185_000, endMs: 1_185_300 },
          { text: "is", token: "is", startMs: 1_185_300, endMs: 1_185_500 },
          { text: "nearly", token: "nearly", startMs: 1_185_500, endMs: 1_185_800 },
          { text: "melting", token: "melting", startMs: 1_185_800, endMs: 1_186_100 },
          { text: "through", token: "through", startMs: 1_186_100, endMs: 1_186_400 },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2: The Township",
      startTime: 1182.08,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.targetEpub.optionalHeadingText).toBe("The Township");
    expect(result.audit.boundaryComparison.targetEpub.bodyHeadText?.startsWith("My suit")).toBe(true);
  });

  test("validateFulcrumSplit splits boundary audit text with word timings inside an utterance", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.5,
          cumulativeWords: 8,
          words: "hurry up and evolve".split(/\s+/).map(word),
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          cumulativeRatio: 1,
          cumulativeWords: 16,
          words: "I pretend the matches came from one of the Minervans".split(/\s+/).map(word),
        }),
      ],
      transcript: {
        version: "test",
        text: "hurry up and evolve I pretend the matches came from one of the Minervans",
        utterances: [{ startMs: 178_000, endMs: 188_000, text: "hurry up and evolve I pretend the matches came from one of the Minervans" }],
        words: [
          { text: "hurry", token: "hurry", startMs: 178_000, endMs: 178_300 },
          { text: "up", token: "up", startMs: 178_300, endMs: 178_500 },
          { text: "and", token: "and", startMs: 178_500, endMs: 178_700 },
          { text: "evolve", token: "evolve", startMs: 178_700, endMs: 179_000 },
          { text: "I", token: "i", startMs: 180_000, endMs: 180_100 },
          { text: "pretend", token: "pretend", startMs: 180_100, endMs: 180_400 },
          { text: "the", token: "the", startMs: 180_400, endMs: 180_600 },
          { text: "matches", token: "matches", startMs: 180_600, endMs: 181_000 },
          { text: "came", token: "came", startMs: 181_000, endMs: 181_300 },
          { text: "from", token: "from", startMs: 181_300, endMs: 181_600 },
          { text: "one", token: "one", startMs: 181_600, endMs: 181_800 },
          { text: "of", token: "of", startMs: 181_800, endMs: 182_000 },
          { text: "the", token: "the", startMs: 182_000, endMs: 182_200 },
          { text: "Minervans", token: "minervans", startMs: 182_200, endMs: 182_800 },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2",
      startTime: 180,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptPrecision).toBe("word");
    expect(result.audit.boundaryComparison.transcriptBefore).toContain("hurry up and evolve");
    expect(result.audit.boundaryComparison.transcriptAfter.startsWith("I pretend the matches")).toBe(true);
    expect(result.audit.boundaryComparison.transcriptAfter).not.toContain("hurry");
    expect(result.audit.boundaryComparison.boundaryWords?.containing).toEqual([]);
    expect(result.audit.boundaryComparison.boundaryWords?.nearestCleanBoundaryTimes).toContain(180);
  });

  test("validateFulcrumSplit exposes containing words when a proposed split lands inside a word", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.5,
          cumulativeWords: 4,
          words: "hurry up and evolve".split(/\s+/).map(word),
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          cumulativeRatio: 1,
          cumulativeWords: 10,
          words: "For Eo I do not react".split(/\s+/).map(word),
        }),
      ],
      transcript: {
        version: "test",
        text: "hurry up and evolve For Eo I do not react",
        utterances: [{ startMs: 178_000, endMs: 182_500, text: "hurry up and evolve For Eo I do not react" }],
        words: [
          { text: "hurry", token: "hurry", startMs: 178_000, endMs: 178_300 },
          { text: "up", token: "up", startMs: 178_300, endMs: 178_500 },
          { text: "and", token: "and", startMs: 178_500, endMs: 178_700 },
          { text: "evolve", token: "evolve", startMs: 178_700, endMs: 179_000 },
          { text: "For", token: "for", startMs: 180_000, endMs: 180_800 },
          { text: "Eo", token: "eo", startMs: 180_800, endMs: 181_000 },
          { text: "I", token: "i", startMs: 181_000, endMs: 181_100 },
          { text: "do", token: "do", startMs: 181_100, endMs: 181_300 },
          { text: "not", token: "not", startMs: 181_300, endMs: 181_500 },
          { text: "react", token: "react", startMs: 181_500, endMs: 182_000 },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2",
      startTime: 180.4,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptPrecision).toBe("word");
    expect(result.audit.boundaryComparison.transcriptBefore.endsWith("hurry up and evolve")).toBe(true);
    expect(result.audit.boundaryComparison.transcriptAfter.startsWith("Eo I do not react")).toBe(true);
    expect(result.audit.boundaryComparison.transcriptBefore).not.toContain("For");
    expect(result.audit.boundaryComparison.transcriptAfter).not.toContain("For");
    expect(result.audit.boundaryComparison.boundaryWords?.containing).toEqual([{ text: "For", startTime: 180, endTime: 180.8 }]);
    expect(result.audit.boundaryComparison.boundaryWords?.nearestCleanBoundaryTimes).toEqual(expect.arrayContaining([180, 180.8]));
  });

  test("validateFulcrumSplit marks utterance precision when word timings are unavailable", async () => {
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.5,
          cumulativeWords: 8,
          words: "tail tail chapter twelve".split(/\s+/).map(word),
        }),
        epubEntry({
          id: "chapter-2",
          title: "Chapter 2",
          cumulativeRatio: 1,
          cumulativeWords: 16,
          words: "head head head head".split(/\s+/).map(word),
        }),
      ],
      transcript: {
        version: "test",
        text: "tail tail chapter twelve head head head head",
        words: [],
        utterances: [{ startMs: 180_000, endMs: 186_000, text: "tail tail chapter twelve head head head head" }],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2",
      startTime: 180,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptPrecision).toBe("utterance");
    expect(result.audit.boundaryComparison.transcriptPrecisionNote).toContain("true boundary may fall inside");
    expect(result.audit.boundaryComparison.transcriptAfter).toContain("tail tail chapter twelve head head");
  });

  test("validateFulcrumSplit defers title-only evidence to the boundary judge", async () => {
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
    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptAfter).toContain("Chapter 1 Helldiver Gold Mars");
    expect(result.audit.boundaryComparison.targetEpub.bodyHeadText).toBe("Helldiver The first thing you should know");
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

  test("validateFulcrumSplit allows the only assigned boundary near a small span edge", async () => {
    const context = ctx({
      durationMs: 213_120,
      manifestation: manifestation({ duration_ms: 213_120 }),
      epubEntries: [
        epubEntry({
          id: "chapter-0",
          title: "Chapter 0",
          href: "chapter0.xhtml",
          wordCount: 4,
          cumulativeWords: 4,
          cumulativeRatio: 0.4,
          words: [word("Before"), word("the"), word("new"), word("part")],
        }),
        epubEntry({
          id: "part-1",
          title: "Part I: Slave",
          href: "part1.xhtml",
          wordCount: 6,
          cumulativeWords: 10,
          cumulativeRatio: 1,
          words: [word("Part"), word("I"), word("Slave"), word("There"), word("is"), word("flower")],
        }),
      ],
      transcript: transcriptWith("Before the new part Part 1 Slave There is flower", 0, 213_120),
    });

    const result = await validateFulcrumSplit(
      context,
      createRootCurationSpan(context),
      {
        spanPath: "root",
        epubNodeId: "part-1",
        title: "Part I: Slave",
        startTime: 196.08,
      },
      {
        targetBoundary: {
          epubNodeId: "part-1",
          epubIndex: 1,
          title: "Part I: Slave",
          expectedStartTime: 196.08,
          localNodeRatio: 0.5,
        },
      }
    );

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
  });

  test("validateFulcrumSplit accepts non-middle assigned targets", async () => {
    const entries = Array.from({ length: 10 }, (_, index) =>
      epubEntry({
        id: `chapter-${index + 1}`,
        title: `Chapter ${index + 1}`,
        href: `chapter${index + 1}.xhtml`,
        wordCount: 10,
        cumulativeWords: (index + 1) * 10,
        cumulativeRatio: (index + 1) / 10,
        words:
          index === 1
            ? [word("Helldiver"), word("opened"), word("beneath"), word("Mars"), word("with"), word("furnace"), word("smoke")]
            : [word("Ordinary"), word("chapter"), word(String(index + 1))],
      })
    );
    const context = ctx({
      durationMs: 10_000_000,
      manifestation: manifestation({ duration_ms: 10_000_000 }),
      epubEntries: entries,
      transcript: transcriptWith("Helldiver opened beneath Mars with furnace smoke", 1_000_000, 1_006_000),
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-2",
      title: "Chapter 2",
      startTime: 1_000,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
  });

  test("validateFulcrumSplit defers accidental generic-token overlap to the boundary judge", async () => {
    const context = ctx({
      durationMs: 1_000_000,
      manifestation: manifestation({ duration_ms: 1_000_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({
          id: "part-3",
          title: "Part III: Gold",
          cumulativeRatio: 0.6,
          cumulativeWords: 16,
          words: [word("This"), word("is"), word("your"), word("slingblade"), word("scrape"), word("earth"), word("veins"), word("pitvipers")],
        }),
        epubEntry({ id: "chapter-after", title: "Chapter After", cumulativeRatio: 1, cumulativeWords: 24 }),
      ],
      transcript: {
        version: "test",
        text: "Part three gold this is your slingblade scrape earth veins pitvipers. This will kill keep going.",
        words: [],
        utterances: [
          { startMs: 198_000, endMs: 205_000, text: "Part three gold this is your slingblade scrape earth veins pitvipers." },
          { startMs: 500_000, endMs: 505_000, text: "This will kill keep going." },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "part-3",
      title: "Part III: Gold",
      startTime: 500,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptAfter).toContain("This will kill keep going");
    expect(result.audit.boundaryComparison.targetEpub.headText).toContain("slingblade");
  });

  test("validateFulcrumSplit defers pre-boundary context to the boundary judge", async () => {
    const openerWords = [
      "Northwoods",
      "agony",
      "claustrophobia",
      "sick",
      "wounded",
      "pain",
      "dreams",
      "darkness",
      "stomach",
      "scream",
    ];
    const context = ctx({
      durationMs: 600_000,
      manifestation: manifestation({ duration_ms: 600_000 }),
      epubEntries: [
        epubEntry({ id: "chapter-before", title: "Chapter Before", cumulativeRatio: 0.4, cumulativeWords: 20 }),
        epubEntry({
          id: "chapter-target",
          title: "Chapter Target",
          cumulativeRatio: 0.8,
          cumulativeWords: 40,
          words: openerWords.map(word),
        }),
        epubEntry({ id: "chapter-after", title: "Chapter After", cumulativeRatio: 1, cumulativeWords: 50 }),
      ],
      transcript: {
        version: "test",
        text:
          "The mud is dark and cold. This hurts. Now I know pain. Part four reaper. Northwoods agony claustrophobia sick wounded pain dreams darkness stomach scream.",
        words: [
          ...transcriptWith("The mud is dark and cold This hurts Now I know pain", 250_000, 256_000).words,
          ...transcriptWith("Part four reaper Northwoods agony claustrophobia sick wounded pain dreams darkness stomach scream", 300_000, 309_000).words,
        ],
        utterances: [
          { startMs: 250_000, endMs: 256_000, text: "The mud is dark and cold. This hurts. Now I know pain." },
          {
            startMs: 300_000,
            endMs: 309_000,
            text: "Part four reaper. Northwoods agony claustrophobia sick wounded pain dreams darkness stomach scream.",
          },
        ],
      },
    });

    const result = await validateFulcrumSplit(context, createRootCurationSpan(context), {
      spanPath: "root",
      epubNodeId: "chapter-target",
      title: "Chapter Target",
      startTime: 250,
    });

    expect(result.accepted).toBe(true);
    if (!result.accepted) throw new Error(result.errors.join("\n"));
    expect(result.audit.boundaryComparison.transcriptAfter).toContain("The mud is dark and cold");
    expect(result.audit.boundaryComparison.targetEpub.headText).toContain("Northwoods");
  });

  test("validateNodeBoundary accepts the first narrated EPUB node after an audio-only preamble", async () => {
    const context = ctx({
      durationMs: 120_000,
      manifestation: manifestation({ duration_ms: 120_000 }),
      audioOnlyIntervals: [{ startTime: 0, endTime: 20, kind: "credits", notes: "opening credits" }],
      epubEntries: [
        epubEntry({
          id: "chapter-1",
          title: "Chapter 1",
          cumulativeRatio: 0.5,
          cumulativeWords: 8,
          words: "The first narrated words begin after the credits".split(/\s+/).map(word),
        }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 16 }),
      ],
      transcript: transcriptFromUtterances([
        { startMs: 0, endMs: 20_000, text: "Audible hopes you have enjoyed this program." },
        { startMs: 20_000, endMs: 27_000, text: "The first narrated words begin after the credits." },
      ]),
    });

    const result = await validateNodeBoundary(context, {
      spanPath: "root",
      epubNodeId: "chapter-1",
      title: "Chapter 1",
      startTime: 20,
    });

    expect(result.accepted).toBe(true);
    if (result.accepted) {
      expect(result.epubIndex).toBe(0);
      expect(result.audit.boundaryComparison.transcriptAfter).toContain("The first narrated words");
    }
  });

  test("validateNodeBoundary rejects boundaries placed inside audio-only intervals", async () => {
    const context = ctx({
      durationMs: 120_000,
      manifestation: manifestation({ duration_ms: 120_000 }),
      audioOnlyIntervals: [{ startTime: 0, endTime: 20, kind: "credits", notes: "opening credits" }],
      epubEntries: [epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 1, cumulativeWords: 8 })],
    });

    const result = await validateNodeBoundary(context, {
      spanPath: "root",
      epubNodeId: "chapter-1",
      title: "Chapter 1",
      startTime: 10,
    });

    expect(result.accepted).toBe(false);
    if (!result.accepted) expect(result.errors.join(" ")).toContain("audio-only interval");
  });

  test("nodeBoundaryTargets are built from the curated audible EPUB node list", () => {
    const context = ctx({
      durationMs: 100_000,
      manifestation: manifestation({ duration_ms: 100_000 }),
      epubEntries: [
        epubEntry({ id: "copyright", title: "Copyright", cumulativeRatio: 0.05, cumulativeWords: 5 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.5, cumulativeWords: 50 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 1, cumulativeWords: 100 }),
      ],
    });
    const filtered = applyAudibleEpubNodeSelection(context, {
      audibleNodeIds: ["chapter-1", "chapter-2"],
      excludedNodes: [{ epubNodeId: "copyright", reason: "copyright", notes: "not narrated" }],
      audioOnlyIntervals: [],
    });

    expect(nodeBoundaryTargets(filtered).map((target) => target.epubNodeId)).toEqual(["chapter-1", "chapter-2"]);
  });

  test("nodeBoundaryToolUseBehavior terminates on a structured node boundary", () => {
    const result = nodeBoundaryToolUseBehavior(null, [
      {
        type: "function_output",
        tool: { name: "submitNodeBoundary" },
        output: JSON.stringify({
          accepted: true,
          kind: "node_boundary",
          spanPath: "root",
          epubNodeId: "chapter-1",
          epubIndex: 0,
          title: "Chapter 1",
          startTime: 20,
          notes: null,
          audit: {
            epubNodeId: "chapter-1",
            title: "Chapter 1",
            startTime: 20,
            boundaryComparison: {
              transcriptPrecision: "utterance",
              transcriptPrecisionNote: null,
              previousEpub: { epubNodeId: null, title: null, tailText: "" },
              targetEpub: { epubNodeId: "chapter-1", title: "Chapter 1", headText: "" },
              transcriptBefore: "",
              transcriptAfter: "",
            },
            transcriptWindow: "",
            candidates: [],
          },
        }),
      } as never,
    ]);
    expect(result.isFinalOutput).toBe(true);
  });

  test("resolveNodeBoundaryChapters isolates failed node tasks and keeps accepted neighbors", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.33, cumulativeWords: 10 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 0.66, cumulativeWords: 20 }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 1, cumulativeWords: 30 }),
      ],
    });
    const reports: Array<{ outcome: string; epubNodeId: string }> = [];
    const chapters = await resolveNodeBoundaryChapters(
      context,
      async (targetBoundary): Promise<NodeBoundaryDecision | null> => {
        if (targetBoundary.epubNodeId === "chapter-2") return null;
        return {
          accepted: true,
          kind: "node_boundary",
          spanPath: "root",
          epubNodeId: targetBoundary.epubNodeId,
          epubIndex: targetBoundary.epubIndex,
          title: targetBoundary.title,
          startTime: targetBoundary.epubIndex * 100,
          notes: null,
          audit: {
            epubNodeId: targetBoundary.epubNodeId,
            title: targetBoundary.title,
            startTime: targetBoundary.epubIndex * 100,
            boundaryComparison: {
              transcriptPrecision: "utterance",
              transcriptPrecisionNote: null,
              previousEpub: { epubNodeId: null, title: null, tailText: "" },
              targetEpub: { epubNodeId: targetBoundary.epubNodeId, title: targetBoundary.title, headText: "" },
              transcriptBefore: "",
              transcriptAfter: "",
            },
            transcriptWindow: "",
            candidates: [],
          },
        };
      },
      { maxConcurrency: 3, reports: reports as never }
    );

    expect(chapters?.map((chapter) => chapter.epubNodeId)).toEqual(["chapter-1", "chapter-3"]);
    expect(reports.map((report) => `${report.epubNodeId}:${report.outcome}`)).toEqual([
      "chapter-1:accepted",
      "chapter-2:failed",
      "chapter-3:accepted",
    ]);
  });

  test("resolveNodeBoundaryChapters drops bare structural markers that would break monotonic chapter order", async () => {
    const context = ctx({
      durationMs: 60_000_000,
      manifestation: manifestation({ duration_ms: 60_000_000 }),
      epubEntries: [
        epubEntry({ id: "part-6", title: "VI", cumulativeRatio: 0.67, cumulativeWords: 67 }),
        epubEntry({ id: "chapter-14", title: "XIV: The Final Door", cumulativeRatio: 0.68, cumulativeWords: 68 }),
      ],
    });
    const reports: NodeBoundaryCurationReport[] = [];
    const starts: Record<string, number> = {
      "part-6": 39_873.3,
      "chapter-14": 39_150.06,
    };
    const chapters = await resolveNodeBoundaryChapters(
      context,
      async (targetBoundary): Promise<NodeBoundaryDecision> => ({
        accepted: true,
        kind: "node_boundary",
        spanPath: "root",
        epubNodeId: targetBoundary.epubNodeId,
        epubIndex: targetBoundary.epubIndex,
        title: targetBoundary.title,
        startTime: starts[targetBoundary.epubNodeId]!,
        notes: null,
        audit: {
          epubNodeId: targetBoundary.epubNodeId,
          title: targetBoundary.title,
          startTime: starts[targetBoundary.epubNodeId]!,
          boundaryComparison: {
            transcriptPrecision: "utterance",
            transcriptPrecisionNote: null,
            previousEpub: { epubNodeId: null, title: null, tailText: "" },
            targetEpub: { epubNodeId: targetBoundary.epubNodeId, title: targetBoundary.title, headText: "" },
            transcriptBefore: "",
            transcriptAfter: "",
          },
          transcriptWindow: "",
          candidates: [],
        },
      }),
      { maxConcurrency: 1, reports }
    );

    expect(chapters).toEqual([{ title: "XIV: The Final Door", startTime: 39_150.06, epubNodeId: "chapter-14" }]);
    expect(reports.map((report) => `${report.epubNodeId}:${report.outcome}`)).toEqual(["part-6:dropped", "chapter-14:accepted"]);
    expect(reports[0]?.errors?.[0]).toContain("more specific EPUB title XIV: The Final Door");
  });

  test("resolveRecursiveChapterSpans returns a leaf-only plan", async () => {
    const context = ctx({
      epubEntries: [epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 1, cumulativeWords: 4 })],
    });
    const chapters = await resolveRecursiveChapterSpans(context, async () => {
      throw new Error("no agent should be spawned when no internal boundaries remain");
    });
    expect(chapters).toEqual([{ title: "Prologue", startTime: 0, epubNodeId: "front" }]);
  });

  test("resolveRecursiveChapterSpans splits and merges child leaves in order", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.5, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 0.8, cumulativeWords: 24 }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 1, cumulativeWords: 32 }),
      ],
    });
    const decisions: string[] = [];
    const startTimes: Record<string, number> = {
      "chapter-1": 120,
      "chapter-2": 220,
      "chapter-3": 260,
    };
    const chapters = await resolveRecursiveChapterSpans(context, async (span, targetBoundary): Promise<RecursiveSpanDecision> => {
      decisions.push(span.path);
      return {
        kind: "split",
        split: {
          accepted: true,
          kind: "split",
          spanPath: span.path,
          epubNodeId: targetBoundary!.epubNodeId,
          epubIndex: targetBoundary!.epubIndex,
          title: targetBoundary!.title,
          startTime: startTimes[targetBoundary!.epubNodeId]!,
          notes: null,
          audit: {
            epubNodeId: targetBoundary!.epubNodeId,
            title: targetBoundary!.title,
            startTime: startTimes[targetBoundary!.epubNodeId]!,
            boundaryComparison: {
              transcriptPrecision: "utterance",
              transcriptPrecisionNote: null,
              previousEpub: { epubNodeId: null, title: null, tailText: "" },
              targetEpub: { epubNodeId: targetBoundary!.epubNodeId, title: targetBoundary!.title, headText: "" },
              transcriptBefore: "",
              transcriptAfter: "",
            },
            transcriptWindow: "",
            candidates: [],
          },
        },
      };
    });
    expect(decisions).toEqual(["root", "L", "R"]);
    expect(chapters?.map((chapter) => chapter.title)).toEqual(["Prologue", "Chapter 1", "Chapter 2", "Chapter 3"]);
  });

  test("resolveRecursiveChapterSpans chooses target boundaries by node count, not word count", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Front", cumulativeRatio: 0.9, cumulativeWords: 90 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.92, cumulativeWords: 92 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 0.94, cumulativeWords: 94 }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 0.96, cumulativeWords: 96 }),
        epubEntry({ id: "chapter-4", title: "Chapter 4", cumulativeRatio: 1, cumulativeWords: 100 }),
      ],
    });
    const targets: ChapterCurationTargetBoundary[] = [];
    await resolveRecursiveChapterSpans(
      context,
      async (_span, targetBoundary): Promise<RecursiveSpanDecision | null> => {
        targets.push(targetBoundary);
        return null;
      }
    );

    expect(targets.map((target) => target.epubNodeId)).toEqual(["chapter-2"]);
    expect(targets[0]?.localNodeRatio).toBe(0.4);
    expect(targets[0]?.expectedStartTime).toBe(276);
  });

  test("resolveRecursiveChapterSpans runs sibling spans concurrently within the configured limit", async () => {
    const context = ctx({
      durationMs: 300_000,
      manifestation: manifestation({ duration_ms: 300_000 }),
      epubEntries: [
        epubEntry({ id: "front", title: "Prologue", cumulativeRatio: 0.2, cumulativeWords: 4 }),
        epubEntry({ id: "chapter-1", title: "Chapter 1", cumulativeRatio: 0.5, cumulativeWords: 16 }),
        epubEntry({ id: "chapter-2", title: "Chapter 2", cumulativeRatio: 0.8, cumulativeWords: 24 }),
        epubEntry({ id: "chapter-3", title: "Chapter 3", cumulativeRatio: 1, cumulativeWords: 32 }),
      ],
    });
    let active = 0;
    let maxActive = 0;
    const startTimes: Record<string, number> = {
      "chapter-1": 120,
      "chapter-2": 220,
      "chapter-3": 260,
    };
    const chapters = await resolveRecursiveChapterSpans(
      context,
      async (span, targetBoundary): Promise<RecursiveSpanDecision> => {
        active++;
        maxActive = Math.max(maxActive, active);
        try {
          if (span.path !== "root") {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
          return {
            kind: "split",
            split: {
              accepted: true,
              kind: "split",
              spanPath: span.path,
              epubNodeId: targetBoundary!.epubNodeId,
              epubIndex: targetBoundary!.epubIndex,
              title: targetBoundary!.title,
              startTime: startTimes[targetBoundary!.epubNodeId]!,
              notes: null,
              audit: {
                epubNodeId: targetBoundary!.epubNodeId,
                title: targetBoundary!.title,
                startTime: startTimes[targetBoundary!.epubNodeId]!,
                boundaryComparison: {
                  transcriptPrecision: "utterance",
                  transcriptPrecisionNote: null,
                  previousEpub: { epubNodeId: null, title: null, tailText: "" },
                  targetEpub: { epubNodeId: targetBoundary!.epubNodeId, title: targetBoundary!.title, headText: "" },
                  transcriptBefore: "",
                  transcriptAfter: "",
                },
                transcriptWindow: "",
                candidates: [],
              },
            },
          };
        } finally {
          active--;
        }
      },
      { maxConcurrency: 2 }
    );

    expect(chapters?.map((chapter) => chapter.title)).toEqual(["Prologue", "Chapter 1", "Chapter 2", "Chapter 3"]);
    expect(maxActive).toBe(2);
  });

  test("resolveRecursiveChapterSpans returns a partial leaf after a failed decision", async () => {
    const reports: Array<{ outcome: string }> = [];
    const chapters = await resolveRecursiveChapterSpans(ctx(), async () => null, { reports: reports as never });
    expect(chapters?.[0]?.title).toBe("Prologue");
    expect(reports[0]?.outcome).toBe("partial_leaf");
  });

  test("findSpokenHeadingBoundaryCandidate accepts a spoken heading followed by the target opener", async () => {
    const context = ctx({
      durationMs: 120_000,
      manifestation: manifestation({ duration_ms: 120_000 }),
      epubEntries: [
        epubEntry({ id: "previous", title: "Previous", cumulativeRatio: 0.5, cumulativeWords: 6 }),
        epubEntry({
          id: "chapter-target",
          title: "The Golden Son",
          cumulativeRatio: 1,
          cumulativeWords: 16,
          words: "Dawn breaks over the city as ships gather in the bay".split(/\s+/).map(word),
        }),
      ],
      transcript: transcriptFromUtterances([{ startMs: 80_000, endMs: 87_000, text: "The Golden Son. Dawn breaks over the city as ships gather in the bay." }]),
    });
    const span = createRootCurationSpan(context);
    const targetBoundary: ChapterCurationTargetBoundary = {
      epubNodeId: "chapter-target",
      epubIndex: 1,
      title: "The Golden Son",
      expectedStartTime: 80,
      localNodeRatio: 0.5,
    };

    const candidate = await findSpokenHeadingBoundaryCandidate(context, span, targetBoundary);

    expect(candidate).not.toBeNull();
    expect(candidate?.source).toBe("spoken_heading");
    expect(candidate?.startTime).toBe(80);
    expect(candidate?.bodyMatchCount ?? 0).toBeGreaterThanOrEqual(3);
  });

  test("findSpokenHeadingBoundaryCandidate ignores body-text title mentions not followed by the target opener", async () => {
    const context = ctx({
      durationMs: 120_000,
      manifestation: manifestation({ duration_ms: 120_000 }),
      epubEntries: [
        epubEntry({ id: "previous", title: "Previous", cumulativeRatio: 0.5, cumulativeWords: 6 }),
        epubEntry({
          id: "chapter-target",
          title: "The Golden Son",
          cumulativeRatio: 1,
          cumulativeWords: 16,
          words: "Dawn breaks over the city as ships gather in the bay".split(/\s+/).map(word),
        }),
      ],
      transcript: transcriptFromUtterances([
        { startMs: 50_000, endMs: 56_000, text: "Dawn breaks over the city as ships gather in the bay." },
        { startMs: 80_000, endMs: 87_000, text: "He remembered the phrase The Golden Son from an old song, then kept walking." },
      ]),
    });
    const span = createRootCurationSpan(context);
    const targetBoundary: ChapterCurationTargetBoundary = {
      epubNodeId: "chapter-target",
      epubIndex: 1,
      title: "The Golden Son",
      expectedStartTime: 80,
      localNodeRatio: 0.5,
    };

    const candidate = await findSpokenHeadingBoundaryCandidate(context, span, targetBoundary);

    expect(candidate).toBeNull();
  });

  test("rankTargetBoundaries ranks by closeness to span midpoint, not word count", () => {
    const context = ctx({
      durationMs: 500_000,
      manifestation: manifestation({ duration_ms: 500_000 }),
      epubEntries: [
        epubEntry({ id: "ch-0", title: "Chapter 0", cumulativeRatio: 0.1, cumulativeWords: 10 }),
        epubEntry({ id: "ch-1", title: "Chapter 1", cumulativeRatio: 0.2, cumulativeWords: 20 }),
        epubEntry({ id: "ch-2", title: "Chapter 2", cumulativeRatio: 0.5, cumulativeWords: 50 }),
        epubEntry({ id: "ch-3", title: "Chapter 3", cumulativeRatio: 0.8, cumulativeWords: 80 }),
        epubEntry({ id: "ch-4", title: "Chapter 4", cumulativeRatio: 1.0, cumulativeWords: 100 }),
      ],
    });
    const span = createRootCurationSpan(context);
    const ranked = rankTargetBoundaries(context, span);

    // ch-2 is at local node ratio 0.4 (2/5), ch-3 at 0.6 — both equidistant from 0.5 by node count;
    // ch-2 wins on epubIndex tiebreak. ch-1 (0.2) and ch-3 (0.6) are next, then ch-4 (0.8).
    expect(ranked.map((t) => t.epubNodeId)).toEqual(["ch-2", "ch-3", "ch-1", "ch-4"]);
    expect(ranked[0]?.localNodeRatio).toBe(0.4);
  });

  test("rankTargetBoundaries next entry is a valid fallback when primary target fails", () => {
    // Verifies that on max-turns the retry loop can pick rankedTargets[1] as an adjacent
    // boundary that still meaningfully splits the span.
    const context = ctx({
      durationMs: 500_000,
      manifestation: manifestation({ duration_ms: 500_000 }),
      epubEntries: [
        epubEntry({ id: "ch-0", title: "Chapter 0", cumulativeRatio: 0.2, cumulativeWords: 20 }),
        epubEntry({ id: "ch-1", title: "Chapter 1", cumulativeRatio: 0.4, cumulativeWords: 40 }),
        epubEntry({ id: "ch-2", title: "Chapter 2", cumulativeRatio: 0.6, cumulativeWords: 60 }),
        epubEntry({ id: "ch-3", title: "Chapter 3", cumulativeRatio: 0.8, cumulativeWords: 80 }),
        epubEntry({ id: "ch-4", title: "Chapter 4", cumulativeRatio: 1.0, cumulativeWords: 100 }),
      ],
    });
    const span = createRootCurationSpan(context);
    const ranked = rankTargetBoundaries(context, span);

    // Primary target is the midpoint node; fallback is its nearest neighbor.
    const primary = ranked[0]!;
    const fallback = ranked[1]!;
    expect(primary.epubNodeId).toBe("ch-2");
    // Fallback is a real internal boundary — distinct from primary and still splits the span.
    expect(fallback.epubNodeId).not.toBe(primary.epubNodeId);
    expect(fallback.epubIndex).toBeGreaterThan(span.epubStartIndex);
    expect(fallback.epubIndex).toBeLessThanOrEqual(span.epubEndIndex);
    // Fallback expected time is plausible (within the span).
    expect(fallback.expectedStartTime).toBeGreaterThan(span.startTime);
    expect(fallback.expectedStartTime).toBeLessThan(span.endTime);
  });

});
