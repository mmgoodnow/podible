import { describe, expect, test } from "bun:test";

import {
  proposeChapterMarkers,
  selectMajorEpubHeadings,
  wordsToTranscriptUtterances,
  type RawAudioChapter,
  type TranscriptUtterance,
} from "../../src/library/chapter-markers";
import type { EpubChapterEntry } from "../../src/library/chapter-analysis";

function epubEntry(title: string, index: number): EpubChapterEntry {
  return {
    id: `ch${index}`,
    title,
    href: `ch${index}.xhtml`,
    text: "",
    words: [],
    tokens: [],
    wordCount: 0,
    cumulativeWords: 0,
    cumulativeRatio: 0,
  };
}

function utterance(startMs: number, text: string): TranscriptUtterance {
  return {
    startMs,
    endMs: startMs + 1500,
    text,
  };
}

function embedded(startMs: number): RawAudioChapter {
  return {
    startMs,
    endMs: startMs + 60_000,
    title: "001",
  };
}

describe("chapter marker proposal", () => {
  test("selects major roman-numeral EPUB headings and drops nested generic labels", () => {
    const headings = selectMajorEpubHeadings(
      [
        "Title Page",
        "Copyright",
        "I: The Traveler",
        "I",
        "II",
        "II: Red Royal",
        "1",
        "III: Grey Thief",
        "Acknowledgments",
      ].map(epubEntry)
    );
    expect(headings.map((heading) => heading.title)).toEqual(["I: The Traveler", "II: Red Royal", "III: Grey Thief"]);
  });

  test("proposes usable audiobook chapters from raw EPUB, transcript, and embedded audio chapters", () => {
    const epubEntries = [
      "Title Page",
      "I: The Traveler",
      "I",
      "II: Red Royal",
      "1",
      "III: Grey Thief",
      "X: One White Rook",
    ].map(epubEntry);
    const transcriptUtterances = [
      utterance(0, "This is audible."),
      utterance(120_000, "Kell wore a very peculiar coat."),
      utterance(2_945_760, "Two."),
      utterance(2_947_000, "Red Royal."),
      utterance(2_949_000, "One."),
      utterance(5_325_790, "Three. Grey thief."),
      utterance(25_189_730, "Ten, one white rook."),
      utterance(41_639_500, "This concludes A Darker Shade of Magic."),
    ];
    const embeddedChapters = [
      embedded(0),
      embedded(1_016_940),
      embedded(1_749_660),
      embedded(2_945_760),
      embedded(5_325_790),
      embedded(25_189_730),
    ];

    const report = proposeChapterMarkers({
      epubEntries,
      transcriptUtterances,
      embeddedChapters,
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "I: The Traveler"],
      [2945.76, "II: Red Royal"],
      [5325.79, "III: Grey Thief"],
      [25189.73, "X: One White Rook"],
      [41639.5, "Closing credits"],
    ]);
  });

  test("synthesizes utterances from timestamped words when raw utterances are unavailable", () => {
    const utterances = wordsToTranscriptUtterances([
      { startMs: 0, endMs: 200, text: "Two." },
      { startMs: 1400, endMs: 1700, text: "Red" },
      { startMs: 1750, endMs: 2100, text: "Royal." },
    ]);

    expect(utterances).toEqual([
      { startMs: 0, endMs: 200, text: "Two." },
      { startMs: 1400, endMs: 2100, text: "Red Royal." },
    ]);
  });

  test("trusts a generic embedded chapter number that matches the EPUB ordinal", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["9. TARGET"].map(epubEntry),
      transcriptUtterances: [
        utterance(10_000, "She target mentioned something in ordinary prose."),
        utterance(60_000, "Alice dropped me off in the morning."),
      ],
      embeddedChapters: [{ startMs: 60_000, endMs: 120_000, title: "009" }],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([[60, "9. TARGET"]]);
  });
});
