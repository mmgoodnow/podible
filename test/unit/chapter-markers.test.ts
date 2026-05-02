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

function epubEntryWithText(title: string, text: string, index: number): EpubChapterEntry {
  return {
    ...epubEntry(title, index),
    text,
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

  test("keeps named non-fiction EPUB headings", () => {
    const headings = selectMajorEpubHeadings(
      ["Title Page", "Copyright Page", "Author’s Note", "Prologue", "Part One: Of Blacke Cholor", "A Private Plague", "Acknowledgments"].map(epubEntry)
    );
    expect(headings.map((heading) => heading.title)).toEqual([
      "Author’s Note",
      "Prologue",
      "Part One: Of Blacke Cholor",
      "A Private Plague",
      "Acknowledgments",
    ]);
  });

  test("keeps spoken front matter, month cards, and useful back matter", () => {
    const headings = selectMajorEpubHeadings(
      ["Copyright", "Preface", "1. PARTY", "OCTOBER", "NOVEMBER", "4. WAKING UP", "EPILOGUE—TREATY", "ACKNOWLEDGMENTS", "DISCOVER MORE"].map(
        epubEntry
      )
    );

    expect(headings.map((heading) => heading.title)).toEqual([
      "Preface",
      "1. PARTY",
      "OCTOBER",
      "NOVEMBER",
      "4. WAKING UP",
      "EPILOGUE—TREATY",
      "ACKNOWLEDGMENTS",
      "DISCOVER MORE",
    ]);
  });

  test("derives useful titles from numeric nonfiction EPUB headings", () => {
    const headings = selectMajorEpubHeadings([
      epubEntryWithText("An Explanatory Note", "AN EXPLANATORY NOTE In the summer of 2003.", 0),
      epubEntryWithText("Preface to the Revised and Expanded Edition", "PREFACE TO THE REVISED AND EXPANDED EDITION.", 1),
      epubEntryWithText("Introduction", "INTRODUCTION: The Hidden Side of Everything Anyone living in the United States.", 2),
      epubEntryWithText("1", "1 What Do Schoolteachers and Sumo Wrestlers Have in Common? Imagine for a moment.", 3),
      epubEntryWithText("2", "2 How Is the Ku Klux Klan Like a Group of Real-Estate Agents? As institutions go.", 4),
      epubEntryWithText("6", "Perfect Parenting, Part II; or: Would a Roshanda by Any Other Name Smell as Sweet? Obsessive or not.", 5),
      epubEntryWithText("Epilogue", "EPILOGUE: Two Paths to Harvard And now.", 6),
    ]);

    expect(headings.map((heading) => heading.title)).toEqual([
      "An Explanatory Note",
      "Introduction: The Hidden Side of Everything",
      "1. What Do Schoolteachers and Sumo Wrestlers Have in Common?",
      "2. How Is the Ku Klux Klan Like a Group of Real-Estate Agents?",
      "6. Perfect Parenting, Part II; or: Would a Roshanda by Any Other Name Smell as Sweet?",
      "Epilogue",
    ]);
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

  test("parses compound spoken chapter ordinals after twenty", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter Twenty-One", "Chapter Twenty-Two"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Chapter 21 December."),
        utterance(120_000, "Chapter 22."),
      ],
      embeddedChapters: [
        { startMs: 60_000, endMs: 120_000, title: "Chapter 21" },
        { startMs: 120_000, endMs: 180_000, title: "Chapter 22" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [60, "Chapter Twenty-One"],
      [120, "Chapter Twenty-Two"],
    ]);
  });

  test("fills skipped generic chapter headings from a learned embedded sequence", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter One", "Chapter Two", "Chapter Three", "Chapter Four"].map(epubEntry),
      transcriptUtterances: [
        utterance(100_000, "Chapter One."),
        utterance(200_000, "Chapter Two."),
        utterance(400_000, "Chapter Four."),
      ],
      embeddedChapters: [
        { startMs: 0, endMs: 10_000, title: "Front matter" },
        { startMs: 100_000, endMs: 200_000, title: "Chapter 2" },
        { startMs: 200_000, endMs: 300_000, title: "Chapter 3" },
        { startMs: 300_000, endMs: 400_000, title: "Chapter 4" },
        { startMs: 400_000, endMs: 500_000, title: "Chapter 5" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [100, "Chapter One"],
      [200, "Chapter Two"],
      [300, "Chapter Three"],
      [400, "Chapter Four"],
    ]);
  });

  test("does not treat intermediate audiobook part endings as closing credits", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Prologue"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Prologue."),
        utterance(120_000, "You have reached the end of a part but not the end of a complete audiobook."),
        utterance(125_000, "Audible hopes you have enjoyed this program."),
        utterance(180_000, "This concludes the book."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Prologue"],
      [180, "Closing credits"],
    ]);
  });

  test("uses audible hopes as final closing credits without a part-ending warning", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Prologue"].map(epubEntry),
      transcriptUtterances: [utterance(60_000, "Prologue."), utterance(180_000, "Audible hopes you have enjoyed this program.")],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Prologue"],
      [180, "Closing credits"],
    ]);
  });

  test("uses publisher closing credits when they precede the audible closing line", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter One"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Chapter One."),
        utterance(180_000, "We hope you've enjoyed this program from Harper Audio. Audible hopes you've enjoyed this program."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Chapter One"],
      [180, "Closing credits"],
    ]);
  });

  test("prefers explicit chapter ordinal when a title appears earlier in prose", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["An Explanatory Note", "1. What Do Schoolteachers and Sumo Wrestlers Have in Common?"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Opening essay text."),
        utterance(120_000, "A simple unasked question, such as what do school teachers and sumo wrestlers have in common?"),
        utterance(180_000, "And so, chapter one, what do school teachers and sumo wrestlers have in common?"),
      ],
      embeddedChapters: [{ startMs: 0, endMs: 240_000, title: "001" }],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "An Explanatory Note"],
      [180, "1. What Do Schoolteachers and Sumo Wrestlers Have in Common?"],
    ]);
  });

  test("matches numbered part headings despite subtitle transcription variants", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Part One: Of Blacke Cholor, without Boyling"].map(epubEntry),
      transcriptUtterances: [utterance(10_000, "Publisher preface."), utterance(60_000, "Part 1. Of Black Collar, Without Boiling.")],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Part One: Of Blacke Cholor, without Boyling"],
    ]);
  });

  test("does not match unnumbered named headings from scattered prose words", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["A Radical Idea"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "The radical surgery seemed like an idea from another era."),
        utterance(120_000, "A radical idea."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "A Radical Idea"],
    ]);
  });

  test("matches adjacent unnumbered named headings closer than fifteen seconds", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["The Goodness of Show Business", "The House That Jimmy Built"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "The goodness of show business."),
        utterance(68_000, "For the boy next door. The house that Jimmy built."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "The Goodness of Show Business"],
      [68, "The House That Jimmy Built"],
    ]);
  });

  test("matches compact and slightly fuzzy unnumbered heading phrases", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        "“A moon shot for cancer”",
        "“The hunting of the sarc”",
        "Atossa’s War",
      ].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "A moonshot for cancer."),
        utterance(120_000, "The hunting of the snark."),
        utterance(180_000, "Atos's War."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "“A moon shot for cancer”"],
      [120, "“The hunting of the sarc”"],
      [180, "Atossa’s War"],
    ]);
  });

  test("does not treat number-word titles as ordinal-only headings", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Thirteen Mountains"].map(epubEntry),
      transcriptUtterances: [utterance(60_000, "Thirteen Mountains.")],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Thirteen Mountains"],
    ]);
  });

  test("uses first story utterance when the first major heading is not spoken", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["I: The Traveler", "II: Red Royal"].map(epubEntry),
      transcriptUtterances: [
        utterance(0, "This is audible."),
        utterance(120_000, "Kell wore a very peculiar coat."),
        utterance(2_945_760, "Two. Red Royal."),
        utterance(21_115_810, "The traveler word appears much later in prose."),
      ],
      embeddedChapters: [
        { startMs: 0, endMs: 1_016_940, title: "001" },
        { startMs: 2_945_760, endMs: 3_522_770, title: "001" },
        { startMs: 21_115_810, endMs: 22_000_000, title: "001" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "I: The Traveler"],
      [2945.76, "II: Red Royal"],
    ]);
  });

  test("does not let numeric embedded chapters jump over a directly spoken next heading", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["23. THE TRUTH", "24. VOTE", "EPILOGUE—TREATY"].map(epubEntry),
      transcriptUtterances: [
        utterance(46_954_138, "23. The Truth."),
        utterance(49_308_713, "Epilogue Treaty."),
        utterance(49_336_320, "Almost everything was back to normal."),
      ],
      embeddedChapters: [
        { startMs: 46_954_138, endMs: 49_336_320, title: "023" },
        { startMs: 49_336_320, endMs: 51_948_797, title: "024" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [46954.138, "23. THE TRUTH"],
      [49308.713, "EPILOGUE—TREATY"],
    ]);
  });
});
