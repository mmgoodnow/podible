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

function entryWithWordCount(title: string, index: number, wordCount: number): EpubChapterEntry {
  return {
    ...epubEntryWithText(title, `${title} story text begins here.`, index),
    wordCount,
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

  test("keeps spoken front matter, interstitial cards, and useful back matter", () => {
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

  test("recognizes short interstitial cards without naming their vocabulary", () => {
    const headings = selectMajorEpubHeadings(["1. FIRST", "BLUE DOOR", "RED SKY", "2. SECOND"].map(epubEntry));

    expect(headings.map((heading) => heading.title)).toEqual(["1. FIRST", "BLUE DOOR", "RED SKY", "2. SECOND"]);
  });

  test("drops isolated short point-of-view cards between book sections and chapters", () => {
    const headings = selectMajorEpubHeadings(
      ["BOOK ONE", "bella", "PREFACE", "1. ENGAGED", "BOOK TWO", "jacob", "PREFACE", "2. WAITING"].map(epubEntry)
    );

    expect(headings.map((heading) => heading.title)).toEqual(["BOOK ONE", "1. ENGAGED", "BOOK TWO", "2. WAITING"]);
  });

  test("lets generic-only EPUB chapter labels fall through to embedded audio chapters", () => {
    const headings = selectMajorEpubHeadings(["Chapter 1", "Chapter 2", "Chapter 3", "Acknowledgments", "Other Titles"].map(epubEntry));

    expect(headings.map((heading) => heading.title)).toEqual([]);
  });

  test("keeps generic chapter labels when they own substantial EPUB content", () => {
    const headings = selectMajorEpubHeadings(
      ["Contents", "Chapter 1", "Chapter 2", "Epilogue"].map((title, index) => ({
        ...epubEntry(title, index),
        wordCount: title.startsWith("Chapter") ? 20_000 : 50,
      }))
    );

    expect(headings.map((heading) => heading.title)).toEqual(["Chapter 1", "Chapter 2", "Epilogue"]);
  });

  test("matches substantial generic chapters from bare spoken ordinals before embedded sections", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter 1", "Chapter 2"].map((title, index) => ({
        ...epubEntry(title, index),
        wordCount: 20_000,
      })),
      transcriptUtterances: [
        utterance(60_000, "1"),
        utterance(900_000, "2"),
      ],
      embeddedChapters: [
        { startMs: 0, endMs: 900_000, title: "001" },
        { startMs: 3_400_000, endMs: 6_000_000, title: "002" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Chapter 1"],
      [900, "Chapter 2"],
    ]);
  });

  test("keeps publisher title credits separate from an unspoken prologue", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        {
          ...epubEntryWithText("Prologue", "Once upon a time, a man came from the sky and killed my wife.", 0),
          wordCount: 500,
        },
      ],
      transcriptUtterances: [
        utterance(0, "Recorded Books and One Click Digital present Golden Son by Pierce Brown, narrated by Tim Gerard Reynolds."),
        utterance(24_920, "Once upon a time, a man came from the sky and killed my wife."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [24.92, "Prologue"],
    ]);
  });

  test("keeps spoken inline tale headings from substantial generic chapters", () => {
    const headings = selectMajorEpubHeadings([
      {
        ...epubEntryWithText(
          "Chapter 1",
          "The pilgrims sat quietly. THE PRIEST’S TALE: “THE MAN WHO CRIED GOD” “ S OMETIMES THERE IS a thin line separating orthodox zeal from apostasy. THE POET’S TALE: “HYPERION CANTOS” I N THE BEGINNING was the Word. THE SOLDIER’S TALE: THE WAR LOVERS I T WAS DURING the battle.",
          0
        ),
        wordCount: 20_000,
      },
    ]);

    expect(headings.map((heading) => heading.title)).toEqual([
      "Chapter 1",
      "The Priests Tale: The Man Who Cried God",
      "The Poets Tale: Hyperion Cantos",
      "The Soldiers Tale: The War Lovers",
    ]);
  });

  test("does not promote inline colon titles from non-generic appendix entries", () => {
    const headings = selectMajorEpubHeadings([
      {
        ...epubEntryWithText("Notes", "PART TWO: AN IMPATIENT WAR 105. This note explains a source citation.", 0),
        wordCount: 20_000,
      },
    ]);

    expect(headings.map((heading) => heading.title)).toEqual(["Notes"]);
  });

  test("matches bare number-word generic labels only as standalone sentences", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter Ten", "Chapter Eleven"].map((title, index) => ({
        ...epubEntry(title, index),
        wordCount: 20_000,
      })),
      transcriptUtterances: [
        utterance(60_000, "Chapter ten."),
        utterance(120_000, "Eleven years of keeping this a secret."),
        utterance(180_000, "Eleven."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Chapter Ten"],
      [180, "Chapter Eleven"],
    ]);
  });

  test("does not treat decimal fragments as standalone ordinal sentences", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter Five"].map((title, index) => ({
        ...epubEntry(title, index),
        wordCount: 20_000,
      })),
      transcriptUtterances: [utterance(60_000, "They accelerated to Mach 1.5 with no lights."), utterance(120_000, "Five.")],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "Chapter Five"],
    ]);
  });

  test("matches generic chapter labels when ordinal is spoken with the EPUB opening", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        {
          ...epubEntryWithText("Chapter 3", "T HE BARGE B ENARES entered the river port of Naiad an hour before sunset.", 0),
          wordCount: 20_000,
        },
      ],
      transcriptUtterances: [
        utterance(60_000, "3 unrelated words about another river port."),
        utterance(120_000, "3 The barge Benares entered the river port of Nyad an hour before sunset."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "Chapter 3"],
    ]);
  });

  test("uses embedded boundaries for generic chapters when the EPUB opening is not spoken with an ordinal", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        {
          ...epubEntryWithText("Chapter 6", "C HRONOS K EEP JUTTED from the easternmost rim of the great range.", 0),
          wordCount: 20_000,
        },
      ],
      transcriptUtterances: [
        utterance(1_000, "Earlier narrative content."),
        utterance(25_000, "Earlier narrative content continues."),
        utterance(50_000, "Earlier narrative content continues."),
        utterance(75_000, "Earlier narrative content continues."),
        utterance(96_000, "They watched the stars burn cold and distant in the high night."),
        utterance(104_000, "Kronos Keep jutted from the easternmost rim of the great range."),
      ],
      embeddedChapters: [
        { startMs: 100_000, title: "021" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [100, "Chapter 6"],
    ]);
  });

  test("drops substantial generic wrappers when named ordinal chapters exist", () => {
    const headings = selectMajorEpubHeadings(
      ["Chapter 8", "1. ULTIMATUM", "2. COMPROMISE", "3. CHOICE", "Chapter 9"].map((title, index) => ({
        ...epubEntry(title, index),
        wordCount: 20_000,
      }))
    );

    expect(headings.map((heading) => heading.title)).toEqual(["1. ULTIMATUM", "2. COMPROMISE", "3. CHOICE"]);
  });

  test("drops many normal generic chapters even when they own content", () => {
    const headings = selectMajorEpubHeadings(
      Array.from({ length: 12 }, (_, index) => ({
        ...epubEntry(`Chapter ${index + 1}`, index),
        wordCount: 5_000,
      }))
    );

    expect(headings.map((heading) => heading.title)).toEqual([]);
  });

  test("trusts user-facing embedded audiobook chapter titles", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Author’s Note", "1. Date you? Date him?!", "Epilogue"].map(epubEntry),
      transcriptUtterances: [
        utterance(24_000, "Author's note."),
        utterance(117_000, "Chapter one. Tyler."),
        utterance(512_000, "Chapter two. Stella."),
        utterance(900_000, "Credits."),
      ],
      embeddedChapters: [
        { startMs: 0, endMs: 24_000, title: "Book Intro" },
        { startMs: 24_000, endMs: 50_000, title: "Author’s Note" },
        { startMs: 50_000, endMs: 117_000, title: "Trigger Warnings" },
        { startMs: 117_000, endMs: 512_000, title: "Chapter 1: Tyler" },
        { startMs: 512_000, endMs: 900_000, title: "Chapter 2: Stella" },
        { startMs: 900_000, endMs: 920_000, title: "Credits" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Book Intro"],
      [24, "Author’s Note"],
      [50, "Trigger Warnings"],
      [117, "Chapter 1: Tyler"],
      [512, "Chapter 2: Stella"],
      [900, "Credits"],
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

  test("uses publisher production credits as closing credits", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Chapter One"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Chapter One."),
        utterance(180_000, "We hope you have enjoyed this production of Golden Son by Pierce Brown."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Chapter One"],
      [180, "Closing credits"],
    ]);
  });

  test("uses Audible production credits before the final Audible hopes line", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Epilogue"].map(epubEntry),
      transcriptUtterances: [
        utterance(60_000, "Epilogue."),
        utterance(180_000, "Produced by Jane Doe for Audible Studios."),
        utterance(210_000, "Audible hopes you have enjoyed this program."),
      ],
      embeddedChapters: [],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [60, "Epilogue"],
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

  test("uses a spoken chapter heading inside a coarse embedded part section", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["Part II: Break", "12: Blood for Blood", "13: Mad Dogs"].map(epubEntry),
      transcriptUtterances: [
        utterance(15_702_000, "Part 2."),
        utterance(15_704_000, "Break."),
        utterance(15_720_000, "Chapter 12."),
        utterance(15_722_000, "Blood for Blood."),
        utterance(18_581_000, "Chapter 13."),
        utterance(18_583_000, "Mad Dogs."),
      ],
      embeddedChapters: [
        { startMs: 15_701_000, endMs: 18_581_000, title: "012" },
        { startMs: 18_581_000, endMs: 20_165_000, title: "013" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [15701, "Part II: Break"],
      [15720, "12: Blood for Blood"],
      [18581, "13: Mad Dogs"],
    ]);
  });

  test("does not override an embedded ordinal boundary with short title prose", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["46: Brotherhood", "47: Free", "48: The Magistrate"].map(epubEntry),
      transcriptUtterances: [
        utterance(59_200_000, "Chapter 46. Brotherhood."),
        utterance(60_345_000, "Instead of freeing them."),
        utterance(60_763_000, "Chapter 47. Free."),
        utterance(61_250_000, "Chapter 48. The Magistrate."),
      ],
      embeddedChapters: [
        { startMs: 59_200_000, endMs: 60_763_000, title: "046" },
        { startMs: 60_763_000, endMs: 61_250_000, title: "047" },
        { startMs: 61_250_000, endMs: 62_307_000, title: "048" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [59200, "46: Brotherhood"],
      [60763, "47: Free"],
      [61250, "48: The Magistrate"],
    ]);
  });

  test("does not move an embedded ordinal boundary to later title prose", () => {
    const report = proposeChapterMarkers({
      epubEntries: ["49: Why We Sing", "50: The Deep", "51: Golden Son"].map(epubEntry),
      transcriptUtterances: [
        utterance(62_307_000, "Chapter 49. Why We Sing."),
        utterance(63_947_000, "Mustang is gone."),
        utterance(64_074_000, "I stand alone listening to the call of the deep mines."),
        utterance(65_285_000, "Chapter 51. Golden Son."),
      ],
      embeddedChapters: [
        { startMs: 62_307_000, endMs: 63_947_000, title: "049" },
        { startMs: 63_947_000, endMs: 65_285_000, title: "050" },
        { startMs: 65_285_000, endMs: 68_577_000, title: "051" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [62307, "49: Why We Sing"],
      [63947, "50: The Deep"],
      [65285, "51: Golden Son"],
    ]);
  });

  test("retitles generic embedded chapter sequences from EPUB numbered headings", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        entryWithWordCount("PROLOGUE: The Treasure Room", 0, 400),
        entryWithWordCount("BOOK ONE: THE CLEAR, CLEAN, SHEER THING", 1, 20),
        entryWithWordCount("1 An Abduction", 2, 1000),
        entryWithWordCount("2 Albert’s Daughters", 3, 1200),
        entryWithWordCount("3 Evacuation", 4, 1300),
        entryWithWordCount("4 An Underground Army", 5, 1400),
        entryWithWordCount("5 St Jude’s Walk", 6, 1500),
        entryWithWordCount("Acknowledgements", 7, 500),
      ],
      transcriptUtterances: [],
      embeddedChapters: [
        { startMs: 0, endMs: 60_000, title: "Chapter 1" },
        { startMs: 60_000, endMs: 150_000, title: "Chapter 2" },
        { startMs: 150_000, endMs: 330_000, title: "Chapter 3" },
        { startMs: 330_000, endMs: 420_000, title: "Chapter 4" },
        { startMs: 420_000, endMs: 660_000, title: "Chapter 5" },
        { startMs: 660_000, endMs: 780_000, title: "Chapter 6" },
        { startMs: 780_000, endMs: 960_000, title: "Chapter 7" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "PROLOGUE: The Treasure Room"],
      [60, "1 An Abduction"],
      [150, "2 Albert’s Daughters"],
      [330, "3 Evacuation"],
      [420, "4 An Underground Army"],
      [660, "5 St Jude’s Walk"],
      [780, "Acknowledgements"],
    ]);
  });

  test("does not trust evenly divided generic embedded chapter sequences", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        entryWithWordCount("1. What Do Schoolteachers and Sumo Wrestlers Have in Common?", 0, 1000),
        entryWithWordCount("2. How Is the Ku Klux Klan Like a Group of Real-Estate Agents?", 1, 1000),
        entryWithWordCount("3. Why Do Drug Dealers Still Live with Their Moms?", 2, 1000),
        entryWithWordCount("4. Where Have All the Criminals Gone?", 3, 1000),
        entryWithWordCount("5. What Makes a Perfect Parent?", 4, 1000),
      ],
      transcriptUtterances: [
        utterance(10_000, "Chapter one. What do schoolteachers and sumo wrestlers have in common?"),
        utterance(20_000, "Chapter two. How is the Ku Klux Klan like a group of real-estate agents?"),
        utterance(30_000, "Chapter three. Why do drug dealers still live with their moms?"),
        utterance(40_000, "Chapter four. Where have all the criminals gone?"),
        utterance(50_000, "Chapter five. What makes a perfect parent?"),
      ],
      embeddedChapters: [
        { startMs: 0, endMs: 3_900_000, title: "001" },
        { startMs: 3_900_000, endMs: 7_800_000, title: "002" },
        { startMs: 7_800_000, endMs: 11_700_000, title: "003" },
        { startMs: 11_700_000, endMs: 15_600_000, title: "004" },
        { startMs: 15_600_000, endMs: 19_500_000, title: "005" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [10, "1. What Do Schoolteachers and Sumo Wrestlers Have in Common?"],
      [30, "3. Why Do Drug Dealers Still Live with Their Moms?"],
      [50, "5. What Makes a Perfect Parent?"],
    ]);
  });

  test("allows distinctive long titles to refine coarse embedded boundaries", () => {
    const report = proposeChapterMarkers({
      epubEntries: [
        "An Explanatory Note",
        "5. What Makes a Perfect Parent?",
        "6. Perfect Parenting, Part II; or: Would a Roshanda by Any Other Name Smell as Sweet?",
      ].map(epubEntry),
      transcriptUtterances: [
        utterance(120_000, "An explanatory note."),
        utterance(16_859_000, "Chapter five. What makes a perfect parent?"),
        utterance(19_367_000, "In a later section about parenting."),
        utterance(20_514_000, "Chapter 6. Perfect parenting, part two, or would a Roshanda by any other name smell as sweet?"),
      ],
      embeddedChapters: [
        { startMs: 16_859_000, endMs: 19_367_000, title: "005" },
        { startMs: 19_367_000, endMs: 23_164_000, title: "006" },
      ],
    });

    expect(report.chapters.map((chapter) => [chapter.startTime, chapter.title])).toEqual([
      [0, "Opening credits"],
      [120, "An Explanatory Note"],
      [16859, "5. What Makes a Perfect Parent?"],
      [20514, "6. Perfect Parenting, Part II; or: Would a Roshanda by Any Other Name Smell as Sweet?"],
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
