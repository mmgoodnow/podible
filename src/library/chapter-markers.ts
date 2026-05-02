import type { EpubChapterEntry } from "./chapter-analysis";

export type TranscriptUtterance = {
  startMs: number;
  endMs: number;
  text: string;
};

export type TranscriptWord = {
  startMs: number;
  endMs: number;
  text: string;
};

export type RawAudioChapter = {
  startMs: number;
  endMs?: number;
  title?: string;
};

export type ProposedChapter = {
  startTime: number;
  title: string;
  confidence: "high" | "medium" | "low";
  reason: string;
};

export type ChapterProposalReport = {
  epubHeadings: string[];
  embeddedChapterCount: number;
  transcriptUtteranceCount: number;
  chapters: ProposedChapter[];
};

type Heading = {
  title: string;
  ordinal: number | null;
  ordinalLabel: "chapter" | "book" | null;
  titleWords: string[];
};

export function wordsToTranscriptUtterances(words: TranscriptWord[]): TranscriptUtterance[] {
  const sorted = [...words]
    .filter((word) => Number.isFinite(word.startMs) && Number.isFinite(word.endMs) && word.text.trim())
    .sort((a, b) => a.startMs - b.startMs);
  const utterances: TranscriptUtterance[] = [];
  let current: TranscriptWord[] = [];

  const flush = () => {
    if (current.length === 0) return;
    utterances.push({
      startMs: current[0]!.startMs,
      endMs: current.at(-1)!.endMs,
      text: current.map((word) => word.text).join(" ").replace(/\s+/g, " ").trim(),
    });
    current = [];
  };

  for (const word of sorted) {
    const previous = current.at(-1);
    if (previous) {
      const gapMs = word.startMs - previous.endMs;
      const durationMs = previous.endMs - current[0]!.startMs;
      if (gapMs >= 900 || durationMs >= 12_000 || /[.!?]$/.test(previous.text)) {
        flush();
      }
    }
    current.push(word);
  }
  flush();
  return utterances;
}

const ROMAN_VALUES = new Map([
  ["i", 1],
  ["v", 5],
  ["x", 10],
  ["l", 50],
  ["c", 100],
  ["d", 500],
  ["m", 1000],
]);

const NUMBER_WORDS = new Map([
  ["one", 1],
  ["two", 2],
  ["three", 3],
  ["four", 4],
  ["five", 5],
  ["six", 6],
  ["seven", 7],
  ["eight", 8],
  ["nine", 9],
  ["ten", 10],
  ["eleven", 11],
  ["twelve", 12],
  ["thirteen", 13],
  ["fourteen", 14],
  ["fifteen", 15],
  ["sixteen", 16],
  ["seventeen", 17],
  ["eighteen", 18],
  ["nineteen", 19],
  ["twenty", 20],
]);

const FRONT_MATTER = new Set([
  "contents",
  "title page",
  "copyright",
  "dedication",
  "epigraph",
  "also by",
  "also by ve schwab",
]);

const BACK_MATTER = new Set([
  "acknowledgments",
  "acknowledgements",
  "about the author",
  "also available",
]);

function normalizeText(value: string): string {
  return value
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseRoman(value: string): number | null {
  const roman = value.toLowerCase();
  if (!/^[ivxlcdm]+$/.test(roman)) return null;
  let total = 0;
  let prev = 0;
  for (let i = roman.length - 1; i >= 0; i -= 1) {
    const current = ROMAN_VALUES.get(roman[i]!) ?? 0;
    if (current < prev) total -= current;
    else total += current;
    prev = current;
  }
  return total > 0 ? total : null;
}

function parseOrdinalToken(value: string): number | null {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (/^\d+$/.test(normalized)) return Number(normalized);
  const numberWord = NUMBER_WORDS.get(normalized);
  if (numberWord) return numberWord;
  return parseRoman(normalized);
}

function parseSpokenOrdinalToken(value: string): number | null {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (/^\d+$/.test(normalized)) return Number(normalized);
  return NUMBER_WORDS.get(normalized) ?? null;
}

function headingFromTitle(title: string): Heading {
  const normalized = normalizeText(title);
  const match = /^(?:(chapter|book)\s+)?([ivxlcdm]+|\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)(?:\s+(.*))?$/.exec(
    normalized
  );
  const ordinal = match ? parseOrdinalToken(match[2]!) : null;
  const titleOnly = match ? (match[3] ?? "").trim() : normalized;
  return {
    title,
    ordinal,
    ordinalLabel: match?.[1] === "chapter" || match?.[1] === "book" ? match[1] : null,
    titleWords: titleOnly.split(" ").filter(Boolean),
  };
}

function isGenericTocTitle(title: string): boolean {
  const normalized = normalizeText(title);
  if (!normalized) return true;
  if (FRONT_MATTER.has(normalized) || BACK_MATTER.has(normalized)) return true;
  if (/^(chapter\s*)?\d+$/.test(normalized)) return true;
  if (/^[ivxlcdm]+$/.test(normalized)) return true;
  return false;
}

export function selectMajorEpubHeadings(entries: EpubChapterEntry[]): Heading[] {
  const titles: string[] = [];
  const seen = new Set<string>();
  for (const entry of entries) {
    const title = entry.title.trim();
    const normalized = normalizeText(title);
    if (BACK_MATTER.has(normalized)) break;
    if (!title || seen.has(title)) continue;
    seen.add(title);
    titles.push(title);
  }
  return titles
    .filter((title) => !isGenericTocTitle(title))
    .filter((title) => {
      const normalized = normalizeText(title);
      if (BACK_MATTER.has(normalized)) return false;
      if (/^(prologue|epilogue)$/.test(normalized)) return true;
      const heading = headingFromTitle(title);
      if (!heading.ordinal) return false;
      if (/^book\s+/.test(normalized)) return true;
      if (/^chapter\s+/.test(normalized)) return true;
      return /^([ivxlcdm]+|\d+)\s+/.test(normalized) && heading.titleWords.length > 0;
    })
    .map(headingFromTitle);
}

function shortUtterancesNear(
  utterances: TranscriptUtterance[],
  startMs: number,
  beforeMs: number,
  afterMs: number
): TranscriptUtterance[] {
  const from = startMs - beforeMs;
  const to = startMs + afterMs;
  return utterances.filter((utterance) => utterance.endMs >= from && utterance.startMs <= to);
}

function windowText(utterances: TranscriptUtterance[], startMs: number): string {
  return shortUtterancesNear(utterances, startMs, 5_000, 14_000)
    .map((utterance) => utterance.text)
    .join(" ");
}

function headingMatchesWindow(heading: Heading, text: string): boolean {
  const normalized = normalizeText(text);
  if (!normalized) return false;
  const titlePhrase = heading.titleWords.join(" ");
  const phraseMatch = titlePhrase.length > 0 && normalized.includes(titlePhrase);
  const wordsMatch = heading.titleWords.length === 0 || phraseMatch || heading.titleWords.every((word) => normalized.includes(word));
  if (!wordsMatch) return false;
  if (!heading.ordinal) return true;
  const tokens = normalized.split(" ");
  const ordinalMatches = tokens.some((token) => parseSpokenOrdinalToken(token) === heading.ordinal);
  if (heading.titleWords.length === 0 && heading.ordinalLabel) {
    return tokens.some((token, index) => token === heading.ordinalLabel && parseSpokenOrdinalToken(tokens[index + 1] ?? "") === heading.ordinal);
  }
  // Many audiobooks speak/show only the chapter title ("LONG NIGHT") while
  // the EPUB labels it as "2. LONG NIGHT"; a strong title phrase is enough.
  return ordinalMatches || phraseMatch || (heading.titleWords.length > 1 && wordsMatch);
}

function findEmbeddedMatch(
  heading: Heading,
  embeddedChapters: RawAudioChapter[],
  utterances: TranscriptUtterance[],
  afterMs: number
): RawAudioChapter | null {
  const candidates = embeddedChapters.filter((chapter) => chapter.startMs > afterMs + 15_000);
  for (const chapter of candidates) {
    if (headingMatchesWindow(heading, windowText(utterances, chapter.startMs))) {
      return chapter;
    }
  }
  return null;
}

function findDirectTranscriptMatch(
  heading: Heading,
  utterances: TranscriptUtterance[],
  afterMs: number
): TranscriptUtterance | null {
  const candidates = utterances.filter((utterance) => utterance.startMs > afterMs + 15_000);
  for (const utterance of candidates) {
    const text = shortUtterancesNear(utterances, utterance.startMs, 1_000, 4_000)
      .map((candidate) => candidate.text)
      .join(" ");
    if (headingMatchesWindow(heading, text)) return utterance;
  }
  return null;
}

function firstStoryUtteranceMs(utterances: TranscriptUtterance[], beforeMs: number): number | null {
  const early = utterances.filter((utterance) => utterance.startMs < beforeMs);
  for (let index = 1; index < early.length; index += 1) {
    const previous = early[index - 1]!;
    const current = early[index]!;
    if (current.startMs - previous.endMs >= 30_000) return current.startMs;
  }
  return early.find((utterance) => normalizeText(utterance.text) !== "this is audible")?.startMs ?? null;
}

function findClosingCreditsMs(utterances: TranscriptUtterance[], afterMs: number): number | null {
  const closing = utterances.find((utterance) => {
    if (utterance.startMs <= afterMs) return false;
    const text = normalizeText(utterance.text);
    return text.startsWith("this concludes ") || text.startsWith("audible hopes you have enjoyed");
  });
  return closing?.startMs ?? null;
}

export function proposeChapterMarkers(input: {
  epubEntries: EpubChapterEntry[];
  transcriptUtterances: TranscriptUtterance[];
  embeddedChapters: RawAudioChapter[];
}): ChapterProposalReport {
  const headings = selectMajorEpubHeadings(input.epubEntries);
  const embedded = [...input.embeddedChapters].sort((a, b) => a.startMs - b.startMs);
  const utterances = [...input.transcriptUtterances].sort((a, b) => a.startMs - b.startMs);
  const chapters: ProposedChapter[] = [];

  let previousMs = -1;
  const firstEmbeddedAfterIntro = embedded.find((chapter) => chapter.startMs > 30_000)?.startMs ?? Number.POSITIVE_INFINITY;
  const firstStoryMs = firstStoryUtteranceMs(utterances, firstEmbeddedAfterIntro);
  if (firstStoryMs !== null && firstStoryMs > 30_000) {
    chapters.push({
      startTime: 0,
      title: "Opening credits",
      confidence: "medium",
      reason: "Audio starts with an intro and first story utterance is delayed.",
    });
  }

  for (const [index, heading] of headings.entries()) {
    let matched = findEmbeddedMatch(heading, embedded, utterances, previousMs);
    let confidence: ProposedChapter["confidence"] = "high";
    let reason = "Matched EPUB major heading to transcript heading near embedded chapter boundary.";

    if (!matched) {
      const direct = findDirectTranscriptMatch(heading, utterances, previousMs);
      if (direct) {
        matched = { startMs: direct.startMs };
        confidence = "medium";
        reason = "Matched EPUB major heading directly in transcript where no embedded chapter boundary matched.";
      }
    }

    if (!matched && index === 0 && firstStoryMs !== null) {
      matched = { startMs: firstStoryMs };
      confidence = "medium";
      reason = "First EPUB heading was not spoken; used first story utterance after opening gap.";
    }

    if (!matched) continue;
    chapters.push({
      startTime: matched.startMs / 1000,
      title: heading.title,
      confidence,
      reason,
    });
    previousMs = matched.startMs;
  }

  const lastStoryStartMs = chapters.at(-1)?.startTime ? chapters.at(-1)!.startTime * 1000 : 0;
  const closingMs = findClosingCreditsMs(utterances, lastStoryStartMs);
  if (closingMs !== null) {
    chapters.push({
      startTime: closingMs / 1000,
      title: "Closing credits",
      confidence: "high",
      reason: "Matched standard audiobook closing-credit phrase in transcript.",
    });
  }

  return {
    epubHeadings: headings.map((heading) => heading.title),
    embeddedChapterCount: embedded.length,
    transcriptUtteranceCount: utterances.length,
    chapters,
  };
}
