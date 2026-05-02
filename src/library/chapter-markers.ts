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
  ordinalLabel: "chapter" | "book" | "part" | null;
  titleWords: string[];
  sourceIndex: number;
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
  ["thirty", 30],
  ["forty", 40],
  ["fifty", 50],
  ["sixty", 60],
  ["seventy", 70],
  ["eighty", 80],
  ["ninety", 90],
]);

const FRONT_MATTER = new Set([
  "contents",
  "title page",
  "copyright",
  "copyright page",
  "dedication",
  "epigraph",
  "also by",
  "also by ve schwab",
]);

const BACK_MATTER = new Set([
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
  const numberWord = parseNumberWords(normalized.split(" "));
  if (numberWord) return numberWord;
  return parseRoman(normalized);
}

function parseSpokenOrdinalToken(value: string): number | null {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (/^\d+$/.test(normalized)) return Number(normalized);
  return NUMBER_WORDS.get(normalized) ?? null;
}

function parseNumberWords(tokens: string[]): number | null {
  if (tokens.length === 0 || tokens.length > 2) return null;
  const first = NUMBER_WORDS.get(tokens[0]!);
  if (!first) return null;
  if (tokens.length === 1) return first;
  if (first < 20 || first % 10 !== 0) return null;
  const second = NUMBER_WORDS.get(tokens[1]!);
  if (!second || second >= 10) return null;
  return first + second;
}

function parseLeadingOrdinal(tokens: string[]): { ordinal: number; consumed: number } | null {
  for (let length = Math.min(2, tokens.length); length >= 1; length -= 1) {
    const value = parseNumberWords(tokens.slice(0, length));
    if (value !== null) return { ordinal: value, consumed: length };
  }
  const first = tokens[0];
  if (!first) return null;
  if (/^\d+$/.test(first)) return { ordinal: Number(first), consumed: 1 };
  const roman = parseRoman(first);
  if (roman !== null) return { ordinal: roman, consumed: 1 };
  return null;
}

function headingFromTitle(title: string, sourceIndex = -1): Heading {
  const normalized = normalizeText(title);
  const tokens = normalized.split(" ").filter(Boolean);
  let label: Heading["ordinalLabel"] = null;
  if (tokens[0] === "chapter" || tokens[0] === "book" || tokens[0] === "part") {
    label = tokens.shift() as Heading["ordinalLabel"];
  }
  const parsed = label || /^\s*(\d+|[ivxlcdm]+)\s*[:.]/i.test(title) ? parseLeadingOrdinal(tokens) : null;
  const ordinal = parsed?.ordinal ?? null;
  const titleOnly = parsed ? tokens.slice(parsed.consumed).join(" ") : normalized;
  return {
    title,
    ordinal,
    ordinalLabel: label,
    titleWords: titleOnly.split(" ").filter(Boolean),
    sourceIndex,
  };
}

function isGenericTocTitle(title: string): boolean {
  const normalized = normalizeText(title);
  if (!normalized) return true;
  if (FRONT_MATTER.has(normalized) || BACK_MATTER.has(normalized)) return true;
  if (isGenericNumberedChapterTocHeading(title)) return true;
  if (/^[ivxlcdm]+$/.test(normalized)) return true;
  return false;
}

function isGenericNumberedChapterTocHeading(title: string): boolean {
  const normalized = normalizeText(title);
  return /^(chapter\s*)?\d+$/.test(normalized);
}

function deriveHeadingTitle(entry: EpubChapterEntry): string {
  const title = entry.title.trim();
  const normalizedTitle = normalizeText(title);
  if (/^\d+$/.test(normalizedTitle)) {
    const firstText = entry.text.replace(/\s+/g, " ").trim();
    const withoutLeadingOrdinal = firstText.replace(new RegExp(`^${normalizedTitle}\\s+`, "i"), "");
    const match = withoutLeadingOrdinal.match(/^(.+?[?!])(?:\s|$)/);
    if (match?.[1]) return `${normalizedTitle}. ${match[1].trim()}`;
  }
  if (normalizedTitle === "introduction") {
    const firstText = entry.text.replace(/\s+/g, " ").trim();
    const match = firstText.match(/^introduction\s*:\s*(.+?)(?:\s+Anyone\b|$)/i);
    if (match?.[1]) return `Introduction: ${match[1].trim()}`;
  }
  return title;
}

export function selectMajorEpubHeadings(entries: EpubChapterEntry[]): Heading[] {
  const headings: Heading[] = [];
  const seen = new Set<string>();
  for (const [sourceIndex, entry] of entries.entries()) {
    const title = deriveHeadingTitle(entry);
    const normalized = normalizeText(title);
    if (BACK_MATTER.has(normalized)) break;
    if (!title || seen.has(title)) continue;
    seen.add(title);
    headings.push(headingFromTitle(title, sourceIndex));
  }
  const ordinalHeadings = headings.filter((heading) => !isGenericTocTitle(heading.title) && isOrdinalChapterStructureHeading(heading));
  const hasOrdinalChapterStructure =
    ordinalHeadings.length >= 3;
  const hasGenericChapterTocStructure = headings.filter((heading) => isGenericNumberedChapterTocHeading(heading.title)).length >= 3;
  const chapterOrdinalHeadings = headings.filter((heading) => !isGenericTocTitle(heading.title) && isOrdinalChapterHeading(heading));
  const firstChapterOrdinalIndex = chapterOrdinalHeadings[0]?.sourceIndex ?? ordinalHeadings[0]?.sourceIndex ?? Number.POSITIVE_INFINITY;
  const lastChapterOrdinalIndex = chapterOrdinalHeadings.at(-1)?.sourceIndex ?? ordinalHeadings.at(-1)?.sourceIndex ?? Number.NEGATIVE_INFINITY;
  const shortInterstitialCounts = countShortInterstitialSequenceHeadings(headings, firstChapterOrdinalIndex, lastChapterOrdinalIndex);
  const hasUniqueShortInterstitialSequence = [...shortInterstitialCounts.values()].filter((count) => count === 1).length >= 2;

  return headings
    .filter((heading) => !isGenericTocTitle(heading.title))
    .filter((heading) => {
      const normalized = normalizeText(heading.title);
      if (BACK_MATTER.has(normalized)) return false;
      if (/^(prologue|epilogue)$/.test(normalized)) return true;
      if (/^epilogue\s+/.test(normalized)) return true;
      if (normalized.startsWith("introduction ")) return true;
      if (isShortUnnumberedHeading(heading)) {
        const shortHeadingCount = shortInterstitialCounts.get(normalizeText(heading.title)) ?? 0;
        return (
          (!hasOrdinalChapterStructure && !hasGenericChapterTocStructure) ||
          (heading.sourceIndex < firstChapterOrdinalIndex && (isFrontMatterNoteHeading(heading) || hasUniqueShortInterstitialSequence)) ||
          (heading.sourceIndex > firstChapterOrdinalIndex && heading.sourceIndex < lastChapterOrdinalIndex && shortHeadingCount === 1) ||
          (heading.sourceIndex > lastChapterOrdinalIndex && hasUniqueShortInterstitialSequence)
        );
      }
      if (!heading.ordinal) return !hasOrdinalChapterStructure && !hasGenericChapterTocStructure && heading.titleWords.length > 0;
      if (isMainOrdinalHeading(heading)) return true;
      return false;
    });
}

function isOrdinalChapterHeading(heading: Heading): boolean {
  if (!heading.ordinal) return false;
  const normalized = normalizeText(heading.title);
  return heading.ordinalLabel === "chapter" || /^chapter\s+/.test(normalized) || (/^([ivxlcdm]+|\d+)\s+/.test(normalized) && heading.titleWords.length > 0);
}

function isOrdinalChapterStructureHeading(heading: Heading): boolean {
  if (!heading.ordinal) return false;
  const normalized = normalizeText(heading.title);
  return (
    heading.ordinalLabel === "chapter" ||
    heading.ordinalLabel === "book" ||
    /^book\s+/.test(normalized) ||
    /^chapter\s+/.test(normalized) ||
    (/^([ivxlcdm]+|\d+)\s+/.test(normalized) && heading.titleWords.length > 0)
  );
}

function isMainOrdinalHeading(heading: Heading): boolean {
  if (!heading.ordinal) return false;
  const normalized = normalizeText(heading.title);
  return (
    heading.ordinalLabel === "chapter" ||
    heading.ordinalLabel === "book" ||
    /^book\s+/.test(normalized) ||
    /^chapter\s+/.test(normalized) ||
    /^part\s+/.test(normalized) ||
    (/^([ivxlcdm]+|\d+)\s+/.test(normalized) && heading.titleWords.length > 0)
  );
}

function isShortUnnumberedHeading(heading: Heading): boolean {
  return !heading.ordinal && heading.titleWords.length > 0 && heading.titleWords.length <= 3;
}

function isFrontMatterNoteHeading(heading: Heading): boolean {
  const normalized = normalizeText(heading.title);
  return normalized.includes("note") || normalized.startsWith("introduction");
}

function countShortInterstitialSequenceHeadings(headings: Heading[], firstMainOrdinalIndex: number, lastMainOrdinalIndex: number): Map<string, number> {
  const counts = new Map<string, number>();
  let run: Heading[] = [];
  let previousBoundary: Heading | null = null;
  const flush = (nextBoundary: Heading | null) => {
    if (run.length >= 2 && previousBoundary && nextBoundary && isOrdinalChapterHeading(previousBoundary) && isOrdinalChapterHeading(nextBoundary)) {
      for (const heading of run) {
        const normalized = normalizeText(heading.title);
        counts.set(normalized, (counts.get(normalized) ?? 0) + 1);
      }
    }
    run = [];
  };
  for (const heading of headings) {
    if (
      !isGenericTocTitle(heading.title) &&
      isShortUnnumberedHeading(heading) &&
      heading.sourceIndex > firstMainOrdinalIndex &&
      heading.sourceIndex < lastMainOrdinalIndex
    ) {
      run.push(heading);
    } else {
      flush(heading);
      previousBoundary = heading;
    }
  }
  flush(null);
  return counts;
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
  const phraseMatch = titlePhrase.length > 0 && (normalized.includes(titlePhrase) || compactPhraseMatches(heading.titleWords, normalized));
  const tokens = normalized.split(" ");
  const ordinalLabelMatches =
    heading.ordinal &&
    heading.ordinalLabel &&
    tokens.some((token, index) => token === heading.ordinalLabel && parseSpokenOrdinalToken(tokens[index + 1] ?? "") === heading.ordinal);
  if (heading.ordinalLabel === "part" && ordinalLabelMatches) return true;
  const wordsMatch = heading.titleWords.length === 0 || phraseMatch || heading.titleWords.every((word) => normalized.includes(word));
  if (!wordsMatch) return false;
  if (!heading.ordinal) return true;
  const ordinalMatches = tokens.some((token) => parseSpokenOrdinalToken(token) === heading.ordinal);
  if (heading.titleWords.length === 0 && heading.ordinalLabel) {
    return Boolean(ordinalLabelMatches);
  }
  // Many audiobooks speak/show only the chapter title ("LONG NIGHT") while
  // the EPUB labels it as "2. LONG NIGHT"; a strong title phrase is enough.
  return ordinalMatches || phraseMatch || (heading.titleWords.length > 1 && wordsMatch);
}

function headingMatchesDirectUtterance(heading: Heading, text: string): boolean {
  const normalized = normalizeText(text);
  if (!normalized) return false;
  if (!heading.ordinal && heading.titleWords.length > 0) {
    return normalized.includes(heading.titleWords.join(" ")) || compactPhraseMatches(heading.titleWords, normalized);
  }
  return headingMatchesWindow(heading, text);
}

function headingOrdinalMatchesText(heading: Heading, text: string): boolean {
  if (!heading.ordinal) return false;
  const tokens = normalizeText(text).split(" ");
  return tokens.some((token, index) => {
    if (parseSpokenOrdinalToken(token) !== heading.ordinal) return false;
    return !heading.ordinalLabel || tokens[index - 1] === heading.ordinalLabel || tokens[index - 1] === "chapter";
  });
}

function shouldPreferDirectMatch(heading: Heading): boolean {
  const normalized = normalizeText(heading.title);
  return isShortUnnumberedHeading(heading) || normalized.startsWith("epilogue");
}

function isOpeningCoveredPrelude(heading: Heading, headingIndex: number, firstOrdinalIndex: number): boolean {
  const normalized = normalizeText(heading.title);
  return headingIndex < firstOrdinalIndex && normalized === "preface";
}

function compactPhraseMatches(headingWords: string[], normalizedText: string): boolean {
  const needle = headingWords.join("");
  if (needle.length < 8) return false;
  const haystack = normalizedText.replace(/\s+/g, "");
  if (haystack.includes(needle)) return true;
  const maxDistance = needle.length >= 10 ? 2 : 1;
  const minLength = Math.max(1, needle.length - maxDistance);
  const maxLength = needle.length + maxDistance;
  for (let length = minLength; length <= maxLength; length += 1) {
    for (let index = 0; index + length <= haystack.length; index += 1) {
      if (editDistanceAtMost(needle, haystack.slice(index, index + length), maxDistance)) return true;
    }
  }
  return false;
}

function editDistanceAtMost(left: string, right: string, maxDistance: number): boolean {
  if (Math.abs(left.length - right.length) > maxDistance) return false;
  let previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  for (let i = 1; i <= left.length; i += 1) {
    const current = [i];
    let rowMin = current[0]!;
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      const value = Math.min(previous[j]! + 1, current[j - 1]! + 1, previous[j - 1]! + cost);
      current[j] = value;
      rowMin = Math.min(rowMin, value);
    }
    if (rowMin > maxDistance) return false;
    previous = current;
  }
  return previous[right.length]! <= maxDistance;
}

function findEmbeddedMatch(
  heading: Heading,
  embeddedChapters: RawAudioChapter[],
  utterances: TranscriptUtterance[],
  afterMs: number
): RawAudioChapter | null {
  const candidates = embeddedChapters.filter((chapter) => chapter.startMs > afterMs + 2_000);
  for (const chapter of candidates) {
    if (headingMatchesWindow(heading, windowText(utterances, chapter.startMs))) {
      return chapter;
    }
    const numericChapterHeading = /^(\d+|chapter\s+\d+)/.test(normalizeText(heading.title));
    if (
      numericChapterHeading &&
      heading.ordinal &&
      chapter.title &&
      parseOrdinalToken(chapter.title) === heading.ordinal
    ) {
      return chapter;
    }
  }
  return null;
}

function findDirectTranscriptMatch(
  heading: Heading,
  utterances: TranscriptUtterance[],
  afterMs: number,
  options: { preferExplicitOrdinal?: boolean } = {}
): TranscriptUtterance | null {
  const minGapMs = !heading.ordinal && heading.titleWords.length > 0 ? 2_000 : 15_000;
  const candidates = utterances.filter((utterance) => utterance.startMs > afterMs + minGapMs);
  let fallback: TranscriptUtterance | null = null;
  for (const utterance of candidates) {
    const text = shortUtterancesNear(utterances, utterance.startMs, 1_000, 4_000)
      .map((candidate) => candidate.text)
      .join(" ");
    if (!headingMatchesDirectUtterance(heading, text)) continue;
    if (!options.preferExplicitOrdinal || !heading.ordinal || headingOrdinalMatchesText(heading, text)) return utterance;
    fallback ??= utterance;
  }
  return fallback;
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
  const closing = utterances.find((utterance, index) => {
    if (utterance.startMs <= afterMs) return false;
    const text = normalizeText(utterance.text);
    if (text.startsWith("this concludes ")) return true;
    if (!text.startsWith("audible hopes you have enjoyed") && !text.startsWith("we hope youve enjoyed this program")) return false;
    const previous = utterances
      .slice(Math.max(0, index - 3), index)
      .map((candidate) => normalizeText(candidate.text))
      .join(" ");
    return !previous.includes("end of a part but not the end of a complete audiobook");
  });
  return closing?.startMs ?? null;
}

function fillSkippedGenericChapterHeadings(chapters: ProposedChapter[], headings: Heading[], embedded: RawAudioChapter[]): ProposedChapter[] {
  const byTitle = new Map(chapters.map((chapter) => [normalizeText(chapter.title), chapter]));
  const offsets = new Map<number, number>();
  for (const heading of headings) {
    if (!heading.ordinal || heading.ordinalLabel !== "chapter" || heading.titleWords.length > 0) continue;
    const chapter = byTitle.get(normalizeText(heading.title));
    if (!chapter) continue;
    const index = embedded.findIndex((candidate) => Math.abs(candidate.startMs / 1000 - chapter.startTime) <= 12);
    if (index < 0) continue;
    const offset = index - heading.ordinal;
    offsets.set(offset, (offsets.get(offset) ?? 0) + 1);
  }
  const [offset, count] = [...offsets.entries()].sort((a, b) => b[1] - a[1])[0] ?? [];
  if (offset === undefined || count < 3) return chapters;

  const out = [...chapters];
  const existingTimes = new Set(out.map((chapter) => Math.round(chapter.startTime * 1000)));
  for (const heading of headings) {
    if (!heading.ordinal || heading.ordinalLabel !== "chapter" || heading.titleWords.length > 0) continue;
    if (byTitle.has(normalizeText(heading.title))) continue;
    const embeddedIndex = heading.ordinal + offset;
    const matched = embedded[embeddedIndex];
    if (!matched || existingTimes.has(matched.startMs)) continue;
    out.push({
      startTime: matched.startMs / 1000,
      title: heading.title,
      confidence: "medium",
      reason: "Filled skipped generic EPUB chapter from the learned embedded chapter sequence.",
    });
  }
  return out.sort((a, b) => a.startTime - b.startTime);
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
  const maxOrdinalHeading = Math.max(0, ...headings.map((heading) => heading.ordinal ?? 0));
  const firstOrdinalIndex = headings.findIndex((heading) => heading.ordinal !== null);
  const preferCoarseTranscriptHeadings = maxOrdinalHeading > 0 && embedded.length <= maxOrdinalHeading && firstOrdinalIndex > 0;

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
    if (chapters.some((chapter) => chapter.title === "Opening credits") && isOpeningCoveredPrelude(heading, index, firstOrdinalIndex)) continue;
    const directFirst = shouldPreferDirectMatch(heading);
    const directOptions = { preferExplicitOrdinal: preferCoarseTranscriptHeadings && heading.ordinal !== null };
    let matched = directFirst ? findDirectTranscriptMatch(heading, utterances, previousMs, directOptions) : findEmbeddedMatch(heading, embedded, utterances, previousMs);
    let confidence: ProposedChapter["confidence"] = "high";
    let reason = "Matched EPUB major heading to transcript heading near embedded chapter boundary.";

    if (!matched) {
      const fallback = directFirst
        ? findEmbeddedMatch(heading, embedded, utterances, previousMs)
        : findDirectTranscriptMatch(heading, utterances, previousMs, directOptions);
      if (fallback) matched = { startMs: fallback.startMs };
    }

    if (matched && directFirst) {
      confidence = "medium";
      reason = "Matched EPUB major heading directly in transcript where no embedded chapter boundary matched.";
    } else if (matched && !directFirst) {
      const matchedStartMs = matched.startMs;
      if (!embedded.some((chapter) => chapter.startMs === matchedStartMs)) {
        confidence = "medium";
        reason = "Matched EPUB major heading directly in transcript where no embedded chapter boundary matched.";
      }
    }

    if (index === 0 && firstStoryMs !== null && (!matched || matched.startMs - firstStoryMs > 300_000)) {
      matched = { startMs: firstStoryMs };
      confidence = "medium";
      reason = "First EPUB heading was not spoken; used first story utterance after opening gap.";
    }

    if (!matched) continue;
    let matchedWindow = headingMatchesWindow(heading, windowText(utterances, matched.startMs));
    if (!matchedWindow && preferCoarseTranscriptHeadings) {
      const direct = findDirectTranscriptMatch(heading, utterances, previousMs, directOptions);
      const matchedStartMs = matched.startMs;
      const nextEmbedded = embedded.find((chapter) => chapter.startMs > matchedStartMs);
      if (direct && direct.startMs >= matchedStartMs && (!nextEmbedded || direct.startMs < nextEmbedded.startMs)) {
        matched = { startMs: direct.startMs };
        matchedWindow = true;
        confidence = "medium";
        reason = "Preferred a spoken transcript heading inside a coarse embedded chapter section.";
      }
    }
    const nextHeading = headings[index + 1];
    const nextDirect = nextHeading && shouldPreferDirectMatch(nextHeading) ? findDirectTranscriptMatch(nextHeading, utterances, previousMs, directOptions) : null;
    if (!matchedWindow && nextDirect && nextDirect.startMs < matched.startMs) {
      continue;
    }
    chapters.push({
      startTime: matched.startMs / 1000,
      title: heading.title,
      confidence,
      reason,
    });
    previousMs = matched.startMs;
  }

  chapters.splice(0, chapters.length, ...fillSkippedGenericChapterHeadings(chapters, headings, embedded));

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
