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
  wordCount: number;
  inline: boolean;
  openingWords: string[];
};

type HeadingCandidate = {
  heading: Heading;
  include: boolean;
  preferDirectMatch: boolean;
  features: {
    genericToc: boolean;
    genericChapterTocStructure: boolean;
    mainOrdinal: boolean;
    ordinalChapterStructure: boolean;
    shortUnnumbered: boolean;
    shortInterstitialSequence: boolean;
    frontMatterNote: boolean;
    openingCoveredPrelude: boolean;
  };
  decision: string;
};

type HeadingMatchEvidence = {
  startMs: number;
  confidence: ProposedChapter["confidence"];
  reason: string;
  matchedWindow: boolean;
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

function headingFromTitle(title: string, sourceIndex = -1, wordCount = 0, options: { inline?: boolean; openingWords?: string[] } = {}): Heading {
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
    wordCount,
    inline: options.inline ?? false,
    openingWords: options.openingWords ?? [],
  };
}

function isGenericTocTitle(title: string): boolean {
  const normalized = normalizeText(title);
  if (!normalized) return true;
  if (isFrontMatterTitle(normalized) || BACK_MATTER.has(normalized)) return true;
  if (isGenericNumberedChapterTocHeading(title)) return true;
  if (/^[ivxlcdm]+$/.test(normalized)) return true;
  return false;
}

function isFrontMatterTitle(normalizedTitle: string): boolean {
  return FRONT_MATTER.has(normalizedTitle) || normalizedTitle.startsWith("also by ");
}

function isGenericNumberedChapterTocHeading(title: string): boolean {
  const normalized = normalizeText(title);
  return /^(chapter\s*)?\d+$/.test(normalized);
}

function isGenericEmbeddedChapterTitle(title?: string): boolean {
  const normalized = normalizeText(title ?? "");
  if (!normalized) return true;
  if (/^\d+$/.test(normalized)) return true;
  if (/^[ivxlcdm]+$/.test(normalized)) return true;
  if (/^(track|disc|cd|section)\s+\d+$/.test(normalized)) return true;
  if (/^(chapter|chap|ch|part|book)\s+(\d+|[ivxlcdm]+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)$/.test(normalized)) {
    return true;
  }
  return false;
}

function embeddedChaptersLookUserFacing(chapters: RawAudioChapter[]): boolean {
  const titled = chapters.filter((chapter) => chapter.title?.trim());
  if (titled.length < 4) return false;
  const nonGenericCount = titled.filter((chapter) => !isGenericEmbeddedChapterTitle(chapter.title)).length;
  return nonGenericCount / titled.length >= 0.75;
}

function chaptersFromUserFacingEmbedded(chapters: RawAudioChapter[]): ProposedChapter[] {
  return chapters
    .filter((chapter) => chapter.title?.trim())
    .map((chapter) => ({
      startTime: chapter.startMs / 1000,
      title: chapter.title!.trim(),
      confidence: "high" as const,
      reason: "Trusted user-facing embedded audiobook chapter title.",
    }));
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

function repairOpeningDropCaps(text: string): string {
  const sample = text.slice(0, 300);
  const dropCapMatches = sample.match(/\b[A-Z]\s+[A-Z]{2,}\b/g) ?? [];
  if (dropCapMatches.length < 2) return text;
  return text.replace(/\b([A-Z])\s+([A-Z]{2,})\b/g, (_, first: string, rest: string) => `${first}${rest}`);
}

function deriveOpeningWords(entry: EpubChapterEntry): string[] {
  const ignored = new Set(["a", "an", "and", "the"]);
  return normalizeText(repairOpeningDropCaps(entry.text))
    .split(" ")
    .filter((word) => word.length >= 3 && !ignored.has(word))
    .slice(0, 8);
}

function titleCaseHeading(value: string): string {
  const smallWords = new Set(["a", "an", "and", "as", "at", "but", "by", "for", "in", "nor", "of", "on", "or", "the", "to", "with", "without"]);
  return normalizeText(value)
    .split(" ")
    .map((word, index) => {
      if (index > 0 && smallWords.has(word)) return word;
      return word
        .split("-")
        .map((part) => part ? `${part[0]!.toUpperCase()}${part.slice(1)}` : part)
        .join("-");
    })
    .join(" ");
}

function cleanInlineTaleSubtitle(value: string): string {
  const tokens = value.replace(/[“”"]/g, " ").replace(/\s+/g, " ").trim().split(" ").filter(Boolean);
  while (tokens.length > 3 && /^[A-Z]$/u.test(tokens.at(-1)!)) tokens.pop();
  return tokens.join(" ");
}

function deriveInlineHeadingTitles(entry: EpubChapterEntry): string[] {
  const title = deriveHeadingTitle(entry);
  if (!isGenericNumberedChapterTocHeading(title) || entry.wordCount < 500) return [];
  const titles: string[] = [];
  const text = entry.text.replace(/\s+/g, " ");
  const quotedInlineTaleHeading = /(?:^|[.!?]["”]?\s+)(THE\s+[A-Z][A-Z’' -]{2,40}\s+TALE):\s*["“]([^"”]{3,80})["”]/g;
  const inlineTaleHeading =
    /(?:^|[.!?]["”]?\s+)(THE\s+[A-Z][A-Z’' -]{2,40}\s+TALE):\s*["“]?([A-Z][A-Z’' -]+?)["”]?(?=\s+["“]?\s*[A-Z](?:\s+[A-Z]{2,}){0,3}\s+[a-z])/g;
  let match: RegExpExecArray | null;
  while ((match = quotedInlineTaleHeading.exec(text))) {
    const label = titleCaseHeading(match[1] ?? "");
    const subtitle = titleCaseHeading(cleanInlineTaleSubtitle(match[2] ?? ""));
    const candidate = `${label}: ${subtitle}`.trim();
    const wordCount = normalizeText(candidate).split(" ").filter(Boolean).length;
    if (wordCount >= 4 && wordCount <= 14) titles.push(candidate);
  }
  while ((match = inlineTaleHeading.exec(text))) {
    const label = titleCaseHeading(match[1] ?? "");
    const subtitle = titleCaseHeading(cleanInlineTaleSubtitle(match[2] ?? ""));
    const candidate = `${label}: ${subtitle}`.trim();
    const wordCount = normalizeText(candidate).split(" ").filter(Boolean).length;
    if (wordCount >= 4 && wordCount <= 14) titles.push(candidate);
  }
  return [...new Set(titles)];
}

function collectHeadingCandidates(entries: EpubChapterEntry[]): HeadingCandidate[] {
  const headings: Heading[] = [];
  const seen = new Set<string>();
  for (const [sourceIndex, entry] of entries.entries()) {
    const title = deriveHeadingTitle(entry);
    const normalized = normalizeText(title);
    if (BACK_MATTER.has(normalized)) break;
    if (!title || seen.has(title)) continue;
    seen.add(title);
    headings.push(headingFromTitle(title, sourceIndex, entry.wordCount, { openingWords: deriveOpeningWords(entry) }));
    const inlineTitles = deriveInlineHeadingTitles(entry);
    for (const [inlineIndex, inlineTitle] of inlineTitles.entries()) {
      const inlineNormalized = normalizeText(inlineTitle);
      if (!inlineTitle || seen.has(inlineNormalized)) continue;
      seen.add(inlineNormalized);
      headings.push(headingFromTitle(inlineTitle, sourceIndex + (inlineIndex + 1) / 100, 0, { inline: true }));
    }
  }
  const ordinalHeadings = headings.filter((heading) => !isGenericTocTitle(heading.title) && isOrdinalChapterStructureHeading(heading));
  const hasOrdinalChapterStructure =
    ordinalHeadings.length >= 3;
  const substantialGenericChapterCount = headings.filter((heading) => isGenericNumberedChapterTocHeading(heading.title) && heading.wordCount >= 500).length;
  const allowSubstantialGenericChapters = !hasOrdinalChapterStructure && substantialGenericChapterCount > 0 && substantialGenericChapterCount <= 10;
  const hasGenericChapterTocStructure = headings.filter((heading) => isGenericNumberedChapterTocHeading(heading.title)).length >= 3;
  const chapterOrdinalHeadings = headings.filter((heading) => !isGenericTocTitle(heading.title) && isOrdinalChapterHeading(heading));
  const firstChapterOrdinalIndex = chapterOrdinalHeadings[0]?.sourceIndex ?? ordinalHeadings[0]?.sourceIndex ?? Number.POSITIVE_INFINITY;
  const lastChapterOrdinalIndex = chapterOrdinalHeadings.at(-1)?.sourceIndex ?? ordinalHeadings.at(-1)?.sourceIndex ?? Number.NEGATIVE_INFINITY;
  const shortInterstitialCounts = countShortInterstitialSequenceHeadings(headings, firstChapterOrdinalIndex, lastChapterOrdinalIndex);
  const hasUniqueShortInterstitialSequence = [...shortInterstitialCounts.values()].filter((count) => count === 1).length >= 2;

  return headings.map((heading, headingIndex) => {
    const normalized = normalizeText(heading.title);
    const genericToc = isGenericTocTitle(heading.title);
    const substantialGenericChapter = allowSubstantialGenericChapters && genericToc && isGenericNumberedChapterTocHeading(heading.title) && heading.wordCount >= 500;
    const mainOrdinal = isMainOrdinalHeading(heading);
    const shortUnnumbered = isShortUnnumberedHeading(heading);
    const shortHeadingCount = shortInterstitialCounts.get(normalized) ?? 0;
    const shortInterstitialSequence =
      shortUnnumbered &&
      ((heading.sourceIndex > firstChapterOrdinalIndex && heading.sourceIndex < lastChapterOrdinalIndex && shortHeadingCount === 1) ||
        (heading.sourceIndex > lastChapterOrdinalIndex && hasUniqueShortInterstitialSequence));
    const frontMatterNote = shortUnnumbered && heading.sourceIndex < firstChapterOrdinalIndex && isFrontMatterNoteHeading(heading);
    const genericStructureAllowsNamedHeading = !hasOrdinalChapterStructure && !hasGenericChapterTocStructure;
    const openingCoveredPrelude = isOpeningCoveredPrelude(heading, headingIndex, headings.findIndex((candidate) => candidate.ordinal !== null));

    let include = false;
    let decision = "excluded";
    if (genericToc && !substantialGenericChapter || BACK_MATTER.has(normalized)) {
      decision = "excluded-generic-toc";
    } else if (substantialGenericChapter) {
      include = true;
      decision = "substantial-generic-chapter-heading";
    } else if (heading.inline) {
      include = true;
      decision = "inline-epub-heading";
    } else if (/^(prologue|epilogue)$/.test(normalized) || /^epilogue\s+/.test(normalized)) {
      include = true;
      decision = "structural-terminal-heading";
    } else if (normalized.startsWith("introduction ")) {
      include = true;
      decision = "derived-introduction-heading";
    } else if (shortUnnumbered && genericStructureAllowsNamedHeading) {
      include = true;
      decision = "named-heading-without-ordinal-structure";
    } else if (frontMatterNote || (shortUnnumbered && heading.sourceIndex < firstChapterOrdinalIndex && hasUniqueShortInterstitialSequence)) {
      include = true;
      decision = "frontmatter-or-opening-interstitial";
    } else if (shortInterstitialSequence) {
      include = true;
      decision = "short-interstitial-sequence";
    } else if (!heading.ordinal && genericStructureAllowsNamedHeading && heading.titleWords.length > 0) {
      include = true;
      decision = "long-named-heading-without-ordinal-structure";
    } else if (mainOrdinal) {
      include = true;
      decision = "ordinal-heading";
    }

    return {
      heading,
      include,
      preferDirectMatch: heading.inline || shortUnnumbered || normalized.startsWith("epilogue") || substantialGenericChapter,
      features: {
        genericToc,
        genericChapterTocStructure: hasGenericChapterTocStructure,
        mainOrdinal,
        ordinalChapterStructure: hasOrdinalChapterStructure,
        shortUnnumbered,
        shortInterstitialSequence,
        frontMatterNote,
        openingCoveredPrelude,
      },
      decision,
    };
  });
}

export function selectMajorEpubHeadings(entries: EpubChapterEntry[]): Heading[] {
  return collectHeadingCandidates(entries)
    .filter((candidate) => candidate.include)
    .map((candidate) => candidate.heading);
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
  const wordsMatch =
    heading.titleWords.length === 0 ||
    phraseMatch ||
    heading.titleWords.every((word) => headingTitleWordMatches(word, normalized, tokens));
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

function standaloneOrdinalTextMatches(text: string, ordinal: number): boolean {
  const normalized = normalizeText(text);
  if (parseOrdinalToken(normalized) === ordinal) return true;
  return text
    .split(/(?<!\d)[.!?。！？]+(?!\d)/u)
    .map((sentence) => normalizeText(sentence))
    .some((sentence) => parseOrdinalToken(sentence) === ordinal);
}

function ordinalWithOpeningWordsMatches(heading: Heading, text: string): boolean {
  if (!heading.ordinal || heading.openingWords.length < 3) return false;
  const tokens = normalizeText(text).split(" ").filter(Boolean);
  if (parseSpokenOrdinalToken(tokens[0] ?? "") !== heading.ordinal) return false;
  const searchable = tokens.slice(1, 14);
  let cursor = 0;
  for (const word of heading.openingWords.slice(0, 4)) {
    const index = searchable.findIndex((token, candidateIndex) => candidateIndex >= cursor && token === word);
    if (index < 0) return false;
    cursor = index + 1;
  }
  return true;
}

function openingTokenMatches(expected: string, actual: string, maxDistance: number): boolean {
  if (actual === expected) return true;
  if (expected.length < 6 || actual.length < 5) return false;
  return editDistanceAtMost(expected, actual, maxDistance);
}

function openingWordsMatchText(heading: Heading, text: string): boolean {
  if (heading.openingWords.length < 4) return false;
  const tokens = normalizeText(text).split(" ").filter(Boolean).slice(0, 32);
  let cursor = 0;
  for (const [wordIndex, word] of heading.openingWords.slice(0, 4).entries()) {
    const end = Math.min(tokens.length, cursor + (wordIndex === 0 ? 20 : 10));
    let foundIndex = -1;
    for (let index = cursor; index < end; index += 1) {
      const maxDistance = wordIndex === 0 ? 2 : 1;
      if (openingTokenMatches(word, tokens[index] ?? "", maxDistance)) {
        foundIndex = index;
        break;
      }
    }
    if (foundIndex < 0 || (wordIndex === 0 && foundIndex > 16)) return false;
    cursor = foundIndex + 1;
  }
  return true;
}

function isBareOrdinalHeading(heading: Heading): boolean {
  return Boolean(heading.ordinal && heading.ordinalLabel && heading.titleWords.length === 0);
}

function headingTitleWordMatches(word: string, normalizedText: string, tokens: string[]): boolean {
  if (normalizedText.includes(word)) return true;
  const ordinal = parseOrdinalToken(word);
  if (ordinal === null) return false;
  if (!/^\d+$/.test(word) && !/^[ivxlcdm]+$/.test(word) && !NUMBER_WORDS.has(word)) return false;
  return tokens.some((token) => parseSpokenOrdinalToken(token) === ordinal || parseOrdinalToken(token) === ordinal);
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
  afterMs: number,
  options: { allowGenericOrdinalMatch?: boolean } = {}
): RawAudioChapter | null {
  const candidates = embeddedChapters.filter((chapter) => chapter.startMs > afterMs + 2_000);
  for (const chapter of candidates) {
    const text = windowText(utterances, chapter.startMs);
    if (headingMatchesWindow(heading, text) || (isBareOrdinalHeading(heading) && openingWordsMatchText(heading, text))) {
      return chapter;
    }
    const numericChapterHeading = /^(\d+|chapter\s+\d+)/.test(normalizeText(heading.title));
    if (
      options.allowGenericOrdinalMatch !== false &&
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
  options: { preferExplicitOrdinal?: boolean; requireExplicitOrdinal?: boolean } = {}
): TranscriptUtterance | null {
  const minGapMs = !heading.ordinal && heading.titleWords.length > 0 ? 2_000 : 15_000;
  const candidates = utterances.filter((utterance) => utterance.startMs > afterMs + minGapMs);
  let fallback: TranscriptUtterance | null = null;
  for (const utterance of candidates) {
    if (isBareOrdinalHeading(heading) && ordinalWithOpeningWordsMatches(heading, utterance.text)) {
      return utterance;
    }
    if (heading.ordinal && isBareOrdinalHeading(heading) && standaloneOrdinalTextMatches(utterance.text, heading.ordinal)) {
      return utterance;
    }
    const text = shortUtterancesNear(utterances, utterance.startMs, 1_000, 4_000)
      .map((candidate) => candidate.text)
      .join(" ");
    if (!headingMatchesDirectUtterance(heading, text)) continue;
    if (!options.preferExplicitOrdinal || !heading.ordinal || headingOrdinalMatchesText(heading, text)) return utterance;
    if (options.requireExplicitOrdinal) continue;
    fallback ??= utterance;
  }
  return fallback;
}

function shouldRequireExplicitOrdinalForDirectOverride(heading: Heading): boolean {
  if (!heading.ordinal) return false;
  const distinctiveWords = heading.titleWords.filter((word) => word.length >= 4 && !["chapter", "part", "book"].includes(word));
  return new Set(distinctiveWords).size <= 1;
}

function firstStoryUtteranceMs(utterances: TranscriptUtterance[], beforeMs: number): number | null {
  const early = utterances.filter((utterance) => utterance.startMs < beforeMs);
  for (let index = 1; index < early.length; index += 1) {
    const previous = early[index - 1]!;
    const current = early[index]!;
    if (current.startMs - previous.endMs >= 30_000) return current.startMs;
  }
  return early.find((utterance) => !isOpeningCreditUtterance(utterance.text))?.startMs ?? null;
}

function isOpeningCreditUtterance(text: string): boolean {
  const normalized = normalizeText(text);
  if (normalized === "this is audible") return true;
  if (normalized.startsWith("recorded books") && normalized.includes("present")) return true;
  return normalized.includes("present") && normalized.includes("narrated by") && normalized.split(" ").length <= 30;
}

function findClosingCreditsMs(utterances: TranscriptUtterance[], afterMs: number): number | null {
  const closing = utterances.find((utterance, index) => {
    if (utterance.startMs <= afterMs) return false;
    const text = normalizeText(utterance.text);
    if (text.startsWith("this concludes ")) return true;
    if (text.includes("audible") && /\bproduc\w*\b/.test(text)) return true;
    if (
      !text.startsWith("audible hopes you have enjoyed") &&
      !text.startsWith("we hope youve enjoyed this program") &&
      !text.startsWith("we hope you have enjoyed this production")
    ) {
      return false;
    }
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

function semanticSequenceHeadingFromEntry(entry: EpubChapterEntry, sourceIndex: number): Heading | null {
  const title = deriveHeadingTitle(entry);
  const normalized = normalizeText(title);
  if (/^prologue(\s+|$)/.test(normalized) || /^epilogue(\s+|$)/.test(normalized)) {
    return headingFromTitle(title, sourceIndex, entry.wordCount, { openingWords: deriveOpeningWords(entry) });
  }
  const tokens = normalized.split(" ").filter(Boolean);
  if (tokens.length < 2 || !/^\d+$/.test(tokens[0]!) || /^\d+$/.test(tokens[1]!)) return null;
  const ordinal = Number(tokens[0]!);
  if (!Number.isSafeInteger(ordinal) || ordinal <= 0 || ordinal > 200) return null;
  return {
    title,
    ordinal,
    ordinalLabel: null,
    titleWords: tokens.slice(1),
    sourceIndex,
    wordCount: entry.wordCount,
    inline: false,
    openingWords: deriveOpeningWords(entry),
  };
}

function sequentialEmbeddedOrdinal(chapter: RawAudioChapter): number | null {
  if (!isGenericEmbeddedChapterTitle(chapter.title)) return null;
  const tokens = normalizeText(chapter.title ?? "").split(" ").filter(Boolean);
  if (tokens[0] === "chapter" || tokens[0] === "track") return parseOrdinalToken(tokens[1] ?? "");
  return parseOrdinalToken(chapter.title ?? "");
}

function embeddedChaptersLookEvenlyDivided(chapters: RawAudioChapter[]): boolean {
  const durations = chapters
    .map((chapter) => (chapter.endMs !== undefined ? chapter.endMs - chapter.startMs : null))
    .filter((duration): duration is number => duration !== null && duration > 0);
  if (durations.length < 5 || durations.length !== chapters.length) return false;
  const average = durations.reduce((sum, duration) => sum + duration, 0) / durations.length;
  const min = Math.min(...durations);
  const max = Math.max(...durations);
  return (max - min) / average <= 0.08;
}

function buildGenericEmbeddedSequenceChapters(
  entries: EpubChapterEntry[],
  embedded: RawAudioChapter[],
  utterances: TranscriptUtterance[],
  firstStoryMs: number | null,
  hasOpeningGap: boolean
): ProposedChapter[] | null {
  if (embeddedChaptersLookEvenlyDivided(embedded)) return null;
  const embeddedOrdinals = embedded.map(sequentialEmbeddedOrdinal);
  if (embeddedOrdinals.length < 5 || embeddedOrdinals.some((ordinal) => ordinal === null)) return null;
  for (const [index, ordinal] of embeddedOrdinals.entries()) {
    if (ordinal !== index + 1) return null;
  }

  const semanticHeadings = entries
    .map((entry, index) => semanticSequenceHeadingFromEntry(entry, index))
    .filter((heading): heading is Heading => heading !== null);
  const numberedHeadings = semanticHeadings.filter((heading) => heading.ordinal !== null && heading.titleWords.length > 0);
  if (numberedHeadings.length < 5) return null;

  const firstNumberedIndex = numberedHeadings[0]!.sourceIndex;
  const lastNumberedIndex = numberedHeadings.at(-1)!.sourceIndex;
  const prelude = semanticHeadings.find(
    (heading) => heading.sourceIndex < firstNumberedIndex && /^prologue(\s+|$)/.test(normalizeText(heading.title))
  );
  const firstNumbered = numberedHeadings[0]!;
  const firstNumberedAtOpeningStory =
    !prelude &&
    hasOpeningGap &&
    firstStoryMs !== null &&
    embedded[0]?.startMs === 0 &&
    firstNumbered.ordinal === 1 &&
    headingMatchesWindow(firstNumbered, windowText(utterances, firstStoryMs));
  const interiorShortUnnumberedHeadingCount = entries.filter((entry, index) => {
    if (index <= firstNumberedIndex || index >= lastNumberedIndex) return false;
    const semantic = semanticSequenceHeadingFromEntry(entry, index);
    if (semantic && semantic.ordinal !== null) return false;
    const heading = headingFromTitle(deriveHeadingTitle(entry), index, entry.wordCount);
    return heading.ordinal === null && isShortUnnumberedHeading(heading);
  }).length;
  if (!prelude && interiorShortUnnumberedHeadingCount >= 2) return null;
  const sectionHeadings = entries
    .map((entry, index) => headingFromTitle(deriveHeadingTitle(entry), index, entry.wordCount))
    .filter((heading) => heading.ordinalLabel === "book" || heading.ordinalLabel === "part");
  if (!prelude && sectionHeadings.length > 0) return null;
  const lastNumbered = numberedHeadings.at(-1);
  const trailingHeadings = lastNumbered
    ? entries
        .map((entry, index) => ({ entry, index, title: deriveHeadingTitle(entry) }))
        .filter(({ entry, index, title }) => {
          if (index <= lastNumbered.sourceIndex) return false;
          const normalized = normalizeText(title);
          if (!normalized || isFrontMatterTitle(normalized) || BACK_MATTER.has(normalized) || isGenericTocTitle(title)) return false;
          return entry.wordCount >= 300 || /^epilogue(\s+|$)/.test(normalized);
        })
    : [];
  const requiredEmbeddedCount = numberedHeadings.length + (prelude ? 1 : 0);
  if (embedded.length < requiredEmbeddedCount) return null;
  const offset = prelude ? 1 : 0;
  const chapters: ProposedChapter[] = [];
  if (prelude) {
    if (hasOpeningGap && firstStoryMs !== null) {
      chapters.push({
        startTime: 0,
        title: "Opening credits",
        confidence: "medium",
        reason: "Audio starts with an intro and first story utterance is delayed.",
      });
      chapters.push({
        startTime: firstStoryMs / 1000,
        title: prelude.title,
        confidence: "medium",
        reason: "Used first story utterance for EPUB prologue before a generic embedded chapter sequence.",
      });
    } else {
      chapters.push({
        startTime: embedded[0]!.startMs / 1000,
        title: prelude.title,
        confidence: "medium",
        reason: "Retitled first generic embedded chapter from EPUB prologue.",
      });
    }
  }
  if (firstNumberedAtOpeningStory) {
    chapters.push({
      startTime: 0,
      title: "Opening credits",
      confidence: "medium",
      reason: "Audio starts with front matter before the first numbered EPUB heading.",
    });
  }

  for (const heading of numberedHeadings) {
    const embeddedIndex = heading.ordinal! - 1 + offset;
    const matched = embedded[embeddedIndex];
    if (!matched && !(firstNumberedAtOpeningStory && heading === firstNumbered)) continue;
    chapters.push({
      startTime: firstNumberedAtOpeningStory && heading === firstNumbered ? firstStoryMs! / 1000 : matched!.startMs / 1000,
      title: heading.title,
      confidence: "medium",
      reason: firstNumberedAtOpeningStory && heading === firstNumbered
        ? "Used first spoken numbered EPUB heading after opening front matter."
        : "Retitled generic embedded chapter sequence from EPUB numbered heading.",
    });
  }

  const lastUsedEmbeddedIndex = Math.max(
    ...chapters.map((chapter) => embedded.findIndex((candidate) => candidate.startMs / 1000 === chapter.startTime))
  );
  for (const [trailingIndex, trailing] of trailingHeadings.entries()) {
    const matched = embedded[lastUsedEmbeddedIndex + 1 + trailingIndex];
    if (!matched) break;
    chapters.push({
      startTime: matched.startMs / 1000,
      title: trailing.title,
      confidence: "medium",
      reason: "Retitled trailing generic embedded chapter from EPUB back-matter heading.",
    });
  }

  return chapters.length >= numberedHeadings.length ? chapters.sort((a, b) => a.startTime - b.startTime) : null;
}

function resolveHeadingCandidate(
  candidate: HeadingCandidate,
  context: {
    index: number;
    headings: Heading[];
    embedded: RawAudioChapter[];
    utterances: TranscriptUtterance[];
    previousMs: number;
    firstStoryMs: number | null;
    preferCoarseTranscriptHeadings: boolean;
    trustEmbeddedGenericOrdinals: boolean;
  }
): HeadingMatchEvidence | null {
  const { heading } = candidate;
  const directOptions = { preferExplicitOrdinal: context.preferCoarseTranscriptHeadings && heading.ordinal !== null };
  const directFirst = candidate.preferDirectMatch;
  let matched = directFirst
    ? findDirectTranscriptMatch(heading, context.utterances, context.previousMs, directOptions)
    : findEmbeddedMatch(heading, context.embedded, context.utterances, context.previousMs, {
        allowGenericOrdinalMatch: context.trustEmbeddedGenericOrdinals,
      });
  let confidence: ProposedChapter["confidence"] = "high";
  let reason = "Matched EPUB major heading to transcript heading near embedded chapter boundary.";

  if (!matched) {
    const fallback = directFirst
      ? findEmbeddedMatch(heading, context.embedded, context.utterances, context.previousMs, {
          allowGenericOrdinalMatch: context.trustEmbeddedGenericOrdinals,
        })
      : findDirectTranscriptMatch(heading, context.utterances, context.previousMs, directOptions);
    if (fallback) matched = { startMs: fallback.startMs };
  }

  if (matched && directFirst) {
    confidence = "medium";
    reason = "Matched EPUB major heading directly in transcript where no embedded chapter boundary matched.";
  } else if (matched && !directFirst) {
    if (context.preferCoarseTranscriptHeadings) {
      const direct = findDirectTranscriptMatch(heading, context.utterances, context.previousMs, {
        ...directOptions,
        requireExplicitOrdinal: shouldRequireExplicitOrdinalForDirectOverride(heading),
      });
      if (direct && direct.startMs > context.previousMs && direct.startMs < matched.startMs) {
        matched = { startMs: direct.startMs };
        confidence = "medium";
        reason = "Preferred a spoken transcript heading inside a coarse embedded chapter section.";
      }
    }
    const matchedStartMs = matched.startMs;
    if (!context.embedded.some((chapter) => chapter.startMs === matchedStartMs) && reason === "Matched EPUB major heading to transcript heading near embedded chapter boundary.") {
      confidence = "medium";
      reason = "Matched EPUB major heading directly in transcript where no embedded chapter boundary matched.";
    }
  }

  if (context.index === 0 && context.firstStoryMs !== null && (!matched || matched.startMs - context.firstStoryMs > 300_000)) {
    matched = { startMs: context.firstStoryMs };
    confidence = "medium";
    reason = "First EPUB heading was not spoken; used first story utterance after opening gap.";
  }

  if (!matched) return null;
  let matchedWindow = headingMatchesWindow(heading, windowText(context.utterances, matched.startMs));
  if (!matchedWindow && context.preferCoarseTranscriptHeadings) {
    const direct = findDirectTranscriptMatch(heading, context.utterances, context.previousMs, {
      ...directOptions,
      requireExplicitOrdinal: shouldRequireExplicitOrdinalForDirectOverride(heading),
    });
    const matchedStartMs = matched.startMs;
    const nextEmbedded = context.embedded.find((chapter) => chapter.startMs > matchedStartMs);
    if (direct && direct.startMs >= matchedStartMs && (!nextEmbedded || direct.startMs < nextEmbedded.startMs)) {
      matched = { startMs: direct.startMs };
      matchedWindow = true;
      confidence = "medium";
      reason = "Preferred a spoken transcript heading inside a coarse embedded chapter section.";
    }
  }

  const nextHeading = context.headings[context.index + 1];
  const nextCandidate = nextHeading ? { ...candidate, heading: nextHeading, preferDirectMatch: shouldPreferDirectMatch(nextHeading) } : null;
  const nextDirect = nextCandidate?.preferDirectMatch ? findDirectTranscriptMatch(nextHeading!, context.utterances, context.previousMs, directOptions) : null;
  if (!matchedWindow && nextDirect && nextDirect.startMs < matched.startMs) {
    return null;
  }

  return {
    startMs: matched.startMs,
    confidence,
    reason,
    matchedWindow,
  };
}

export function proposeChapterMarkers(input: {
  epubEntries: EpubChapterEntry[];
  transcriptUtterances: TranscriptUtterance[];
  embeddedChapters: RawAudioChapter[];
}): ChapterProposalReport {
  const candidates = collectHeadingCandidates(input.epubEntries).filter((candidate) => candidate.include);
  const headings = candidates.map((candidate) => candidate.heading);
  const embedded = [...input.embeddedChapters].sort((a, b) => a.startMs - b.startMs);
  const utterances = [...input.transcriptUtterances].sort((a, b) => a.startMs - b.startMs);
  const chapters: ProposedChapter[] = [];
  if (embeddedChaptersLookUserFacing(embedded)) {
    return {
      epubHeadings: headings.map((heading) => heading.title),
      embeddedChapterCount: embedded.length,
      transcriptUtteranceCount: utterances.length,
      chapters: chaptersFromUserFacingEmbedded(embedded),
    };
  }
  const maxOrdinalHeading = Math.max(0, ...headings.map((heading) => heading.ordinal ?? 0));
  const firstOrdinalIndex = headings.findIndex((heading) => heading.ordinal !== null);
  const preferCoarseTranscriptHeadings = maxOrdinalHeading > 0 && embedded.length <= maxOrdinalHeading && firstOrdinalIndex > 0;

  let previousMs = -1;
  const firstEmbeddedAfterIntro = embedded.find((chapter) => chapter.startMs > 30_000)?.startMs ?? Number.POSITIVE_INFINITY;
  const firstStoryMs = firstStoryUtteranceMs(utterances, firstEmbeddedAfterIntro);
  const startsWithOpeningCredit = utterances.some((utterance) => utterance.startMs <= 5_000 && isOpeningCreditUtterance(utterance.text));
  const hasOpeningGap = firstStoryMs !== null && (firstStoryMs > 30_000 || startsWithOpeningCredit);
  const trustEmbeddedGenericOrdinals = !embeddedChaptersLookEvenlyDivided(embedded);
  const sequenceChapters = buildGenericEmbeddedSequenceChapters(input.epubEntries, embedded, utterances, firstStoryMs, hasOpeningGap);
  if (sequenceChapters) {
    const sequenceLastStoryStartMs = sequenceChapters.at(-1)?.startTime ? sequenceChapters.at(-1)!.startTime * 1000 : 0;
    const sequenceClosingMs = findClosingCreditsMs(utterances, sequenceLastStoryStartMs);
    if (sequenceClosingMs !== null) {
      sequenceChapters.push({
        startTime: sequenceClosingMs / 1000,
        title: "Closing credits",
        confidence: "high",
        reason: "Matched standard audiobook closing-credit phrase in transcript.",
      });
    }
    return {
      epubHeadings: headings.map((heading) => heading.title),
      embeddedChapterCount: embedded.length,
      transcriptUtteranceCount: utterances.length,
      chapters: sequenceChapters,
    };
  }
  if (hasOpeningGap) {
    chapters.push({
      startTime: 0,
      title: "Opening credits",
      confidence: "medium",
      reason: "Audio starts with an intro and first story utterance is delayed.",
    });
  }

  for (const [index, candidate] of candidates.entries()) {
    const { heading } = candidate;
    if (chapters.some((chapter) => chapter.title === "Opening credits") && candidate.features.openingCoveredPrelude) continue;
    const matched = resolveHeadingCandidate(candidate, {
      index,
      headings,
      embedded,
      utterances,
      previousMs,
      firstStoryMs,
      preferCoarseTranscriptHeadings,
      trustEmbeddedGenericOrdinals,
    });
    if (!matched) continue;
    chapters.push({
      startTime: matched.startMs / 1000,
      title: heading.title,
      confidence: matched.confidence,
      reason: matched.reason,
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
