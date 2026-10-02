import { normalizeWords, wordErrorRate } from "./wer";

export type Exclusion = {
  id: string;
  left: string;
  span: string;
  right: string;
  reason: string;
};

export type FormattingRule = {
  id: string;
  canonical: string;
  variants: string[];
};

type Interval = { start: number; end: number; words: string[] };
type Counts = {
  referenceWords: number;
  hypothesisWords: number;
  substitutions: number;
  deletions: number;
  insertions: number;
  errors: number;
  disagreementRate: number;
};
type Change = { rule: string; start: number; before: string[]; after: string[] };

function positions(words: string[], needle: string[]) {
  const found: number[] = [];
  for (let i = 0; i <= words.length - needle.length; i++) {
    if (needle.every((word, j) => words[i + j] === word)) found.push(i);
  }
  return found;
}

function locate(words: string[], left: string[], right: string[], maxGap: number): Interval {
  const starts = positions(words, left);
  const ends = positions(words, right);
  if (starts.length !== 1 || ends.length !== 1) throw new Error("anchors must each occur exactly once");
  const start = starts[0]! + left.length;
  const end = ends[0]!;
  if (end < start || end - start > maxGap) throw new Error("reversed or oversized anchored gap");
  return { start, end, words: words.slice(start, end) };
}

function counts(reference: string[], hypothesis: string[]): Counts {
  if (!reference.length) {
    return { referenceWords: 0, hypothesisWords: hypothesis.length, substitutions: 0,
      deletions: 0, insertions: hypothesis.length, errors: hypothesis.length, disagreementRate: 0 };
  }
  const score = wordErrorRate(reference.join(" "), hypothesis.join(" "));
  const errors = score.substitutions + score.deletions + score.insertions;
  return { referenceWords: score.referenceWords, hypothesisWords: score.hypothesisWords,
    substitutions: score.substitutions, deletions: score.deletions, insertions: score.insertions,
    errors, disagreementRate: errors / score.referenceWords };
}

function total(scores: Counts[]): Counts {
  const sum = scores.reduce((sum, score) => ({
    referenceWords: sum.referenceWords + score.referenceWords,
    hypothesisWords: sum.hypothesisWords + score.hypothesisWords,
    substitutions: sum.substitutions + score.substitutions,
    deletions: sum.deletions + score.deletions,
    insertions: sum.insertions + score.insertions,
    errors: sum.errors + score.errors,
    disagreementRate: 0,
  }), counts([], []));
  if (!sum.referenceWords) throw new Error("adjustments removed the entire reference");
  return { ...sum, disagreementRate: sum.errors / sum.referenceWords };
}

export function normalizeFormatting(words: string[], rules: FormattingRule[]) {
  const seen = new Set<string>();
  const patterns = rules.flatMap(rule => {
    const after = normalizeWords(rule.canonical);
    if (!after.length || !rule.id) throw new Error("formatting rules need an id and nonempty canonical words");
    return rule.variants.map(variant => {
      const before = normalizeWords(variant);
      const key = before.join(" ");
      if (!before.length || seen.has(key)) throw new Error("empty or duplicate formatting variant");
      seen.add(key);
      return { rule: rule.id, before, after };
    });
  }).sort((a, b) => b.before.length - a.before.length);
  const output: string[] = [];
  const changes: Change[] = [];
  for (let i = 0; i < words.length;) {
    const match = patterns.find(p => p.before.every((word, j) => words[i + j] === word));
    if (!match) { output.push(words[i++]!); continue; }
    output.push(...match.after);
    if (match.before.join(" ") !== match.after.join(" ")) {
      changes.push({ rule: match.rule, start: i, before: match.before, after: match.after });
    }
    i += match.before.length;
  }
  return { words: output, changes };
}

/** Candidates must be justified from reference context, never generated from model errors. */
export function adjustedEpubAgreement(
  reference: string,
  hypotheses: Record<string, string>,
  exclusions: Exclusion[],
  formattingRules: FormattingRule[] = [],
) {
  const expected = normalizeWords(reference);
  const actual = Object.fromEntries(Object.entries(hypotheses).map(([id, text]) => [id, normalizeWords(text)]));
  if (!expected.length || !Object.keys(actual).length) throw new Error("nonempty reference and hypotheses required");
  if (expected.length > 10000 || Object.values(actual).some(words => words.length > 10000)) {
    throw new Error("bounded scorer accepts at most 10000 words per text");
  }
  if (Object.values(actual).some(words => (expected.length + 1) * (words.length + 1) > 25000000)) {
    throw new Error("bounded scorer accepts at most 25000000 alignment cells");
  }
  const accepted: Array<{ id: string; reason: string; left: string[]; right: string[];
    reference: Interval; hypotheses: Record<string, Interval> }> = [];
  const rejected: Array<{ id: string; reason: string }> = [];
  const ids = new Set<string>();
  for (const exclusion of exclusions) {
    if (!exclusion.id || ids.has(exclusion.id)) throw new Error("exclusion ids must be nonempty and unique");
    ids.add(exclusion.id);
    try {
      const left = normalizeWords(exclusion.left);
      const right = normalizeWords(exclusion.right);
      const span = normalizeWords(exclusion.span);
      if (left.length < 4 || right.length < 4 || !exclusion.reason.trim() || !span.length || span.length > 3) {
        throw new Error("require reason, >=4-word anchors and a 1-3-word reference span");
      }
      const ref = locate(expected, left, right, 3);
      if (ref.words.join(" ") !== span.join(" ")) throw new Error("reference span does not match declaration");
      const intervals = Object.fromEntries(Object.entries(actual).map(([id, words]) => [id, locate(words, left, right, 8)]));
      for (const previous of accepted) {
        const nonoverlap = (a: Interval, b: Interval) => a.end <= b.start || b.end <= a.start;
        if (!nonoverlap(ref, previous.reference)) throw new Error("overlapping reference spans");
        for (const id of Object.keys(actual)) {
          const current = intervals[id]!;
          const prior = previous.hypotheses[id]!;
          if (!nonoverlap(current, prior) || (ref.start < previous.reference.start) !== (current.start < prior.start)) {
            throw new Error("overlapping or inconsistent hypothesis span order");
          }
        }
      }
      accepted.push({ id: exclusion.id, reason: exclusion.reason, left, right, reference: ref, hypotheses: intervals });
    } catch (error) {
      // Reject for every model when even one model cannot be anchored safely.
      rejected.push({ id: exclusion.id, reason: error instanceof Error ? error.message : String(error) });
    }
  }
  accepted.sort((a, b) => a.reference.start - b.reference.start);
  const models = Object.fromEntries(Object.entries(actual).map(([id, words]) => {
    const segments: Array<{ reference: string[]; hypothesis: string[] }> = [];
    let refStart = 0;
    let hypStart = 0;
    for (const gap of accepted) {
      segments.push({ reference: expected.slice(refStart, gap.reference.start),
        hypothesis: words.slice(hypStart, gap.hypotheses[id]!.start) });
      refStart = gap.reference.end;
      hypStart = gap.hypotheses[id]!.end;
    }
    segments.push({ reference: expected.slice(refStart), hypothesis: words.slice(hypStart) });
    const formatted = segments.map(segment => ({ reference: normalizeFormatting(segment.reference, formattingRules),
      hypothesis: normalizeFormatting(segment.hypothesis, formattingRules) }));
    const formattingOnlyReference = normalizeFormatting(expected, formattingRules);
    const formattingOnlyHypothesis = normalizeFormatting(words, formattingRules);
    return [id, {
      raw: counts(expected, words),
      soundAdjusted: total(segments.map(s => counts(s.reference, s.hypothesis))),
      formattingOnly: counts(formattingOnlyReference.words, formattingOnlyHypothesis.words),
      soundAndFormattingAdjusted: total(formatted.map(s => counts(s.reference.words, s.hypothesis.words))),
      removedReferenceWords: accepted.reduce((n, gap) => n + gap.reference.words.length, 0),
      removedHypothesisWords: accepted.reduce((n, gap) => n + gap.hypotheses[id]!.words.length, 0),
      formattingChanges: formatted.map((s, segment) => ({ segment, reference: s.reference.changes,
        hypothesis: s.hypothesis.changes })),
      formattingOnlyChanges: { reference: formattingOnlyReference.changes, hypothesis: formattingOnlyHypothesis.changes },
    }];
  }));
  return { label: "EPUB agreement, not audio-verified WER", accepted, rejected, models };
}
