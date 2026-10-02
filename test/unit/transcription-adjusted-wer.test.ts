import { expect, test } from "bun:test";
import { adjustedEpubAgreement, normalizeFormatting, type Exclusion } from "../../scripts/transcription-eval/adjusted-wer";

const exclusion: Exclusion = { id: "groan", left: "the bright light hurt", span: "grr",
  right: "i said before sleeping", reason: "Reference independently identifies an unintelligible vocalization" };
const reference = "the bright light hurt grr i said before sleeping";

test("excludes independently anchored reference and both hypothesis gaps, including empty gaps", () => {
  const result = adjustedEpubAgreement(reference, {
    first: "the bright light hurt oh no i said before sleeping",
    second: "the bright light hurt i said before sleeping",
  }, [exclusion]);
  expect(result.accepted).toHaveLength(1);
  expect(result.models.first!.raw.errors).toBe(2);
  expect(result.models.first!.soundAdjusted.errors).toBe(0);
  expect(result.models.first!.soundAdjusted.referenceWords).toBe(8);
  expect(result.models.first!.removedHypothesisWords).toBe(2);
  expect(result.models.second!.removedHypothesisWords).toBe(0);
  expect(result.accepted[0]!.reference).toEqual({ start: 4, end: 5, words: ["grr"] });
});

test("one missing, repeated, reversed or oversized anchor rejects exclusion for every model", () => {
  for (const bad of ["the light hurt i said before sleeping", `${reference} ${reference}`,
    "i said before sleeping the bright light hurt",
    "the bright light hurt a b c d e f g h i i said before sleeping"]) {
    const result = adjustedEpubAgreement(reference, { first: reference, second: bad }, [exclusion]);
    expect(result.accepted).toHaveLength(0);
    expect(result.rejected).toHaveLength(1);
    expect(result.models.first!.soundAdjusted).toEqual(result.models.first!.raw);
    expect(result.models.second!.soundAdjusted).toEqual(result.models.second!.raw);
  }
});

test("rejects undeclared or overly broad reference gaps and duplicate ids", () => {
  expect(adjustedEpubAgreement(reference, { first: reference }, [{ ...exclusion, span: "unknown" }]).rejected).toHaveLength(1);
  expect(adjustedEpubAgreement(reference, { first: reference }, [{ ...exclusion, left: "light hurt" }]).rejected).toHaveLength(1);
  expect(() => adjustedEpubAgreement(reference, { first: reference }, [exclusion, exclusion])).toThrow("unique");
  const overlap = adjustedEpubAgreement(reference, { first: reference }, [exclusion, { ...exclusion, id: "duplicate-span" }]);
  expect(overlap.accepted).toHaveLength(1);
  expect(overlap.rejected[0]!.reason).toBe("overlapping reference spans");
});

test("no automatic removal of unfamiliar names, lexical interjections or stammers", () => {
  const result = adjustedEpubAgreement("ow pfft borant j john four", { first: "oh borrent john four" }, []);
  expect(result.accepted).toHaveLength(0);
  expect(result.models.first!.soundAdjusted).toEqual(result.models.first!.raw);
  expect(result.models.first!.soundAdjusted.errors).toBeGreaterThan(0);
});

test("formatting is symmetric, audited, longest-match-first and changes its denominator explicitly", () => {
  const result = adjustedEpubAgreement("23 couldve stopped", { first: "twenty three could have stopped" }, [], [
    { id: "number", canonical: "twenty three", variants: ["23"] },
    { id: "contraction", canonical: "could have", variants: ["couldve"] },
  ]);
  expect(result.models.first!.raw.referenceWords).toBe(3);
  expect(result.models.first!.soundAndFormattingAdjusted.referenceWords).toBe(5);
  expect(result.models.first!.soundAndFormattingAdjusted.errors).toBe(0);
  expect(result.models.first!.formattingChanges[0]!.reference).toHaveLength(2);
  expect(normalizeFormatting(["3", "000"], [
    { id: "single", canonical: "three", variants: ["3"] },
    { id: "thousands", canonical: "three thousand", variants: ["3 000"] },
  ]).words).toEqual(["three", "thousand"]);
});

test("formatting preserves homophones, morphology and partial lexical attempts without explicit rules", () => {
  const result = adjustedEpubAgreement("to liked fffr four", { first: "two like four" }, []);
  expect(result.models.first!.soundAndFormattingAdjusted).toEqual(result.models.first!.raw);
  expect(() => normalizeFormatting(["a"], [{ id: "bad", canonical: "", variants: ["a"] }])).toThrow();
  expect(() => normalizeFormatting(["a"], [{ id: "bad", canonical: "b", variants: ["a", "a"] }])).toThrow();
});

test("bounds memory and requires nonempty data", () => {
  expect(() => adjustedEpubAgreement("", { first: "word" }, [])).toThrow();
  expect(() => adjustedEpubAgreement("word", {}, [])).toThrow();
  expect(() => adjustedEpubAgreement("word ".repeat(10001), { first: "word" }, [])).toThrow("bounded");
  expect(() => adjustedEpubAgreement("word ".repeat(5000), { first: "word ".repeat(5000) }, [])).toThrow("alignment cells");
});

test("whole-clip hypotheses retain leading omissions rather than searching for later endpoints", () => {
  const result = adjustedEpubAgreement("the opening words are missing but the rest is here",
    { first: "but the rest is here", second: "the opening words are missing but the rest is here" }, []);
  expect(result.models.first!.raw.deletions).toBe(5);
  expect(result.models.first!.soundAndFormattingAdjusted.deletions).toBe(5);
  expect(result.models.first!.soundAndFormattingAdjusted.referenceWords).toBe(10);
});

test("formatting never bridges an excluded sound gap", () => {
  const result = adjustedEpubAgreement("the bright light hurt grr i said before sleeping", {
    first: "the bright light hurt oh i said before sleeping",
  }, [exclusion], [{ id: "cross-gap", canonical: "changed", variants: ["hurt i"] }]);
  expect(result.models.first!.soundAndFormattingAdjusted.referenceWords).toBe(8);
  expect(result.models.first!.formattingChanges.every(s => !s.reference.length && !s.hypothesis.length)).toBe(true);
});
