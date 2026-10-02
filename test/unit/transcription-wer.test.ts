import { expect, test } from "bun:test";
import { normalizeWords, wordErrorRate } from "../../scripts/transcription-eval/wer";

test("WER counts substitutions, deletions and insertions", () => {
  expect(wordErrorRate("one two three", "one four three").substitutions).toBe(1);
  expect(wordErrorRate("one two three", "one three").deletions).toBe(1);
  expect(wordErrorRate("one two three", "one two extra three").insertions).toBe(1);
  expect(wordErrorRate("one two three", "one two extra three").wer).toBeCloseTo(1 / 3);
});

test("WER ignores case and punctuation but retains accented letters and words", () => {
  expect(normalizeWords("DON’T touch décor!")).toEqual(["dont", "touch", "décor"]);
  expect(wordErrorRate("Don't stop.", "DON’T STOP!").wer).toBe(0);
  expect(() => wordErrorRate("", "speech")).toThrow();
  expect(wordErrorRate("one two", "").deletions).toBe(2);
});
