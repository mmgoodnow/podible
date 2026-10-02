export function normalizeWords(text: string): string[] {
  return text.normalize("NFKC").toLowerCase().replace(/[’']/g, "")
    .match(/[\p{L}\p{N}]+/gu) ?? [];
}

export function wordErrorRate(reference: string, hypothesis: string) {
  const expected = normalizeWords(reference);
  const actual = normalizeWords(hypothesis);
  if (!expected.length) throw new Error("WER requires a nonempty reference");
  const width = actual.length + 1;
  const distances = new Uint32Array((expected.length + 1) * width);
  for (let i = 0; i <= expected.length; i++) distances[i * width] = i;
  for (let j = 0; j <= actual.length; j++) distances[j] = j;
  for (let i = 1; i <= expected.length; i++) {
    for (let j = 1; j <= actual.length; j++) {
      distances[i * width + j] = Math.min(
        distances[(i - 1) * width + j - 1]! + Number(expected[i - 1] !== actual[j - 1]),
        distances[(i - 1) * width + j]! + 1,
        distances[i * width + j - 1]! + 1,
      );
    }
  }
  let i = expected.length;
  let j = actual.length;
  const errors: Array<{ type: "substitution" | "deletion" | "insertion"; expected?: string; actual?: string }> = [];
  while (i || j) {
    const distance = distances[i * width + j]!;
    if (i && j && distance === distances[(i - 1) * width + j - 1]! + Number(expected[i - 1] !== actual[j - 1])) {
      if (expected[i - 1] !== actual[j - 1]) errors.push({ type: "substitution", expected: expected[i - 1], actual: actual[j - 1] });
      i--; j--;
    } else if (i && distance === distances[(i - 1) * width + j]! + 1) {
      errors.push({ type: "deletion", expected: expected[--i] });
    } else {
      errors.push({ type: "insertion", actual: actual[--j] });
    }
  }
  const substitutions = errors.filter(e => e.type === "substitution").length;
  const deletions = errors.filter(e => e.type === "deletion").length;
  const insertions = errors.filter(e => e.type === "insertion").length;
  return { referenceWords: expected.length, hypothesisWords: actual.length, substitutions, deletions, insertions,
    wer: (substitutions + deletions + insertions) / expected.length, errors: errors.reverse() };
}
