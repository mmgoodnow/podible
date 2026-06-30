import { describe, expect, test } from "bun:test";

import { inferLanguageFromReleaseTitles, normalizeManifestationLanguageCode } from "../../src/library/language";

describe("manifestation language inference", () => {
  test("normalizes common release language tags", () => {
    expect(normalizeManifestationLanguageCode("ENG")).toBe("en");
    expect(normalizeManifestationLanguageCode("spa")).toBe("es");
    expect(normalizeManifestationLanguageCode("pt-BR")).toBe("pt");
    expect(normalizeManifestationLanguageCode("unknown")).toBe(null);
  });

  test("infers unambiguous language from bracketed release metadata", () => {
    expect(inferLanguageFromReleaseTitles(["A Conjuring of Light by V.E. Schwab [ENG / M4B]"])).toBe("en");
    expect(inferLanguageFromReleaseTitles(["Freakonomics by Steven D. Levitt [SPA / MP3]"])).toBe("es");
  });

  test("does not treat ordinary short title words as language tags", () => {
    expect(inferLanguageFromReleaseTitles(["It by Stephen King M4B"])).toBe(null);
  });

  test("returns null for conflicting release language metadata", () => {
    expect(inferLanguageFromReleaseTitles(["Book [ENG / M4B]", "Book [SPA / MP3]"])).toBe(null);
  });
});
