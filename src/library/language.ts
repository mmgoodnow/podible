const LANGUAGE_ALIASES = new Map<string, string>([
  ["da", "da"],
  ["dan", "da"],
  ["danish", "da"],
  ["de", "de"],
  ["deu", "de"],
  ["ger", "de"],
  ["german", "de"],
  ["en", "en"],
  ["eng", "en"],
  ["english", "en"],
  ["es", "es"],
  ["spa", "es"],
  ["spanish", "es"],
  ["fr", "fr"],
  ["fra", "fr"],
  ["fre", "fr"],
  ["french", "fr"],
  ["it", "it"],
  ["ita", "it"],
  ["italian", "it"],
  ["ja", "ja"],
  ["jpn", "ja"],
  ["japanese", "ja"],
  ["nl", "nl"],
  ["dut", "nl"],
  ["dutch", "nl"],
  ["pl", "pl"],
  ["pol", "pl"],
  ["polish", "pl"],
  ["pt", "pt"],
  ["pt-br", "pt"],
  ["por", "pt"],
  ["portuguese", "pt"],
]);

export function normalizeManifestationLanguageCode(value: string | null | undefined): string | null {
  const normalized = value?.trim().toLowerCase().replace(/_/g, "-") ?? "";
  if (!normalized) return null;
  if (/^[a-z]{2}$/u.test(normalized)) return normalized;
  const localeMatch = normalized.match(/^([a-z]{2})-[a-z0-9]+$/u);
  if (localeMatch?.[1]) return localeMatch[1];
  return LANGUAGE_ALIASES.get(normalized) ?? null;
}

export function inferLanguageFromReleaseTitles(titles: string[]): string | null {
  const candidates = new Set<string>();
  for (const title of titles) {
    const lower = title.toLowerCase();
    const bracketedTokens = Array.from(lower.matchAll(/[\[(]([^\])]+)[\])]/gu)).flatMap(
      (match) => match[1]?.split(/[^a-z0-9-]+/u) ?? []
    );
    for (const token of bracketedTokens.filter(Boolean)) {
      const language = normalizeManifestationLanguageCode(token);
      if (language) candidates.add(language);
    }
    for (const token of lower.split(/[^a-z0-9-]+/u).filter(Boolean)) {
      if (/^[a-z]{2}$/u.test(token)) continue;
      const language = normalizeManifestationLanguageCode(token);
      if (language) candidates.add(language);
    }
  }
  return candidates.size === 1 ? Array.from(candidates)[0]! : null;
}
