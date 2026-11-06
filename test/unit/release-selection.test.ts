import { expect, test } from "bun:test";
import { assessReleaseCrossSeedability, matchingSelectionOffers, releaseDownloadVolumeFactor } from "../../src/media/release-selection";
import type { MovieOffer } from "../../src/movies/offers";

const offer: MovieOffer = { key: "first", title: "A semantic release", provider: " Tracker A ", sizeBytes: 1000,
  seeders: 3, url: "https://private.invalid/?passkey=hidden", infoHash: "a".repeat(40), attributes: {} };

test("cross-seed estimates count normalized tracker names, not duplicate search results or hashes", () => {
  const offers = [offer, { ...offer, key: "other-upload", provider: "TRACKER\u00a0A", infoHash: "b".repeat(40) },
    { ...offer, key: "third", provider: "Tracker B", infoHash: "c".repeat(40), sizeBytes: 1001 },
    { ...offer, key: "unknown", provider: "  ", infoHash: null, sizeBytes: null }];
  const evidence = assessReleaseCrossSeedability(offers, [0, 1, 2, 3], "tie-break");
  expect(evidence).toMatchObject({ distinctTrackerCount: 2, unknownProviderCount: 1,
    sizeEvidence: { knownCount: 3, distinctCount: 2, unknownCount: 1 },
    hashEvidence: { knownCount: 3, distinctCount: 3, unknownCount: 1 } });
  expect(evidence.diagnostic).toContain("not verified byte reuse");
  expect(JSON.stringify(evidence)).not.toContain("hidden");
  expect(assessReleaseCrossSeedability(offers, [0, 1], "off")).toMatchObject({ preference: "off", distinctTrackerCount: 1 });
});

test("same-sized unrelated titles and identical hashes are diagnostics, never automatic semantic grouping", () => {
  const offers = [offer, { ...offer, key: "unrelated", title: "A different film", provider: "B" }];
  expect(assessReleaseCrossSeedability(offers, [0, 1], "tie-break")).toMatchObject({
    distinctTrackerCount: 2, sizeEvidence: { distinctCount: 1 }, hashEvidence: { distinctCount: 1, duplicateIdentityCount: 1 } });
  expect(assessReleaseCrossSeedability(offers, [0, 1], "tie-break").diagnostic).toContain("Runtime may skip an already-owned infoHash");
  expect(matchingSelectionOffers(offers, 0, [])).toEqual([]);
});

test("matching alternatives preserve distinct same-tracker offers and resolve only server-owned indexes", () => {
  const offers = [offer, { ...offer, key: "other-upload" }, { ...offer, key: "elsewhere", provider: "B" },
    { ...offer, title: "Duplicate of selected" }];
  const alternatives = matchingSelectionOffers(offers, 0, [0, 1, 2, 3]);
  expect(alternatives).toEqual([offers[1]!, offers[2]!]);
  expect(alternatives[0]).toBe(offers[1]!);
  expect(() => matchingSelectionOffers(offers, 0, [1, 1])).toThrow("unique");
  expect(() => matchingSelectionOffers(offers, 0, [99])).toThrow("unknown");
  expect(() => matchingSelectionOffers(offers, 99, [])).toThrow("unknown");
  expect(() => matchingSelectionOffers(offers, null, [1])).toThrow("require a selected");
  expect(matchingSelectionOffers(offers, null, [])).toEqual([]);
  expect(() => assessReleaseCrossSeedability(offers, [0, 0], "off")).toThrow("unique");
  expect(() => assessReleaseCrossSeedability(offers, [4], "tie-break")).toThrow("unknown");
});

test("download-volume evidence accepts only whitelisted scalar Torznab attributes", () => {
  const factor = (attr: unknown) => releaseDownloadVolumeFactor({ ...offer, attributes: {
    attr, description: "freeleech passkey=hidden", downloadvolumefactor: 0,
  } });
  expect(factor({ name: "downloadvolumefactor", value: "0.5" })).toBe(0.5);
  expect(factor([{ name: "downloadfactor", value: 0 }])).toBe(0);
  expect(factor({ name: "FreeLeech", value: "true" })).toBe(0);
  expect(factor({ name: "freeleech", value: false })).toBe(1);
  expect(factor({ name: "freeleech", value: "1" })).toBe(0);
  for (const attr of [undefined, { name: "description", value: "freeleech" },
    { name: "downloadvolumefactor", value: "hidden" }, { name: "downloadvolumefactor", value: -1 },
    { name: "downloadvolumefactor", value: Infinity }, { name: "downloadvolumefactor", value: "" },
    { name: "downloadvolumefactor", value: true }, { name: "downloadvolumefactor", value: {} },
    { name: "freeleech", value: "100%" }, [{ name: "freeleech", value: true }, { name: "downloadfactor", value: 1 }]]) {
    expect(factor(attr)).toBeNull();
  }
  const evidence = assessReleaseCrossSeedability([{ ...offer, attributes: {
    attr: [{ name: "downloadfactor", value: 0 }, { name: "private", value: "hidden" }], description: "hidden",
  } }], [0], "tie-break");
  expect(evidence.offers[0]?.downloadVolumeFactor).toBe(0);
  expect(JSON.stringify(evidence)).not.toContain("hidden");
});
