import { tool } from "@openai/agents";
import { z } from "zod";
import type { MovieOffer } from "../movies/offers";
import { modelEvidenceText } from "../library/decisions";

const indexesSchema = z.array(z.number().int().nonnegative().safe()).max(2000);
export const matchingIndexesSchema = indexesSchema.default([]);

export const releaseSelectionInstructions = `Group copies by same work, encode, cut, release group, language tracks and TV episode coverage; don't merge editions, remakes or partial/full packs. Similar title, size or hash alone doesn't prove equivalence; private copies can have different hashes.
Before using tracker coverage, call assessCrossSeedability with all known group indexes including the primary. It counts distinct normalized tracker names, not results. Reuse isn't verified and shared hashes may not add a registration.
Use tracker coverage only to break ties among viable releases with equal quality and episode coverage, never to justify an equal-quality upgrade. Prefer proper/repack among otherwise comparable releases. Within a group, prefer a lower known downloadVolumeFactor (0 = freeleech) at equal viability; unknown isn't freeleech and a dead source doesn't win.
Set matchingIndexes to all other known copies in the chosen group, including distinct ones on the same tracker, or [] for no group/index:null. Don't search just to boost tracker count.`;

function knownIndexes(offers: readonly MovieOffer[], indexes: readonly number[]) {
  const parsed = indexesSchema.parse(indexes);
  if (new Set(parsed).size !== parsed.length)
    throw new Error("Release selection indexes must be unique");
  if (parsed.some(index => !offers[index]))
    throw new Error("Release selection references an unknown index");
  return parsed;
}

export function matchingSelectionOffers(offers: readonly MovieOffer[], selectedIndex: number | null,
  matchingIndexes: readonly number[]): MovieOffer[] {
  const indexes = knownIndexes(offers, matchingIndexes);
  if (selectedIndex === null) {
    if (indexes.length) throw new Error("Matching releases require a selected release");
    return [];
  }
  knownIndexes(offers, [selectedIndex]);
  const seen = new Set([offers[selectedIndex]!.key]);
  return indexes.flatMap(index => {
    const offer = offers[index]!;
    if (seen.has(offer.key)) return [];
    seen.add(offer.key);
    return [offer];
  });
}

export function normalizedProviderName(provider: string): string {
  return provider.normalize("NFKC").trim().replace(/\s+/g, " ").toLowerCase();
}

/** Only recognized scalar Torznab attributes enter model evidence; never spread raw metadata. */
export function releaseDownloadVolumeFactor(offer: MovieOffer): number | null {
  const raw = offer.attributes.attr;
  const attributes = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const factors: number[] = [];
  for (const attribute of attributes) {
    if (!attribute || typeof attribute !== "object") continue;
    const { name, value } = attribute as Record<string, unknown>;
    if (typeof name !== "string") continue;
    const key = name.trim().toLowerCase();
    if (key === "downloadvolumefactor" || key === "downloadfactor") {
      if (typeof value !== "number" && (typeof value !== "string" || !value.trim())) continue;
      const factor = Number(value);
      if (Number.isFinite(factor) && factor >= 0) factors.push(factor);
    } else if (key === "freeleech") {
      const flag = typeof value === "string" ? value.trim().toLowerCase() : value;
      if (flag === true || flag === 1 || flag === "true" || flag === "1") factors.push(0);
      else if (flag === false || flag === 0 || flag === "false" || flag === "0") factors.push(1);
    }
  }
  // Conflicting flags are not authority to claim a cheaper download.
  return factors.length && factors.every(factor => factor === factors[0]) ? factors[0]! : null;
}

export function assessReleaseCrossSeedability(offers: readonly MovieOffer[], indexes: readonly number[]) {
  const group = knownIndexes(offers, indexes).map(index => ({ index, offer: offers[index]! }));
  const providers = group.map(({ offer }) => normalizedProviderName(offer.provider)).filter(Boolean);
  const sizes = group.flatMap(({ offer }) => Number.isSafeInteger(offer.sizeBytes) && offer.sizeBytes! >= 0 ? [offer.sizeBytes!] : []);
  const hashes = group.flatMap(({ offer }) => offer.infoHash && /^[a-f0-9]{40}$|^[a-f0-9]{64}$/i.test(offer.infoHash) ? [offer.infoHash.toLowerCase()] : []);
  return {
    distinctTrackerCount: new Set(providers).size,
    unknownProviderCount: group.length - providers.length,
    offers: group.map(({ index, offer }) => ({ index, provider: modelEvidenceText(offer.provider, 80), sizeBytes: offer.sizeBytes,
      downloadVolumeFactor: releaseDownloadVolumeFactor(offer) })),
    sizeEvidence: { knownCount: sizes.length, distinctCount: new Set(sizes).size, unknownCount: group.length - sizes.length },
    hashEvidence: { knownCount: hashes.length, distinctCount: new Set(hashes).size,
      duplicateIdentityCount: hashes.length - new Set(hashes).size, unknownCount: group.length - hashes.length },
    diagnostic: "Estimated availability of an agent-proposed semantic group, not verified byte reuse or new registrations. Equal sizes/hashes do not prove matching contents; different hashes can reflect tracker-specific metainfo. Runtime may skip an already-owned infoHash even when listed by distinct trackers.",
  };
}

export function createCrossSeedabilityTool(offers: () => readonly MovieOffer[], maximumIndexes = 2000) {
  return tool({
    name: "assessCrossSeedability",
    description: "Assess a proposed semantic same-release group using distinct tracker names and safe size/hash/download-volume diagnostics. No network requests or downloads.",
    parameters: z.object({ indexes: indexesSchema.min(1).max(maximumIndexes) }),
    async execute({ indexes }) {
      try { return assessReleaseCrossSeedability(offers(), indexes); }
      catch { return { error: "Supply unique known release indexes for one semantic group." }; }
    },
  });
}
