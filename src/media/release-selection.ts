import { tool } from "@openai/agents";
import { z } from "zod";
import type { MovieOffer } from "../movies/offers";
import type { MovieSettings } from "../movies/settings";

const indexesSchema = z.array(z.number().int().nonnegative().safe()).max(2000);
export const matchingIndexesSchema = indexesSchema.default([]);

export const releaseSelectionInstructions = `Group likely matching copies semantically: the same work, encode, cut, release group, language tracks and (for TV) episode coverage. Similar titles, sizes or hashes alone do not establish a group; do not merge different encodes, editions, remakes, seasons or partial/complete packs.
Call assessCrossSeedability with all known indexes in each likely group (including the candidate to download) before using tracker coverage to choose. It counts distinct normalized tracker names, not results. Sizes and hashes are diagnostics, not proof that files can be reused; private torrents may have different hashes for the same media.
Tracker listings sharing an infoHash are not proof of additional torrent registrations or new seeding coverage; runtime may skip an already-owned identity. Do not promise coverage from the estimate.
When selectionPreference is "tie-break", prefer greater estimated tracker coverage only among viable choices with equal quality and requested coverage. Hard constraints, quality tiers/preferences, identity and episode coverage always win. Within the chosen semantic group, prefer a lower known downloadVolumeFactor (0 means freeleech) when source viability is otherwise equal. Unknown factors are unknown, not freeleech. Never pick a dead source merely for freeleech.
When selectionPreference is "off", do not rank by tracker coverage or download volume factors, but still record any matching group you identify.
Return matchingIndexes for ALL other known copies in the chosen semantic group, including genuinely distinct copies on the same tracker. Do not return merely similar or unrelated results. Return [] if no group is known or index:null. Do not perform extra searches solely to increase tracker counts. Estimates are not verified cross-seeds and must not justify an equal-quality upgrade.`;

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

export function assessReleaseCrossSeedability(offers: readonly MovieOffer[], indexes: readonly number[],
  preference: MovieSettings["crossSeedPreference"]) {
  const group = knownIndexes(offers, indexes).map(index => ({ index, offer: offers[index]! }));
  const providers = group.map(({ offer }) => normalizedProviderName(offer.provider)).filter(Boolean);
  const sizes = group.flatMap(({ offer }) => Number.isSafeInteger(offer.sizeBytes) && offer.sizeBytes! >= 0 ? [offer.sizeBytes!] : []);
  const hashes = group.flatMap(({ offer }) => offer.infoHash && /^[a-f0-9]{40}$|^[a-f0-9]{64}$/i.test(offer.infoHash) ? [offer.infoHash.toLowerCase()] : []);
  return {
    preference,
    distinctTrackerCount: new Set(providers).size,
    unknownProviderCount: group.length - providers.length,
    offers: group.map(({ index, offer }) => ({ index, provider: offer.provider, sizeBytes: offer.sizeBytes,
      downloadVolumeFactor: releaseDownloadVolumeFactor(offer) })),
    sizeEvidence: { knownCount: sizes.length, distinctCount: new Set(sizes).size, unknownCount: group.length - sizes.length },
    hashEvidence: { knownCount: hashes.length, distinctCount: new Set(hashes).size,
      duplicateIdentityCount: hashes.length - new Set(hashes).size, unknownCount: group.length - hashes.length },
    diagnostic: "Estimated availability of an agent-proposed semantic group, not verified byte reuse or new registrations. Equal sizes/hashes do not prove matching contents; different hashes can reflect tracker-specific metainfo. Runtime may skip an already-owned infoHash even when listed by distinct trackers.",
  };
}

export function createCrossSeedabilityTool(offers: () => readonly MovieOffer[],
  preference: MovieSettings["crossSeedPreference"]) {
  return tool({
    name: "assessCrossSeedability",
    description: "Assess a proposed semantic same-release group using distinct tracker names and safe size/hash/download-volume diagnostics. No network requests or downloads.",
    parameters: z.object({ indexes: indexesSchema.min(1) }),
    async execute({ indexes }) {
      try { return assessReleaseCrossSeedability(offers(), indexes, preference); }
      catch { return { error: "Supply unique known release indexes for one semantic group." }; }
    },
  });
}
