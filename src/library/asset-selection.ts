import type { AssetRow, ManifestationRow } from "../app-types";

function assetMediaType(asset: AssetRow): "audio" | "ebook" {
  return asset.kind === "ebook" ? "ebook" : "audio";
}

function scoreAudioAsset(asset: AssetRow): number {
  let score = 0;
  if (asset.kind === "single" && asset.mime === "audio/mp4") score += 100;
  if (asset.kind === "single" && asset.mime === "audio/mpeg") score += 80;
  if (asset.kind === "multi") score += 60;
  score += Math.min(50, Math.trunc((asset.duration_ms ?? 0) / 60_000));
  return score;
}

export function selectPreferredAudioAsset(assets: AssetRow[]): AssetRow | null {
  const audio = assets.filter((asset) => assetMediaType(asset) === "audio");
  if (audio.length === 0) return null;
  return [...audio].sort((a, b) => {
    const score = scoreAudioAsset(b) - scoreAudioAsset(a);
    if (score !== 0) return score;
    if (a.created_at !== b.created_at) return b.created_at.localeCompare(a.created_at);
    return b.id - a.id;
  })[0] ?? null;
}

// Score a manifestation by aggregating its containers. For a single-container
// audio manifestation this matches scoreAudioAsset of that one container,
// preserving today's selection ordering. Multi-container manifestations sum
// their containers' scores so a 2-part GraphicAudio release reads as "more
// substantial" than a single-part Audible release of the same book — but the
// per-container heuristics (m4b > mp3) still dominate for typical libraries.
function scoreAudioManifestation(containers: AssetRow[]): number {
  if (containers.length === 0) return 0;
  let score = 0;
  for (const container of containers) {
    score += scoreAudioAsset(container);
  }
  // Small bonus for being a single-container manifestation, since today's
  // libraries are 99% single-container and we want behavior continuity.
  if (containers.length === 1) score += 1;
  return score;
}

export type ManifestationWithContainers = {
  manifestation: ManifestationRow;
  containers: AssetRow[];
};

export function selectPreferredAudioManifestation(
  manifestations: ManifestationWithContainers[]
): ManifestationWithContainers | null {
  const audio = manifestations.filter((entry) => entry.manifestation.kind === "audio" && entry.containers.length > 0);
  if (audio.length === 0) return null;
  return [...audio].sort((a, b) => {
    const aScore = scoreAudioManifestation(a.containers) + a.manifestation.preferred_score;
    const bScore = scoreAudioManifestation(b.containers) + b.manifestation.preferred_score;
    if (aScore !== bScore) return bScore - aScore;
    if (a.manifestation.created_at !== b.manifestation.created_at) {
      return b.manifestation.created_at.localeCompare(a.manifestation.created_at);
    }
    return b.manifestation.id - a.manifestation.id;
  })[0] ?? null;
}
