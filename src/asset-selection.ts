import type { AssetRow } from "./app-types";

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
