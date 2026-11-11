import path from "node:path";
import type { BooksRepo } from "../repo";
import { captureReuseFile, captureReuseRoot } from "../media/reuse-layout";
import { planContentReuse, type ReuseSourceFile } from "../media/reuse-plan";
import type { TorrentSnapshot } from "../media/torrent-client";
import { playableTvMember, type TvSeasonSelection } from "./season-promotion";

const minReuseRatio = 0.5;

export async function planSeasonPackPrefill(input: {
  repo: BooksRepo; contentRoot: string; selection: TvSeasonSelection;
  metadata: { infoHash: string; files: { path: string; size: number }[] }; active: () => void;
}): Promise<Array<[string, ReuseSourceFile]>> {
  const { repo, contentRoot, selection, metadata, active } = input;
  const root = await captureReuseRoot({ rootPath: contentRoot, markerName: ".podible-content.json" });
  const selectedVersion = new Map(selection.selections.map(row => [row.episode_id, row.version_id]));
  const sizes = new Set(metadata.files.filter(file => playableTvMember(file.path)).map(file => file.size));
  const sources = new Map<string, ReuseSourceFile>();
  for (const versionId of new Set(selectedVersion.values())) {
    for (const file of repo.tv.version(versionId)?.files ?? []) {
      if (!file.episodeIds.length || !file.episodeIds.every(id => selectedVersion.get(id) === versionId) ||
          sources.has(file.id) || !sizes.has(file.sizeBytes) || repo.releaseHealth.isContentFileBlocked(file.id)) continue;
      const source = await captureReuseFile({ root, sourceId: file.id, sourcePath: path.join(root.path, file.relativePath) })
        .catch((error: NodeJS.ErrnoException) => { if (error.code === "ENOENT") return null; throw error; });
      active();
      if (source?.size === file.sizeBytes) sources.set(file.id, source);
    }
  }
  const plan = planContentReuse({ candidate: { identity: { v1: metadata.infoHash }, files: metadata.files.map(file => ({
    ...file, role: playableTvMember(file.path) ? "content" : "sidecar",
  })) }, sources: [...sources.values()], policy: { version: 1, minReuseRatio, maxExtraDownloadBytes: Number.MAX_SAFE_INTEGER } });
  if (plan.status === "rejected") return [];
  return plan.files.flatMap(file => file.source ? [[file.candidatePath, file.source] as [string, ReuseSourceFile]] : []);
}

export function hasVerifiedSeasonPackReuse(seen: TorrentSnapshot, size: number) {
  return size > 0 && seen.sizeBytes === size && seen.uncheckedBytes === 0 &&
    Number.isSafeInteger(seen.verifiedBytes) && seen.verifiedBytes! <= size && seen.verifiedBytes! >= Math.ceil(size * minReuseRatio) &&
    !seen.seedOnly && !seen.integrityError && (!seen.errorKind || seen.errorKind === "tracker") && seen.state !== "checking";
}

export function hasVerifiedLocalSeasonPackPrefill(seen: TorrentSnapshot, size: number) {
  return seen.state === "inactive" && seen.downloadedEver === 0 && hasVerifiedSeasonPackReuse(seen, size);
}
