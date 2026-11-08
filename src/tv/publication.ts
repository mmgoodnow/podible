import path from "node:path";
import { withKeyedMutex } from "../runtime/keyed-mutex";
import { reconcileLibraryView, type LibraryViewEntry } from "../media/library-view";
import type { TvRepo } from "./repo";
import type { TvEntry, TvEpisode, TvVersion } from "./types";

export type TvRoots = { contentRoot: string; viewRoot: string; serverViewRoot: string };
export type TvPublishedFile = { serverPath: string; episodes: TvEpisode[] };
export interface TvLibrary {
  scan(): Promise<void>;
  verify(entry: TvEntry, files: TvPublishedFile[]): Promise<boolean>;
}
export function tvRoots(root: string, serverViewRoot: string): TvRoots {
  if (!path.isAbsolute(root) || !path.isAbsolute(serverViewRoot)) throw new Error("TV roots must be absolute");
  return { contentRoot: path.join(root, "objects"), viewRoot: path.join(root, "views", "plex", "tv"), serverViewRoot };
}
async function serialized<T>(root: string, action: () => Promise<T>): Promise<T> {
  return withKeyedMutex({ kind: "publication", key: path.resolve(root) }, action);
}
function safe(value: string) {
  return value.normalize("NFC").replace(/[<>:"/\\|?*\x00-\x1f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 90) || "Series";
}
export function tvdbAiredNumber(episode: TvEpisode) {
  const number = episode.numbering.find((item) => item.scheme === "tvdb:aired");
  if (!number || number.season === null || number.episode === null) throw new Error("A verified TVDB aired mapping is required for this Plex view");
  return { season: number.season, episode: number.episode };
}
function versionEntries(repo: TvRepo, roots: TvRoots, targetId: number, version: TvVersion): Array<LibraryViewEntry & { episodes: TvEpisode[] }> {
  const target = repo.target(targetId)!;
  const entry = repo.entry(target.entry_id)!;
  if (!entry.tvdbId) throw new Error("Plex TV publication needs an explicit TVDB series identity");
  const title = safe(entry.title);
  const folder = `${title}${entry.year ? ` (${entry.year})` : ""} {tvdb-${entry.tvdbId}}`;
  return version.files.map((file) => {
    const episodes = file.episodeIds.map((id) => repo.episode(id, entry.id)!);
    if (episodes.some((episode) => !episode)) throw new Error("TV file references an unknown episode");
    const coordinates = episodes.map(tvdbAiredNumber).sort((a, b) => a.season - b.season || a.episode - b.episode);
    const first = coordinates[0]!;
    if (coordinates.some((coordinate, index) => coordinate.season !== first.season || coordinate.episode !== first.episode + index))
      throw new Error("A multi-episode Plex file requires contiguous provider episodes in one season");
    const code = `S${String(first.season).padStart(2, "0")}E${String(first.episode).padStart(2, "0")}` +
      (coordinates.length > 1 ? `-E${String(coordinates.at(-1)!.episode).padStart(2, "0")}` : "");
    const extension = path.extname(file.relativePath).toLowerCase();
    if (![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(extension)) throw new Error("Unsupported TV container");
    const contentPath = path.resolve(roots.contentRoot, file.relativePath);
    if (!contentPath.startsWith(path.resolve(roots.contentRoot) + path.sep)) throw new Error("TV file escaped its canonical root");
    return { relativePath: `${folder}/Season ${String(first.season).padStart(2, "0")}/${title} - ${code} - t${targetId}v${version.id}${extension}`,
      contentPath, episodes };
  });
}
/** The provider's Plex projection is replaceable; it never establishes episode identity. */
export function tvViewEntries(repo: TvRepo, roots: TvRoots): LibraryViewEntry[] {
  const selected = new Map<string, { targetId: number; versionId: number }>();
  const pending = repo.publications();
  for (const target of repo.targets()) {
    const active = repo.activeEpisodes(target.id);
    const staged = pending.find((item) => item.target_id === target.id);
    for (const row of active) selected.set(`${target.id}:${row.version_id}`, { targetId: target.id, versionId: row.version_id });
    if (staged) selected.set(`${target.id}:${staged.version_id}`, { targetId: target.id, versionId: staged.version_id });
  }
  return [...selected.values()].flatMap(({ targetId, versionId }) => {
    const staged = pending.find((item) => item.target_id === targetId);
    if (staged?.version_id === versionId) return versionEntries(repo, roots, targetId, repo.version(versionId)!);
    const active = new Set(repo.activeEpisodes(targetId).filter((item) => item.version_id === versionId).map((item) => item.episode_id));
    const version = repo.version(versionId)!;
    const files = version.files.filter((file) => file.episodeIds.some((id) => active.has(id)));
    if (files.some((file) => file.episodeIds.some((id) => !active.has(id))))
      throw new Error("Cannot partially replace a combined episode file");
    return versionEntries(repo, roots, targetId, { ...version, files });
  });
}
export function reconcileTvView(repo: TvRepo, roots: TvRoots, assertActive: () => void = () => {}) {
  return serialized(roots.viewRoot, () => reconcileLibraryView({ ...roots, entries: tvViewEntries(repo, roots), assertActive }));
}
export async function publishTvVersion(input: { repo: TvRepo; roots: TvRoots; library: TvLibrary; targetId: number; versionId: number; assertActive?: () => void }) {
  const { repo, roots, targetId, versionId, library } = input;
  const active = input.assertActive ?? (() => {});
  return serialized(roots.viewRoot, async () => {
    active(); repo.stagePublication(targetId, versionId);
    const pending = repo.publications().find((item) => item.target_id === targetId)!;
    await reconcileLibraryView({ ...roots, entries: tvViewEntries(repo, roots), assertActive: active });
    active();
    const entry = repo.entry(repo.target(targetId)!.entry_id)!;
    const files = versionEntries(repo, roots, targetId, repo.version(versionId)!).map((file) => ({
      serverPath: path.posix.join(roots.serverViewRoot, file.relativePath), episodes: file.episodes,
    }));
    await library.scan();
    if (!await library.verify(entry, files)) throw new Error("Plex has not verified the exact TV episodes and paths; existing episodes remain selected");
    active(); repo.promotePublication(targetId, versionId, pending.expected_revision);
    active(); repo.clearPublication(targetId, versionId);
    await reconcileLibraryView({ ...roots, entries: tvViewEntries(repo, roots), assertActive: active });
    repo.event(targetId, null, `tv-published:${targetId}:${versionId}`, "available", "Episode identity and file paths verified in Plex");
  });
}
