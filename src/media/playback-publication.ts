import path from "node:path";
import type { AppSettings } from "../app-types";
import { movieRoots } from "../movies/acquisition";
import { movieViewEntries } from "../movies/publication";
import type { Movie } from "../movies/types";
import type { BooksRepo } from "../repo";
import { renderTvFileEntry, tvRoots } from "../tv/publication";

export type PublicationFiles = ReadonlyMap<string, number>;
type Repo = Pick<BooksRepo, "movies" | "tv">;

function scoped<T extends object>(repo: T, overrides: Partial<T>): T {
  return new Proxy(repo, { get(target, property) {
    if (Object.hasOwn(overrides, property)) return Reflect.get(overrides, property);
    const value = Reflect.get(target, property);
    return typeof value === "function" ? value.bind(target) : value;
  } });
}

export function moviePlaybackFiles(repo: Repo, settings: AppSettings, movie: Movie): PublicationFiles {
  const roots = movieRoots(settings.movies.managedRoot, settings.movies.plexViewPath);
  const published = new Set(repo.movies.publishedVersionsForMovie(movie.id).map((version) => version.id));
  const files = new Map<string, number>();
  for (const target of repo.movies.targets(movie.id)) {
    if (target.active_version_id === null || !published.has(target.active_version_id)) continue;
    const version = repo.movies.version(target.active_version_id);
    if (version?.files.length !== 1) continue;
    const [entry, extra] = movieViewEntries(scoped(repo.movies, { targets: () => [target], publications: () => [] }), roots);
    const file = version.files[0]!;
    if (!entry || extra || entry.contentPath !== path.resolve(roots.contentRoot, file.relativePath)) continue;
    files.set(path.posix.join(roots.serverViewRoot, entry.relativePath), file.sizeBytes);
  }
  return files;
}

export function episodePlaybackFiles(repo: Repo, settings: AppSettings, entryId: number, onlyEpisodeId?: number): Map<number, PublicationFiles> {
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath);
  const files = new Map<number, Map<string, number>>();
  for (const row of repo.tv.playbackFiles(entryId, onlyEpisodeId)) {
    const view = renderTvFileEntry(row, roots, row.targetId, row.versionId, row.relativePath, [row]);
    const byPath = files.get(row.episodeId) ?? new Map<string, number>();
    byPath.set(path.posix.join(roots.serverViewRoot, view.relativePath), row.sizeBytes);
    files.set(row.episodeId, byPath);
  }
  return files;
}
