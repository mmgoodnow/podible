import path from "node:path";
import type { AppSettings } from "../app-types";
import { movieRoots } from "../movies/acquisition";
import { movieViewEntries } from "../movies/publication";
import type { Movie } from "../movies/types";
import type { BooksRepo } from "../repo";
import { tvRoots, tvViewEntries } from "../tv/publication";

export type PublicationFiles = ReadonlyMap<string, number>;
type Repo = Pick<BooksRepo, "movies" | "tv" | "contentLoss">;

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

export function episodePlaybackFiles(repo: Repo, settings: AppSettings, entryId: number): Map<number, PublicationFiles> {
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath);
  const files = new Map<number, Map<string, number>>();
  for (const target of repo.tv.targets(entryId)) {
    const active = repo.tv.activeEpisodes(target.id);
    if (!active.length) continue;
    const lost = new Set(repo.contentLoss.fileIds({ namespace: "tv-target", id: String(target.id) }));
    // Scope the actual renderer to active versions only; never use staged paths as evidence.
    const rendered = tvViewEntries(scoped(repo.tv, { targets: () => [target], publications: () => [], activeEpisodes: () => active }), roots);
    const byContent = new Map<string, string[]>();
    for (const entry of rendered) {
      const paths = byContent.get(entry.contentPath) ?? [];
      paths.push(path.posix.join(roots.serverViewRoot, entry.relativePath));
      byContent.set(entry.contentPath, paths);
    }
    const versions = new Map<number, ReturnType<BooksRepo["tv"]["version"]>>();
    for (const { episode_id, version_id } of active) {
      if (!versions.has(version_id)) versions.set(version_id, repo.tv.version(version_id));
      const version = versions.get(version_id);
      if (!version || version.entryId !== entryId || version.targetId !== target.id) continue;
      const matching = version.files.filter((file) => file.episodeIds.includes(episode_id));
      const file = matching[0];
      // Combined episodes have no verified per-episode byte/time boundaries.
      if (!file || matching.length !== 1 || file.episodeIds.length !== 1 || lost.has(file.id)) continue;
      const paths = byContent.get(path.resolve(roots.contentRoot, file.relativePath));
      if (paths?.length !== 1) continue;
      const byPath = files.get(episode_id) ?? new Map<string, number>();
      byPath.set(paths[0]!, file.sizeBytes);
      files.set(episode_id, byPath);
    }
  }
  return files;
}
