import path from "node:path";
import { measureView } from "../media/acquisition-latency";
import type { MovieRepo } from "./repo";
import type { Movie } from "./types";
import { withKeyedMutex } from "../runtime/keyed-mutex";
import {
  reconcileLibraryView,
  type LibraryViewEntry,
} from "../media/library-view";

export interface MovieLibrary {
  scan(paths?: string[]): Promise<void>;
  verify(movie: Movie, serverVisiblePaths: string[]): Promise<boolean>;
}
export type MovieViewRoots = {
  contentRoot: string;
  viewRoot: string;
  serverViewRoot: string;
};

async function serialized<T>(
  key: string,
  action: () => Promise<T>,
): Promise<T> {
  return withKeyedMutex({ kind: "publication", key }, action);
}
function safeTitle(value: string) {
  return (
    value
      .normalize("NFC")
      .replace(/[<>:"/\\|?*\x00-\x1f]/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100) || "Movie"
  );
}
/** A pure desired-state renderer: code/naming changes are applied by reconciliation. */
export function movieViewEntries(
  repo: MovieRepo,
  roots: MovieViewRoots,
): LibraryViewEntry[] {
  const versions = new Map<string, { targetId: number; versionId: number }>();
  for (const target of repo.targets())
    if (target.active_version_id)
      versions.set(`${target.id}:${target.active_version_id}`, {
        targetId: target.id,
        versionId: target.active_version_id,
      });
  for (const publication of repo.publications())
    versions.set(`${publication.target_id}:${publication.version_id}`, {
      targetId: publication.target_id,
      versionId: publication.version_id,
    });
  return [...versions.values()].flatMap(({ targetId, versionId }) => {
    const target = repo.target(targetId)!;
    const movie = repo.movie(target.movie_id)!;
    const title = safeTitle(movie.title);
    const folder = `${title}${movie.year ? ` (${movie.year})` : ""} {tmdb-${movie.tmdbId}}`;
    return repo.version(versionId)!.files.map((file, index) => {
      const extension = path.extname(file.relativePath).toLowerCase();
      if (![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(extension))
        throw new Error("Unsupported movie container");
      const contentPath = path.resolve(roots.contentRoot, file.relativePath);
      if (
        !contentPath.startsWith(`${path.resolve(roots.contentRoot)}${path.sep}`)
      )
        throw new Error("Content file escapes root");
      return {
        relativePath: `${folder}/${title} - t${targetId}v${versionId}${index ? ` - pt${index + 1}` : ""}${extension}`,
        contentPath,
      };
    });
  });
}

export async function reconcileMovieView(
  repo: MovieRepo,
  roots: MovieViewRoots,
  assertActive: () => void = () => {},
) {
  return serialized(path.resolve(roots.viewRoot), async () => {
    assertActive();
    return reconcileLibraryView({
      ...roots,
      entries: movieViewEntries(repo, roots),
      assertActive,
      onLinkApplied: assertActive,
    });
  });
}

/** Intent survives a crash before indexing, after promotion, or during link retirement. */
export async function publishMovieVersion(input: {
  repo: MovieRepo;
  roots: MovieViewRoots;
  library: MovieLibrary;
  targetId: number;
  versionId: number;
  assertActive?: () => void;
  afterPromotion?: () => void | Promise<void>;
  scan?: boolean;
  pendingIsExpected?: boolean;
  onReconcile?: (ms: number) => void;
}) {
  const { repo, roots, library, targetId, versionId } = input;
  const active = input.assertActive ?? (() => {});
  return serialized(path.resolve(roots.viewRoot), async () => {
    active();
    repo.stagePublication(targetId, versionId);
    const pending = repo
      .publications()
      .find((entry) => entry.target_id === targetId)!;
    await measureView(() => reconcileLibraryView({
      ...roots,
      entries: movieViewEntries(repo, roots),
      assertActive: active,
      onLinkApplied: active,
    }), input.onReconcile);
    active();
    const target = repo.target(targetId)!;
    const movie = repo.movie(target.movie_id)!;
    const content = new Set(
      repo
        .version(versionId)!
        .files.map((file) =>
          path.resolve(roots.contentRoot, file.relativePath),
        ),
    );
    const paths = movieViewEntries(repo, roots)
      .filter((entry) => content.has(entry.contentPath))
      .map((entry) =>
        path.posix.join(roots.serverViewRoot, entry.relativePath),
      );
    if (input.scan !== false) await library.scan(paths);
    if (!(await library.verify(movie, paths))) {
      if (input.pendingIsExpected) return false;
      throw new Error(
        "Plex has not verified the expected movie and content paths; previous version remains active",
      );
    }
    active();
    repo.selectVersion(targetId, versionId, pending.expected_revision);
    await input.afterPromotion?.();
    active();
    repo.clearPublication(targetId, versionId);
    await measureView(() => reconcileLibraryView({
      ...roots,
      entries: movieViewEntries(repo, roots),
      assertActive: active,
      onLinkApplied: active,
    }), input.onReconcile);
    repo.event(
      targetId,
      null,
      `published:${targetId}:${versionId}`,
      "available",
      "Verified in Plex and ready to watch",
    );
    return true;
  });
}
