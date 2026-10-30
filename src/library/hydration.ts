import { downloadCover } from "./covers";
import { queueMissingCoverGeneration } from "./cover-generation";
import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { BookRow, LibraryBook } from "../app-types";
import type { BooksRepo } from "../repo";
import { durableStep, type WorkerContext } from "../worker/context";
import { createHash } from "node:crypto";

export const CURRENT_OPENLIBRARY_METADATA_VERSION = 4;

export type OpenLibraryMetadataStatus = "current" | "stale" | "never_hydrated";

export function openLibraryMetadataStatus(book: BookRow): OpenLibraryMetadataStatus {
  if (book.openlibrary_metadata_version >= CURRENT_OPENLIBRARY_METADATA_VERSION) return "current";
  return book.openlibrary_hydrated_at ? "stale" : "never_hydrated";
}

export async function hydrateBookFromOpenLibrary(
  repo: BooksRepo,
  book: LibraryBook,
  options?: { ctx: WorkerContext; stepPrefix: string; requireMetadata?: boolean }
): Promise<boolean> {
  const run = <T>(stage: string, handler: () => T | Promise<T>): Promise<T> =>
    options ? durableStep(options.ctx, `${options.stepPrefix}:${stage}`, handler) : Promise.resolve().then(handler);
  const metadata = await run("fetch", async () => {
    const result = await fetchOpenLibraryMetadata({
      title: book.title,
      author: book.author,
      openLibraryKey: book.identifiers.openlibrary ?? null,
    }).catch(() => null);
    if (!result && options?.ctx.execution && options.requireMetadata) {
      throw new Error(`Open Library metadata unavailable for book ${book.id}`);
    }
    return result;
  });
  if (!metadata) return false;

  const mergedIdentifiers = {
    ...book.identifiers,
    ...(metadata.identifiers ?? {}),
  };

  const currentRow = repo.getBookRow(book.id);
  let coverPath = currentRow?.cover_path ?? null;
  if (!coverPath && metadata.coverUrl) {
    const filename = options?.ctx.execution
      ? `.cover-metadata-${createHash("sha256").update(`${options.ctx.execution.key}:${options.stepPrefix}`).digest("hex").slice(0, 24)}`
      : "cover";
    const downloaded = await run("cover", () => downloadCover(repo, book, metadata.coverUrl!, filename));
    if (downloaded) {
      coverPath = downloaded;
    }
  }

  await run("publish", () => repo.transaction(() => {
    options?.ctx.execution?.assertActive();
    const latest = repo.getBookRow(book.id);
    if (!latest) return;
    repo.updateBookMetadata(book.id, {
      coverPath: latest.cover_path ?? coverPath ?? null,
      publishedAt: metadata.publishedAt ?? book.publishedAt ?? null,
      description: metadata.description ?? book.description ?? null,
      descriptionHtml: metadata.descriptionHtml ?? book.descriptionHtml ?? null,
      language: metadata.language ?? null,
      identifiers: mergedIdentifiers,
      series: metadata.series ?? book.series,
      openLibraryMetadataVersion: CURRENT_OPENLIBRARY_METADATA_VERSION,
      openLibraryHydratedAt: new Date().toISOString(),
    });
    if (!latest.cover_path && !coverPath) queueMissingCoverGeneration(repo, book.id);
  }));

  return true;
}
