import { downloadCover } from "./covers";
import { queueMissingCoverGeneration } from "./cover-generation";
import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { BookRow, LibraryBook } from "../app-types";
import type { BooksRepo } from "../repo";

export const CURRENT_OPENLIBRARY_METADATA_VERSION = 1;

export type OpenLibraryMetadataStatus = "current" | "stale" | "never_hydrated";

export function openLibraryMetadataStatus(book: BookRow): OpenLibraryMetadataStatus {
  if (book.openlibrary_metadata_version >= CURRENT_OPENLIBRARY_METADATA_VERSION) return "current";
  return book.openlibrary_hydrated_at ? "stale" : "never_hydrated";
}

export async function hydrateBookFromOpenLibrary(repo: BooksRepo, book: LibraryBook): Promise<boolean> {
  const metadata = await fetchOpenLibraryMetadata({
    title: book.title,
    author: book.author,
    openLibraryKey: book.identifiers.openlibrary ?? null,
  }).catch(() => null);
  if (!metadata) return false;

  const mergedIdentifiers = {
    ...book.identifiers,
    ...(metadata.identifiers ?? {}),
  };

  const currentRow = repo.getBookRow(book.id);
  let coverPath = currentRow?.cover_path ?? null;
  if (!coverPath && metadata.coverUrl) {
    const downloaded = await downloadCover(repo, book, metadata.coverUrl);
    if (downloaded) {
      coverPath = downloaded;
    }
  }

  repo.updateBookMetadata(book.id, {
    coverPath: coverPath ?? null,
    publishedAt: metadata.publishedAt ?? book.publishedAt ?? null,
    description: metadata.description ?? book.description ?? null,
    descriptionHtml: metadata.descriptionHtml ?? book.descriptionHtml ?? null,
    language: metadata.language ?? book.language ?? null,
    identifiers: mergedIdentifiers,
    series: metadata.series ?? book.series,
    openLibraryMetadataVersion: CURRENT_OPENLIBRARY_METADATA_VERSION,
    openLibraryHydratedAt: new Date().toISOString(),
  });
  if (!coverPath) queueMissingCoverGeneration(repo, book.id);

  return true;
}
