import { downloadCover } from "./covers";
import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { LibraryBook } from "../app-types";
import type { BooksRepo } from "../repo";

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
  });

  return true;
}
