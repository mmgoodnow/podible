import { BooksRepo } from "../repo";

import { hydrateBookFromOpenLibrary } from "./hydration";
import { resolveOpenLibraryCandidate } from "./openlibrary";
import { triggerAutoAcquire } from "./service";

export async function createOrReuseBookFromOpenLibrary(
  repo: BooksRepo,
  openLibraryKey: string
): Promise<{ bookId: number; acquisitionJobId: number }> {
  const resolved = await resolveOpenLibraryCandidate({ openLibraryKey });
  if (!resolved) {
    throw new Error("Open Library match not found");
  }

  const canonicalKey = resolved.identifiers.openlibrary ?? openLibraryKey.trim();
  const bookId = repo.transaction(() => {
    const existing =
      repo.findBookByOpenLibraryKey(canonicalKey) ??
      repo.findBookByTitleAuthor(resolved.title, resolved.author);

    const book =
      existing ??
      repo.createBook({
        title: resolved.title,
        author: resolved.author,
      });

    repo.updateBookMetadata(book.id, {
      publishedAt: resolved.publishedAt ?? null,
      language: resolved.language ?? null,
      identifiers: resolved.identifiers,
    });

    return book.id;
  });

  const hydrated = repo.getBook(bookId);
  if (hydrated) {
    await hydrateBookFromOpenLibrary(repo, hydrated);
  }

  const acquisitionJobId = await triggerAutoAcquire(repo, bookId);
  return { bookId, acquisitionJobId };
}
