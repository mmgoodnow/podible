import type { BookRow } from "./types";

type OpenLibraryDoc = {
  title?: string;
  author_name?: string[];
  first_publish_year?: number;
  language?: string[];
  isbn?: string[];
  key?: string;
};

type OpenLibraryResponse = {
  docs?: OpenLibraryDoc[];
};

export type OpenLibraryMetadata = {
  publishedAt?: string;
  language?: string;
  isbn?: string;
  identifiers?: Record<string, string>;
};

export async function fetchOpenLibraryMetadata(book: Pick<BookRow, "title" | "author" | "isbn">): Promise<OpenLibraryMetadata | null> {
  const query = `${book.title} ${book.author}`.trim();
  if (!query) return null;

  const url = new URL("https://openlibrary.org/search.json");
  url.searchParams.set("q", query);
  url.searchParams.set("limit", "1");

  const response = await fetch(url, { method: "GET" });
  if (!response.ok) {
    return null;
  }

  const payload = (await response.json()) as OpenLibraryResponse;
  const doc = payload.docs?.[0];
  if (!doc) return null;

  const publishedAt = doc.first_publish_year ? `${doc.first_publish_year}-01-01T00:00:00.000Z` : undefined;
  const language = doc.language?.[0];
  const isbn = book.isbn ?? doc.isbn?.[0];
  const identifiers: Record<string, string> = {};
  if (doc.key) identifiers.openlibrary = doc.key;
  if (isbn) identifiers.isbn = isbn;

  return {
    publishedAt,
    language,
    isbn,
    identifiers,
  };
}
