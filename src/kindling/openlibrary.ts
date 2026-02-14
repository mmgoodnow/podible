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

export type OpenLibraryCandidate = {
  openLibraryKey: string;
  title: string;
  author: string;
  publishedAt?: string;
  language?: string;
  isbn?: string;
  identifiers: Record<string, string>;
};

type ResolveOptions = {
  openLibraryKey?: string | null;
  isbn?: string | null;
  title?: string | null;
  author?: string | null;
};

function normalizeOpenLibraryKey(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\/works\/OL\d+W$/i.test(trimmed)) {
    const suffix = trimmed.slice("/works/".length);
    return `/works/${suffix.toUpperCase()}`;
  }
  if (/^OL\d+W$/i.test(trimmed)) {
    return `/works/${trimmed.toUpperCase()}`;
  }
  return null;
}

async function fetchSearchDocs(params: Record<string, string>): Promise<OpenLibraryDoc[]> {
  const url = new URL("https://openlibrary.org/search.json");
  for (const [key, value] of Object.entries(params)) {
    if (value) {
      url.searchParams.set(key, value);
    }
  }

  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return [];
  const payload = (await response.json()) as OpenLibraryResponse;
  return payload.docs ?? [];
}

function docToCandidate(doc: OpenLibraryDoc, fallbackTitle?: string, fallbackAuthor?: string): OpenLibraryCandidate | null {
  const key = typeof doc.key === "string" ? normalizeOpenLibraryKey(doc.key) : null;
  const title = (doc.title ?? fallbackTitle ?? "").trim();
  const author = (doc.author_name?.[0] ?? fallbackAuthor ?? "").trim();
  if (!key || !title || !author) return null;

  const publishedAt = doc.first_publish_year ? `${doc.first_publish_year}-01-01T00:00:00.000Z` : undefined;
  const language = doc.language?.[0];
  const isbn = doc.isbn?.[0];
  const identifiers: Record<string, string> = {
    openlibrary: key,
  };
  if (isbn) identifiers.isbn = isbn;

  return {
    openLibraryKey: key,
    title,
    author,
    publishedAt,
    language,
    isbn,
    identifiers,
  };
}

function candidateToMetadata(candidate: OpenLibraryCandidate, preferredIsbn?: string | null): OpenLibraryMetadata {
  const isbn = preferredIsbn?.trim() || candidate.isbn;
  const identifiers = { ...candidate.identifiers };
  if (isbn) identifiers.isbn = isbn;
  return {
    publishedAt: candidate.publishedAt,
    language: candidate.language,
    isbn,
    identifiers,
  };
}

function pickCandidateByKey(candidates: OpenLibraryCandidate[], openLibraryKey: string): OpenLibraryCandidate | null {
  const normalized = normalizeOpenLibraryKey(openLibraryKey);
  if (!normalized) return null;
  for (const candidate of candidates) {
    if (candidate.openLibraryKey === normalized) return candidate;
  }
  return null;
}

export async function searchOpenLibrary(query: string, limit = 20): Promise<OpenLibraryCandidate[]> {
  const trimmed = query.trim();
  if (!trimmed) return [];
  const safeLimit = Math.max(1, Math.min(50, Math.trunc(limit || 20)));
  const docs = await fetchSearchDocs({
    q: trimmed,
    limit: String(safeLimit),
  });
  const out: OpenLibraryCandidate[] = [];
  for (const doc of docs) {
    const candidate = docToCandidate(doc);
    if (candidate) out.push(candidate);
  }
  return out;
}

export async function resolveOpenLibraryCandidate(options: ResolveOptions): Promise<OpenLibraryCandidate | null> {
  const openLibraryKey = options.openLibraryKey?.trim() ?? "";
  if (openLibraryKey) {
    const normalized = normalizeOpenLibraryKey(openLibraryKey);
    if (!normalized) {
      throw new Error("Invalid openLibraryKey format");
    }
    const keyed = await searchOpenLibrary(`key:${normalized}`, 5);
    const exactKey = pickCandidateByKey(keyed, normalized);
    if (exactKey) return exactKey;

    const fallback = await searchOpenLibrary(normalized, 5);
    const fallbackExact = pickCandidateByKey(fallback, normalized);
    if (fallbackExact) return fallbackExact;
  }

  const isbn = options.isbn?.trim() ?? "";
  if (isbn) {
    const docs = await fetchSearchDocs({
      isbn,
      limit: "1",
    });
    const candidate = docs
      .map((doc) => docToCandidate(doc, options.title ?? undefined, options.author ?? undefined))
      .find((value): value is OpenLibraryCandidate => value !== null);
    if (candidate) return candidate;
  }

  const title = options.title?.trim() ?? "";
  const author = options.author?.trim() ?? "";
  const query = `${title} ${author}`.trim();
  if (!query) return null;
  const docs = await fetchSearchDocs({
    q: query,
    limit: "1",
  });
  const candidate = docs
    .map((doc) => docToCandidate(doc, title, author))
    .find((value): value is OpenLibraryCandidate => value !== null);
  return candidate ?? null;
}

export async function fetchOpenLibraryMetadata(
  book: Pick<BookRow, "title" | "author" | "isbn"> & { openLibraryKey?: string | null }
): Promise<OpenLibraryMetadata | null> {
  const candidate = await resolveOpenLibraryCandidate({
    openLibraryKey: book.openLibraryKey ?? null,
    isbn: book.isbn,
    title: book.title,
    author: book.author,
  });
  if (!candidate) return null;
  return candidateToMetadata(candidate, book.isbn);
}
