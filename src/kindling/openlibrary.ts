import type { BookRow } from "./types";

type OpenLibraryDoc = {
  title?: string;
  author_name?: string[];
  first_publish_year?: number;
  language?: string[];
  isbn?: string[];
  key?: string;
  cover_i?: number;
};

type OpenLibraryResponse = {
  docs?: OpenLibraryDoc[];
};

type OpenLibraryWorkResponse = {
  description?: string | { value?: string };
  covers?: number[];
};

type OpenLibraryEditionsResponse = {
  entries?: Array<{
    isbn_13?: string[];
    isbn_10?: string[];
  }>;
};

export type OpenLibraryMetadata = {
  publishedAt?: string;
  language?: string;
  isbn?: string;
  identifiers?: Record<string, string>;
  description?: string;
  descriptionHtml?: string;
  coverUrl?: string;
};

export type OpenLibraryCandidate = {
  openLibraryKey: string;
  title: string;
  author: string;
  publishedAt?: string;
  language?: string;
  isbn?: string;
  coverId?: number;
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

function normalizeIsbn(value: string): string | null {
  const stripped = value.replace(/[\s-]+/g, "").trim().toUpperCase();
  if (!stripped) return null;
  if (/^\d{13}$/.test(stripped)) return stripped;
  if (/^\d{9}[\dX]$/.test(stripped)) return stripped;
  return null;
}

function coverUrlFromId(coverId: number): string {
  return `https://covers.openlibrary.org/b/id/${coverId}-L.jpg`;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function toDescriptionHtml(value: string): string {
  const paragraphs = value
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => `<p>${escapeHtml(part).replace(/\n/g, "<br/>")}</p>`);
  if (paragraphs.length > 0) return paragraphs.join("\n");
  return `<p>${escapeHtml(value)}</p>`;
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

async function fetchWorkDetails(openLibraryKey: string): Promise<{ description?: string; coverId?: number } | null> {
  const url = new URL(`https://openlibrary.org${openLibraryKey}.json`);
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return null;
  const payload = (await response.json()) as OpenLibraryWorkResponse;
  const rawDescription =
    typeof payload.description === "string"
      ? payload.description
      : payload.description && typeof payload.description.value === "string"
        ? payload.description.value
        : undefined;
  const description = rawDescription?.trim() || undefined;
  const coverId = Array.isArray(payload.covers) ? payload.covers.find((value) => Number.isInteger(value) && value > 0) : undefined;
  return {
    description,
    coverId,
  };
}

async function fetchEditionIsbn(openLibraryKey: string): Promise<string | undefined> {
  const url = new URL(`https://openlibrary.org${openLibraryKey}/editions.json`);
  url.searchParams.set("limit", "20");
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) return undefined;
  const payload = (await response.json()) as OpenLibraryEditionsResponse;
  for (const entry of payload.entries ?? []) {
    const values = [...(entry.isbn_13 ?? []), ...(entry.isbn_10 ?? [])];
    for (const raw of values) {
      const normalized = normalizeIsbn(raw);
      if (normalized) return normalized;
    }
  }
  return undefined;
}

function docToCandidate(doc: OpenLibraryDoc, fallbackTitle?: string, fallbackAuthor?: string): OpenLibraryCandidate | null {
  const key = typeof doc.key === "string" ? normalizeOpenLibraryKey(doc.key) : null;
  const title = (doc.title ?? fallbackTitle ?? "").trim();
  const author = (doc.author_name?.[0] ?? fallbackAuthor ?? "").trim();
  if (!key || !title || !author) return null;

  const publishedAt = doc.first_publish_year ? `${doc.first_publish_year}-01-01T00:00:00.000Z` : undefined;
  const language = doc.language?.[0];
  const isbn = doc.isbn?.[0];
  const coverId = Number.isInteger(doc.cover_i) && Number(doc.cover_i) > 0 ? Number(doc.cover_i) : undefined;
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
    coverId,
    identifiers,
  };
}

async function enrichCandidate(candidate: OpenLibraryCandidate): Promise<OpenLibraryCandidate> {
  if (candidate.isbn) return candidate;
  const fallbackIsbn = await fetchEditionIsbn(candidate.openLibraryKey).catch(() => undefined);
  if (!fallbackIsbn) return candidate;
  return {
    ...candidate,
    isbn: fallbackIsbn,
    identifiers: {
      ...candidate.identifiers,
      isbn: fallbackIsbn,
    },
  };
}

function candidateToMetadata(
  candidate: OpenLibraryCandidate,
  preferredIsbn?: string | null,
  details?: { description?: string; coverId?: number } | null
): OpenLibraryMetadata {
  const isbn = preferredIsbn?.trim() || candidate.isbn;
  const identifiers = { ...candidate.identifiers };
  if (isbn) identifiers.isbn = isbn;

  const description = details?.description;
  const descriptionHtml = description ? toDescriptionHtml(description) : undefined;
  const coverId = details?.coverId ?? candidate.coverId;

  return {
    publishedAt: candidate.publishedAt,
    language: candidate.language,
    isbn,
    identifiers,
    description,
    descriptionHtml,
    coverUrl: coverId ? coverUrlFromId(coverId) : undefined,
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
    if (exactKey) return enrichCandidate(exactKey);

    const fallback = await searchOpenLibrary(normalized, 5);
    const fallbackExact = pickCandidateByKey(fallback, normalized);
    if (fallbackExact) return enrichCandidate(fallbackExact);
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
    if (candidate) return enrichCandidate(candidate);
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
  if (!candidate) return null;
  return enrichCandidate(candidate);
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

  const details = await fetchWorkDetails(candidate.openLibraryKey).catch(() => null);
  return candidateToMetadata(candidate, book.isbn, details);
}
