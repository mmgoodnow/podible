import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { BooksRepo } from "./repo";
import type { LibraryBook } from "./types";

function sanitizePathSegment(value: string): string {
  const trimmed = value.trim();
  const sanitized = trimmed
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return sanitized || "Unknown";
}

async function downloadCover(repo: BooksRepo, book: LibraryBook, coverUrl: string): Promise<string | null> {
  try {
    const response = await fetch(coverUrl, { method: "GET" });
    if (!response.ok) return null;
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length === 0) return null;

    let ext = ".jpg";
    try {
      const parsed = new URL(coverUrl);
      const rawExt = path.extname(parsed.pathname).toLowerCase();
      if (rawExt === ".png") ext = ".png";
    } catch {
      // keep default extension
    }

    const settings = repo.getSettings();
    const bookDir = path.join(
      settings.libraryRoot,
      sanitizePathSegment(book.author || "Unknown"),
      sanitizePathSegment(book.title || `book-${book.id}`)
    );
    await mkdir(bookDir, { recursive: true });

    const coverPath = path.join(bookDir, `cover${ext}`);
    await writeFile(coverPath, bytes);
    return coverPath;
  } catch {
    return null;
  }
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
  });

  return true;
}
