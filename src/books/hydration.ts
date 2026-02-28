import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { dataDir } from "../config";

import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { BooksRepo } from "./repo";
import type { LibraryBook } from "./types";

const coverCacheDir = path.join(dataDir, "kindling-covers");

async function downloadCover(bookId: number, coverUrl: string): Promise<string | null> {
  try {
    const response = await fetch(coverUrl, { method: "GET" });
    if (!response.ok) return null;
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length === 0) return null;

    await mkdir(coverCacheDir, { recursive: true });

    let ext = ".jpg";
    try {
      const parsed = new URL(coverUrl);
      const rawExt = path.extname(parsed.pathname).toLowerCase();
      if (rawExt === ".png") ext = ".png";
    } catch {
      // keep default extension
    }

    const coverPath = path.join(coverCacheDir, `book-${bookId}${ext}`);
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
    const downloaded = await downloadCover(book.id, metadata.coverUrl);
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
