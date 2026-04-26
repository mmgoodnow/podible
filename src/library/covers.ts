import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import type { LibraryBook } from "../app-types";
import type { BooksRepo } from "../repo";

function sanitizePathSegment(value: string): string {
  const trimmed = value.trim();
  const sanitized = trimmed
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return sanitized || "Unknown";
}

export async function downloadCover(repo: BooksRepo, book: LibraryBook, coverUrl: string): Promise<string | null> {
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
