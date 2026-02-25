import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import { dataDir } from "../config";

import { fetchOpenLibraryMetadata } from "./openlibrary";
import type { KindlingRepo } from "./repo";
import type { LibraryBook } from "./types";

const coverCacheDir = path.join(dataDir, "kindling-covers");

function isLikelyValidImageBytes(bytes: Uint8Array, extHint: string | null): boolean {
  if (bytes.length < 16) return false;
  const isPng =
    bytes.length >= 8 &&
    bytes[0] === 0x89 &&
    bytes[1] === 0x50 &&
    bytes[2] === 0x4e &&
    bytes[3] === 0x47 &&
    bytes[4] === 0x0d &&
    bytes[5] === 0x0a &&
    bytes[6] === 0x1a &&
    bytes[7] === 0x0a;
  const isJpeg =
    bytes.length >= 32 &&
    bytes[0] === 0xff &&
    bytes[1] === 0xd8 &&
    bytes[bytes.length - 2] === 0xff &&
    bytes[bytes.length - 1] === 0xd9;

  if (extHint === ".png") return isPng;
  if (extHint === ".jpg" || extHint === ".jpeg") return isJpeg;
  return isPng || isJpeg;
}

async function isUsableCoverFile(coverPath: string): Promise<boolean> {
  try {
    const bytes = new Uint8Array(await readFile(coverPath));
    return isLikelyValidImageBytes(bytes, path.extname(coverPath).toLowerCase() || null);
  } catch {
    return false;
  }
}

async function downloadCover(bookId: number, coverUrl: string): Promise<string | null> {
  try {
    const response = await fetch(coverUrl, { method: "GET" });
    if (!response.ok) return null;
    const contentType = (response.headers.get("Content-Type") || "").toLowerCase();
    if (contentType && !contentType.startsWith("image/")) return null;
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length === 0) return null;

    await mkdir(coverCacheDir, { recursive: true });

    let ext = ".jpg";
    if (contentType.includes("image/png")) {
      ext = ".png";
    } else if (contentType.includes("image/jpeg") || contentType.includes("image/jpg")) {
      ext = ".jpg";
    }
    try {
      if (!contentType.startsWith("image/")) {
        const parsed = new URL(coverUrl);
        const rawExt = path.extname(parsed.pathname).toLowerCase();
        if (rawExt === ".png") ext = ".png";
      }
    } catch {
      // keep default extension
    }

    if (!isLikelyValidImageBytes(bytes, ext)) {
      return null;
    }

    const coverPath = path.join(coverCacheDir, `book-${bookId}${ext}`);
    await writeFile(coverPath, bytes);
    return coverPath;
  } catch {
    return null;
  }
}

export async function hydrateBookFromOpenLibrary(repo: KindlingRepo, book: LibraryBook): Promise<boolean> {
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
  if (coverPath && !(await isUsableCoverFile(coverPath))) {
    coverPath = null;
  }
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
