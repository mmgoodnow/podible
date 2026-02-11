import { promises as fs } from "node:fs";
import path from "node:path";

import { getDurationSeconds } from "../media/probe-cache";

import type { KindlingRepo } from "./repo";

type ScanResult = {
  booksCreated: number;
  assetsCreated: number;
};

type FileInfo = {
  path: string;
  name: string;
  ext: string;
  size: number;
  mtimeMs: number;
};

async function safeReadDir(dir: string) {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return [];
  }
}

async function listFiles(dir: string): Promise<FileInfo[]> {
  const entries = await safeReadDir(dir);
  const out: FileInfo[] = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const fullPath = path.join(dir, entry.name);
    const stat = await fs.stat(fullPath).catch(() => null);
    if (!stat || !stat.isFile()) continue;
    out.push({
      path: fullPath,
      name: entry.name,
      ext: path.extname(entry.name).toLowerCase(),
      size: Number(stat.size),
      mtimeMs: Number(stat.mtimeMs),
    });
  }
  return out;
}

export async function scanLibraryRoot(repo: KindlingRepo, libraryRoot: string): Promise<ScanResult> {
  let booksCreated = 0;
  let assetsCreated = 0;

  const authorDirs = (await safeReadDir(libraryRoot)).filter((entry) => entry.isDirectory());
  for (const authorDir of authorDirs) {
    const author = authorDir.name;
    const authorPath = path.join(libraryRoot, author);
    const bookDirs = (await safeReadDir(authorPath)).filter((entry) => entry.isDirectory());

    for (const bookDir of bookDirs) {
      const title = bookDir.name;
      const bookPath = path.join(authorPath, title);
      const files = await listFiles(bookPath);
      let book = repo.findBookByTitleAuthor(title, author);
      if (!book) {
        book = repo.createBook({ title, author });
        booksCreated += 1;
      }

      const hasCover = files.find((file) => [".jpg", ".jpeg", ".png"].includes(file.ext));
      if (hasCover) {
        repo.updateBookMetadata(book.id, { coverPath: hasCover.path });
      }

      const m4 = files.filter((file) => [".m4b", ".m4a", ".mp4"].includes(file.ext));
      if (m4.length > 0) {
        const chosen = [...m4].sort((a, b) => b.size - a.size)[0];
        if (chosen && !repo.hasAssetFilePath(chosen.path)) {
          const durationMs = Math.round((getDurationSeconds(chosen.path, chosen.mtimeMs) ?? 0) * 1000);
          repo.addAsset({
            bookId: book.id,
            kind: "single",
            mime: chosen.ext === ".m4b" || chosen.ext === ".m4a" || chosen.ext === ".mp4" ? "audio/mp4" : "audio/mpeg",
            totalSize: chosen.size,
            durationMs,
            sourceReleaseId: null,
            files: [
              {
                path: chosen.path,
                size: chosen.size,
                start: 0,
                end: Math.max(0, chosen.size - 1),
                durationMs,
                title: null,
              },
            ],
          });
          assetsCreated += 1;
        }
      } else {
        const mp3 = files.filter((file) => file.ext === ".mp3").sort((a, b) => a.name.localeCompare(b.name));
        if (mp3.length > 0 && mp3.every((file) => !repo.hasAssetFilePath(file.path))) {
          let cursor = 0;
          let durationMsTotal = 0;
          const assetFiles = mp3.map((file, index) => {
            const durationMs = Math.round((getDurationSeconds(file.path, file.mtimeMs) ?? 0) * 1000);
            const row = {
              path: file.path,
              size: file.size,
              start: cursor,
              end: cursor + file.size - 1,
              durationMs,
              title: `Part ${index + 1}`,
            };
            cursor += file.size;
            durationMsTotal += durationMs;
            return row;
          });

          repo.addAsset({
            bookId: book.id,
            kind: mp3.length > 1 ? "multi" : "single",
            mime: "audio/mpeg",
            totalSize: mp3.reduce((sum, file) => sum + file.size, 0),
            durationMs: durationMsTotal,
            sourceReleaseId: null,
            files: assetFiles,
          });
          assetsCreated += 1;
        }
      }

      const epub = files.find((file) => file.ext === ".epub");
      const pdf = files.find((file) => file.ext === ".pdf");
      const ebook = epub ?? pdf;
      if (ebook && !repo.hasAssetFilePath(ebook.path)) {
        repo.addAsset({
          bookId: book.id,
          kind: "ebook",
          mime: ebook.ext === ".pdf" ? "application/pdf" : "application/epub+zip",
          totalSize: ebook.size,
          durationMs: null,
          sourceReleaseId: null,
          files: [
            {
              path: ebook.path,
              size: ebook.size,
              start: 0,
              end: Math.max(0, ebook.size - 1),
              durationMs: 0,
              title: null,
            },
          ],
        });
        assetsCreated += 1;
      }
    }
  }

  return { booksCreated, assetsCreated };
}
