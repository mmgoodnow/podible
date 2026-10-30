import { promises as fs } from "node:fs";
import path from "node:path";

import { getDurationSeconds } from "../media/probe-cache";
import { computeEpubWordCount } from "./chapter-analysis";

import { hydrateBookFromOpenLibrary } from "./hydration";
import type { BooksRepo } from "../repo";
import type { WorkerContext } from "../worker/context";

/**
 * Filesystem-first scanner for existing libraries.
 *
 * It walks `libraryRoot/Author/Title`, creates missing book rows, and creates
 * assets for detected audio/ebook files when those file paths are not already
 * tracked in `asset_files`.
 */
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

function isHiddenOrBundleDir(name: string): boolean {
  const lower = name.toLowerCase();
  if (lower.startsWith(".")) return true;
  if (lower.endsWith(".app")) return true;
  return false;
}

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
    if (isHiddenOrBundleDir(entry.name)) continue;
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

/**
 * Import loose files that already exist under the library root into the
 * database model. This does not call rTorrent or perform search/snatch.
 */
export async function scanLibraryRoot(repo: BooksRepo, libraryRoot: string, ctx?: WorkerContext): Promise<ScanResult> {
  // The execution-scoped repository fences every mutation inside its transaction.
  repo = ctx?.repo ?? repo;
  const run = <T>(name: string, handler: () => T | Promise<T>): Promise<T> =>
    ctx?.execution ? ctx.execution.run(name, handler) : Promise.resolve().then(handler);
  const prefix = `scan:${path.resolve(libraryRoot)}`;
  let booksCreated = 0;
  let assetsCreated = 0;

  const authorDirs = await run(`${prefix}:authors`, async () => (await safeReadDir(libraryRoot))
    .filter((entry) => entry.isDirectory() && !isHiddenOrBundleDir(entry.name))
    .map((entry) => entry.name).sort());
  for (const author of authorDirs) {
    const authorPath = path.join(libraryRoot, author);
    const bookDirs = await run(`${prefix}:${author}:directories`, async () => (await safeReadDir(authorPath))
      .filter((entry) => entry.isDirectory() && !isHiddenOrBundleDir(entry.name))
      .map((entry) => entry.name).sort());

    for (const bookDir of bookDirs) {
      const title = bookDir;
      const bookPath = path.join(authorPath, title);
      const sourcePrefix = `${prefix}:${JSON.stringify([author, title])}`;
      const files = await run(`${sourcePrefix}:files`, () => listFiles(bookPath));
      const hasAudio = files.some((file) => [".m4b", ".m4a", ".mp4", ".mp3"].includes(file.ext));
      const hasEbook = files.some((file) => file.ext === ".epub" || file.ext === ".pdf");
      if (!hasAudio && !hasEbook) {
        continue;
      }
      const discovered = await run(`${sourcePrefix}:book`, () => repo.transaction(() => {
        ctx?.execution?.assertActive();
        const existing = repo.findBookByTitleAuthor(title, author);
        return { id: (existing ?? repo.createBook({ title, author })).id, created: !existing };
      }));
      booksCreated += Number(discovered.created);
      const book = repo.getBookRow(discovered.id);
      if (!book) throw new Error(`Scanned book ${discovered.id} was removed during refresh`);
      const hydrated = repo.getBook(book.id);
      if (hydrated) {
        await hydrateBookFromOpenLibrary(repo, hydrated, ctx ? { ctx, stepPrefix: `${sourcePrefix}:hydrate` } : undefined);
      }

      const hasCover = files.find((file) => [".jpg", ".jpeg", ".png"].includes(file.ext));
      if (hasCover) {
        await run(`${sourcePrefix}:cover`, () => {
          ctx?.execution?.assertActive();
          repo.updateBookMetadata(discovered.id, { coverPath: hasCover.path });
        });
      }

      // Probing can be slow; persist its result without holding the publication transaction.
      const audioDurations = await run(`${sourcePrefix}:audio-probe`, () => {
        const m4 = files.filter((file) => [".m4b", ".m4a", ".mp4"].includes(file.ext));
        const selected = m4.length
          ? [...m4].sort((a, b) => b.size - a.size).slice(0, 1)
          : files.filter((file) => file.ext === ".mp3");
        return Object.fromEntries(selected.filter((file) => !repo.hasAssetFilePath(file.path)).map((file) => [
          file.path, Math.round((getDurationSeconds(file.path, file.mtimeMs) ?? 0) * 1000),
        ]));
      });
      assetsCreated += await run(`${sourcePrefix}:audio`, () => repo.transaction(() => {
        ctx?.execution?.assertActive();
        const m4 = files.filter((file) => [".m4b", ".m4a", ".mp4"].includes(file.ext));
        if (m4.length > 0) {
          const chosen = [...m4].sort((a, b) => b.size - a.size)[0];
          if (chosen && !repo.hasAssetFilePath(chosen.path)) {
            const durationMs = audioDurations[chosen.path] ?? 0;
            const manifestation = repo.addManifestation({
              bookId: discovered.id,
              kind: "audio",
            });
            repo.addAsset({
              bookId: discovered.id,
              kind: "single",
              mime: chosen.ext === ".m4b" || chosen.ext === ".m4a" || chosen.ext === ".mp4" ? "audio/mp4" : "audio/mpeg",
              totalSize: chosen.size,
              durationMs,
              sourceReleaseId: null,
              manifestationId: manifestation.id,
              importNote: "Filesystem scanner selected largest m4 audio file.",
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
            return 1;
          }
        } else {
          const mp3 = files.filter((file) => file.ext === ".mp3").sort((a, b) => a.name.localeCompare(b.name));
          if (mp3.length > 0 && mp3.every((file) => !repo.hasAssetFilePath(file.path))) {
            let cursor = 0;
            let durationMsTotal = 0;
            const assetFiles = mp3.map((file, index) => {
              const durationMs = audioDurations[file.path] ?? 0;
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

            const manifestation = repo.addManifestation({
              bookId: discovered.id,
              kind: "audio",
            });
            repo.addAsset({
              bookId: discovered.id,
              kind: mp3.length > 1 ? "multi" : "single",
              mime: "audio/mpeg",
              totalSize: mp3.reduce((sum, file) => sum + file.size, 0),
              durationMs: durationMsTotal,
              sourceReleaseId: null,
              manifestationId: manifestation.id,
              importNote: `Filesystem scanner selected ${mp3.length} MP3 file${mp3.length === 1 ? "" : "s"}.`,
              files: assetFiles,
            });
            return 1;
          }
        }
        return 0;
      }));

      const epub = files.find((file) => file.ext === ".epub");
      const pdf = files.find((file) => file.ext === ".pdf");
      const ebook = epub ?? pdf;
      if (epub && book.word_count == null) {
        await run(`${sourcePrefix}:word-count`, async () => {
          const wordCount = await computeEpubWordCount(epub.path).catch(() => null);
          if (wordCount !== null) {
            ctx?.execution?.assertActive();
            repo.updateBookMetadata(discovered.id, { wordCount });
          }
        });
      }
      assetsCreated += await run(`${sourcePrefix}:ebook`, () => repo.transaction(() => {
        ctx?.execution?.assertActive();
        if (ebook && !repo.hasAssetFilePath(ebook.path)) {
          const manifestation = repo.addManifestation({
            bookId: discovered.id,
            kind: "ebook",
          });
          repo.addAsset({
            bookId: discovered.id,
            kind: "single",
            mime: ebook.ext === ".pdf" ? "application/pdf" : "application/epub+zip",
            totalSize: ebook.size,
            durationMs: null,
            sourceReleaseId: null,
            manifestationId: manifestation.id,
            importNote: "Filesystem scanner selected ebook file.",
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
          return 1;
        }
        return 0;
      }));
    }
  }

  return { booksCreated, assetsCreated };
}
