import { promises as fs, watch } from "node:fs";
import path from "node:path";

import { ensureDataDir, libraryIndexPath } from "../config";
import { resolveCoverPath } from "../media/covers";
import { bookId, mimeFromExt, preferLonger, readAudioMetadata, readOpfMetadata } from "../media/metadata";
import { getDurationSeconds, readFfprobeChapters } from "../media/probe-cache";
import { AudioSegment, Book, BookBuildResult, ChapterTiming, PendingSingleMeta, TranscodeStatus } from "../types";
import {
  queuedSources,
  saveTranscodeStatus,
  statusKey,
  transcodeJobs,
  transcodeOutputPath,
  transcodeStatus,
} from "../transcode";

const readyBooks = new Map<string, Book>();
let rescanTimer: ReturnType<typeof setTimeout> | null = null;

async function fileExists(filePath: string): Promise<boolean> {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch {
    return false;
  }
}

async function safeReadDir(dir: string) {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch (err) {
    console.warn(`Skipping unreadable dir ${dir}:`, (err as Error).message);
    return [];
  }
}

function reviveBook(book: any): Book | null {
  if (!book || typeof book !== "object") return null;
  if (typeof book.id !== "string" || typeof book.title !== "string" || typeof book.author !== "string") return null;
  const publishedAt = book.publishedAt ? new Date(book.publishedAt) : undefined;
  const { addedAt: _addedAt, ...rest } = book;
  const revived: Book = {
    ...rest,
    publishedAt,
  };
  return revived;
}

async function loadLibraryIndex() {
  await ensureDataDir();
  try {
    const content = await fs.readFile(libraryIndexPath, "utf8");
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) {
      parsed.forEach((entry: any) => {
        const book = reviveBook(entry);
        if (book) readyBooks.set(book.id, book);
      });
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn("Failed to read library index:", (err as Error).message);
    }
  }
}

async function saveLibraryIndex() {
  await ensureDataDir();
  const payload = Array.from(readyBooks.values()).map((book) => {
    if (!book.addedAt) return book;
    const { addedAt, ...rest } = book;
    return rest;
  });
  await fs.writeFile(libraryIndexPath, JSON.stringify(payload, null, 2), "utf8");
}

function bookFromMeta(meta: PendingSingleMeta, outputPath: string, outputStat: Awaited<ReturnType<typeof fs.stat>>): Book {
  const mime = mimeFromExt(path.extname(outputPath));
  const durationSeconds = meta.durationSeconds ?? getDurationSeconds(outputPath, outputStat.mtimeMs);
  return {
    id: meta.id,
    title: meta.title,
    author: meta.author,
    kind: "single",
    mime,
    totalSize: outputStat.size,
    primaryFile: outputPath,
    coverPath: meta.coverPath,
    durationSeconds,
    publishedAt: meta.publishedAt,
    addedAt: meta.addedAt,
    description: meta.description,
    descriptionHtml: meta.descriptionHtml,
    language: meta.language,
    isbn: meta.isbn,
    identifiers: meta.identifiers,
    chapters: meta.chapters,
  };
}

function addedAtFromStat(stat: Awaited<ReturnType<typeof fs.stat>> | null): Date {
  if (!stat) return new Date();
  const birth = stat.birthtimeMs;
  if (Number.isFinite(birth) && birth > 0) return new Date(birth);
  const mtime = stat.mtimeMs;
  if (Number.isFinite(mtime) && mtime > 0) return new Date(mtime);
  return new Date();
}

async function buildBook(author: string, bookDir: string, title: string): Promise<BookBuildResult | null> {
  const t0 = Date.now();
  console.log(`[scan] start book author="${author}" title="${title}" path="${bookDir}"`);
  const entries = await safeReadDir(bookDir);
  const files = entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
  const m4bs = files.filter((f) => f.toLowerCase().endsWith(".m4b")).sort();
  const mp3s = files.filter((f) => f.toLowerCase().endsWith(".mp3")).sort();
  const pngs = files.filter((f) => f.toLowerCase().endsWith(".png")).sort();
  const jpgs = files.filter((f) => f.toLowerCase().endsWith(".jpg") || f.toLowerCase().endsWith(".jpeg")).sort();
  const bookDirStat = await fs.stat(bookDir).catch(() => null);
  const opf = await readOpfMetadata(bookDir, files);
  const audioMetaSource = m4bs[0] ? path.join(bookDir, m4bs[0]) : mp3s[0] ? path.join(bookDir, mp3s[0]) : null;
  const audioMetaStat = audioMetaSource ? await fs.stat(audioMetaSource).catch(() => null) : null;
  const audioMeta = audioMetaStat ? readAudioMetadata(audioMetaSource!, audioMetaStat.mtimeMs) : null;
  const chapterMeta =
    m4bs[0] && audioMetaStat ? readFfprobeChapters(audioMetaSource!, audioMetaStat.mtimeMs) ?? undefined : undefined;

  const coverPath = await resolveCoverPath(bookDir, m4bs, mp3s, pngs, jpgs);
  const displayTitle = opf?.title ?? title;
  const displayAuthor = audioMeta?.artist ?? audioMeta?.albumArtist ?? opf?.author ?? author;
  const id = bookId(author, title);
  const description = preferLonger(opf?.description, audioMeta?.description);
  const descriptionHtml = preferLonger(opf?.descriptionHtml, audioMeta?.descriptionHtml);
  const language = audioMeta?.language ?? opf?.language;
  const isbn = opf?.isbn;
  const identifiers = opf?.identifiers;
  const addedAt = addedAtFromStat(bookDirStat ?? audioMetaStat);

  if (m4bs.length > 0) {
    const filePath = path.join(bookDir, m4bs[0]);
    const stat = audioMetaStat ?? (await fs.stat(filePath).catch(() => null));
    if (!stat) return null;
    const durationProbe = getDurationSeconds(filePath, stat.mtimeMs);
    if (durationProbe === undefined) {
      transcodeStatus.set(statusKey(filePath), {
        source: filePath,
        target: transcodeOutputPath(filePath, stat),
        mtimeMs: stat.mtimeMs,
        state: "failed",
        error: "ffprobe duration missing",
        durationMs: undefined,
      });
      await saveTranscodeStatus();
      console.warn(`[scan] skipping m4b with missing duration author="${author}" title="${title}" path="${filePath}"`);
      return null;
    }
    const target = transcodeOutputPath(filePath, stat);
    const meta: PendingSingleMeta = {
      id,
      title: displayTitle,
      author: displayAuthor,
      coverPath,
      durationSeconds: getDurationSeconds(filePath, stat.mtimeMs),
      publishedAt: opf?.publishedAt ?? audioMeta?.date ?? stat.mtime,
      addedAt,
      description,
      descriptionHtml,
      language,
      isbn,
      identifiers,
      chapters: chapterMeta,
    };

    const existing = transcodeStatus.get(statusKey(filePath));
    if (
      existing &&
      existing.state === "done" &&
      existing.mtimeMs === stat.mtimeMs &&
      existing.target &&
      (await fileExists(existing.target))
    ) {
      const targetStat = await fs.stat(existing.target).catch(() => null);
      if (targetStat) {
        const durationSeconds = meta.durationSeconds ?? getDurationSeconds(existing.target, targetStat.mtimeMs);
        const ready: Book = {
          id: meta.id,
          title: meta.title,
          author: meta.author,
          kind: "single",
          mime: mimeFromExt(path.extname(existing.target)),
          totalSize: targetStat.size,
          primaryFile: existing.target,
          coverPath: meta.coverPath,
          durationSeconds,
          publishedAt: meta.publishedAt,
          addedAt: meta.addedAt,
          description: meta.description,
          descriptionHtml: meta.descriptionHtml,
          language: meta.language,
          isbn: meta.isbn,
          identifiers: meta.identifiers,
          chapters: meta.chapters,
        };
        console.log(
          `[scan] found ready m4b author="${author}" title="${title}" size=${targetStat.size} in ${Date.now() - t0}ms`
        );
        return { ready, sourcePath: filePath };
      }
    }

    const existingStatus = transcodeStatus.get(statusKey(filePath));
    const needsReset = !existingStatus || existingStatus.mtimeMs !== stat.mtimeMs;
    const pending: TranscodeStatus = {
      source: filePath,
      target,
      mtimeMs: stat.mtimeMs,
      state: "pending",
      error: needsReset ? undefined : existingStatus?.error,
      durationMs: meta.durationSeconds ? meta.durationSeconds * 1000 : existingStatus?.durationMs,
      meta,
    };
    transcodeStatus.set(statusKey(filePath), pending);
    if (!queuedSources.has(filePath) && pending.state !== "working") {
      queuedSources.add(filePath);
      transcodeJobs.push({
        source: filePath,
        target,
        mtimeMs: stat.mtimeMs,
        meta,
      });
      console.log(`[scan] queued transcode author="${author}" title="${title}" source="${filePath}"`);
    }
    return { pendingJob: { source: filePath, target, mtimeMs: stat.mtimeMs, meta }, sourcePath: filePath };
  }

  if (mp3s.length > 0) {
    const segments: AudioSegment[] = [];
    let cursor = 0;
    let cursorMs = 0;
    let durationSeconds = 0;
    let publishedAt: Date | undefined = opf?.publishedAt ?? audioMeta?.date;
    for (const name of mp3s) {
      const filePath = path.join(bookDir, name);
      const stat = await fs.stat(filePath).catch(() => null);
      if (!stat || stat.size <= 0) {
        console.warn(`[scan] skipping zero-size mp3 author="${author}" title="${title}" file="${filePath}"`);
        transcodeStatus.set(statusKey(filePath), {
          source: filePath,
          target: filePath,
          mtimeMs: stat?.mtimeMs ?? Date.now(),
          state: "failed",
          error: "zero-size mp3",
        });
        return { sourcePath: filePath };
      }
      if ((!publishedAt || !opf?.publishedAt) && (!publishedAt || stat.mtime < publishedAt)) publishedAt = stat.mtime;
      const fileMeta = readAudioMetadata(filePath, stat.mtimeMs);
      const duration = getDurationSeconds(filePath, stat.mtimeMs);
      if (duration === undefined) {
        console.warn(`[scan] skipping mp3 with missing duration author="${author}" title="${title}" file="${filePath}"`);
        transcodeStatus.set(statusKey(filePath), {
          source: filePath,
          target: filePath,
          mtimeMs: stat.mtimeMs,
          state: "failed",
          error: "mp3 duration missing",
        });
        return { sourcePath: filePath };
      }
      const durationMs = Math.max(0, Math.round(duration * 1000));
      durationSeconds += duration;
      const start = cursor;
      const end = cursor + stat.size - 1;
      segments.push({
        path: filePath,
        name,
        size: stat.size,
        start,
        end,
        durationMs,
        title: fileMeta?.title,
      });
      cursor += stat.size;
      cursorMs += durationMs;
    }
    if (segments.length === 0) return null;
    const mime = mimeFromExt(path.extname(mp3s[0]));
    const result: Book = {
      id,
      title: displayTitle,
      author: displayAuthor,
      kind: "multi",
      mime,
      totalSize: segments.reduce((sum, seg) => sum + seg.size, 0),
      files: segments,
      coverPath,
      durationSeconds,
      publishedAt,
      addedAt,
      description,
      descriptionHtml,
      language,
      isbn,
      identifiers,
    };
    console.log(
      `[scan] built multi book author="${author}" title="${title}" files=${segments.length} duration=${Math.round(durationSeconds)}s in ${Date.now() - t0}ms`
    );
    return { ready: result, sourcePath: bookDir };
  }

  return null;
}

async function scanAndQueue(scanRoots: string[]): Promise<void> {
  const started = Date.now();
  let ready = 0;
  for (const root of scanRoots) {
    const stat = await fs.stat(root).catch(() => null);
    if (!stat || !stat.isDirectory()) {
      console.warn(`[scan] skipping missing root: ${root}`);
      continue;
    }
    const authorDirs = await safeReadDir(root);
    for (const authorDir of authorDirs.filter((d) => d.isDirectory())) {
      const author = authorDir.name;
      const authorPath = path.join(root, author);
      const bookDirs = await safeReadDir(authorPath);
      for (const bookDir of bookDirs.filter((d) => d.isDirectory())) {
        const bookTitle = bookDir.name;
        const bookPath = path.join(authorPath, bookTitle);
        const result = await buildBook(author, bookPath, bookTitle);
        if (result?.ready) {
          readyBooks.set(result.ready.id, result.ready);
          ready += 1;
        }
      }
    }
  }
  await saveLibraryIndex();
  await saveTranscodeStatus();
  console.log(
    `[scan] completed ready=${readyBooks.size} queued=${queuedSources.size} in ${Date.now() - started}ms roots=${scanRoots.join("|")}`
  );
}

function scheduleRescan(scanRoots: string[], delayMs = 500) {
  if (rescanTimer) return;
  rescanTimer = setTimeout(() => {
    rescanTimer = null;
    scanAndQueue(scanRoots).catch((err) => console.error("Rescan failed:", err));
  }, delayMs);
}

function startWatchers(scanRoots: string[]) {
  for (const root of scanRoots) {
    try {
      const watcher = watch(root, { recursive: true }, (eventType, filename) => {
        const fileLabel = filename ? ` file="${filename}"` : "";
        console.log(`[watch] root="${root}" event=${eventType}${fileLabel}`);
        scheduleRescan(scanRoots, 500);
      });
      watcher.on("error", (err) => console.warn(`Watcher error for ${root}:`, err?.message ?? err));
    } catch (err) {
      console.warn(`Failed to watch ${root}:`, (err as Error).message);
    }
  }
}

async function findBookById(id: string): Promise<Book | null> {
  const book = readyBooks.get(id);
  return book ?? null;
}

function readyBooksSorted(): Book[] {
  const books = Array.from(readyBooks.values());
  books.sort((a, b) => {
    const at = (a.addedAt ?? a.publishedAt)?.getTime() ?? 0;
    const bt = (b.addedAt ?? b.publishedAt)?.getTime() ?? 0;
    return bt - at;
  });
  return books;
}

function feedBooksSorted(): Book[] {
  const combined = new Map<string, Book>();
  Array.from(readyBooks.values()).forEach((book) => combined.set(book.id, book));
  Array.from(transcodeStatus.values()).forEach((status) => {
    if (status.state === "done" || !status.meta) return;
    if (combined.has(status.meta.id)) return;
    const meta = status.meta;
    combined.set(meta.id, {
      id: meta.id,
      title: meta.title,
      author: meta.author,
      kind: "single",
      mime: mimeFromExt(".mp3"),
      totalSize: 0,
      coverPath: meta.coverPath,
      durationSeconds: meta.durationSeconds,
      publishedAt: meta.publishedAt,
      addedAt: meta.addedAt,
      description: meta.description,
      descriptionHtml: meta.descriptionHtml,
      language: meta.language,
      isbn: meta.isbn,
      identifiers: meta.identifiers,
      chapters: meta.chapters,
    });
  });
  const books = Array.from(combined.values());
  books.sort((a, b) => {
    const at = (a.addedAt ?? a.publishedAt)?.getTime() ?? 0;
    const bt = (b.addedAt ?? b.publishedAt)?.getTime() ?? 0;
    return bt - at;
  });
  return books;
}

export {
  bookFromMeta,
  fileExists,
  findBookById,
  feedBooksSorted,
  loadLibraryIndex,
  readyBooks,
  readyBooksSorted,
  saveLibraryIndex,
  scanAndQueue,
  startWatchers,
};
