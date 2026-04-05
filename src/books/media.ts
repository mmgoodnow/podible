import { promises as fs } from "node:fs";
import path from "node:path";

import { parseRange, segmentsForRange, streamSegmentsWithXingPatch } from "../streaming/range";
import { buildId3ChaptersTag } from "../streaming/id3";
import { readFfprobeChapters } from "../media/probe-cache";
import { selectPreferredAudioAsset } from "./asset-selection";
import { loadStoredChapterTimings } from "./chapter-analysis";

import type { BooksRepo } from "./repo";
import type { AssetFileRow, AssetRow, BookRow, LibraryBook } from "./types";

export { selectPreferredAudioAsset } from "./asset-selection";

export type PreferredAudio = {
  book: LibraryBook;
  bookRow: BookRow;
  asset: AssetRow;
  files: AssetFileRow[];
};

type ChapterTiming = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
  startOffset?: number;
  endOffset?: number;
};

function extensionForMime(mime: string): string {
  if (mime === "audio/mp4") return "m4a";
  if (mime === "audio/mpeg") return "mp3";
  if (mime === "application/epub+zip") return "epub";
  if (mime === "application/pdf") return "pdf";
  return "bin";
}

export function streamExtension(asset: AssetRow): string {
  return extensionForMime(asset.mime);
}

async function buildFallbackChapterTimings(asset: AssetRow, files: AssetFileRow[]): Promise<ChapterTiming[] | null> {
  if (asset.kind === "ebook") return null;
  if (asset.kind === "single") {
    const file = files[0];
    if (!file) return null;
    const stat = await fs.stat(file.path).catch(() => null);
    if (!stat) return null;
    const chapters = readFfprobeChapters(file.path, Number(stat.mtimeMs));
    if (!chapters || chapters.length === 0) return null;
    return chapters.map((chapter) => ({
      ...chapter,
    }));
  }

  const out: ChapterTiming[] = [];
  let cursorMs = 0;
  for (const [index, file] of files.entries()) {
    const startMs = cursorMs;
    const endMs = startMs + file.duration_ms;
    out.push({
      id: `ch${index}`,
      title: file.title ?? path.basename(file.path, path.extname(file.path)),
      startMs,
      endMs,
      startOffset: file.start,
      endOffset: file.end,
    });
    cursorMs = endMs;
  }
  return out;
}

async function buildChapterTimings(repo: BooksRepo, asset: AssetRow, files: AssetFileRow[]): Promise<ChapterTiming[] | null> {
  const stored = await loadStoredChapterTimings(repo, asset, files);
  if (stored && stored.length > 0) {
    return stored;
  }
  return buildFallbackChapterTimings(asset, files);
}

export async function buildChapters(
  repo: BooksRepo,
  asset: AssetRow,
  files: AssetFileRow[]
): Promise<{ version: string; chapters: Array<{ startTime: number; title: string }> } | null> {
  const timings = await buildChapterTimings(repo, asset, files);
  if (!timings || timings.length === 0) return null;
  return {
    version: "1.2.0",
    chapters: timings.map((chapter) => ({
      startTime: chapter.startMs / 1000,
      title: chapter.title,
    })),
  };
}

function coverMimeFromPath(coverPath: string): string {
  const ext = path.extname(coverPath).toLowerCase();
  if (ext === ".png") return "image/png";
  return "image/jpeg";
}

export async function streamAudioAsset(
  request: Request,
  repo: BooksRepo,
  asset: AssetRow,
  files: AssetFileRow[],
  coverPath?: string | null
): Promise<Response> {
  if (asset.kind === "ebook") {
    return new Response("Not found", { status: 404 });
  }

  const rangeHeader = request.headers.get("range");
  const headers: Record<string, string> = {
    "Accept-Ranges": "bytes",
    "Content-Type": asset.mime,
  };

  if (asset.kind === "single") {
    const file = files[0];
    if (!file) return new Response("Not found", { status: 404 });
    const payload = Bun.file(file.path);
    const range = parseRange(rangeHeader, file.size);
    if (!range) {
      headers["Content-Length"] = String(file.size);
      return new Response(payload, { status: 200, headers });
    }
    headers["Content-Length"] = String(range.end - range.start + 1);
    headers["Content-Range"] = `bytes ${range.start}-${range.end}/${file.size}`;
    return new Response(payload.slice(range.start, range.end + 1), { status: 206, headers });
  }

  const timings = await buildChapterTimings(repo, asset, files);
  let tag: Uint8Array<ArrayBufferLike> = new Uint8Array();
  if (timings && timings.length > 0) {
    let coverArt: { mime: string; data: Uint8Array<ArrayBufferLike> } | undefined;
    if (coverPath) {
      const bytes = await fs.readFile(coverPath).catch(() => null);
      if (bytes && bytes.length > 0) {
        coverArt = { mime: coverMimeFromPath(coverPath), data: bytes };
      }
    }
    tag = buildId3ChaptersTag(timings, coverArt);
    if (timings.some((chapter) => chapter.startOffset !== undefined || chapter.endOffset !== undefined)) {
      tag = buildId3ChaptersTag(timings, coverArt, tag.byteLength);
    }
  }

  const totalAudio = files.reduce((sum, file) => sum + file.size, 0);
  const totalSize = totalAudio + tag.byteLength;
  const range = parseRange(rangeHeader, totalSize) ?? { start: 0, end: totalSize - 1 };
  if (range.start >= totalSize) {
    headers["Content-Range"] = `bytes */${totalSize}`;
    return new Response("Range Not Satisfiable", { status: 416, headers });
  }

  const includeTag = range.start < tag.byteLength;
  const tagStart = range.start;
  const tagEnd = Math.min(range.end, tag.byteLength - 1);
  const includeAudio = range.end >= tag.byteLength;
  const audioStart = Math.max(0, range.start - tag.byteLength);
  const audioEnd = Math.max(0, range.end - tag.byteLength);
  const slices = includeAudio
    ? segmentsForRange(
        files.map((file) => ({
          path: file.path,
          name: path.basename(file.path),
          size: file.size,
          start: file.start,
          end: file.end,
          durationMs: file.duration_ms,
          title: file.title ?? undefined,
        })),
        audioStart,
        audioEnd
      )
    : [];

  headers["Content-Length"] = String(range.end - range.start + 1);
  headers["Content-Range"] = `bytes ${range.start}-${range.end}/${totalSize}`;

  const tagSlice = includeTag ? tag.slice(tagStart, tagEnd + 1) : null;
  const audioStream = includeAudio
    ? await streamSegmentsWithXingPatch(slices, {
        durationSeconds: (asset.duration_ms ?? 0) / 1000,
        audioSize: totalAudio,
      })
    : null;

  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      if (tagSlice) controller.enqueue(tagSlice);
      if (!audioStream) {
        controller.close();
        return;
      }
      const reader = audioStream.getReader();
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        controller.enqueue(chunk.value);
      }
      controller.close();
    },
  });

  return new Response(body, {
    status: 206,
    headers,
  });
}

export function preferredAudioForBooks(repo: BooksRepo): PreferredAudio[] {
  const books = repo.listAllBooks();
  const out: PreferredAudio[] = [];

  for (const book of books) {
    const allAssets = repo.listAssetsByBook(book.id);
    const chosen = selectPreferredAudioAsset(allAssets);
    if (!chosen) continue;
    const files = repo.getAssetFiles(chosen.id);
    const bookRow = repo.getBookRow(book.id);
    if (!bookRow) continue;
    out.push({
      book,
      bookRow,
      asset: chosen,
      files,
    });
  }

  return out.sort((a, b) => b.book.addedAt.localeCompare(a.book.addedAt));
}
