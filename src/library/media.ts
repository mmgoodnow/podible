import { promises as fs } from "node:fs";
import path from "node:path";

import { parseRange, segmentsForRange, streamSegmentsWithXingPatch } from "../streaming/range";
import { buildId3ChaptersTag } from "../streaming/id3";
import { loadStoredTranscriptPayload } from "./chapter-analysis";
import type { StoredTranscriptUtterance } from "./chapter-analysis";
import { readFfprobeChapters } from "../media/probe-cache";
import { selectPreferredAudioAsset, selectPreferredAudioManifestation } from "./asset-selection";

import type { BooksRepo } from "../repo";
import type { AssetFileRow, AssetRow, BookRow, LibraryBook, ManifestationRow } from "../app-types";
import type { AudioSegment } from "../types";

export { selectPreferredAudioAsset, selectPreferredAudioManifestation } from "./asset-selection";

export type PreferredAudio = {
  book: LibraryBook;
  bookRow: BookRow;
  asset: AssetRow;
  files: AssetFileRow[];
};

// New manifestation-shaped result. Today every manifestation has exactly one
// container, so `containers[0]` is the asset that the legacy PreferredAudio
// would have surfaced. Multi-container manifestations (GraphicAudio etc.)
// will arrive in step 3.
export type PreferredManifestation = {
  book: LibraryBook;
  bookRow: BookRow;
  manifestation: ManifestationRow;
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>;
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

export function streamExtensionForManifestation(containers: Array<{ asset: AssetRow }>): string {
  const primary = containers.find((container) => container.asset.kind !== "ebook")?.asset;
  return primary ? streamExtension(primary) : "bin";
}

function containerDurationMs(asset: AssetRow, files: AssetFileRow[]): number {
  return asset.duration_ms ?? files.reduce((sum, file) => sum + file.duration_ms, 0);
}

export function manifestationDurationMs(
  manifestation: Pick<ManifestationRow, "duration_ms">,
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>
): number | null {
  if (manifestation.duration_ms !== null) return manifestation.duration_ms;
  const audioContainers = containers.filter((container) => container.asset.kind !== "ebook");
  if (audioContainers.length === 0) return null;
  return audioContainers.reduce((sum, container) => sum + containerDurationMs(container.asset, container.files), 0);
}

function containerSizeBytes(asset: AssetRow, files: AssetFileRow[]): number {
  return asset.total_size || files.reduce((sum, file) => sum + file.size, 0);
}

function flattenAudioFiles(containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>): AudioSegment[] {
  const out: AudioSegment[] = [];
  let cursor = 0;
  for (const container of containers) {
    for (const file of container.files) {
      const start = cursor;
      const end = start + file.size - 1;
      out.push({
        path: file.path,
        name: path.basename(file.path),
        size: file.size,
        start,
        end,
        durationMs: file.duration_ms,
        title: file.title ?? undefined,
      });
      cursor = end + 1;
    }
  }
  return out;
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

const GENERIC_CHAPTER_LABEL_RES: RegExp[] = [
  /^\s*chapter\s+[0-9a-z]+\s*$/i, // "Chapter 1", "chapter one"
  /^\s*ch\.?\s*[0-9a-z]+\s*$/i, // "Ch 1", "Ch. 12"
  /^\s*[0-9]{1,4}\s*$/, // "1", "001"
  /^\s*track\s+[0-9a-z]+\s*$/i, // "Track 01"
  /^\s*part\s+[0-9a-z]+\s*$/i, // "Part 2"
];
const TRANSCRIPT_LABEL_MAX_CHARS = 60;

export function isGenericChapterLabel(label: string): boolean {
  return GENERIC_CHAPTER_LABEL_RES.some((re) => re.test(label));
}

export function pickTranscriptLabelForWindow(
  utterances: StoredTranscriptUtterance[],
  windowStartMs: number,
  windowEndMs: number
): string | null {
  // Find the first utterance whose midpoint is inside the window.
  const windowMs = windowEndMs - windowStartMs;
  if (windowMs <= 0) return null;
  for (const u of utterances) {
    const mid = (u.startMs + u.endMs) / 2;
    if (mid < windowStartMs) continue;
    if (mid >= windowEndMs) return null;
    const text = u.text.trim().replace(/\s+/g, " ");
    if (!text) continue;
    if (text.length <= TRANSCRIPT_LABEL_MAX_CHARS) return stripTerminalPunctuation(text);
    // Truncate at the last word boundary before the limit, then add ellipsis.
    const slice = text.slice(0, TRANSCRIPT_LABEL_MAX_CHARS);
    const lastSpace = slice.lastIndexOf(" ");
    const cut = lastSpace > TRANSCRIPT_LABEL_MAX_CHARS / 2 ? slice.slice(0, lastSpace) : slice;
    return `${stripTerminalPunctuation(cut)}…`;
  }
  return null;
}

function stripTerminalPunctuation(text: string): string {
  return text.replace(/[\s.,:;!?\-—–]+$/u, "");
}

export function applyTranscriptLabels(timings: ChapterTiming[], utterances: StoredTranscriptUtterance[]): ChapterTiming[] {
  if (utterances.length === 0) return timings;
  const sorted = [...utterances].sort((a, b) => a.startMs - b.startMs);
  return timings.map((chapter) => {
    if (!isGenericChapterLabel(chapter.title)) return chapter;
    const label = pickTranscriptLabelForWindow(sorted, chapter.startMs, chapter.endMs);
    if (label) return { ...chapter, title: label };
    // Transcript is available but has nothing inside this window — mark explicitly
    // so the UI doesn't read as if this were a normal content chapter.
    return { ...chapter, title: `Unknown (${chapter.title.trim()})` };
  });
}

async function buildChapterTimings(repo: BooksRepo, asset: AssetRow, files: AssetFileRow[]): Promise<ChapterTiming[] | null> {
  const timings = await buildFallbackChapterTimings(asset, files);
  if (!timings || timings.length === 0) return timings;
  if (asset.kind === "ebook") return timings;
  const transcript = await loadStoredTranscriptPayload(repo, asset.id).catch(() => null);
  const utterances = transcript?.utterances ?? [];
  if (utterances.length === 0) return timings;
  return applyTranscriptLabels(timings, utterances);
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

async function buildManifestationChapterTimings(
  repo: BooksRepo,
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>
): Promise<ChapterTiming[] | null> {
  const audioContainers = containers.filter((container) => container.asset.kind !== "ebook");
  if (audioContainers.length === 0) return null;
  if (audioContainers.length === 1) {
    const container = audioContainers[0]!;
    return buildChapterTimings(repo, container.asset, container.files);
  }

  const out: ChapterTiming[] = [];
  let timeCursor = 0;
  let byteCursor = 0;

  for (const [containerIndex, container] of audioContainers.entries()) {
    const durationMs = containerDurationMs(container.asset, container.files);
    const sizeBytes = containerSizeBytes(container.asset, container.files);
    const timings = await buildChapterTimings(repo, container.asset, container.files);
    const containerTimings =
      timings && timings.length > 0
        ? timings
        : [
            {
              id: "container",
              title: container.files[0]?.title ?? `Part ${containerIndex + 1}`,
              startMs: 0,
              endMs: durationMs,
              startOffset: 0,
              endOffset: Math.max(0, sizeBytes - 1),
            },
          ];

    for (const chapter of containerTimings) {
      out.push({
        ...chapter,
        id: `c${containerIndex}-${chapter.id}`,
        startMs: timeCursor + chapter.startMs,
        endMs: timeCursor + chapter.endMs,
        startOffset: chapter.startOffset === undefined ? undefined : byteCursor + chapter.startOffset,
        endOffset: chapter.endOffset === undefined ? undefined : byteCursor + chapter.endOffset,
      });
    }

    timeCursor += durationMs;
    byteCursor += sizeBytes;
  }

  return out.length > 0 ? out : null;
}

export async function buildManifestationChapters(
  repo: BooksRepo,
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>
): Promise<{ version: string; chapters: Array<{ startTime: number; title: string }> } | null> {
  const timings = await buildManifestationChapterTimings(repo, containers);
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

async function readCoverArt(coverPath?: string | null): Promise<{ mime: string; data: Uint8Array<ArrayBufferLike> } | undefined> {
  if (!coverPath) return undefined;
  const bytes = await fs.readFile(coverPath).catch(() => null);
  if (!bytes || bytes.length === 0) return undefined;
  return { mime: coverMimeFromPath(coverPath), data: bytes };
}

async function streamTaggedAudioSegments(
  request: Request,
  mime: string,
  segments: AudioSegment[],
  timings: ChapterTiming[] | null,
  durationMs: number,
  coverPath?: string | null
): Promise<Response> {
  const rangeHeader = request.headers.get("range");
  const headers: Record<string, string> = {
    "Accept-Ranges": "bytes",
    "Content-Type": mime,
  };

  let tag: Uint8Array<ArrayBufferLike> = new Uint8Array();
  if (timings && timings.length > 0) {
    const coverArt = await readCoverArt(coverPath);
    tag = buildId3ChaptersTag(timings, coverArt);
    if (timings.some((chapter) => chapter.startOffset !== undefined || chapter.endOffset !== undefined)) {
      tag = buildId3ChaptersTag(timings, coverArt, tag.byteLength);
    }
  }

  const totalAudio = segments.reduce((sum, file) => sum + file.size, 0);
  const totalSize = totalAudio + tag.byteLength;
  if (totalSize <= 0) return new Response("Not found", { status: 404 });

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
  const slices = includeAudio ? segmentsForRange(segments, audioStart, audioEnd) : [];

  headers["Content-Length"] = String(range.end - range.start + 1);
  headers["Content-Range"] = `bytes ${range.start}-${range.end}/${totalSize}`;

  const tagSlice = includeTag ? tag.slice(tagStart, tagEnd + 1) : null;
  const audioStream = includeAudio
    ? await streamSegmentsWithXingPatch(slices, {
        durationSeconds: durationMs / 1000,
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

  return streamTaggedAudioSegments(
    request,
    asset.mime,
    flattenAudioFiles([{ asset, files }]),
    await buildChapterTimings(repo, asset, files),
    asset.duration_ms ?? 0,
    coverPath
  );
}

export async function streamAudioManifestation(
  request: Request,
  repo: BooksRepo,
  manifestation: ManifestationRow,
  containers: Array<{ asset: AssetRow; files: AssetFileRow[] }>,
  coverPath?: string | null
): Promise<Response> {
  if (manifestation.kind !== "audio") return new Response("Not found", { status: 404 });
  const audioContainers = containers.filter((container) => container.asset.kind !== "ebook");
  if (audioContainers.length === 0) return new Response("Not found", { status: 404 });
  if (audioContainers.length === 1) {
    const container = audioContainers[0]!;
    return streamAudioAsset(request, repo, container.asset, container.files, coverPath);
  }

  const primary = audioContainers[0]!.asset;
  return streamTaggedAudioSegments(
    request,
    primary.mime,
    flattenAudioFiles(audioContainers),
    await buildManifestationChapterTimings(repo, audioContainers),
    manifestation.duration_ms ?? audioContainers.reduce((sum, container) => sum + containerDurationMs(container.asset, container.files), 0),
    coverPath
  );
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

// Manifestation-shaped equivalent of preferredAudioForBooks. Picks the best
// audio manifestation per book, returns it with all its containers and their
// files. Today this is functionally identical to preferredAudioForBooks since
// each manifestation has exactly one container; the difference becomes
// visible once multi-container manifestations exist.
export function preferredAudioManifestationsForBooks(repo: BooksRepo): PreferredManifestation[] {
  const books = repo.listAllBooks();
  const out: PreferredManifestation[] = [];

  for (const book of books) {
    const manifestations = repo.listManifestationsByBook(book.id);
    const candidates = manifestations.map((manifestation) => ({
      manifestation,
      containers: repo.listAssetsByManifestation(manifestation.id),
    }));
    const chosen = selectPreferredAudioManifestation(candidates);
    if (!chosen) continue;
    const bookRow = repo.getBookRow(book.id);
    if (!bookRow) continue;
    out.push({
      book,
      bookRow,
      manifestation: chosen.manifestation,
      containers: chosen.containers.map((asset) => ({ asset, files: repo.getAssetFiles(asset.id) })),
    });
  }

  return out.sort((a, b) => b.book.addedAt.localeCompare(a.book.addedAt));
}
