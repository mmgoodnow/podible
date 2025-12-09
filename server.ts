import { promises as fs, createReadStream } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { Readable } from "node:stream";
import { XMLParser } from "fast-xml-parser";

type BookKind = "single" | "multi";

type AudioSegment = {
  path: string;
  name: string;
  size: number;
  start: number;
  end: number;
  durationMs: number;
  title?: string;
};

type Book = {
  id: string;
  title: string;
  author: string;
  kind: BookKind;
  mime: string;
  totalSize: number;
  primaryFile?: string;
  files?: AudioSegment[];
  coverPath?: string;
  durationSeconds?: number;
  publishedAt?: Date;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  isbn?: string;
  identifiers?: Record<string, string>;
  chapters?: ChapterTiming[];
};

const FEED_TITLE = process.env.POD_TITLE ?? "Podible Audiobooks";
const FEED_DESCRIPTION = process.env.POD_DESCRIPTION ?? "Podcast feed for audiobooks";
const FEED_LANGUAGE = process.env.POD_LANGUAGE ?? "en-us";
const FEED_COPYRIGHT = process.env.POD_COPYRIGHT ?? "";
const FEED_AUTHOR = process.env.POD_AUTHOR ?? "Unknown";
const FEED_OWNER_NAME = process.env.POD_OWNER_NAME ?? "Owner";
const FEED_OWNER_EMAIL = process.env.POD_OWNER_EMAIL ?? "owner@example.com";
const rawExplicit = (process.env.POD_EXPLICIT ?? "clean").toLowerCase();
const FEED_EXPLICIT = ["yes", "no", "clean"].includes(rawExplicit) ? rawExplicit : "clean";
const FEED_CATEGORY = process.env.POD_CATEGORY ?? "Arts";
const FEED_TYPE = process.env.POD_TYPE ?? "episodic";
const FEED_IMAGE_URL = process.env.POD_IMAGE_URL;
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  trimValues: false,
  textNodeName: "#text",
});

const durationCache = new Map<
  string,
  {
    mtimeMs: number;
    duration?: number;
    failed?: boolean;
  }
>();

const probeCache = new Map<
  string,
  {
    mtimeMs: number;
    data: ProbeData | null;
  }
>();

type ChapterTiming = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
};

type TranscodeRecord = {
  source: string;
  output: string;
  mtimeMs: number;
};

const scanRoots = (() => {
  const roots = process.argv.slice(2).filter(Boolean);
  if (roots.length === 0) {
    console.error("Pass one or more library roots via argv");
  }
  return roots;
})();

const transcodeDir = (() => {
  const dir = process.env.TRANSCODE_DIR ?? path.join(process.env.TMPDIR ?? "/tmp", "podible-transcodes");
  return dir;
})();

const transcodeManifestPath = path.join(transcodeDir, "manifest.json");

async function ensureTranscodeDir() {
  await fs.mkdir(transcodeDir, { recursive: true });
}

async function readTranscodeManifest(): Promise<TranscodeRecord[]> {
  await ensureTranscodeDir();
  try {
    const content = await fs.readFile(transcodeManifestPath, "utf8");
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) return parsed as TranscodeRecord[];
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn("Failed to read transcode manifest:", (err as Error).message);
    }
  }
  return [];
}

async function writeTranscodeManifest(records: TranscodeRecord[]) {
  await ensureTranscodeDir();
  await fs.writeFile(transcodeManifestPath, JSON.stringify(records, null, 2), "utf8");
}

async function cleanupTranscodes() {
  const manifest = await readTranscodeManifest();
  const kept: TranscodeRecord[] = [];
  for (const record of manifest) {
    const sourceStat = await fs.stat(record.source).catch(() => null);
    const outputStat = await fs.stat(record.output).catch(() => null);
    const stale = !sourceStat || !outputStat || sourceStat.mtimeMs !== record.mtimeMs;
    if (stale) {
      if (outputStat) {
        await fs.unlink(record.output).catch(() => {});
      }
      continue;
    }
    kept.push(record);
  }
  if (kept.length !== manifest.length) {
    await writeTranscodeManifest(kept);
  }
}

function transcodeOutputPath(source: string, sourceStat: Awaited<ReturnType<typeof fs.stat>>): string {
  const extless = path.basename(source, path.extname(source));
  const safeName = slugify(extless) || "book";
  const hash = sourceStat.mtimeMs.toString(36);
  return path.join(transcodeDir, `${safeName}-${hash}.mp3`);
}

function transcodeM4bToMp3(source: string, target: string): void {
  const result = spawnSync(
    "ffmpeg",
    [
      "-y",
      "-i",
      source,
      "-map_metadata",
      "0",
      "-map_chapters",
      "0",
      "-write_id3v2",
      "1",
      "-id3v2_version",
      "3",
      "-codec:a",
      "libmp3lame",
      "-qscale:a",
      "2",
      target,
    ],
    { stdio: "ignore" }
  );
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`ffmpeg failed with status ${result.status}`);
  }
}

async function ensureTranscodedMp3(source: string, sourceStat: Awaited<ReturnType<typeof fs.stat>>): Promise<string | null> {
  await ensureTranscodeDir();
  const manifest = await readTranscodeManifest();
  const existing = manifest.find((rec) => rec.source === source && rec.mtimeMs === sourceStat.mtimeMs);
  if (existing) {
    const exists = await fs.stat(existing.output).catch(() => null);
    if (exists) return existing.output;
  }

  const target = transcodeOutputPath(source, sourceStat);
  try {
    transcodeM4bToMp3(source, target);
    await fs.utimes(target, sourceStat.atime, sourceStat.mtime);
  } catch (err) {
    console.warn(`Failed to transcode ${source}:`, (err as Error).message);
    return null;
  }

  const updatedManifest = manifest.filter((rec) => rec.source !== source);
  updatedManifest.push({ source, output: target, mtimeMs: sourceStat.mtimeMs });
  await writeTranscodeManifest(updatedManifest);
  return target;
}

const port = Number(process.env.PORT ?? 80);

function slugify(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/--+/g, "-");
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

function cleanMetaValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const lowered = trimmed.toLowerCase();
  if (lowered === "unknown" || lowered === "no description") return undefined;
  return trimmed;
}

function normalizeDescriptionHtml(raw: string | undefined): string | undefined {
  const decoded = raw ? decodeXmlEntities(raw) : undefined;
  const cleaned = cleanMetaValue(decoded);
  if (!cleaned) return undefined;
  return cleaned;
}

function htmlToPlainText(html: string | undefined): string | undefined {
  if (!html) return undefined;
  const withBreaks = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<\/li>/gi, "\n")
    .replace(/<li>/gi, "- ");
  const withoutTags = withBreaks.replace(/<[^>]+>/g, "");
  const normalized = withoutTags
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]+/g, " ")
    .trim();
  return normalized || undefined;
}

function toArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function nodeText(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (typeof value === "object" && value && "#text" in (value as Record<string, unknown>)) {
    const text = (value as Record<string, unknown>)["#text"];
    if (typeof text === "string" || typeof text === "number") return String(text);
  }
  return undefined;
}

type OpfMetadata = {
  title?: string;
  author?: string;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  publishedAt?: Date;
  isbn?: string;
  identifiers: Record<string, string>;
};

type AudioTagMetadata = {
  title?: string;
  artist?: string;
  albumArtist?: string;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  date?: Date;
};

type ProbeData = {
  duration?: number;
  tags?: Record<string, string>;
  chapters?: FfprobeChapter[];
};

type FfprobeChapter = {
  start_time?: string;
  end_time?: string;
  tags?: Record<string, string>;
};

function parseOpfContent(content: string): OpfMetadata | null {
  let parsed: any;
  try {
    parsed = xmlParser.parse(content);
  } catch (err) {
    console.warn(`Failed to parse OPF XML: ${(err as Error).message}`);
    return null;
  }
  const metadata = parsed?.package?.metadata;
  if (!metadata) return null;

  const title = cleanMetaValue(nodeText(metadata.title ?? metadata["dc:title"]));
  const author = cleanMetaValue(nodeText(metadata.creator ?? metadata["dc:creator"]));
  const rawDescription = normalizeDescriptionHtml(nodeText(metadata.description ?? metadata["dc:description"]));
  const description = htmlToPlainText(rawDescription);
  const language = cleanMetaValue(nodeText(metadata.language ?? metadata["dc:language"]));
  const rawDate = nodeText(metadata.date ?? metadata["dc:date"]);

  const identifiers: Record<string, string> = {};
  const identifierNodes = toArray(metadata.identifier ?? metadata["dc:identifier"]);
  identifierNodes.forEach((idNode: any) => {
    const value = cleanMetaValue(nodeText(idNode));
    if (!value) return;
    const scheme = typeof idNode === "object" ? idNode.scheme || idNode["opf:scheme"] : undefined;
    if (scheme) identifiers[String(scheme).toLowerCase()] = value;
  });
  const isbn = identifiers["isbn"];

  let publishedAt: Date | undefined;
  if (rawDate) {
    const parsedDate = new Date(rawDate);
    if (!Number.isNaN(parsedDate.getTime())) {
      publishedAt = parsedDate;
    }
  }

  return {
    title,
    author,
    description,
    descriptionHtml: rawDescription,
    language,
    publishedAt,
    isbn,
    identifiers,
  };
}

function parseAudioTagDate(raw: string | undefined): Date | undefined {
  if (!raw) return undefined;
  const cleaned = raw.trim();
  if (!cleaned) return undefined;
  const parsed = new Date(cleaned);
  if (!Number.isNaN(parsed.getTime())) return parsed;
  return undefined;
}

function preferLonger(first?: string, second?: string): string | undefined {
  const a = first?.trim() ?? "";
  const b = second?.trim() ?? "";
  const aLen = a.length;
  const bLen = b.length;
  if (bLen > aLen) return b;
  if (aLen > bLen) return a;
  return bLen > 0 ? b : aLen > 0 ? a : undefined;
}

function readAudioMetadata(filePath: string, mtimeMs: number): AudioTagMetadata | null {
  const probed = probeData(filePath, mtimeMs);
  if (!probed || !probed.tags) return null;
  const tags = probed.tags;
  const descriptionRaw = normalizeDescriptionHtml(
    tags.description || tags.DESCRIPTION || tags.comment || tags.COMMENT
  );
  const description = htmlToPlainText(descriptionRaw);
  return {
    title: cleanMetaValue(tags.title || tags.TITLE),
    artist: cleanMetaValue(tags.artist || tags.ARTIST),
    albumArtist: cleanMetaValue(tags.album_artist || tags.ALBUM_ARTIST),
    description,
    descriptionHtml: descriptionRaw,
    language: cleanMetaValue(tags.language || tags.LANGUAGE),
    date: parseAudioTagDate(tags.date || tags.DATE),
  };
}

function readFfprobeChapters(filePath: string, mtimeMs: number): ChapterTiming[] | null {
  const probed = probeData(filePath, mtimeMs);
  const chapters = probed?.chapters;
  if (!chapters || chapters.length === 0) return null;
  const timings: ChapterTiming[] = [];
  chapters.forEach((chap, index) => {
    const start = Math.max(0, Math.round(Number.parseFloat(chap.start_time ?? "0") * 1000));
    const end = Math.max(start, Math.round(Number.parseFloat(chap.end_time ?? "0") * 1000));
    const tagTitle =
      chap.tags?.title ||
      chap.tags?.TITLE ||
      chap.tags?.name ||
      chap.tags?.NAME ||
      `Chapter ${index + 1}`;
    const title = cleanMetaValue(tagTitle) ?? `Chapter ${index + 1}`;
    timings.push({
      id: `ch${index}`,
      title,
      startMs: start,
      endMs: end,
    });
  });
  return timings;
}

async function readOpfMetadata(bookDir: string, files: string[]): Promise<OpfMetadata | null> {
  const opfFile = files.find((f) => f.toLowerCase().endsWith(".opf"));
  if (!opfFile) return null;
  const opfPath = path.join(bookDir, opfFile);
  try {
    const content = await fs.readFile(opfPath, "utf8");
    return parseOpfContent(content);
  } catch (err) {
    console.warn(`Failed to read OPF for ${bookDir}: ${(err as Error).message}`);
    return null;
  }
}

function normalizeAudioExt(ext: string): "mp3" | "m4a" {
  const lower = ext.toLowerCase();
  if (lower === ".mp3") return "mp3";
  if (lower === ".m4a" || lower === ".m4b" || lower === ".mp4") return "m4a";
  return "mp3";
}

function mimeFromExt(ext: string): string {
  const normalized = normalizeAudioExt(ext);
  return normalized === "m4a" ? "audio/mp4" : "audio/mpeg";
}

function bookExtension(book: Book): string {
  const sourcePath =
    book.kind === "single"
      ? book.primaryFile
      : book.files && book.files.length > 0
        ? book.files[0].path || book.files[0].name
        : undefined;
  if (sourcePath) {
    return normalizeAudioExt(path.extname(sourcePath));
  }
  return normalizeAudioExt(book.mime);
}

function bookMime(book: Book): string {
  const ext = bookExtension(book);
  return mimeFromExt(ext);
}

function bookId(author: string, title: string): string {
  return slugify(`${author}-${title}`);
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function truncate(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 3).trimEnd()}...`;
}

function firstLine(value: string): string {
  return value.split(/\r?\n/).map((part) => part.trim()).find(Boolean) ?? "";
}

async function safeReadDir(dir: string) {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch (err) {
    console.warn(`Skipping unreadable dir ${dir}:`, (err as Error).message);
    return [];
  }
}

async function buildBook(author: string, bookDir: string, title: string): Promise<Book | null> {
  const entries = await safeReadDir(bookDir);
  const files = entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
  const m4bs = files.filter((f) => f.toLowerCase().endsWith(".m4b")).sort();
  const mp3s = files.filter((f) => f.toLowerCase().endsWith(".mp3")).sort();
  const covers = files.filter((f) => f.toLowerCase().endsWith(".jpg")).sort();
  const opf = await readOpfMetadata(bookDir, files);
  const audioMetaSource = m4bs[0] ? path.join(bookDir, m4bs[0]) : mp3s[0] ? path.join(bookDir, mp3s[0]) : null;
  const audioMetaStat = audioMetaSource ? await fs.stat(audioMetaSource).catch(() => null) : null;
  const audioMeta = audioMetaStat ? readAudioMetadata(audioMetaSource!, audioMetaStat.mtimeMs) : null;
  const chapterMeta =
    m4bs[0] && audioMetaStat ? readFfprobeChapters(audioMetaSource!, audioMetaStat.mtimeMs) ?? undefined : undefined;

  const coverPath = covers.length > 0 ? path.join(bookDir, covers[0]) : undefined;
  const displayTitle = audioMeta?.title ?? opf?.title ?? title;
  const displayAuthor = audioMeta?.artist ?? audioMeta?.albumArtist ?? opf?.author ?? author;
  const description = preferLonger(opf?.description, audioMeta?.description);
  const descriptionHtml = preferLonger(opf?.descriptionHtml, audioMeta?.descriptionHtml);
  const language = audioMeta?.language ?? opf?.language;
  const isbn = opf?.isbn;
  const identifiers = opf?.identifiers;

  if (m4bs.length > 0) {
    const filePath = path.join(bookDir, m4bs[0]);
    const stat = audioMetaStat ?? (await fs.stat(filePath).catch(() => null));
    if (!stat) return null;
    const transcoded = await ensureTranscodedMp3(filePath, stat);
    if (!transcoded) return null;
    const targetStat = await fs.stat(transcoded).catch(() => null);
    if (!targetStat) return null;
    const durationSeconds = getDurationSeconds(transcoded, targetStat.mtimeMs);
    const mime = mimeFromExt(path.extname(transcoded));
    return {
      id: bookId(author, title),
      title: displayTitle,
      author: displayAuthor,
      kind: "single",
      mime,
      totalSize: targetStat.size,
      primaryFile: transcoded,
      coverPath,
      durationSeconds,
      publishedAt: opf?.publishedAt ?? audioMeta?.date ?? stat.mtime,
      description,
      descriptionHtml,
      language,
      isbn,
      identifiers,
      chapters: chapterMeta,
    };
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
      if (!stat) continue;
      if ((!publishedAt || !opf?.publishedAt) && (!publishedAt || stat.mtime < publishedAt)) publishedAt = stat.mtime;
      const fileMeta = readAudioMetadata(filePath, stat.mtimeMs);
      const duration = getDurationSeconds(filePath, stat.mtimeMs) ?? 0;
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
    return {
      id: bookId(author, title),
      title: displayTitle,
      author: displayAuthor,
      kind: "multi",
      mime,
      totalSize: segments[segments.length - 1].end + 1,
      files: segments,
      coverPath,
      durationSeconds: durationSeconds,
      publishedAt,
      description,
      descriptionHtml,
      language,
      isbn,
      identifiers,
    };
  }

  return null;
}

async function scanBooks(): Promise<Book[]> {
  const books: Book[] = [];
  for (const root of scanRoots) {
    const authors = await safeReadDir(root);
    for (const authorEntry of authors) {
      if (!authorEntry.isDirectory()) continue;
      const author = authorEntry.name;
      const authorPath = path.join(root, author);
      const bookDirs = await safeReadDir(authorPath);
      for (const bookEntry of bookDirs) {
        if (!bookEntry.isDirectory()) continue;
        const book = await buildBook(author, path.join(authorPath, bookEntry.name), bookEntry.name);
        if (book) books.push(book);
      }
    }
  }
  books.sort((a, b) => {
    const at = a.publishedAt ? a.publishedAt.getTime() : 0;
    const bt = b.publishedAt ? b.publishedAt.getTime() : 0;
    return bt - at;
  });
  return books;
}

async function findBookById(id: string): Promise<Book | null> {
  for (const root of scanRoots) {
    const authors = await safeReadDir(root);
    for (const authorEntry of authors) {
      if (!authorEntry.isDirectory()) continue;
      const author = authorEntry.name;
      const authorPath = path.join(root, author);
      const bookDirs = await safeReadDir(authorPath);
      for (const bookEntry of bookDirs) {
        if (!bookEntry.isDirectory()) continue;
        const candidate = await buildBook(author, path.join(authorPath, bookEntry.name), bookEntry.name);
        if (candidate && candidate.id === id) return candidate;
      }
    }
  }
  return null;
}

function parseRange(rangeHeader: string | null, size: number): { start: number; end: number } | null {
  if (!rangeHeader) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader);
  if (!match) return null;
  let start = match[1] ? Number(match[1]) : 0;
  let end = match[2] ? Number(match[2]) : size - 1;
  if (Number.isNaN(start) || Number.isNaN(end)) return null;
  if (match[1] === "" && match[2] !== "") {
    // suffix range
    const length = end;
    start = size - length;
    end = size - 1;
  }
  if (start < 0 || end < start || start >= size) return null;
  end = Math.min(end, size - 1);
  return { start, end };
}

function segmentsForRange(files: AudioSegment[], start: number, end: number): AudioSegment[] {
  return files
    .map((file) => {
      if (file.end < start || file.start > end) return null;
      const relativeStart = Math.max(start, file.start) - file.start;
      const relativeEnd = Math.min(end, file.end) - file.start;
      return {
        ...file,
        start: relativeStart,
        end: relativeEnd,
      };
    })
    .filter((f): f is AudioSegment => Boolean(f));
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function synchsafeSize(size: number): Uint8Array {
  const out = new Uint8Array(4);
  out[0] = (size >> 21) & 0x7f;
  out[1] = (size >> 14) & 0x7f;
  out[2] = (size >> 7) & 0x7f;
  out[3] = size & 0x7f;
  return out;
}

function writeUint32BE(value: number): Uint8Array {
  const out = new Uint8Array(4);
  out[0] = (value >>> 24) & 0xff;
  out[1] = (value >>> 16) & 0xff;
  out[2] = (value >>> 8) & 0xff;
  out[3] = value & 0xff;
  return out;
}

const encoder = new TextEncoder();

function id3Frame(id: string, payload: Uint8Array): Uint8Array {
  const header = new Uint8Array(10);
  header.set(encoder.encode(id).slice(0, 4));
  header.set(synchsafeSize(payload.byteLength), 4);
  // flags remain zeroed
  return concatBytes([header, payload]);
}

function textFrame(id: string, text: string): Uint8Array {
  const textBytes = encoder.encode(text);
  const payload = new Uint8Array(1 + textBytes.length);
  payload[0] = 0x03; // UTF-8
  payload.set(textBytes, 1);
  return id3Frame(id, payload);
}

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "";
  const total = Math.floor(seconds);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}:${m.toString().padStart(2, "0")}:${s.toString().padStart(2, "0")}`;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

function chapFrame(chapterId: string, title: string, startMs: number, endMs: number): Uint8Array {
  const idBytes = encoder.encode(chapterId);
  const titleFrame = textFrame("TIT2", title);
  const payload = concatBytes([
    idBytes,
    new Uint8Array([0x00]), // terminator
    writeUint32BE(startMs),
    writeUint32BE(endMs),
    writeUint32BE(0xffffffff), // start offset: unknown
    writeUint32BE(0xffffffff), // end offset: unknown
    titleFrame,
  ]);
  return id3Frame("CHAP", payload);
}

function ctocFrame(childIds: string[]): Uint8Array {
  const elementId = encoder.encode("toc");
  const childrenBytes = concatBytes(
    childIds.map((id) => concatBytes([encoder.encode(id), new Uint8Array([0x00])]))
  );
  const titleFrame = textFrame("TIT2", "Chapters");
  const payload = concatBytes([
    elementId,
    new Uint8Array([0x00]), // terminator
    new Uint8Array([0x03]), // flags: top-level + ordered
    new Uint8Array([childIds.length]),
    childrenBytes,
    titleFrame,
  ]);
  return id3Frame("CTOC", payload);
}

function buildId3ChaptersTag(timings: ChapterTiming[]): Uint8Array {
  if (timings.length === 0) return new Uint8Array();
  const childIds = timings.map((c) => c.id);
  const frames = [ctocFrame(childIds), ...timings.map((chap) => chapFrame(chap.id, chap.title, chap.startMs, chap.endMs))];
  const framesBytes = concatBytes(frames);
  const header = new Uint8Array(10);
  header.set(encoder.encode("ID3"));
  header[3] = 0x04; // version 2.4.0
  header[4] = 0x00;
  header[5] = 0x00; // flags
  header.set(synchsafeSize(framesBytes.byteLength), 6);
  return concatBytes([header, framesBytes]);
}

function estimateId3TagLength(book: Book): number {
  if (book.kind !== "multi" || !book.files) return 0;
  const dummyTimings: ChapterTiming[] = book.files.map((segment, index) => ({
    id: `ch${index}`,
    title: path.basename(segment.name, path.extname(segment.name)),
    startMs: 0,
    endMs: 0,
  }));
  return buildId3ChaptersTag(dummyTimings).byteLength;
}

function streamSegments(segments: AudioSegment[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    async pull(controller) {
      if (index >= segments.length) {
        controller.close();
        return;
      }
      const segment = segments[index];
      const reader = Readable.toWeb(
        createReadStream(segment.path, { start: segment.start, end: segment.end })
      ).getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        controller.enqueue(value);
      }
      index += 1;
    },
  });
}

function probeData(filePath: string, mtimeMs: number): ProbeData | null {
  const cached = probeCache.get(filePath);
  if (cached && cached.mtimeMs === mtimeMs) return cached.data;
  const result = spawnSync(
    "ffprobe",
    ["-v", "error", "-print_format", "json", "-show_format", "-show_chapters", filePath],
    { encoding: "utf8" }
  );
  if (result.error || result.status !== 0) {
    const message = result.error ? result.error.message : result.stderr || String(result.status);
    console.warn(`ffprobe failed for ${filePath}: ${message}`);
    probeCache.set(filePath, { mtimeMs, data: null });
    return null;
  }
  try {
    const parsed = JSON.parse(result.stdout);
    const format = parsed?.format ?? {};
    const durationStr: string | undefined = format.duration;
    const duration = durationStr ? Number.parseFloat(durationStr) : undefined;
    const tags = format.tags as Record<string, string> | undefined;
    const chapters = (parsed?.chapters ?? []) as FfprobeChapter[];
    const data: ProbeData = {
      duration: Number.isFinite(duration) ? duration : undefined,
      tags,
      chapters,
    };
    probeCache.set(filePath, { mtimeMs, data });
    return data;
  } catch (err) {
    console.warn(`Failed to parse ffprobe output for ${filePath}: ${(err as Error).message}`);
    probeCache.set(filePath, { mtimeMs, data: null });
    return null;
  }
}

function getDurationSeconds(filePath: string, mtimeMs: number): number | undefined {
  const cached = durationCache.get(filePath);
  if (cached && cached.mtimeMs === mtimeMs) {
    if (cached.failed) return undefined;
    return cached.duration;
  }
  const probed = probeData(filePath, mtimeMs);
  if (!probed || probed.duration === undefined) {
    durationCache.set(filePath, { mtimeMs, failed: true });
    return undefined;
  }
  durationCache.set(filePath, { mtimeMs, duration: probed.duration, failed: false });
  return probed.duration;
}

async function buildChapterTimings(book: Book): Promise<ChapterTiming[] | null> {
  if (book.kind === "single") {
    if (book.chapters && book.chapters.length > 0) return book.chapters;
    return null;
  }
  if (!book.files) return null;
  const timings: ChapterTiming[] = [];
  let cursorMs = 0;
  book.files.forEach((segment, index) => {
    const durationMs = segment.durationMs;
    const startMs = cursorMs;
    const endMs = startMs + durationMs;
    const chapterTitle =
      segment.title ||
      path.basename(segment.name, path.extname(segment.name)) ||
      `Part ${index + 1}`;
    timings.push({
      id: `ch${index}`,
      title: chapterTitle,
      startMs,
      endMs,
    });
    cursorMs = endMs;
  });
  return timings;
}

async function buildChapters(book: Book) {
  const timings = await buildChapterTimings(book);
  if (!timings) return null;
  return {
    version: "1.2.0",
    chapters: timings.map((chap) => ({
      startTime: chap.startMs / 1000,
      title: chap.title,
    })),
  };
}

function formatDateIso(date: Date | undefined): string | undefined {
  if (!date) return undefined;
  const iso = date.toISOString();
  return iso.slice(0, 10);
}

function bookIsbn(book: Book): string | undefined {
  if (book.isbn) return book.isbn;
  const identifiers = book.identifiers ?? {};
  return identifiers["isbn"] ?? identifiers["ISBN"];
}

function cleanLanguage(language: string | undefined): string | undefined {
  if (!language) return undefined;
  const trimmed = language.trim();
  if (!trimmed) return undefined;
  if (trimmed.toLowerCase() === "unknown") return undefined;
  return trimmed;
}

function buildItemNotes(book: Book): { description: string; subtitle: string; descriptionHtml?: string } {
  const baseDescription = book.description?.trim() ?? htmlToPlainText(book.descriptionHtml)?.trim();
  const summaryParts: string[] = [];
  if (baseDescription) {
    summaryParts.push(baseDescription);
  } else {
    summaryParts.push(`${book.title} by ${book.author}`);
  }

  const detailBits: string[] = [];
  const language = cleanLanguage(book.language);
  const isbn = bookIsbn(book);
  const published = formatDateIso(book.publishedAt);
  if (language) detailBits.push(`Language: ${language}`);
  if (isbn) detailBits.push(`ISBN: ${isbn}`);
  if (published) detailBits.push(`Published: ${published}`);
  if (book.kind === "multi" && book.files?.length) detailBits.push(`Parts: ${book.files.length}`);
  if (book.durationSeconds) {
    const mins = Math.round((book.durationSeconds / 60) * 10) / 10;
    detailBits.push(`Length: ${mins} min`);
  }

  if (detailBits.length > 0) {
    summaryParts.push(detailBits.join(" • "));
  }

  const description = summaryParts.join("\n\n");
  const subtitleSource = baseDescription || book.author || book.title;
  const subtitle = truncate(firstLine(subtitleSource), 200) || book.author;
  return { description, subtitle, descriptionHtml: book.descriptionHtml };
}

function rssFeed(books: Book[], origin: string): { body: string; lastModified: Date } {
  const firstCover = books.find((b) => b.coverPath);
  const channelImage = FEED_IMAGE_URL || (firstCover ? `${origin}/covers/${firstCover.id}.jpg` : "");
  const latestMtime = books
    .map((b) => b.publishedAt?.getTime() ?? 0)
    .filter((t) => t > 0);
  const lastModifiedMs = latestMtime.length > 0 ? Math.max(...latestMtime) : Date.now();
  const lastModified = new Date(lastModifiedMs);
  const pubDate = lastModified.toUTCString();
  const items = books
    .map((book) => {
      const ext = bookExtension(book);
      const mime = bookMime(book);
      const enclosureUrl = `${origin}/stream/${book.id}.${ext}`;
      const cover = book.coverPath ? `<itunes:image href="${origin}/covers/${book.id}.jpg" />` : "";
      const tagLength = estimateId3TagLength(book);
      const enclosureLength = book.totalSize + tagLength;
      const durationSeconds = book.durationSeconds ?? 0;
      const duration = formatDuration(durationSeconds);
      const itemPubDate = (book.publishedAt ?? lastModified).toUTCString();
      const fallbackDescription = `${book.title} by ${book.author}`;
      const hasChapters = book.kind === "multi" || (book.chapters && book.chapters.length > 0);
      const chaptersTag = hasChapters
        ? `<podcast:chapters url="${origin}/chapters/${book.id}.json" type="application/json+chapters" />`
        : "";
      const chaptersDebugTag = hasChapters
        ? `<podcast:chaptersDebug url="${origin}/chapters-debug/${book.id}.json" type="application/json" />`
        : "";
      const { description, subtitle, descriptionHtml } = buildItemNotes(book);
      const descriptionForXml = descriptionHtml
        ? `<![CDATA[${descriptionHtml.replace(/]]>/g, "]]]]><![CDATA[>")}]]>`
        : escapeXml(description || fallbackDescription);
      return [
        "<item>",
        `<guid isPermaLink="false">${escapeXml(book.id)}</guid>`,
        `<title>${escapeXml(book.title)}</title>`,
        `<itunes:author>${escapeXml(book.author)}</itunes:author>`,
        `<itunes:subtitle>${escapeXml(subtitle)}</itunes:subtitle>`,
        `<enclosure url="${enclosureUrl}" length="${enclosureLength}" type="${mime}" />`,
        `<link>${enclosureUrl}</link>`,
        `<pubDate>${itemPubDate}</pubDate>`,
        `<description>${descriptionForXml}</description>`,
        `<itunes:summary>${escapeXml(description || fallbackDescription)}</itunes:summary>`,
        `<itunes:explicit>${FEED_EXPLICIT}</itunes:explicit>`,
        duration ? `<itunes:duration>${duration}</itunes:duration>` : "",
        `<itunes:episodeType>full</itunes:episodeType>`,
        cover,
        chaptersTag,
        chaptersDebugTag,
        "</item>",
      ]
        .filter(Boolean)
        .join("");
    })
    .join("");

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
<title>${escapeXml(FEED_TITLE)}</title>
<link>${origin}/feed.xml</link>
<atom:link href="${origin}/feed.xml" rel="self" type="application/rss+xml" />
<description>${escapeXml(FEED_DESCRIPTION)}</description>
<language>${FEED_LANGUAGE}</language>
<copyright>${escapeXml(FEED_COPYRIGHT)}</copyright>
<lastBuildDate>${pubDate}</lastBuildDate>
<itunes:subtitle>${escapeXml(FEED_DESCRIPTION)}</itunes:subtitle>
<itunes:author>${escapeXml(FEED_AUTHOR)}</itunes:author>
<itunes:summary>${escapeXml(FEED_DESCRIPTION)}</itunes:summary>
<itunes:explicit>${FEED_EXPLICIT}</itunes:explicit>
<itunes:owner><itunes:name>${escapeXml(FEED_OWNER_NAME)}</itunes:name><itunes:email>${escapeXml(FEED_OWNER_EMAIL)}</itunes:email></itunes:owner>
${channelImage ? `<itunes:image href="${channelImage}" />` : ""}
<itunes:category text="${escapeXml(FEED_CATEGORY)}" />
<itunes:type>${FEED_TYPE}</itunes:type>
${items}
</channel>
</rss>`;

  return { body, lastModified };
}

function requestOrigin(request: Request): string {
  const url = new URL(request.url);
  const forwardedProto = request.headers.get("x-forwarded-proto");
  const proto = forwardedProto ? forwardedProto.split(",")[0].trim() : url.protocol.replace(":", "");
  return `${proto}://${url.host}`;
}

async function handleFeed(request: Request): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const origin = requestOrigin(request);
  const books = await scanBooks();
  const { body, lastModified } = rssFeed(books, origin);
  const etag = `W/"${lastModified.getTime()}"`;
  return new Response(body, {
    headers: {
      "Content-Type": "application/rss+xml",
      "Last-Modified": lastModified.toUTCString(),
      ETag: etag,
    },
  });
}

async function handleFeedDebug(request: Request): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const origin = requestOrigin(request);
  const books = await scanBooks();
  const { body, lastModified } = rssFeed(books, origin);
  const etag = `W/"${lastModified.getTime()}"`;
  return new Response(body, {
    headers: {
      "Content-Type": "text/xml; charset=utf-8",
      "Content-Disposition": "inline",
      "Last-Modified": lastModified.toUTCString(),
      ETag: etag,
    },
  });
}

async function handleStream(request: Request, bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book) return new Response("Not found", { status: 404 });
  const rangeHeader = request.headers.get("range");
  const mime = bookMime(book);
  const headers: Record<string, string> = {
    "Accept-Ranges": "bytes",
    "Content-Type": mime,
  };

  if (book.kind === "single" && book.primaryFile) {
    const size = book.totalSize;
    const file = Bun.file(book.primaryFile);
    const range = parseRange(rangeHeader, size);
    if (!range) {
      headers["Content-Length"] = String(size);
      return new Response(file, { status: 200, headers });
    }
    headers["Content-Length"] = String(range.end - range.start + 1);
    headers["Content-Range"] = `bytes ${range.start}-${range.end}/${size}`;
    return new Response(file.slice(range.start, range.end + 1), { status: 206, headers });
  }

  const files = book.files ?? [];
  const timings = await buildChapterTimings(book);
  if (!timings) return new Response("Not found", { status: 404 });
  const tag = buildId3ChaptersTag(timings);
  const tagLength = tag.byteLength;
  const audioSize = book.totalSize;
  const totalSize = tagLength + audioSize;
  const range = parseRange(rangeHeader, totalSize) ?? { start: 0, end: totalSize - 1 };
  if (range.start >= totalSize) {
    headers["Content-Range"] = `bytes */${totalSize}`;
    return new Response("Range Not Satisfiable", { status: 416, headers });
  }

  const tagStart = range.start;
  const tagEnd = Math.min(range.end, tagLength - 1);
  const includeTag = tagStart < tagLength;
  const audioRangeStart = Math.max(range.start, tagLength) - tagLength;
  const audioRangeEnd = range.end - tagLength;
  const includeAudio = range.end >= tagLength;

  headers["Content-Length"] = String(range.end - range.start + 1);
  if (rangeHeader) {
    headers["Content-Range"] = `bytes ${range.start}-${range.end}/${totalSize}`;
  }
  const status = rangeHeader ? 206 : 200;

  if (includeTag && !includeAudio) {
    return new Response(tag.slice(tagStart, tagEnd + 1), { status, headers });
  }

  const audioSlices = includeAudio ? segmentsForRange(files, audioRangeStart, audioRangeEnd) : [];
  if (includeAudio && audioSlices.length === 0) {
    headers["Content-Range"] = `bytes */${totalSize}`;
    return new Response("Range Not Satisfiable", { status: 416, headers });
  }

  const tagSlice = includeTag ? tag.slice(tagStart, tagEnd + 1) : null;
  const audioStream = includeAudio ? streamSegments(audioSlices) : null;
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      if (tagSlice) {
        controller.enqueue(tagSlice);
      }
      if (!audioStream) {
        controller.close();
        return;
      }
      const reader = audioStream.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        controller.enqueue(value);
      }
      controller.close();
    },
  });

  return new Response(body, { status, headers });
}

async function handleChapters(bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book) return new Response("Not found", { status: 404 });
  const chapters = await buildChapters(book);
  if (!chapters) return new Response("Not found", { status: 404 });
  return new Response(JSON.stringify(chapters, null, 2), {
    headers: { "Content-Type": "application/json+chapters" },
  });
}

async function handleChaptersDebug(bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book) return new Response("Not found", { status: 404 });
  const chapters = await buildChapters(book);
  if (!chapters) return new Response("Not found", { status: 404 });
  return new Response(JSON.stringify(chapters, null, 2), {
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

async function handleCover(bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book || !book.coverPath) return new Response("Not found", { status: 404 });
  const file = Bun.file(book.coverPath);
  if (!(await file.exists())) return new Response("Not found", { status: 404 });
  return new Response(file, {
    headers: { "Content-Type": "image/jpeg" },
  });
}

Bun.serve({
  port,
  fetch: (request: Request) => {
    const url = new URL(request.url);
    const pathname = url.pathname;
    if (pathname === "/feed.xml") return handleFeed(request);
    if (pathname === "/feed-debug.xml") return handleFeedDebug(request);
    if (pathname.startsWith("/stream/")) {
      const idWithExt = pathname.replace("/stream/", "");
      const id = idWithExt.replace(/\.(mp3|m4a|m4b|mp4)$/i, "");
      return handleStream(request, id);
    }
    if (pathname.startsWith("/chapters/")) {
      const [, , idWithExt = ""] = pathname.split("/");
      const id = idWithExt.replace(/\.json$/, "");
      return handleChapters(id);
    }
    if (pathname.startsWith("/chapters-debug/")) {
      const [, , idWithExt = ""] = pathname.split("/");
      const id = idWithExt.replace(/\.json$/, "");
      return handleChaptersDebug(id);
    }
    if (pathname.startsWith("/covers/")) {
      const [, , idWithExt = ""] = pathname.split("/");
      const id = idWithExt.replace(/\.jpg$/, "");
      return handleCover(id);
    }
    return new Response("Not found", { status: 404 });
  },
});

const localBase = `http://localhost${port === 80 ? "" : `:${port}`}`;
console.log(`Listening on port ${port}. Roots: ${scanRoots.join(", ") || "none"}`);
console.log(`Feed: ${localBase}/feed.xml`);
console.log(`Feed (debug/plain): ${localBase}/feed-debug.xml`);

cleanupTranscodes().catch((err) => {
  console.warn("Transcode cleanup failed:", err);
});

async function logInitialScan() {
  if (scanRoots.length === 0) return;
  const books = await scanBooks();
  const authors = new Set(books.map((b) => b.author));
  const singles = books.filter((b) => b.kind === "single").length;
  const multis = books.filter((b) => b.kind === "multi").length;
  const covers = books.filter((b) => Boolean(b.coverPath)).length;
  console.log(
    `Initial scan: ${books.length} books (${singles} single m4b, ${multis} multi mp3) from ${authors.size} authors, covers: ${covers}`
  );
  if (books.length === 0) return;
  const sample = books[0];
  const ext = bookExtension(sample);
  console.log(`Sample stream: ${localBase}/stream/${sample.id}.${ext}`);
  const multiWithChapters = books.find((b) => b.kind === "multi");
  if (multiWithChapters) {
    console.log(`Sample chapters: ${localBase}/chapters/${multiWithChapters.id}.json`);
    console.log(`Sample chapters (debug): ${localBase}/chapters-debug/${multiWithChapters.id}.json`);
  }
  const withCover = books.find((b) => b.coverPath);
  if (withCover) {
    console.log(`Sample cover: ${localBase}/covers/${withCover.id}.jpg`);
  }
}

logInitialScan().catch((err) => {
  console.error("Initial scan failed:", err);
});
