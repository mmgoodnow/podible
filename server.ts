import { promises as fs, createReadStream, readFileSync, writeFileSync, watch } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { Readable } from "node:stream";
import { XMLParser } from "fast-xml-parser";
import { randomBytes } from "node:crypto";
import {
  AudioSegment,
  AudioTagMetadata,
  Book,
  BookBuildResult,
  BookKind,
  ChapterTiming,
  FfprobeChapter,
  JobChannel,
  OpfMetadata,
  PendingSingleMeta,
  ProbeData,
  TranscodeJob,
  TranscodeState,
  TranscodeStatus,
} from "./src/types";
import {
  FEED_AUTHOR,
  FEED_CATEGORY,
  FEED_COPYRIGHT,
  FEED_DESCRIPTION,
  FEED_EXPLICIT,
  FEED_IMAGE_URL,
  FEED_LANGUAGE,
  FEED_OWNER_EMAIL,
  FEED_OWNER_NAME,
  FEED_TITLE,
  FEED_TYPE,
  apiKeyPath,
  brandImageExists,
  brandImagePath,
  dataDir,
  ensureDataDir,
  ensureDataDirSync,
  libraryIndexPath,
  port,
  probeCachePath,
  transcodeStatusPath,
} from "./src/config";
import {
  cleanMetaValue,
  decodeXmlEntities,
  escapeXml,
  firstLine,
  htmlToPlainText,
  nodeText,
  normalizeDescriptionHtml,
  slugify,
  toArray,
  truncate,
} from "./src/utils/strings";

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
let probeCacheLoaded = false;

function persistProbeCache() {
  try {
    ensureDataDirSync();
    const payload = Array.from(probeCache.entries()).map(([file, value]) => ({
      file,
      mtimeMs: value.mtimeMs,
      data: value.data,
    }));
    writeFileSync(probeCachePath, JSON.stringify(payload));
  } catch (err) {
    console.warn(`Failed to persist ffprobe cache: ${(err as Error).message}`);
  }
}

const scanRoots = (() => {
  const roots = process.argv
    .slice(2)
    .filter((arg) => arg && !arg.startsWith("-"));
  if (roots.length === 0) {
    console.error("Pass one or more library roots via argv");
  }
  return roots;
})();

const transcodeStatus = new Map<string, TranscodeStatus>();
const readyBooks = new Map<string, Book>();
const queuedSources = new Set<string>();
let rescanTimer: ReturnType<typeof setTimeout> | null = null;
let initialScanPromise: Promise<void> | null = null;

function statusKey(source: string): string {
  return source;
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch {
    return false;
  }
}

async function loadTranscodeStatus() {
  await ensureDataDir();
  try {
    const content = await fs.readFile(transcodeStatusPath, "utf8");
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) {
      parsed.forEach((entry: any) => {
        if (!entry || typeof entry !== "object") return;
        if (typeof entry.source !== "string" || typeof entry.target !== "string") return;
        if (typeof entry.mtimeMs !== "number" || typeof entry.state !== "string") return;
        const record: TranscodeStatus = {
          source: entry.source,
          target: entry.target,
          mtimeMs: entry.mtimeMs,
          state: entry.state as TranscodeState,
          error: typeof entry.error === "string" ? entry.error : undefined,
        };
        transcodeStatus.set(statusKey(record.source), record);
      });
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn("Failed to read transcode status:", (err as Error).message);
    }
  }
}

async function saveTranscodeStatus() {
  await ensureDataDir();
  await fs.writeFile(transcodeStatusPath, JSON.stringify(Array.from(transcodeStatus.values()), null, 2), "utf8");
}

function reviveBook(book: any): Book | null {
  if (!book || typeof book !== "object") return null;
  if (typeof book.id !== "string" || typeof book.title !== "string" || typeof book.author !== "string") return null;
  const revived: Book = {
    ...book,
    publishedAt: book.publishedAt ? new Date(book.publishedAt) : undefined,
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
  await fs.writeFile(libraryIndexPath, JSON.stringify(Array.from(readyBooks.values()), null, 2), "utf8");
}

async function loadOrCreateApiKey(): Promise<string> {
  await ensureDataDir();
  try {
    const existing = await fs.readFile(apiKeyPath, "utf8");
    const trimmed = existing.trim();
    if (trimmed) {
      console.log(`[auth] loaded API key from ${apiKeyPath}`);
      console.log(`[auth] API key: ${trimmed}`);
      return trimmed;
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn(`Failed to read API key: ${(err as Error).message}`);
    }
  }
  const key = randomBytes(24).toString("hex");
  await fs.writeFile(apiKeyPath, key, "utf8");
  console.log(`[auth] generated new API key at ${apiKeyPath}`);
  console.log(`[auth] API key: ${key}`);
  return key;
}

const apiKeyPromise = loadOrCreateApiKey();

function createJobChannel<T>(): JobChannel<T> {
  const queue: T[] = [];
  let wake: (() => void) | null = null;
  return {
    push(job: T) {
      queue.push(job);
      if (wake) {
        wake();
        wake = null;
      }
    },
    async *stream() {
      for (;;) {
        if (queue.length === 0) {
          await new Promise<void>((resolve) => (wake = resolve));
          continue;
        }
        const next = queue.shift();
        if (next !== undefined) yield next;
      }
    },
  };
}

const transcodeJobs = createJobChannel<TranscodeJob>();

function transcodeOutputPath(source: string, sourceStat: Awaited<ReturnType<typeof fs.stat>>): string {
  const extless = path.basename(source, path.extname(source));
  const safeName = slugify(extless) || "book";
  const hash = sourceStat.mtimeMs.toString(36);
  return path.join(dataDir, `${safeName}-${hash}.mp3`);
}

async function transcodeM4bToMp3(
  source: string,
  target: string,
  onProgress: (outTimeMs: number | undefined, speed: number | undefined) => void
): Promise<void> {
  await ensureDataDir();
  const proc = Bun.spawn({
    cmd: [
      "ffmpeg",
      "-y",
      "-nostdin",
      "-i",
      source,
      "-vn",
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
        "-threads",
        "1",
        "-progress",
        "pipe:1",
      "-stats_period",
      "1",
      "-loglevel",
      "error",
      target,
    ],
    stdout: "pipe",
    stderr: "inherit",
    stdin: "ignore",
  });

  let buffer = "";
  const reader = proc.stdout?.getReader();
  if (reader) {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += Buffer.from(value).toString("utf8");
      const parts = buffer.split(/\r?\n/);
      buffer = parts.pop() ?? "";
      let outTimeMs: number | undefined;
      let speed: number | undefined;
      parts
        .filter(Boolean)
        .forEach((line) => {
          const [key, valueStr] = line.split("=");
          if (key === "out_time_ms" || key === "out_time_us") {
            const parsed = Number(valueStr);
            if (Number.isFinite(parsed)) {
              outTimeMs = parsed / 1000; // microseconds -> ms
            }
          } else if (key === "speed") {
            const numeric = Number((valueStr || "").replace(/x$/, ""));
            if (Number.isFinite(numeric)) speed = numeric;
          }
        });
      if (outTimeMs !== undefined || speed !== undefined) {
        onProgress(outTimeMs, speed);
      }
    }
  }

  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    throw new Error(`ffmpeg failed with status ${exitCode}`);
  }
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
    description: meta.description,
    descriptionHtml: meta.descriptionHtml,
    language: meta.language,
    isbn: meta.isbn,
    identifiers: meta.identifiers,
    chapters: meta.chapters,
  };
}

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

async function safeReadDir(dir: string) {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch (err) {
    console.warn(`Skipping unreadable dir ${dir}:`, (err as Error).message);
    return [];
  }
}

function coverOutputPath(sourcePath: string, mtimeMs: number, ext: "jpg" | "png" = "jpg"): string {
  const base = path.basename(sourcePath, path.extname(sourcePath));
  const safeName = slugify(base) || "cover";
  const hash = mtimeMs.toString(36);
  return path.join(dataDir, `cover-${safeName}-${hash}.${ext}`);
}

async function extractEmbeddedCover(sourcePath: string, mtimeMs: number): Promise<string | undefined> {
  await ensureDataDir();
  const output = coverOutputPath(sourcePath, mtimeMs, "jpg");
  const existing = await fs.stat(output).catch(() => null);
  if (existing && existing.isFile() && existing.size > 0) return output;
  const result = spawnSync(
    "ffmpeg",
    [
      "-y",
      "-nostdin",
      "-loglevel",
      "error",
      "-i",
      sourcePath,
      "-map",
      "0:v:0",
      "-an",
      "-c:v",
      "mjpeg",
      "-frames:v",
      "1",
      output,
    ],
    { stdio: "ignore" }
  );
  if (result.status === 0) {
    const stat = await fs.stat(output).catch(() => null);
    if (stat && stat.isFile() && stat.size > 0) return output;
  }
  await fs.rm(output, { force: true }).catch(() => {});
  return undefined;
}

async function extractCoverFromEpub(epubPath: string, mtimeMs: number): Promise<string | undefined> {
  const list = spawnSync("unzip", ["-Z1", epubPath], { encoding: "utf8" });
  if (list.status !== 0) return undefined;
  const entries = list.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const candidates = entries
    .filter((name) => /\.(jpg|jpeg|png)$/i.test(name))
    .sort((a, b) => {
      const aCover = /cover/i.test(a) ? 0 : 1;
      const bCover = /cover/i.test(b) ? 0 : 1;
      return aCover - bCover;
    });
  for (const name of candidates) {
    const lower = name.toLowerCase();
    const ext: "jpg" | "png" = lower.endsWith(".png") ? "png" : "jpg";
    const output = coverOutputPath(epubPath, mtimeMs, ext);
    const existing = await fs.stat(output).catch(() => null);
    if (existing && existing.isFile() && existing.size > 0) return output;
    const extracted = spawnSync("unzip", ["-p", epubPath, name], { encoding: "buffer" });
    if (extracted.status !== 0 || !extracted.stdout || (extracted.stdout as Buffer).length === 0) {
      await fs.rm(output, { force: true }).catch(() => {});
      continue;
    }
    try {
      await ensureDataDir();
      await fs.writeFile(output, extracted.stdout as Buffer);
      const stat = await fs.stat(output).catch(() => null);
      if (stat && stat.isFile() && stat.size > 0) return output;
    } catch {
      await fs.rm(output, { force: true }).catch(() => {});
    }
  }
  return undefined;
}

async function resolveCoverPath(
  bookDir: string,
  m4bs: string[],
  mp3s: string[],
  epubs: string[],
  pngs: string[],
  jpgs: string[]
): Promise<string | undefined> {
  if (m4bs.length > 0) {
    const filePath = path.join(bookDir, m4bs[0]);
    const stat = await fs.stat(filePath).catch(() => null);
    if (stat) {
      const embedded = await extractEmbeddedCover(filePath, stat.mtimeMs);
      if (embedded) return embedded;
    }
  }
  for (const mp3 of mp3s) {
    const filePath = path.join(bookDir, mp3);
    const stat = await fs.stat(filePath).catch(() => null);
    if (!stat) continue;
    const embedded = await extractEmbeddedCover(filePath, stat.mtimeMs);
    if (embedded) return embedded;
  }
  for (const epub of epubs) {
    const epubPath = path.join(bookDir, epub);
    const stat = await fs.stat(epubPath).catch(() => null);
    if (!stat) continue;
    const cover = await extractCoverFromEpub(epubPath, stat.mtimeMs);
    if (cover) return cover;
  }
  if (pngs.length > 0) return path.join(bookDir, pngs[0]);
  if (jpgs.length > 0) return path.join(bookDir, jpgs[0]);
  return undefined;
}

async function buildBook(author: string, bookDir: string, title: string): Promise<BookBuildResult | null> {
  const t0 = Date.now();
  console.log(`[scan] start book author="${author}" title="${title}" path="${bookDir}"`);
  const entries = await safeReadDir(bookDir);
  const files = entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
  const m4bs = files.filter((f) => f.toLowerCase().endsWith(".m4b")).sort();
  const mp3s = files.filter((f) => f.toLowerCase().endsWith(".mp3")).sort();
  const epubs = files.filter((f) => f.toLowerCase().endsWith(".epub")).sort();
  const pngs = files.filter((f) => f.toLowerCase().endsWith(".png")).sort();
  const jpgs = files.filter((f) => f.toLowerCase().endsWith(".jpg") || f.toLowerCase().endsWith(".jpeg")).sort();
  const opf = await readOpfMetadata(bookDir, files);
  const audioMetaSource = m4bs[0] ? path.join(bookDir, m4bs[0]) : mp3s[0] ? path.join(bookDir, mp3s[0]) : null;
  const audioMetaStat = audioMetaSource ? await fs.stat(audioMetaSource).catch(() => null) : null;
  const audioMeta = audioMetaStat ? readAudioMetadata(audioMetaSource!, audioMetaStat.mtimeMs) : null;
  const chapterMeta =
    m4bs[0] && audioMetaStat ? readFfprobeChapters(audioMetaSource!, audioMetaStat.mtimeMs) ?? undefined : undefined;

  const coverPath = await resolveCoverPath(bookDir, m4bs, mp3s, epubs, pngs, jpgs);
  const displayTitle = opf?.title ?? title;
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
      id: bookId(author, title),
      title: displayTitle,
      author: displayAuthor,
      coverPath,
      durationSeconds: getDurationSeconds(filePath, stat.mtimeMs),
      publishedAt: opf?.publishedAt ?? audioMeta?.date ?? stat.mtime,
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
        const mime = mimeFromExt(path.extname(existing.target));
        const ready: Book = {
          id: meta.id,
          title: meta.title,
          author: meta.author,
          kind: "single",
          mime,
          totalSize: targetStat.size,
          primaryFile: existing.target,
          coverPath: meta.coverPath,
          durationSeconds,
          publishedAt: meta.publishedAt,
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
    console.log(
      `[scan] built multi book author="${author}" title="${title}" files=${segments.length} duration=${Math.round(durationSeconds)}s in ${Date.now() - t0}ms`
    );
    return { ready: result };
  }

  return null;
}

async function scanAndQueue(): Promise<void> {
  const started = Date.now();
  const nextReady = new Map<string, Book>();
  const seenSources = new Set<string>();
  for (const root of scanRoots) {
    const authors = await safeReadDir(root);
    for (const authorEntry of authors) {
      if (!authorEntry.isDirectory()) continue;
      const author = authorEntry.name;
      const authorPath = path.join(root, author);
      const bookDirs = await safeReadDir(authorPath);
      for (const bookEntry of bookDirs) {
        if (!bookEntry.isDirectory()) continue;
        const bookDir = path.join(authorPath, bookEntry.name);
        const built = await buildBook(author, bookDir, bookEntry.name);
        if (!built) continue;
        if (built.ready) {
          nextReady.set(built.ready.id, built.ready);
        }
        if (built.pendingJob) {
          seenSources.add(built.pendingJob.source);
        }
        if (built.sourcePath) {
          seenSources.add(built.sourcePath);
        }
      }
    }
  }
  for (const key of Array.from(transcodeStatus.keys())) {
    if (!seenSources.has(key)) {
      transcodeStatus.delete(key);
    }
  }
  readyBooks.clear();
  for (const [id, book] of nextReady.entries()) {
    readyBooks.set(id, book);
  }
  await saveTranscodeStatus();
  await saveLibraryIndex();
  console.log(
    `[scan] completed ready=${readyBooks.size} queued=${queuedSources.size} in ${Date.now() - started}ms roots=${scanRoots.join("|")}`
  );
}

function scheduleRescan(delayMs = 500) {
  if (rescanTimer) return;
  rescanTimer = setTimeout(() => {
    rescanTimer = null;
    scanAndQueue().catch((err) => console.error("Rescan failed:", err));
  }, delayMs);
}

function startWatchers() {
  for (const root of scanRoots) {
    try {
      const watcher = watch(root, { recursive: true }, () => {
        scheduleRescan(500);
      });
      watcher.on("error", (err) => console.warn(`Watcher error for ${root}:`, err?.message ?? err));
    } catch (err) {
      console.warn(`Failed to watch ${root}:`, (err as Error).message);
    }
  }
}

async function workerLoop() {
  for await (const job of transcodeJobs.stream()) {
    const status = transcodeStatus.get(statusKey(job.source));
    if (!status || status.mtimeMs !== job.mtimeMs) {
      queuedSources.delete(job.source);
      continue;
    }
    status.state = "working";
    status.error = undefined;
    if (!status.durationMs && job.meta.durationSeconds) {
      status.durationMs = job.meta.durationSeconds * 1000;
    }
    status.outTimeMs = status.outTimeMs ?? 0;
    status.speed = undefined;
    await saveTranscodeStatus();
    console.log(`[transcode] start source="${job.source}" -> target="${job.target}"`);
    try {
      let lastProgressPersist = 0;
      let lastProgressLogMs = 0;
      let lastOutReported = 0;
      await transcodeM4bToMp3(job.source, job.target, (outTimeMs, speed) => {
        status.outTimeMs = outTimeMs;
        status.speed = speed;
        const now = Date.now();
        if (outTimeMs && status.durationMs && outTimeMs > lastOutReported + 5000) {
          lastOutReported = outTimeMs;
          const ratio = Math.min(1, outTimeMs / status.durationMs);
          const pct = Math.round(ratio * 100);
          const elapsedText = formatDurationAllowZero(outTimeMs / 1000);
          const totalText = formatDurationAllowZero(status.durationMs / 1000);
          if (now - lastProgressLogMs > 1500) {
            lastProgressLogMs = now;
            console.log(
              `[transcode] progress source="${job.source}" ${elapsedText} / ${totalText} (${pct}%)${speed ? ` speed=${speed.toFixed(1)}x` : ""}`
            );
          }
        }
        if (now - lastProgressPersist > 2000) {
          lastProgressPersist = now;
          saveTranscodeStatus().catch(() => {});
        }
      });
      await fs.utimes(job.target, new Date(), new Date(job.mtimeMs));
      const outStat = await fs.stat(job.target);
      status.state = "done";
      status.target = job.target;
      status.outTimeMs = undefined;
      status.speed = undefined;
      await saveTranscodeStatus();
      const book = bookFromMeta(job.meta, job.target, outStat);
      readyBooks.set(book.id, book);
      await saveLibraryIndex();
      console.log(
        `[transcode] done source="${job.source}" -> target="${job.target}" size=${outStat.size} duration=${Math.round(book.durationSeconds ?? 0)}s`
      );
    } catch (err) {
      status.state = "failed";
      status.error = (err as Error).message;
      status.outTimeMs = undefined;
      status.speed = undefined;
      await saveTranscodeStatus();
      console.warn(`Failed to transcode ${job.source}:`, (err as Error).message);
    } finally {
      queuedSources.delete(job.source);
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
    const at = a.publishedAt ? a.publishedAt.getTime() : 0;
    const bt = b.publishedAt ? b.publishedAt.getTime() : 0;
    return bt - at;
  });
  return books;
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

function formatDurationAllowZero(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "0:00";
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
  if (!probeCacheLoaded) {
    try {
      const content = readFileSync(probeCachePath, "utf8");
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        parsed.forEach((entry: any) => {
          if (!entry || typeof entry !== "object") return;
          if (typeof entry.file !== "string" || typeof entry.mtimeMs !== "number") return;
          probeCache.set(entry.file, { mtimeMs: entry.mtimeMs, data: entry.data ?? null });
        });
      }
    } catch {
      // ignore cache read errors
    }
    probeCacheLoaded = true;
  }

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
    persistProbeCache();
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
    persistProbeCache();
    return data;
  } catch (err) {
    console.warn(`Failed to parse ffprobe output for ${filePath}: ${(err as Error).message}`);
    probeCache.set(filePath, { mtimeMs, data: null });
    persistProbeCache();
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

function rssFeed(books: Book[], origin: string, keySuffix = ""): { body: string; lastModified: Date } {
  const firstCover = books.find((b) => b.coverPath);
  const channelImage =
    FEED_IMAGE_URL ||
    (brandImageExists
      ? `${origin}/podible.png${keySuffix}`
      : firstCover
        ? `${origin}/covers/${firstCover.id}.jpg${keySuffix}`
        : "");
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
      const enclosureUrl = `${origin}/stream/${book.id}.${ext}${keySuffix}`;
      const cover = book.coverPath ? `<itunes:image href="${origin}/covers/${book.id}.jpg${keySuffix}" />` : "";
      const tagLength = estimateId3TagLength(book);
      const enclosureLength = book.totalSize + tagLength;
      const durationSeconds = book.durationSeconds ?? 0;
      const duration = formatDuration(durationSeconds);
      const itemPubDate = (book.publishedAt ?? lastModified).toUTCString();
      const fallbackDescription = `${book.title} by ${book.author}`;
      const hasChapters = book.kind === "multi" || (book.chapters && book.chapters.length > 0);
      const chaptersTag = hasChapters
        ? `<podcast:chapters url="${origin}/chapters/${book.id}.json${keySuffix}" type="application/json+chapters" />`
        : "";
      const chaptersDebugTag = hasChapters
        ? `<podcast:chaptersDebug url="${origin}/chapters-debug/${book.id}.json${keySuffix}" type="application/json" />`
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
<link>${origin}/feed.xml${keySuffix}</link>
<atom:link href="${origin}/feed.xml${keySuffix}" rel="self" type="application/rss+xml" />
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
  const started = Date.now();
  const origin = requestOrigin(request);
  const key = new URL(request.url).searchParams.get("key");
  const keySuffix = key ? `?key=${encodeURIComponent(key)}` : "";
  console.log(`[feed] start /feed.xml roots=${scanRoots.join("|")}`);
  const books = readyBooksSorted();
  const scannedMs = Date.now() - started;
  const { body, lastModified } = rssFeed(books, origin, keySuffix);
  const totalMs = Date.now() - started;
  console.log(
    `[feed] /feed.xml books=${books.length} scan=${scannedMs}ms total=${totalMs}ms roots=${scanRoots.join("|")}`
  );
  const etag = `W/"${lastModified.getTime()}"`;
  return new Response(body, {
    headers: {
      "Content-Type": "application/rss+xml",
      "Last-Modified": lastModified.toUTCString(),
      ETag: etag,
    },
  });
}

async function homePage(request: Request): Promise<Response> {
  const started = Date.now();
  console.log("[home] start /");
  const url = new URL(request.url);
  const origin = requestOrigin(request);
  const queryKey = url.searchParams.get("key");
  const keySuffix = queryKey ? `?key=${encodeURIComponent(queryKey)}` : "";
  const books = readyBooksSorted();
  const authors = new Set(books.map((b) => b.author));
  const singles = books.filter((b) => b.kind === "single").length;
  const multis = books.filter((b) => b.kind === "multi").length;
  const covers = books.filter((b) => Boolean(b.coverPath)).length;
  const statusValues = Array.from(transcodeStatus.values());
  const done = statusValues.filter((s) => s.state === "done").length;
  const pending = statusValues.filter((s) => s.state === "pending").length;
  const working = statusValues.filter((s) => s.state === "working").length;
  const isProbeFailure = (s: TranscodeStatus) =>
    Boolean(
      s.state === "failed" &&
        s.error &&
        (s.error.toLowerCase().includes("ffprobe") ||
          s.error.toLowerCase().includes("duration") ||
          s.error.toLowerCase().includes("zero-size"))
    );
  const failedProbes = statusValues.filter(isProbeFailure).length;
  const failedTranscodesOnly = statusValues.filter((s) => s.state === "failed" && !isProbeFailure(s)).length;
  const failedAll = statusValues.filter((s) => s.state === "failed").length;
  const active = statusValues.find((s) => s.state === "working");
  const activeProgress = (() => {
    if (!active || !active.durationMs) return null;
    const elapsed = active.outTimeMs ?? 0;
    const clampedElapsed = Math.max(0, Math.min(elapsed, active.durationMs));
    const ratio = Math.min(1, clampedElapsed / active.durationMs);
    return { ratio, elapsed: clampedElapsed, durationMs: active.durationMs, speed: active.speed };
  })();
  const totalTranscodes = done + pending + working + failedTranscodesOnly;
  const percent = totalTranscodes === 0 ? 100 : Math.min(100, Math.round((done / totalTranscodes) * 100));
  const barWidth =
    totalTranscodes === 0
      ? 100
      : activeProgress
        ? Math.min(100, Math.round(((done + activeProgress.ratio) / totalTranscodes) * 100))
        : percent;
  const durationMs = Date.now() - started;
  const sample = books[0];
  const sampleExt = sample ? bookExtension(sample) : undefined;
  const multiWithChapters = books.find((b) => b.kind === "multi");
  const coverBook = books.find((b) => b.coverPath);
  const previewItems = books.slice(0, Math.min(books.length, 12));
  console.log(
    `[home] done books=${books.length} singles=${singles} transcodes done=${done} pending=${pending} working=${working} failed=${failedAll} queue=${queuedSources.size} in ${durationMs}ms`
  );
  const body = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Podible</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 640px; margin: 48px auto; padding: 0 16px; color: #0f172a; }
    h1 { margin-bottom: 8px; }
    p { margin: 0 0 12px 0; }
    .card { border: 1px solid #e2e8f0; border-radius: 12px; padding: 16px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06); }
    .stat { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }
    .stat:last-child { border-bottom: none; }
    .label { color: #475569; }
    .value { font-weight: 600; }
    .progress { height: 10px; background: #e2e8f0; border-radius: 999px; overflow: hidden; margin-top: 8px; }
    .bar { height: 100%; background: linear-gradient(90deg, #38bdf8, #0ea5e9); width: ${barWidth}%; transition: width 0.3s ease; }
    code { background: #f8fafc; border: 1px solid #e2e8f0; padding: 2px 6px; border-radius: 6px; }
    .links { list-style: none; padding: 0; margin: 0; }
    .feed-preview { margin-top: 32px; display: flex; flex-direction: column; gap: 12px; }
    .feed-item { display: grid; grid-template-columns: 64px 1fr; gap: 12px; padding: 12px; border: 1px solid #e2e8f0; border-radius: 12px; align-items: center; background: #fff; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05); }
    .feed-cover { width: 64px; height: 64px; border-radius: 10px; overflow: hidden; background: linear-gradient(135deg, #c7d2fe, #e0f2fe); display: flex; align-items: center; justify-content: center; color: #0f172a; font-weight: 700; font-size: 18px; border: 1px solid #e2e8f0; }
    .feed-cover img { width: 100%; height: 100%; object-fit: cover; display: block; }
    .feed-title { margin: 0 0 6px 0; font-size: 16px; line-height: 1.3; color: #0f172a; }
    .feed-desc { margin: 0; color: #475569; font-size: 14px; line-height: 1.45; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
  </style>
</head>
<body>
  <h1>Podible</h1>
  ${brandImageExists ? `<p><img src="${origin}/podible.png${keySuffix}" alt="Podcast artwork" style="max-width: 160px; border-radius: 12px; border: 1px solid #e2e8f0;" /></p>` : ""}
  <p>Transcode progress and library status.</p>
  <div class="card">
    <div class="stat"><span class="label">Total books</span><span class="value">${books.length}</span></div>
    <div class="stat"><span class="label">Single m4b books</span><span class="value">${singles}</span></div>
    <div class="stat"><span class="label">Multi mp3 books</span><span class="value">${multis}</span></div>
    <div class="stat"><span class="label">Authors</span><span class="value">${authors.size}</span></div>
    <div class="stat"><span class="label">Covers</span><span class="value">${covers}</span></div>
    <div class="stat"><span class="label">Failed transcodes</span><span class="value">${failedTranscodesOnly}</span></div>
    <div class="stat"><span class="label">Failed probes</span><span class="value">${failedProbes}</span></div>
    <div class="stat"><span class="label">Transcode status</span><span class="value">done ${done} / ${totalTranscodes} (pending ${pending}, working ${working}, failed ${failedTranscodesOnly})</span></div>
    <div class="stat"><span class="label">Active job</span><span class="value">${
      activeProgress
        ? `${formatDurationAllowZero(activeProgress.elapsed / 1000)} / ${formatDurationAllowZero(activeProgress.durationMs / 1000)} (${Math.round(activeProgress.ratio * 100)}%)${
            activeProgress.speed ? ` @ ${activeProgress.speed.toFixed(1)}x` : ""
          }`
        : active
          ? "working (no progress yet)"
          : "None"
    }</span></div>
    <div class="stat"><span class="label">Queue</span><span class="value">${queuedSources.size}</span></div>
    <div class="progress"><div class="bar"></div></div>
  </div>
  <p style="margin-top:16px;">Scan time: ${durationMs} ms</p>
  <p>Links:</p>
  <ul class="links">
    <li><a href="${origin}/feed.xml${keySuffix}">Feed</a></li>
    <li><a href="${origin}/feed-debug.xml${keySuffix}">Feed Debug</a></li>
    ${
      sample && sampleExt
        ? `<li><a href="${origin}/stream/${sample.id}.${sampleExt}${keySuffix}">Stream</a></li>`
        : ""
    }
    ${
      multiWithChapters
        ? `<li><a href="${origin}/chapters/${multiWithChapters.id}.json${keySuffix}">Chapters</a></li>`
        : ""
    }
    ${
      coverBook
        ? `<li><a href="${origin}/covers/${coverBook.id}.jpg${keySuffix}">Cover</a></li>`
        : ""
    }
  </ul>
  ${
    previewItems.length > 0
      ? `<div class="feed-preview">
    <h2 style="margin:20px 0 8px 0;">Feed preview</h2>
    ${previewItems
      .map((book) => {
        const coverUrl = book.coverPath ? `${origin}/covers/${book.id}.jpg${keySuffix}` : "";
        const initials =
          (book.title || "")
            .split(/\s+/)
            .map((p) => p[0])
            .join("")
            .slice(0, 2)
            .toUpperCase() || "AB";
        const { description } = buildItemNotes(book);
        const desc = truncate(description || `${book.title} by ${book.author}`, 220);
        return `<div class="feed-item">
      <div class="feed-cover">${coverUrl ? `<img src="${coverUrl}" alt="${book.title} cover" />` : initials}</div>
      <div>
        <p class="feed-title">${escapeXml(book.title)} <span style="color:#94a3b8;">— ${escapeXml(book.author)}</span></p>
        <p class="feed-desc">${escapeXml(desc)}</p>
      </div>
    </div>`;
      })
      .join("")}
  </div>`
      : ""
  }
</body>
</html>`;
  return new Response(body, {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

async function handleFeedDebug(request: Request): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const started = Date.now();
  const origin = requestOrigin(request);
  const key = new URL(request.url).searchParams.get("key");
  const keySuffix = key ? `?key=${encodeURIComponent(key)}` : "";
  console.log(`[feed] start /feed-debug.xml roots=${scanRoots.join("|")}`);
  const books = readyBooksSorted();
  const scannedMs = Date.now() - started;
  const { body, lastModified } = rssFeed(books, origin, keySuffix);
  const totalMs = Date.now() - started;
  console.log(
    `[feed] /feed-debug.xml books=${books.length} scan=${scannedMs}ms total=${totalMs}ms roots=${scanRoots.join("|")}`
  );
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
  const ext = path.extname(book.coverPath).toLowerCase();
  const mime = ext === ".png" ? "image/png" : "image/jpeg";
  return new Response(file, {
    headers: { "Content-Type": mime },
  });
}

function authorize(request: Request, key: string): boolean {
  const url = new URL(request.url);
  const queryKey = url.searchParams.get("key");
  if (queryKey && queryKey.trim() === key) return true;
  const header = request.headers.get("authorization");
  if (header?.toLowerCase().startsWith("bearer ")) {
    const token = header.slice("bearer ".length).trim();
    if (token === key) return true;
  }
  const apiKeyHeader = request.headers.get("x-api-key");
  if (apiKeyHeader && apiKeyHeader.trim() === key) return true;
  return false;
}

await ensureDataDir();
await Promise.all([loadTranscodeStatus(), loadLibraryIndex()]);
initialScanPromise = scanAndQueue();
void workerLoop();
startWatchers();

Bun.serve({
  port,
  fetch: async (request: Request) => {
    const apiKey = await apiKeyPromise;
    if (!authorize(request, apiKey)) {
      return new Response("Unauthorized", {
        status: 401,
        headers: { "WWW-Authenticate": 'Bearer realm="podible"' },
      });
    }
    const url = new URL(request.url);
    const pathname = url.pathname;
    if (pathname === "/") return homePage(request);
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
    if (pathname === "/podible.png" && brandImageExists) {
      const file = Bun.file(brandImagePath);
      return new Response(file, { headers: { "Content-Type": "image/png" } });
    }
    return new Response("Not found", { status: 404 });
  },
});

const localBase = `http://localhost${port === 80 ? "" : `:${port}`}`;
console.log(`Listening on port ${port}. Roots: ${scanRoots.join(", ") || "none"}`);
console.log(`Feed: ${localBase}/feed.xml`);
console.log(`Feed (debug/plain): ${localBase}/feed-debug.xml`);

async function logInitialScan() {
  if (scanRoots.length === 0) return;
  const books = readyBooksSorted();
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

initialScanPromise
  ?.then(() => logInitialScan())
  .catch((err) => {
    console.error("Initial scan failed:", err);
  });
