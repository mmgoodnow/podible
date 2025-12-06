import { promises as fs, createReadStream } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { Readable } from "node:stream";

type BookKind = "single" | "multi";

type AudioSegment = {
  path: string;
  name: string;
  size: number;
  start: number;
  end: number;
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
};

const scanRoots = (() => {
  const roots = process.argv.slice(2).filter(Boolean);
  if (roots.length === 0) {
    console.error("Pass one or more library roots via argv");
  }
  return roots;
})();

const port = Number(process.env.PORT ?? 80);

function slugify(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/--+/g, "-");
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

  const coverPath = covers.length > 0 ? path.join(bookDir, covers[0]) : undefined;

  if (m4bs.length > 0) {
    const filePath = path.join(bookDir, m4bs[0]);
    const stat = await fs.stat(filePath).catch(() => null);
    if (!stat) return null;
    return {
      id: bookId(author, title),
      title,
      author,
      kind: "single",
      mime: "audio/mp4",
      totalSize: stat.size,
      primaryFile: filePath,
      coverPath,
    };
  }

  if (mp3s.length > 0) {
    const segments: AudioSegment[] = [];
    let cursor = 0;
    for (const name of mp3s) {
      const filePath = path.join(bookDir, name);
      const stat = await fs.stat(filePath).catch(() => null);
      if (!stat) continue;
      const start = cursor;
      const end = cursor + stat.size - 1;
      segments.push({ path: filePath, name, size: stat.size, start, end });
      cursor += stat.size;
    }
    if (segments.length === 0) return null;
    return {
      id: bookId(author, title),
      title,
      author,
      kind: "multi",
      mime: "audio/mpeg",
      totalSize: segments[segments.length - 1].end + 1,
      files: segments,
      coverPath,
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

let ffprobeAvailable: boolean | null = null;

function probeDurationSeconds(filePath: string): number | null {
  if (ffprobeAvailable === false) return null;
  const result = spawnSync("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ], { encoding: "utf8" });
  if (result.error) {
    ffprobeAvailable = false;
    return null;
  }
  if (result.status !== 0) return null;
  ffprobeAvailable = true;
  const value = parseFloat(result.stdout.trim());
  return Number.isFinite(value) ? value : null;
}

async function buildChapters(book: Book) {
  if (book.kind !== "multi" || !book.files) return null;
  const chapters: { startTime: number; title: string }[] = [];
  let cursor = 0;
  for (const segment of book.files) {
    chapters.push({
      startTime: cursor,
      title: path.basename(segment.name, path.extname(segment.name)),
    });
    const duration = probeDurationSeconds(segment.path);
    if (duration !== null) {
      cursor += duration;
    } else {
      cursor += 1;
    }
  }
  return {
    version: "1.2.0",
    chapters,
  };
}

function rssFeed(books: Book[], origin: string): string {
  const items = books
    .map((book) => {
      const enclosureUrl = `${origin}/stream/${book.id}`;
      const cover = book.coverPath ? `<itunes:image href="${origin}/covers/${book.id}.jpg" />` : "";
      const chaptersTag =
        book.kind === "multi"
          ? `<podcast:chapters url="${origin}/chapters/${book.id}.json" type="application/json+chapters" />`
          : "";
      return [
        "<item>",
        `<guid isPermaLink="false">${escapeXml(book.id)}</guid>`,
        `<title>${escapeXml(book.title)}</title>`,
        `<itunes:author>${escapeXml(book.author)}</itunes:author>`,
        `<enclosure url="${enclosureUrl}" length="${book.totalSize}" type="${book.mime}" />`,
        cover,
        chaptersTag,
        "</item>",
      ]
        .filter(Boolean)
        .join("");
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>Library Feed</title>
<link>${origin}/feed.xml</link>
<description>Podcast feed for audiobooks</description>
${items}
</channel>
</rss>`;
}

async function handleFeed(request: Request): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const url = new URL(request.url);
  const origin = `${url.protocol}//${url.host}`;
  const books = await scanBooks();
  const body = rssFeed(books, origin);
  return new Response(body, {
    headers: {
      "Content-Type": "application/rss+xml",
    },
  });
}

async function handleStream(request: Request, bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book) return new Response("Not found", { status: 404 });
  const rangeHeader = request.headers.get("range");
  const size = book.totalSize;
  const headers: Record<string, string> = {
    "Accept-Ranges": "bytes",
    "Content-Type": book.mime,
  };

  if (book.kind === "single" && book.primaryFile) {
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
  const range = parseRange(rangeHeader, size) ?? { start: 0, end: size - 1 };
  const slices = segmentsForRange(files, range.start, range.end);
  if (slices.length === 0) {
    headers["Content-Range"] = `bytes */${size}`;
    return new Response("Range Not Satisfiable", { status: 416, headers });
  }

  headers["Content-Length"] = String(range.end - range.start + 1);
  if (rangeHeader) {
    headers["Content-Range"] = `bytes ${range.start}-${range.end}/${size}`;
  }
  const status = rangeHeader ? 206 : 200;
  return new Response(streamSegments(slices), { status, headers });
}

async function handleChapters(bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book || book.kind !== "multi") return new Response("Not found", { status: 404 });
  const chapters = await buildChapters(book);
  if (!chapters) return new Response("Not found", { status: 404 });
  return new Response(JSON.stringify(chapters, null, 2), {
    headers: { "Content-Type": "application/json+chapters" },
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
    if (pathname.startsWith("/stream/")) {
      const [, , id = ""] = pathname.split("/");
      return handleStream(request, id);
    }
    if (pathname.startsWith("/chapters/")) {
      const [, , idWithExt = ""] = pathname.split("/");
      const id = idWithExt.replace(/\.json$/, "");
      return handleChapters(id);
    }
    if (pathname.startsWith("/covers/")) {
      const [, , idWithExt = ""] = pathname.split("/");
      const id = idWithExt.replace(/\.jpg$/, "");
      return handleCover(id);
    }
    return new Response("Not found", { status: 404 });
  },
});

console.log(`Listening on port ${port}. Roots: ${scanRoots.join(", ") || "none"}`);
