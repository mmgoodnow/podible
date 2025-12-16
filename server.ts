import { promises as fs } from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import {
  Book,
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
  ensureDataDir,
  port,
} from "./src/config";
import {
  escapeXml,
  firstLine,
  htmlToPlainText,
  truncate,
} from "./src/utils/strings";
import { formatDuration, formatDurationAllowZero } from "./src/utils/time";
import { bookExtension, bookMime } from "./src/media/metadata";
import {
  bookFromMeta,
  findBookById,
  loadLibraryIndex,
  readyBooks,
  readyBooksSorted,
  saveLibraryIndex,
  scanAndQueue,
  startWatchers,
} from "./src/library";
import { buildChapters, buildChapterTimings } from "./src/library/chapters";
import { buildId3ChaptersTag, estimateId3TagLength } from "./src/streaming/id3";
import { parseRange, segmentsForRange, streamSegments } from "./src/streaming/range";
import {
  loadTranscodeStatus,
  queuedSources,
  saveTranscodeStatus,
  statusKey,
  transcodeJobs,
  transcodeM4bToMp3,
  transcodeOutputPath,
  transcodeStatus,
} from "./src/transcode";

const scanRoots = (() => {
  const roots = process.argv
    .slice(2)
    .filter((arg) => arg && !arg.startsWith("-"));
  if (roots.length === 0) {
    console.error("Pass one or more library roots via argv");
  }
  return roots;
})();

let initialScanPromise: Promise<void> | null = null;

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
initialScanPromise = scanAndQueue(scanRoots);
void workerLoop();
startWatchers(scanRoots);

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
