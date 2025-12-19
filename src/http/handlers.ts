import path from "node:path";

import { brandImageExists } from "../config";
import { jsonFeed } from "../feed/json";
import { buildItemNotes, rssFeed } from "../feed/rss";
import { buildChapters, buildChapterTimings } from "../library/chapters";
import { findBookById, readyBooksSorted } from "../library";
import { bookExtension, bookMime } from "../media/metadata";
import { getProbeFailures } from "../media/probe-cache";
import { buildId3ChaptersTag } from "../streaming/id3";
import { parseRange, segmentsForRange, streamSegments } from "../streaming/range";
import { transcodeStatus, queuedSources } from "../transcode";
import { TranscodeStatus } from "../types";
import { escapeXml, truncate } from "../utils/strings";
import { formatDurationAllowZero } from "../utils/time";

function requestOrigin(request: Request): string {
  const url = new URL(request.url);
  const forwardedProto = request.headers.get("x-forwarded-proto");
  const proto = forwardedProto ? forwardedProto.split(",")[0].trim() : url.protocol.replace(":", "");
  return `${proto}://${url.host}`;
}

async function handleFeed(request: Request, scanRoots: string[]): Promise<Response> {
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

async function handleJsonFeed(request: Request, scanRoots: string[]): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const started = Date.now();
  const origin = requestOrigin(request);
  const key = new URL(request.url).searchParams.get("key");
  const keySuffix = key ? `?key=${encodeURIComponent(key)}` : "";
  console.log(`[feed] start /feed.json roots=${scanRoots.join("|")}`);
  const books = readyBooksSorted();
  const scannedMs = Date.now() - started;
  const { body, lastModified } = jsonFeed(books, origin, keySuffix);
  const totalMs = Date.now() - started;
  console.log(
    `[feed] /feed.json books=${books.length} scan=${scannedMs}ms total=${totalMs}ms roots=${scanRoots.join("|")}`
  );
  const etag = `W/"${lastModified.getTime()}"`;
  return new Response(body, {
    headers: {
      "Content-Type": "application/feed+json; charset=utf-8",
      "Last-Modified": lastModified.toUTCString(),
      ETag: etag,
    },
  });
}

async function handleJsonFeedDebug(request: Request, scanRoots: string[]): Promise<Response> {
  if (scanRoots.length === 0) {
    return new Response("No roots configured. Pass library directories via argv.", { status: 500 });
  }
  const started = Date.now();
  const origin = requestOrigin(request);
  const key = new URL(request.url).searchParams.get("key");
  const keySuffix = key ? `?key=${encodeURIComponent(key)}` : "";
  console.log(`[feed] start /feed-debug.json roots=${scanRoots.join("|")}`);
  const books = readyBooksSorted();
  const scannedMs = Date.now() - started;
  const { body, lastModified } = jsonFeed(books, origin, keySuffix);
  const totalMs = Date.now() - started;
  console.log(
    `[feed] /feed-debug.json books=${books.length} scan=${scannedMs}ms total=${totalMs}ms roots=${scanRoots.join("|")}`
  );
  const etag = `W/"${lastModified.getTime()}"`;
  return new Response(JSON.stringify(JSON.parse(body), null, 2), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
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
  const probeFailures = getProbeFailures().sort((a, b) => b.mtimeMs - a.mtimeMs);
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
    .probe-errors { margin-top: 16px; border: 1px solid #e2e8f0; border-radius: 12px; padding: 12px; background: #f8fafc; }
    .probe-errors summary { cursor: pointer; font-weight: 600; color: #0f172a; }
    .probe-errors ul { list-style: none; padding: 0; margin: 12px 0 0 0; display: grid; gap: 10px; }
    .probe-errors li { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px 12px; }
    .probe-errors .file { font-weight: 600; font-size: 13px; color: #0f172a; margin-bottom: 6px; word-break: break-all; }
    .probe-errors .error { font-family: "SFMono-Regular", ui-monospace, Menlo, Consolas, monospace; font-size: 12px; color: #b91c1c; white-space: pre-wrap; }
    .actions { margin: 16px 0 0 0; }
    .restart { border: 1px solid #ef4444; background: #fee2e2; color: #991b1b; padding: 8px 12px; border-radius: 10px; font-weight: 600; cursor: pointer; }
    .restart:hover { background: #fecaca; }
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
  ${
    probeFailures.length > 0
      ? `<details class="probe-errors">
    <summary>Failed probe details (${probeFailures.length})</summary>
    <ul>
      ${probeFailures
        .map(
          (failure) =>
            `<li><div class="file">${escapeXml(failure.file)}</div><div class="error">${escapeXml(
              failure.error
            )}</div></li>`
        )
        .join("")}
    </ul>
  </details>`
      : ""
  }
  <form class="actions" method="post" action="${origin}/restart${keySuffix}">
    <button class="restart" type="submit">Restart server</button>
  </form>
  <p style="margin-top:16px;">Scan time: ${durationMs} ms</p>
  <p>Links:</p>
  <ul class="links">
    <li><a href="${origin}/feed.xml${keySuffix}">Feed</a></li>
    <li><a href="${origin}/feed-debug.xml${keySuffix}">Feed Debug</a></li>
    <li><a href="${origin}/feed.json${keySuffix}">JSON Feed</a></li>
    <li><a href="${origin}/feed-debug.json${keySuffix}">JSON Feed Debug</a></li>
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

async function handleFeedDebug(request: Request, scanRoots: string[]): Promise<Response> {
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

async function handleEpub(request: Request, bookIdValue: string): Promise<Response> {
  const book = await findBookById(bookIdValue);
  if (!book || !book.epubPath) return new Response("Not found", { status: 404 });
  const file = Bun.file(book.epubPath);
  if (!(await file.exists())) return new Response("Not found", { status: 404 });
  const stat = await file.stat().catch(() => null);
  if (!stat || stat.size <= 0) return new Response("Not found", { status: 404 });

  const rangeHeader = request.headers.get("range");
  const headers: Record<string, string> = {
    "Accept-Ranges": "bytes",
    "Content-Type": "application/epub+zip",
    "Content-Disposition": `inline; filename="${book.id}.epub"`,
  };
  const range = parseRange(rangeHeader, stat.size);
  if (!range) {
    headers["Content-Length"] = String(stat.size);
    return new Response(file, { status: 200, headers });
  }
  headers["Content-Length"] = String(range.end - range.start + 1);
  headers["Content-Range"] = `bytes ${range.start}-${range.end}/${stat.size}`;
  return new Response(file.slice(range.start, range.end + 1), { status: 206, headers });
}

export {
  handleChapters,
  handleChaptersDebug,
  handleCover,
  handleEpub,
  handleFeed,
  handleFeedDebug,
  handleJsonFeed,
  handleJsonFeedDebug,
  handleStream,
  homePage,
  requestOrigin,
};
