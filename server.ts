import { promises as fs } from "node:fs";
import { randomBytes } from "node:crypto";
import { apiKeyPath, brandImageExists, brandImagePath, ensureDataDir, port } from "./src/config";
import { formatDurationAllowZero } from "./src/utils/time";
import { bookExtension } from "./src/media/metadata";
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
import {
  handleChapters,
  handleChaptersDebug,
  handleCover,
  handleFeed,
  handleFeedDebug,
  handleStream,
  homePage,
} from "./src/http/handlers";
import { authorize } from "./src/http/auth";
import { loadTranscodeStatus, queuedSources, saveTranscodeStatus, statusKey, transcodeOutputPath, transcodeStatus } from "./src/transcode";
import { workerLoop } from "./src/transcode/worker";

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
    if (pathname === "/feed.xml") return handleFeed(request, scanRoots);
    if (pathname === "/feed-debug.xml") return handleFeedDebug(request, scanRoots);
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
