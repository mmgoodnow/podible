import { promises as fs } from "node:fs";

import { buildJsonFeed, buildRssFeed } from "./feed";
import { authorizeRequest } from "./auth";
import { buildChapters, preferredAudioForBooks, streamAudioAsset, streamExtension } from "./media";
import { KindlingRepo } from "./repo";
import { fetchOpenLibraryMetadata } from "./openlibrary";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import type { AppSettings, MediaType } from "./types";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

async function readJson<T>(request: Request): Promise<T> {
  const body = await request.text();
  if (!body.trim()) {
    throw new Error("JSON body is required");
  }
  return JSON.parse(body) as T;
}

function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid id");
  }
  return parsed;
}

function parseLimit(value: string | null): number {
  if (!value) return 50;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) return 50;
  return Math.min(parsed, 200);
}

export function createKindlingFetchHandler(repo: KindlingRepo, startTime: number): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    const settings = repo.getSettings();
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (!authorizeRequest(request, settings)) {
      return new Response("Unauthorized", {
        status: 401,
        headers: { "WWW-Authenticate": 'Bearer realm="kindling"' },
      });
    }

    try {
      if (pathname === "/health" && request.method === "GET") {
        return json({
          ok: true,
          ...repo.getHealthSummary(),
        });
      }

      if (pathname === "/server" && request.method === "GET") {
        return json({
          name: "kindling-backend",
          runtime: "bun",
          uptimeMs: Date.now() - startTime,
          now: new Date().toISOString(),
        });
      }

      if (pathname === "/settings" && request.method === "GET") {
        return json(repo.getSettings());
      }

      if (pathname === "/settings" && request.method === "PUT") {
        const payload = await readJson<AppSettings>(request);
        return json(repo.updateSettings(payload));
      }

      if (pathname === "/library" && request.method === "GET") {
        const limit = parseLimit(url.searchParams.get("limit"));
        const cursorParam = url.searchParams.get("cursor");
        const cursor = cursorParam ? parseId(cursorParam) : undefined;
        const q = url.searchParams.get("q") ?? undefined;
        const result = repo.listBooks(limit, cursor, q);
        return json(result);
      }

      if (pathname === "/library" && request.method === "POST") {
        const payload = await readJson<{ title: string; author: string }>(request);
        if (!payload.title?.trim() || !payload.author?.trim()) {
          return json({ error: "title and author are required" }, 400);
        }
        const book = repo.createBook({
          title: payload.title.trim(),
          author: payload.author.trim(),
        });
        const metadata = await fetchOpenLibraryMetadata(book).catch(() => null);
        if (metadata) {
          repo.updateBookMetadata(book.id, {
            publishedAt: metadata.publishedAt ?? null,
            language: metadata.language ?? null,
            isbn: metadata.isbn ?? null,
            identifiers: metadata.identifiers,
          });
        }
        const jobId = await triggerAutoAcquire(repo, book.id);
        return json({
          book: repo.getBook(book.id),
          acquisition_job_id: jobId,
        }, 201);
      }

      if (pathname === "/library/refresh" && request.method === "POST") {
        const job = repo.createJob({
          type: "scan",
          payload: { fullRefresh: true },
        });
        return json({ jobId: job.id }, 202);
      }

      if (pathname.startsWith("/library/") && request.method === "GET") {
        const id = parseId(pathname.split("/")[2] ?? "");
        const book = repo.getBook(id);
        if (!book) return json({ error: "not_found" }, 404);
        return json({
          book,
          releases: repo.listReleasesByBook(id),
          assets: repo.listAssetsByBook(id),
        });
      }

      if (pathname === "/search" && request.method === "POST") {
        const payload = await readJson<{ query: string; media: MediaType }>(request);
        if (!payload.query?.trim() || (payload.media !== "audio" && payload.media !== "ebook")) {
          return json({ error: "query and media are required" }, 400);
        }
        const results = await runSearch(settings, {
          query: payload.query.trim(),
          media: payload.media,
        });
        return json({ results });
      }

      if (pathname === "/snatch" && request.method === "POST") {
        const payload = await readJson<{
          bookId: number;
          provider: string;
          title: string;
          mediaType: MediaType;
          url: string;
          sizeBytes?: number | null;
          infoHash?: string | null;
        }>(request);
        const outcome = await runSnatch(repo, settings, payload);
        return json(outcome, outcome.idempotent ? 200 : 201);
      }

      if (pathname === "/releases" && request.method === "GET") {
        const id = parseId(url.searchParams.get("bookId") ?? "");
        return json({ releases: repo.listReleasesByBook(id) });
      }

      if (pathname === "/downloads" && request.method === "GET") {
        return json({ downloads: repo.listDownloads() });
      }

      if (pathname.startsWith("/downloads/") && pathname.endsWith("/retry") && request.method === "POST") {
        const parts = pathname.split("/");
        const jobId = parseId(parts[2] ?? "");
        const retried = repo.retryJob(jobId);
        return json({ job: retried }, 202);
      }

      if (pathname.startsWith("/downloads/") && request.method === "GET") {
        const jobId = parseId(pathname.split("/")[2] ?? "");
        const download = repo.getDownload(jobId);
        if (!download) return json({ error: "not_found" }, 404);
        return json(download);
      }

      if (pathname === "/import/reconcile" && request.method === "POST") {
        const job = repo.createJob({ type: "reconcile" });
        return json({ jobId: job.id }, 202);
      }

      if (pathname === "/assets" && request.method === "GET") {
        const bookId = parseId(url.searchParams.get("bookId") ?? "");
        const assets = repo.listAssetsByBook(bookId).map((asset) => ({
          ...asset,
          files: repo.getAssetFiles(asset.id),
          stream_ext: streamExtension(asset),
        }));
        return json({ assets });
      }

      if (pathname.startsWith("/stream/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.split(".")[0] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target) return new Response("Not found", { status: 404 });
        const book = repo.getBookByAsset(assetId);
        return streamAudioAsset(request, target.asset, target.files, book?.cover_path);
      }

      if (pathname.startsWith("/chapters/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.replace(/\.json$/i, ""));
        const target = repo.getAssetWithFiles(assetId);
        if (!target) return json({ error: "not_found" }, 404);
        const chapters = await buildChapters(target.asset, target.files);
        if (!chapters) return json({ error: "not_found" }, 404);
        return new Response(JSON.stringify(chapters, null, 2), {
          headers: { "Content-Type": "application/json+chapters" },
        });
      }

      if (pathname.startsWith("/covers/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const bookId = parseId(idPart.replace(/\.jpg$/i, ""));
        const book = repo.getBookRow(bookId);
        if (!book?.cover_path) return new Response("Not found", { status: 404 });
        const file = Bun.file(book.cover_path);
        if (!(await file.exists())) return new Response("Not found", { status: 404 });
        return new Response(file, {
          headers: {
            "Content-Type": book.cover_path.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
          },
        });
      }

      if (pathname === "/feed.xml" && request.method === "GET") {
        return buildRssFeed(request, repo, settings.feed.title, settings.feed.author);
      }

      if (pathname === "/feed.json" && request.method === "GET") {
        return buildJsonFeed(request, repo, settings.feed.title, settings.feed.author);
      }

      if (pathname.startsWith("/ebook/") && request.method === "GET") {
        const assetId = parseId(pathname.split("/")[2] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target || target.asset.kind !== "ebook") return new Response("Not found", { status: 404 });
        const first = target.files[0];
        if (!first) return new Response("Not found", { status: 404 });
        const file = Bun.file(first.path);
        if (!(await file.exists())) return new Response("Not found", { status: 404 });
        return new Response(file, {
          headers: {
            "Content-Type": target.asset.mime,
            "Content-Disposition": `attachment; filename="${first.path.split("/").pop() ?? `book-${assetId}`}"`,
          },
        });
      }

      return new Response("Not found", { status: 404 });
    } catch (error) {
      return json({ error: (error as Error).message }, 400);
    }
  };
}
