import { promises as fs } from "node:fs";

import { buildJsonFeed, buildRssFeed } from "./feed";
import {
  buildSessionCookie,
  resolveSessionFromRequest,
} from "./auth";
import { loadStoredTranscriptPayload } from "./chapter-analysis";
import { buildChapters, streamAudioAsset, streamExtension } from "./media";
import { BooksRepo } from "./repo";
import {
  addApiKey,
  isHtmlPageRoute,
  parseAppLoginPath,
  sanitizeRedirectPath,
} from "./http/support";
import { handleAuthRoute, isPublicRoute } from "./http/auth-routes";
import { handleAdminRoute, isAdminRoute } from "./http/admin-routes";
import { json, parseId, redirect } from "./http/route-helpers";
import { handleUserRoute } from "./http/user-routes";
import { handleRpcMethod, handleRpcRequest } from "./rpc";
import type { AppSettings } from "./types";

function acceptsBrotli(request: Request): boolean {
  return (request.headers.get("accept-encoding")?.toLowerCase() ?? "").includes("br");
}

async function maybeCompressBrotli(request: Request, response: Response): Promise<Response> {
  if (!acceptsBrotli(request) || !response.body || response.headers.has("Content-Encoding")) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.set("Content-Encoding", "br");
  headers.append("Vary", "Accept-Encoding");
  headers.delete("Content-Length");

  return new Response(response.body.pipeThrough(new CompressionStream("brotli")), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function jsonResponse(
  request: Request,
  value: unknown,
  status = 200,
  contentType = "application/json; charset=utf-8"
): Promise<Response> {
  return maybeCompressBrotli(
    request,
    new Response(JSON.stringify(value, null, 2), {
      status,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-store",
      },
    })
  );
}

export function createPodibleFetchHandler(repo: BooksRepo, startTime: number): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    const startedAt = Date.now();
    let settings = repo.getSettings();
    const url = new URL(request.url);
    const method = request.method;
    const pathname = url.pathname;
    const appLoginPath = parseAppLoginPath(pathname);
    const redirectTo = sanitizeRedirectPath(url.searchParams.get("redirectTo"));
    let logSuffix = "";
    let currentSession = resolveSessionFromRequest(request, (tokenHash) => repo.getSessionByTokenHash(tokenHash));
    if (currentSession) {
      currentSession = repo.touchSession(currentSession.id) ?? currentSession;
    }

    const logRequest = (status: number): void => {
      const elapsedMs = Date.now() - startedAt;
      const suffix = logSuffix ? ` ${logSuffix}` : "";
      console.log(`[http] ${method} ${pathname} status=${status} ms=${elapsedMs}${suffix}`);
    };

    let response: Response;
    const isAuthenticatedRequest = currentSession !== null;

    const publicRoute = isPublicRoute(pathname, appLoginPath);
    const isRpcRoute = pathname === "/rpc" || pathname.startsWith("/rpc/");
    if (!publicRoute && !isRpcRoute && !isAuthenticatedRequest) {
      if (request.method === "GET" && isHtmlPageRoute(pathname)) {
        const nextPath = `${pathname}${url.search}`;
        response = redirect(`/login?redirectTo=${encodeURIComponent(nextPath)}`);
      } else {
        response = new Response("Unauthorized", {
          status: 401,
          headers: { "WWW-Authenticate": 'Bearer realm="podible"' },
        });
      }
      logRequest(response.status);
      return response;
    }

    try {
      const hasAdminAccess = (currentSession?.is_admin ?? 0) === 1;

      const authMatch = await handleAuthRoute({
        repo,
        request,
        settings,
        currentSession,
        pathname,
        method,
        redirectTo,
        appLoginPath,
      });
      if (authMatch) {
        settings = authMatch.settings;
        response = authMatch.response;
        logRequest(response.status);
        return response;
      }

      if (isAdminRoute(pathname) && !hasAdminAccess) {
        response = new Response("Forbidden", { status: 403 });
        logRequest(response.status);
        return response;
      }

      const adminMatch = await handleAdminRoute({
        repo,
        request,
        settings,
        currentSession,
        pathname,
        method,
      });
      if (adminMatch) {
        settings = adminMatch.settings;
        response = adminMatch.response;
        logRequest(response.status);
        return response;
      }

      const userResponse = await handleUserRoute({
        repo,
        request,
        settings,
        currentSession,
        pathname,
        method,
        isAuthenticatedRequest,
      });
      if (userResponse) {
        response = userResponse;
        logRequest(response.status);
        return response;
      }

      if (pathname === "/rpc" && request.method === "POST") {
        try {
          const cloned = await request.clone().text();
          const payload = JSON.parse(cloned) as { method?: unknown };
          if (typeof payload.method === "string" && payload.method.trim()) {
            logSuffix = `rpc=${payload.method.trim()}`;
          }
        } catch {
          // ignore parse errors in logging path; handler will return JSON-RPC parse errors.
        }
        response = await handleRpcRequest(request, { repo, startTime, request, session: currentSession });
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/rpc/") && request.method === "GET") {
        const parts = pathname
          .slice("/rpc/".length)
          .split("/")
          .filter(Boolean);
        if (parts.length > 0) {
          logSuffix = `rpc=${parts.join(".")}`;
        }
        if (parts.length !== 2) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const params: Record<string, unknown> = {};
        for (const [key, value] of url.searchParams.entries()) {
          const existing = params[key];
          if (existing === undefined) {
            params[key] = value;
            continue;
          }
          if (Array.isArray(existing)) {
            existing.push(value);
          } else {
            params[key] = [existing, value];
          }
        }
        response = await handleRpcMethod(parts.join("."), params, { repo, startTime, request, session: currentSession }, { id: null, readOnly: true });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/assets" && request.method === "GET") {
        const bookId = parseId(url.searchParams.get("bookId") ?? "");
        const assets = repo.listAssetsByBook(bookId).map((asset) => ({
          ...asset,
          files: repo.getAssetFiles(asset.id),
          stream_ext: streamExtension(asset),
        }));
        response = json({ assets });
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/stream/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.split(".")[0] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const book = repo.getBookByAsset(assetId);
        response = await streamAudioAsset(request, repo, target.asset, target.files, book?.cover_path);
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/chapters/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.replace(/\.json$/i, ""));
        const target = repo.getAssetWithFiles(assetId);
        if (!target) {
          response = json({ error: "not_found" }, 404);
          logRequest(response.status);
          return response;
        }
        const chapters = await buildChapters(repo, target.asset, target.files);
        if (!chapters) {
          response = json({ error: "not_found" }, 404);
          logRequest(response.status);
          return response;
        }
        response = await jsonResponse(request, chapters);
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/transcripts/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.replace(/\.json$/i, ""));
        const asset = repo.getAsset(assetId);
        if (!asset || asset.kind === "ebook") {
          response = json({ error: "not_found" }, 404);
          logRequest(response.status);
          return response;
        }
        const transcript = await loadStoredTranscriptPayload(repo, assetId);
        if (!transcript) {
          response = json({ error: "not_found" }, 404);
          logRequest(response.status);
          return response;
        }
        response = await jsonResponse(request, transcript);
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/covers/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const bookId = parseId(idPart.replace(/\.jpg$/i, ""));
        const book = repo.getBookRow(bookId);
        if (!book?.cover_path) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const file = Bun.file(book.cover_path);
        if (!(await file.exists())) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        response = new Response(file, {
          headers: {
            "Content-Type": book.cover_path.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
          },
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/feed.xml" && request.method === "GET") {
        response = buildRssFeed(request, repo, settings.feed.title, settings.feed.author);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/feed.json" && request.method === "GET") {
        response = buildJsonFeed(request, repo, settings.feed.title, settings.feed.author);
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/ebook/") && request.method === "GET") {
        const assetId = parseId(pathname.split("/")[2] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target || target.asset.kind !== "ebook") {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const first = target.files[0];
        if (!first) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const file = Bun.file(first.path);
        if (!(await file.exists())) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        response = new Response(file, {
          headers: {
            "Content-Type": target.asset.mime,
            "Content-Disposition": `attachment; filename="${first.path.split("/").pop() ?? `book-${assetId}`}"`,
          },
        });
        logRequest(response.status);
        return response;
      }

      response = new Response("Not found", { status: 404 });
      logRequest(response.status);
      return response;
    } catch (error) {
      response = json({ error: (error as Error).message }, 400);
      logRequest(response.status);
      return response;
    }
  };
}
