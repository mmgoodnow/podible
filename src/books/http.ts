import { promises as fs } from "node:fs";

import { buildJsonFeed, buildRssFeed } from "./feed";
import { searchOpenLibrary } from "./openlibrary";
import {
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
  resolveSessionFromRequest,
} from "./auth";
import { loadStoredTranscriptPayload } from "./chapter-analysis";
import { buildChapters, streamAudioAsset, streamExtension } from "./media";
import { buildPlexAuthUrl, createEphemeralPlexIdentity, createPlexPin, fetchPlexServerDevices } from "./plex";
import { BooksRepo } from "./repo";
import {
  addApiKey,
  createBookFromOpenLibrary,
  isHtmlPageRoute,
  parseAppLoginPath,
  parseMediaSelection,
  renderActivityPage,
  renderAddPage,
  renderAdminPage,
  renderAppAuthErrorPage,
  renderBookPage,
  renderLandingPage,
  renderLibraryPage,
  renderLoginPage,
  renderPlexImmediateResultPage,
  renderPlexLoadingPage,
  sanitizeRedirectPath,
  waitForPlexLoginResult,
} from "./http/support";
import { handleRpcMethod, handleRpcRequest } from "./rpc";
import { triggerAutoAcquire } from "./service";
import type { AppSettings } from "./types";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

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

function redirect(location: string, status = 303): Response {
  return new Response(null, {
    status,
    headers: {
      Location: location,
      "Cache-Control": "no-store",
    },
  });
}

function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid id");
  }
  return parsed;
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

    const isPublicRoute =
      pathname === "/" ||
      pathname === "/login" ||
      pathname === "/logout" ||
      pathname === "/login/plex/start" ||
      pathname === "/login/plex/loading" ||
      pathname === "/login/plex/complete" ||
      appLoginPath !== null;
    const isRpcRoute = pathname === "/rpc" || pathname.startsWith("/rpc/");
    if (!isPublicRoute && !isRpcRoute && !isAuthenticatedRequest) {
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

      if (pathname === "/login" && request.method === "GET") {
        response = renderLoginPage(settings, {
          notice: url.searchParams.get("notice"),
          error: url.searchParams.get("error"),
          currentUser: currentSession,
          redirectTo,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/login/plex/start" && request.method === "POST") {
        if (settings.auth.mode !== "plex") {
          response = json({ error: "Plex sign-in is not enabled." }, 403);
          logRequest(response.status);
          return response;
        }
        try {
          repo.deleteExpiredPlexLoginAttempts(new Date(Date.now() - 15 * 60_000).toISOString());
          const identity = createEphemeralPlexIdentity(settings.auth.plex.productName);
          const pin = await createPlexPin(identity);
          repo.createPlexLoginAttempt({
            pinId: pin.id,
            clientIdentifier: identity.clientIdentifier,
            publicJwkJson: JSON.stringify(identity.publicJwk),
            privateKeyPkcs8: identity.privateKeyPkcs8,
          });
          console.log(`[plex] created pin id=${pin.id} clientId=${identity.clientIdentifier}`);
          const forwardUrl = new URL(request.url);
          forwardUrl.pathname = "/login/plex/complete";
          forwardUrl.search = "";
          forwardUrl.searchParams.set("pinId", String(pin.id));
          if (redirectTo) {
            forwardUrl.searchParams.set("redirectTo", redirectTo);
          }
          response = json({
            pinId: pin.id,
            authUrl: buildPlexAuthUrl(identity, pin.code, forwardUrl.toString()),
          });
        } catch (error) {
          response = json({ error: (error as Error).message || "Unable to start Plex sign-in." }, 502);
        }
        logRequest(response.status);
        return response;
      }

      if (pathname === "/login/plex/loading" && request.method === "GET") {
        response = renderPlexLoadingPage(settings);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/login/plex/complete" && request.method === "GET") {
        if (settings.auth.mode !== "plex") {
          response = renderPlexImmediateResultPage({ ok: false, redirectTo: "/", error: "Plex sign-in is not enabled." });
          logRequest(response.status);
          return response;
        }
        const pinId = Number.parseInt(url.searchParams.get("pinId") ?? "", 10);
        if (!Number.isInteger(pinId) || pinId <= 0) {
          response = renderPlexImmediateResultPage({ ok: false, redirectTo: "/login", error: "Missing or invalid Plex PIN id." });
          logRequest(response.status);
          return response;
        }
        const result = await waitForPlexLoginResult(repo, settings, pinId, null, redirectTo);
        settings = result.settings;
        response = renderPlexImmediateResultPage({
          ok: result.kind === "success",
          redirectTo: result.redirectTo,
          error: result.kind === "error" ? result.error : null,
        });
        if (result.kind === "success") {
          response.headers.append("Set-Cookie", buildSessionCookie(result.sessionToken, request));
        }
        logRequest(response.status);
        return response;
      }

      if (pathname === "/logout" && request.method === "POST") {
        if (currentSession) {
          repo.deleteSession(currentSession.id);
        }
        response = redirect("/login?notice=Signed%20out.");
        response.headers.append("Set-Cookie", clearSessionCookie(request));
        logRequest(response.status);
        return response;
      }

      if (appLoginPath && request.method === "GET") {
        repo.deleteExpiredAppLoginAttempts(new Date().toISOString());
        const attempt = repo.getAppLoginAttempt(appLoginPath.attemptId);
        if (!attempt) {
          response = new Response(await renderAppAuthErrorPage(settings, "This app sign-in attempt is missing or has expired.").text(), {
            status: 400,
            headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
          });
          logRequest(response.status);
          return response;
        }
        const attemptPath = `/auth/app/${encodeURIComponent(attempt.id)}`;
        if (appLoginPath.isComplete) {
          if (!currentSession) {
            response = redirect(attemptPath);
            logRequest(response.status);
            return response;
          }
          const code = createSessionToken();
          repo.createAuthCode({
            codeHash: hashSessionToken(code),
            userId: currentSession.user_id,
            attemptId: attempt.id,
            expiresAt: new Date(Date.now() + 5 * 60_000).toISOString(),
          });
          const callbackUrl = new URL(attempt.redirect_uri);
          callbackUrl.searchParams.set("code", code);
          callbackUrl.searchParams.set("state", attempt.state);
          response = redirect(callbackUrl.toString(), 302);
          logRequest(response.status);
          return response;
        }
        if (currentSession) {
          response = redirect(`${attemptPath}/complete`);
          logRequest(response.status);
          return response;
        }
        response = renderLoginPage(settings, {
          currentUser: currentSession,
          redirectTo: `${attemptPath}/complete`,
          inlinePlexLogin: true,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/" && request.method === "GET") {
        response = isAuthenticatedRequest
          ? renderLandingPage(repo, settings, currentSession, null)
          : renderLoginPage(settings, {
              currentUser: currentSession,
            });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin" && !hasAdminAccess) {
        response = new Response("Forbidden", { status: 403 });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin/plex" && request.method === "GET") {
        response = redirect("/admin");
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin/plex/select" && request.method === "POST") {
        if (settings.auth.mode !== "plex") {
          response = new Response("Forbidden", { status: 403 });
          logRequest(response.status);
          return response;
        }
        const form = new URLSearchParams(await request.text());
        const machineId = (form.get("machineId") ?? "").trim();
        if (!machineId) {
          response = redirect("/admin?plex_error=Missing%20machine%20id");
          logRequest(response.status);
          return response;
        }
        settings = repo.updateSettings({
          ...settings,
          auth: {
            ...settings.auth,
            plex: {
              ...settings.auth.plex,
              machineId,
            },
          },
        });
        response = redirect("/admin?plex_notice=Selected%20server.");
        logRequest(response.status);
        return response;
      }

      if (pathname === "/library" && request.method === "GET") {
        response = renderLibraryPage(repo, settings, {
          query: url.searchParams.get("q"),
          currentUser: currentSession,
          apiKey: null,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/add" && request.method === "GET") {
        const query = (url.searchParams.get("q") ?? "").trim();
        if (!query) {
          response = renderAddPage(settings, { currentUser: currentSession, apiKey: null });
          logRequest(response.status);
          return response;
        }
        try {
          const results = await searchOpenLibrary(query, 10);
          response = renderAddPage(settings, {
            query,
            results,
            status: `Found ${results.length} result${results.length === 1 ? "" : "s"} for “${query}”.`,
            currentUser: currentSession,
            apiKey: null,
          });
        } catch (error) {
          response = renderAddPage(settings, {
            query,
            error: `Search failed: ${(error as Error).message}`,
            currentUser: currentSession,
            apiKey: null,
          });
        }
        logRequest(response.status);
        return response;
      }

      if (pathname === "/add" && request.method === "POST") {
        const body = await request.text();
        const form = new URLSearchParams(body);
        const openLibraryKey = (form.get("openLibraryKey") ?? "").trim();
        if (!openLibraryKey) {
          const addResponse = renderAddPage(settings, {
            error: "openLibraryKey is required.",
            currentUser: currentSession,
            apiKey: null,
          });
          response = new Response(await addResponse.text(), {
            status: 400,
            headers: addResponse.headers,
          });
          logRequest(response.status);
          return response;
        }
        try {
          const bookId = await createBookFromOpenLibrary(repo, openLibraryKey);
          response = redirect(`/book/${bookId}`);
        } catch (error) {
          const addResponse = renderAddPage(settings, {
            error: `Add failed: ${(error as Error).message}`,
            currentUser: currentSession,
            apiKey: null,
          });
          response = new Response(await addResponse.text(), {
            status: 400,
            headers: addResponse.headers,
          });
        }
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/book/") && request.method === "GET") {
        const bookId = parseId(pathname.split("/")[2] ?? "");
        response = await renderBookPage(repo, settings, bookId, {
          notice: url.searchParams.get("notice"),
          error: url.searchParams.get("error"),
          currentUser: currentSession,
          apiKey: null,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname.startsWith("/book/") && pathname.endsWith("/acquire") && request.method === "POST") {
        const bookId = parseId(pathname.split("/")[2] ?? "");
        const body = await request.text();
        const form = new URLSearchParams(body);
        const media = parseMediaSelection(form.get("media"));
        const book = repo.getBookRow(bookId);
        if (!book) {
          response = new Response("Not found", { status: 404 });
          logRequest(response.status);
          return response;
        }
        const jobId = await triggerAutoAcquire(repo, bookId, media);
        const notice = `Queued ${media.join(" + ")} acquire for ${book.title} (job ${jobId}).`;
        response = redirect(`/book/${bookId}?notice=${encodeURIComponent(notice)}`);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/activity" && request.method === "GET") {
        response = renderActivityPage(repo, settings, {
          notice: url.searchParams.get("notice"),
          error: url.searchParams.get("error"),
          currentUser: currentSession,
          apiKey: null,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/activity/refresh" && request.method === "POST") {
        const job = repo.createJob({ type: "full_library_refresh" });
        response = redirect(`/activity?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin/refresh" && request.method === "POST") {
        const job = repo.createJob({ type: "full_library_refresh" });
        response = redirect(`/admin?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin" && request.method === "GET") {
        let plexServers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }> = [];
        const notice = url.searchParams.get("notice");
        const error = url.searchParams.get("error");
        let plexError = url.searchParams.get("plex_error");
        if (settings.auth.mode === "plex" && settings.auth.plex.ownerToken) {
          try {
            plexServers = await fetchPlexServerDevices(settings);
          } catch (error) {
            plexError = plexError || (error as Error).message || "Unable to load Plex servers.";
          }
        }
        response = renderAdminPage(repo, settings, currentSession, {
          plexServers,
          apiKey: null,
          notice,
          error,
          plexNotice: url.searchParams.get("plex_notice"),
          plexError,
        });
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
