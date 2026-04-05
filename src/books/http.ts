import { promises as fs } from "node:fs";

import { hydrateBookFromOpenLibrary } from "./hydration";
import { buildJsonFeed, buildRssFeed } from "./feed";
import { resolveOpenLibraryCandidate, searchOpenLibrary, type OpenLibraryCandidate } from "./openlibrary";
import {
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
  resolveSessionFromRequest,
  sessionExpiresAt,
} from "./auth";
import { loadStoredTranscriptPayload, selectPreferredEpubAsset } from "./chapter-analysis";
import { buildChapters, preferredAudioForBooks, selectPreferredAudioAsset, streamAudioAsset, streamExtension } from "./media";
import {
  buildPlexAuthUrl,
  checkPlexUserAccess,
  createEphemeralPlexIdentity,
  createPlexPin,
  exchangePlexPinForToken,
  fetchPlexServerDevices,
  fetchPlexUser,
} from "./plex";
import { BooksRepo } from "./repo";
import { handleRpcMethod, handleRpcRequest } from "./rpc";
import { triggerAutoAcquire } from "./service";
import type { AppSettings, PlexJwk, SessionWithUserRow, UserRow } from "./types";

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

function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid id");
  }
  return parsed;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function addApiKey(path: string, apiKey: string | null): string {
  void apiKey;
  return path;
}

function truncateText(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

function formatMinutes(durationMs: number | null): string {
  if (!durationMs || !Number.isFinite(durationMs) || durationMs <= 0) return "Unknown";
  return `${Math.round(durationMs / 60000)} min`;
}

function parseMediaSelection(value: string | null): Array<"audio" | "ebook"> {
  if (value === "audio") return ["audio"];
  if (value === "ebook") return ["ebook"];
  return ["audio", "ebook"];
}

function messageMarkup(notice?: string | null, error?: string | null): string {
  const blocks: string[] = [];
  if (notice?.trim()) {
    blocks.push(`<p class="muted" style="margin-top: 10px;">${escapeHtml(notice)}</p>`);
  }
  if (error?.trim()) {
    blocks.push(`<p style="margin-top: 10px; color: #8b0000;">${escapeHtml(error)}</p>`);
  }
  return blocks.join("");
}

function describeBookState(book: {
  status: string;
  audioStatus: string;
  ebookStatus: string;
}): string {
  if (book.audioStatus === "imported" && book.ebookStatus === "imported") {
    return "Audio and eBook are both ready.";
  }
  if (book.audioStatus === "imported") {
    return "Audio is ready now.";
  }
  if (book.ebookStatus === "imported") {
    return "The eBook is ready while audio is still in progress.";
  }
  if (book.status === "error") {
    return "This book needs attention before it will be fully ready.";
  }
  if (book.status === "downloading" || book.status === "snatched") {
    return "Podible is still working on this book.";
  }
  return "This book is still being prepared.";
}

function formatOverallStatus(status: string): string {
  if (status === "imported") return "Ready";
  if (status === "partial") return "Partially ready";
  if (status === "downloading") return "In progress";
  if (status === "downloaded") return "Downloaded";
  if (status === "snatched") return "Queued";
  if (status === "error") return "Needs attention";
  return "Wanted";
}

function formatMediaStatus(label: string, status: string): string {
  if (status === "imported") return `${label} ready`;
  if (status === "downloading") return `${label} downloading`;
  if (status === "downloaded") return `${label} downloaded`;
  if (status === "snatched") return `${label} queued`;
  if (status === "error") return `${label} needs attention`;
  return `${label} wanted`;
}

function formatBookStatusLine(book: {
  status: string;
  audioStatus: string;
  ebookStatus: string;
  fullPseudoProgress?: number;
}): string {
  const parts = [formatOverallStatus(book.status), formatMediaStatus("Audio", book.audioStatus), formatMediaStatus("eBook", book.ebookStatus)];
  if (typeof book.fullPseudoProgress === "number" && Number.isFinite(book.fullPseudoProgress) && book.status !== "imported") {
    parts.push(`${book.fullPseudoProgress}%`);
  }
  return parts.join(" • ");
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

function displayUserName(user: Pick<UserRow, "display_name" | "username">): string {
  return user.display_name?.trim() || user.username;
}

function sanitizeRedirectPath(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed.startsWith("/")) return null;
  if (trimmed.startsWith("//")) return null;
  return trimmed;
}

function parseAppLoginPath(pathname: string): { attemptId: string; isComplete: boolean } | null {
  const match = pathname.match(/^\/auth\/app\/([^/]+?)(\/complete)?$/);
  if (!match?.[1]) return null;
  return {
    attemptId: decodeURIComponent(match[1]),
    isComplete: match[2] === "/complete",
  };
}

function renderAppAuthErrorPage(settings: AppSettings, message: string): Response {
  const body = `
    <section class="hero">
      <h1>App sign-in</h1>
      <p>${escapeHtml(message)}</p>
    </section>`;
  return renderAppPage("App sign in", body, settings, null);
}

function isHtmlPageRoute(pathname: string): boolean {
  return pathname === "/" || pathname === "/library" || pathname === "/add" || pathname === "/activity" || pathname === "/admin" || pathname === "/login" || pathname === "/login/plex/loading" || pathname.startsWith("/book/");
}

function renderAppPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  extraNav = "",
  apiKey: string | null = null
): Response {
  const showAdminNav = Boolean(apiKey) || (currentUser?.is_admin ?? 0) === 1;
  const accountNav = currentUser
    ? `<span class="muted">Signed in as ${escapeHtml(displayUserName(currentUser))}</span>
       <form method="post" action="${escapeHtml(addApiKey("/logout", apiKey))}" class="nav-signout-form">
         <button type="submit" class="nav-signout-button">Sign out</button>
       </form>`
    : `<a href="${escapeHtml(addApiKey("/login", apiKey))}">Sign in</a>`;
  const nav = `
    <nav class="site-nav">
      <a href="${escapeHtml(addApiKey("/", apiKey))}">Home</a>
      <a href="${escapeHtml(addApiKey("/library", apiKey))}">Library</a>
      <a href="${escapeHtml(addApiKey("/add", apiKey))}">Add</a>
      <a href="${escapeHtml(addApiKey("/activity", apiKey))}">Activity</a>
      ${showAdminNav ? `<a href="${escapeHtml(addApiKey("/admin", apiKey))}">Admin</a>` : ""}
      ${accountNav}
      ${extraNav}
    </nav>`;
  return new Response(
    `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      :root {
        --bg: #f6f7f3;
        --paper: #fffdf7;
        --line: #ddd6c8;
        --text: #1f261c;
        --muted: #5f6b58;
        --accent: #285943;
        --accent-soft: #eef5f0;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: ui-serif, Georgia, serif;
        background: radial-gradient(circle at top, #fffefb 0%, var(--bg) 60%);
        color: var(--text);
      }
      a { color: var(--accent); text-decoration: none; }
      a:hover { text-decoration: underline; }
      .page { max-width: 1120px; margin: 0 auto; padding: 18px; }
      .site-nav { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 18px; font-size: 14px; }
      .nav-signout-form { display: inline-flex; margin: 0; }
      .site-nav .nav-signout-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 36px;
        padding: 8px 12px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: #fff;
        color: var(--text);
        font: inherit;
        cursor: pointer;
      }
      .site-nav .nav-signout-button:hover { background: #faf8f1; }
      .hero, .card { background: var(--paper); border: 1px solid var(--line); border-radius: 16px; box-shadow: 0 1px 2px rgba(31,38,28,.05); }
      .hero { padding: 20px; margin-bottom: 18px; }
      .hero h1 { margin: 0 0 8px; font-size: 34px; line-height: 1.05; }
      .hero p { margin: 0; color: var(--muted); max-width: 70ch; }
      .grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 14px; }
      .span-12 { grid-column: span 12; }
      .span-8 { grid-column: span 8; }
      .span-6 { grid-column: span 6; }
      .span-4 { grid-column: span 4; }
      .card { padding: 14px; }
      .card h2 { margin: 0 0 8px; font-size: 18px; }
      .muted { color: var(--muted); }
      .book-list { display: grid; gap: 10px; }
      .book-row { display: grid; grid-template-columns: 72px minmax(0, 1fr); gap: 12px; align-items: start; padding: 10px; border: 1px solid var(--line); border-radius: 12px; background: #fff; }
      .cover, .cover-fallback { width: 72px; height: 72px; border-radius: 10px; }
      .cover-fallback { display: flex; align-items: center; justify-content: center; background: var(--accent-soft); color: var(--accent); font-weight: 700; }
      .cover { object-fit: cover; display: block; border: 1px solid var(--line); }
      .meta h3 { margin: 0 0 2px; font-size: 18px; }
      .meta p { margin: 0; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; font-size: 14px; }
      .button-link, .actions button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 36px;
        padding: 8px 12px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: #fff;
        color: var(--text);
        text-decoration: none;
      }
      .button-link:hover { text-decoration: none; }
      .button-link-primary {
        background: var(--accent);
        border-color: var(--accent);
        color: #fff;
      }
      .button-link-primary:hover {
        color: #fff;
      }
      .pill { display: inline-flex; padding: 3px 8px; border-radius: 999px; border: 1px solid var(--line); background: #fff; font-size: 12px; color: var(--muted); }
      .stats { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
      .empty { padding: 16px; border: 1px dashed var(--line); border-radius: 12px; color: var(--muted); background: #fff; }
      .detail-grid { display: grid; grid-template-columns: 180px minmax(0, 1fr); gap: 18px; }
      .detail-cover, .detail-cover-fallback { width: 180px; height: 180px; border-radius: 16px; }
      .detail-cover { object-fit: cover; border: 1px solid var(--line); display: block; }
      .detail-cover-fallback { display: flex; align-items: center; justify-content: center; background: var(--accent-soft); color: var(--accent); font-size: 42px; font-weight: 700; }
      .section-list { display: grid; gap: 8px; }
      .chapter-row { display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px solid var(--line); font-size: 14px; }
      @media (max-width: 900px) {
        .span-8, .span-6, .span-4 { grid-column: span 12; }
        .detail-grid { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div class="page">
      ${nav}
      ${body}
    </div>
  </body>
</html>`,
    {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    }
  );
}

function coverMarkup(coverUrl: string | null, title: string, large = false): string {
  const initials =
    title
      .split(/\s+/)
      .map((part) => part[0] || "")
      .join("")
      .slice(0, 2)
      .toUpperCase() || "BK";
  if (!coverUrl) {
    return `<div class="${large ? "detail-cover-fallback" : "cover-fallback"}">${escapeHtml(initials)}</div>`;
  }
  return `<img class="${large ? "detail-cover" : "cover"}" src="${escapeHtml(coverUrl)}" alt="${escapeHtml(title)} cover" />`;
}

function renderLandingPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  apiKey: string | null = null
): Response {
  const recentBooks = repo.listAllBooks().slice(0, 6);
  const featured = preferredAudioForBooks(repo).slice(0, 6);
  const inProgress = repo.listInProgressBooks().slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 6);
  const body = `
    <section class="hero">
      <h1>Podible</h1>
      <p>Your shelf for audiobooks and eBooks.</p>
      <div class="stats">
        <span class="pill">${featured.length} ready to play</span>
        <span class="pill">${inProgress.length} in progress</span>
        <span class="pill">${needsAttention.length} need attention</span>
      </div>
      <div class="actions" style="margin-top: 14px;">
        <a href="${escapeHtml(addApiKey("/library", apiKey))}">Browse library</a>
        <a href="${escapeHtml(addApiKey("/add", apiKey))}">Add a book</a>
      </div>
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>Ready now</h2>
        ${
          featured.length > 0
            ? `<div class="book-list">${featured
                .map(({ book, asset }) => {
                  const detailUrl = addApiKey(`/book/${book.id}`, apiKey);
                  const streamUrl = addApiKey(`/stream/${asset.id}.${streamExtension(asset)}`, apiKey);
                  return `<article class="book-row">
                    ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title)}
                    <div class="meta">
                      <h3><a href="${escapeHtml(detailUrl)}">${escapeHtml(book.title)}</a></h3>
                      <p class="muted">${escapeHtml(book.author)}</p>
                      <p class="muted">${formatMinutes(book.durationMs)} • ${escapeHtml(formatBookStatusLine(book))}</p>
                      <p class="muted">${escapeHtml(truncateText((book.description || `${book.title} by ${book.author}`).replace(/\s+/g, " "), 160))}</p>
                      <div class="actions">
                        <a href="${escapeHtml(detailUrl)}">Details</a>
                        <a href="${escapeHtml(streamUrl)}">Play</a>
                      </div>
                    </div>
                  </article>`;
                })
                .join("")}</div>`
            : `<div class="empty">No playable books yet.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Still working</h2>
        ${
          inProgress.length > 0
            ? `<div class="section-list">${inProgress
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No active work right now.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Needs attention</h2>
        ${
          needsAttention.length > 0
            ? `<div class="section-list">${needsAttention
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">Nothing needs attention right now.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Recently added</h2>
        ${
          recentBooks.length > 0
            ? `<div class="section-list">${recentBooks
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatOverallStatus(book.status))}${book.durationMs ? ` • ${formatMinutes(book.durationMs)}` : ""}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No books yet.</div>`
        }
      </section>
    </div>`;
  return renderAppPage("Podible", body, settings, currentUser, "", apiKey);
}

function renderLibraryPage(
  repo: BooksRepo,
  settings: AppSettings,
  options: { query?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const query = options.query?.trim() ?? "";
  const books = repo.listBooks(200, undefined, query || undefined).items;
  const body = `
    <section class="hero">
      <h1>Library</h1>
      <p>${books.length} book${books.length === 1 ? "" : "s"}${query ? ` matching “${escapeHtml(query)}”` : ""}.</p>
      <form method="get" action="${escapeHtml(addApiKey("/library", apiKey))}">
        <div class="actions" style="margin-top: 14px;">
          <input type="search" name="q" value="${escapeHtml(query)}" placeholder="Search by title or author" style="min-width: 280px; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
          <button type="submit">Search</button>
          ${query ? `<a href="${escapeHtml(addApiKey("/library", apiKey))}">Clear</a>` : ""}
        </div>
      </form>
    </section>
    <section class="card span-12">
      ${
        books.length > 0
          ? `<div class="book-list">${books
              .map((book) => {
                const asset = selectPreferredAudioAsset(repo.listAssetsByBook(book.id));
                const detailUrl = addApiKey(`/book/${book.id}`, apiKey);
                const streamUrl = asset ? addApiKey(`/stream/${asset.id}.${streamExtension(asset)}`, apiKey) : null;
                return `<article class="book-row">
                  ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title)}
                  <div class="meta">
                    <h3><a href="${escapeHtml(detailUrl)}">${escapeHtml(book.title)}</a></h3>
                    <p class="muted">${escapeHtml(book.author)}</p>
                    <div class="stats">
                      <span class="pill">${escapeHtml(book.status)}</span>
                      <span class="pill">audio ${escapeHtml(book.audioStatus)}</span>
                      <span class="pill">ebook ${escapeHtml(book.ebookStatus)}</span>
                    </div>
                    <div class="actions">
                      <a href="${escapeHtml(detailUrl)}">Details</a>
                      ${streamUrl ? `<a href="${escapeHtml(streamUrl)}">Play</a>` : ""}
                    </div>
                  </div>
                </article>`;
              })
              .join("")}</div>`
          : `<div class="empty">No books found.</div>`
      }
    </section>`;
  return renderAppPage("Library", body, settings, options.currentUser ?? null, "", apiKey);
}

function renderAddPage(
  settings: AppSettings,
  options: {
    query?: string;
    results?: OpenLibraryCandidate[];
    status?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
    apiKey?: string | null;
  } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const query = options.query?.trim() ?? "";
  const results = options.results ?? [];
  const status = options.status?.trim() ?? "";
  const error = options.error?.trim() ?? "";
  const resultMarkup =
    query && results.length === 0 && !error
      ? `<div class="empty">No matches for “${escapeHtml(query)}”.</div>`
      : results.length > 0
        ? `<div class="book-list">${results
            .map((result) => {
              const publishYear = result.publishedAt ? new Date(result.publishedAt).getUTCFullYear() : null;
              return `<article class="book-row">
                ${coverMarkup(result.coverId ? `https://covers.openlibrary.org/b/id/${result.coverId}-L.jpg` : null, result.title)}
                <div class="meta">
                  <h3>${escapeHtml(result.title)}</h3>
                  <p class="muted">${escapeHtml(result.author)}${publishYear ? ` • ${publishYear}` : ""}</p>
                  <form method="post" action="${escapeHtml(addApiKey("/add", apiKey))}">
                    <input type="hidden" name="openLibraryKey" value="${escapeHtml(result.openLibraryKey)}" />
                    <div class="actions">
                      <button type="submit">Add and acquire</button>
                    </div>
                  </form>
                </div>
              </article>`;
            })
            .join("")}</div>`
        : `<div class="empty">Search by title and author to add a book.</div>`;

  const body = `
    <section class="hero">
      <h1>Add a book</h1>
      <p>Search for a book, pick the right match, and Podible will add it to your library and start finding files.</p>
    </section>
    <div class="grid">
      <section class="card span-12">
        <h2>Search catalog</h2>
        <form method="get" action="${escapeHtml(addApiKey("/add", apiKey))}">
          <div class="actions">
            <input type="search" name="q" value="${escapeHtml(query)}" placeholder="Title Author (e.g. Hyperion Dan Simmons)" style="min-width: 320px; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
            <button type="submit">Search</button>
          </div>
        </form>
        ${status ? `<p class="muted" style="margin-top: 10px;">${escapeHtml(status)}</p>` : ""}
        ${error ? `<p style="margin-top: 10px; color: #8b0000;">${escapeHtml(error)}</p>` : ""}
      </section>
      <section class="card span-12">
        <h2>Results</h2>
        ${resultMarkup}
      </section>
    </div>`;
  return renderAppPage("Add", body, settings, options.currentUser ?? null, "", apiKey);
}

function renderLoginPage(
  settings: AppSettings,
  options: {
    notice?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
    apiKey?: string | null;
    redirectTo?: string | null;
    inlinePlexLogin?: boolean;
  } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const redirectTo = sanitizeRedirectPath(options.redirectTo) ?? "/";
  const inlinePlexLogin = options.inlinePlexLogin === true;
  const plexStartPath = addApiKey(`/login/plex/start?redirectTo=${encodeURIComponent(redirectTo)}`, apiKey);
  const plexLoadingPath = addApiKey(`/login/plex/loading?redirectTo=${encodeURIComponent(redirectTo)}`, apiKey);
  const body = `
    <section class="hero">
      <h1>Sign in</h1>
      <p>Use your Plex account to sign in to this Podible instance.</p>
      ${messageMarkup(options.notice, options.error)}
      <div class="actions" style="margin-top: 14px;">
        <button id="plex-login-btn" type="button">Sign in with Plex</button>
      </div>
      <p id="plex-login-status" class="muted" style="margin-top: 10px;"></p>
    </section>
    <script>
      (() => {
        const button = document.getElementById("plex-login-btn");
        const status = document.getElementById("plex-login-status");
        const startUrl = ${JSON.stringify(plexStartPath)};
        const loadingUrl = ${JSON.stringify(plexLoadingPath)};
        const successPath = ${JSON.stringify(addApiKey(redirectTo, apiKey))};
        const inlineLogin = ${inlinePlexLogin ? "true" : "false"};
        function setStatus(message) {
          if (status) status.textContent = message || "";
        }
        if (!inlineLogin) {
          window.addEventListener("message", (event) => {
            if (event.origin !== window.location.origin || !event.data || event.data.type !== "podible-plex-login") {
              return;
            }
            if (event.data.ok) {
              window.location.href = event.data.redirectTo || successPath;
              return;
            }
            setStatus(event.data.error || "Plex sign-in failed.");
            if (button) button.disabled = false;
          });
        }
        button?.addEventListener("click", async () => {
          button.disabled = true;
          setStatus("Opening Plex sign-in…");
          const popup = inlineLogin ? null : window.open(loadingUrl, "podible-plex-login", "width=640,height=760");
          if (!inlineLogin && !popup) {
            setStatus("Popup blocked. Please allow popups for this site.");
            button.disabled = false;
            return;
          }
          try {
            const response = await fetch(startUrl, { method: "POST" });
            const payload = await response.json();
            if (!response.ok || !payload.authUrl) {
              throw new Error(payload.error || "Unable to start Plex sign-in.");
            }
            if (inlineLogin) {
              window.location.href = payload.authUrl;
              return;
            }
            popup.location.href = payload.authUrl;
            setStatus("Finish sign-in in the Plex window…");
          } catch (error) {
            if (popup) popup.close();
            setStatus(error && error.message ? error.message : "Unable to start Plex sign-in.");
            button.disabled = false;
          }
        });
      })();
    </script>`;
  return renderAppPage("Sign in", body, settings, options.currentUser ?? null, "", apiKey);
}

function renderPlexLoadingPage(settings: AppSettings, apiKey: string | null = null): Response {
  const body = `
    <section class="hero">
      <h1>Plex sign-in</h1>
      <p>Finish the Plex sign-in flow in this window. Podible will continue automatically when Plex redirects back.</p>
    </section>`;
  return renderAppPage("Plex sign in", body, settings, null, "", apiKey);
}

async function waitForPlexLoginResult(
  repo: BooksRepo,
  settings: AppSettings,
  pinId: number,
  apiKey: string | null,
  redirectTo: string | null
): Promise<
  | { kind: "error"; settings: AppSettings; error: string; redirectTo: string }
  | { kind: "success"; settings: AppSettings; redirectTo: string; sessionToken: string }
> {
  let currentSettings = settings;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const result = await resolvePlexLoginStatus(repo, currentSettings, pinId, apiKey, redirectTo);
    currentSettings = result.settings;
    if (result.kind === "success" || result.kind === "error") {
      return result;
    }
    await Bun.sleep(3000);
  }
  return {
    kind: "error",
    settings: currentSettings,
    error: "Timed out waiting for Plex sign-in.",
    redirectTo: "/login",
  };
}

function renderPlexImmediateResultPage(
  result: { ok: boolean; redirectTo: string; error?: string | null }
): Response {
  const payload = JSON.stringify({
    type: "podible-plex-login",
    ok: result.ok,
    redirectTo: result.redirectTo,
    error: result.error ?? null,
  });
  return new Response(
    `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Plex sign-in complete</title>
  </head>
  <body style="font-family: ui-sans-serif, system-ui, sans-serif; padding: 24px;">
    <p>${escapeHtml(result.ok ? "Sign-in complete. You can close this window." : result.error ?? "Plex sign-in failed.")}</p>
    <script>
      (function () {
        var payload = ${payload};
        if (window.opener && !window.opener.closed) {
          window.opener.postMessage(payload, window.location.origin);
          ${result.ok ? "window.close();" : ""}
        } else if (payload.ok && payload.redirectTo) {
          window.location.href = payload.redirectTo;
        }
      })();
    </script>
  </body>
</html>`,
    {
      status: result.ok ? 200 : 400,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    }
  );
}

async function resolvePlexLoginStatus(
  repo: BooksRepo,
  settings: AppSettings,
  pinId: number,
  apiKey: string | null = null,
  redirectTo: string | null = null
): Promise<
  | { kind: "pending"; settings: AppSettings; message: string }
  | { kind: "error"; settings: AppSettings; error: string; redirectTo: string }
  | { kind: "success"; settings: AppSettings; redirectTo: string; sessionToken: string }
> {
  const attempt = repo.getPlexLoginAttempt(pinId);
  if (!attempt) {
    return {
      kind: "error",
      settings,
      error: "This Plex login attempt is missing or has expired.",
      redirectTo: "/login",
    };
  }
  const ageMs = Date.now() - Date.parse(attempt.created_at);
  if (!Number.isFinite(ageMs) || ageMs > 15 * 60_000) {
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "error",
      settings,
      error: "This Plex login attempt has expired. Please try again.",
      redirectTo: "/login",
    };
  }
  const identity = {
    productName: settings.auth.plex.productName,
    clientIdentifier: attempt.client_identifier,
    publicJwk: JSON.parse(attempt.public_jwk_json) as PlexJwk,
    privateKeyPkcs8: attempt.private_key_pkcs8,
  };

  console.log(
    `[plex] checking login status pinId=${pinId} ownerToken=${settings.auth.plex.ownerToken ? "present" : "missing"} machineId=${settings.auth.plex.machineId || "missing"} clientId=${attempt.client_identifier}`
  );

  let plexToken: string;
  try {
    plexToken = await exchangePlexPinForToken(identity, pinId);
  } catch (error) {
    const message = (error as Error).message || "";
    if (message.includes("has not been claimed yet")) {
      return {
        kind: "pending",
        settings,
        message: "Waiting for Plex to finish sign-in…",
      };
    }
    if (message.includes("rate limited")) {
      return {
        kind: "pending",
        settings,
        message: "Plex is asking Podible to slow down. Retrying…",
      };
    }
    console.log(`[plex] login failed pinId=${pinId} error=${message || "unknown"}`);
    return {
      kind: "error",
      settings,
      error: message || "Plex sign-in failed.",
      redirectTo: "/login",
    };
  }

  console.log(`[plex] pin claimed pinId=${pinId}`);

  try {
    const plexUser = await fetchPlexUser(settings, plexToken, attempt.client_identifier);
    console.log(`[plex] fetched user id=${plexUser.id} username=${plexUser.username}`);
    const existingPlexUser = repo.listUsers("plex").find((user) => user.provider_user_id === plexUser.id) ?? null;
    const existingUsers = repo.listUsers();
    const isBootstrap = existingUsers.length === 0;
    let allowed = false;
    let denialReason = "This Plex user is not allowed on this Podible instance.";

    if (isBootstrap) {
      allowed = true;
    } else if (!settings.auth.plex.ownerToken || !settings.auth.plex.machineId) {
      allowed = Boolean(existingPlexUser);
      if (!allowed) {
        denialReason = "An admin needs to choose which Plex server controls Podible access first.";
      }
    } else {
      const hasServerAccess =
        plexUser.id === (existingUsers.find((user) => user.is_admin === 1 && user.provider === "plex")?.provider_user_id ?? "")
          ? true
          : await checkPlexUserAccess(settings, plexUser.id);
      allowed = hasServerAccess;
    }

    if (!allowed) {
      repo.deletePlexLoginAttempt(pinId);
      return {
        kind: "error",
        settings,
        error: denialReason,
        redirectTo: "/login",
      };
    }

    const hasAdminUser = existingUsers.some((user) => user.is_admin === 1);
    if (!settings.auth.plex.ownerToken && (!hasAdminUser || existingPlexUser?.is_admin === 1)) {
      settings = repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          plex: {
            ...settings.auth.plex,
            ownerToken: plexToken,
          },
        },
      });
      console.log(`[plex] captured owner token from user id=${plexUser.id}`);
    }

    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: plexUser.id,
      username: plexUser.username,
      displayName: plexUser.displayName,
      thumbUrl: plexUser.thumbUrl,
      isAdmin: existingPlexUser ? existingPlexUser.is_admin === 1 : !hasAdminUser,
    });
    const sessionToken = createSessionToken();
    repo.createSession(user.id, hashSessionToken(sessionToken), sessionExpiresAt());
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "success",
      settings,
      redirectTo: addApiKey(sanitizeRedirectPath(redirectTo) ?? "/", apiKey),
      sessionToken,
    };
  } catch (error) {
    const message = (error as Error).message || "Plex sign-in failed.";
    console.log(`[plex] login failed pinId=${pinId} error=${message || "unknown"}`);
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "error",
      settings,
      error: message,
      redirectTo: "/login",
    };
  }
}

async function renderBookPage(
  repo: BooksRepo,
  settings: AppSettings,
  bookId: number,
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Promise<Response> {
  const apiKey = flash.apiKey ?? null;
  const book = repo.getBook(bookId);
  const bookRow = repo.getBookRow(bookId);
  if (!book || !bookRow) {
    return new Response("Not found", { status: 404 });
  }
  const assets = repo.listAssetsByBook(bookId);
  const audio = selectPreferredAudioAsset(assets);
  const ebook = selectPreferredEpubAsset(assets);
  const audioFiles = audio ? repo.getAssetFiles(audio.id) : [];
  const chapters = audio ? await buildChapters(repo, audio, audioFiles) : null;
  const transcriptUrl = audio ? addApiKey(`/transcripts/${audio.id}.json`, apiKey) : null;
  const streamUrl = audio ? addApiKey(`/stream/${audio.id}.${streamExtension(audio)}`, apiKey) : null;
  const chaptersUrl = audio ? addApiKey(`/chapters/${audio.id}.json`, apiKey) : null;
  const ebookUrl = ebook ? addApiKey(`/ebook/${ebook.id}`, apiKey) : null;
  const releases = repo.listReleasesByBook(bookId).slice(0, 8);
  const stateSummary = describeBookState(book);
  const body = `
    <section class="hero">
      <div class="detail-grid">
        ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title, true)}
        <div>
          <h1>${escapeHtml(book.title)}</h1>
          <p class="muted">${escapeHtml(book.author)}</p>
          <div class="stats">
            <span class="pill">${escapeHtml(stateSummary)}</span>
            <span class="pill">${escapeHtml(formatMediaStatus("Audio", book.audioStatus))}</span>
            <span class="pill">${escapeHtml(formatMediaStatus("eBook", book.ebookStatus))}</span>
          </div>
          <div class="actions" style="margin-top: 12px;">
            ${streamUrl ? `<a class="button-link button-link-primary" href="${escapeHtml(streamUrl)}">Play audio</a>` : ""}
            ${ebookUrl ? `<a class="button-link" href="${escapeHtml(ebookUrl)}">Download EPUB/PDF</a>` : ""}
          </div>
          ${messageMarkup(flash.notice, flash.error)}
          <p style="margin-top: 12px;">${escapeHtml(book.description || `${book.title} by ${book.author}`)}</p>
        </div>
      </div>
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>What you can do now</h2>
        <div class="section-list">
          <div><strong>Audio:</strong> ${audio ? "Ready to play" : "Still looking"}</div>
          <div><strong>eBook:</strong> ${ebook ? "Ready to download" : "Still looking"}</div>
          <div><strong>Duration:</strong> ${formatMinutes(book.durationMs)}</div>
        </div>
        <div class="actions" style="margin-top: 12px;">
          ${streamUrl ? `<a class="button-link button-link-primary" href="${escapeHtml(streamUrl)}">Play audio</a>` : ""}
          ${ebookUrl ? `<a class="button-link" href="${escapeHtml(ebookUrl)}">Download EPUB/PDF</a>` : ""}
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="audio" />
            <button type="submit">Find audio</button>
          </form>
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="ebook" />
            <button type="submit">Find ebook</button>
          </form>
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="both" />
            <button type="submit">Find both</button>
          </form>
        </div>
      </section>
      <section class="card span-6">
        <h2>Available now</h2>
        <div class="section-list">
          <div><strong>Audio:</strong> ${audio ? "Available" : "Not ready yet"}</div>
          <div><strong>eBook:</strong> ${ebook ? "Available" : "Not ready yet"}</div>
          <div><strong>Transcript:</strong> ${transcriptUrl ? "Available" : "Not ready yet"}</div>
          <div><strong>Chapters:</strong> ${chaptersUrl ? "Available" : "Not ready yet"}</div>
        </div>
        <div class="actions" style="margin-top: 12px;">
          ${chaptersUrl ? `<a class="button-link" href="${escapeHtml(chaptersUrl)}">Chapters JSON</a>` : ""}
          ${transcriptUrl ? `<a class="button-link" href="${escapeHtml(transcriptUrl)}">Transcript JSON</a>` : ""}
        </div>
      </section>
      <section class="card span-12">
        <h2>Chapter preview</h2>
        ${
          chapters?.chapters?.length
            ? `<div class="section-list">${chapters.chapters
                .slice(0, 12)
                .map((chapter) => `<div class="chapter-row"><span>${escapeHtml(chapter.title)}</span><span class="muted">${chapter.startTime.toFixed(0)}s</span></div>`)
                .join("")}${chapters.chapters.length > 12 ? `<div class="muted">+ ${chapters.chapters.length - 12} more</div>` : ""}</div>`
            : `<div class="empty">No chapter data yet.</div>`
        }
      </section>
      <section class="card span-12">
        <h2>Release history</h2>
        ${
          releases.length > 0
            ? `<div class="section-list">${releases
                .map(
                  (release) => `<div>
                    <strong>${escapeHtml(release.title)}</strong>
                    <div class="muted">${escapeHtml(release.media_type)} • ${escapeHtml(release.provider)} • ${escapeHtml(release.status)}${release.error ? ` • ${escapeHtml(release.error)}` : ""}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No release activity yet.</div>`
        }
      </section>
    </div>`;
  return renderAppPage(
    book.title,
    body,
    settings,
    flash.currentUser ?? null,
    `<a href="${escapeHtml(addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey))}">Raw JSON</a>`,
    apiKey
  );
}

function renderActivityPage(
  repo: BooksRepo,
  settings: AppSettings,
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Response {
  const inProgress = repo.listInProgressBooks();
  const recentBooks = repo.listAllBooks().filter((book) => book.status === "imported").slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 8);
  const apiKey = flash.apiKey ?? null;
  const body = `
    <section class="hero">
      <h1>Activity</h1>
      <p>What Podible is working on right now, what just landed, and anything that needs attention.</p>
      <div class="actions" style="margin-top: 14px;">
        <form method="post" action="${escapeHtml(addApiKey("/activity/refresh", apiKey))}">
          <button type="submit">Refresh library</button>
        </form>
      </div>
      ${messageMarkup(flash.notice, flash.error)}
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>Books in progress</h2>
        ${
          inProgress.length > 0
            ? `<div class="section-list">${inProgress
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No active work right now.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Recently ready</h2>
        ${
          recentBooks.length > 0
            ? `<div class="section-list">${recentBooks
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ready to play${book.durationMs ? ` • ${formatMinutes(book.durationMs)}` : ""}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No recently finished books yet.</div>`
        }
      </section>
      <section class="card span-12">
        <h2>Needs attention</h2>
        ${
          needsAttention.length > 0
            ? `<div class="section-list">${needsAttention
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">Nothing needs attention right now.</div>`
        }
      </section>
    </div>`;
  return renderAppPage("Activity", body, settings, flash.currentUser ?? null, "", apiKey);
}

async function createBookFromOpenLibrary(repo: BooksRepo, openLibraryKey: string): Promise<number> {
  const resolved = await resolveOpenLibraryCandidate({ openLibraryKey });
  if (!resolved) {
    throw new Error("Open Library match not found");
  }

  const book = repo.createBook({
    title: resolved.title,
    author: resolved.author,
  });

  repo.updateBookMetadata(book.id, {
    publishedAt: resolved.publishedAt ?? null,
    language: resolved.language ?? null,
    identifiers: resolved.identifiers,
  });

  const hydrated = repo.getBook(book.id);
  if (hydrated) {
    await hydrateBookFromOpenLibrary(repo, hydrated);
  }

  await triggerAutoAcquire(repo, book.id);
  return book.id;
}

function renderAdminPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  options: {
    apiKey?: string | null;
    plexServers?: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }>;
    notice?: string | null;
    error?: string | null;
    plexNotice?: string | null;
    plexError?: string | null;
  } = {}
): Response {
  const health = repo.getHealthSummary();
  const users = repo.listUsers();
  const apiKey = options.apiKey ?? null;
  const settingsJson = escapeHtml(JSON.stringify(settings, null, 2));
  const plexServers = options.plexServers ?? [];
  const activeJobs = (health.jobs.queued ?? 0) + (health.jobs.running ?? 0);
  const failedJobs = health.jobs.failed ?? 0;
  const releaseIssues = health.releases.failed ?? 0;

  const userRows =
    users.length > 0
      ? users
          .map(
            (user) => `<tr>
  <td>${user.id}</td>
  <td>${escapeHtml(user.provider)}</td>
  <td>${escapeHtml(user.username)}</td>
  <td>${escapeHtml(user.display_name || "")}</td>
  <td>${user.is_admin ? "yes" : "no"}</td>
</tr>`
          )
          .join("")
      : `<tr><td colspan="5">No users yet.</td></tr>`;

  const body = `<style>
      :root {
        --line-soft: #ebe5d8;
        --code-bg: #f3f1ea;
        --danger: #8b0000;
        --danger-border: #6f0000;
      }
      .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 14px;
        align-items: start;
      }
      .card-full { grid-column: span 12; }
      .card-mid { grid-column: span 6; }
      .page-header {
        background:
          radial-gradient(circle at 90% 10%, rgba(40, 89, 67, 0.08), transparent 45%),
          linear-gradient(180deg, #fffdf7, #f7f5ed);
      }
      .header-grid {
        display: grid;
        grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr);
        gap: 14px;
      }
      .header-grid > div { min-width: 0; }
      .panel {
        border: 0;
        padding: 0;
        margin: 0;
        background: transparent;
      }
      .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
      .settings-actions {
        display: flex;
        gap: 12px;
        justify-content: space-between;
        align-items: flex-start;
        flex-wrap: wrap;
      }
      .settings-actions-left {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        align-items: center;
      }
      input, button, textarea, select { font: inherit; }
      input, select {
        padding: 8px 10px;
        min-width: 220px;
        border: 1px solid var(--line);
        border-radius: 10px;
        background: #fff;
      }
      button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 36px;
        padding: 8px 12px;
        cursor: pointer;
        border: 1px solid var(--line);
        border-radius: 10px;
        background: #fff;
        color: var(--text);
      }
      button:hover { background: #faf8f1; }
      textarea {
        width: 100%;
        min-height: 220px;
        padding: 10px 12px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 13px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: #fff;
      }
      .table-wrap {
        overflow: auto;
        border: 1px solid var(--line-soft);
        border-radius: 12px;
        background: #fff;
      }
      table {
        border-collapse: collapse;
        width: 100%;
        margin-top: 0;
        min-width: 640px;
      }
      th, td { border: 1px solid var(--line-soft); padding: 6px 7px; text-align: left; font-size: 13px; vertical-align: top; }
      th { background: #f8f5ed; }
      code { background: var(--code-bg); padding: 2px 4px; border-radius: 4px; }
      pre {
        margin: 0;
        padding: 8px;
        border: 1px solid var(--line-soft);
        border-radius: 12px;
        background: #fff;
        overflow: auto;
      }
      pre code { background: transparent; padding: 0; }
      .card h2 + .panel, .card h2 + .table-wrap, .card h2 + p { margin-top: 0; }
      .feed-preview-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      .feed-preview-item {
        display: grid;
        grid-template-columns: 64px minmax(0, 1fr);
        gap: 10px;
        border: 1px solid var(--line-soft);
        border-radius: 12px;
        background: #fff;
        padding: 8px;
      }
      .feed-preview-cover {
        position: relative;
        width: 64px;
        height: 64px;
        border-radius: 8px;
        border: 1px solid var(--line-soft);
        background: linear-gradient(135deg, #eef5f0, #f7f5ed);
        color: var(--muted);
        font-weight: 700;
        overflow: hidden;
      }
      .feed-preview-fallback {
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .feed-preview-cover img {
        position: absolute;
        inset: 0;
        width: 100%;
        height: 100%;
        object-fit: cover;
        display: block;
      }
      .feed-preview-body { min-width: 0; }
      .feed-preview-title-row {
        display: flex;
        justify-content: space-between;
        gap: 8px;
        align-items: baseline;
      }
      .feed-preview-title-row strong {
        display: block;
        min-width: 0;
        line-height: 1.2;
      }
      .feed-preview-author {
        margin: 2px 0 4px;
        color: var(--muted);
        font-size: 12px;
      }
      .feed-preview-desc {
        margin: 0;
        color: var(--text);
        font-size: 12px;
        line-height: 1.35;
      }
      .feed-preview-links {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 6px;
        font-size: 12px;
      }
      .feed-preview-links a:hover { text-decoration: underline; }
      @media (max-width: 1200px) {
        .card-mid { grid-column: span 12; }
        .header-grid { grid-template-columns: 1fr; }
        .feed-preview-grid { grid-template-columns: 1fr; }
      }
      @media (max-width: 900px) {
        .dashboard-grid { gap: 10px; }
        input, select { min-width: 150px; }
        table { min-width: 560px; }
        .feed-preview-item { grid-template-columns: 56px minmax(0, 1fr); }
        .feed-preview-cover { width: 56px; height: 56px; }
      }
    </style>
      <section class="hero page-header">
          <div class="header-grid">
            <div>
              <h1>Admin</h1>
              <p class="muted">Manage settings, users, library refreshes, and recovery tools.</p>
              <div class="stats">
                <span class="pill">${activeJobs} active jobs</span>
                <span class="pill">${failedJobs} failed jobs</span>
                <span class="pill">${releaseIssues} release issues</span>
              </div>
            </div>
            <div>
              <h2>Settings JSON</h2>
              <div class="panel">
                <div class="settings-actions">
                  <div class="settings-actions-left">
                    <button id="settings-save-btn" type="button">Save Settings</button>
                    <form method="post" action="${escapeHtml(addApiKey("/admin/refresh", apiKey))}" style="margin: 0;">
                      <button type="submit">Refresh Library</button>
                    </form>
                  </div>
                  <button id="wipe-db-btn" type="button" style="background: var(--danger); color: #fff; border: 1px solid var(--danger-border);">Wipe Entire Database</button>
                </div>
                <p id="settings-status" class="muted"></p>
                ${messageMarkup(options.notice, options.error)}
                <textarea id="settings-editor" spellcheck="false">${settingsJson}</textarea>
              </div>
            </div>
          </div>
        </section>
      <div class="dashboard-grid">

        ${
          settings.auth.mode === "plex"
            ? `<section class="card card-full">
          <h2>Plex Access Control</h2>
          <p class="muted">Choose which Plex server controls who can sign in to Podible. Future Plex logins will only be allowed for users who can access that server.</p>
          <p class="muted">Owner token: <strong>${settings.auth.plex.ownerToken ? "captured" : "missing"}</strong> | Selected server: <strong>${escapeHtml(settings.auth.plex.machineName || settings.auth.plex.machineId || "not set")}</strong></p>
          ${messageMarkup(options.plexNotice, options.plexError)}
          ${
            plexServers.length > 0
              ? `<div class="feed-preview-grid">${plexServers
                  .map(
                    (server) => `<form method="post" action="${escapeHtml(addApiKey("/admin/plex/select", apiKey))}" class="feed-preview-item">
  <input type="hidden" name="machineId" value="${escapeHtml(server.machineId)}" />
  <input type="hidden" name="machineName" value="${escapeHtml(server.name)}" />
  <div class="feed-preview-body" style="grid-column: 1 / -1;">
    <div class="feed-preview-title-row">
      <strong>${escapeHtml(server.name)}</strong>
      <span class="muted">${server.machineId === settings.auth.plex.machineId ? "Selected" : "Available"}</span>
    </div>
    <p class="feed-preview-author">${escapeHtml(server.product || "Plex server")} • ${server.owned ? "owned" : "shared"}${server.sourceTitle ? ` • ${escapeHtml(server.sourceTitle)}` : ""}</p>
    <p class="feed-preview-desc"><code>${escapeHtml(server.machineId)}</code></p>
    <div class="feed-preview-links">
      <button type="submit">${server.machineId === settings.auth.plex.machineId ? "Selected server" : "Use this server"}</button>
    </div>
  </div>
</form>`
                  )
                  .join("")}</div>`
              : `<p class="muted">No Plex servers were found for the current owner token.</p>`
          }
        </section>`
            : ""
        }

        <section class="card card-mid">
          <h2>Users</h2>
          <div class="panel">
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Provider</th>
                    <th>Username</th>
                    <th>Display Name</th>
                    <th>Admin</th>
                  </tr>
                </thead>
                <tbody>${userRows}</tbody>
              </table>
            </div>
          </div>
        </section>

        <section class="card card-full">
          <h2>Manual Search + Snatch</h2>
          <div class="panel">
            <div class="row">
              <input id="manual-book-id" type="number" min="1" placeholder="Book ID" style="min-width: 110px;" />
              <select id="manual-media">
                <option value="audio">audio</option>
                <option value="ebook">ebook</option>
              </select>
              <input id="manual-query" type="text" placeholder="Search query (e.g. Twilight Stephenie Meyer)" />
              <button id="manual-search-btn" type="button">Search</button>
            </div>
            <p id="manual-search-status" class="muted"></p>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Title</th>
                    <th>Provider</th>
                    <th>Seeders</th>
                    <th>Size</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody id="manual-search-body"><tr><td colspan="5">No search yet.</td></tr></tbody>
              </table>
            </div>
          </div>
        </section>

        <section class="card card-mid">
          <h2>Manual Import</h2>
          <div class="panel">
            <div class="row">
              <input id="manual-import-book-id" type="number" min="1" placeholder="Book ID" style="min-width: 110px;" />
              <select id="manual-import-media">
                <option value="audio">audio</option>
                <option value="ebook">ebook</option>
              </select>
              <input id="manual-import-path" type="text" placeholder="Absolute path to file or folder" />
              <button id="manual-import-inspect-btn" type="button">Inspect</button>
              <button id="manual-import-btn" type="button">Import</button>
            </div>
            <p id="manual-import-status" class="muted"></p>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Use</th>
                    <th>Path</th>
                    <th>Type</th>
                    <th>Size</th>
                  </tr>
                </thead>
                <tbody id="manual-import-files-body"><tr><td colspan="4">No inspection yet.</td></tr></tbody>
              </table>
            </div>
          </div>
        </section>

        <section class="card card-mid">
          <h2>Recent Downloads</h2>
          <div class="panel">
            <div class="row">
              <button id="downloads-refresh-btn" type="button">Refresh Downloads</button>
            </div>
            <p id="downloads-status" class="muted"></p>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Job</th>
                    <th>Release</th>
                    <th>Media</th>
                    <th>Status</th>
                    <th>Progress</th>
                    <th>Transfer</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody id="downloads-table-body"><tr><td colspan="7">Loading...</td></tr></tbody>
              </table>
            </div>
          </div>
        </section>

        <section class="card card-mid">
          <h2>Recent Jobs</h2>
          <div class="panel">
            <div class="row">
              <label for="jobs-limit">Limit</label>
              <input id="jobs-limit" type="number" min="1" max="200" value="25" style="min-width: 90px;" />
              <label for="jobs-type">Type</label>
              <select id="jobs-type">
                <option value="">all</option>
                <option value="full_library_refresh">full library refresh</option>
                <option value="acquire">acquire</option>
                <option value="download">download</option>
                <option value="import">import</option>
                <option value="reconcile">reconcile</option>
                <option value="chapter_analysis">chapter analysis</option>
              </select>
              <button id="jobs-refresh-btn" type="button">Refresh Jobs</button>
            </div>
            <p id="jobs-status" class="muted"></p>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Book</th>
                    <th>Release</th>
                    <th>Attempts</th>
                    <th>Updated</th>
                    <th>Action</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody id="jobs-table-body"><tr><td colspan="9">Loading...</td></tr></tbody>
              </table>
            </div>
          </div>
        </section>
      </div>
    <script>
      (function () {
        function withAuth(path) {
          var url = new URL(path, window.location.origin);
          return url.pathname + url.search;
        }
        async function api(path, init) {
          return fetch(withAuth(path), init || {});
        }
        var rpcId = 1;
        async function rpc(method, params) {
          var res = await api("/rpc", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              jsonrpc: "2.0",
              id: rpcId++,
              method: method,
              params: params || {},
            }),
          });
          if (!res.ok) {
            throw new Error("HTTP " + res.status);
          }
          var payload = await res.json();
          if (!payload || payload.jsonrpc !== "2.0") {
            throw new Error("Invalid RPC response");
          }
          if (payload.error) {
            var details = payload.error.data && payload.error.data.message ? " " + payload.error.data.message : "";
            throw new Error(payload.error.message + details);
          }
          return payload.result;
        }
        function text(id, value) {
          var el = document.getElementById(id);
          if (el) el.textContent = value;
        }

        var manualBookInput = document.getElementById("manual-book-id");
        var manualMediaInput = document.getElementById("manual-media");
        var manualQueryInput = document.getElementById("manual-query");
        var manualSearchBtn = document.getElementById("manual-search-btn");
        var manualSearchBody = document.getElementById("manual-search-body");

        function manualSearchCell(row, value) {
          var td = document.createElement("td");
          td.textContent = value;
          row.appendChild(td);
          return td;
        }

        function renderManualSearchResults(items, bookId, media) {
          if (!manualSearchBody) return;
          manualSearchBody.innerHTML = "";
          if (!Array.isArray(items) || items.length === 0) {
            var emptyRow = document.createElement("tr");
            var emptyCell = document.createElement("td");
            emptyCell.colSpan = 5;
            emptyCell.textContent = "No results.";
            emptyRow.appendChild(emptyCell);
            manualSearchBody.appendChild(emptyRow);
            return;
          }
          items.forEach(function (item) {
            var row = document.createElement("tr");
            manualSearchCell(row, String(item.title || ""));
            manualSearchCell(row, String(item.provider || ""));
            manualSearchCell(row, item.seeders == null ? "" : String(item.seeders));
            manualSearchCell(row, item.sizeBytes == null ? "" : formatBytes(Number(item.sizeBytes)));

            var action = document.createElement("td");
            var btn = document.createElement("button");
            btn.type = "button";
            btn.textContent = "Snatch";
            btn.addEventListener("click", async function () {
              btn.disabled = true;
              text("manual-search-status", "Snatching...");
              try {
                var snatched = await rpc("snatch.create", {
                  bookId: bookId,
                  provider: item.provider,
                  title: item.title,
                  mediaType: media,
                  url: item.url,
                  infoHash: item.infoHash || null,
                  guid: item.guid || null,
                  sizeBytes: item.sizeBytes == null ? null : Number(item.sizeBytes),
                });
                text("manual-search-status", "Snatched release " + String(snatched.release.id) + ", download job " + String(snatched.jobId) + ".");
                if (typeof loadDownloads === "function") {
                  loadDownloads();
                }
                if (typeof loadJobs === "function") {
                  loadJobs();
                }
              } catch (err) {
                text("manual-search-status", "Snatch failed: " + (err && err.message ? err.message : "request error"));
              } finally {
                btn.disabled = false;
              }
            });
            action.appendChild(btn);
            row.appendChild(action);
            manualSearchBody.appendChild(row);
          });
        }

        async function runManualSearch() {
          var rawBookId = manualBookInput ? manualBookInput.value : "";
          var bookId = parseInt(rawBookId || "", 10);
          if (!Number.isFinite(bookId) || bookId <= 0) {
            text("manual-search-status", "Enter a valid Book ID.");
            if (manualSearchBody) {
              manualSearchBody.innerHTML = '<tr><td colspan="5">No results.</td></tr>';
            }
            return;
          }
          var media = manualMediaInput && manualMediaInput.value === "ebook" ? "ebook" : "audio";
          var query = (manualQueryInput && manualQueryInput.value ? manualQueryInput.value : "").trim();
          if (!query) {
            text("manual-search-status", "Enter a search query.");
            if (manualSearchBody) {
              manualSearchBody.innerHTML = '<tr><td colspan="5">No results.</td></tr>';
            }
            return;
          }
          text("manual-search-status", "Searching...");
          try {
            var payload = await rpc("search.run", { query: query, media: media });
            var items = payload && Array.isArray(payload.results) ? payload.results : [];
            renderManualSearchResults(items, bookId, media);
            text("manual-search-status", "Found " + items.length + " result(s).");
          } catch (err) {
            text("manual-search-status", "Search failed: " + (err && err.message ? err.message : "request error"));
            renderManualSearchResults([], bookId, media);
          }
        }

        if (manualSearchBtn) {
          manualSearchBtn.addEventListener("click", async function () {
            await runManualSearch();
          });
        }
        if (manualQueryInput) {
          manualQueryInput.addEventListener("keydown", async function (event) {
            if (event.key !== "Enter") return;
            event.preventDefault();
            await runManualSearch();
          });
        }

        var manualImportBookInput = document.getElementById("manual-import-book-id");
        var manualImportMediaInput = document.getElementById("manual-import-media");
        var manualImportPathInput = document.getElementById("manual-import-path");
        var manualImportInspectBtn = document.getElementById("manual-import-inspect-btn");
        var manualImportBtn = document.getElementById("manual-import-btn");
        var manualImportFilesBody = document.getElementById("manual-import-files-body");
        var manualImportFiles = [];
        var manualImportInspectedPath = "";

        function manualImportSupportsMedia(file, mediaType) {
          if (!file || typeof file !== "object") return false;
          if (mediaType === "ebook") return Boolean(file.supportedEbook);
          return Boolean(file.supportedAudio);
        }

        function renderManualImportFiles() {
          if (!manualImportFilesBody) return;
          var mediaType = manualImportMediaInput && manualImportMediaInput.value === "ebook" ? "ebook" : "audio";
          manualImportFilesBody.innerHTML = "";
          if (!Array.isArray(manualImportFiles) || manualImportFiles.length === 0) {
            manualImportFilesBody.innerHTML = '<tr><td colspan="4">No inspection yet.</td></tr>';
            return;
          }
          manualImportFiles.forEach(function (file) {
            var row = document.createElement("tr");
            var selectCell = document.createElement("td");
            var use = manualImportSupportsMedia(file, mediaType);
            var checkbox = document.createElement("input");
            checkbox.type = "checkbox";
            checkbox.setAttribute("data-source-path", String(file.sourcePath || ""));
            checkbox.checked = use;
            checkbox.disabled = !use;
            selectCell.appendChild(checkbox);

            var pathCell = document.createElement("td");
            pathCell.textContent = String(file.relativePath || file.sourcePath || "");

            var typeCell = document.createElement("td");
            if (Boolean(file.supportedAudio) && Boolean(file.supportedEbook)) {
              typeCell.textContent = "audio+ebook";
            } else if (Boolean(file.supportedAudio)) {
              typeCell.textContent = "audio";
            } else if (Boolean(file.supportedEbook)) {
              typeCell.textContent = "ebook";
            } else {
              typeCell.textContent = "other";
            }

            var sizeCell = document.createElement("td");
            sizeCell.textContent = formatBytes(Number(file.size));

            row.appendChild(selectCell);
            row.appendChild(pathCell);
            row.appendChild(typeCell);
            row.appendChild(sizeCell);
            manualImportFilesBody.appendChild(row);
          });
        }

        if (manualImportMediaInput) {
          manualImportMediaInput.addEventListener("change", function () {
            renderManualImportFiles();
          });
        }

        if (manualImportInspectBtn) {
          manualImportInspectBtn.addEventListener("click", async function () {
            var sourcePath = (manualImportPathInput && manualImportPathInput.value ? manualImportPathInput.value : "").trim();
            if (!sourcePath) {
              text("manual-import-status", "Enter a source path.");
              return;
            }
            manualImportInspectBtn.disabled = true;
            text("manual-import-status", "Inspecting...");
            try {
              var inspected = await rpc("import.inspect", { path: sourcePath });
              manualImportFiles = inspected && Array.isArray(inspected.files) ? inspected.files : [];
              manualImportInspectedPath = sourcePath;
              renderManualImportFiles();
              var mediaType = manualImportMediaInput && manualImportMediaInput.value === "ebook" ? "ebook" : "audio";
              var supportedCount = manualImportFiles.filter(function (file) {
                return manualImportSupportsMedia(file, mediaType);
              }).length;
              text(
                "manual-import-status",
                "Inspected " +
                  String(manualImportFiles.length) +
                  " file(s); " +
                  String(supportedCount) +
                  " match " +
                  mediaType +
                  "."
              );
            } catch (err) {
              manualImportFiles = [];
              manualImportInspectedPath = "";
              renderManualImportFiles();
              text("manual-import-status", "Inspect failed: " + (err && err.message ? err.message : "request error"));
            } finally {
              manualImportInspectBtn.disabled = false;
            }
          });
        }

        if (manualImportBtn) {
          manualImportBtn.addEventListener("click", async function () {
            var rawBookId = manualImportBookInput ? manualImportBookInput.value : "";
            var bookId = parseInt(rawBookId || "", 10);
            if (!Number.isFinite(bookId) || bookId <= 0) {
              text("manual-import-status", "Enter a valid Book ID.");
              return;
            }
            var mediaType = manualImportMediaInput && manualImportMediaInput.value === "ebook" ? "ebook" : "audio";
            var sourcePath = (manualImportPathInput && manualImportPathInput.value ? manualImportPathInput.value : "").trim();
            if (!sourcePath) {
              text("manual-import-status", "Enter a source path.");
              return;
            }
            if (manualImportFiles.length > 0 && manualImportInspectedPath !== sourcePath) {
              text("manual-import-status", "Path changed since inspect. Inspect again before importing.");
              return;
            }
            var selectedPaths = null;
            if (manualImportFiles.length > 0 && manualImportFilesBody) {
              selectedPaths = [];
              var selected = manualImportFilesBody.querySelectorAll("input[data-source-path]");
              selected.forEach(function (node) {
                if (!(node instanceof HTMLInputElement) || !node.checked || node.disabled) return;
                var value = node.getAttribute("data-source-path");
                if (value) {
                  selectedPaths.push(value);
                }
              });
              if (selectedPaths.length === 0) {
                text("manual-import-status", "Select at least one supported file to import.");
                return;
              }
            }
            manualImportBtn.disabled = true;
            text("manual-import-status", "Importing...");
            try {
              var params = {
                bookId: bookId,
                mediaType: mediaType,
                path: sourcePath,
              };
              if (selectedPaths) {
                params.selectedPaths = selectedPaths;
              }
              var imported = await rpc("import.manual", params);
              text(
                "manual-import-status",
                "Imported release " + String(imported.release.id) + " to asset " + String(imported.assetId) + "."
              );
              if (typeof loadDownloads === "function") {
                loadDownloads();
              }
              if (typeof loadJobs === "function") {
                loadJobs();
              }
            } catch (err) {
              text("manual-import-status", "Import failed: " + (err && err.message ? err.message : "request error"));
            } finally {
              manualImportBtn.disabled = false;
            }
          });
        }

        var settingsEditor = document.getElementById("settings-editor");
        var settingsSaveBtn = document.getElementById("settings-save-btn");
        var wipeDbBtn = document.getElementById("wipe-db-btn");
        var jobsTableBody = document.getElementById("jobs-table-body");
        var jobsRefreshBtn = document.getElementById("jobs-refresh-btn");
        var jobsLimitInput = document.getElementById("jobs-limit");
        var jobsTypeInput = document.getElementById("jobs-type");
        var downloadsTableBody = document.getElementById("downloads-table-body");
        var downloadsRefreshBtn = document.getElementById("downloads-refresh-btn");
        async function loadSettings() {
          try {
            text("settings-status", "Loading...");
            var payload = await rpc("settings.get", {});
            settingsEditor.value = JSON.stringify(payload, null, 2);
            text("settings-status", "Loaded.");
          } catch (err) {
            text("settings-status", "Load failed: " + (err && err.message ? err.message : "request error"));
          }
        }
        if (settingsEditor && settingsSaveBtn) {
          settingsSaveBtn.addEventListener("click", async function () {
            text("settings-status", "Saving...");
            var parsed;
            try {
              parsed = JSON.parse(settingsEditor.value || "{}");
            } catch (err) {
              text("settings-status", "Invalid JSON: " + (err && err.message ? err.message : "parse error"));
              return;
            }
            var payload;
            try {
              payload = await rpc("settings.update", parsed);
            } catch (err) {
              text("settings-status", "Save failed: " + (err && err.message ? err.message : "request error"));
              return;
            }
            settingsEditor.value = JSON.stringify(payload, null, 2);
            text("settings-status", "Saved.");
          });
          loadSettings();
        }
        if (wipeDbBtn) {
          wipeDbBtn.addEventListener("click", async function () {
            var confirmed = window.confirm(
              "Wipe the entire Kindling database? This deletes books, releases, assets, and jobs, but keeps settings."
            );
            if (!confirmed) {
              return;
            }
            wipeDbBtn.disabled = true;
            text("settings-status", "Wiping database...");
            try {
              var result = await rpc("admin.wipeDatabase", {});
              text(
                "settings-status",
                "Database wiped. Deleted books=" +
                  String(result.deleted && result.deleted.books || 0) +
                  ", releases=" +
                  String(result.deleted && result.deleted.releases || 0) +
                  ", assets=" +
                  String(result.deleted && result.deleted.assets || 0) +
                  ", files=" +
                  String(result.deleted && result.deleted.assetFiles || 0) +
                  ", jobs=" +
                  String(result.deleted && result.deleted.jobs || 0) +
                  ". Reloading..."
              );
              window.setTimeout(function () {
                window.location.reload();
              }, 300);
            } catch (err) {
              text("settings-status", "Wipe failed: " + (err && err.message ? err.message : "request error"));
              wipeDbBtn.disabled = false;
            }
          });
        }

        function formatBytes(value) {
          if (!Number.isFinite(value) || value < 0) return "";
          var units = ["B", "KB", "MB", "GB", "TB"];
          var n = value;
          var i = 0;
          while (n >= 1024 && i < units.length - 1) {
            n = n / 1024;
            i += 1;
          }
          var rounded = i === 0 ? String(Math.round(n)) : n.toFixed(1);
          return rounded + " " + units[i];
        }

        function downloadsCell(row, value) {
          var td = document.createElement("td");
          td.textContent = value;
          row.appendChild(td);
          return td;
        }

        function renderDownloads(items) {
          if (!downloadsTableBody) return;
          downloadsTableBody.innerHTML = "";
          if (!Array.isArray(items) || items.length === 0) {
            var emptyRow = document.createElement("tr");
            var emptyCell = document.createElement("td");
            emptyCell.colSpan = 7;
            emptyCell.textContent = "No downloads found.";
            emptyRow.appendChild(emptyCell);
            downloadsTableBody.appendChild(emptyRow);
            return;
          }

          items.forEach(function (download) {
            var row = document.createElement("tr");
            var jobCell = document.createElement("td");
            var link = document.createElement("a");
            link.href = withAuth("/rpc/downloads/get?jobId=" + String(download.job_id));
            link.textContent = String(download.job_id);
            jobCell.appendChild(link);
            row.appendChild(jobCell);

            var jobStatus = String(download.job_status || "");
            var releaseStatus = String(download.release_status || "");
            downloadsCell(row, download.release_id == null ? "" : String(download.release_id));
            downloadsCell(row, String(download.media_type || ""));
            downloadsCell(
              row,
              (jobStatus ? "download:" + jobStatus : "") + (releaseStatus ? (jobStatus ? " / " : "") + "release:" + releaseStatus : "")
            );

            var progressLabel = String(download.fullPseudoProgress ?? 0) + "%";
            if (jobStatus === "succeeded" && releaseStatus === "failed") {
              progressLabel = "done (import failed)";
            } else if (jobStatus === "failed" && !releaseStatus) {
              progressLabel = "download failed";
            }
            downloadsCell(row, progressLabel);

            var transfer = "";
            if (download.release_status === "downloading" && download.downloadProgress) {
              var progress = download.downloadProgress;
              var bytesDone = formatBytes(progress.bytesDone);
              var sizeBytes = formatBytes(progress.sizeBytes);
              var downRate = formatBytes(progress.downRate);
              var percent = Number.isFinite(progress.percent) ? String(progress.percent) + "%" : "";
              if (bytesDone || sizeBytes) {
                transfer = bytesDone + (sizeBytes ? " / " + sizeBytes : "");
              }
              if (downRate) {
                transfer += (transfer ? " @ " : "") + downRate + "/s";
              }
              if (percent) {
                transfer += transfer ? " (" + percent + ")" : percent;
              }
            }
            downloadsCell(row, transfer);
            downloadsCell(row, String(download.job_error || download.release_error || ""));
            downloadsTableBody.appendChild(row);
          });
        }

        async function loadDownloads() {
          text("downloads-status", "Loading...");
          try {
            var payload = await rpc("downloads.list", {});
            var items = payload && Array.isArray(payload.downloads) ? payload.downloads : [];
            renderDownloads(items);
            text("downloads-status", "Loaded " + items.length + " download(s).");
          } catch (err) {
            text("downloads-status", "Load failed: " + (err && err.message ? err.message : "request error"));
            renderDownloads([]);
          }
        }

        if (downloadsRefreshBtn) {
          downloadsRefreshBtn.addEventListener("click", function () {
            loadDownloads();
          });
        }
        loadDownloads();

        function jobsCell(row, value) {
          var td = document.createElement("td");
          td.textContent = value;
          row.appendChild(td);
          return td;
        }

        function renderJobs(items) {
          if (!jobsTableBody) return;
          jobsTableBody.innerHTML = "";
          if (!Array.isArray(items) || items.length === 0) {
            var emptyRow = document.createElement("tr");
            var emptyCell = document.createElement("td");
            emptyCell.colSpan = 9;
            emptyCell.textContent = "No jobs found.";
            emptyRow.appendChild(emptyCell);
            jobsTableBody.appendChild(emptyRow);
            return;
          }
          items.forEach(function (job) {
            var row = document.createElement("tr");
            var idCell = document.createElement("td");
            var link = document.createElement("a");
            link.href = withAuth("/rpc/jobs/get?jobId=" + String(job.id));
            link.textContent = String(job.id);
            idCell.appendChild(link);
            row.appendChild(idCell);
            jobsCell(row, String(job.type || ""));
            jobsCell(row, String(job.status || ""));
            var bookLabel = "";
            if (job.book_id != null) {
              bookLabel = job.book_title ? String(job.book_id) + " " + String(job.book_title) : String(job.book_id);
            }
            jobsCell(row, bookLabel);
            jobsCell(row, job.release_id == null ? "" : String(job.release_id));
            jobsCell(row, String(job.attempt_count || 0) + "/" + String(job.max_attempts || 0));
            jobsCell(row, String(job.updated_at || ""));
            var actionCell = document.createElement("td");
            var status = String(job.status || "");
            if (status === "failed" || status === "cancelled") {
              var retryBtn = document.createElement("button");
              retryBtn.type = "button";
              retryBtn.textContent = "Retry";
              retryBtn.addEventListener("click", async function () {
                retryBtn.disabled = true;
                text("jobs-status", "Retrying job " + String(job.id) + "...");
                try {
                  await rpc("jobs.retry", { jobId: Number(job.id) });
                  text("jobs-status", "Retried job " + String(job.id) + ".");
                  loadJobs();
                  if (typeof loadDownloads === "function") {
                    loadDownloads();
                  }
                } catch (err) {
                  text("jobs-status", "Retry failed: " + (err && err.message ? err.message : "request error"));
                  retryBtn.disabled = false;
                }
              });
              actionCell.appendChild(retryBtn);
            }
            row.appendChild(actionCell);
            jobsCell(row, String(job.error || ""));
            jobsTableBody.appendChild(row);
          });
        }

        async function loadJobs() {
          var limit = 25;
          if (jobsLimitInput) {
            var parsed = parseInt(jobsLimitInput.value || "25", 10);
            if (Number.isFinite(parsed) && parsed > 0) {
              limit = Math.min(parsed, 200);
            }
          }
          var params = { limit: limit };
          if (jobsTypeInput && jobsTypeInput.value) {
            params.type = jobsTypeInput.value;
          }
          text("jobs-status", "Loading...");
          try {
            var payload = await rpc("jobs.list", params);
            var items = payload && Array.isArray(payload.jobs) ? payload.jobs : [];
            renderJobs(items);
            text("jobs-status", "Loaded " + items.length + " job(s).");
          } catch (err) {
            text("jobs-status", "Load failed: " + (err && err.message ? err.message : "request error"));
            renderJobs([]);
          }
        }

        if (jobsRefreshBtn) {
          jobsRefreshBtn.addEventListener("click", function () {
            loadJobs();
          });
        }
        loadJobs();
      })();
    </script>`;

  return renderAppPage("Admin", body, settings, currentUser, "", apiKey);
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
        const machineName = (form.get("machineName") ?? "").trim();
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
              machineName,
            },
          },
        });
        response = redirect(`/admin?plex_notice=${encodeURIComponent(`Selected ${machineName || machineId}.`)}`);
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
