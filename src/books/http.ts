import { promises as fs } from "node:fs";

import { hydrateBookFromOpenLibrary } from "./hydration";
import { buildJsonFeed, buildRssFeed } from "./feed";
import { resolveOpenLibraryCandidate, searchOpenLibrary, type OpenLibraryCandidate } from "./openlibrary";
import {
  authorizeRequest,
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
  isApiKeyAuthorized,
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
  isPlexUserAllowed,
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
  if (!apiKey) return path;
  return path.includes("?") ? `${path}&api_key=${encodeURIComponent(apiKey)}` : `${path}?api_key=${encodeURIComponent(apiKey)}`;
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

function normalizeLocalUserKey(value: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "user";
}

function displayUserName(user: Pick<UserRow, "display_name" | "username">): string {
  return user.display_name?.trim() || user.username;
}

function renderAppPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  extraNav = ""
): Response {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  const accountNav = currentUser
    ? `<span class="muted">Signed in as ${escapeHtml(displayUserName(currentUser))}</span>
       <form method="post" action="${escapeHtml(addApiKey("/logout", apiKey))}" style="display:inline-flex;">
         <button type="submit">Sign out</button>
       </form>`
    : settings.auth.mode === "local" || settings.auth.mode === "plex"
      ? `<a href="${escapeHtml(addApiKey("/login", apiKey))}">Sign in</a>`
      : "";
  const nav = `
    <nav class="site-nav">
      <a href="${escapeHtml(addApiKey("/", apiKey))}">Home</a>
      <a href="${escapeHtml(addApiKey("/library", apiKey))}">Library</a>
      <a href="${escapeHtml(addApiKey("/add", apiKey))}">Add</a>
      <a href="${escapeHtml(addApiKey("/activity", apiKey))}">Activity</a>
      <a href="${escapeHtml(addApiKey("/admin", apiKey))}">Admin</a>
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

function renderLandingPage(repo: BooksRepo, settings: AppSettings, currentUser: SessionWithUserRow | null = null): Response {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  const recentBooks = repo.listAllBooks().slice(0, 6);
  const featured = preferredAudioForBooks(repo).slice(0, 6);
  const inProgress = repo.listInProgressBooks().slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 6);
  const body = `
    <section class="hero">
      <h1>Your audiobook shelf</h1>
      <p>Find something ready to play, check what Podible is still working on, and fix anything that needs attention without dropping into the admin console.</p>
      <div class="stats">
        <span class="pill">${featured.length} ready to play</span>
        <span class="pill">${inProgress.length} active imports</span>
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
  return renderAppPage("Podible", body, settings, currentUser);
}

function renderLibraryPage(
  repo: BooksRepo,
  settings: AppSettings,
  options: { query?: string | null; currentUser?: SessionWithUserRow | null } = {}
): Response {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
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
  return renderAppPage("Library", body, settings, options.currentUser ?? null);
}

function renderAddPage(
  settings: AppSettings,
  options: {
    query?: string;
    results?: OpenLibraryCandidate[];
    status?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
  } = {}
): Response {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  const query = options.query?.trim() ?? "";
  const results = options.results ?? [];
  const status = options.status?.trim() ?? "";
  const error = options.error?.trim() ?? "";
  const resultMarkup =
    query && results.length === 0 && !error
      ? `<div class="empty">No Open Library matches for “${escapeHtml(query)}”.</div>`
      : results.length > 0
        ? `<div class="book-list">${results
            .map((result) => {
              const publishYear = result.publishedAt ? new Date(result.publishedAt).getUTCFullYear() : null;
              return `<article class="book-row">
                ${coverMarkup(result.coverId ? `https://covers.openlibrary.org/b/id/${result.coverId}-L.jpg` : null, result.title)}
                <div class="meta">
                  <h3>${escapeHtml(result.title)}</h3>
                  <p class="muted">${escapeHtml(result.author)}${publishYear ? ` • ${publishYear}` : ""}</p>
                  <p class="muted">${escapeHtml(result.openLibraryKey)}</p>
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
        : `<div class="empty">Search Open Library by title and author to add a book.</div>`;

  const body = `
    <section class="hero">
      <h1>Add a book</h1>
      <p>Search Open Library, pick the correct work, and Podible will create the book and queue acquisition.</p>
    </section>
    <div class="grid">
      <section class="card span-12">
        <h2>Search Open Library</h2>
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
  return renderAppPage("Add", body, settings, options.currentUser ?? null);
}

function renderLoginPage(
  settings: AppSettings,
  localUsers: UserRow[],
  options: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null } = {}
): Response {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  if (settings.auth.mode === "plex") {
    const body = `
      <section class="hero">
        <h1>Sign in</h1>
        <p>Use your Plex account to sign in to this Podible instance. Podible will create its own local session after Plex confirms your identity.</p>
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
          const startUrl = ${JSON.stringify(addApiKey("/login/plex/start", apiKey))};
          const loadingUrl = ${JSON.stringify(addApiKey("/login/plex/loading", apiKey))};
          const successPath = ${JSON.stringify(addApiKey("/", apiKey))};
          function setStatus(message) {
            if (status) status.textContent = message || "";
          }
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
          button?.addEventListener("click", async () => {
            button.disabled = true;
            setStatus("Opening Plex sign-in…");
            const popup = window.open(loadingUrl, "podible-plex-login", "width=640,height=760");
            if (!popup) {
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
              popup.location.href = payload.authUrl;
              setStatus("Finish sign-in in the Plex window…");
            } catch (error) {
              popup.close();
              setStatus(error && error.message ? error.message : "Unable to start Plex sign-in.");
              button.disabled = false;
            }
          });
        })();
      </script>`;
    return renderAppPage("Sign in", body, settings, options.currentUser ?? null);
  }

  if (settings.auth.mode !== "local") {
    const body = `
      <section class="hero">
        <h1>Sign in</h1>
        <p>Browser sign-in is not enabled for this Podible instance.</p>
        ${messageMarkup(options.notice, options.error)}
      </section>`;
    return renderAppPage("Sign in", body, settings, options.currentUser ?? null);
  }

  const existingUsersMarkup =
    localUsers.length > 0
      ? `<div class="section-list">${localUsers
          .map(
            (user) => `<form method="post" action="${escapeHtml(addApiKey("/login", apiKey))}" class="book-row" style="grid-template-columns: minmax(0, 1fr);">
                <input type="hidden" name="userId" value="${user.id}" />
                <div class="meta">
                  <h3>${escapeHtml(displayUserName(user))}</h3>
                  <p class="muted">@${escapeHtml(user.username)}${user.is_admin ? " • admin" : ""}</p>
                  <div class="actions"><button type="submit">Sign in</button></div>
                </div>
              </form>`
          )
          .join("")}</div>`
      : `<div class="empty">No local users yet. Create the first one below.</div>`;

  const body = `
    <section class="hero">
      <h1>Sign in</h1>
      <p>Choose an existing user or create a new local user for this Podible instance.</p>
      ${messageMarkup(options.notice, options.error)}
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>Existing users</h2>
        ${existingUsersMarkup}
      </section>
      <section class="card span-6">
        <h2>Create a user</h2>
        <form method="post" action="${escapeHtml(addApiKey("/login", apiKey))}">
          <div class="section-list">
            <label>
              <div class="muted">Username</div>
              <input type="text" name="username" placeholder="alice" style="min-width: 100%; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
            </label>
            <label>
              <div class="muted">Display name</div>
              <input type="text" name="displayName" placeholder="Alice" style="min-width: 100%; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
            </label>
          </div>
          <div class="actions" style="margin-top: 12px;">
            <button type="submit">Create and sign in</button>
          </div>
        </form>
      </section>
    </div>`;
  return renderAppPage("Sign in", body, settings, options.currentUser ?? null);
}

function renderPlexLoadingPage(settings: AppSettings): Response {
  const body = `
    <section class="hero">
      <h1>Plex sign-in</h1>
      <p>Finish the Plex sign-in flow in this window. Podible will continue automatically when Plex redirects back.</p>
    </section>`;
  return renderAppPage("Plex sign in", body, settings);
}

function renderPlexCompletePage(
  settings: AppSettings,
  pinId: number,
  request: Request
): Response {
  const statusUrl = new URL(addApiKey(`/login/plex/status?pinId=${pinId}`, settings.auth.mode === "apikey" ? settings.auth.key : null), request.url);
  const loadingMessage = "Waiting for Plex to finish sign-in…";
  return new Response(
    `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Plex sign-in complete</title>
  </head>
  <body style="font-family: ui-sans-serif, system-ui, sans-serif; padding: 24px;">
    <p id="plex-complete-status">${escapeHtml(loadingMessage)}</p>
    <script>
      (function () {
        var statusUrl = ${JSON.stringify(statusUrl.toString())};
        var statusEl = document.getElementById("plex-complete-status");
        var tries = 0;
        var maxTries = 30;
        function setStatus(message) {
          if (statusEl) statusEl.textContent = message || "";
        }
        function finish(payload) {
          if (window.opener && !window.opener.closed) {
            window.opener.postMessage(payload, window.location.origin);
            if (payload.ok) {
              window.close();
            }
            return;
          }
          if (payload.ok && payload.redirectTo) {
            window.location.href = payload.redirectTo;
          }
        }
        async function poll() {
          tries += 1;
          if (tries > maxTries) {
            finish({
              type: "podible-plex-login",
              ok: false,
              redirectTo: "/login",
              error: "Timed out waiting for Plex sign-in."
            });
            setStatus("Timed out waiting for Plex sign-in.");
            return;
          }
          try {
            var response = await fetch(statusUrl, {
              method: "GET",
              headers: { "Accept": "application/json" },
              credentials: "same-origin"
            });
            var payload = await response.json();
            if (payload.pending) {
              setStatus(payload.message || ${JSON.stringify(loadingMessage)});
              window.setTimeout(poll, payload.retryAfterMs || 3000);
              return;
            }
            finish({
              type: "podible-plex-login",
              ok: !!payload.ok,
              redirectTo: payload.redirectTo || "/",
              error: payload.error || null
            });
            if (!payload.ok) {
              setStatus(payload.error || "Plex sign-in failed.");
            }
          } catch (error) {
            if (tries < 5) {
              setStatus("Still waiting for Plex…");
              window.setTimeout(poll, 3000);
              return;
            }
            finish({
              type: "podible-plex-login",
              ok: false,
              redirectTo: "/login",
              error: "Unable to complete Plex sign-in."
            });
            setStatus("Unable to complete Plex sign-in.");
          }
        }
        poll();
      })();
    </script>
  </body>
</html>`,
    {
      status: 200,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    }
  );
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
  pinId: number
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

    if (!allowed || !isPlexUserAllowed(settings, plexUser)) {
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
      redirectTo: addApiKey("/", settings.auth.mode === "apikey" ? settings.auth.key : null),
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
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null } = {}
): Promise<Response> {
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
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
          <div><strong>Audio:</strong> ${audio ? "Ready to play" : "Not attached yet"}</div>
          <div><strong>eBook:</strong> ${ebook ? "Ready to export" : "Not attached yet"}</div>
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
        <h2>Files and exports</h2>
        <div class="section-list">
          <div><strong>Audio:</strong> ${audio ? `ready (#${audio.id}, ${escapeHtml(audio.kind)})` : "not attached"}</div>
          <div><strong>eBook:</strong> ${ebook ? `ready (#${ebook.id})` : "not attached"}</div>
          <div><strong>Transcript:</strong> ${transcriptUrl ? "available" : "not available yet"}</div>
          <div><strong>Chapters:</strong> ${chaptersUrl ? "available" : "not available yet"}</div>
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
    `<a href="${escapeHtml(addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey))}">Raw JSON</a>`
  );
}

function renderActivityPage(
  repo: BooksRepo,
  settings: AppSettings,
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null } = {}
): Response {
  const inProgress = repo.listInProgressBooks();
  const recentBooks = repo.listAllBooks().filter((book) => book.status === "imported").slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 8);
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
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
  return renderAppPage("Activity", body, settings, flash.currentUser ?? null);
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
    plexServers?: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }>;
    plexNotice?: string | null;
    plexError?: string | null;
  } = {}
): Response {
  const health = repo.getHealthSummary();
  const books = repo.listBooks(30).items;
  const previewBooks = preferredAudioForBooks(repo).slice(0, 12);
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  const settingsJson = escapeHtml(JSON.stringify(settings, null, 2));
  const plexServers = options.plexServers ?? [];

  const rows = books
    .map((book) => {
      const detailPath = addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey);
      return `<tr>
  <td><a href="${escapeHtml(detailPath)}">${book.id}</a></td>
  <td>${escapeHtml(book.title)}</td>
  <td>${escapeHtml(book.author)}</td>
  <td>${escapeHtml(book.status)}</td>
  <td>${escapeHtml(book.audioStatus)}</td>
  <td>${escapeHtml(book.ebookStatus)}</td>
  <td class="book-progress" data-book-id="${book.id}" data-audio-status="${escapeHtml(book.audioStatus)}" data-ebook-status="${escapeHtml(book.ebookStatus)}">${escapeHtml(String(book.fullPseudoProgress))}%</td>
  <td>
    <button type="button" class="view-files-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Files</button>
    <button type="button" class="report-import-issue-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}" data-audio-status="${escapeHtml(book.audioStatus)}" data-ebook-status="${escapeHtml(book.ebookStatus)}">Wrong File</button>
    <button type="button" class="agent-acquire-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Agent Acquire</button>
    <button type="button" class="delete-book-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Delete</button>
  </td>
</tr>`;
    })
    .join("");

  const previewCards = previewBooks
    .map(({ book, asset }) => {
      const streamPath = addApiKey(`/stream/${asset.id}.${streamExtension(asset)}`, apiKey);
      const chaptersPath = addApiKey(`/chapters/${asset.id}.json`, apiKey);
      const detailPath = addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey);
      const coverPath = book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null;
      const description = (book.description || "").trim() || `${book.title} by ${book.author}`;
      const preview = truncateText(description.replace(/\s+/g, " "), 220);
      const initials =
        (book.title || "")
          .split(/\s+/)
          .map((part) => part[0] || "")
          .join("")
          .slice(0, 2)
          .toUpperCase() || "BK";
      return `<article class="feed-preview-item">
  <div class="feed-preview-cover">
    <span class="feed-preview-fallback">${escapeHtml(initials)}</span>
    ${
      coverPath
        ? `<img src="${escapeHtml(coverPath)}" alt="${escapeHtml(book.title)} cover" loading="lazy" onerror="this.remove()" />`
        : ""
    }
  </div>
  <div class="feed-preview-body">
    <div class="feed-preview-title-row">
      <strong>${escapeHtml(book.title)}</strong>
      <span class="muted">#${book.id}</span>
    </div>
    <p class="feed-preview-author">${escapeHtml(book.author)}</p>
    <p class="feed-preview-desc">${escapeHtml(preview)}</p>
    <div class="feed-preview-links">
      <a href="${escapeHtml(streamPath)}">Stream</a>
      <a href="${escapeHtml(chaptersPath)}">Chapters</a>
      <a href="${escapeHtml(detailPath)}">JSON</a>
    </div>
  </div>
</article>`;
    })
    .join("");

  const body = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Podible</title>
    <style>
      :root {
        --bg: #f5f7fb;
        --card: #ffffff;
        --line: #d7deea;
        --line-soft: #e7edf7;
        --text: #1b2430;
        --muted: #5d6a7c;
        --code-bg: #eef3fb;
        --danger: #8b0000;
        --danger-border: #6f0000;
      }
      * { box-sizing: border-box; }
      body {
        font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
        margin: 0;
        padding: 14px;
        background: radial-gradient(circle at top, #fbfdff 0%, var(--bg) 55%);
        color: var(--text);
      }
      h1, h2 { margin: 0 0 8px; }
      h2 { font-size: 18px; }
      p { margin: 0 0 8px; }
      .muted { color: var(--muted); }
      ul { margin-top: 6px; margin-bottom: 0; padding-left: 18px; }
      .page { max-width: 1600px; margin: 0 auto; }
      .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 12px;
        align-items: start;
      }
      .card {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
        box-shadow: 0 1px 2px rgba(22, 34, 51, 0.04);
        min-width: 0;
      }
      .card-full { grid-column: 1 / -1; }
      .card-wide { grid-column: span 8; }
      .card-mid { grid-column: span 6; }
      .card-narrow { grid-column: span 4; }
      .page-header {
        background:
          radial-gradient(circle at 90% 10%, rgba(95, 143, 255, 0.08), transparent 45%),
          linear-gradient(180deg, #ffffff, #fbfdff);
      }
      .header-grid {
        display: grid;
        grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr);
        gap: 10px;
      }
      .header-grid > div { min-width: 0; }
      .panel {
        border: 0;
        border-radius: 0;
        padding: 0;
        margin: 0;
        background: transparent;
      }
      .row { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
      input, button, textarea, select { font: inherit; }
      input, select {
        padding: 6px 8px;
        min-width: 220px;
        border: 1px solid var(--line);
        border-radius: 7px;
        background: #fff;
      }
      button {
        padding: 6px 10px;
        cursor: pointer;
        border: 1px solid var(--line);
        border-radius: 7px;
        background: #f8fbff;
      }
      button:hover { background: #f1f6ff; }
      textarea {
        width: 100%;
        min-height: 220px;
        padding: 8px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 13px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: #fbfdff;
      }
      .table-wrap {
        overflow: auto;
        border: 1px solid var(--line-soft);
        border-radius: 8px;
        background: #fff;
      }
      table {
        border-collapse: collapse;
        width: 100%;
        margin-top: 0;
        min-width: 640px;
      }
      th, td { border: 1px solid var(--line-soft); padding: 6px 7px; text-align: left; font-size: 13px; vertical-align: top; }
      th { background: #f7faff; }
      code { background: var(--code-bg); padding: 2px 4px; border-radius: 4px; }
      pre {
        margin: 0;
        padding: 8px;
        border: 1px solid var(--line-soft);
        border-radius: 8px;
        background: #fbfdff;
        overflow: auto;
      }
      pre code { background: transparent; padding: 0; }
      .card h2 + .panel, .card h2 + .table-wrap, .card h2 + p { margin-top: 0; }
      #library-files-panel { display: none; }
      .path-cell { word-break: break-all; }
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
        border-radius: 10px;
        background: #fff;
        padding: 8px;
      }
      .feed-preview-cover {
        position: relative;
        width: 64px;
        height: 64px;
        border-radius: 8px;
        border: 1px solid var(--line-soft);
        background: linear-gradient(135deg, #e6efff, #f3f7ff);
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
      .feed-preview-links a {
        color: #1d4ed8;
        text-decoration: none;
      }
      .feed-preview-links a:hover { text-decoration: underline; }
      @media (max-width: 1200px) {
        .card-wide, .card-mid { grid-column: span 12; }
        .card-narrow { grid-column: span 6; }
        .header-grid { grid-template-columns: 1fr; }
        .feed-preview-grid { grid-template-columns: 1fr; }
      }
      @media (max-width: 900px) {
        body { padding: 10px; }
        .dashboard-grid { gap: 10px; }
        .card-narrow, .card-mid, .card-wide { grid-column: span 12; }
        input, select { min-width: 150px; }
        table { min-width: 560px; }
        .feed-preview-item { grid-template-columns: 56px minmax(0, 1fr); }
        .feed-preview-cover { width: 56px; height: 56px; }
      }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="dashboard-grid">
        <section class="card card-full page-header">
          <div class="header-grid">
            <div>
              <h1>Podible Backend</h1>
              <p class="muted">Auth mode: <strong>${escapeHtml(settings.auth.mode)}</strong>${apiKey ? ` | Authorized links include <code>api_key</code>` : ""}</p>
              <p>Queue: <strong>${health.queueSize}</strong> | Jobs: <code>${escapeHtml(JSON.stringify(health.jobs))}</code> | Releases: <code>${escapeHtml(JSON.stringify(health.releases))}</code></p>
            </div>
            <div>
              <h2>Settings JSON</h2>
              <div class="panel">
                <div class="row">
                  <button id="settings-save-btn" type="button">Save Settings</button>
                  <button id="wipe-db-btn" type="button" style="margin-left: auto; background: var(--danger); color: #fff; border: 1px solid var(--danger-border);">Wipe Entire Database</button>
                </div>
                <p id="settings-status" class="muted"></p>
                <textarea id="settings-editor" spellcheck="false">${settingsJson}</textarea>
              </div>
            </div>
          </div>
        </section>

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

        ${
          previewCards
            ? `<section class="card card-full">
          <h2>Feed Preview</h2>
          <p class="muted">First 12 audio books with preferred stream assets. Stream links use each asset's native extension (no forced mp3 transcode).</p>
          <div class="feed-preview-grid">
            ${previewCards}
          </div>
        </section>`
            : ""
        }

        <section class="card card-mid">
          <h2>Open Library Search</h2>
          <div class="panel">
            <div class="row">
              <input id="ol-query" type="text" placeholder="Title Author (e.g. Hyperion Dan Simmons)" />
              <button id="ol-search-btn" type="button">Search</button>
            </div>
            <p id="ol-status" class="muted"></p>
            <ul id="ol-results"></ul>
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

        <section class="card card-full">
          <h2>Recent Library</h2>
          <p id="library-status" class="muted"></p>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Title</th>
                  <th>Author</th>
                  <th>Status</th>
                  <th>Audio</th>
                  <th>Ebook</th>
                  <th>Progress</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="library-table-body">${rows || '<tr><td colspan="8">No books yet.</td></tr>'}</tbody>
            </table>
          </div>
        </section>

        <section id="library-files-panel" class="card card-full">
          <div class="row">
            <strong id="library-files-title">Imported Files</strong>
            <button id="library-files-close-btn" type="button">Close</button>
          </div>
          <p id="library-files-status" class="muted"></p>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Asset</th>
                  <th>Kind</th>
                  <th>Path</th>
                  <th>Size</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="library-files-body"><tr><td colspan="5">Select a book to view imported files.</td></tr></tbody>
            </table>
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
    </div>
    <script>
      (function () {
        var queryApiKey = new URLSearchParams(window.location.search).get("api_key");
        function withAuth(path) {
          var url = new URL(path, window.location.origin);
          if (queryApiKey && !url.searchParams.get("api_key")) {
            url.searchParams.set("api_key", queryApiKey);
          }
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

        var queryInput = document.getElementById("ol-query");
        var searchBtn = document.getElementById("ol-search-btn");
        var resultList = document.getElementById("ol-results");
        async function runOpenLibrarySearch() {
          var q = (queryInput.value || "").trim();
          if (!q) {
            text("ol-status", "Enter a search query.");
            resultList.innerHTML = "";
            return;
          }
          text("ol-status", "Searching...");
          var payload;
          try {
            payload = await rpc("openlibrary.search", { q: q, limit: 10 });
          } catch (err) {
            text("ol-status", "Search failed: " + (err && err.message ? err.message : "request error"));
            resultList.innerHTML = "";
            return;
          }
          var items = Array.isArray(payload.results) ? payload.results : [];
          resultList.innerHTML = "";
          text("ol-status", "Found " + items.length + " result(s).");
          items.forEach(function (item) {
            var li = document.createElement("li");
            var label = document.createElement("span");
            label.textContent = item.title + " — " + item.author + " (" + item.openLibraryKey + ")";
            var btn = document.createElement("button");
            btn.type = "button";
            btn.style.marginLeft = "8px";
            btn.textContent = "Add";
            btn.addEventListener("click", async function () {
              text("ol-status", "Adding " + item.title + "...");
              var created;
              try {
                created = await rpc("library.create", { openLibraryKey: item.openLibraryKey });
              } catch (err) {
                text("ol-status", "Add failed: " + (err && err.message ? err.message : "request error"));
                return;
              }
              text("ol-status", 'Added "' + created.book.title + '" (id ' + created.book.id + '). Refresh to see it below.');
            });
            li.appendChild(label);
            li.appendChild(btn);
            resultList.appendChild(li);
          });
        }
        if (searchBtn && queryInput && resultList) {
          searchBtn.addEventListener("click", async function () {
            await runOpenLibrarySearch();
          });
          queryInput.addEventListener("keydown", async function (event) {
            if (event.key !== "Enter") return;
            event.preventDefault();
            await runOpenLibrarySearch();
          });
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

        function mediaStatusPseudo(status) {
          if (status === "imported") return 100;
          if (status === "downloaded") return 90;
          if (status === "downloading") return 20;
          if (status === "snatched") return 10;
          return 0;
        }

        function updateLibraryProgressFromDownloads(items) {
          var cells = Array.prototype.slice.call(document.querySelectorAll("td.book-progress"));
          if (cells.length === 0) return;

          var mediaProgressByBook = {};
          if (Array.isArray(items)) {
            items.forEach(function (download) {
              var bookId = Number(download.book_id);
              var media = String(download.media_type || "");
              var progress = Number(download.fullPseudoProgress);
              if (!Number.isFinite(bookId) || bookId <= 0) return;
              if (media !== "audio" && media !== "ebook") return;
              if (!Number.isFinite(progress)) return;
              var key = String(bookId);
              if (!mediaProgressByBook[key]) {
                mediaProgressByBook[key] = {};
              }
              var current = mediaProgressByBook[key][media];
              if (!Number.isFinite(current) || progress > current) {
                mediaProgressByBook[key][media] = progress;
              }
            });
          }

          cells.forEach(function (cell) {
            var bookId = Number(cell.getAttribute("data-book-id") || "");
            if (!Number.isFinite(bookId) || bookId <= 0) return;
            var key = String(bookId);
            var audioStatus = String(cell.getAttribute("data-audio-status") || "wanted");
            var ebookStatus = String(cell.getAttribute("data-ebook-status") || "wanted");
            var audioProgress = mediaStatusPseudo(audioStatus);
            var ebookProgress = mediaStatusPseudo(ebookStatus);
            var bookMedia = mediaProgressByBook[key] || {};
            if (Number.isFinite(bookMedia.audio)) {
              audioProgress = Math.max(audioProgress, Number(bookMedia.audio));
            }
            if (Number.isFinite(bookMedia.ebook)) {
              ebookProgress = Math.max(ebookProgress, Number(bookMedia.ebook));
            }
            var combined = Math.round((audioProgress + ebookProgress) / 2);
            cell.textContent = String(combined) + "%";
          });
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
            updateLibraryProgressFromDownloads(items);
            text("downloads-status", "Loaded " + items.length + " download(s).");
          } catch (err) {
            text("downloads-status", "Load failed: " + (err && err.message ? err.message : "request error"));
            renderDownloads([]);
            updateLibraryProgressFromDownloads([]);
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

        var libraryTableBody = document.getElementById("library-table-body");
        var libraryFilesPanel = document.getElementById("library-files-panel");
        var libraryFilesTitle = document.getElementById("library-files-title");
        var libraryFilesStatus = document.getElementById("library-files-status");
        var libraryFilesBody = document.getElementById("library-files-body");
        var libraryFilesCloseBtn = document.getElementById("library-files-close-btn");
        function ensureLibraryEmptyRow() {
          if (!libraryTableBody) return;
          if (libraryTableBody.children.length > 0) return;
          var row = document.createElement("tr");
          var cell = document.createElement("td");
          cell.colSpan = 8;
          cell.textContent = "No books yet.";
          row.appendChild(cell);
          libraryTableBody.appendChild(row);
        }

        function renderLibraryFiles(assets) {
          if (!libraryFilesBody) return;
          libraryFilesBody.innerHTML = "";
          if (!Array.isArray(assets) || assets.length === 0) {
            libraryFilesBody.innerHTML = '<tr><td colspan="5">No imported assets for this book.</td></tr>';
            return;
          }
          var rowCount = 0;
          assets.forEach(function (asset) {
            var files = Array.isArray(asset.files) ? asset.files : [];
            files.forEach(function (file) {
              rowCount += 1;
              var row = document.createElement("tr");
              var assetCell = document.createElement("td");
              assetCell.textContent = String(asset.id || "");
              row.appendChild(assetCell);

              var kindCell = document.createElement("td");
              kindCell.textContent = String(asset.kind || "");
              row.appendChild(kindCell);

              var pathCell = document.createElement("td");
              pathCell.className = "path-cell";
              pathCell.textContent = String(file.path || "");
              row.appendChild(pathCell);

              var sizeCell = document.createElement("td");
              sizeCell.textContent = formatBytes(Number(file.size));
              row.appendChild(sizeCell);

              var actionCell = document.createElement("td");
              var link = document.createElement("a");
              if (asset.kind === "ebook") {
                link.href = withAuth("/ebook/" + String(asset.id));
                link.textContent = "Download";
              } else {
                var ext = String(asset.stream_ext || "mp3");
                link.href = withAuth("/stream/" + String(asset.id) + "." + ext);
                link.textContent = "Stream";
              }
              actionCell.appendChild(link);
              row.appendChild(actionCell);

              libraryFilesBody.appendChild(row);
            });
          });
          if (rowCount === 0) {
            libraryFilesBody.innerHTML = '<tr><td colspan="5">No imported files for this book.</td></tr>';
          }
        }

        async function loadLibraryFiles(bookId, bookTitle) {
          if (!libraryFilesPanel) return;
          libraryFilesPanel.style.display = "block";
          if (libraryFilesTitle) {
            libraryFilesTitle.textContent = 'Imported Files - "' + bookTitle + '"';
          }
          text("library-files-status", "Loading...");
          try {
            var response = await api("/assets?bookId=" + encodeURIComponent(String(bookId)));
            if (!response.ok) {
              throw new Error("HTTP " + response.status);
            }
            var payload = await response.json();
            var assets = payload && Array.isArray(payload.assets) ? payload.assets : [];
            renderLibraryFiles(assets);
            text("library-files-status", "Loaded " + assets.length + " asset(s).");
          } catch (err) {
            renderLibraryFiles([]);
            text("library-files-status", "Load failed: " + (err && err.message ? err.message : "request error"));
          }
        }

        if (libraryFilesCloseBtn && libraryFilesPanel) {
          libraryFilesCloseBtn.addEventListener("click", function () {
            libraryFilesPanel.style.display = "none";
          });
        }

        var viewFilesButtons = Array.prototype.slice.call(document.querySelectorAll(".view-files-btn"));
        viewFilesButtons.forEach(function (button) {
          button.addEventListener("click", async function () {
            var rawId = button.getAttribute("data-book-id") || "";
            var bookId = parseInt(rawId, 10);
            if (!Number.isFinite(bookId) || bookId <= 0) {
              text("library-status", "View files failed: invalid book id.");
              return;
            }
            var bookTitle = button.getAttribute("data-book-title") || ("Book " + String(bookId));
            await loadLibraryFiles(bookId, bookTitle);
          });
        });

        var reportImportIssueButtons = Array.prototype.slice.call(document.querySelectorAll(".report-import-issue-btn"));
        function pickReleaseForImportIssue(details, mediaType) {
          var releases = details && Array.isArray(details.releases) ? details.releases : [];
          var assets = details && Array.isArray(details.assets) ? details.assets : [];
          var mediaReleases = releases.filter(function (release) {
            return String(release.media_type || "") === mediaType;
          });
          if (mediaReleases.length === 0) return null;

          var mediaAsset = assets.find(function (asset) {
            var kind = String(asset.kind || "");
            if (mediaType === "ebook") return kind === "ebook";
            return kind !== "ebook";
          });
          if (mediaAsset && Number.isFinite(Number(mediaAsset.source_release_id))) {
            var sourceReleaseId = Number(mediaAsset.source_release_id);
            var fromAsset = mediaReleases.find(function (release) {
              return Number(release.id) === sourceReleaseId;
            });
            if (fromAsset) return Number(fromAsset.id);
          }

          var imported = mediaReleases.find(function (release) {
            return String(release.status || "") === "imported";
          });
          if (imported) return Number(imported.id);
          return Number(mediaReleases[0].id);
        }
        reportImportIssueButtons.forEach(function (button) {
          button.addEventListener("click", async function () {
            var rawId = button.getAttribute("data-book-id") || "";
            var bookId = parseInt(rawId, 10);
            if (!Number.isFinite(bookId) || bookId <= 0) {
              text("library-status", "Wrong file report failed: invalid book id.");
              return;
            }
            var bookTitle = button.getAttribute("data-book-title") || ("Book " + String(bookId));
            var audioStatus = String(button.getAttribute("data-audio-status") || "wanted");
            var ebookStatus = String(button.getAttribute("data-ebook-status") || "wanted");
            var suggested = audioStatus === "imported" && ebookStatus !== "imported" ? "audio" : ebookStatus === "imported" && audioStatus !== "imported" ? "ebook" : "audio";
            var response = window.prompt('Which media is wrong? Enter "audio" or "ebook".', suggested);
            if (!response) return;
            var mediaType = response.trim().toLowerCase();
            if (mediaType !== "audio" && mediaType !== "ebook") {
              text("library-status", "Wrong file report failed: media must be audio or ebook.");
              return;
            }
            button.disabled = true;
            text("library-status", 'Reporting wrong ' + mediaType + ' file for "' + bookTitle + '"...');
            try {
              var details = await rpc("library.get", { bookId: bookId });
              var releaseId = pickReleaseForImportIssue(details, mediaType);
              if (!Number.isFinite(releaseId) || releaseId <= 0) {
                throw new Error("No matching release found for selected media");
              }
              var result = await rpc("library.reportImportIssue", {
                bookId: bookId,
                mediaType: mediaType,
                releaseId: releaseId,
              });
              if (result && result.action === "agent_imported") {
                text("library-status", 'Agent imported replacement ' + mediaType + ' file for "' + bookTitle + '".');
              } else if (result && result.action === "wrong_file_review_queued") {
                text("library-status", 'Queued wrong-file review for "' + bookTitle + '" (job ' + String(result.jobId) + ").");
              } else {
                text("library-status", 'Queued agent reacquire for "' + bookTitle + '" (job ' + String(result.jobId) + ").");
              }
              if (typeof loadJobs === "function") {
                loadJobs();
              }
              if (typeof loadDownloads === "function") {
                loadDownloads();
              }
            } catch (err) {
              text("library-status", "Wrong file report failed: " + (err && err.message ? err.message : "request error"));
            } finally {
              button.disabled = false;
            }
          });
        });

        var agentAcquireButtons = Array.prototype.slice.call(document.querySelectorAll(".agent-acquire-btn"));
        agentAcquireButtons.forEach(function (button) {
          button.addEventListener("click", async function () {
            var rawId = button.getAttribute("data-book-id") || "";
            var bookId = parseInt(rawId, 10);
            if (!Number.isFinite(bookId) || bookId <= 0) {
              text("library-status", "Agent acquire failed: invalid book id.");
              return;
            }
            var bookTitle = button.getAttribute("data-book-title") || ("Book " + String(bookId));
            button.disabled = true;
            text("library-status", 'Queueing agent acquire for "' + bookTitle + '"...');
            try {
              var queued = await rpc("library.acquire", {
                bookId: bookId,
                media: ["audio", "ebook"],
                forceAgent: true,
                priorFailure: true,
              });
              text("library-status", 'Queued agent acquire for "' + bookTitle + '" (job ' + String(queued.jobId) + ").");
              if (typeof loadJobs === "function") {
                loadJobs();
              }
            } catch (err) {
              text("library-status", "Agent acquire failed: " + (err && err.message ? err.message : "request error"));
            } finally {
              button.disabled = false;
            }
          });
        });

        var deleteButtons = Array.prototype.slice.call(document.querySelectorAll(".delete-book-btn"));
        deleteButtons.forEach(function (button) {
          button.addEventListener("click", async function () {
            var rawId = button.getAttribute("data-book-id") || "";
            var bookId = parseInt(rawId, 10);
            if (!Number.isFinite(bookId) || bookId <= 0) {
              text("library-status", "Delete failed: invalid book id.");
              return;
            }
            var bookTitle = button.getAttribute("data-book-title") || ("Book " + String(bookId));
            if (!window.confirm('Delete "' + bookTitle + '" and imported files?')) {
              return;
            }
            button.disabled = true;
            text("library-status", "Deleting...");
            try {
              var result = await rpc("library.delete", { bookId: bookId });
              var row = button.closest("tr");
              if (row && row.parentNode) {
                row.parentNode.removeChild(row);
              }
              ensureLibraryEmptyRow();
              text(
                "library-status",
                'Deleted "' + bookTitle + '" (book ' + String(result.deletedBookId) + ', files ' + String(result.deletedAssetFileCount || 0) + ")."
              );
            } catch (err) {
              text("library-status", "Delete failed: " + (err && err.message ? err.message : "request error"));
            } finally {
              button.disabled = false;
            }
          });
        });
      })();
    </script>
  </body>
</html>`;

  return new Response(body, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

export function createPodibleFetchHandler(repo: BooksRepo, startTime: number): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    const startedAt = Date.now();
    let settings = repo.getSettings();
    const url = new URL(request.url);
    const method = request.method;
    const pathname = url.pathname;
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

    const isPublicRoute =
      pathname === "/login" ||
      pathname === "/logout" ||
      pathname === "/login/plex/start" ||
      pathname === "/login/plex/loading" ||
      pathname === "/login/plex/complete" ||
      pathname === "/login/plex/status";
    if (!isPublicRoute && !authorizeRequest(request, settings, () => currentSession)) {
      response = new Response("Unauthorized", {
        status: 401,
        headers: { "WWW-Authenticate": 'Bearer realm="podible"' },
      });
      logRequest(response.status);
      return response;
    }

    try {
      const hasAdminAccess =
        (process.env.NODE_ENV !== "production" && ["localhost", "127.0.0.1", "::1"].includes(url.hostname)) ||
        (currentSession?.is_admin ?? 0) === 1 ||
        isApiKeyAuthorized(request, settings);

      if (pathname === "/login" && request.method === "GET") {
        response = renderLoginPage(settings, repo.listUsers("local"), {
          notice: url.searchParams.get("notice"),
          error: url.searchParams.get("error"),
          currentUser: currentSession,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/login" && request.method === "POST") {
        if (settings.auth.mode !== "local") {
          response = new Response("Forbidden", { status: 403 });
          logRequest(response.status);
          return response;
        }
        const body = await request.text();
        const form = new URLSearchParams(body);
        const userIdValue = form.get("userId");
        let user: UserRow | null = null;

        if (userIdValue) {
          const userId = Number.parseInt(userIdValue, 10);
          if (Number.isInteger(userId) && userId > 0) {
            const existing = repo.getUserById(userId);
            if (existing?.provider === "local") {
              user = existing;
            }
          }
        } else {
          const username = (form.get("username") ?? "").trim();
          const displayName = (form.get("displayName") ?? "").trim();
          if (!username) {
            const page = renderLoginPage(settings, repo.listUsers("local"), { error: "Username is required." });
            response = new Response(await page.text(), {
              status: 400,
              headers: page.headers,
            });
            logRequest(response.status);
            return response;
          }
          const providerUserId = normalizeLocalUserKey(username);
          const localUsers = repo.listUsers("local");
          const existing = localUsers.find((candidate) => candidate.provider_user_id === providerUserId) ?? null;
          user = repo.upsertUser({
            provider: "local",
            providerUserId,
            username,
            displayName: displayName || username,
            isAdmin: existing ? existing.is_admin === 1 : localUsers.length === 0,
          });
        }

        if (!user) {
          const page = renderLoginPage(settings, repo.listUsers("local"), { error: "User not found." });
          response = new Response(await page.text(), {
            status: 400,
            headers: page.headers,
          });
          logRequest(response.status);
          return response;
        }

        const sessionToken = createSessionToken();
        repo.createSession(user.id, hashSessionToken(sessionToken), sessionExpiresAt());
        response = redirect("/");
        response.headers.append("Set-Cookie", buildSessionCookie(sessionToken, request));
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
        response = renderPlexCompletePage(settings, pinId, request);
        logRequest(response.status);
        return response;
      }

      if (pathname === "/login/plex/status" && request.method === "GET") {
        if (settings.auth.mode !== "plex") {
          response = json({ ok: false, pending: false, redirectTo: "/", error: "Plex sign-in is not enabled." }, 403);
          logRequest(response.status);
          return response;
        }
        const pinId = Number.parseInt(url.searchParams.get("pinId") ?? "", 10);
        if (!Number.isInteger(pinId) || pinId <= 0) {
          response = json({ ok: false, pending: false, redirectTo: "/login", error: "Missing or invalid Plex PIN id." }, 400);
          logRequest(response.status);
          return response;
        }
        const statusResult = await resolvePlexLoginStatus(repo, settings, pinId);
        settings = statusResult.settings;
        if (statusResult.kind === "pending") {
          response = json({ ok: false, pending: true, message: statusResult.message, pinId, retryAfterMs: 3000 });
          logRequest(response.status);
          return response;
        }
        if (statusResult.kind === "error") {
          response = json({ ok: false, pending: false, redirectTo: statusResult.redirectTo, error: statusResult.error }, 400);
          logRequest(response.status);
          return response;
        }
        response = json({ ok: true, pending: false, redirectTo: statusResult.redirectTo });
        response.headers.append("Set-Cookie", buildSessionCookie(statusResult.sessionToken, request));
        logRequest(response.status);
        return response;
      }

      if (pathname === "/logout" && request.method === "POST") {
        if (currentSession) {
          repo.deleteSession(currentSession.id);
        }
        response = redirect(addApiKey("/login?notice=Signed%20out.", settings.auth.mode === "apikey" ? settings.auth.key : null));
        response.headers.append("Set-Cookie", clearSessionCookie(request));
        logRequest(response.status);
        return response;
      }

      if (pathname === "/" && request.method === "GET") {
        response = renderLandingPage(repo, settings, currentSession);
        logRequest(response.status);
        return response;
      }

      if ((pathname === "/admin" || pathname === "/rpc" || pathname.startsWith("/rpc/")) && !hasAdminAccess) {
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
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/add" && request.method === "GET") {
        const query = (url.searchParams.get("q") ?? "").trim();
        if (!query) {
          response = renderAddPage(settings, { currentUser: currentSession });
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
          });
        } catch (error) {
          response = renderAddPage(settings, {
            query,
            error: `Search failed: ${(error as Error).message}`,
            currentUser: currentSession,
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
          const addResponse = renderAddPage(settings, { error: "openLibraryKey is required.", currentUser: currentSession });
          response = new Response(await addResponse.text(), {
            status: 400,
            headers: addResponse.headers,
          });
          logRequest(response.status);
          return response;
        }
        try {
          const bookId = await createBookFromOpenLibrary(repo, openLibraryKey);
          response = redirect(addApiKey(`/book/${bookId}`, settings.auth.mode === "apikey" ? settings.auth.key : null));
        } catch (error) {
          const addResponse = renderAddPage(settings, {
            error: `Add failed: ${(error as Error).message}`,
            currentUser: currentSession,
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
        response = redirect(
          addApiKey(`/book/${bookId}?notice=${encodeURIComponent(notice)}`, settings.auth.mode === "apikey" ? settings.auth.key : null)
        );
        logRequest(response.status);
        return response;
      }

      if (pathname === "/activity" && request.method === "GET") {
        response = renderActivityPage(repo, settings, {
          notice: url.searchParams.get("notice"),
          error: url.searchParams.get("error"),
          currentUser: currentSession,
        });
        logRequest(response.status);
        return response;
      }

      if (pathname === "/activity/refresh" && request.method === "POST") {
        const job = repo.createJob({ type: "full_library_refresh" });
        response = redirect(
          addApiKey(`/activity?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`, settings.auth.mode === "apikey" ? settings.auth.key : null)
        );
        logRequest(response.status);
        return response;
      }

      if (pathname === "/admin" && request.method === "GET") {
        let plexServers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }> = [];
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
        response = await handleRpcRequest(request, { repo, startTime });
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
          if (key === "api_key") continue;
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
        response = await handleRpcMethod(parts.join("."), params, { repo, startTime }, { id: null, readOnly: true });
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
