import { hydrateBookFromOpenLibrary } from "../hydration";
import { buildSessionCookie, createSessionToken, hashSessionToken, sessionExpiresAt } from "../auth";
import { loadStoredTranscriptPayload, selectPreferredEpubAsset } from "../chapter-analysis";
import { buildChapters, preferredAudioForBooks, selectPreferredAudioAsset, streamExtension } from "../media";
import {
  checkPlexUserAccess,
  exchangePlexPinForToken,
  fetchPlexUser,
} from "../plex";
import { triggerAutoAcquire } from "../service";
import { escapeXml, firstLine, htmlToPlainText, truncate } from "../../utils/strings";

import { BooksRepo } from "../repo";
import { resolveOpenLibraryCandidate, type OpenLibraryCandidate } from "../openlibrary";
import type { AppSettings, PlexJwk, SessionWithUserRow } from "../types";

import {
  addApiKey,
  escapeHtml,
  isHtmlPageRoute,
  messageMarkup,
  parseAppLoginPath,
  renderAppAuthErrorPage,
  renderAppPage,
  sanitizeRedirectPath,
} from "./common";
import { renderAdminPage } from "./admin-page";

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
    <style>
      .login-shell {
        min-height: calc(100vh - 36px);
        display: grid;
        place-items: center;
      }
      .login-hero {
        max-width: 560px;
        margin: 0 auto;
        padding: 28px;
        text-align: center;
        background:
          radial-gradient(circle at top, rgba(40, 89, 67, 0.08), transparent 55%),
          var(--paper);
      }
      .login-hero p {
        margin-left: auto;
        margin-right: auto;
      }
      .login-hero .actions {
        justify-content: center;
      }
      button.login-cta {
        min-width: 220px;
        justify-content: center;
        font-weight: 600;
        background: #e5a00d;
        border-color: #e5a00d;
        color: #1f261c;
      }
      button.login-cta:hover {
        background: #d09108;
        border-color: #d09108;
        color: #1f261c;
      }
    </style>
    <div class="login-shell">
    <section class="hero login-hero">
      <h1>Sign in to Podible</h1>
      <p>Use Plex to sign in and open your library.</p>
      ${messageMarkup(options.notice, options.error)}
      <div class="actions" style="margin-top: 14px;">
        <button id="plex-login-btn" type="button" class="login-cta">Continue with Plex</button>
      </div>
      <p id="plex-login-status" class="muted" style="margin-top: 10px;"></p>
    </section>
    </div>
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
        function openCenteredPopup(url, name, width, height) {
          const dualScreenLeft = window.screenLeft ?? window.screenX ?? 0;
          const dualScreenTop = window.screenTop ?? window.screenY ?? 0;
          const viewportWidth = window.innerWidth || document.documentElement.clientWidth || screen.width;
          const viewportHeight = window.innerHeight || document.documentElement.clientHeight || screen.height;
          const left = Math.max(0, Math.round(dualScreenLeft + (viewportWidth - width) / 2));
          const top = Math.max(0, Math.round(dualScreenTop + (viewportHeight - height) / 2));
          const features = [
            "popup=yes",
            "width=" + width,
            "height=" + height,
            "left=" + left,
            "top=" + top,
            "resizable=yes",
            "scrollbars=yes",
          ].join(",");
          return window.open(url, name, features);
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
          const popup = inlineLogin ? null : openCenteredPopup(loadingUrl, "podible-plex-login", 520, 680);
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
  return renderAppPage("Sign in", body, settings, options.currentUser ?? null, "", apiKey, { showNav: false });
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
    const existingPlexUsers = repo.listUsers("plex");
    const existingPlexUser = existingPlexUsers.find((user) => user.provider_user_id === plexUser.id) ?? null;
    const existingUsers = repo.listUsers();
    const isBootstrap = existingPlexUsers.length === 0;
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
        plexUser.id === (existingPlexUsers.find((user) => user.is_admin === 1)?.provider_user_id ?? "")
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

    const hasPlexAdminUser = existingPlexUsers.some((user) => user.is_admin === 1);
    if (!settings.auth.plex.ownerToken && (!hasPlexAdminUser || existingPlexUser?.is_admin === 1)) {
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
      isAdmin: existingPlexUser ? existingPlexUser.is_admin === 1 : !hasPlexAdminUser,
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

export {
  addApiKey,
  parseAppLoginPath,
  isHtmlPageRoute,
  parseMediaSelection,
  sanitizeRedirectPath,
  renderAddPage,
  renderAdminPage,
  renderAppAuthErrorPage,
  renderActivityPage,
  renderBookPage,
  renderLandingPage,
  renderLibraryPage,
  renderLoginPage,
  renderPlexImmediateResultPage,
  renderPlexLoadingPage,
  createBookFromOpenLibrary,
  waitForPlexLoginResult,
};
