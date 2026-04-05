import type { AppSettings, SessionWithUserRow, UserRow } from "../types";

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

function renderAppPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  extraNav = "",
  apiKey: string | null = null
): Response {
  void settings;
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
      .site-nav { display: flex; gap: 14px; flex-wrap: wrap; align-items: baseline; margin-bottom: 18px; font-size: 14px; }
      .nav-signout-form { display: inline-flex; align-items: baseline; margin: 0; }
      input, select, textarea, button { font: inherit; }
      input, select, textarea {
        padding: 8px 10px;
        border: 1px solid var(--line);
        border-radius: 10px;
        background: #fff;
        color: var(--text);
      }
      button:not(.nav-signout-button), .button-link, .actions button {
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
        cursor: pointer;
      }
      button:not(.nav-signout-button):hover, .button-link:hover {
        background: #faf8f1;
        text-decoration: none;
      }
      .site-nav .nav-signout-button {
        display: inline-block;
        appearance: none;
        -webkit-appearance: none;
        min-height: 0;
        margin: 0;
        padding: 0;
        border: 0;
        border-radius: 0;
        background: transparent;
        box-shadow: none;
        color: var(--muted);
        font: inherit;
        cursor: pointer;
        line-height: inherit;
        text-decoration: none;
      }
      .site-nav .nav-signout-button:hover {
        color: var(--accent);
        text-decoration: underline;
      }
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
      .button-link-primary {
        background: var(--accent);
        border-color: var(--accent);
        color: #fff;
      }
      .button-link-primary:hover {
        background: var(--accent);
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

export {
  addApiKey,
  escapeHtml,
  isHtmlPageRoute,
  messageMarkup,
  parseAppLoginPath,
  renderAppAuthErrorPage,
  renderAppPage,
  sanitizeRedirectPath,
};
