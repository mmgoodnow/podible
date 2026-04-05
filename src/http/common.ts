import type { AppSettings, SessionWithUserRow, UserRow } from "../app-types";

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
    blocks.push(`<p style="margin-top: 10px; color: var(--danger);">${escapeHtml(error)}</p>`);
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

function renderAppPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  extraNav = "",
  apiKey: string | null = null,
  options: { showNav?: boolean } = {}
): Response {
  void settings;
  const showNav = options.showNav !== false;
  const showAdminNav = Boolean(apiKey) || (currentUser?.is_admin ?? 0) === 1;
  const themeToggle = `<button type="button" class="theme-toggle" id="theme-toggle">System</button>`;
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
      ${themeToggle}
      ${extraNav}
    </nav>`;
  return new Response(
    `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="color-scheme" content="light dark" />
    <title>${escapeHtml(title)}</title>
    <script>
      (() => {
        const stored = localStorage.getItem("podible-theme");
        if (stored === "dark" || stored === "light") {
          document.documentElement.dataset.theme = stored;
        }
      })();
    </script>
    <style>
      :root {
        color-scheme: light;
        --bg: #f6f7f3;
        --paper: #fffdf7;
        --surface: #ffffff;
        --surface-hover: #faf8f1;
        --line: #ddd6c8;
        --line-soft: #ebe5d8;
        --text: #1f261c;
        --muted: #5f6b58;
        --accent: #285943;
        --accent-contrast: #ffffff;
        --accent-soft: #eef5f0;
        --code-bg: #f3f1ea;
        --danger: #8b0000;
        --danger-border: #6f0000;
        --danger-contrast: #fff;
        --bg-grad-start: #fffefb;
        --shadow: 0 1px 2px rgba(31, 38, 28, 0.05);
      }
      :root[data-theme="dark"] {
        color-scheme: dark;
        --bg: #111713;
        --paper: #18211b;
        --surface: #1d2720;
        --surface-hover: #233127;
        --line: #324037;
        --line-soft: #2a352e;
        --text: #edf4eb;
        --muted: #a5b5a5;
        --accent: #8cc2a5;
        --accent-contrast: #0f1713;
        --accent-soft: #1e2b23;
        --code-bg: #101713;
        --danger: #ff8f8f;
        --danger-border: #b85d5d;
        --danger-contrast: #1a0f0f;
        --bg-grad-start: #1a231d;
        --shadow: 0 8px 24px rgba(0, 0, 0, 0.24);
      }
      @media (prefers-color-scheme: dark) {
        :root:not([data-theme]) {
          color-scheme: dark;
          --bg: #111713;
          --paper: #18211b;
          --surface: #1d2720;
          --surface-hover: #233127;
          --line: #324037;
          --line-soft: #2a352e;
          --text: #edf4eb;
          --muted: #a5b5a5;
          --accent: #8cc2a5;
          --accent-contrast: #0f1713;
          --accent-soft: #1e2b23;
          --code-bg: #101713;
          --danger: #ff8f8f;
          --danger-border: #b85d5d;
          --danger-contrast: #1a0f0f;
          --bg-grad-start: #1a231d;
          --shadow: 0 8px 24px rgba(0, 0, 0, 0.24);
        }
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: ui-serif, Georgia, serif;
        background: radial-gradient(circle at top, var(--bg-grad-start) 0%, var(--bg) 60%);
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
        background: var(--surface);
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
        background: var(--surface);
        color: var(--text);
        text-decoration: none;
        cursor: pointer;
      }
      button:not(.nav-signout-button):hover, .button-link:hover {
        background: var(--surface-hover);
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
      .site-nav .theme-toggle {
        display: inline-block;
        appearance: none;
        -webkit-appearance: none;
        min-height: 0;
        margin: 0 0 0 auto;
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
      .site-nav .theme-toggle:hover {
        color: var(--accent);
        text-decoration: underline;
      }
      .hero, .card { background: var(--paper); border: 1px solid var(--line); border-radius: 16px; box-shadow: var(--shadow); }
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
      .book-row { display: grid; grid-template-columns: 72px minmax(0, 1fr); gap: 12px; align-items: start; padding: 10px; border: 1px solid var(--line); border-radius: 12px; background: var(--surface); }
      .cover, .cover-fallback { width: 72px; height: 72px; border-radius: 10px; }
      .cover-fallback { display: flex; align-items: center; justify-content: center; background: var(--accent-soft); color: var(--accent); font-weight: 700; }
      .cover { object-fit: cover; display: block; border: 1px solid var(--line); }
      .meta h3 { margin: 0 0 2px; font-size: 18px; }
      .meta p { margin: 0; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; font-size: 14px; }
      .button-link-primary {
        background: var(--accent);
        border-color: var(--accent);
        color: var(--accent-contrast);
      }
      .button-link-primary:hover {
        background: var(--accent);
        color: var(--accent-contrast);
      }
      .pill { display: inline-flex; padding: 3px 8px; border-radius: 999px; border: 1px solid var(--line); background: var(--surface); font-size: 12px; color: var(--muted); }
      .stats { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
      .empty { padding: 16px; border: 1px dashed var(--line); border-radius: 12px; color: var(--muted); background: var(--surface); }
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
      ${showNav ? "" : `<div class="site-nav" style="justify-content: flex-end; margin-bottom: 18px;">${themeToggle}</div>`}
      ${showNav ? nav : ""}
      ${body}
    </div>
    <script>
      (() => {
        const button = document.getElementById("theme-toggle");
        const root = document.documentElement;
        const media = window.matchMedia("(prefers-color-scheme: dark)");

        function storedTheme() {
          const value = localStorage.getItem("podible-theme");
          return value === "dark" || value === "light" ? value : "system";
        }

        function resolvedTheme(mode) {
          if (mode === "system") {
            return media.matches ? "dark" : "light";
          }
          return mode;
        }

        function cycleTheme(mode) {
          if (mode === "system") return "dark";
          if (mode === "dark") return "light";
          return "system";
        }

        function apply(mode) {
          if (mode === "dark" || mode === "light") {
            root.dataset.theme = mode;
          } else {
            delete root.dataset.theme;
          }
          if (button) {
            const resolved = resolvedTheme(mode);
            const label = mode[0].toUpperCase() + mode.slice(1) + (mode === "system" ? " (" + resolved + ")" : "");
            button.textContent = label;
          }
        }

        apply(storedTheme());
        media.addEventListener("change", () => {
          if (storedTheme() === "system") apply("system");
        });
        button?.addEventListener("click", () => {
          const next = cycleTheme(storedTheme());
          if (next === "system") {
            localStorage.removeItem("podible-theme");
          } else {
            localStorage.setItem("podible-theme", next);
          }
          apply(next);
        });
      })();
    </script>
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

export {
  addApiKey,
  escapeHtml,
  messageMarkup,
  renderAppAuthErrorPage,
  renderAppPage,
  sanitizeRedirectPath,
};
