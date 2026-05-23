import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { renderAdminDownloadsPageScript, renderAdminJobsPageScript, renderAdminSettingsPageScript } from "./admin-page-client";
import { decodePlexTokenExpiry } from "../plex";

type PlexServerView = {
  machineId: string;
  name: string;
  product: string;
  owned: boolean;
  sourceTitle: string | null;
};

type AdminPageOptions = {
  apiKey?: string | null;
  notice?: string | null;
  error?: string | null;
};

function plexOwnerTokenStatus(token: string): string {
  if (!token) return "missing";
  const expiry = decodePlexTokenExpiry(token);
  if (expiry.expSeconds === null) return "captured";
  if (expiry.expired) return "EXPIRED - log out and sign in again as a Plex admin to re-link";
  const days = Math.floor((expiry.expiresInMs ?? 0) / 86_400_000);
  const hours = Math.floor(((expiry.expiresInMs ?? 0) % 86_400_000) / 3_600_000);
  if (days > 0) return `captured (expires in ${days}d ${hours}h)`;
  if (hours > 0) return `captured (expires in ${hours}h - re-link soon)`;
  const minutes = Math.max(0, Math.floor((expiry.expiresInMs ?? 0) / 60_000));
  return `captured (expires in ${minutes}m - re-link now)`;
}

function adminPageStyles(): string {
  return `<style>
    .admin-grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 14px; align-items: start; }
    .admin-grid > * { min-width: 0; }
    .admin-card-link { display: grid; gap: 6px; color: inherit; text-decoration: none; min-height: 126px; }
    .admin-card-link:hover { text-decoration: none; background: var(--surface-hover); }
    .admin-card-link h2 { margin: 0; }
    .admin-card-link p { margin: 0; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .settings-actions { display: flex; gap: 12px; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; }
    .settings-actions-left { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .settings-editor { width: 100%; min-height: 620px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
    .admin-grid input, .admin-grid select { min-width: 180px; }
    .status-cell { max-width: 360px; overflow-wrap: anywhere; }
    pre { margin: 0; padding: 8px; border: 1px solid var(--line-soft); border-radius: 12px; background: var(--surface); overflow: auto; }
    @media (max-width: 900px) { .admin-grid .span-4, .admin-grid .span-6, .admin-grid .span-8 { grid-column: span 12; } }
  </style>`;
}

function adminTitle(title: string): string {
  return `<div class="section-title-row"><h2>${escapeHtml(title)}</h2><span class="admin-only-pill">Admin only</span></div>`;
}

function adminSubnav(apiKey: string | null): string {
  const links = [
    ["/admin", "Hub"],
    ["/admin/settings", "Settings"],
    ["/admin/users", "Users"],
    ["/admin/jobs", "Jobs"],
    ["/admin/downloads", "Downloads"],
    ["/admin/content", "Content Ops"],
    ["/admin/curation", "Curation"],
    ["/admin/db", "DB"],
  ];
  return `<div class="actions" style="margin-bottom: 14px;">${links
    .map(([href, label]) => `<a class="button-link" href="${escapeHtml(addApiKey(href, apiKey))}">${escapeHtml(label)}</a>`)
    .join("")}</div>`;
}

function adminPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  apiKey: string | null,
  script = ""
): Response {
  return renderAppPage(title, `${adminPageStyles()}${adminSubnav(apiKey)}${body}${script}`, settings, currentUser, "", apiKey);
}

export function renderAdminPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  options: AdminPageOptions = {}
): Response {
  const health = repo.getHealthSummary();
  const contentRows = repo.listAdminContentOps();
  const activeJobs = (health.jobs.queued ?? 0) + (health.jobs.running ?? 0);
  const failedJobs = health.jobs.failed ?? 0;
  const failedContent = contentRows.filter((row) => row.transcript_status === "failed" || row.chapter_status === "failed").length;
  const apiKey = options.apiKey ?? null;
  const cards = [
    ["/admin/settings", "Settings", "Edit app settings, Plex access control, refresh the library, and handle dev reset operations."],
    ["/admin/users", "Users", "Inspect browser and app users with their admin status."],
    ["/admin/jobs", "Jobs", "Review recent jobs, filter by type, and retry failed work."],
    ["/admin/downloads", "Downloads", "Inspect recent download jobs and live transfer progress."],
    ["/admin/content", "Content Ops", "Track transcription, chapter analysis, and curation readiness."],
    ["/admin/curation", "Curation", "Browse agentic chapter-curation runs, trees, rulers, and traces."],
    ["/admin/db", "DB Explorer", "Read-only table browsing for lightweight database inspection."],
  ];
  const body = `
    <section class="hero">
      <h1>Admin</h1>
      <p>Focused admin tools for operations, recovery, and diagnostics.</p>
      <div class="stats">
        <span class="pill">${activeJobs} active jobs</span>
        <span class="pill">${failedJobs} failed jobs</span>
        <span class="pill">${failedContent} content issues</span>
      </div>
      ${messageMarkup(options.notice, options.error)}
    </section>
    <div class="admin-grid">
      ${cards
        .map(
          ([href, label, description]) => `<a class="card admin-card-link span-4 admin-only-card" href="${escapeHtml(addApiKey(href, apiKey))}">
            <span class="admin-only-pill">Admin only</span>
            <h2>${escapeHtml(label)}</h2>
            <p class="muted">${escapeHtml(description)}</p>
          </a>`
        )
        .join("")}
    </div>`;
  return adminPage("Admin", body, settings, currentUser, apiKey);
}

export function renderAdminSettingsPage(
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  options: AdminPageOptions & {
    plexServers?: PlexServerView[];
    plexNotice?: string | null;
    plexError?: string | null;
  } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const settingsJson = escapeHtml(JSON.stringify(settings, null, 2));
  const plexServers = options.plexServers ?? [];
  const selectedPlexServer = plexServers.find((server) => server.machineId === settings.auth.plex.machineId) ?? null;
  const plexServerOptions = plexServers
    .map((server) => {
      const label = `${server.name} (${server.owned ? "owned" : "shared"}${server.sourceTitle ? ` - ${server.sourceTitle}` : ""})`;
      return `<option value="${escapeHtml(server.machineId)}"${server.machineId === settings.auth.plex.machineId ? " selected" : ""}>${escapeHtml(label)}</option>`;
    })
    .join("");
  const body = `
    <section class="hero"><h1>Settings</h1><p>Edit runtime configuration and Plex access control.</p></section>
    <div class="admin-grid">
      ${
        settings.auth.mode === "plex"
          ? `<section class="card span-12 admin-only-card">
              ${adminTitle("Plex Access Control")}
              <p class="muted">Choose which Plex server controls future browser sign-ins.</p>
              <p class="muted">Owner token: <strong>${escapeHtml(plexOwnerTokenStatus(settings.auth.plex.ownerToken))}</strong> | Selected server: <strong>${escapeHtml(selectedPlexServer?.name || settings.auth.plex.machineId || "not set")}</strong></p>
              ${messageMarkup(options.plexNotice, options.plexError)}
              ${
                plexServers.length > 0
                  ? `<form method="post" action="${escapeHtml(addApiKey("/admin/plex/select", apiKey))}" class="row">
                      <select id="plex-server-select" name="machineId">${plexServerOptions}</select>
                      <button type="submit">Use selected server</button>
                    </form>`
                  : `<p class="muted">No Plex servers were found for the current owner token.</p>`
              }
            </section>`
          : ""
      }
      <section class="card span-12 admin-only-card">
        ${adminTitle("Settings JSON")}
        <div class="settings-actions">
          <div class="settings-actions-left">
            <button id="settings-save-btn" type="button">Save Settings</button>
            <form method="post" action="${escapeHtml(addApiKey("/admin/refresh", apiKey))}" style="margin: 0;">
              <button type="submit">Refresh Library</button>
            </form>
          </div>
          <button id="wipe-db-btn" type="button" style="background: var(--danger); color: var(--danger-contrast); border: 1px solid var(--danger-border);">Wipe Entire Database</button>
        </div>
        <p id="settings-status" class="muted"></p>
        ${messageMarkup(options.notice, options.error)}
        <textarea id="settings-editor" class="settings-editor" spellcheck="false">${settingsJson}</textarea>
      </section>
    </div>`;
  return adminPage("Admin Settings", body, settings, currentUser, apiKey, renderAdminSettingsPageScript());
}

export function renderAdminUsersPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  options: AdminPageOptions = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const rows = repo.listUsers();
  const userRows = rows.length
    ? rows
        .map(
          (user) => `<tr>
            <td>${user.id}</td>
            <td>${escapeHtml(user.provider)}</td>
            <td>${escapeHtml(user.username)}</td>
            <td>${escapeHtml(user.display_name || "")}</td>
            <td>${user.is_admin ? "yes" : "no"}</td>
            <td>${escapeHtml(user.updated_at)}</td>
          </tr>`
        )
        .join("")
    : `<tr><td colspan="6">No users yet.</td></tr>`;
  const body = `
    <section class="hero"><h1>Users</h1><p>${rows.length} user${rows.length === 1 ? "" : "s"} with app access.</p></section>
    <section class="card admin-only-card">
      ${adminTitle("Users")}
      <div class="table-wrap"><table>
        <thead><tr><th>ID</th><th>Provider</th><th>Username</th><th>Display Name</th><th>Admin</th><th>Updated</th></tr></thead>
        <tbody>${userRows}</tbody>
      </table></div>
    </section>`;
  return adminPage("Admin Users", body, settings, currentUser, apiKey);
}

export function renderAdminDownloadsPage(settings: AppSettings, currentUser: SessionWithUserRow | null, options: AdminPageOptions = {}): Response {
  const apiKey = options.apiKey ?? null;
  const body = `
    <section class="hero"><h1>Downloads</h1><p>Recent download jobs with release status and transfer progress.</p></section>
    <section class="card admin-only-card">
      ${adminTitle("Recent Downloads")}
      <div class="row"><button id="downloads-refresh-btn" type="button">Refresh Downloads</button></div>
      <p id="downloads-status" class="muted"></p>
      <div class="table-wrap"><table>
        <thead><tr><th>Job</th><th>Release</th><th>Media</th><th>Status</th><th>Progress</th><th>Transfer</th><th>Error</th></tr></thead>
        <tbody id="downloads-table-body"><tr><td colspan="7">Loading...</td></tr></tbody>
      </table></div>
    </section>`;
  return adminPage("Admin Downloads", body, settings, currentUser, apiKey, renderAdminDownloadsPageScript());
}

export function renderAdminJobsPage(settings: AppSettings, currentUser: SessionWithUserRow | null, options: AdminPageOptions = {}): Response {
  const apiKey = options.apiKey ?? null;
  const body = `
    <section class="hero"><h1>Jobs</h1><p>Review and retry background work.</p></section>
    <section class="card admin-only-card">
      ${adminTitle("Recent Jobs")}
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
      <div class="table-wrap"><table>
        <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Book</th><th>Release</th><th>Attempts</th><th>Updated</th><th>Action</th><th>Error</th></tr></thead>
        <tbody id="jobs-table-body"><tr><td colspan="9">Loading...</td></tr></tbody>
      </table></div>
    </section>`;
  return adminPage("Admin Jobs", body, settings, currentUser, apiKey, renderAdminJobsPageScript());
}

export function renderAdminContentPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  options: AdminPageOptions = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const rows = repo.listAdminContentOps();
  const failed = rows.filter((row) => row.transcript_status === "failed" || row.chapter_status === "failed").length;
  const withChapters = rows.filter((row) => row.has_chapters === 1).length;
  const tableRows = rows.length
    ? rows
        .map((row) => {
          const transcript = [row.transcript_status, row.transcript_error].filter(Boolean).join(": ") || "not requested";
          const chapter = [
            row.chapter_status,
            row.has_chapters ? "chapters ready" : null,
            row.resolved_boundary_count !== null && row.total_boundary_count !== null ? `${row.resolved_boundary_count}/${row.total_boundary_count} boundaries` : null,
            row.chapter_error,
          ]
            .filter(Boolean)
            .join(" - ") || "not requested";
          return `<tr>
            <td><a href="${escapeHtml(addApiKey(`/book/${row.book_id}`, apiKey))}">${escapeHtml(row.title)}</a><div class="muted">${escapeHtml(row.author)}</div></td>
            <td>${row.manifestation_id}</td>
            <td class="status-cell">${escapeHtml(transcript)}</td>
            <td class="status-cell">${escapeHtml(chapter)}</td>
            <td>${escapeHtml(row.updated_at || "")}</td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="5">No content operations yet.</td></tr>`;
  const body = `
    <section class="hero">
      <h1>Content Ops</h1>
      <p>Transcription, chapter analysis, and curation state across audio editions.</p>
      <div class="stats"><span class="pill">${rows.length} audio editions</span><span class="pill">${withChapters} with chapters</span><span class="pill">${failed} failed</span></div>
    </section>
    <section class="card admin-only-card">
      ${adminTitle("Transcription / Chapter Analysis")}
      <div class="table-wrap"><table>
        <thead><tr><th>Book</th><th>Edition</th><th>Transcript</th><th>Chapter Analysis</th><th>Updated</th></tr></thead>
        <tbody>${tableRows}</tbody>
      </table></div>
    </section>`;
  return adminPage("Admin Content Ops", body, settings, currentUser, apiKey);
}

export function renderAdminDbPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  input: { table?: string | null; limit?: number; offset?: number; apiKey?: string | null; error?: string | null } = {}
): Response {
  const apiKey = input.apiKey ?? null;
  const tables = repo.adminDbTableNames();
  const table = input.table && tables.includes(input.table) ? input.table : tables[0] ?? "";
  const limit = Math.min(Math.max(Math.trunc(input.limit ?? 25), 1), 100);
  const offset = Math.max(Math.trunc(input.offset ?? 0), 0);
  let page: { columns: string[]; rows: Array<Record<string, unknown>>; total: number } = { columns: [], rows: [], total: 0 };
  let error = input.error ?? null;
  if (table) {
    try {
      page = repo.adminDbTablePage(table, limit, offset);
    } catch (caught) {
      error = (caught as Error).message;
    }
  }
  const options = tables.map((name) => `<option value="${escapeHtml(name)}"${name === table ? " selected" : ""}>${escapeHtml(name)}</option>`).join("");
  const rows = page.rows.length
    ? page.rows
        .map((row) => `<tr>${page.columns.map((column) => `<td>${escapeHtml(formatDbValue(row[column]))}</td>`).join("")}</tr>`)
        .join("")
    : `<tr><td colspan="${Math.max(1, page.columns.length)}">No rows.</td></tr>`;
  const nextOffset = offset + limit < page.total ? offset + limit : offset;
  const prevOffset = Math.max(0, offset - limit);
  const body = `
    <section class="hero"><h1>DB Explorer</h1><p>Read-only table browsing for lightweight diagnostics. No SQL editor is exposed.</p></section>
    <section class="card admin-only-card">
      ${adminTitle("Tables")}
      ${messageMarkup(null, error)}
      <form method="get" action="${escapeHtml(addApiKey("/admin/db", apiKey))}" class="row">
        <label for="db-table">Table</label>
        <select id="db-table" name="table">${options}</select>
        <label for="db-limit">Limit</label>
        <input id="db-limit" name="limit" type="number" min="1" max="100" value="${limit}" style="min-width: 90px;" />
        <input name="offset" type="hidden" value="0" />
        <button type="submit">Load</button>
      </form>
      <p class="muted">${escapeHtml(table)}: ${offset + 1}-${Math.min(offset + page.rows.length, page.total)} of ${page.total}</p>
      <div class="actions">
        <a class="button-link" href="${escapeHtml(addApiKey(`/admin/db?table=${encodeURIComponent(table)}&limit=${limit}&offset=${prevOffset}`, apiKey))}">Previous</a>
        <a class="button-link" href="${escapeHtml(addApiKey(`/admin/db?table=${encodeURIComponent(table)}&limit=${limit}&offset=${nextOffset}`, apiKey))}">Next</a>
      </div>
      <div class="table-wrap"><table>
        <thead><tr>${page.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>
        <tbody>${rows}</tbody>
      </table></div>
    </section>`;
  return adminPage("Admin DB Explorer", body, settings, currentUser, apiKey);
}

export function renderAdminCurationPage(settings: AppSettings, currentUser: SessionWithUserRow | null, options: AdminPageOptions = {}): Response {
  const apiKey = options.apiKey ?? null;
  const body = `
    <section class="hero"><h1>Chapter Curation</h1><p>Agentic chapter-curation run dashboard.</p></section>
    <section class="card admin-only-card curation-shell">
      ${adminTitle("Runs")}
      <div class="row" style="margin-bottom: 10px;">
        <label><input id="curation-auto-refresh" type="checkbox" checked /> Auto-refresh 5s</label>
        <button id="curation-refresh" type="button">Refresh</button>
      </div>
      <div class="admin-grid">
        <aside class="span-4"><h2>Previous Runs</h2><div id="curation-runs" class="section-list"></div></aside>
        <main class="span-8"><h2 id="curation-title">Run</h2><div id="curation-current"></div></main>
      </div>
    </section>
    <style>
      .curation-run-button { width: 100%; justify-content: flex-start; text-align: left; display: grid; gap: 3px; margin-bottom: 8px; }
      .curation-metrics { display: grid; grid-template-columns: repeat(4, minmax(100px, 1fr)); gap: 8px; margin: 10px 0; }
      .curation-tree { display: grid; grid-template-columns: repeat(auto-fill, minmax(104px, 1fr)); gap: 6px; max-height: 420px; overflow: auto; }
      .curation-tree-node { border: 1px solid var(--line); border-radius: 8px; padding: 7px; background: var(--surface); text-align: left; }
      .curation-tree-node[data-trace-file] { cursor: pointer; border-color: color-mix(in srgb, var(--accent) 50%, var(--line)); }
      .curation-ruler { position: relative; height: 88px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); overflow: hidden; margin: 10px 0; }
      .curation-tick { position: absolute; top: 18px; height: 52px; width: 2px; background: var(--accent); }
      .curation-tick.leaf { background: #35834d; }
      .curation-tick.error { background: var(--danger); opacity: 0.45; width: auto; }
      #curation-trace pre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 520px; }
      @media (max-width: 900px) { .curation-metrics { grid-template-columns: 1fr 1fr; } }
    </style>
    <script>
      (() => {
        let selectedRunId = null;
        let latestRuns = [];
        let timer = null;
        const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
        const fmt = (value) => value ? new Date(value).toLocaleString() : "";
        const dur = (seconds) => seconds == null ? "" : seconds < 90 ? Math.round(seconds) + "s" : Math.round(seconds / 60) + "m";
        function metric(label, value) { return '<div class="pill"><strong>' + esc(value) + '</strong>&nbsp;' + esc(label) + '</div>'; }
        function renderRuns(runs) {
          return runs.map((run) => '<button type="button" class="curation-run-button" data-run-id="' + esc(run.id) + '"><strong>' + esc(run.caseLabel) + '</strong><span class="muted">' + esc(run.status) + ' - ' + esc(fmt(run.startedAt)) + ' - ' + esc(dur(run.durationSeconds)) + '</span></button>').join("") || '<div class="empty">No runs found.</div>';
        }
        function pct(value, total) { return total ? Math.max(0, Math.min(100, value / total * 100)) : 0; }
        function renderRuler(run) {
          const total = run.audiobookDurationSeconds || 1;
          const markers = (run.boundaryMarkers || []).slice(0, 180).map((marker) => '<span class="curation-tick ' + esc(marker.source) + '" title="' + esc(marker.title + ' ' + marker.startTime + 's') + '" style="left:' + pct(marker.startTime, total) + '%"></span>').join("");
          const failed = (run.failedSpans || []).map((span) => '<span class="curation-tick error" title="' + esc(span.path) + '" style="left:' + pct(span.startTime || 0, total) + '%;width:' + Math.max(0.4, pct((span.endTime || 0) - (span.startTime || 0), total)) + '%"></span>').join("");
          return '<h2>Boundary Ruler</h2><div class="curation-ruler">' + failed + markers + '</div>';
        }
        function renderTree(run) {
          const traces = new Map((run.traceSummaries || []).map((trace) => [trace.spanPath, trace.file]));
          const nodes = (run.treeSpans || []).slice(0, 220).map((span) => {
            const file = traces.get(span.path) || "";
            return '<button type="button" class="curation-tree-node" data-span-path="' + esc(span.path) + '"' + (file ? ' data-trace-file="' + esc(file) + '"' : '') + '><strong><code>' + esc(span.path) + '</code></strong><div class="muted">n ' + esc(span.nodeCount ?? "") + ' - t ' + esc(span.toolCalls) + ' - r ' + esc(span.judgeRejected) + '</div><div>' + esc(span.terminal || "active") + '</div></button>';
          }).join("");
          return '<h2>Binary Tree Progress</h2><div class="curation-tree">' + (nodes || '<div class="empty">No spans yet.</div>') + '</div>';
        }
        function renderCurrent(run) {
          if (!run) return '<div class="empty">No run selected.</div>';
          document.querySelector("#curation-title").textContent = run.caseLabel + " " + run.status;
          return '<div class="curation-metrics">' +
            metric("tool calls", run.metrics.toolCalls) +
            metric("splits", run.metrics.splits) +
            metric("leaves", run.metrics.leaves) +
            metric("errors", run.metrics.spanErrors) +
            '</div>' + renderTree(run) + renderRuler(run) +
            '<section id="curation-trace" class="card" style="margin-top: 12px;"><h2>Thinking Trace</h2><div class="muted">Click a binary-tree span with recorded trace data.</div></section>' +
            '<section class="card" style="margin-top: 12px;"><h2>Event Tail</h2><div class="table-wrap"><table><tbody>' + (run.eventTail || []).map((event) => '<tr><td>' + esc(fmt(event.ts)) + '</td><td><code>' + esc(event.type) + '</code></td><td>' + esc(event.message) + '</td></tr>').join("") + '</tbody></table></div></section>';
        }
        async function loadTrace(file) {
          const response = await fetch('/admin/curation/api/trace?runId=' + encodeURIComponent(selectedRunId || '') + '&file=' + encodeURIComponent(file), { cache: 'no-store' });
          const data = await response.json();
          const text = JSON.stringify(data.finalOutput ?? null, null, 2);
          document.querySelector("#curation-trace").innerHTML = '<h2>Thinking Trace</h2><p class="muted"><code>' + esc(file) + '</code></p>' + (data.reasoningSummaries?.length ? '<pre>' + esc(data.reasoningSummaries.join("\\n\\n")) + '</pre>' : '<p class="muted">No stored reasoning summary.</p>') + '<h2>Final Output</h2><pre>' + esc(text) + '</pre>';
        }
        async function refresh() {
          const response = await fetch('/admin/curation/api/runs?selectedRunId=' + encodeURIComponent(selectedRunId || ''), { cache: 'no-store' });
          const data = await response.json();
          latestRuns = data.runs || [];
          if (!selectedRunId && latestRuns[0]) selectedRunId = latestRuns[0].id;
          const selected = latestRuns.find((run) => run.id === selectedRunId) || latestRuns[0];
          document.querySelector("#curation-runs").innerHTML = renderRuns(latestRuns);
          document.querySelector("#curation-current").innerHTML = renderCurrent(selected);
        }
        document.querySelector("#curation-runs").addEventListener("click", (event) => {
          const button = event.target.closest("[data-run-id]");
          if (!button) return;
          selectedRunId = button.getAttribute("data-run-id");
          refresh();
        });
        document.querySelector("#curation-current").addEventListener("click", (event) => {
          const button = event.target.closest("[data-trace-file]");
          if (!button) return;
          loadTrace(button.getAttribute("data-trace-file"));
        });
        document.querySelector("#curation-refresh").addEventListener("click", refresh);
        document.querySelector("#curation-auto-refresh").addEventListener("change", (event) => {
          if (timer) clearInterval(timer);
          timer = event.target.checked ? setInterval(refresh, 5000) : null;
        });
        refresh();
        timer = setInterval(refresh, 5000);
      })();
    </script>`;
  return adminPage("Admin Curation", body, settings, currentUser, apiKey);
}

function formatDbValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Uint8Array) return `[${value.byteLength} bytes]`;
  if (typeof value === "object") return JSON.stringify(value);
  const text = String(value);
  return text.length > 500 ? `${text.slice(0, 500)}...` : text;
}
