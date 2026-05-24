import { BooksRepo } from "../repo";
import type { AppSettings, DownloadView, JobRow, JobType, SessionWithUserRow } from "../app-types";
import { manifestationDurationMs, preferredAudioManifestationsForBooks } from "../library/media";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { renderAdminOpsPageScript, renderAdminSettingsPageScript } from "./admin-page-client";
import { decodePlexTokenExpiry } from "../plex";
import { formatProcessUptime, type BuildInfo } from "../build-info";
import { formatBookStatusLine, formatMinutes } from "./page-helpers";

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
  buildInfo?: BuildInfo | null;
  startTime?: number;
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
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .ops-kpis { display: grid; grid-template-columns: repeat(4, minmax(130px, 1fr)); gap: 10px; margin-top: 14px; }
    .ops-kpi { border: 1px solid var(--line-soft); border-radius: 8px; padding: 10px; background: var(--surface); }
    .ops-kpi strong { display: block; font-size: 22px; line-height: 1.1; }
    .ops-list { display: grid; gap: 0; }
    .ops-item { padding: 10px 0; border-top: 1px solid var(--line-soft); }
    .ops-item:first-child { border-top: 0; padding-top: 0; }
    .ops-item strong { display: block; }
    details.ops-details { margin-top: 10px; }
    details.ops-details summary { cursor: pointer; color: var(--accent); }
    .settings-actions { display: flex; gap: 12px; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; }
    .settings-actions-left { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .settings-editor { width: 100%; min-height: 620px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
    .admin-grid input, .admin-grid select { min-width: 180px; }
    .build-info { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin: -4px 0 16px; color: var(--muted); font-size: 12px; }
    .build-info code { color: inherit; }
    .status-cell { max-width: 360px; overflow-wrap: anywhere; }
    pre { margin: 0; padding: 8px; border: 1px solid var(--line-soft); border-radius: 12px; background: var(--surface); overflow: auto; }
    @media (max-width: 900px) { .admin-grid .span-4, .admin-grid .span-6, .admin-grid .span-8, .ops-kpis { grid-column: span 12; grid-template-columns: 1fr 1fr; } }
  </style>`;
}

function adminTitle(title: string): string {
  return `<div class="section-title-row"><h2>${escapeHtml(title)}</h2><span class="admin-only-pill">Admin only</span></div>`;
}

const OPS_JOB_TYPES: JobType[] = ["full_library_refresh", "acquire", "download", "import", "reconcile", "chapter_analysis"];

function jobTarget(job: JobRow): string {
  return [job.book_id ? `book ${job.book_id}` : null, job.release_id ? `release ${job.release_id}` : null].filter(Boolean).join(" / ") || "no target";
}

function renderOpsList(items: string[], empty: string): string {
  if (items.length === 0) return `<div class="empty">${escapeHtml(empty)}</div>`;
  return `<div class="ops-list">${items.join("")}</div>`;
}

function renderOpsItem(title: string, detail: string, href?: string | null): string {
  const titleMarkup = href ? `<a href="${escapeHtml(href)}">${escapeHtml(title)}</a>` : escapeHtml(title);
  return `<div class="ops-item"><strong>${titleMarkup}</strong><div class="muted">${escapeHtml(detail)}</div></div>`;
}

function renderJobOpsItem(job: JobRow): string {
  const detail = [
    job.status,
    jobTarget(job),
    `attempts ${job.attempt_count}/${job.max_attempts}`,
    job.error ? `error: ${job.error}` : null,
  ]
    .filter(Boolean)
    .join(" - ");
  return renderOpsItem(`${job.type} job ${job.id}`, detail, job.book_id ? `/book/${job.book_id}` : null);
}

function renderDownloadOpsItem(download: DownloadView): string {
  const status = download.release_status || download.job_status || "unknown";
  const detail = [
    status,
    download.media_type || null,
    download.book_id ? `book ${download.book_id}` : null,
    download.release_error || download.job_error ? `error: ${download.release_error || download.job_error}` : null,
  ]
    .filter(Boolean)
    .join(" - ");
  return renderOpsItem(`download job ${download.job_id}`, detail, download.book_id ? `/book/${download.book_id}` : null);
}

function renderStatusKpi(label: string, value: number, detail: string): string {
  return `<div class="ops-kpi"><strong>${value}</strong><span>${escapeHtml(label)}</span><div class="muted">${escapeHtml(detail)}</div></div>`;
}

function renderStatusCounts(label: string, counts: Record<string, number>): string {
  const text = ["running", "queued", "failed", "succeeded"]
    .map((status) => `${status} ${counts[status] ?? 0}`)
    .join(" - ");
  return renderStatusKpi(label, Object.values(counts).reduce((sum, count) => sum + Number(count), 0), text);
}

function renderBuildInfo(buildInfo: BuildInfo | null | undefined, startTime: number | undefined): string {
  const sha = buildInfo?.sha ? buildInfo.sha.slice(0, 7) : "unknown";
  const message = buildInfo?.message ? ` - ${buildInfo.message}` : "";
  const uptimeSeconds = startTime ? (Date.now() - startTime) / 1000 : process.uptime();
  return `<div class="build-info">Commit: <code>${escapeHtml(sha)}</code>${escapeHtml(message)} · Uptime: ${escapeHtml(formatProcessUptime(uptimeSeconds))}</div>`;
}

function adminPage(
  title: string,
  body: string,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  apiKey: string | null,
  script = "",
  options: Pick<AdminPageOptions, "buildInfo" | "startTime"> = {}
): Response {
  return renderAppPage(
    title,
    `${adminPageStyles()}${renderBuildInfo(options.buildInfo, options.startTime)}${body}${script}`,
    settings,
    currentUser,
    "",
    apiKey
  );
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
          </div>
          <button id="wipe-db-btn" type="button" style="background: var(--danger); color: var(--danger-contrast); border: 1px solid var(--danger-border);">Wipe Entire Database</button>
        </div>
        <p id="settings-status" class="muted"></p>
        ${messageMarkup(options.notice, options.error)}
        <textarea id="settings-editor" class="settings-editor" spellcheck="false">${settingsJson}</textarea>
      </section>
    </div>`;
  return adminPage("Admin Settings", body, settings, currentUser, apiKey, renderAdminSettingsPageScript(), options);
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
  return adminPage("Admin Users", body, settings, currentUser, apiKey, "", options);
}

export function renderAdminOpsPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  options: AdminPageOptions = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const inProgress = repo.listInProgressBooks().slice(0, 8);
  const recentBooks = repo.listAllBooks().filter((book) => book.status === "imported").slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 8);
  const playableByBookId = new Map(preferredAudioManifestationsForBooks(repo).map((entry) => [entry.book.id, entry]));
  const health = repo.getHealthSummary();
  const recentJobs = repo.listRecentJobs(40);
  const activeJobs = recentJobs.filter((job) => job.status === "queued" || job.status === "running");
  const failedJobs = recentJobs.filter((job) => job.status === "failed");
  const recentDownloads = repo.listDownloads().slice(0, 40);
  const activeDownloads = recentDownloads.filter((download) => download.job_status === "queued" || download.job_status === "running" || download.release_status === "downloading");
  const failedDownloads = recentDownloads.filter((download) => download.job_status === "failed" || download.release_status === "failed");
  const contentRows = repo.listAdminContentOps();
  const failedContentRows = contentRows.filter((row) => row.transcript_status === "failed" || row.chapter_status === "failed");
  const pendingContentRows = contentRows.filter((row) => row.transcript_status === "pending" || row.chapter_status === "pending");
  const failedContent = failedContentRows.length;
  const withChapters = contentRows.filter((row) => row.has_chapters === 1).length;
  const attentionItems = [
    ...failedJobs.slice(0, 5).map(renderJobOpsItem),
    ...failedDownloads.slice(0, 5).map(renderDownloadOpsItem),
    ...failedContentRows.slice(0, 5).map((row) =>
      renderOpsItem(
        row.title,
        [
          row.transcript_status === "failed" ? `transcript failed: ${row.transcript_error || "unknown"}` : null,
          row.chapter_status === "failed" ? `chapters failed: ${row.chapter_error || "unknown"}` : null,
          `edition ${row.manifestation_id}`,
        ]
          .filter(Boolean)
          .join(" - "),
        `/book/${row.book_id}`
      )
    ),
  ];
  const activeItems = [
    ...activeJobs.slice(0, 6).map(renderJobOpsItem),
    ...activeDownloads.slice(0, 4).map(renderDownloadOpsItem),
    ...pendingContentRows.slice(0, 4).map((row) =>
      renderOpsItem(
        row.title,
        [
          row.transcript_status === "pending" ? "transcript pending" : null,
          row.chapter_status === "pending" ? "chapter analysis pending" : null,
          `edition ${row.manifestation_id}`,
        ]
          .filter(Boolean)
          .join(" - "),
        `/book/${row.book_id}`
      )
    ),
  ];
  const recentReadyItems = recentBooks.map((book) => {
    const playable = playableByBookId.get(book.id);
    const audioDurationMs = playable ? manifestationDurationMs(playable.manifestation, playable.containers) : null;
    return renderOpsItem(book.title, `${book.author} - ready${audioDurationMs ? ` - ${formatMinutes(audioDurationMs)}` : ""}`, `/book/${book.id}`);
  });
  const contentSummaryItems = [
    renderOpsItem("Failed", `${failedContentRows.length} editions need recovery`),
    renderOpsItem("Pending", `${pendingContentRows.length} editions are queued or running`),
    renderOpsItem("Ready", `${withChapters} editions have chapters`),
  ];
  const body = `
    <section class="hero">
      <h1>Ops</h1>
      <p>Operational triage, recovery queues, downloads, and content pipeline state.</p>
      <div class="ops-kpis">
        ${renderStatusKpi("Need attention", needsAttention.length + failedJobs.length + failedDownloads.length + failedContent, `${needsAttention.length} books - ${failedJobs.length} jobs - ${failedDownloads.length} downloads - ${failedContent} content`)}
        ${renderStatusKpi("Active work", activeJobs.length + activeDownloads.length + pendingContentRows.length, `${activeJobs.length} jobs - ${activeDownloads.length} downloads - ${pendingContentRows.length} content`)}
        ${renderStatusCounts("Jobs", health.jobs)}
        ${renderStatusCounts("Releases", health.releases)}
      </div>
      <div class="actions" style="margin-top: 14px;">
        <form method="post" action="${escapeHtml(addApiKey("/admin/refresh", apiKey))}">
          <button type="submit">Refresh library</button>
        </form>
      </div>
      ${messageMarkup(options.notice, options.error)}
    </section>
    <div class="admin-grid">
      <section class="card span-6 admin-only-card">
        ${adminTitle("Triage")}
        ${renderOpsList(
          [
            ...needsAttention.map((book) => renderOpsItem(book.title, `${book.author} - ${formatBookStatusLine(book)}`, `/book/${book.id}`)),
            ...attentionItems,
          ].slice(0, 12),
          "Nothing needs attention right now."
        )}
      </section>
      <section class="card span-6 admin-only-card">
        ${adminTitle("Active Work")}
        ${renderOpsList(
          [
            ...inProgress.map((book) => renderOpsItem(book.title, `${book.author} - ${formatBookStatusLine(book)}`, `/book/${book.id}`)),
            ...activeItems,
          ].slice(0, 12),
          "No active work right now."
        )}
      </section>
      <section class="card span-6 admin-only-card">
        ${adminTitle("Recently Ready")}
        ${renderOpsList(recentReadyItems, "No recently finished books yet.")}
      </section>
      <section class="card span-6 admin-only-card">
        ${adminTitle("Content Pipeline")}
        <p class="muted">${contentRows.length} audio edition${contentRows.length === 1 ? "" : "s"} - ${withChapters} with chapters - ${failedContent} failed</p>
        ${renderOpsList(contentSummaryItems, "No content operations yet.")}
      </section>
      <section class="card span-12 admin-only-card">
        ${adminTitle("Raw Queues")}
        <details class="ops-details">
          <summary>Jobs table</summary>
          <div class="row" style="margin-top: 10px;">
            <label for="jobs-limit">Limit</label>
            <input id="jobs-limit" type="number" min="1" max="200" value="25" style="min-width: 90px;" />
            <label for="jobs-type">Type</label>
            <select id="jobs-type">
              <option value="">all</option>
              ${OPS_JOB_TYPES.map((type) => `<option value="${escapeHtml(type)}">${escapeHtml(type.replace(/_/g, " "))}</option>`).join("")}
            </select>
            <button id="jobs-refresh-btn" type="button">Refresh Jobs</button>
          </div>
          <p id="jobs-status" class="muted"></p>
          <div class="table-wrap"><table>
            <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Book</th><th>Release</th><th>Attempts</th><th>Updated</th><th>Action</th><th>Error</th></tr></thead>
            <tbody id="jobs-table-body"><tr><td colspan="9">Loading...</td></tr></tbody>
          </table></div>
        </details>
        <details class="ops-details">
          <summary>Downloads table</summary>
          <div class="row" style="margin-top: 10px;"><button id="downloads-refresh-btn" type="button">Refresh Downloads</button></div>
          <p id="downloads-status" class="muted"></p>
          <div class="table-wrap"><table>
            <thead><tr><th>Job</th><th>Release</th><th>Media</th><th>Status</th><th>Progress</th><th>Transfer</th><th>Error</th></tr></thead>
            <tbody id="downloads-table-body"><tr><td colspan="7">Loading...</td></tr></tbody>
          </table></div>
        </details>
      </section>
    </div>`;
  return adminPage("Admin Ops", body, settings, currentUser, apiKey, renderAdminOpsPageScript(), options);
}

export function renderAdminDbPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null,
  input: AdminPageOptions & { table?: string | null; limit?: number; offset?: number } = {}
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
  return adminPage("Admin DB Explorer", body, settings, currentUser, apiKey, "", input);
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
  return adminPage("Admin Curation", body, settings, currentUser, apiKey, "", options);
}

function formatDbValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Uint8Array) return `[${value.byteLength} bytes]`;
  if (typeof value === "object") return JSON.stringify(value);
  const text = String(value);
  return text.length > 500 ? `${text.slice(0, 500)}...` : text;
}
