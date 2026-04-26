import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { renderAdminPageScript } from "./admin-page-client";
import { decodePlexTokenExpiry } from "../plex";

function plexOwnerTokenStatus(token: string): string {
  if (!token) return "missing";
  const expiry = decodePlexTokenExpiry(token);
  if (expiry.expSeconds === null) return "captured";
  if (expiry.expired) return "EXPIRED — log out and sign in again as a Plex admin to re-link";
  const days = Math.floor((expiry.expiresInMs ?? 0) / 86_400_000);
  const hours = Math.floor(((expiry.expiresInMs ?? 0) % 86_400_000) / 3_600_000);
  if (days > 0) return `captured (expires in ${days}d ${hours}h)`;
  if (hours > 0) return `captured (expires in ${hours}h — re-link soon)`;
  const minutes = Math.max(0, Math.floor((expiry.expiresInMs ?? 0) / 60_000));
  return `captured (expires in ${minutes}m — re-link now)`;
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
  const selectedPlexServer =
    plexServers.find((server) => server.machineId === settings.auth.plex.machineId) ?? null;
  const plexServerOptions = plexServers
    .map((server) => {
      const label = `${server.name} (${server.owned ? "owned" : "shared"}${server.sourceTitle ? ` • ${server.sourceTitle}` : ""})`;
      return `<option value="${escapeHtml(server.machineId)}"${
        server.machineId === settings.auth.plex.machineId ? " selected" : ""
      }>${escapeHtml(label)}</option>`;
    })
    .join("");

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
      .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 14px;
        align-items: start;
      }
      .admin-top-grid {
        grid-column: span 12;
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 14px;
        align-items: start;
      }
      .admin-top-grid > * { min-width: 0; }
      .admin-stack {
        grid-column: span 6;
        display: grid;
        gap: 14px;
        align-content: start;
        min-width: 0;
      }
      .admin-stack > * { min-width: 0; }
      .settings-card {
        grid-column: span 6;
        min-width: 0;
      }
      .card-full { grid-column: span 12; }
      .card-mid { grid-column: span 6; }
      .page-header h1 {
        margin: 0 0 8px;
        line-height: 1.05;
      }
      .page-header p { margin: 0; }
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
      .dashboard-grid input, .dashboard-grid select {
        min-width: 220px;
      }
      .dashboard-grid textarea {
        width: 100%;
        min-height: 220px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 13px;
      }
      .settings-editor {
        min-height: 620px;
      }
      .table-wrap {
        overflow: auto;
        min-width: 0;
        border: 1px solid var(--line-soft);
        border-radius: 12px;
        background: var(--surface);
      }
      table {
        border-collapse: collapse;
        width: 100%;
        margin-top: 0;
        min-width: 640px;
      }
      th, td { border: 1px solid var(--line-soft); padding: 6px 7px; text-align: left; font-size: 13px; vertical-align: top; }
      th { background: var(--accent-soft); }
      code { background: var(--code-bg); padding: 2px 4px; border-radius: 4px; }
      pre {
        margin: 0;
        padding: 8px;
        border: 1px solid var(--line-soft);
        border-radius: 12px;
        background: var(--surface);
        overflow: auto;
      }
      pre code { background: transparent; padding: 0; }
      .card h2 + .panel, .card h2 + .table-wrap, .card h2 + p { margin-top: 0; }
      @media (max-width: 1200px) {
        .card-mid { grid-column: span 12; }
        .admin-stack, .settings-card { grid-column: span 12; }
      }
      @media (max-width: 900px) {
        .dashboard-grid { gap: 10px; }
        input, select { min-width: 150px; }
        table { min-width: 560px; }
      }
    </style>
      <div class="dashboard-grid">
        <div class="admin-top-grid">
          <div class="admin-stack">
            <section class="card page-header">
              <h1>Admin</h1>
              <p class="muted">Manage settings, users, library refreshes, and recovery tools.</p>
              <div class="stats">
                <span class="pill">${activeJobs} active jobs</span>
                <span class="pill">${failedJobs} failed jobs</span>
                <span class="pill">${releaseIssues} release issues</span>
              </div>
            </section>

            ${
              settings.auth.mode === "plex"
                ? `<section class="card">
          <h2>Plex Access Control</h2>
          <p class="muted">Choose which Plex server controls who can sign in to Podible. Future Plex logins will only be allowed for users who can access that server.</p>
          <p class="muted">Owner token: <strong>${escapeHtml(plexOwnerTokenStatus(settings.auth.plex.ownerToken))}</strong> | Selected server: <strong>${escapeHtml(selectedPlexServer?.name || settings.auth.plex.machineId || "not set")}</strong></p>
          ${messageMarkup(options.plexNotice, options.plexError)}
          ${
            plexServers.length > 0
              ? `<form method="post" action="${escapeHtml(addApiKey("/admin/plex/select", apiKey))}" class="panel">
          <div class="row">
            <select id="plex-server-select" name="machineId">
              ${plexServerOptions}
            </select>
            <button type="submit">Use selected server</button>
          </div>
        </form>`
              : `<p class="muted">No Plex servers were found for the current owner token.</p>`
          }
        </section>`
                : ""
            }

            <section class="card">
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
          </div>

          <section class="card settings-card">
            <h2>Settings JSON</h2>
            <div class="panel">
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
            </div>
          </section>
        </div>

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
            <div class="row" style="margin-top: 8px;">
              <input id="manual-manifestation-label" type="text" placeholder="Edition label (e.g. GraphicAudio dramatization)" />
              <button id="manual-group-snatch-btn" type="button">Snatch Checked as One Edition</button>
            </div>
            <p id="manual-search-status" class="muted"></p>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th></th>
                    <th>Title</th>
                    <th>Provider</th>
                    <th>Seeders</th>
                    <th>Size</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody id="manual-search-body"><tr><td colspan="6">No search yet.</td></tr></tbody>
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

        <section class="card card-full">
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
    ${renderAdminPageScript()}`;

  return renderAppPage("Admin", body, settings, currentUser, "", apiKey);
}

export { renderAdminPage };
