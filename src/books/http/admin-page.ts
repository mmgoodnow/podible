import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../types";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";

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
      .page-header {
        background:
          radial-gradient(circle at 90% 10%, rgba(40, 89, 67, 0.08), transparent 45%),
          linear-gradient(180deg, #fffdf7, #f7f5ed);
      }
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
          <p class="muted">Owner token: <strong>${settings.auth.plex.ownerToken ? "captured" : "missing"}</strong> | Selected server: <strong>${escapeHtml(selectedPlexServer?.name || settings.auth.plex.machineId || "not set")}</strong></p>
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
                <button id="wipe-db-btn" type="button" style="background: var(--danger); color: #fff; border: 1px solid var(--danger-border);">Wipe Entire Database</button>
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
    <script>
      (function () {
        function withAuth(path) {
          var url = new URL(path, window.location.origin);
          return url.pathname + url.search;
        }

        async function rpcCall(method, params) {
          const response = await fetch(withAuth("/rpc"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: method, params: params ?? {} }),
          });
          const payload = await response.json();
          if (!response.ok || payload.error) {
            throw new Error(payload.error?.message || response.statusText || "Request failed");
          }
          return payload.result;
        }

        function formatBytes(bytes) {
          if (typeof bytes !== "number" || !isFinite(bytes) || bytes < 0) return "";
          if (bytes < 1024) return bytes + " B";
          if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
          if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
          return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
        }

        function formatPercent(value) {
          if (typeof value !== "number" || !isFinite(value)) return "";
          return Math.max(0, Math.min(100, value)).toFixed(1) + "%";
        }

        function formatDate(value) {
          if (!value) return "";
          const date = new Date(value);
          if (isNaN(date.getTime())) return String(value);
          return date.toLocaleString();
        }

        async function refreshDownloads() {
          const status = document.getElementById("downloads-status");
          const body = document.getElementById("downloads-table-body");
          status.textContent = "Loading downloads…";
          try {
            const result = await rpcCall("downloads.list", { limit: 25 });
            const rows = result.downloads || [];
            if (rows.length === 0) {
              body.innerHTML = '<tr><td colspan="7">No downloads found.</td></tr>';
            } else {
              body.innerHTML = rows.map(function (download) {
                return '<tr>' +
                  '<td>' + download.jobId + '</td>' +
                  '<td>' + (download.releaseTitle || '') + '</td>' +
                  '<td>' + (download.media || '') + '</td>' +
                  '<td>' + (download.status || '') + '</td>' +
                  '<td>' + formatPercent(download.progressPercent) + '</td>' +
                  '<td>' + [formatBytes(download.downloadedBytes), download.transferRateBytesPerSecond ? formatBytes(download.transferRateBytesPerSecond) + '/s' : ''].filter(Boolean).join(' • ') + '</td>' +
                  '<td>' + (download.error || '') + '</td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' download' + (rows.length === 1 ? '' : 's') + ' loaded.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function refreshJobs() {
          const status = document.getElementById("jobs-status");
          const body = document.getElementById("jobs-table-body");
          const limit = Number(document.getElementById("jobs-limit").value || 25);
          const type = document.getElementById("jobs-type").value || undefined;
          status.textContent = "Loading jobs…";
          try {
            const result = await rpcCall("jobs.list", { limit: limit, type: type });
            const rows = result.jobs || [];
            if (rows.length === 0) {
              body.innerHTML = '<tr><td colspan="9">No jobs found.</td></tr>';
            } else {
              body.innerHTML = rows.map(function (job) {
                var retryBtn = '';
                if (job.status === 'failed') {
                  retryBtn = '<button type="button" data-job-retry="' + job.id + '">Retry</button>';
                }
                return '<tr>' +
                  '<td>' + job.id + '</td>' +
                  '<td>' + (job.type || '') + '</td>' +
                  '<td>' + (job.status || '') + '</td>' +
                  '<td>' + (job.book_id || '') + '</td>' +
                  '<td>' + (job.release_id || '') + '</td>' +
                  '<td>' + (job.attempt_count || 0) + '</td>' +
                  '<td>' + formatDate(job.updated_at) + '</td>' +
                  '<td>' + retryBtn + '</td>' +
                  '<td>' + (job.error || '') + '</td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' job' + (rows.length === 1 ? '' : 's') + ' loaded.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function runManualSearch() {
          const status = document.getElementById("manual-search-status");
          const body = document.getElementById("manual-search-body");
          const bookId = Number(document.getElementById("manual-book-id").value || 0);
          const media = document.getElementById("manual-media").value;
          const query = document.getElementById("manual-query").value.trim();
          if (!bookId || !query) {
            status.textContent = "Book ID and query are required.";
            return;
          }
          status.textContent = "Searching…";
          try {
            const result = await rpcCall("search.run", { bookId: bookId, media: media, query: query });
            const rows = result.releases || [];
            if (rows.length === 0) {
              body.innerHTML = '<tr><td colspan="5">No results.</td></tr>';
            } else {
              body.innerHTML = rows.map(function (release) {
                var btn = document.createElement("button");
                btn.type = "button";
                btn.textContent = "Snatch";
                btn.setAttribute("data-snatch-info-hash", release.infoHash);
                btn.setAttribute("data-snatch-book-id", String(bookId));
                btn.setAttribute("data-snatch-media", media);
                return '<tr>' +
                  '<td>' + (release.title || '') + '</td>' +
                  '<td>' + (release.provider || '') + '</td>' +
                  '<td>' + (release.seeders ?? '') + '</td>' +
                  '<td>' + formatBytes(release.sizeBytes) + '</td>' +
                  '<td><button type="button" data-snatch-info-hash="' + release.infoHash + '" data-snatch-book-id="' + bookId + '" data-snatch-media="' + media + '">Snatch</button></td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' result' + (rows.length === 1 ? '' : 's') + '.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function inspectManualImport() {
          const status = document.getElementById("manual-import-status");
          const body = document.getElementById("manual-import-files-body");
          const path = document.getElementById("manual-import-path").value.trim();
          const media = document.getElementById("manual-import-media").value;
          if (!path) {
            status.textContent = "Path is required.";
            return;
          }
          status.textContent = "Inspecting…";
          try {
            const result = await rpcCall("import.inspect", { path: path, media: media });
            const files = result.files || [];
            if (files.length === 0) {
              body.innerHTML = '<tr><td colspan="4">No files found.</td></tr>';
            } else {
              body.innerHTML = files.map(function (file) {
                return '<tr>' +
                  '<td><input type="checkbox" data-import-path="' + file.path.replace(/"/g, '&quot;') + '"' + (file.selected ? ' checked' : '') + ' /></td>' +
                  '<td>' + file.path + '</td>' +
                  '<td>' + (file.kind || '') + '</td>' +
                  '<td>' + formatBytes(file.sizeBytes) + '</td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = files.length + ' file' + (files.length === 1 ? '' : 's') + ' found.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function runManualImport() {
          const status = document.getElementById("manual-import-status");
          const bookId = Number(document.getElementById("manual-import-book-id").value || 0);
          const media = document.getElementById("manual-import-media").value;
          const path = document.getElementById("manual-import-path").value.trim();
          const selectedPaths = Array.from(document.querySelectorAll("[data-import-path]:checked")).map(function (input) {
            return input.getAttribute("data-import-path");
          }).filter(Boolean);
          if (!bookId || !path) {
            status.textContent = "Book ID and path are required.";
            return;
          }
          status.textContent = "Importing…";
          try {
            await rpcCall("import.manual", { bookId: bookId, media: media, path: path, selectedPaths: selectedPaths });
            status.textContent = "Import queued.";
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function saveSettings() {
          const status = document.getElementById("settings-status");
          const editor = document.getElementById("settings-editor");
          status.textContent = "Saving…";
          try {
            const nextSettings = JSON.parse(editor.value);
            await rpcCall("settings.update", { settings: nextSettings });
            status.textContent = "Saved.";
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function wipeDatabase() {
          if (!window.confirm("Wipe the entire database? This cannot be undone.")) return;
          const status = document.getElementById("settings-status");
          status.textContent = "Wiping database…";
          try {
            await rpcCall("admin.wipeDatabase", {});
            window.location.href = withAuth("/admin");
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function retryJob(jobId) {
          const status = document.getElementById("jobs-status");
          status.textContent = "Retrying job " + jobId + "…";
          try {
            await rpcCall("jobs.retry", { jobId: jobId });
            await refreshJobs();
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function snatchRelease(bookId, media, infoHash) {
          const status = document.getElementById("manual-search-status");
          status.textContent = "Snatching…";
          try {
            await rpcCall("snatch.create", { bookId: Number(bookId), media: media, infoHash: infoHash });
            status.textContent = "Snatch created.";
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        document.getElementById("settings-save-btn")?.addEventListener("click", saveSettings);
        document.getElementById("wipe-db-btn")?.addEventListener("click", wipeDatabase);
        document.getElementById("manual-search-btn")?.addEventListener("click", runManualSearch);
        document.getElementById("manual-import-inspect-btn")?.addEventListener("click", inspectManualImport);
        document.getElementById("manual-import-btn")?.addEventListener("click", runManualImport);
        document.getElementById("downloads-refresh-btn")?.addEventListener("click", refreshDownloads);
        document.getElementById("jobs-refresh-btn")?.addEventListener("click", refreshJobs);
        document.getElementById("jobs-table-body")?.addEventListener("click", function (event) {
          const button = event.target.closest("[data-job-retry]");
          if (button) retryJob(Number(button.getAttribute("data-job-retry")));
        });
        document.getElementById("manual-search-body")?.addEventListener("click", function (event) {
          const button = event.target.closest("[data-snatch-info-hash]");
          if (button) {
            snatchRelease(
              button.getAttribute("data-snatch-book-id"),
              button.getAttribute("data-snatch-media"),
              button.getAttribute("data-snatch-info-hash")
            );
          }
        });

        refreshDownloads();
        refreshJobs();
      })();
    </script>`;

  return renderAppPage("Admin", body, settings, currentUser, "", apiKey);
}

export { renderAdminPage };
