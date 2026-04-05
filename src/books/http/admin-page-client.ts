function renderAdminRuntimeScript(): string {
  return `
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

        function escapeHtml(value) {
          return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
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
  `;
}

function renderSettingsScript(): string {
  return `
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
  `;
}

function renderDownloadsScript(): string {
  return `
        async function refreshDownloads() {
          const status = document.getElementById("downloads-status");
          const body = document.getElementById("downloads-table-body");
          status.textContent = "Loading downloads…";
          try {
            const result = await rpcCall("downloads.list", {});
            const rows = result.downloads || [];
            if (rows.length === 0) {
              body.innerHTML = '<tr><td colspan="7">No downloads found.</td></tr>';
            } else {
              body.innerHTML = rows.map(function (download) {
                const progressPercent = download.downloadProgress?.percent ?? (typeof download.fullPseudoProgress === "number" ? download.fullPseudoProgress * 100 : null);
                const transferBits = [];
                if (typeof download.downloadProgress?.bytesDone === "number") {
                  transferBits.push(formatBytes(download.downloadProgress.bytesDone));
                }
                if (typeof download.downloadProgress?.downRate === "number") {
                  transferBits.push(formatBytes(download.downloadProgress.downRate) + '/s');
                }
                return '<tr>' +
                  '<td>' + escapeHtml(download.job_id) + '</td>' +
                  '<td>' + escapeHtml(download.release_id) + '</td>' +
                  '<td>' + escapeHtml(download.media_type || '') + '</td>' +
                  '<td>' + escapeHtml(download.release_status || download.job_status || '') + '</td>' +
                  '<td>' + escapeHtml(formatPercent(progressPercent)) + '</td>' +
                  '<td>' + escapeHtml(transferBits.join(' • ')) + '</td>' +
                  '<td>' + escapeHtml(download.release_error || download.job_error || '') + '</td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' download' + (rows.length === 1 ? '' : 's') + ' loaded.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }
  `;
}

function renderJobsScript(): string {
  return `
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
                  retryBtn = '<button type="button" data-job-retry="' + escapeHtml(job.id) + '">Retry</button>';
                }
                return '<tr>' +
                  '<td>' + escapeHtml(job.id) + '</td>' +
                  '<td>' + escapeHtml(job.type || '') + '</td>' +
                  '<td>' + escapeHtml(job.status || '') + '</td>' +
                  '<td>' + escapeHtml(job.book_title || job.book_id || '') + '</td>' +
                  '<td>' + escapeHtml(job.release_id || '') + '</td>' +
                  '<td>' + escapeHtml(job.attempt_count || 0) + '</td>' +
                  '<td>' + escapeHtml(formatDate(job.updated_at)) + '</td>' +
                  '<td>' + retryBtn + '</td>' +
                  '<td>' + escapeHtml(job.error || '') + '</td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' job' + (rows.length === 1 ? '' : 's') + ' loaded.';
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
  `;
}

function renderManualSearchScript(): string {
  return `
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
            const result = await rpcCall("search.run", { media: media, query: query });
            const rows = result.results || [];
            if (rows.length === 0) {
              body.innerHTML = '<tr><td colspan="5">No results.</td></tr>';
            } else {
              body.innerHTML = rows.map(function (release) {
                return '<tr>' +
                  '<td>' + escapeHtml(release.title || '') + '</td>' +
                  '<td>' + escapeHtml(release.provider || '') + '</td>' +
                  '<td>' + escapeHtml(release.seeders ?? '') + '</td>' +
                  '<td>' + escapeHtml(formatBytes(release.sizeBytes)) + '</td>' +
                  '<td><button type="button" data-snatch-book-id="' + escapeHtml(bookId) + '" data-snatch-provider="' + escapeHtml(release.provider || '') + '" data-snatch-title="' + escapeHtml(release.title || '') + '" data-snatch-media-type="' + escapeHtml(media) + '" data-snatch-url="' + escapeHtml(release.url || '') + '" data-snatch-guid="' + escapeHtml(release.guid || '') + '" data-snatch-info-hash="' + escapeHtml(release.infoHash || '') + '" data-snatch-size-bytes="' + escapeHtml(release.sizeBytes ?? '') + '">Snatch</button></td>' +
                '</tr>';
              }).join('');
            }
            status.textContent = rows.length + ' result' + (rows.length === 1 ? '' : 's') + '.';
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }

        async function snatchRelease(button) {
          const status = document.getElementById("manual-search-status");
          status.textContent = "Snatching…";
          try {
            await rpcCall("snatch.create", {
              bookId: Number(button.getAttribute("data-snatch-book-id")),
              provider: button.getAttribute("data-snatch-provider"),
              title: button.getAttribute("data-snatch-title"),
              mediaType: button.getAttribute("data-snatch-media-type"),
              url: button.getAttribute("data-snatch-url"),
              guid: button.getAttribute("data-snatch-guid") || undefined,
              infoHash: button.getAttribute("data-snatch-info-hash") || undefined,
              sizeBytes: button.getAttribute("data-snatch-size-bytes") || undefined,
            });
            status.textContent = "Snatch created.";
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }
  `;
}

function renderManualImportScript(): string {
  return `
        async function inspectManualImport() {
          const status = document.getElementById("manual-import-status");
          const body = document.getElementById("manual-import-files-body");
          const path = document.getElementById("manual-import-path").value.trim();
          if (!path) {
            status.textContent = "Path is required.";
            return;
          }
          status.textContent = "Inspecting…";
          try {
            const result = await rpcCall("import.inspect", { path: path });
            const files = result.files || [];
            if (files.length === 0) {
              body.innerHTML = '<tr><td colspan="4">No files found.</td></tr>';
            } else {
              body.innerHTML = files.map(function (file) {
                return '<tr>' +
                  '<td><input type="checkbox" data-import-path="' + escapeHtml(file.path) + '" checked /></td>' +
                  '<td>' + escapeHtml(file.path) + '</td>' +
                  '<td>' + escapeHtml(file.kind || '') + '</td>' +
                  '<td>' + escapeHtml(formatBytes(file.sizeBytes)) + '</td>' +
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
          const mediaType = document.getElementById("manual-import-media").value;
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
            await rpcCall("import.manual", { bookId: bookId, mediaType: mediaType, path: path, selectedPaths: selectedPaths });
            status.textContent = "Import queued.";
          } catch (error) {
            status.textContent = error instanceof Error ? error.message : String(error);
          }
        }
  `;
}

function renderAdminBootstrapScript(): string {
  return `
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
          const button = event.target.closest("[data-snatch-book-id]");
          if (button) {
            snatchRelease(button);
          }
        });

        refreshDownloads();
        refreshJobs();
  `;
}

export function renderAdminPageScript(): string {
  return `<script>
      (function () {
${renderAdminRuntimeScript()}
${renderSettingsScript()}
${renderDownloadsScript()}
${renderJobsScript()}
${renderManualSearchScript()}
${renderManualImportScript()}
${renderAdminBootstrapScript()}
      })();
    </script>`;
}
