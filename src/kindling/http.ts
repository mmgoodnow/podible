import { promises as fs } from "node:fs";

import { buildJsonFeed, buildRssFeed } from "./feed";
import { authorizeRequest } from "./auth";
import { buildChapters, preferredAudioForBooks, streamAudioAsset, streamExtension } from "./media";
import { KindlingRepo } from "./repo";
import { handleRpcMethod, handleRpcRequest } from "./rpc";
import type { AppSettings } from "./types";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
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

function renderHomePage(repo: KindlingRepo, settings: AppSettings): Response {
  const health = repo.getHealthSummary();
  const books = repo.listBooks(30).items;
  const apiKey = settings.auth.mode === "apikey" ? settings.auth.key : null;
  const settingsJson = escapeHtml(JSON.stringify(settings, null, 2));
  const rpcExample = escapeHtml(
    JSON.stringify(
      {
        jsonrpc: "2.0",
        id: 1,
        method: "settings.get",
        params: {},
      },
      null,
      2
    )
  );
  const links = [
    "/",
    "/rpc/system/health",
    "/rpc/settings/get",
    "/rpc/jobs/list?limit=20",
    "/assets?bookId=1",
    "/feed.xml",
    "/feed.json",
  ];

  const linkItems = links
    .map((path) => `<li><a href="${escapeHtml(addApiKey(path, apiKey))}">${escapeHtml(path)}</a></li>`)
    .join("");

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
    <button type="button" class="agent-acquire-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Agent Acquire</button>
    <button type="button" class="delete-book-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Delete</button>
  </td>
</tr>`;
    })
    .join("");

  const body = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Podible</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; margin: 24px; }
      h1, h2 { margin: 0 0 12px; }
      .muted { color: #555; }
      ul { margin-top: 8px; }
      .panel { border: 1px solid #ddd; border-radius: 8px; padding: 12px; margin: 12px 0 18px; }
      .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
      input, button, textarea, select { font: inherit; }
      input { padding: 6px 8px; min-width: 220px; }
      button { padding: 6px 10px; cursor: pointer; }
      textarea { width: 100%; min-height: 200px; padding: 8px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
      table { border-collapse: collapse; width: 100%; margin-top: 8px; }
      th, td { border: 1px solid #ddd; padding: 8px; text-align: left; font-size: 14px; }
      th { background: #f6f6f6; }
      code { background: #f4f4f4; padding: 2px 4px; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>Podible Backend</h1>
    <p class="muted">Auth mode: <strong>${escapeHtml(settings.auth.mode)}</strong>${apiKey ? ` | Authorized links include <code>api_key</code>` : ""}</p>
    <h2>Health</h2>
    <p>Queue: <strong>${health.queueSize}</strong> | Jobs: <code>${escapeHtml(JSON.stringify(health.jobs))}</code> | Releases: <code>${escapeHtml(JSON.stringify(health.releases))}</code></p>
    <h2>Quick Links</h2>
    <ul>${linkItems}</ul>
    <p class="muted">Control/data API uses <code>POST /rpc</code>; read-only convenience links are available at <code>GET /rpc/&lt;ns&gt;/&lt;method&gt;</code>.</p>
    <pre><code>${rpcExample}</code></pre>
    <h2>Open Library Search</h2>
    <div class="panel">
      <div class="row">
        <input id="ol-query" type="text" placeholder="Title Author (e.g. Hyperion Dan Simmons)" />
        <button id="ol-search-btn" type="button">Search</button>
      </div>
      <p id="ol-status" class="muted"></p>
      <ul id="ol-results"></ul>
    </div>
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
    <h2>Recent Library</h2>
    <p id="library-status" class="muted"></p>
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
    <h2>Recent Downloads</h2>
    <div class="panel">
      <div class="row">
        <button id="downloads-refresh-btn" type="button">Refresh Downloads</button>
      </div>
      <p id="downloads-status" class="muted"></p>
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
    <h2>Settings JSON</h2>
    <div class="panel">
      <div class="row">
        <button id="settings-save-btn" type="button">Save Settings</button>
      </div>
      <p id="settings-status" class="muted"></p>
      <textarea id="settings-editor" spellcheck="false">${settingsJson}</textarea>
    </div>
    <h2>Recent Jobs</h2>
    <div class="panel">
      <div class="row">
        <label for="jobs-limit">Limit</label>
        <input id="jobs-limit" type="number" min="1" max="200" value="25" style="min-width: 90px;" />
        <label for="jobs-type">Type</label>
        <select id="jobs-type">
          <option value="">all</option>
          <option value="scan">scan</option>
          <option value="download">download</option>
          <option value="import">import</option>
          <option value="transcode">transcode</option>
          <option value="reconcile">reconcile</option>
        </select>
        <button id="jobs-refresh-btn" type="button">Refresh Jobs</button>
      </div>
      <p id="jobs-status" class="muted"></p>
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
            <th>Error</th>
          </tr>
        </thead>
        <tbody id="jobs-table-body"><tr><td colspan="8">Loading...</td></tr></tbody>
      </table>
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

            downloadsCell(row, download.release_id == null ? "" : String(download.release_id));
            downloadsCell(row, String(download.media_type || ""));
            downloadsCell(row, String(download.release_status || download.job_status || ""));
            downloadsCell(row, String(download.fullPseudoProgress ?? 0) + "%");

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
            downloadsCell(row, String(download.job_error || ""));
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
            emptyCell.colSpan = 8;
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
            jobsCell(row, job.book_id == null ? "" : String(job.book_id));
            jobsCell(row, job.release_id == null ? "" : String(job.release_id));
            jobsCell(row, String(job.attempt_count || 0) + "/" + String(job.max_attempts || 0));
            jobsCell(row, String(job.updated_at || ""));
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

export function createPodibleFetchHandler(repo: KindlingRepo, startTime: number): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    const settings = repo.getSettings();
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (!authorizeRequest(request, settings)) {
      return new Response("Unauthorized", {
        status: 401,
        headers: { "WWW-Authenticate": 'Bearer realm="podible"' },
      });
    }

    try {
      if (pathname === "/" && request.method === "GET") {
        return renderHomePage(repo, settings);
      }

      if (pathname === "/rpc" && request.method === "POST") {
        return handleRpcRequest(request, { repo, startTime });
      }

      if (pathname.startsWith("/rpc/") && request.method === "GET") {
        const parts = pathname
          .slice("/rpc/".length)
          .split("/")
          .filter(Boolean);
        if (parts.length !== 2) {
          return new Response("Not found", { status: 404 });
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
        return handleRpcMethod(parts.join("."), params, { repo, startTime }, { id: null, readOnly: true });
      }

      if (pathname === "/assets" && request.method === "GET") {
        const bookId = parseId(url.searchParams.get("bookId") ?? "");
        const assets = repo.listAssetsByBook(bookId).map((asset) => ({
          ...asset,
          files: repo.getAssetFiles(asset.id),
          stream_ext: streamExtension(asset),
        }));
        return json({ assets });
      }

      if (pathname.startsWith("/stream/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.split(".")[0] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target) return new Response("Not found", { status: 404 });
        const book = repo.getBookByAsset(assetId);
        return streamAudioAsset(request, target.asset, target.files, book?.cover_path);
      }

      if (pathname.startsWith("/chapters/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const assetId = parseId(idPart.replace(/\.json$/i, ""));
        const target = repo.getAssetWithFiles(assetId);
        if (!target) return json({ error: "not_found" }, 404);
        const chapters = await buildChapters(target.asset, target.files);
        if (!chapters) return json({ error: "not_found" }, 404);
        return new Response(JSON.stringify(chapters, null, 2), {
          headers: { "Content-Type": "application/json+chapters" },
        });
      }

      if (pathname.startsWith("/covers/") && request.method === "GET") {
        const idPart = pathname.split("/")[2] ?? "";
        const bookId = parseId(idPart.replace(/\.jpg$/i, ""));
        const book = repo.getBookRow(bookId);
        if (!book?.cover_path) return new Response("Not found", { status: 404 });
        const file = Bun.file(book.cover_path);
        if (!(await file.exists())) return new Response("Not found", { status: 404 });
        return new Response(file, {
          headers: {
            "Content-Type": book.cover_path.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
          },
        });
      }

      if (pathname === "/feed.xml" && request.method === "GET") {
        return buildRssFeed(request, repo, settings.feed.title, settings.feed.author);
      }

      if (pathname === "/feed.json" && request.method === "GET") {
        return buildJsonFeed(request, repo, settings.feed.title, settings.feed.author);
      }

      if (pathname.startsWith("/ebook/") && request.method === "GET") {
        const assetId = parseId(pathname.split("/")[2] ?? "");
        const target = repo.getAssetWithFiles(assetId);
        if (!target || target.asset.kind !== "ebook") return new Response("Not found", { status: 404 });
        const first = target.files[0];
        if (!first) return new Response("Not found", { status: 404 });
        const file = Bun.file(first.path);
        if (!(await file.exists())) return new Response("Not found", { status: 404 });
        return new Response(file, {
          headers: {
            "Content-Type": target.asset.mime,
            "Content-Disposition": `attachment; filename="${first.path.split("/").pop() ?? `book-${assetId}`}"`,
          },
        });
      }

      return new Response("Not found", { status: 404 });
    } catch (error) {
      return json({ error: (error as Error).message }, 400);
    }
  };
}
