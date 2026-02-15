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
  <td><button type="button" class="delete-book-btn" data-book-id="${book.id}" data-book-title="${escapeHtml(book.title)}">Delete</button></td>
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
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="library-table-body">${rows || '<tr><td colspan="7">No books yet.</td></tr>'}</tbody>
    </table>
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
        if (searchBtn && queryInput && resultList) {
          searchBtn.addEventListener("click", async function () {
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
          });
        }

        var settingsEditor = document.getElementById("settings-editor");
        var settingsSaveBtn = document.getElementById("settings-save-btn");
        var jobsTableBody = document.getElementById("jobs-table-body");
        var jobsRefreshBtn = document.getElementById("jobs-refresh-btn");
        var jobsLimitInput = document.getElementById("jobs-limit");
        var jobsTypeInput = document.getElementById("jobs-type");
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
          cell.colSpan = 7;
          cell.textContent = "No books yet.";
          row.appendChild(cell);
          libraryTableBody.appendChild(row);
        }

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
