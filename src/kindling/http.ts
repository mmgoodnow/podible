import { promises as fs } from "node:fs";

import { buildJsonFeed, buildRssFeed } from "./feed";
import { authorizeRequest } from "./auth";
import { buildChapters, preferredAudioForBooks, streamAudioAsset, streamExtension } from "./media";
import { KindlingRepo } from "./repo";
import { fetchOpenLibraryMetadata, resolveOpenLibraryCandidate, searchOpenLibrary } from "./openlibrary";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import type { AppSettings, MediaType } from "./types";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

async function readJson<T>(request: Request): Promise<T> {
  const body = await request.text();
  if (!body.trim()) {
    throw new Error("JSON body is required");
  }
  return JSON.parse(body) as T;
}

function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid id");
  }
  return parsed;
}

function parseLimit(value: string | null): number {
  if (!value) return 50;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) return 50;
  return Math.min(parsed, 200);
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
  const links = [
    "/health",
    "/server",
    "/settings",
    "/library",
    "/downloads",
    "/feed.xml",
    "/feed.json",
  ];

  const linkItems = links
    .map((path) => `<li><a href="${escapeHtml(addApiKey(path, apiKey))}">${escapeHtml(path)}</a></li>`)
    .join("");

  const rows = books
    .map((book) => {
      const detailPath = addApiKey(`/library/${book.id}`, apiKey);
      return `<tr>
  <td><a href="${escapeHtml(detailPath)}">${book.id}</a></td>
  <td>${escapeHtml(book.title)}</td>
  <td>${escapeHtml(book.author)}</td>
  <td>${escapeHtml(book.status)}</td>
  <td>${escapeHtml(book.audioStatus)}</td>
  <td>${escapeHtml(book.ebookStatus)}</td>
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
      input, button, textarea { font: inherit; }
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
    <h2>Open Library Search</h2>
    <div class="panel">
      <div class="row">
        <input id="ol-query" type="text" placeholder="Title Author (e.g. Hyperion Dan Simmons)" />
        <button id="ol-search-btn" type="button">Search</button>
      </div>
      <p id="ol-status" class="muted"></p>
      <ul id="ol-results"></ul>
    </div>
    <h2>Add By ISBN</h2>
    <div class="panel">
      <div class="row">
        <input id="isbn-input" type="text" placeholder="9780553283686" />
        <button id="isbn-add-btn" type="button">Add Book</button>
      </div>
      <p id="isbn-status" class="muted"></p>
    </div>
    <h2>Settings JSON</h2>
    <div class="panel">
      <div class="row">
        <button id="settings-save-btn" type="button">Save Settings</button>
      </div>
      <p id="settings-status" class="muted"></p>
      <textarea id="settings-editor" spellcheck="false">${settingsJson}</textarea>
    </div>
    <h2>Recent Library</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Title</th>
          <th>Author</th>
          <th>Status</th>
          <th>Audio</th>
          <th>Ebook</th>
        </tr>
      </thead>
      <tbody>${rows || '<tr><td colspan="6">No books yet.</td></tr>'}</tbody>
    </table>
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
            var res = await api("/openlibrary/search?q=" + encodeURIComponent(q) + "&limit=10");
            if (!res.ok) {
              text("ol-status", "Search failed: " + res.status);
              resultList.innerHTML = "";
              return;
            }
            var payload = await res.json();
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
                var createRes = await api("/library", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ openLibraryKey: item.openLibraryKey }),
                });
                if (!createRes.ok) {
                  var err = await createRes.text();
                  text("ol-status", "Add failed: " + createRes.status + " " + err);
                  return;
                }
                var created = await createRes.json();
                text("ol-status", 'Added "' + created.book.title + '" (id ' + created.book.id + '). Refresh to see it below.');
              });
              li.appendChild(label);
              li.appendChild(btn);
              resultList.appendChild(li);
            });
          });
        }

        var isbnInput = document.getElementById("isbn-input");
        var isbnAddBtn = document.getElementById("isbn-add-btn");
        if (isbnInput && isbnAddBtn) {
          isbnAddBtn.addEventListener("click", async function () {
            var isbn = (isbnInput.value || "").trim();
            if (!isbn) {
              text("isbn-status", "Enter an ISBN.");
              return;
            }
            text("isbn-status", "Adding ISBN " + isbn + "...");
            var res = await api("/library", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ isbn: isbn }),
            });
            if (!res.ok) {
              var errText = await res.text();
              text("isbn-status", "Add failed: " + res.status + " " + errText);
              return;
            }
            var created = await res.json();
            text("isbn-status", 'Added "' + created.book.title + '" (id ' + created.book.id + ').');
          });
        }

        var settingsEditor = document.getElementById("settings-editor");
        var settingsSaveBtn = document.getElementById("settings-save-btn");
        async function loadSettings() {
          try {
            text("settings-status", "Loading...");
            var res = await api("/settings");
            if (!res.ok) {
              text("settings-status", "Load failed: " + res.status);
              return;
            }
            var payload = await res.json();
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
            var res = await api("/settings", {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(parsed),
            });
            if (!res.ok) {
              var errText = await res.text();
              text("settings-status", "Save failed: " + res.status + " " + errText);
              return;
            }
            var payload = await res.json();
            settingsEditor.value = JSON.stringify(payload, null, 2);
            text("settings-status", "Saved.");
          });
          loadSettings();
        }
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

      if (pathname === "/health" && request.method === "GET") {
        return json({
          ok: true,
          ...repo.getHealthSummary(),
        });
      }

      if (pathname === "/server" && request.method === "GET") {
        return json({
          name: "podible-backend",
          runtime: "bun",
          uptimeMs: Date.now() - startTime,
          now: new Date().toISOString(),
        });
      }

      if (pathname === "/settings" && request.method === "GET") {
        return json(repo.getSettings());
      }

      if (pathname === "/settings" && request.method === "PUT") {
        const payload = await readJson<AppSettings>(request);
        return json(repo.updateSettings(payload));
      }

      if (pathname === "/library" && request.method === "GET") {
        const limit = parseLimit(url.searchParams.get("limit"));
        const cursorParam = url.searchParams.get("cursor");
        const cursor = cursorParam ? parseId(cursorParam) : undefined;
        const q = url.searchParams.get("q") ?? undefined;
        const result = repo.listBooks(limit, cursor, q);
        return json(result);
      }

      if (pathname === "/openlibrary/search" && request.method === "GET") {
        const q = (url.searchParams.get("q") ?? "").trim();
        if (!q) {
          return json({ error: "q is required" }, 400);
        }
        const limit = parseLimit(url.searchParams.get("limit"));
        const results = await searchOpenLibrary(q, Math.min(limit, 50));
        return json({ results });
      }

      if (pathname === "/library" && request.method === "POST") {
        const payload = await readJson<{
          title?: string;
          author?: string;
          openLibraryKey?: string;
          isbn?: string;
        }>(request);

        const hasIdentifier = Boolean(payload.openLibraryKey?.trim() || payload.isbn?.trim());
        const resolved = hasIdentifier
          ? await resolveOpenLibraryCandidate({
              openLibraryKey: payload.openLibraryKey,
              isbn: payload.isbn,
              title: payload.title,
              author: payload.author,
            })
          : null;

        let title = payload.title?.trim() ?? "";
        let author = payload.author?.trim() ?? "";

        if (resolved) {
          title = resolved.title;
          author = resolved.author;
        }

        if (!title || !author) {
          return json({ error: "title and author are required (or provide openLibraryKey/isbn)" }, 400);
        }

        if (hasIdentifier && !resolved) {
          return json({ error: "Open Library match not found" }, 404);
        }

        const book = repo.createBook({
          title,
          author,
        });

        const metadata = resolved
          ? {
              publishedAt: resolved.publishedAt ?? null,
              language: resolved.language ?? null,
              isbn: (payload.isbn?.trim() || resolved.isbn) ?? null,
              identifiers: {
                ...resolved.identifiers,
                ...(payload.isbn?.trim() ? { isbn: payload.isbn.trim() } : {}),
              },
            }
          : await fetchOpenLibraryMetadata({
              title: book.title,
              author: book.author,
              isbn: payload.isbn ?? null,
              openLibraryKey: payload.openLibraryKey ?? null,
            }).catch(() => null);
        if (metadata) {
          repo.updateBookMetadata(book.id, {
            publishedAt: metadata.publishedAt ?? null,
            language: metadata.language ?? null,
            isbn: metadata.isbn ?? null,
            identifiers: metadata.identifiers,
          });
        }
        const jobId = await triggerAutoAcquire(repo, book.id);
        return json({
          book: repo.getBook(book.id),
          acquisition_job_id: jobId,
        }, 201);
      }

      if (pathname === "/library/refresh" && request.method === "POST") {
        const job = repo.createJob({
          type: "scan",
          payload: { fullRefresh: true },
        });
        return json({ jobId: job.id }, 202);
      }

      if (pathname.startsWith("/library/") && request.method === "GET") {
        const id = parseId(pathname.split("/")[2] ?? "");
        const book = repo.getBook(id);
        if (!book) return json({ error: "not_found" }, 404);
        return json({
          book,
          releases: repo.listReleasesByBook(id),
          assets: repo.listAssetsByBook(id),
        });
      }

      if (pathname === "/search" && request.method === "POST") {
        const payload = await readJson<{ query: string; media: MediaType }>(request);
        if (!payload.query?.trim() || (payload.media !== "audio" && payload.media !== "ebook")) {
          return json({ error: "query and media are required" }, 400);
        }
        const results = await runSearch(settings, {
          query: payload.query.trim(),
          media: payload.media,
        });
        return json({ results });
      }

      if (pathname === "/snatch" && request.method === "POST") {
        const payload = await readJson<{
          bookId: number;
          provider: string;
          title: string;
          mediaType: MediaType;
          url: string;
          infoHash: string;
          sizeBytes?: number | null;
        }>(request);
        if (!payload.infoHash?.trim()) {
          return json({ error: "infoHash is required" }, 400);
        }
        const outcome = await runSnatch(repo, settings, payload);
        return json(outcome, outcome.idempotent ? 200 : 201);
      }

      if (pathname === "/releases" && request.method === "GET") {
        const id = parseId(url.searchParams.get("bookId") ?? "");
        return json({ releases: repo.listReleasesByBook(id) });
      }

      if (pathname === "/downloads" && request.method === "GET") {
        return json({ downloads: repo.listDownloads() });
      }

      if (pathname.startsWith("/downloads/") && pathname.endsWith("/retry") && request.method === "POST") {
        const parts = pathname.split("/");
        const jobId = parseId(parts[2] ?? "");
        const retried = repo.retryJob(jobId);
        return json({ job: retried }, 202);
      }

      if (pathname.startsWith("/downloads/") && request.method === "GET") {
        const jobId = parseId(pathname.split("/")[2] ?? "");
        const download = repo.getDownload(jobId);
        if (!download) return json({ error: "not_found" }, 404);
        return json(download);
      }

      if (pathname === "/import/reconcile" && request.method === "POST") {
        const job = repo.createJob({ type: "reconcile" });
        return json({ jobId: job.id }, 202);
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
