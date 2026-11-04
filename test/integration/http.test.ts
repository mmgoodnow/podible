import { afterAll, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createTRPCClient, httpLink } from "@trpc/client";
import type { AppRouter } from "../../src/trpc";
import { FakeBookTorrentClient, bookTorrentMetainfo } from "../mocks/book-torrent";
import { startMockTorznab } from "../mocks/torznab";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";

const isolatedDataDir = await mkdtemp(path.join(os.tmpdir(), "podible-http-test-data-"));

const { runMigrations } = await import("../../src/db");
const { createPodibleFetchHandler } = await import("../../src/http");
const { BooksRepo } = await import("../../src/repo");
const { hashSessionToken } = await import("../../src/auth");

const TINY_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+XGfQAAAAASUVORK5CYII=";

function createBrowserSessionCookie(
  repo: InstanceType<typeof BooksRepo>,
  options: { provider?: "plex" | "local"; username?: string; displayName?: string; isAdmin?: boolean } = {}
): string {
  const provider = options.provider ?? "plex";
  const username = options.username ?? (options.isAdmin ? "admin" : "user");
  const user = repo.upsertUser({
    provider,
    providerUserId: `${provider}-${username}`,
    username,
    displayName: options.displayName ?? username,
    isAdmin: options.isAdmin ?? false,
  });
  const token = `${username}-session-token`;
  repo.createSession(user.id, hashSessionToken(token), new Date(Date.now() + 60_000).toISOString());
  return `podible_session=${token}`;
}

async function rpc(
  fetchHandler: (request: Request) => Promise<Response>,
  method: string,
  params: unknown,
  id = 1,
  headers: Record<string, string> = {}
) {
  const response = await fetchHandler(
    new Request("http://localhost/rpc", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        method,
        params,
      }),
    })
  );
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

async function plexResultLocation(response: Response, ok: boolean, redirectTo = ok ? "/" : "/login"): Promise<URL> {
  expect(response.status).toBe(303);
  expect(response.headers.get("cache-control")).toBe("no-store");
  const location = response.headers.get("location");
  expect(location).toBeTruthy();
  const url = new URL(location!, "http://app.test");
  expect(url.origin).toBe("http://app.test");
  expect(url.pathname).toBe("/login/plex/result");
  expect(url.searchParams.get("ok")).toBe(ok ? "1" : "0");
  expect(url.searchParams.get("redirectTo")).toBe(redirectTo);
  expect([...url.searchParams.keys()].sort()).toEqual(ok ? ["ok", "redirectTo"] : ["error", "ok", "redirectTo"]);
  expect(await response.text()).toBe("");
  return url;
}

afterAll(async () => {
  await rm(isolatedDataDir, { recursive: true, force: true });
});

describe("podible http", () => {
  test("manual snatch RPC injects the private book client and preserves hash idempotency", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const torrent = bookTorrentMetainfo("Piranesi.mp3");
    const hash = infoHashFromTorrentBytes(torrent);
    const torznab = startMockTorznab({ results: [], torrents: { piranesi: torrent } });
    try {
      const settings = repo.ensureSettings();
      settings.libraryRoot = path.join(isolatedDataDir, "private-manual-snatch");
      settings.rtorrent.url = "http://127.0.0.1:1/legacy-must-not-be-used";
      repo.updateSettings(settings);
      const bookTorrentClient = new FakeBookTorrentClient(settings.libraryRoot);
      const handler = createPodibleFetchHandler(repo, Date.now(), null, { bookTorrentClient });
      const book = repo.createBook({ title: "Piranesi", author: "Susanna Clarke" });
      const params = {
        bookId: book.id, provider: "fixture", title: "Piranesi MP3", mediaType: "audio",
        url: `${torznab.baseUrl}/torrent/piranesi.torrent`, infoHash: hash, sizeBytes: 10,
      };
      const cookie = createBrowserSessionCookie(repo, { isAdmin: true });
      const result = await rpc(handler, "snatch.create", params, 1, { cookie });
      expect(result.error).toBeUndefined();
      expect(result.result).toMatchObject({ idempotent: false, release: { info_hash: hash } });
      const again = await rpc(handler, "snatch.create", params, 2, { cookie });
      expect(again.error).toBeUndefined();
      expect(again.result).toMatchObject({ idempotent: true, jobId: result.result.jobId });
      expect(bookTorrentClient.adds).toEqual([hash]);
      expect(bookTorrentClient.resumes).toEqual([hash]);
      expect(repo.bookTorrents.get(hash)).toMatchObject({
        book_id: book.id, release_id: result.result.release.id, client_id: bookTorrentClient.id,
      });
      expect(repo.listReleasesByBook(book.id)).toHaveLength(1);
      expect(repo.listManifestationsByBook(book.id)).toHaveLength(1);
      const binding = repo.bookTorrents.get(hash)!;
      expect((await bookTorrentClient.files({ clientId: bookTorrentClient.id, nativeId: hash }))[0]!.path)
        .toStartWith(path.join(binding.root_path, hash) + path.sep);
    } finally {
      torznab.stop();
      db.close();
    }
  });

  test("redirects protected legacy admin pages to the SPA and removes curation and database routes", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now(), {
      sha: "123456789abcdef",
      message: "Test commit subject",
    });
    const adminRedirect = await fetchHandler(new Request("http://localhost/admin"));
    expect(adminRedirect.status).toBe(303);
    expect(adminRedirect.headers.get("location")).toBe("/login?redirectTo=%2Fadmin");

    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });
    const redirects = [
      ["/admin", "/settings"], ["/admin/settings", "/settings"], ["/admin/users", "/users"],
      ["/admin/plex", "/settings"], ["/admin/ops", "/ops"], ["/admin/jobs", "/ops"],
      ["/admin/downloads", "/ops"], ["/admin/content", "/ops"],
      ["/admin/settings?plex_notice=Selected%20server.", "/settings?plex_notice=Selected%20server."],
    ];
    for (const [legacy, canonical] of redirects) {
      const response = await fetchHandler(new Request(`http://localhost${legacy}`, { headers: { cookie: adminCookie } }));
      expect(response.status).toBe(303);
      expect(response.headers.get("location")).toBe(canonical);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.text()).not.toContain("<html");
      const forbidden = await fetchHandler(new Request(`http://localhost${legacy}`, { headers: { cookie: userCookie } }));
      expect(forbidden.status).toBe(403);
      expect(forbidden.headers.get("location")).toBeNull();
      const loggedOut = await fetchHandler(new Request(`http://localhost${legacy}`));
      expect(loggedOut.status).toBe(303);
      expect(loggedOut.headers.get("location")).toBe(`/login?redirectTo=${encodeURIComponent(legacy)}`);
    }

    for (const removed of ["/admin/curation", "/admin/curation/api/runs", "/admin/curation/api/trace?runId=anything&file=trace.json",
      "/admin/curation/api/unknown", "/admin/db", "/admin/db?table=books&limit=5", "/admin/db?table=sqlite_master"]) {
      for (const cookie of [adminCookie, userCookie, ""]) {
        const response = await fetchHandler(new Request(`http://localhost${removed}`, { headers: cookie ? { cookie } : {} }));
        expect(response.status).toBe(404);
        expect(response.headers.get("location")).toBeNull();
      }
    }
    for (const legacy of ["/admin/refresh", "/admin/plex/select"]) {
      for (const [cookie, expectedStatus] of [["", 401], [userCookie, 403]] as const) {
        const response = await fetchHandler(new Request(`http://localhost${legacy}`, {
          method: "POST", headers: { cookie, "Content-Type": "application/x-www-form-urlencoded" }, body: "machineId=forbidden-server",
        }));
        expect(response.status).toBe(expectedStatus);
        expect(response.headers.get("cache-control")).toBe("no-store");
      }
    }
    expect(repo.listJobsByType("full_library_refresh")).toEqual([]);
    expect(repo.getSettings().auth.plex.machineId).toBe(settings.auth.plex.machineId);
    for (const canonical of ["/settings", "/users", "/ops"]) {
      const page = await fetchHandler(new Request(`http://localhost${canonical}`, { headers: { cookie: adminCookie } }));
      expect(page.status).toBe(200);
      expect(page.headers.get("content-type")).toContain("text/html");
      const html = await page.text();
      expect(html).toContain('id="root"');
      expect(html).not.toContain("Wipe Entire Database");
      expect(html).not.toContain("DB Explorer");
    }

    db.close();
  });

  test("serves ebook downloads with unicode filenames", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const book = repo.createBook({ title: "Carl's Doomsday Scenario", author: "Matt Dinniman" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
    const ebookPath = path.join(isolatedDataDir, "Carl’s Doomsday Scenario.epub");
    await writeFile(ebookPath, "epub bytes");
    const asset = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "application/epub+zip",
      totalSize: 10,
      manifestationId: manifestation.id,
      files: [{ path: ebookPath, size: 10, start: 0, end: 9, durationMs: 0, title: null }],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const cookie = createBrowserSessionCookie(repo, { username: "reader" });
    const response = await fetchHandler(new Request(`http://localhost/ebook/${asset.id}`, { headers: { cookie } }));

    expect(response.status).toBe(200);
    expect(response.headers.get("content-disposition")).toContain("filename*=");
    expect(response.headers.get("content-disposition")).toContain("Carl%E2%80%99s%20Doomsday%20Scenario.epub");
    expect(await response.text()).toBe("epub bytes");
    db.close();
  });

  test("supports rpc health/settings/library and removed rest routes 404", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (url.startsWith("https://openlibrary.org/")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL123W",
                title: "Dune",
                author_name: ["Frank Herbert"],
                first_publish_year: 1965,
                language: ["eng"],
              },
            ],
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
      const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });

      const healthRpc = await rpc(fetchHandler, "system.health", {}, 1);
      expect(healthRpc.result.ok).toBe(true);

      const createdRpc = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL123W" }, 2, {
        cookie: userCookie,
      });
      expect(createdRpc.result.book.title).toBe("Dune");
      expect(createdRpc.result.book.identifiers.openlibrary).toBe("/works/OL123W");
      expect(createdRpc.result.acquisition_job_id).toBeGreaterThan(0);

      const jobRpc = await rpc(fetchHandler, "jobs.get", { jobId: createdRpc.result.acquisition_job_id }, 21, {
        cookie: adminCookie,
      });
      expect(jobRpc.result.job.id).toBe(createdRpc.result.acquisition_job_id);
      expect(jobRpc.result.job.type).toBe("acquire");

      const listRpc = await rpc(fetchHandler, "library.list", { limit: 10 }, 3, {
        cookie: userCookie,
      });
      expect(Array.isArray(listRpc.result.items)).toBe(true);
      expect(listRpc.result.items.length).toBe(1);

      const settingsRpc = await rpc(fetchHandler, "settings.get", {}, 4, {
        cookie: adminCookie,
      });
      expect(settingsRpc.result.auth.mode).toBe("plex");

      const removed = [
        new Request("http://localhost/health", { method: "GET" }),
        new Request("http://localhost/server", { method: "GET" }),
        new Request("http://localhost/settings", { method: "PUT", body: "{}" }),
        new Request("http://localhost/openlibrary/search?q=dune", { method: "GET" }),
        new Request("http://localhost/library", { method: "POST", body: "{}" }),
        new Request("http://localhost/library/refresh", { method: "POST" }),
        new Request("http://localhost/library/1", { method: "GET" }),
        new Request("http://localhost/search", { method: "POST", body: "{}" }),
        new Request("http://localhost/snatch", { method: "POST", body: "{}" }),
        new Request("http://localhost/releases?bookId=1", { method: "GET" }),
        new Request("http://localhost/downloads", { method: "GET" }),
        new Request("http://localhost/downloads/1", { method: "GET" }),
        new Request("http://localhost/downloads/1/retry", { method: "POST" }),
        new Request("http://localhost/import/reconcile", { method: "POST" }),
        new Request("http://localhost/classic", { method: "GET" }),
        new Request("http://localhost/classic/library?q=Dune", { method: "GET" }),
        new Request("http://localhost/classic/add?q=Dune", { method: "GET" }),
        new Request(`http://localhost/classic/book/${createdRpc.result.book.id}`, { method: "GET" }),
        new Request("http://localhost/classic/unknown", { method: "GET" }),
        new Request("http://localhost/classic/add", { method: "POST", body: "openLibraryKey=%2Fworks%2FOL123W" }),
        new Request("http://localhost/add", { method: "POST", body: "openLibraryKey=%2Fworks%2FOL123W" }),
        new Request(`http://localhost/book/${createdRpc.result.book.id}/acquire`, { method: "POST", body: "media=audio" }),
      ];
      for (const request of removed) {
        const response = await fetchHandler(
          new Request(request.url, {
            method: request.method,
            headers: { cookie: adminCookie },
            body: request.method === "GET" ? undefined : await request.text(),
          })
        );
        expect(response.status).toBe(404);
      }
      expect(repo.listBooks(10).items).toHaveLength(1);
      expect(repo.listJobsByType("acquire")).toHaveLength(1);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("exposes library editions, release history, transcripts and downloads over RPC", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const standardRelease = repo.createRelease({
      bookId: book.id,
      provider: "mock",
      providerGuid: "standard-guid",
      title: "Dune Standard Audio",
      mediaType: "audio",
      infoHash: "1111111111111111111111111111111111111111",
      url: "https://example.com/dune-standard.torrent",
      status: "imported",
    });
    const standardManifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    const standardAudio = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mp4",
      totalSize: 123,
      durationMs: 1_000,
      sourceReleaseId: standardRelease.id,
      manifestationId: standardManifestation.id,
      files: [
        {
          path: path.join(isolatedDataDir, "dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    const transcriptPath = path.join(isolatedDataDir, "dune-transcript.json");
    await writeFile(transcriptPath, JSON.stringify({ text: "Fear is the mind-killer", words: [] }));
    repo.upsertManifestationTranscript({
      manifestationId: standardManifestation.id,
      status: "succeeded",
      source: "whisper_transcript",
      algorithmVersion: "test",
      fingerprint: "test",
      transcriptPath,
    });
    const graphicAudio = repo.addManifestation({
      bookId: book.id,
      kind: "audio",
      label: "GraphicAudio dramatization",
      editionNote: "full cast",
    });
    const graphicAsset = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 456,
      durationMs: 2_000,
      manifestationId: graphicAudio.id,
      sequenceInManifestation: 0,
      files: [
        {
          path: path.join(isolatedDataDir, "dune-graphic.mp3"),
          size: 456,
          start: 0,
          end: 455,
          durationMs: 2_000,
          title: "Dune GraphicAudio",
        },
      ],
    });

    const ebookManifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
    const ebookPath = path.join(isolatedDataDir, "dune.pdf");
    await writeFile(ebookPath, "pdf bytes");
    const ebookAsset = repo.addAsset({
      bookId: book.id,
      manifestationId: ebookManifestation.id,
      kind: "single",
      mime: "application/pdf",
      totalSize: 9,
      files: [{ path: ebookPath, size: 9, start: 0, end: 8, durationMs: 0 }],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const library = await rpc(fetchHandler, "library.list", {}, 1, { cookie: userCookie });
    expect(library.result.items).toHaveLength(1);
    expect(library.result.items[0]).toMatchObject({ id: book.id, title: "Dune" });

    const detail = await rpc(fetchHandler, "library.get", { bookId: book.id }, 2, { cookie: userCookie });
    expect(detail.error).toBeUndefined();
    expect(detail.result.book).toMatchObject({ id: book.id, audioStatus: "imported", ebookStatus: "imported" });
    expect(detail.result.releases).toMatchObject([{ id: standardRelease.id, title: "Dune Standard Audio" }]);
    expect(detail.result.assets.map((asset: any) => asset.id).sort()).toEqual(
      [standardAudio.id, graphicAsset.id, ebookAsset.id].sort()
    );
    const playback = detail.result.book.playback;
    expect(playback.audio).toMatchObject({
      manifestationId: standardManifestation.id,
      streamUrl: `http://localhost/stream/m/${standardManifestation.id}.m4a`,
      chaptersUrl: `http://localhost/chapters/m/${standardManifestation.id}.json`,
      transcriptUrl: `http://localhost/transcripts/m/${standardManifestation.id}.json`,
    });
    expect(playback.audioOptions).toHaveLength(2);
    expect(playback.audioOptions.find((option: any) => option.manifestationId === graphicAudio.id)).toMatchObject({
      label: "GraphicAudio dramatization",
      editionNote: "full cast",
      streamUrl: `http://localhost/stream/m/${graphicAudio.id}.mp3`,
      transcriptUrl: null,
    });
    expect(playback.ebook).toEqual({
      assetId: ebookAsset.id,
      downloadUrl: `http://localhost/ebook/${ebookAsset.id}`,
      mimeType: "application/pdf",
      sizeBytes: 9,
    });
    expect(library.result.items[0].playback).toEqual(playback);

    const transcript = await fetchHandler(new Request(playback.audio.transcriptUrl, { headers: { cookie: userCookie } }));
    expect(transcript.status).toBe(200);
    expect(await transcript.json()).toMatchObject({ text: "Fear is the mind-killer" });
    const download = await fetchHandler(new Request(playback.ebook.downloadUrl, { headers: { cookie: userCookie } }));
    expect(download.status).toBe(200);
    expect(download.headers.get("content-type")).toContain("application/pdf");
    expect(await download.text()).toBe("pdf bytes");

    db.close();
  });

  test("filters library.list by title or author and returns no unrelated books", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const dune = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const hyperion = repo.createBook({ title: "Hyperion", author: "Dan Simmons" });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    for (const [q, expectedIds] of [
      ["Hyperion", [hyperion.id]],
      ["frank herbert", [dune.id]],
      ["missing title", []],
    ] as const) {
      const library = await rpc(fetchHandler, "library.list", { q }, 1, { cookie: userCookie });
      expect(library.error).toBeUndefined();
      expect(library.result.items.map((item: any) => item.id)).toEqual([...expectedIds]);
    }

    db.close();
  });

  test("requires authentication and queues targeted audio acquisition over RPC", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const unauthorized = await rpc(fetchHandler, "library.acquire", { bookId: book.id, media: ["audio"] });
    expect(unauthorized.error.code).toBe(-32001);
    expect(repo.listJobsByType("acquire")).toHaveLength(0);
    const queued = await rpc(fetchHandler, "library.acquire", { bookId: book.id, media: ["audio"] }, 2, {
      cookie: userCookie,
    });
    expect(queued.error).toBeUndefined();
    expect(queued.result.media).toEqual(["audio"]);

    const jobs = repo.listJobsByType("acquire");
    expect(jobs.length).toBe(1);
    expect(jobs[0]?.id).toBe(queued.result.jobId);
    expect(jobs[0]?.book_id).toBe(book.id);
    expect(jobs[0]?.status).toBe("queued");
    expect(JSON.parse(jobs[0]?.payload_json ?? "{}").media).toEqual(["audio"]);

    db.close();
  });

  test("exposes book activity job state over authenticated RPC and keeps refresh admin-only", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      manifestationId: manifestation.id,
      files: [
        {
          path: path.join(isolatedDataDir, "activity-dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    const failed = repo.createBook({ title: "Broken Book", author: "Someone" });
    const failedRelease = repo.createRelease({
      bookId: failed.id,
      provider: "test",
      title: "Broken Book",
      mediaType: "audio",
      infoHash: "abcdef1234567890abcdef1234567890abcdef12",
      url: "https://example.com/broken.torrent",
      status: "failed",
    });
    repo.setReleaseStatus(failedRelease.id, "failed", "boom");
    const runningJob = repo.createJob({ type: "acquire", bookId: book.id, status: "running" });
    const failedJob = repo.createJob({ type: "download", bookId: failed.id, status: "failed" });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const client = (cookie?: string) => createTRPCClient<AppRouter>({
      links: [httpLink({
        url: "http://localhost/api/trpc",
        fetch: async (input, init) => {
          const response = await fetchHandler(new Request(input.toString(), {
            ...init,
            headers: {
              ...Object.fromEntries(new Headers(init?.headers)),
              ...(cookie ? { cookie } : {}),
            },
          }));
          return {
            status: response.status,
            ok: response.ok,
            headers: response.headers,
            json: () => response.json(),
            text: () => response.text(),
          };
        },
      })],
    });
    await expect(client().book.activity.query({ bookId: book.id })).rejects.toMatchObject({
      data: { code: "UNAUTHORIZED" },
    });
    const browser = client(userCookie);
    const working = await browser.book.activity.query({ bookId: book.id });
    expect(working.activeAcquisition).toBe(true);
    expect(working.jobs).toMatchObject([{ id: runningJob.id, type: "acquire", status: "running", state: "running" }]);
    expect(working.jobs).toHaveLength(1);
    const broken = await browser.book.activity.query({ bookId: failed.id });
    expect(broken.activeAcquisition).toBe(false);
    expect(broken.jobs).toMatchObject([{ id: failedJob.id, type: "download", status: "failed", state: "failed" }]);
    expect(broken.jobs).toHaveLength(1);
    expect(broken.jobs[0]?.message).toContain("could not finish");
    const ready = await rpc(fetchHandler, "library.get", { bookId: book.id }, 1, { cookie: userCookie });
    expect(ready.result.book.audioStatus).toBe("imported");
    expect(ready.result.book.playback.audio.manifestationId).toBe(manifestation.id);
    const failedDetail = await rpc(fetchHandler, "library.get", { bookId: failed.id }, 2, { cookie: userCookie });
    expect(failedDetail.result.book.audioStatus).toBe("error");
    expect(failedDetail.result.book.playback.audio).toBeNull();

    const activity = await fetchHandler(
      new Request("http://localhost/activity", {
        headers: { cookie: userCookie },
      })
    );
    expect(activity.status).toBe(303);
    expect(activity.headers.get("location")).toBe("/");

    const rejected = await fetchHandler(
      new Request("http://localhost/activity/refresh", {
        method: "POST",
        headers: { cookie: userCookie },
      })
    );
    expect(rejected.status).toBe(403);
    expect(repo.listJobsByType("full_library_refresh").length).toBe(0);

    const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });
    const adminOps = await fetchHandler(
      new Request("http://localhost/admin/ops", {
        headers: { cookie: adminCookie },
      })
    );
    expect(adminOps.status).toBe(303);
    expect(adminOps.headers.get("location")).toBe("/ops");
    expect(adminOps.headers.get("cache-control")).toBe("no-store");

    const refreshed = await fetchHandler(
      new Request("http://localhost/admin/refresh", {
        method: "POST",
        headers: { cookie: adminCookie },
      })
    );
    expect(refreshed.status).toBe(303);
    expect(refreshed.headers.get("location")).toContain("/ops?notice=");
    expect(refreshed.headers.get("cache-control")).toBe("no-store");
    expect(repo.listJobsByType("full_library_refresh").length).toBe(1);

    db.close();
  });

  test("queues a refresh job from admin ops", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });
    const refreshed = await fetchHandler(
      new Request("http://localhost/admin/refresh", {
        method: "POST",
        headers: { cookie: adminCookie },
      })
    );
    expect(refreshed.status).toBe(303);
    expect(refreshed.headers.get("location")).toContain("/ops?notice=");
    expect(refreshed.headers.get("cache-control")).toBe("no-store");
    expect(repo.listJobsByType("full_library_refresh").length).toBe(1);

    db.close();
  });

  test("redirects logged-out protected admin and activity requests to login", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    for (const pathname of ["/admin", "/admin/settings", "/admin/users", "/admin/ops", "/admin/jobs", "/admin/downloads", "/admin/content", "/admin/plex", "/activity?foo=bar"]) {
      const response = await fetchHandler(new Request(`http://localhost${pathname}`));
      expect(response.status).toBe(303);
      expect(response.headers.get("location")).toBe(`/login?redirectTo=${encodeURIComponent(pathname)}`);
    }
    for (const pathname of ["/classic", "/classic/library", "/classic/add", "/classic/book/1?foo=bar", "/admin/curation", "/admin/curation/api/runs", "/admin/db?table=books&limit=5"]) {
      const response = await fetchHandler(new Request(`http://localhost${pathname}`));
      expect(response.status).toBe(404);
      expect(response.headers.get("location")).toBeNull();
    }

    db.close();
  });

  test("searches the catalog and attributes library.create to the authenticated actor", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if (url.pathname === "/search.json") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      if (url.pathname === "/works/OL45804W.json") {
        return new Response(
          JSON.stringify({
            description: "On the world called Hyperion, beyond the law of the Hegemony of Man, there waits the creature called the Shrike.",
            covers: [12345],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "alice" });

      const found = await rpc(fetchHandler, "openlibrary.search", { q: "Hyperion Dan Simmons" }, 1, { cookie: userCookie });
      expect(found.result.results).toMatchObject([{
        title: "Hyperion",
        author: "Dan Simmons",
        openLibraryKey: "/works/OL45804W",
      }]);
      const created = await rpc(fetchHandler, "library.create", {
        openLibraryKey: found.result.results[0].openLibraryKey,
      }, 2, { cookie: userCookie });
      expect(created.error).toBeUndefined();
      const alice = repo.listUsers().find((user) => user.username === "alice");
      expect(alice).toBeDefined();
      const bookId = created.result.book.id;
      expect(repo.getBookRow(bookId)?.added_by_user_id).toBe(alice!.id);
      expect(repo.getJob(created.result.acquisition_job_id)).toMatchObject({ book_id: bookId, type: "acquire", status: "queued" });

      const detail = await rpc(fetchHandler, "library.get", { bookId }, 3, { cookie: userCookie });
      expect(detail.result.book).toMatchObject({
        title: "Hyperion",
        author: "Dan Simmons",
        addedByUserId: alice!.id,
        addedByUser: { id: alice!.id, username: "alice" },
      });
      expect(created.result.book.addedByUser).toEqual(detail.result.book.addedByUser);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports browser-session access and logout", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      manifestationId: manifestation.id,
      files: [
        {
          path: path.join(isolatedDataDir, "login-dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    const unauthorized = await rpc(fetchHandler, "library.list", {});
    expect(unauthorized.error.code).toBe(-32001);

    const loginPage = await fetchHandler(new Request("http://app.test/login?redirectTo=%2Flibrary"));
    expect(loginPage.status).toBe(200);
    const loginBody = await loginPage.text();
    expect(loginBody).toContain('id="root"');
    expect(loginBody.includes('<nav class="site-nav">')).toBe(false);

    const cookieHeader = createBrowserSessionCookie(repo, {
      username: "alice",
      displayName: "Alice",
    });
    const sessionHash = hashSessionToken(cookieHeader.slice("podible_session=".length));
    const initialSession = repo.getSessionByTokenHash(sessionHash)!;
    repo.createSession(initialSession.user_id, hashSessionToken("expired-browser-token"), "2000-01-01T00:00:00.000Z");
    const expired = await fetchHandler(new Request("http://app.test/rpc/auth/me", {
      headers: { cookie: "podible_session=expired-browser-token" },
    }));
    expect(expired.status).toBe(200);
    expect(expired.headers.get("set-cookie")).toBeNull();
    expect((await expired.json() as any).error.code).toBe(-32001);
    const authed = await fetchHandler(
      new Request("http://app.test/rpc/library/list", {
        headers: { cookie: cookieHeader },
      })
    );
    expect(authed.status).toBe(200);
    expect(authed.headers.get("set-cookie")).toContain("Max-Age=34560000");
    expect(authed.headers.get("set-cookie")).toContain(`${cookieHeader}; Path=/; HttpOnly; SameSite=Lax`);
    const authedBody = await authed.json() as any;
    expect(authedBody.result.items).toMatchObject([{ id: book.id, title: "Dune" }]);
    const me = await fetchHandler(new Request("http://app.test/rpc/auth/me", { headers: { cookie: cookieHeader } }));
    expect(me.status).toBe(200);
    expect(me.headers.get("set-cookie")).toContain("Max-Age=34560000");
    const meBody = await me.json() as any;
    expect(meBody.result.user).toMatchObject({ username: "alice", displayName: "Alice" });
    expect(meBody.result.session.kind).toBe("browser");
    expect(repo.getSessionByTokenHash(sessionHash)?.expires_at).toBe(initialSession.expires_at);

    const logout = await fetchHandler(
      new Request("http://app.test/logout", {
        method: "POST",
        headers: { cookie: cookieHeader },
      })
    );
    expect(logout.status).toBe(303);
    expect(logout.headers.get("set-cookie")).toContain("Max-Age=0");
    expect(repo.getSessionByTokenHash(sessionHash)).toBeNull();
    const afterLogout = await rpc(fetchHandler, "library.list", {}, 2, { cookie: cookieHeader });
    expect(afterLogout.error.code).toBe(-32001);

    db.close();
  });

  test("supports app login flow for Kindling-style bearer auth", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: {
        ...settings.auth,
        mode: "plex",
        appRedirectURIs: ["kindling://auth/podible"],
      },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    const begin = await rpc(fetchHandler, "auth.beginAppLogin", { redirectUri: "kindling://auth/podible" }, 1);
    expect(begin.result.authorizeUrl).toContain("/auth/app/");
    expect(begin.result.state).toBeTruthy();
    const authorizeUrl = begin.result.authorizeUrl as string;

    const authorize = await fetchHandler(new Request(authorizeUrl));
    const authorizePath = new URL(authorizeUrl).pathname;
    expect(authorize.status).toBe(303);
    expect(authorize.headers.get("cache-control")).toBe("no-store");
    const loginLocation = new URL(authorize.headers.get("location")!, "http://localhost");
    expect(loginLocation.pathname).toBe("/login");
    expect(loginLocation.searchParams.get("inline")).toBe("1");
    expect(loginLocation.searchParams.get("redirectTo")).toBe(`${authorizePath}/complete`);
    expect(await authorize.text()).toBe("");
    const unauthenticatedComplete = await fetchHandler(new Request(`${authorizeUrl}/complete`));
    expect(unauthenticatedComplete.status).toBe(303);
    expect(unauthenticatedComplete.headers.get("location")).toBe(authorize.headers.get("location"));
    expect(db.query("SELECT COUNT(*) AS n FROM auth_codes").get()).toEqual({ n: 0 });
    expect(repo.getAppLoginAttempt(begin.result.attemptId)).not.toBeNull();
    const browserCookie = createBrowserSessionCookie(repo, {
      username: "kindling",
      displayName: "Kindling",
    });
    const authenticatedAuthorize = await fetchHandler(new Request(authorizeUrl, { headers: { cookie: browserCookie } }));
    expect(authenticatedAuthorize.status).toBe(303);
    expect(authenticatedAuthorize.headers.get("location")).toBe(`${authorizePath}/complete`);

    const complete = await fetchHandler(
      new Request(`http://app.test${authorizePath}/complete`, {
        headers: { cookie: browserCookie },
      })
    );
    expect(complete.status).toBe(302);
    expect(complete.headers.get("cache-control")).toBe("no-store");
    expect(await complete.text()).toBe("");
    const callbackLocation = complete.headers.get("location") ?? "";
    expect(callbackLocation.startsWith("kindling://auth/podible?")).toBe(true);
    const callbackUrl = new URL(callbackLocation);
    const code = callbackUrl.searchParams.get("code");
    const state = callbackUrl.searchParams.get("state");
    expect(code).toBeTruthy();
    expect(state).toBe(begin.result.state);
    expect(db.query("SELECT code_hash FROM auth_codes").get()).toEqual({ code_hash: hashSessionToken(code!) });

    const exchange = await rpc(fetchHandler, "auth.exchange", { code }, 2);
    expect(typeof exchange.result.accessToken).toBe("string");
    expect(exchange.result.user.username).toBe("kindling");
    expect(repo.getAppLoginAttempt(begin.result.attemptId)).toBeNull();
    const replayedExchange = await rpc(fetchHandler, "auth.exchange", { code }, 4);
    expect(replayedExchange.error.code).toBe(-32000);

    const me = await rpc(fetchHandler, "auth.me", {}, 3, {
      Authorization: `Bearer ${exchange.result.accessToken}`,
    });
    expect(me.result.user.username).toBe("kindling");
    expect(me.result.session.kind).toBe("app");

    db.close();
  });

  test("auth SPA shells expose no secrets and invalid or expired attempts redirect without granting sessions or codes", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    settings.auth.plex.ownerToken = "owner-token-must-stay-server-side";
    settings.agents.apiKey = "agent-key-must-stay-server-side";
    repo.updateSettings(settings);
    const handler = createPodibleFetchHandler(repo, Date.now());
    try {
      for (const route of ["/login?redirectTo=%2Flibrary", "/login/plex/loading", "/login/plex/result?ok=1&code=untrusted-code&redirectTo=%2Flibrary"]) {
        const page = await handler(new Request(`http://app.test${route}`));
        expect(page.status).toBe(200);
        const body = await page.text();
        expect(body).toContain('id="root"');
        expect(body).not.toContain("owner-token-must-stay-server-side");
        expect(body).not.toContain("agent-key-must-stay-server-side");
        expect(body).not.toContain("untrusted-code");
      }
      for (const pinId of ["", "0", "-1", "123junk", "9007199254740992"]) {
        const response = await handler(new Request(`http://app.test/login/plex/complete?pinId=${pinId}`));
        const result = await plexResultLocation(response, false);
        expect(result.searchParams.get("error")).toBe("Missing or invalid Plex PIN id.");
        expect(response.headers.get("set-cookie")).toBeNull();
      }
      repo.createPlexLoginAttempt({ pinId: 321, clientIdentifier: "expired-fixture", publicJwkJson: "{}", privateKeyPkcs8: "not-used-for-expired-attempt" });
      db.query("UPDATE plex_login_attempts SET created_at=? WHERE pin_id=?").run("2000-01-01T00:00:00.000Z", 321);
      const expiredPlex = await handler(new Request("http://app.test/login/plex/complete?pinId=321"));
      const expiredResult = await plexResultLocation(expiredPlex, false);
      expect(expiredResult.searchParams.get("error")).toContain("has expired");
      expect(expiredPlex.headers.get("set-cookie")).toBeNull();
      expect(repo.getPlexLoginAttempt(321)).toBeNull();
      repo.createAppLoginAttempt({ id: "expired-app", redirectUri: "kindling://auth/podible", state: "expired-state", expiresAt: "2000-01-01T00:00:00.000Z" });
      for (const attempt of ["missing-app", "expired-app"]) for (const suffix of ["", "/complete"]) {
        const response = await handler(new Request(`http://app.test/auth/app/${attempt}${suffix}`));
        expect(response.status).toBe(303);
        expect(response.headers.get("cache-control")).toBe("no-store");
        const location = new URL(response.headers.get("location")!, "http://app.test");
        expect(location.pathname).toBe("/login");
        expect(location.searchParams.get("error")).toContain("missing or has expired");
        expect(location.searchParams.has("code")).toBe(false);
        expect(await response.text()).toBe("");
        expect(response.headers.get("set-cookie")).toBeNull();
      }
      expect(repo.getAppLoginAttempt("expired-app")).toBeNull();
      expect(db.query("SELECT COUNT(*) AS n FROM sessions").get()).toEqual({ n: 0 });
      expect(db.query("SELECT COUNT(*) AS n FROM auth_codes").get()).toEqual({ n: 0 });
    } finally { db.close(); }
  });

  test("supports Plex browser login and creates a browser session", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        const body = JSON.parse(String(init.body ?? "{}"));
        expect(body.strong).toBe(true);
        expect(typeof body.jwk?.x).toBe("string");
        expect(typeof body.jwk?.kid).toBe("string");
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        expect(url.searchParams.get("deviceJWT")?.split(".").length).toBe(3);
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "plex-jwt-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("plex-jwt-token");
        return new Response(
          JSON.stringify({
            id: "plex-user-1",
            username: "alice",
            title: "Alice",
            thumb: "https://example.com/alice.jpg",
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
      });

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 123,
        durationMs: 1_000,
        manifestationId: manifestation.id,
        files: [
          {
            path: path.join(isolatedDataDir, "plex-dune.mp3"),
            size: 123,
            start: 0,
            end: 122,
            durationMs: 1_000,
            title: "Dune",
          },
        ],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());

      const loginPage = await fetchHandler(new Request("http://app.test/login"));
      expect(loginPage.status).toBe(200);
      const loginBody = await loginPage.text();
      expect(loginBody).toContain('id="root"');
      expect(loginBody.includes('<nav class="site-nav">')).toBe(false);

      const start = await fetchHandler(
        new Request("http://app.test/login/plex/start?redirectTo=%2Flibrary%3Fview%3Daudio", {
          method: "POST",
        })
      );
      expect(start.status).toBe(200);
      const startPayload = (await start.json()) as { authUrl: string; pinId: number };
      expect(startPayload.pinId).toBe(123);
      expect(startPayload.authUrl).toContain("https://app.plex.tv/auth#?");
      expect(startPayload.authUrl).toContain("PINCODE123");
      expect(startPayload.authUrl).toContain(encodeURIComponent("http://app.test/login/plex/complete?pinId=123"));
      const forwardUrl = new URL(new URLSearchParams(new URL(startPayload.authUrl).hash.slice(2)).get("forwardUrl")!);
      expect(forwardUrl.origin).toBe("http://app.test");
      expect(forwardUrl.pathname).toBe("/login/plex/complete");
      expect(forwardUrl.searchParams.get("redirectTo")).toBe("/library?view=audio");

      const complete = await fetchHandler(new Request(forwardUrl.toString()));
      const resultLocation = await plexResultLocation(complete, true, "/library?view=audio");
      const setCookie = complete.headers.get("set-cookie") ?? "";
      expect(setCookie.includes("podible_session=")).toBe(true);

      const cookieHeader = setCookie.split(";")[0] ?? "";
      expect(setCookie).toContain("Max-Age=34560000");
      expect(resultLocation.href).not.toContain(cookieHeader.slice("podible_session=".length));
      expect(resultLocation.href).not.toContain("plex-jwt-token");
      expect(resultLocation.href).not.toContain("PINCODE123");
      const authed = await rpc(fetchHandler, "library.list", {}, 1, { cookie: cookieHeader });
      expect(authed.error).toBeUndefined();
      expect(authed.result.items).toMatchObject([{ id: book.id, title: "Dune" }]);
      const me = await rpc(fetchHandler, "auth.me", {}, 2, { cookie: cookieHeader });
      expect(me.result.user).toMatchObject({ username: "alice", displayName: "Alice", isAdmin: true });
      expect(me.result.session.kind).toBe("browser");
      expect(repo.listUsers("plex")[0]?.is_admin).toBe(1);
      expect(repo.getSettings().auth.plex.ownerToken).toBe("plex-jwt-token");
      expect(repo.getPlexLoginAttempt(123)).toBeNull();
      const consumedAttempt = await fetchHandler(new Request(forwardUrl.toString()));
      const consumedResult = await plexResultLocation(consumedAttempt, false);
      expect(consumedResult.searchParams.get("error")).toContain("missing or has expired");
      expect(consumedAttempt.headers.get("set-cookie")).toBeNull();
      expect(repo.listUsers("plex")).toHaveLength(1);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports Plex bootstrap even when a local dev admin user already exists", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "plex-jwt-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("plex-jwt-token");
        return new Response(
          JSON.stringify({
            id: "plex-user-1",
            username: "alice",
            title: "Alice",
            thumb: "https://images.example/alice.jpg",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
      });

      repo.upsertUser({
        provider: "local",
        providerUserId: "dev:michael",
        username: "michael",
        displayName: "Michael Goodnow",
        isAdmin: true,
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const start = await fetchHandler(
        new Request("http://app.test/login/plex/start?redirectTo=%2F%5Cuntrusted.invalid", {
          method: "POST",
        })
      );
      expect(start.status).toBe(200);
      const startPayload = await start.json() as { authUrl: string };
      const forwardUrl = new URL(new URLSearchParams(new URL(startPayload.authUrl).hash.slice(2)).get("forwardUrl")!);
      expect(forwardUrl.searchParams.has("redirectTo")).toBe(false);

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123&redirectTo=%2F%5Cuntrusted.invalid"));
      await plexResultLocation(complete, true);
      expect(complete.headers.get("set-cookie")).toContain("podible_session=");

      expect(repo.listUsers("plex")).toHaveLength(1);
      expect(repo.getSettings().auth.plex.ownerToken).toBe("plex-jwt-token");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("discovers Plex servers through adminWeb RPC and retains the admin-only compatibility selection POST", async () => {
    const originalFetch = globalThis.fetch;
    let plexResourcesFetches = 0;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/resources") {
        plexResourcesFetches += 1;
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("owner-token");
        return new Response(
          `<?xml version="1.0"?>
          <MediaContainer>
            <Device name="Family Server" product="Plex Media Server" clientIdentifier="family-server" owned="1" provides="server"/>
            <Device name="Friends Server" product="Plex Media Server" clientIdentifier="friends-server" owned="1" provides="server"/>
          </MediaContainer>`,
          { status: 200, headers: { "Content-Type": "application/xml" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: {
            ...settings.auth.plex,
            ownerToken: "owner-token",
          },
        },
      });
      const adminUser = repo.upsertUser({
        provider: "plex",
        providerUserId: "owner",
        username: "owner",
        displayName: "Owner",
        isAdmin: true,
      });
      repo.createSession(adminUser.id, hashSessionToken("admin-session"), new Date(Date.now() + 60_000).toISOString());

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const adminCookie = "podible_session=admin-session";

      const page = await fetchHandler(
        new Request("http://app.test/admin/settings", {
          headers: { cookie: adminCookie },
        })
      );
      expect(page.status).toBe(303);
      expect(page.headers.get("location")).toBe("/settings");
      expect(page.headers.get("cache-control")).toBe("no-store");
      expect(plexResourcesFetches).toBe(0);

      const servers = await rpc(fetchHandler, "adminWeb.plexServers", {}, 1, { cookie: adminCookie });
      expect(servers.error).toBeUndefined();
      expect(servers.result.source).toBe("fresh");
      expect(servers.result.servers.map((server: { name: string }) => server.name)).toEqual(["Family Server", "Friends Server"]);
      expect(JSON.stringify(servers.result)).not.toContain("owner-token");
      expect(plexResourcesFetches).toBe(1);

      const cachedServers = await rpc(fetchHandler, "adminWeb.plexServers", {}, 2, { cookie: adminCookie });
      expect(cachedServers.error).toBeUndefined();
      expect(cachedServers.result.source).toBe("cache");
      expect(cachedServers.result.servers).toEqual(servers.result.servers);
      expect(plexResourcesFetches).toBe(1);

      const beforeSelection = repo.getSettings();
      const selected = await fetchHandler(
        new Request("http://app.test/admin/plex/select", {
          method: "POST",
          headers: {
            cookie: adminCookie,
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: "machineId=family-server",
        })
      );
      expect(selected.status).toBe(303);
      expect(selected.headers.get("location")).toBe("/settings?plex_notice=Selected%20server.");
      expect(selected.headers.get("cache-control")).toBe("no-store");
      expect(repo.getSettings().auth.plex.machineId).toBe("family-server");
      expect(repo.getSettings()).toEqual({ ...beforeSelection, auth: { ...beforeSelection.auth,
        plex: { ...beforeSelection.auth.plex, machineId: "family-server" } } });
      expect(plexResourcesFetches).toBe(1);

      const missing = await fetchHandler(new Request("http://app.test/admin/plex/select", {
        method: "POST", headers: { cookie: adminCookie, "Content-Type": "application/x-www-form-urlencoded" }, body: "machineId=%20",
      }));
      expect(missing.status).toBe(303);
      expect(missing.headers.get("location")).toBe("/settings?plex_error=Missing%20machine%20id");
      expect(repo.getSettings().auth.plex.machineId).toBe("family-server");

      const rpcSelected = await rpc(fetchHandler, "adminWeb.selectPlexServer", { machineId: "friends-server" }, 3, { cookie: adminCookie });
      expect(rpcSelected.error).toBeUndefined();
      expect(rpcSelected.result).toEqual({ selectedMachineId: "friends-server" });
      expect(repo.getSettings().auth.plex.machineId).toBe("friends-server");
      expect(plexResourcesFetches).toBe(2);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("gates new Plex sign-ins by access to the selected Plex server", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "candidate-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("candidate-token");
        return new Response(
          JSON.stringify({
            id: "friend-user",
            username: "friend",
            title: "Friend",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/users") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("owner-token");
        return new Response(
          `<?xml version="1.0"?>
          <MediaContainer>
            <User id="friend-user" title="Friend">
              <Server machineIdentifier="other-server" />
            </User>
          </MediaContainer>`,
          { status: 200, headers: { "Content-Type": "application/xml" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: {
            ...settings.auth.plex,
            ownerToken: "owner-token",
            machineId: "family-server",
          },
        },
      });
      repo.upsertUser({
        provider: "plex",
        providerUserId: "owner-user",
        username: "owner",
        displayName: "Owner",
        isAdmin: true,
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const start = await fetchHandler(new Request("http://app.test/login/plex/start", { method: "POST" }));
      expect(start.status).toBe(200);

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123"));
      const resultLocation = await plexResultLocation(complete, false);
      expect(complete.headers.get("set-cookie")).toBeNull();
      expect(resultLocation.searchParams.get("error")).toBe("This Plex user is not allowed on this Podible instance.");
      expect(repo.listUsers("plex")).toHaveLength(1);
      expect(repo.getPlexLoginAttempt(123)).toBeNull();

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("allows existing non-admin user to log in when owner token is expired", async () => {
    // Build an expired JWT so checkPlexUserAccess short-circuits with a throw.
    const expiredJwt = [
      Buffer.from(JSON.stringify({ alg: "EdDSA", typ: "JWT" })).toString("base64url"),
      Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) - 3600 })).toString("base64url"),
      "fake-sig",
    ].join(".");

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "non-admin-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        return new Response(
          JSON.stringify({ id: "existing-non-admin", username: "alice", title: "Alice" }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      // /api/users should NOT be called because the token is expired
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: { ...settings.auth.plex, ownerToken: expiredJwt, machineId: "family-server" },
        },
      });
      repo.upsertUser({ provider: "plex", providerUserId: "owner-user", username: "owner", displayName: "Owner", isAdmin: true });
      repo.upsertUser({ provider: "plex", providerUserId: "existing-non-admin", username: "alice", displayName: "Alice", isAdmin: false });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const start = await fetchHandler(new Request("http://app.test/login/plex/start", { method: "POST" }));
      expect(start.status).toBe(200);

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123"));
      await plexResultLocation(complete, true);
      const cookieHeader = (complete.headers.get("set-cookie") ?? "").split(";")[0]!;
      expect(cookieHeader).toStartWith("podible_session=");
      const me = await rpc(fetchHandler, "auth.me", {}, 1, { cookie: cookieHeader });
      expect(me.result.user.username).toBe("alice");
      expect(me.result.user.isAdmin).toBe(false);
      expect(me.result.session.kind).toBe("browser");
      expect(repo.getSettings().auth.plex.ownerToken).toBe(expiredJwt);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("denies unknown user with re-link message when owner token is expired", async () => {
    const expiredJwt = [
      Buffer.from(JSON.stringify({ alg: "EdDSA", typ: "JWT" })).toString("base64url"),
      Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) - 3600 })).toString("base64url"),
      "fake-sig",
    ].join(".");

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "new-user-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        return new Response(
          JSON.stringify({ id: "brand-new-user", username: "newbie", title: "Newbie" }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: { ...settings.auth.plex, ownerToken: expiredJwt, machineId: "family-server" },
        },
      });
      repo.upsertUser({ provider: "plex", providerUserId: "owner-user", username: "owner", displayName: "Owner", isAdmin: true });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const start = await fetchHandler(new Request("http://app.test/login/plex/start", { method: "POST" }));
      expect(start.status).toBe(200);

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123"));
      const resultLocation = await plexResultLocation(complete, false);
      expect(complete.headers.get("set-cookie")).toBeNull();
      expect(resultLocation.searchParams.get("error")).toContain("owner token has expired");
      expect(resultLocation.searchParams.get("error")).toContain("admin needs to re-link");
      expect(resultLocation.href).not.toContain("new-user-token");
      expect(repo.listUsers("plex")).toHaveLength(1); // only admin, new user not added
      expect(repo.getPlexLoginAttempt(123)).toBeNull();

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("allows library RPC but denies admin pages and admin RPC to non-admin browser sessions", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
    });

    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: "plex-user-2",
      username: "bob",
      displayName: "Bob",
      isAdmin: false,
    });
    repo.createSession(user.id, hashSessionToken("session-token"), new Date(Date.now() + 60_000).toISOString());

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const cookieHeader = "podible_session=session-token";

    const library = await rpc(fetchHandler, "library.list", {}, 1, { cookie: cookieHeader });
    expect(library.error).toBeUndefined();
    expect(library.result.items).toEqual([]);

    const admin = await fetchHandler(
      new Request("http://app.test/admin", {
        headers: { cookie: cookieHeader },
      })
    );
    expect(admin.status).toBe(403);

    const rpcResponse = await fetchHandler(
      new Request("http://app.test/rpc", {
        method: "POST",
        headers: {
          cookie: cookieHeader,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "settings.get",
          params: {},
        }),
      })
    );
    expect(rpcResponse.status).toBe(200);
    const rpcPayload = (await rpcResponse.json()) as any;
    expect(rpcPayload.error.code).toBe(-32003);

    db.close();
  });

  test("catalog failures are RPC errors, not empty search results", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: unknown) => new Response("Internal Server Error", { status: 500 })) as typeof fetch;
    const db = new Database(":memory:");
    try {
      runMigrations(db);
      const repo = new BooksRepo(db);
      repo.ensureSettings();
      const handler = createPodibleFetchHandler(repo, Date.now());
      const cookie = createBrowserSessionCookie(repo, { username: "reader" });
      const found = await rpc(handler, "openlibrary.search", { q: "one day", limit: 50 }, 1, { cookie });
      expect(found.error.code).toBe(-32000);
      expect(found.error.message).toContain("Open Library search failed (HTTP 500)");
      expect(found.result).toBeUndefined();
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("catalog RPC returns fallback results after a large upstream search fails", async () => {
    const originalFetch = globalThis.fetch;
    const limits: string[] = [];
    globalThis.fetch = (async (input: unknown) => {
      const limit = new URL(String(input)).searchParams.get("limit")!;
      limits.push(limit);
      return limit === "50" ? new Response("Internal Server Error", { status: 500 }) : Response.json({
        docs: [{ key: "/works/OL123W", title: "One Day", author_name: ["David Nicholls"] }],
      });
    }) as typeof fetch;
    const db = new Database(":memory:");
    try {
      runMigrations(db);
      const repo = new BooksRepo(db);
      repo.ensureSettings();
      const handler = createPodibleFetchHandler(repo, Date.now());
      const cookie = createBrowserSessionCookie(repo, { username: "reader" });
      const found = await rpc(handler, "openlibrary.search", { q: "one day", limit: 50 }, 1, { cookie });
      expect(found.error).toBeUndefined();
      expect(found.result.results).toMatchObject([{ title: "One Day", author: "David Nicholls" }]);
      expect(limits).toEqual(["50", "10"]);
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("supports rpc openlibrary.search and add-by-key-from-search flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      const query = url.searchParams.get("q") ?? "";
      if (query === "Hyperion Dan Simmons" || query.includes("OL45804W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

      const found = await rpc(fetchHandler, "openlibrary.search", { q: "Hyperion Dan Simmons", limit: 5 }, 1, {
        cookie: userCookie,
      });
      expect(found.result.results.length).toBe(1);
      expect(found.result.results[0].openLibraryKey).toBe("/works/OL45804W");

      const created = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL45804W" }, 2, {
        cookie: userCookie,
      });
      expect(created.result.book.title).toBe("Hyperion");
      expect(created.result.book.author).toBe("Dan Simmons");
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL45804W");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc add-by-key flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if ((url.searchParams.get("q") ?? "").includes("OL82563W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL82563W",
                title: "To Kill a Mockingbird",
                author_name: ["Harper Lee"],
                first_publish_year: 1960,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
      const reader = repo.listUsers().find((user) => user.username === "reader");
      const created = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL82563W" }, 1, {
        cookie: userCookie,
      });

      expect(created.result.book.title).toBe("To Kill a Mockingbird");
      expect(created.result.book.author).toBe("Harper Lee");
      expect(created.result.book.addedByUserId).toBe(reader?.id);
      expect(created.result.book.addedByUser).toEqual({
        id: reader?.id,
        username: "reader",
        displayName: "reader",
        thumbUrl: null,
      });
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL82563W");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("reuses existing book row when add-by-key is repeated for the same Open Library work", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if ((url.searchParams.get("q") ?? "").includes("OL25432584W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL25432584W",
                title: "Tough Guy",
                author_name: ["Rachel Reid"],
                first_publish_year: 2020,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

      const first = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL25432584W" }, 1, {
        cookie: userCookie,
      });
      const second = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL25432584W" }, 2, {
        cookie: userCookie,
      });

      expect(first.result.book.id).toBe(second.result.book.id);
      expect(repo.listAllBooks().map((book: any) => book.title)).toEqual(["Tough Guy"]);
      expect(repo.listJobsByType("acquire").filter((job) => job.book_id === first.result.book.id).length).toBe(2);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports read-only GET rpc bridge and blocks mutating methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { wordCount: 188_000 });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const readRes = await fetchHandler(
      new Request(`http://localhost/rpc/library/get?bookId=${book.id}`, {
        headers: { cookie: userCookie },
      })
    );
    expect(readRes.status).toBe(200);
    const readJson = (await readRes.json()) as any;
    expect(readJson.jsonrpc).toBe("2.0");
    expect(readJson.id).toBeNull();
    expect(readJson.result.book.id).toBe(book.id);
    expect(readJson.result.book.wordCount).toBe(188000);

    const writeRes = await fetchHandler(
      new Request("http://localhost/rpc/settings/update?auth.mode=plex")
    );
    expect(writeRes.status).toBe(200);
    const writeJson = (await writeRes.json()) as any;
    expect(writeJson.error.code).toBe(-32601);

    db.close();
  });

  test("supports rpc library.rehydrate for existing books", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" && url.origin !== "https://covers.openlibrary.org") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }
      if (url.pathname === "/search.json" && (url.searchParams.get("q") ?? "") === "Neuromancer William Gibson") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45754W",
                title: "Neuromancer",
                author_name: ["William Gibson"],
                first_publish_year: 1984,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.pathname === "/works/OL45754W.json") {
        return new Response(
          JSON.stringify({
            description: "A seminal cyberpunk novel.",
            covers: [12345],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.origin === "https://covers.openlibrary.org") {
        return new Response(Buffer.from(TINY_PNG_BASE64, "base64"), {
          status: 200,
          headers: { "Content-Type": "image/png" },
        });
      }
      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      const libraryRoot = path.join(isolatedDataDir, "library-rehydrate");
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
        libraryRoot,
      });
      const book = repo.createBook({ title: "Neuromancer", author: "William Gibson" });
      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });

      const hydrated = await rpc(fetchHandler, "library.rehydrate", { bookId: book.id }, 1, {
        cookie: adminCookie,
      });
      expect(hydrated.result.attempted).toBe(1);
      expect(hydrated.result.updatedBookIds).toEqual([book.id]);

      const fetched = repo.getBook(book.id);
      expect(fetched?.identifiers.openlibrary).toBe("/works/OL45754W");
      expect(fetched?.description).toBe("A seminal cyberpunk novel.");
      expect(fetched?.descriptionHtml).toContain("<p>");
      expect(fetched?.coverUrl).toBe(`/covers/${book.id}.jpg`);
      if (fetched?.coverUrl) {
        const coverRes = await fetchHandler(
          new Request(`http://localhost${fetched.coverUrl}`, {
            headers: { cookie: adminCookie },
          })
        );
        expect(coverRes.status).toBe(200);
        expect((await coverRes.arrayBuffer()).byteLength).toBeGreaterThan(16);
      }

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc openlibrary.covers and openlibrary.setCover for existing books", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" && url.origin !== "https://covers.openlibrary.org") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }
      if (url.pathname === "/search.json" && (url.searchParams.get("q") ?? "") === "key:/works/OL17352669W") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL17352669W",
                title: "A Court of Thorns and Roses",
                author_name: ["Sarah J. Maas"],
                first_publish_year: 2015,
                language: ["eng"],
                cover_i: 111,
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.pathname === "/works/OL17352669W.json") {
        return new Response(JSON.stringify({ covers: [111, 222] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.pathname === "/works/OL17352669W/editions.json") {
        return new Response(
          JSON.stringify({
            entries: [
              {
                key: "/books/OL47309317M",
                title: "A Court of Thorns and Roses",
                covers: [222, 333],
                publish_date: "2020",
                publishers: ["Bloomsbury Publishing"],
                languages: [{ key: "/languages/eng" }],
                isbn_13: ["9781635575569"],
              },
              {
                key: "/books/OL28352792M",
                title: "A Court of Thorns and Roses",
                covers: [444],
                publish_date: "Nov 05, 2019",
                publishers: ["Bloomsbury YA"],
                languages: [{ key: "/languages/eng" }],
                isbn_13: ["9781547604173"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.origin === "https://covers.openlibrary.org" && url.pathname === "/b/id/444-L.jpg") {
        return new Response(Buffer.from(TINY_PNG_BASE64, "base64"), {
          status: 200,
          headers: { "Content-Type": "image/png" },
        });
      }
      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      const libraryRoot = path.join(isolatedDataDir, "library-cover-rpc");
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
        torznab: [],
        libraryRoot,
      });
      const book = repo.createBook({ title: "A Court of Thorns and Roses", author: "Sarah J. Maas" });
      repo.updateBookMetadata(book.id, { identifiers: { openlibrary: "/works/OL17352669W" } });
      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

      const covers = await rpc(fetchHandler, "openlibrary.covers", { bookId: book.id, limit: 10 }, 1, {
        cookie: userCookie,
      });
      expect(covers.result.results.map((candidate: any) => candidate.coverId)).toEqual([111, 222, 333, 444]);
      expect(covers.result.results[2]).toMatchObject({
        coverId: 333,
        coverUrl: "https://covers.openlibrary.org/b/id/333-L.jpg",
        source: "edition",
        editionKey: "/books/OL47309317M",
        publisher: "Bloomsbury Publishing",
        isbn: "9781635575569",
      });

      const applied = await rpc(fetchHandler, "openlibrary.setCover", { bookId: book.id, coverId: 444 }, 2, {
        cookie: userCookie,
      });
      expect(applied.result.cover.coverUrl).toBe("https://covers.openlibrary.org/b/id/444-L.jpg");
      expect(applied.result.book.coverUrl).toBe(`/covers/${book.id}.jpg`);

      const coverRes = await fetchHandler(
        new Request(`http://localhost/covers/${book.id}.jpg`, {
          headers: { cookie: userCookie },
        })
      );
      expect(coverRes.status).toBe(200);
      expect((await coverRes.arrayBuffer()).byteLength).toBeGreaterThan(16);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc library.acquire for existing books", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const acquire = await rpc(fetchHandler, "library.acquire", { bookId: book.id, media: ["ebook"] }, 1, {
      cookie: userCookie,
    });
    expect(acquire.result.jobId).toBeGreaterThan(0);
    expect(acquire.result.media).toEqual(["ebook"]);

    const job = repo.getJob(acquire.result.jobId);
    expect(job?.type).toBe("acquire");
    expect(job?.book_id).toBe(book.id);
    expect(JSON.parse(job?.payload_json ?? "{}").media).toEqual(["ebook"]);

    db.close();
  });

  test("serves stored transcript json for manifestations", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      manifestationId: manifestation.id,
      files: [
        {
          path: path.join(isolatedDataDir, "transcript-route.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    const transcriptFilePath = path.join(isolatedDataDir, "transcript-route.json");
    await Bun.write(
      transcriptFilePath,
      JSON.stringify({
        version: "1.2.0",
        text: "fear is the mind killer",
        words: [
          { startMs: 0, endMs: 300, text: "fear", token: "fear" },
          { startMs: 301, endMs: 500, text: "is", token: "is" },
        ],
      })
    );
    repo.upsertManifestationTranscript({
      manifestationId: manifestation.id,
      status: "succeeded",
      source: "full_transcript_epub",
      algorithmVersion: "test",
      fingerprint: "fp",
      transcriptPath: transcriptFilePath,
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const response = await fetchHandler(
      new Request(`http://localhost/transcripts/m/${manifestation.id}.json`, {
        headers: { cookie: userCookie },
      })
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("application/json");
    const payload = (await response.json()) as any;
    expect(payload.text).toBe("fear is the mind killer");
    expect(payload.words[0].text).toBe("fear");

    const missing = await fetchHandler(
      new Request("http://localhost/transcripts/m/999.json", {
        headers: { cookie: userCookie },
      })
    );
    expect(missing.status).toBe(404);

    db.close();
  });

  test("exposes opaque playback urls over rpc and serves manifestation transcripts", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio", label: "GraphicAudio dramatization" });
    const partOne = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mp4",
      totalSize: 100,
      durationMs: 1_000,
      manifestationId: manifestation.id,
      sequenceInManifestation: 0,
      files: [
        {
          path: path.join(isolatedDataDir, "red-rising-part-1.m4b"),
          size: 100,
          start: 0,
          end: 99,
          durationMs: 1_000,
          title: "Part 1",
        },
      ],
    });
    const partTwo = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mp4",
      totalSize: 200,
      durationMs: 2_000,
      manifestationId: manifestation.id,
      sequenceInManifestation: 1,
      files: [
        {
          path: path.join(isolatedDataDir, "red-rising-part-2.m4b"),
          size: 200,
          start: 0,
          end: 199,
          durationMs: 2_000,
          title: "Part 2",
        },
      ],
    });
    const manifestationTranscriptPath = path.join(isolatedDataDir, "red-rising-transcript.json");
    await Bun.write(
      manifestationTranscriptPath,
      JSON.stringify({
        version: "test",
        text: "part one\n\npart two",
        words: [
          { startMs: 0, endMs: 100, text: "part", token: "part" },
          { startMs: 1_000, endMs: 1_100, text: "two", token: "two" },
        ],
        utterances: [
          { startMs: 0, endMs: 100, text: "part one" },
          { startMs: 1_000, endMs: 1_100, text: "part two" },
        ],
      })
    );
    repo.upsertManifestationTranscript({
      manifestationId: manifestation.id,
      status: "succeeded",
      source: "whisper_transcript",
      algorithmVersion: "test",
      fingerprint: "combined",
      transcriptPath: manifestationTranscriptPath,
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const result = await rpc(fetchHandler, "library.get", { bookId: book.id }, 1, { cookie: userCookie });
    const playback = result.result.book.playback;
    expect(playback.audio.manifestationId).toBe(manifestation.id);
    expect(playback.audio.streamUrl).toBe(`http://localhost/stream/m/${manifestation.id}.m4a`);
    expect(playback.audio.chaptersUrl).toBe(`http://localhost/chapters/m/${manifestation.id}.json`);
    expect(playback.audio.transcriptUrl).toBe(`http://localhost/transcripts/m/${manifestation.id}.json`);
    expect(playback.audioOptions.map((option: any) => option.manifestationId)).toEqual([manifestation.id]);
    expect(playback.audioOptions[0].transcriptUrl).toBe(`http://localhost/transcripts/m/${manifestation.id}.json`);

    const transcript = await fetchHandler(
      new Request(playback.audio.transcriptUrl, {
        headers: { cookie: userCookie },
      })
    );
    expect(transcript.status).toBe(200);
    const transcriptBody = await transcript.json() as any;
    expect(transcriptBody.text).toBe("part one\n\npart two");
    expect(transcriptBody.words.map((word: any) => word.startMs)).toEqual([0, 1_000]);
    expect(transcriptBody.utterances.map((utterance: any) => utterance.startMs)).toEqual([0, 1_000]);

    db.close();
  });

  test("serves transcript json without app-level brotli recompression", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      manifestationId: manifestation.id,
      files: [
        {
          path: path.join(isolatedDataDir, "transcript-route-br.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    const transcriptFilePath2 = path.join(isolatedDataDir, "transcript-route-br.json");
    await Bun.write(
      transcriptFilePath2,
      JSON.stringify({
        version: "1.2.0",
        text: "fear is the mind killer",
        words: [
          { startMs: 0, endMs: 300, text: "fear", token: "fear" },
          { startMs: 301, endMs: 500, text: "is", token: "is" },
        ],
      })
    );
    repo.upsertManifestationTranscript({
      manifestationId: manifestation.id,
      status: "succeeded",
      source: "full_transcript_epub",
      algorithmVersion: "test",
      fingerprint: "fp-br",
      transcriptPath: transcriptFilePath2,
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const response = await fetchHandler(
      new Request(`http://localhost/transcripts/m/${manifestation.id}.json`, {
        headers: { "Accept-Encoding": "br", cookie: userCookie },
      })
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-encoding")).toBeNull();
    const payload = (await response.json()) as any;
    expect(payload.text).toBe("fear is the mind killer");
    expect(repo.getManifestationTranscript(manifestation.id)?.transcript_path).toBeTruthy();

    db.close();
  });
});
