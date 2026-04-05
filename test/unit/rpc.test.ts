import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomBytes } from "node:crypto";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import JSZip from "jszip";

import { runMigrations } from "../../src/books/db";
import { hashSessionToken } from "../../src/books/auth";
import { handleRpcMethod, handleRpcRequest } from "../../src/books/rpc";
import { BooksRepo } from "../../src/books/repo";
import { defaultSettings } from "../../src/books/settings";

type RpcCallerAuth = "none" | "user" | "admin";

function createRpcSession(repo: BooksRepo, role: Exclude<RpcCallerAuth, "none">) {
  const user = repo.upsertUser({
    provider: "plex",
    providerUserId: role,
    username: role,
    displayName: role,
    isAdmin: role === "admin",
  });
  return repo.createSession(
    user.id,
    hashSessionToken(`rpc-${role}-token-${randomBytes(8).toString("hex")}`),
    new Date(Date.now() + 60_000).toISOString()
  );
}

function makeRpcContext(repo: BooksRepo, request: Request, auth: RpcCallerAuth = "admin") {
  return {
    repo,
    startTime: Date.now() - 1000,
    request,
    session: auth === "none" ? null : createRpcSession(repo, auth),
  };
}

async function callRpc(repo: BooksRepo, body: string | object, auth: RpcCallerAuth = "admin") {
  const request = new Request("http://localhost/rpc", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  const response = await handleRpcRequest(request, makeRpcContext(repo, request, auth));
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

async function createMinimalEpub(filePath: string): Promise<void> {
  const zip = new JSZip();
  zip.file("mimetype", "application/epub+zip", { compression: "STORE" });
  zip.file(
    "META-INF/container.xml",
    `<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>`
  );
  zip.file(
    "OEBPS/content.opf",
    `<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Test Book</dc:title>
    <dc:creator>Test Author</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:test-book</dc:identifier>
  </metadata>
  <manifest>
    <item id="ch1" href="chapter1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>`
  );
  zip.file(
    "OEBPS/chapter1.xhtml",
    `<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body><p>One two three four five.</p></body></html>`
  );
  await writeFile(filePath, await zip.generateAsync({ type: "uint8array" }));
}

describe("json-rpc handler", () => {
  test("dispatches representative methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const settings = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "settings.get",
      params: {},
    });
    expect(settings.result.auth).toBeTruthy();

    const listed = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.list",
      params: { limit: 10 },
    });
    expect(Array.isArray(listed.result.items)).toBe(true);

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const acquire = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 22,
      method: "library.acquire",
      params: {
        bookId: book.id,
        media: ["audio"],
        forceAgent: true,
        priorFailure: true,
        rejectedUrls: ["https://example.com/bad.torrent"],
      },
    });
    expect(acquire.result.jobId).toBeGreaterThan(0);
    expect(acquire.result.media).toEqual(["audio"]);
    expect(acquire.result.forceAgent).toBe(true);
    expect(acquire.result.priorFailure).toBe(true);
    expect(acquire.result.rejectedUrls).toEqual(["https://example.com/bad.torrent"]);
    expect(acquire.result.rejectedGuids).toEqual([]);
    expect(acquire.result.rejectedInfoHashes).toEqual([]);
    const acquireJob = repo.getJob(acquire.result.jobId);
    expect(acquireJob?.type).toBe("acquire");
    expect(acquireJob?.book_id).toBe(book.id);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").media).toEqual(["audio"]);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").forceAgent).toBe(true);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").priorFailure).toBe(true);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").rejectedUrls).toEqual(["https://example.com/bad.torrent"]);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").rejectedGuids).toEqual([]);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").rejectedInfoHashes).toEqual([]);

    const queued = repo.createJob({ type: "full_library_refresh" });
    const chapter = repo.createJob({ type: "chapter_analysis", bookId: book.id, payload: { assetId: 1, ebookAssetId: 2 } });
    const jobs = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 3,
      method: "jobs.list",
      params: { limit: 5 },
    });
    expect(Array.isArray(jobs.result.jobs)).toBe(true);
    expect(jobs.result.jobs[0].id).toBe(chapter.id);
    expect(jobs.result.jobs.some((job: { type: string }) => job.type === "chapter_analysis")).toBe(true);

    const filteredJobs = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 4,
      method: "jobs.list",
      params: { limit: 5, type: "chapter_analysis" },
    });
    expect(filteredJobs.result.jobs).toHaveLength(1);
    expect(filteredJobs.result.jobs[0].id).toBe(chapter.id);
    expect(filteredJobs.result.jobs[0].type).toBe("chapter_analysis");

    db.close();
  });

  test("library.inProgress returns LibraryBook-shaped rows and filters terminal books", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const wanted = repo.createBook({ title: "Wanted Book", author: "Author" });

    const partial = repo.createBook({ title: "Partial Book", author: "Author" });
    const partialAudio = repo.createRelease({
      bookId: partial.id,
      provider: "test",
      title: "Partial audio",
      mediaType: "audio",
      infoHash: "1111111111111111111111111111111111111111",
      url: "https://example.com/partial-audio.torrent",
      status: "imported",
    });
    repo.addAsset({
      bookId: partial.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 100,
      sourceReleaseId: partialAudio.id,
      files: [
        {
          path: "/tmp/partial-audio.mp3",
          size: 100,
          start: 0,
          end: 99,
          durationMs: 1000,
          title: null,
        },
      ],
    });

    const imported = repo.createBook({ title: "Imported Book", author: "Author" });
    const importedAudio = repo.createRelease({
      bookId: imported.id,
      provider: "test",
      title: "Imported audio",
      mediaType: "audio",
      infoHash: "2222222222222222222222222222222222222222",
      url: "https://example.com/imported-audio.torrent",
      status: "imported",
    });
    repo.addAsset({
      bookId: imported.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 100,
      sourceReleaseId: importedAudio.id,
      files: [
        {
          path: "/tmp/imported-audio.mp3",
          size: 100,
          start: 0,
          end: 99,
          durationMs: 1000,
          title: null,
        },
      ],
    });
    const importedEbook = repo.createRelease({
      bookId: imported.id,
      provider: "test",
      title: "Imported ebook",
      mediaType: "ebook",
      infoHash: "3333333333333333333333333333333333333333",
      url: "https://example.com/imported-ebook.torrent",
      status: "imported",
    });
    repo.addAsset({
      bookId: imported.id,
      kind: "ebook",
      mime: "application/epub+zip",
      totalSize: 100,
      sourceReleaseId: importedEbook.id,
      files: [
        {
          path: "/tmp/imported-ebook.epub",
          size: 100,
          start: 0,
          end: 99,
          durationMs: 0,
          title: null,
        },
      ],
    });

    const errored = repo.createBook({ title: "Errored Book", author: "Author" });
    repo.createRelease({
      bookId: errored.id,
      provider: "test",
      title: "Errored audio",
      mediaType: "audio",
      infoHash: "4444444444444444444444444444444444444444",
      url: "https://example.com/errored-audio.torrent",
      status: "failed",
    });
    repo.createRelease({
      bookId: errored.id,
      provider: "test",
      title: "Errored ebook",
      mediaType: "ebook",
      infoHash: "5555555555555555555555555555555555555555",
      url: "https://example.com/errored-ebook.torrent",
      status: "failed",
    });

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.inProgress",
      params: {},
    });

    expect(Array.isArray(result.result.items)).toBe(true);
    const rows = result.result.items as Array<any>;
    const ids = rows.map((row) => row.id).sort((a, b) => a - b);
    expect(ids).toEqual([wanted.id, partial.id]);

    const partialRow = rows.find((row) => row.id === partial.id);
    expect(partialRow.status).toBe("partial");
    expect(partialRow.audioStatus).toBe("imported");
    expect(partialRow.ebookStatus).toBe("wanted");
    expect(typeof partialRow.fullPseudoProgress).toBe("number");
    expect(typeof partialRow.updatedAt).toBe("string");
    expect(partialRow.title).toBe("Partial Book");
    expect(partialRow.author).toBe("Author");
    expect(typeof partialRow.identifiers).toBe("object");

    const filtered = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.inProgress",
      params: { bookIds: [imported.id, partial.id] },
    });
    expect(filtered.result.items.map((row: any) => row.id)).toEqual([partial.id]);

    db.close();
  });

  test("library.inProgress includes error books when active recovery jobs exist", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const book = repo.createBook({ title: "Twilight", author: "Stephenie Meyer" });
    repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight audio failed",
      mediaType: "audio",
      infoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      url: "https://example.com/twilight-audio.torrent",
      status: "failed",
    });
    repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight ebook failed",
      mediaType: "ebook",
      infoHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      url: "https://example.com/twilight-ebook.torrent",
      status: "failed",
    });

    const errored = repo.getBook(book.id);
    expect(errored?.status).toBe("error");

    let result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.inProgress",
      params: { bookIds: [book.id] },
    });
    expect(result.result.items).toEqual([]);

    const recoveryJob = repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: { bookId: book.id, media: ["audio"] },
    });

    result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.inProgress",
      params: { bookIds: [book.id] },
    });
    expect(result.result.items.length).toBe(1);
    expect(result.result.items[0].id).toBe(book.id);
    expect(result.result.items[0].status).toBe("error");

    repo.markJobSucceeded(recoveryJob.id);
    result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 3,
      method: "library.inProgress",
      params: { bookIds: [book.id] },
    });
    expect(result.result.items).toEqual([]);

    db.close();
  });

  test("admin.wipeDatabase clears mutable tables and preserves settings", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const baselineSettings = repo.ensureSettings();
    repo.updateSettings({
      ...baselineSettings,
      feed: { ...baselineSettings.feed, title: "Books Test Feed" },
    });

    const root = await mkdtemp(path.join(os.tmpdir(), "books-wipe-"));
    const assetPath = path.join(root, "library", "Dune.epub");
    const coverPath = path.join(root, "library", "Dune.jpg");
    await mkdir(path.dirname(assetPath), { recursive: true });
    await writeFile(assetPath, Buffer.from("epub"));
    await writeFile(coverPath, Buffer.from([0xff, 0xd8, 0xff, 0xd9]));

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { coverPath });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "manual",
      providerGuid: null,
      title: "Dune.epub",
      mediaType: "ebook",
      infoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      url: "/tmp/Dune.epub",
      status: "imported",
    });
    repo.addAsset({
      bookId: book.id,
      kind: "ebook",
      mime: "application/epub+zip",
      totalSize: 123,
      sourceReleaseId: release.id,
      files: [
        {
          path: assetPath,
          size: 123,
          start: 0,
          end: 122,
          durationMs: 0,
          title: "Dune",
        },
      ],
    });
    const audioAsset = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 456,
      durationMs: 1000,
      files: [
        {
          path: path.join(root, "library", "Dune.mp3"),
          size: 456,
          start: 0,
          end: 455,
          durationMs: 1000,
          title: "Dune Audio",
        },
      ],
    });
    repo.upsertChapterAnalysis({
      assetId: audioAsset.id,
      status: "succeeded",
      source: "epub_ai",
      algorithmVersion: "test",
      fingerprint: "fp",
      transcriptFingerprint: "tfp",
      chaptersJson: JSON.stringify({
        version: "1.2.0",
        chapters: [{ id: "ch0", title: "Chapter 1", startMs: 0, endMs: 1000 }],
      }),
      resolvedBoundaryCount: 1,
      totalBoundaryCount: 1,
    });
    repo.upsertAssetTranscript({
      assetId: audioAsset.id,
      status: "succeeded",
      source: "whisper",
      algorithmVersion: "test",
      fingerprint: "tfp",
      transcriptJson: JSON.stringify({
        version: "1.2.0",
        text: "hello world",
        words: [{ startMs: 0, endMs: 500, text: "hello", token: "hello" }],
      }),
    });
    repo.createJob({ type: "acquire", bookId: book.id });
    repo.setJsonState("probe_cache_v1", [{ file: assetPath, mtimeMs: 123, data: null, error: "boom" }]);

    const wiped = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 99,
      method: "admin.wipeDatabase",
      params: {},
    });

    expect(wiped.result.settingsPreserved).toBe(true);
    expect(wiped.result.deleted.books).toBe(1);
    expect(wiped.result.deleted.releases).toBe(1);
    expect(wiped.result.deleted.assets).toBe(2);
    expect(wiped.result.deleted.assetFiles).toBe(2);
    expect(wiped.result.deleted.jobs).toBe(1);
    expect(wiped.result.deleted.chapterAnalysis).toBe(1);
    expect(wiped.result.deleted.assetTranscripts).toBe(1);
    expect(wiped.result.deleted.appState).toBe(1);
    expect(wiped.result.deletedAssetFileCount).toBe(2);
    expect(wiped.result.deletedAssetPaths).toEqual([assetPath, path.join(root, "library", "Dune.mp3")]);
    expect(wiped.result.deletedCoverFileCount).toBe(1);
    expect(wiped.result.deletedCoverPaths).toEqual([coverPath]);
    expect(repo.listBooks(10).items).toHaveLength(0);
    expect(repo.listReleasesByBook(book.id)).toHaveLength(0);
    expect(repo.listJobsByType("acquire")).toHaveLength(0);
    expect(repo.getJsonState("probe_cache_v1")).toBeNull();
    expect(repo.getHealthSummary().queueSize).toBe(0);
    expect(repo.getSettings().feed.title).toBe("Books Test Feed");
    expect(await Bun.file(assetPath).exists()).toBe(false);
    expect(await Bun.file(coverPath).exists()).toBe(false);

    const nextBook = repo.createBook({ title: "Hyperion", author: "Dan Simmons" });
    expect(nextBook.id).toBe(1);

    db.close();
  });

  test("jobs.retry requeues failed jobs and rejects non-retryable jobs", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const failed = repo.createJob({ type: "acquire", status: "failed" });
    const retried = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "jobs.retry",
      params: { jobId: failed.id },
    });
    expect(retried.result.job.id).toBe(failed.id);
    expect(retried.result.job.status).toBe("queued");

    const queued = repo.createJob({ type: "acquire", status: "queued" });
    const notRetryable = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "jobs.retry",
      params: { jobId: queued.id },
    });
    expect(notRetryable.error.code).toBe(-32000);
    expect(notRetryable.error.data.error).toBe("not_retryable");

    db.close();
  });

  test("help lists rpc methods with readOnly flags", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "help",
      params: {},
    });

    expect(result.result.name).toBe("podible-rpc");
    expect(result.result.version).toBe("v1");
    expect(Array.isArray(result.result.methods)).toBe(true);
    expect(result.result.methods.some((m: any) => m.name === "help" && m.readOnly === true)).toBe(true);
    expect(result.result.methods.some((m: any) => m.name === "library.create" && m.readOnly === false)).toBe(true);
    expect(result.result.methods.some((m: any) => m.name === "system.health")).toBe(true);

    db.close();
  });

  test("library.reportImportIssue queues async wrong-file review import job", async () => {
    const originalFetch = globalThis.fetch;
    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);

      const root = await mkdtemp(path.join(os.tmpdir(), "books-report-issue-"));
      const badPath = path.join(root, "download");
      const libraryPath = path.join(root, "library");
      await mkdir(badPath, { recursive: true });
      await writeFile(path.join(badPath, "readme.txt"), Buffer.from("wrong payload"));
      await mkdir(libraryPath, { recursive: true });
      const importedPath = path.join(libraryPath, "wrong.mp3");
      await writeFile(importedPath, Buffer.from("wrong import link target"));
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "plex" },
          libraryRoot: libraryPath,
          rtorrent: {
            transport: "http-xmlrpc",
            url: "http://mock.local/RPC2",
            username: "",
            password: "",
          },
          agents: {
            ...defaultSettings().agents,
            apiKey: "",
          },
        })
      );

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const release = repo.createRelease({
        bookId: book.id,
        provider: "mock",
        providerGuid: "guid-1",
        title: "Dune Wrong",
        mediaType: "audio",
        infoHash: "abc123",
        url: "https://example.com/wrong-audio.torrent",
        status: "imported",
      });
      const asset = repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 12,
        sourceReleaseId: release.id,
        files: [
          {
            path: importedPath,
            sourcePath: path.join(badPath, "Dune - Part 01.mp3"),
            size: 12,
            start: 0,
            end: 11,
            durationMs: 1000,
            title: "Part 1",
          },
        ],
      });

      // Force base_path to our temp folder.
      globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
        const body = String(init?.body ?? "");
        const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
        const xml = (() => {
          switch (method) {
            case "d.name":
              return '<?xml version="1.0"?><methodResponse><params><param><value><string>Dune</string></value></param></params></methodResponse>';
            case "d.hash":
              return '<?xml version="1.0"?><methodResponse><params><param><value><string>ABC123</string></value></param></params></methodResponse>';
            case "d.complete":
              return '<?xml version="1.0"?><methodResponse><params><param><value><i8>1</i8></value></param></params></methodResponse>';
            case "d.base_path":
              return `<?xml version="1.0"?><methodResponse><params><param><value><string>${badPath}</string></value></param></params></methodResponse>`;
            case "d.bytes_done":
              return '<?xml version="1.0"?><methodResponse><params><param><value><i8>100</i8></value></param></params></methodResponse>';
            case "d.size_bytes":
              return '<?xml version="1.0"?><methodResponse><params><param><value><i8>100</i8></value></param></params></methodResponse>';
            case "d.left_bytes":
              return '<?xml version="1.0"?><methodResponse><params><param><value><i8>0</i8></value></param></params></methodResponse>';
            case "d.down.rate":
              return '<?xml version="1.0"?><methodResponse><params><param><value><i8>0</i8></value></param></params></methodResponse>';
            default:
              return '<?xml version="1.0"?><methodResponse><params><param><value><string></string></value></param></params></methodResponse>';
          }
        })();
        return new Response(xml, { status: 200, headers: { "Content-Type": "text/xml" } });
      }) as unknown as typeof fetch;

      const result = await callRpc(repo, {
        jsonrpc: "2.0",
        id: 1,
        method: "library.reportImportIssue",
        params: {
          bookId: book.id,
          mediaType: "audio",
        },
      });

      expect(result.result.action).toBe("wrong_file_review_queued");
      expect(result.result.releaseId).toBe(release.id);
      expect(result.result.mediaType).toBe("audio");
      expect(result.result.jobId).toBeGreaterThan(0);
      expect(result.result.rejectedSourcePathsCount).toBe(1);
      expect(result.result.deletedAssetCount).toBe(1);
      expect(result.result.deletedAssetFileCount).toBe(1);
      expect(result.result.deletedAssetPaths).toEqual([importedPath]);

      const failedRelease = repo.getRelease(release.id);
      expect(failedRelease?.status).toBe("downloaded");
      expect(repo.getAsset(asset.id)).toBeNull();
      expect(await Bun.file(importedPath).exists()).toBe(false);

      const reviewJob = repo.getJob(result.result.jobId);
      expect(reviewJob?.type).toBe("import");
      expect(reviewJob?.release_id).toBe(release.id);
      const payload = JSON.parse(reviewJob?.payload_json ?? "{}");
      expect(payload.reason).toBe("user_reported_wrong_file");
      expect(payload.userReportedIssue).toBe(true);
      expect(payload.rejectedSourcePaths).toEqual([path.join(badPath, "Dune - Part 01.mp3")]);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("library.reportImportIssue targets imported release when releaseId is omitted", async () => {
    const originalFetch = globalThis.fetch;
    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "plex" },
          rtorrent: {
            transport: "http-xmlrpc",
            url: "http://mock.local/RPC2",
            username: "",
            password: "",
          },
          agents: {
            ...defaultSettings().agents,
            apiKey: "",
          },
        })
      );

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const importedRelease = repo.createRelease({
        bookId: book.id,
        provider: "mock",
        providerGuid: "guid-imported",
        title: "Dune Imported",
        mediaType: "audio",
        infoHash: "1111111111111111111111111111111111111111",
        url: "https://example.com/imported-audio.torrent",
        status: "imported",
      });
      const root = await mkdtemp(path.join(os.tmpdir(), "books-report-issue-target-"));
      const importedPath = path.join(root, "Dune.mp3");
      await writeFile(importedPath, Buffer.from("wrong import"));
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 1234,
        sourceReleaseId: importedRelease.id,
        files: [
          {
            path: importedPath,
            sourcePath: "/downloads/raw/Dune/Dune - Part 01.mp3",
            size: 1234,
            start: 0,
            end: 1233,
            durationMs: 1000,
            title: "Part 1",
          },
        ],
      });
      const newerRelease = repo.createRelease({
        bookId: book.id,
        provider: "mock",
        providerGuid: "guid-newer",
        title: "Dune Newer Attempt",
        mediaType: "audio",
        infoHash: "2222222222222222222222222222222222222222",
        url: "https://example.com/newer-audio.torrent",
        status: "snatched",
      });

      globalThis.fetch = (async () => {
        throw new Error("rTorrent unavailable");
      }) as unknown as typeof fetch;

      const result = await callRpc(repo, {
        jsonrpc: "2.0",
        id: 1,
        method: "library.reportImportIssue",
        params: {
          bookId: book.id,
          mediaType: "audio",
        },
      });

      expect(result.result.action).toBe("wrong_file_review_queued");
      expect(result.result.releaseId).toBe(importedRelease.id);
      expect(result.result.releaseId).not.toBe(newerRelease.id);
      expect(result.result.deletedAssetCount).toBe(1);

      const failedImported = repo.getRelease(importedRelease.id);
      expect(failedImported?.status).toBe("downloaded");
      const untouchedNewer = repo.getRelease(newerRelease.id);
      expect(untouchedNewer?.status).toBe("snatched");
      expect(repo.listAssetsByBook(book.id)).toHaveLength(0);
      expect(await Bun.file(importedPath).exists()).toBe(false);

      const reviewJob = repo.getJob(result.result.jobId);
      expect(reviewJob?.type).toBe("import");
      expect(reviewJob?.release_id).toBe(importedRelease.id);
      const payload = JSON.parse(reviewJob?.payload_json ?? "{}");
      expect(payload.rejectedSourcePaths).toEqual(["/downloads/raw/Dune/Dune - Part 01.mp3"]);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("returns parse error for malformed json", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, "{");
    expect(result.error.code).toBe(-32700);
    db.close();
  });

  test("rejects batch requests", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, [
      {
        jsonrpc: "2.0",
        id: 1,
        method: "settings.get",
        params: {},
      },
    ]);
    expect(result.error.code).toBe(-32600);
    db.close();
  });

  test("returns method not found", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "nope.method",
      params: {},
    });
    expect(result.error.code).toBe(-32601);
    db.close();
  });

  test("returns invalid params", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "search.run",
      params: { query: "Dune", media: "video" },
    });
    expect(result.error.code).toBe(-32602);
    db.close();
  });

  test("requires valid params for library.acquire", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const missingBook = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.acquire",
      params: { bookId: 999 },
    });
    expect(missingBook.error.code).toBe(-32000);

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const invalidMedia = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.acquire",
      params: { bookId: book.id, media: "video" },
    });
    expect(invalidMedia.error.code).toBe(-32602);

    db.close();
  });

  test("requires openLibraryKey for library.create", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.create",
      params: { title: "Dune" },
    });
    expect(result.error.code).toBe(-32602);
    db.close();
  });

  test("maps domain errors to -32000", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.get",
      params: { bookId: 999 },
    });
    expect(result.error.code).toBe(-32000);
    db.close();
  });

  test("library.delete cascades DB rows and removes asset+cover files", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "books-delete-"));
    const assetPath = path.join(root, "book.mp3");
    const coverPath = path.join(root, "cover.jpg");
    await writeFile(assetPath, Buffer.from("test-audio"));
    await writeFile(coverPath, Buffer.from([0xff, 0xd8, 0xff, 0xd9]));

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { coverPath });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Dune audio",
      mediaType: "audio",
      infoHash: "abc123",
      url: "https://example.com/dune-audio.torrent",
      status: "downloaded",
    });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 10,
      sourceReleaseId: release.id,
      files: [
        {
          path: assetPath,
          size: 10,
          start: 0,
          end: 9,
          durationMs: 1000,
          title: null,
        },
      ],
    });

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.delete",
      params: { bookId: book.id },
    });
    expect(result.result.deletedBookId).toBe(book.id);
    expect(result.result.deletedAssetFileCount).toBe(1);
    expect(repo.getBook(book.id)).toBeNull();
    expect(repo.listReleasesByBook(book.id)).toEqual([]);
    expect(repo.listAssetsByBook(book.id)).toEqual([]);

    expect(await Bun.file(assetPath).exists()).toBe(false);
    expect(await Bun.file(coverPath).exists()).toBe(false);

    db.close();
  });

  test("downloads.get returns live progress while downloading", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
      const body = String(init?.body ?? "");
      const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
      const xml = (() => {
        switch (method) {
          case "d.name":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>Dune</string></value></param></params></methodResponse>';
          case "d.hash":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>ABC</string></value></param></params></methodResponse>';
          case "d.complete":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>0</i8></value></param></params></methodResponse>';
          case "d.base_path":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>/downloads/dune</string></value></param></params></methodResponse>';
          case "d.bytes_done":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>500</i8></value></param></params></methodResponse>';
          case "d.size_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>1000</i8></value></param></params></methodResponse>';
          case "d.left_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>500</i8></value></param></params></methodResponse>';
          case "d.down.rate":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>100</i8></value></param></params></methodResponse>';
          default:
            return '<?xml version="1.0"?><methodResponse><params><param><value><string></string></value></param></params></methodResponse>';
        }
      })();
      return new Response(xml, { status: 200, headers: { "Content-Type": "text/xml" } });
    }) as unknown as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "plex" },
          rtorrent: {
            transport: "http-xmlrpc",
            url: "http://mock.local/RPC2",
            username: "",
            password: "",
          },
        })
      );

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const release = repo.createRelease({
        bookId: book.id,
        provider: "test",
        title: "Dune Audio",
        mediaType: "audio",
        infoHash: "abc123",
        url: "https://example.com/dune.torrent",
        status: "downloading",
      });
      const job = repo.createJob({ type: "download", releaseId: release.id, bookId: book.id });

      const result = await callRpc(repo, {
        jsonrpc: "2.0",
        id: 1,
        method: "downloads.get",
        params: { jobId: job.id },
      });
      expect(result.result.release_status).toBe("downloading");
      expect(result.result.downloadProgress.bytesDone).toBe(500);
      expect(result.result.downloadProgress.sizeBytes).toBe(1000);
      expect(result.result.downloadProgress.fraction).toBe(0.5);
      expect(result.result.downloadProgress.percent).toBe(50);
      expect(result.result.fullPseudoProgress).toBe(55);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("library.get and library.inProgress use live download fraction for book fullPseudoProgress", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
      const body = String(init?.body ?? "");
      const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
      const xml = (() => {
        switch (method) {
          case "d.name":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>Twilight Audio</string></value></param></params></methodResponse>';
          case "d.hash":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>ABC123ABC123ABC123ABC123ABC123ABC123ABCD</string></value></param></params></methodResponse>';
          case "d.complete":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>0</i8></value></param></params></methodResponse>';
          case "d.base_path":
            return '<?xml version="1.0"?><methodResponse><params><param><value><string>/downloads/twilight-audio</string></value></param></params></methodResponse>';
          case "d.bytes_done":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>500</i8></value></param></params></methodResponse>';
          case "d.size_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>1000</i8></value></param></params></methodResponse>';
          case "d.left_bytes":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>500</i8></value></param></params></methodResponse>';
          case "d.down.rate":
            return '<?xml version="1.0"?><methodResponse><params><param><value><i8>100</i8></value></param></params></methodResponse>';
          default:
            return '<?xml version="1.0"?><methodResponse><params><param><value><string></string></value></param></params></methodResponse>';
        }
      })();
      return new Response(xml, { status: 200, headers: { "Content-Type": "text/xml" } });
    }) as unknown as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "plex" },
          rtorrent: {
            transport: "http-xmlrpc",
            url: "http://mock.local/RPC2",
            username: "",
            password: "",
          },
        })
      );

      const book = repo.createBook({ title: "Twilight", author: "Stephenie Meyer" });
      repo.createRelease({
        bookId: book.id,
        provider: "test",
        title: "Twilight Audio",
        mediaType: "audio",
        infoHash: "abc123abc123abc123abc123abc123abc123abcd",
        url: "https://example.com/twilight-audio.torrent",
        status: "downloading",
      });
      const ebookRelease = repo.createRelease({
        bookId: book.id,
        provider: "test",
        title: "Twilight Ebook",
        mediaType: "ebook",
        infoHash: "def123def123def123def123def123def123def1",
        url: "https://example.com/twilight-ebook.torrent",
        status: "imported",
      });
      repo.addAsset({
        bookId: book.id,
        kind: "ebook",
        mime: "application/epub+zip",
        totalSize: 123,
        sourceReleaseId: ebookRelease.id,
        files: [
          {
            path: "/tmp/twilight.epub",
            size: 123,
            start: 0,
            end: 122,
            durationMs: 0,
            title: null,
          },
        ],
      });

      const baseline = repo.getBook(book.id);
      expect(baseline?.status).toBe("partial");
      expect(baseline?.audioStatus).toBe("downloading");
      expect(baseline?.ebookStatus).toBe("imported");
      expect(baseline?.fullPseudoProgress).toBe(60);

      const getResult = await callRpc(repo, {
        jsonrpc: "2.0",
        id: 1,
        method: "library.get",
        params: { bookId: book.id },
      });
      expect(getResult.result.book.status).toBe("partial");
      expect(getResult.result.book.audioStatus).toBe("downloading");
      expect(getResult.result.book.ebookStatus).toBe("imported");
      expect(getResult.result.book.fullPseudoProgress).toBe(77.5);

      const inProgressResult = await callRpc(repo, {
        jsonrpc: "2.0",
        id: 2,
        method: "library.inProgress",
        params: { bookIds: [book.id] },
      });
      expect(inProgressResult.result.items.length).toBe(1);
      expect(inProgressResult.result.items[0].id).toBe(book.id);
      expect(inProgressResult.result.items[0].fullPseudoProgress).toBe(77.5);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("import.manual creates a release and imports local file", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "books-manual-import-"));
    const sourceDir = path.join(root, "source");
    const libraryRoot = path.join(root, "library");
    await mkdir(sourceDir, { recursive: true });
    await mkdir(libraryRoot, { recursive: true });
    const sourceFile = path.join(sourceDir, "twilight.epub");
    await createMinimalEpub(sourceFile);

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        libraryRoot,
      })
    );

    const book = repo.createBook({ title: "Twilight", author: "Stephenie Meyer" });
    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "import.manual",
      params: {
        bookId: book.id,
        mediaType: "ebook",
        path: sourceFile,
      },
    });

    expect(result.result.release.book_id).toBe(book.id);
    expect(result.result.release.status).toBe("imported");
    expect(result.result.assetId).toBeGreaterThan(0);

    const assets = repo.listAssetsByBook(book.id);
    expect(assets.length).toBe(1);
    const files = repo.getAssetFiles(assets[0].id);
    expect(files.length).toBe(1);
    expect(await Bun.file(files[0].path).exists()).toBe(true);

    db.close();
  });

  test("agent.search.plan returns deterministic planning payload", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        torznab: [],
      })
    );

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "agent.search.plan",
      params: {
        query: "Dune Frank Herbert",
        media: "audio",
      },
    });

    expect(result.result.resultCount).toBe(0);
    expect(result.result.decision.mode).toBe("deterministic");
    expect(result.result.decision.candidate).toBeNull();

    db.close();
  });

  test("import.inspect lists files and import.manual supports selectedPaths", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "books-manual-select-"));
    const sourceDir = path.join(root, "source");
    const libraryRoot = path.join(root, "library");
    await mkdir(sourceDir, { recursive: true });
    await mkdir(libraryRoot, { recursive: true });

    const part1 = path.join(sourceDir, "01-intro.epub");
    const part2 = path.join(sourceDir, "02-main.pdf");
    const notes = path.join(sourceDir, "notes.txt");
    await writeFile(part1, Buffer.from("epub-part-1"));
    await writeFile(part2, Buffer.from("pdf-part-2"));
    await writeFile(notes, Buffer.from("ignore-me"));

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        libraryRoot,
      })
    );

    const inspect = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "import.inspect",
      params: { path: sourceDir },
    });
    expect(inspect.result.path).toBe(sourceDir);
    expect(Array.isArray(inspect.result.files)).toBe(true);
    expect(inspect.result.files.length).toBe(3);
    const ebookFiles = inspect.result.files.filter((file: any) => file.supportedEbook);
    expect(ebookFiles.length).toBe(2);

    const book = repo.createBook({ title: "Box Set", author: "Example Author" });
    const imported = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "import.manual",
      params: {
        bookId: book.id,
        mediaType: "ebook",
        path: sourceDir,
        selectedPaths: [part2],
      },
    });

    expect(imported.result.release.status).toBe("imported");
    const assets = repo.listAssetsByBook(book.id);
    expect(assets.length).toBe(1);
    expect(assets[0].kind).toBe("ebook");
    const files = repo.getAssetFiles(assets[0].id);
    expect(files.length).toBe(1);
    expect(path.basename(files[0].path)).toContain("Box Set.pdf");

    db.close();
  });

  test("agent.import.plan returns deterministic selected paths", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "books-agent-import-plan-"));
    const sourceDir = path.join(root, "source");
    await mkdir(sourceDir, { recursive: true });
    const sourceFile = path.join(sourceDir, "book.epub");
    await writeFile(sourceFile, Buffer.from("epub-bytes"));

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "agent.import.plan",
      params: {
        path: sourceDir,
        mediaType: "ebook",
      },
    });

    expect(result.result.fileCount).toBe(1);
    expect(result.result.decision.mode).toBe("deterministic");
    expect(result.result.decision.selectedPaths).toEqual([sourceFile]);

    db.close();
  });

  test("blocks write methods in read-only rpc dispatch", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const request = new Request("http://localhost/rpc/settings/update");
    const response = await handleRpcMethod("settings.update", {}, makeRpcContext(repo, request), { readOnly: true });
    expect(response.status).toBe(200);
    const payload = (await response.json()) as any;
    expect(payload.error.code).toBe(-32601);
    db.close();
  });

  test("enforces public, user, and admin rpc auth levels", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();

    const publicRequest = new Request("http://localhost/rpc/help");
    const publicResponse = await handleRpcMethod("help", {}, makeRpcContext(repo, publicRequest, "none"));
    const publicPayload = (await publicResponse.json()) as any;
    expect(publicPayload.result.name).toBe("podible-rpc");

    const userRequest = new Request("http://localhost/rpc/library/list");
    const unauthenticatedUserResponse = await handleRpcMethod("library.list", {}, makeRpcContext(repo, userRequest, "none"));
    const unauthenticatedUserPayload = (await unauthenticatedUserResponse.json()) as any;
    expect(unauthenticatedUserPayload.error.code).toBe(-32001);

    const adminRequest = new Request("http://localhost/rpc/settings/get");
    const nonAdminResponse = await handleRpcMethod("settings.get", {}, makeRpcContext(repo, adminRequest, "user"));
    const nonAdminPayload = (await nonAdminResponse.json()) as any;
    expect(nonAdminPayload.error.code).toBe(-32003);

    const adminResponse = await handleRpcMethod("settings.get", {}, makeRpcContext(repo, adminRequest, "admin"));
    const adminPayload = (await adminResponse.json()) as any;
    expect(adminPayload.result.auth.mode).toBe("plex");

    db.close();
  });

  test("auth.beginAppLogin validates redirect URIs and auth.exchange returns an app session", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings(
      defaultSettings({
        ...settings,
        auth: {
          ...settings.auth,
          appRedirectURIs: ["kindling://auth/podible"],
        },
      })
    );

    const beginRequest = new Request("http://localhost/rpc");
    const invalidBegin = await handleRpcMethod(
      "auth.beginAppLogin",
      { redirectUri: "kindling://evil/callback" },
      makeRpcContext(repo, beginRequest, "none")
    );
    const invalidPayload = (await invalidBegin.json()) as any;
    expect(invalidPayload.error.code).toBe(-32602);

    const begin = await handleRpcMethod(
      "auth.beginAppLogin",
      { redirectUri: "kindling://auth/podible" },
      makeRpcContext(repo, beginRequest, "none")
    );
    const beginPayload = (await begin.json()) as any;
    expect(beginPayload.result.authorizeUrl).toContain("/auth/app/");
    expect(beginPayload.result.state).toBeTruthy();

    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: "plex-user-1",
      username: "reader",
      displayName: "Reader",
      isAdmin: false,
    });
    repo.createAuthCode({
      codeHash: hashSessionToken("one-time-code"),
      userId: user.id,
      attemptId: beginPayload.result.attemptId,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });

    const exchangeRequest = new Request("http://localhost/rpc");
    const exchange = await handleRpcMethod(
      "auth.exchange",
      { code: "one-time-code" },
      makeRpcContext(repo, exchangeRequest, "none")
    );
    const exchangePayload = (await exchange.json()) as any;
    expect(typeof exchangePayload.result.accessToken).toBe("string");
    expect(exchangePayload.result.user.username).toBe("reader");

    const appRequest = new Request("http://localhost/rpc", {
      headers: { Authorization: `Bearer ${exchangePayload.result.accessToken}` },
    });
    const appSession = repo.getSessionByTokenHash(hashSessionToken(exchangePayload.result.accessToken));
    const me = await handleRpcMethod("auth.me", {}, { repo, startTime: Date.now() - 1000, request: appRequest, session: appSession });
    const mePayload = (await me.json()) as any;
    expect(mePayload.result.session.kind).toBe("app");
    expect(mePayload.result.user.username).toBe("reader");

    db.close();
  });
});
