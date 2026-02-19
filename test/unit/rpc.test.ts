import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { handleRpcMethod, handleRpcRequest } from "../../src/kindling/rpc";
import { KindlingRepo } from "../../src/kindling/repo";
import { defaultSettings } from "../../src/kindling/settings";

async function callRpc(repo: KindlingRepo, body: string | object) {
  const request = new Request("http://localhost/rpc", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  const response = await handleRpcRequest(request, { repo, startTime: Date.now() - 1000 });
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

describe("json-rpc handler", () => {
  test("dispatches representative methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
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
    const acquireJob = repo.getJob(acquire.result.jobId);
    expect(acquireJob?.type).toBe("acquire");
    expect(acquireJob?.book_id).toBe(book.id);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").media).toEqual(["audio"]);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").forceAgent).toBe(true);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").priorFailure).toBe(true);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").rejectedUrls).toEqual(["https://example.com/bad.torrent"]);

    const queued = repo.createJob({ type: "full_library_refresh" });
    const jobs = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 3,
      method: "jobs.list",
      params: { limit: 5 },
    });
    expect(Array.isArray(jobs.result.jobs)).toBe(true);
    expect(jobs.result.jobs[0].id).toBe(queued.id);

    db.close();
  });

  test("library.reportImportIssue queues forced agent reacquire when no importable files", async () => {
    const originalFetch = globalThis.fetch;
    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new KindlingRepo(db);

      const root = await mkdtemp(path.join(os.tmpdir(), "kindling-report-issue-"));
      const badPath = path.join(root, "download");
      await mkdir(badPath, { recursive: true });
      await writeFile(path.join(badPath, "readme.txt"), Buffer.from("wrong payload"));
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "local", key: "test" },
          libraryRoot: path.join(root, "library"),
          rtorrent: {
            transport: "http-xmlrpc",
            url: "http://mock.local/RPC2",
            username: "",
            password: "",
          },
          agents: {
            ...defaultSettings().agents,
            enabled: false,
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

      expect(result.result.action).toBe("reacquire_queued");
      expect(result.result.releaseId).toBe(release.id);
      expect(result.result.mediaType).toBe("audio");
      expect(result.result.rejectedUrls).toEqual([release.url]);
      expect(result.result.jobId).toBeGreaterThan(0);

      const failedRelease = repo.getRelease(release.id);
      expect(failedRelease?.status).toBe("failed");
      expect(String(failedRelease?.error || "")).toContain("User-reported import issue");

      const acquireJob = repo.getJob(result.result.jobId);
      expect(acquireJob?.type).toBe("acquire");
      const payload = JSON.parse(acquireJob?.payload_json ?? "{}");
      expect(payload.bookId).toBe(book.id);
      expect(payload.media).toEqual(["audio"]);
      expect(payload.forceAgent).toBe(true);
      expect(payload.priorFailure).toBe(true);
      expect(payload.rejectedUrls).toEqual([release.url]);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("returns parse error for malformed json", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, "{");
    expect(result.error.code).toBe(-32700);
    db.close();
  });

  test("rejects batch requests", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

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
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-delete-"));
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
      const repo = new KindlingRepo(db);
      repo.updateSettings(
        defaultSettings({
          auth: { mode: "local", key: "test" },
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

  test("import.manual creates a release and imports local file", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-manual-import-"));
    const sourceDir = path.join(root, "source");
    const libraryRoot = path.join(root, "library");
    await mkdir(sourceDir, { recursive: true });
    await mkdir(libraryRoot, { recursive: true });
    const sourceFile = path.join(sourceDir, "twilight.epub");
    await writeFile(sourceFile, Buffer.from("epub-bytes"));

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
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
    const repo = new KindlingRepo(db);
    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
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
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-manual-select-"));
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
        auth: { mode: "local", key: "test" },
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
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-agent-import-plan-"));
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
    const repo = new KindlingRepo(db);
    repo.ensureSettings();

    const response = await handleRpcMethod("settings.update", {}, { repo, startTime: Date.now() - 1000 }, { readOnly: true });
    expect(response.status).toBe(200);
    const payload = (await response.json()) as any;
    expect(payload.error.code).toBe(-32601);
    db.close();
  });
});
