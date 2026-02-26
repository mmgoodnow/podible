import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/books/db";
import { BooksRepo } from "../../src/books/repo";
import { defaultSettings } from "../../src/books/settings";
import { infoHashFromTorrentBytes } from "../../src/books/torrent";
import { pollMsForMedia, runWorker, selectDownloadPollMs } from "../../src/books/worker";
import { startMockTorznab } from "../mocks/torznab";

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce15:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

describe("worker acquire auto-acquire retries", () => {
  test("marks acquire job for retry when snatch fails", async () => {
    const torznab = startMockTorznab({
      results: [{ title: "Dune Audio", torrentId: "audio", size: 1234 }],
      torrents: { audio: makeTorrentBytes("dune-audio") },
    });

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
        torznab: [{ name: "mock", baseUrl: torznab.baseUrl }],
        // Intentionally unreachable to force runSnatch failure.
        rtorrent: {
          transport: "http-xmlrpc",
          url: "http://127.0.0.1:65534/RPC2",
          username: "",
          password: "",
        },
      })
    );

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const acquireJob = repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: { bookId: book.id, media: ["audio"] },
    });

    let stopWorker = false;
    const logs: string[] = [];
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
      onLog: (line) => logs.push(line),
    });

    try {
      const started = Date.now();
      for (;;) {
        const current = repo.getJob(acquireJob.id);
        if (current && current.attempt_count >= 1) break;
        if (Date.now() - started > 4_000) {
          throw new Error("Timed out waiting for acquire job retry");
        }
        await sleep(50);
      }
      const current = repo.getJob(acquireJob.id);
      expect(current).toBeTruthy();
      expect(current?.status).toBe("queued");
      expect(current?.attempt_count).toBe(1);
      expect((current?.error ?? "").length).toBeGreaterThan(0);
      expect(repo.listReleasesByBook(book.id).length).toBe(0);
      expect(logs.some((line) => line.includes("snatch_error="))).toBe(true);
    } finally {
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
      torznab.stop();
      db.close();
    }
  });

  test("retries acquire when one media snatch fails after another media succeeds", async () => {
    const audioTorrent = makeTorrentBytes("eclipse-audio");
    const ebookTorrent = makeTorrentBytes("eclipse-ebook");
    const torznab = startMockTorznab({
      results: [
        { title: "Eclipse by Stephenie Meyer [ENG / M4B]", torrentId: "audio", size: 1111 },
        { title: "Eclipse by Stephenie Meyer [ENG / EPUB]", torrentId: "ebook", size: 2222 },
      ],
      torrents: { audio: audioTorrent, ebook: ebookTorrent },
    });

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
        torznab: [{ name: "mock", baseUrl: torznab.baseUrl }],
        rtorrent: {
          transport: "http-xmlrpc",
          url: "http://rtorrent.mock/RPC2",
          username: "",
          password: "",
        },
      })
    );

    const book = repo.createBook({ title: "Eclipse", author: "Stephenie Meyer" });
    const acquireJob = repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: { bookId: book.id, media: ["audio", "ebook"] },
    });

    const originalFetch = globalThis.fetch;
    let loadCallCount = 0;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      if (url === "http://rtorrent.mock/RPC2") {
        const body = String(init?.body ?? "");
        const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
        if (method === "load.raw_start") {
          loadCallCount += 1;
          if (loadCallCount === 1) {
            return new Response("boom", { status: 500 });
          }
          return new Response(
            `<?xml version="1.0"?><methodResponse><params><param><value><int>0</int></value></param></params></methodResponse>`,
            {
              status: 200,
              headers: { "Content-Type": "text/xml" },
            }
          );
        }
        throw new Error(`Unexpected rTorrent method in test: ${method}`);
      }
      return (originalFetch as typeof fetch)(input as any, init);
    }) as typeof fetch;

    let stopWorker = false;
    const logs: string[] = [];
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
      onLog: (line) => logs.push(line),
    });

    try {
      const started = Date.now();
      for (;;) {
        const current = repo.getJob(acquireJob.id);
        if (current && current.attempt_count >= 1) break;
        if (Date.now() - started > 4_000) {
          throw new Error("Timed out waiting for partial acquire retry");
        }
        await sleep(50);
      }

      const current = repo.getJob(acquireJob.id);
      expect(current?.status).toBe("queued");
      expect(current?.attempt_count).toBe(1);
      expect(String(current?.error || "")).toContain("partially failed");
      expect(String(current?.error || "")).toContain("audio:");

      const releases = repo.listReleasesByBook(book.id);
      expect(releases.length).toBe(1);
      expect(releases[0]?.media_type).toBe("ebook");
      expect(repo.listJobsByType("download").length).toBe(1);

      expect(logs.some((line) => line.includes("media=audio") && line.includes("snatch_error="))).toBe(true);
      expect(logs.some((line) => line.includes("media=ebook") && line.includes("download_job="))).toBe(true);
    } finally {
      stopWorker = true;
      globalThis.fetch = originalFetch;
      await Promise.race([worker, sleep(1000)]);
      torznab.stop();
      db.close();
    }
  });
});

describe("worker robustness", () => {
  test("does not crash when failed job row was deleted before markJobFailed", async () => {
    let claimed = false;
    const logs: string[] = [];
    let stopWorker = false;

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      claimNextRunnableJob: () => {
        if (claimed) return null;
        claimed = true;
        return {
          id: 123,
          type: "acquire",
          status: "running",
          book_id: 1,
          release_id: null,
          payload_json: JSON.stringify({ bookId: 1, media: ["audio"] }),
          error: null,
          attempt_count: 0,
          max_attempts: 5,
          next_run_at: null,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        };
      },
      getBookRow: () => ({ id: 1, title: "Twilight", author: "Stephenie Meyer" }),
      markJobFailed: () => null,
    } as unknown as BooksRepo;

    const worker = runWorker({
      repo: fakeRepo,
      getSettings: () =>
        defaultSettings({
          auth: { mode: "local", key: "test" },
          torznab: [],
        }),
      shouldStop: () => stopWorker,
      onLog: (line) => {
        logs.push(line);
        if (line.includes("failed but row missing")) {
          stopWorker = true;
        }
      },
    });

    try {
      const started = Date.now();
      for (;;) {
        if (logs.some((line) => line.includes("failed but row missing"))) break;
        if (Date.now() - started > 3000) {
          throw new Error("Timed out waiting for missing-row failure log");
        }
        await sleep(25);
      }
      await Promise.race([worker, sleep(1000)]);
      expect(logs.some((line) => line.includes("failed but row missing"))).toBe(true);
    } finally {
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
    }
  });
});

describe("worker import recovery", () => {
  test("queues forced agent acquire when deterministic import cannot map files", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "books-import-recovery-"));
    const downloadDir = path.join(root, "download");
    const libraryRoot = path.join(root, "library");
    await mkdir(downloadDir, { recursive: true });
    await mkdir(libraryRoot, { recursive: true });
    await writeFile(path.join(downloadDir, "unrelated.txt"), Buffer.from("not-a-book"));

    const infoHash = infoHashFromTorrentBytes(makeTorrentBytes("import-recovery"));
    const rtorrentUrl = "http://rtorrent.mock/RPC2";

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
        libraryRoot,
        rtorrent: {
          transport: "http-xmlrpc",
          url: rtorrentUrl,
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
      title: "Dune Wrong Payload",
      mediaType: "audio",
      infoHash,
      url: "https://example.com/wrong.torrent",
      status: "downloaded",
    });
    const importJob = repo.createJob({
      type: "import",
      bookId: book.id,
      releaseId: release.id,
      payload: { reason: "test-import-recovery" },
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      if (url !== rtorrentUrl) {
        throw new Error(`Unexpected fetch url: ${url}`);
      }
      const body = String(init?.body ?? "");
      const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
      const xmlValue = (() => {
        if (method === "d.name") return "<string>Dune Wrong Payload</string>";
        if (method === "d.hash") return `<string>${infoHash.toUpperCase()}</string>`;
        if (method === "d.complete") return "<i8>1</i8>";
        if (method === "d.base_path") return `<string>${downloadDir}</string>`;
        if (method === "d.bytes_done") return "<i8>100</i8>";
        if (method === "d.size_bytes") return "<i8>100</i8>";
        if (method === "d.left_bytes") return "<i8>0</i8>";
        if (method === "d.down.rate") return "<i8>0</i8>";
        throw new Error(`Unexpected rTorrent method in test: ${method}`);
      })();
      return new Response(`<?xml version="1.0"?><methodResponse><params><param><value>${xmlValue}</value></param></params></methodResponse>`, {
        status: 200,
        headers: { "Content-Type": "text/xml" },
      });
    }) as unknown as typeof fetch;

    let stopWorker = false;
    const logs: string[] = [];
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
      onLog: (line) => logs.push(line),
    });

    try {
      const started = Date.now();
      for (;;) {
        const current = repo.getJob(importJob.id);
        if (current?.status === "cancelled" || current?.status === "succeeded") break;
        if (Date.now() - started > 4000) {
          throw new Error("Timed out waiting for import recovery flow");
        }
        await sleep(50);
      }

      const finalImportJob = repo.getJob(importJob.id);
      expect(finalImportJob?.status).toBe("cancelled");
      expect(String(finalImportJob?.error || "")).toContain("recoveryQueued=true");

      const failedRelease = repo.getRelease(release.id);
      expect(failedRelease?.status).toBe("failed");
      expect(String(failedRelease?.error || "")).toContain("deterministic+agent");

      const acquireJobs = repo
        .listJobsByType("acquire")
        .filter((job) => job.book_id === book.id && job.id !== importJob.id);
      expect(acquireJobs.length).toBeGreaterThan(0);
      const recoveryAcquire = acquireJobs[acquireJobs.length - 1];
      const payload = JSON.parse(recoveryAcquire.payload_json ?? "{}");
      expect(payload.bookId).toBe(book.id);
      expect(payload.media).toEqual(["audio"]);
      expect(payload.forceAgent).toBe(true);
      expect(payload.priorFailure).toBe(true);
      expect(payload.rejectedUrls).toEqual([release.url]);
      expect(payload.rejectedGuids).toEqual(["guid-1"]);
      expect(payload.rejectedInfoHashes).toEqual([release.info_hash]);
      expect(logs.some((line) => line.includes("queued_acquire_forced_agent=1"))).toBe(true);
    } finally {
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
      globalThis.fetch = originalFetch;
      db.close();
    }
  });
});

describe("download ETA polling", () => {
  test("caps ebook max poll interval at 1 second", () => {
    expect(pollMsForMedia("ebook", 5000)).toBe(1000);
    expect(pollMsForMedia("ebook", 800)).toBe(800);
    expect(pollMsForMedia("audio", 5000)).toBe(5000);
  });

  test("uses fast poll near completion", () => {
    const pollMs = selectDownloadPollMs(
      {
        name: null,
        hash: null,
        complete: false,
        basePath: null,
        bytesDone: 90,
        sizeBytes: 100,
        leftBytes: 10,
        downRate: 1,
      },
      5000
    );
    expect(pollMs).toBe(500);
  });

  test("uses medium poll for mid ETA window", () => {
    const pollMs = selectDownloadPollMs(
      {
        name: null,
        hash: null,
        complete: false,
        basePath: null,
        bytesDone: 0,
        sizeBytes: 500,
        leftBytes: 500,
        downRate: 5,
      },
      5000
    );
    expect(pollMs).toBe(2000);
  });

  test("falls back to configured poll when rate is unavailable", () => {
    const pollMs = selectDownloadPollMs(
      {
        name: null,
        hash: null,
        complete: false,
        basePath: null,
        bytesDone: 50,
        sizeBytes: 100,
        leftBytes: 50,
        downRate: 0,
      },
      5000
    );
    expect(pollMs).toBe(5000);
  });

  test("derives left bytes when not provided", () => {
    const pollMs = selectDownloadPollMs(
      {
        name: null,
        hash: null,
        complete: false,
        basePath: null,
        bytesDone: 960,
        sizeBytes: 1000,
        leftBytes: null,
        downRate: 2,
      },
      5000
    );
    expect(pollMs).toBe(500);
  });
});
