import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { KindlingRepo } from "../../src/kindling/repo";
import { defaultSettings } from "../../src/kindling/settings";
import { infoHashFromTorrentBytes } from "../../src/kindling/torrent";
import { pollMsForMedia, runWorker, selectDownloadPollMs } from "../../src/kindling/worker";
import { startMockTorznab } from "../mocks/torznab";

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce15:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

describe("worker scan auto-acquire retries", () => {
  test("marks scan job for retry when snatch fails", async () => {
    const torznab = startMockTorznab({
      results: [{ title: "Dune Audio", torrentId: "audio", size: 1234 }],
      torrents: { audio: makeTorrentBytes("dune-audio") },
    });

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
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
    const scanJob = repo.createJob({
      type: "scan",
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
        const current = repo.getJob(scanJob.id);
        if (current && current.attempt_count >= 1) break;
        if (Date.now() - started > 4_000) {
          throw new Error("Timed out waiting for scan job retry");
        }
        await sleep(50);
      }
      const current = repo.getJob(scanJob.id);
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
});

describe("worker import recovery", () => {
  test("queues forced agent scan when deterministic import cannot map files", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-import-recovery-"));
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
        if (current?.status === "succeeded") break;
        if (Date.now() - started > 4000) {
          throw new Error("Timed out waiting for import recovery flow");
        }
        await sleep(50);
      }

      const failedRelease = repo.getRelease(release.id);
      expect(failedRelease?.status).toBe("failed");
      expect(String(failedRelease?.error || "")).toContain("deterministic+agent");

      const scanJobs = repo
        .listJobsByType("scan")
        .filter((job) => job.book_id === book.id && job.id !== importJob.id);
      expect(scanJobs.length).toBeGreaterThan(0);
      const recoveryScan = scanJobs[scanJobs.length - 1];
      const payload = JSON.parse(recoveryScan.payload_json ?? "{}");
      expect(payload.bookId).toBe(book.id);
      expect(payload.media).toEqual(["audio"]);
      expect(payload.forceAgent).toBe(true);
      expect(payload.priorFailure).toBe(true);
      expect(payload.rejectedUrls).toEqual([release.url]);
      expect(logs.some((line) => line.includes("queued_scan_forced_agent=1"))).toBe(true);
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
