import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { pollMsForMedia, runWorker, selectDownloadPollMs } from "../../src/worker";
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
        auth: { mode: "plex" },
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
        auth: { mode: "plex" },
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
    const queuedJobs = [
      {
        id: 123,
        type: "acquire",
        status: "queued",
        book_id: 1,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 1, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    ];
    const logs: string[] = [];
    let stopWorker = false;

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getBookRow: () => ({ id: 1, title: "Twilight", author: "Stephenie Meyer" }),
      markJobFailed: () => null,
    } as unknown as BooksRepo;

    const worker = runWorker({
      repo: fakeRepo,
      getSettings: () =>
        defaultSettings({
          auth: { mode: "plex" },
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

describe("worker concurrency", () => {
  test("serializes acquire jobs globally even for different media on the same book", async () => {
    const queuedJobs = [
      {
        id: 1,
        type: "acquire",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 7, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
      {
        id: 2,
        type: "acquire",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 7, media: ["ebook"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      },
    ];
    let stopWorker = false;
    let active = 0;
    let maxConcurrent = 0;
    let processed = 0;

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: () => null,
      getRelease: () => null,
    } as unknown as BooksRepo;

    await Promise.race([
      runWorker(
        {
          repo: fakeRepo,
          getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
          shouldStop: () => stopWorker,
        },
        {
          concurrency: 2,
          processJob: async () => {
            active += 1;
            maxConcurrent = Math.max(maxConcurrent, active);
            await sleep(50);
            active -= 1;
            processed += 1;
            if (processed === 2) stopWorker = true;
            return "done";
          },
          handleJobFailure: async () => {
            throw new Error("unexpected failure");
          },
        }
      ),
      sleep(1000),
    ]);

    expect(processed).toBe(2);
    expect(maxConcurrent).toBe(1);
  });

  test("serializes conflicting same-media acquire jobs for the same book", async () => {
    const queuedJobs = [
      {
        id: 1,
        type: "acquire",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 7, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
      {
        id: 2,
        type: "acquire",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 7, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      },
    ];
    let stopWorker = false;
    let active = 0;
    let maxConcurrent = 0;
    let processed = 0;

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: () => null,
      getRelease: () => null,
    } as unknown as BooksRepo;

    await Promise.race([
      runWorker(
        {
          repo: fakeRepo,
          getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
          shouldStop: () => stopWorker,
        },
        {
          concurrency: 2,
          processJob: async () => {
            active += 1;
            maxConcurrent = Math.max(maxConcurrent, active);
            await sleep(50);
            active -= 1;
            processed += 1;
            if (processed === 2) stopWorker = true;
            return "done";
          },
          handleJobFailure: async () => {
            throw new Error("unexpected failure");
          },
        }
      ),
      sleep(1000),
    ]);

    expect(processed).toBe(2);
    expect(maxConcurrent).toBe(1);
  });

  test("starts newly queued non-conflicting jobs while a long job is still active", async () => {
    const queuedJobs: Array<Record<string, unknown>> = [
      {
        id: 1,
        type: "chapter_analysis",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 11, ebookAssetId: 21 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
    ];
    let stopWorker = false;
    const started: number[] = [];
    let releaseSecondJob: (() => void) | undefined;
    const firstJobGate = new Promise<void>((resolve) => {
      releaseSecondJob = resolve;
    });

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: (assetId: number) => (assetId === 11 ? { id: 11, book_id: 7, source_release_id: null } : { id: 12, book_id: 8, source_release_id: null }),
      getRelease: () => null,
    } as unknown as BooksRepo;

    const worker = runWorker(
      {
        repo: fakeRepo,
        getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
        shouldStop: () => stopWorker,
      },
      {
        concurrency: 2,
        processJob: async (_ctx, job) => {
          started.push(job.id);
          if (job.id === 1) {
            await firstJobGate;
          }
          if (job.id === 2) {
            stopWorker = true;
          }
          return "done";
        },
        handleJobFailure: async () => {
          throw new Error("unexpected failure");
        },
      }
    );

    try {
      const startedWaitingAt = Date.now();
      while (!started.includes(1)) {
        if (Date.now() - startedWaitingAt > 1000) {
          throw new Error("Timed out waiting for first job to start");
        }
        await sleep(10);
      }

      queuedJobs.push({
        id: 2,
        type: "download",
        status: "queued",
        book_id: 8,
        release_id: 42,
        payload_json: JSON.stringify({ infoHash: "abc123" }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      });

      const secondJobWaitingAt = Date.now();
      while (!started.includes(2)) {
        if (Date.now() - secondJobWaitingAt > 1000) {
          throw new Error("Timed out waiting for second job to start while first was active");
        }
        await sleep(10);
      }

      expect(started).toEqual([1, 2]);
    } finally {
      releaseSecondJob?.();
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
    }
  });

  test("runs chapter analysis jobs in parallel for different assets", async () => {
    const queuedJobs = [
      {
        id: 1,
        type: "chapter_analysis",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 101, ebookAssetId: 201 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
      {
        id: 2,
        type: "chapter_analysis",
        status: "queued",
        book_id: 9,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 102, ebookAssetId: 202 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      },
    ];
    let stopWorker = false;
    let active = 0;
    let maxConcurrent = 0;
    let processed = 0;

    const assets = new Map([
      [101, { id: 101, book_id: 7, kind: "single", mime: "audio/mp4", total_size: 1, duration_ms: 1, source_release_id: null, created_at: "", updated_at: "" }],
      [102, { id: 102, book_id: 9, kind: "single", mime: "audio/mp4", total_size: 1, duration_ms: 1, source_release_id: null, created_at: "", updated_at: "" }],
    ]);

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: (assetId: number) => assets.get(assetId) ?? null,
      getRelease: () => null,
    } as unknown as BooksRepo;

    await Promise.race([
      runWorker(
        {
          repo: fakeRepo,
          getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
          shouldStop: () => stopWorker,
        },
        {
          concurrency: 2,
          processJob: async () => {
            active += 1;
            maxConcurrent = Math.max(maxConcurrent, active);
            await sleep(50);
            active -= 1;
            processed += 1;
            if (processed === 2) stopWorker = true;
            return "done";
          },
          handleJobFailure: async () => {
            throw new Error("unexpected failure");
          },
        }
      ),
      sleep(1000),
    ]);

    expect(processed).toBe(2);
    expect(maxConcurrent).toBe(2);
  });

  test("prefers acquire jobs over older chapter analysis jobs while respecting the acquire throttle", async () => {
    const queuedJobs = [
      {
        id: 1,
        type: "chapter_analysis",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 101, ebookAssetId: 201 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
      {
        id: 2,
        type: "chapter_analysis",
        status: "queued",
        book_id: 9,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 102, ebookAssetId: 202 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      },
      {
        id: 3,
        type: "chapter_analysis",
        status: "queued",
        book_id: 10,
        release_id: null,
        payload_json: JSON.stringify({ assetId: 103, ebookAssetId: 203 }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:02.000Z",
        updated_at: "2026-01-01T00:00:02.000Z",
      },
      {
        id: 4,
        type: "acquire",
        status: "queued",
        book_id: 11,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 11, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:03.000Z",
        updated_at: "2026-01-01T00:00:03.000Z",
      },
      {
        id: 5,
        type: "acquire",
        status: "queued",
        book_id: 12,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 12, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:04.000Z",
        updated_at: "2026-01-01T00:00:04.000Z",
      },
    ];
    let stopWorker = false;
    const started: number[] = [];
    const assets = new Map([
      [101, { id: 101, book_id: 7, kind: "single", mime: "audio/mp4", total_size: 1, duration_ms: 1, source_release_id: null, created_at: "", updated_at: "" }],
      [102, { id: 102, book_id: 9, kind: "single", mime: "audio/mp4", total_size: 1, duration_ms: 1, source_release_id: null, created_at: "", updated_at: "" }],
      [103, { id: 103, book_id: 10, kind: "single", mime: "audio/mp4", total_size: 1, duration_ms: 1, source_release_id: null, created_at: "", updated_at: "" }],
    ]);

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: (assetId: number) => assets.get(assetId) ?? null,
      getRelease: () => null,
    } as unknown as BooksRepo;

    await Promise.race([
      runWorker(
        {
          repo: fakeRepo,
          getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
          shouldStop: () => stopWorker,
        },
        {
          concurrency: 3,
          processJob: async (_ctx, job) => {
            started.push(job.id);
            if (started.length >= 3) {
              stopWorker = true;
            }
            await sleep(25);
            return "done";
          },
          handleJobFailure: async () => {
            throw new Error("unexpected failure");
          },
        }
      ),
      sleep(1000),
    ]);

    expect(started.length).toBe(3);
    expect(started[0]).toBe(4);
    expect(started.filter((id) => id >= 4)).toEqual([4]);
    expect(started.filter((id) => id <= 3)).toEqual([1, 2]);
  });

  test("limits concurrent acquire jobs globally", async () => {
    const queuedJobs = [
      {
        id: 1,
        type: "acquire",
        status: "queued",
        book_id: 7,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 7, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:00.000Z",
        updated_at: "2026-01-01T00:00:00.000Z",
      },
      {
        id: 2,
        type: "acquire",
        status: "queued",
        book_id: 8,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 8, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:01.000Z",
        updated_at: "2026-01-01T00:00:01.000Z",
      },
      {
        id: 3,
        type: "acquire",
        status: "queued",
        book_id: 9,
        release_id: null,
        payload_json: JSON.stringify({ bookId: 9, media: ["audio"] }),
        error: null,
        attempt_count: 0,
        max_attempts: 5,
        next_run_at: null,
        created_at: "2026-01-01T00:00:02.000Z",
        updated_at: "2026-01-01T00:00:02.000Z",
      },
    ];
    let stopWorker = false;
    let active = 0;
    let maxConcurrent = 0;
    let processed = 0;

    const fakeRepo = {
      requeueRunningJobs: () => 0,
      listRunnableJobs: () => queuedJobs.filter((job) => job.status === "queued"),
      claimQueuedJob: (jobId: number) => {
        const job = queuedJobs.find((candidate) => candidate.id === jobId && candidate.status === "queued");
        if (!job) return null;
        job.status = "running";
        return job;
      },
      getAsset: () => null,
      getRelease: () => null,
    } as unknown as BooksRepo;

    await Promise.race([
      runWorker(
        {
          repo: fakeRepo,
          getSettings: () => defaultSettings({ auth: { mode: "plex" } }),
          shouldStop: () => stopWorker,
        },
        {
          concurrency: 3,
          processJob: async () => {
            active += 1;
            maxConcurrent = Math.max(maxConcurrent, active);
            await sleep(50);
            active -= 1;
            processed += 1;
            if (processed === 3) stopWorker = true;
            return "done";
          },
          handleJobFailure: async () => {
            throw new Error("unexpected failure");
          },
        }
      ),
      sleep(1000),
    ]);

    expect(processed).toBe(3);
    expect(maxConcurrent).toBe(1);
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
        auth: { mode: "plex" },
        libraryRoot,
        rtorrent: {
          transport: "http-xmlrpc",
          url: rtorrentUrl,
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
        if (method === "d.is_active") return "<i8>0</i8>";
        if (method === "d.base_path") return `<string>${downloadDir}</string>`;
        if (method === "d.directory") return `<string>${downloadDir}</string>`;
        if (method === "d.is_multi_file") return "<i8>0</i8>";
        if (method === "f.multicall") {
          return "<array><data><value><array><data><value><string>unrelated.txt</string></value></data></array></value></data></array>";
        }
        if (method === "d.bytes_done") return "<i8>100</i8>";
        if (method === "d.size_bytes") return "<i8>100</i8>";
        if (method === "d.left_bytes") return "<i8>0</i8>";
        if (method === "d.down.rate") return "<i8>0</i8>";
        if (method === "d.message") return "<string></string>";
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

describe("worker stalled torrent recovery", () => {
  test("queues forced agent reacquire when rTorrent reports a recoverable download error", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const infoHash = infoHashFromTorrentBytes(makeTorrentBytes("stalled-download"));
    const rtorrentUrl = "http://rtorrent.mock/RPC2";

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        rtorrent: {
          transport: "http-xmlrpc",
          url: rtorrentUrl,
          username: "",
          password: "",
        },
      })
    );

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "mock",
      providerGuid: "guid-stalled",
      title: "Dune Audio",
      mediaType: "audio",
      infoHash,
      url: "https://example.com/dune-audio.torrent",
      status: "downloading",
    });
    const downloadJob = repo.createJob({
      type: "download",
      bookId: book.id,
      releaseId: release.id,
      payload: { infoHash },
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
        if (method === "d.name") return "<string>Dune Audio</string>";
        if (method === "d.hash") return `<string>${infoHash.toUpperCase()}</string>`;
        if (method === "d.complete") return "<i8>0</i8>";
        if (method === "d.is_active") return "<i8>0</i8>";
        if (method === "d.base_path") return "<string>/downloads/dune</string>";
        if (method === "d.directory") return "<string>/downloads</string>";
        if (method === "d.is_multi_file") return "<i8>0</i8>";
        if (method === "f.multicall") return "<array><data></data></array>";
        if (method === "d.bytes_done") return "<i8>10</i8>";
        if (method === "d.size_bytes") return "<i8>100</i8>";
        if (method === "d.left_bytes") return "<i8>90</i8>";
        if (method === "d.down.rate") return "<i8>0</i8>";
        if (method === "d.message") return "<string>tracker status: torrent no longer registered</string>";
        throw new Error(`Unexpected rTorrent method in test: ${method}`);
      })();
      return new Response(
        `<?xml version="1.0"?><methodResponse><params><param><value>${xmlValue}</value></param></params></methodResponse>`,
        {
          status: 200,
          headers: { "Content-Type": "text/xml" },
        }
      );
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
        const current = repo.getJob(downloadJob.id);
        if (current?.status === "cancelled") break;
        if (Date.now() - started > 4000) {
          throw new Error("Timed out waiting for stalled download recovery");
        }
        await sleep(50);
      }

      const finalDownloadJob = repo.getJob(downloadJob.id);
      expect(finalDownloadJob?.status).toBe("cancelled");
      expect(String(finalDownloadJob?.error || "")).toContain("recoveryAcquireJob=");

      const failedRelease = repo.getRelease(release.id);
      expect(failedRelease?.status).toBe("failed");
      expect(String(failedRelease?.error || "")).toContain("queuing forced reacquire");

      const acquireJobs = repo.listJobsByType("acquire").filter((job) => job.book_id === book.id);
      expect(acquireJobs.length).toBe(1);
      const payload = JSON.parse(acquireJobs[0]?.payload_json ?? "{}");
      expect(payload.media).toEqual(["audio"]);
      expect(payload.forceAgent).toBe(true);
      expect(payload.priorFailure).toBe(true);
      expect(payload.requireResult).toBe(true);
      expect(payload.notifyOnFailure).toBe(true);
      expect(payload.rejectedUrls).toEqual([release.url]);
      expect(payload.rejectedGuids).toEqual(["guid-stalled"]);
      expect(payload.rejectedInfoHashes).toEqual([release.info_hash]);
      expect(logs.some((line) => line.includes("action=recover"))).toBe(true);
    } finally {
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("reacquires instead of notifying when the tracker error message suggests manual action", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const infoHash = infoHashFromTorrentBytes(makeTorrentBytes("manual-attention"));
    const rtorrentUrl = "http://rtorrent.mock/RPC2";

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        rtorrent: {
          transport: "http-xmlrpc",
          url: rtorrentUrl,
          username: "",
          password: "",
        },
      })
    );

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "mock",
      providerGuid: "guid-manual",
      title: "Dune Audio",
      mediaType: "audio",
      infoHash,
      url: "https://example.com/dune-audio.torrent",
      status: "downloading",
    });
    const downloadJob = repo.createJob({
      type: "download",
      bookId: book.id,
      releaseId: release.id,
      payload: { infoHash },
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      if (url === rtorrentUrl) {
        const body = String(init?.body ?? "");
        const method = /<methodName>([^<]+)<\/methodName>/.exec(body)?.[1] ?? "";
        const xmlValue = (() => {
          if (method === "d.name") return "<string>Dune Audio</string>";
          if (method === "d.hash") return `<string>${infoHash.toUpperCase()}</string>`;
          if (method === "d.complete") return "<i8>0</i8>";
          if (method === "d.is_active") return "<i8>0</i8>";
          if (method === "d.base_path") return "<string>/downloads/dune</string>";
          if (method === "d.directory") return "<string>/downloads</string>";
          if (method === "d.is_multi_file") return "<i8>0</i8>";
          if (method === "f.multicall") return "<array><data></data></array>";
          if (method === "d.bytes_done") return "<i8>10</i8>";
          if (method === "d.size_bytes") return "<i8>100</i8>";
          if (method === "d.left_bytes") return "<i8>90</i8>";
          if (method === "d.down.rate") return "<i8>0</i8>";
          if (method === "d.message") return "<string>tracker requires bonus credits before download</string>";
          throw new Error(`Unexpected rTorrent method in test: ${method}`);
        })();
        return new Response(
          `<?xml version="1.0"?><methodResponse><params><param><value>${xmlValue}</value></param></params></methodResponse>`,
          {
            status: 200,
            headers: { "Content-Type": "text/xml" },
          }
        );
      }
      throw new Error(`Unexpected fetch url: ${url}`);
    }) as unknown as typeof fetch;

    let stopWorker = false;
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
    });

    try {
      const started = Date.now();
      for (;;) {
        const current = repo.getJob(downloadJob.id);
        if (current?.status === "cancelled") break;
        if (Date.now() - started > 4000) {
          throw new Error("Timed out waiting for manual-attention reacquire");
        }
        await sleep(50);
      }

      const acquireJobs = repo.listJobsByType("acquire").filter((job) => job.book_id === book.id);
      expect(acquireJobs).toHaveLength(1);
      const payload = JSON.parse(acquireJobs[0]?.payload_json ?? "{}");
      expect(payload.media).toEqual(["audio"]);
      expect(payload.requireResult).toBe(true);
      expect(payload.notifyOnFailure).toBe(true);
      expect(repo.getRelease(release.id)?.status).toBe("failed");
      expect(String(repo.getJob(downloadJob.id)?.error || "")).toContain("recoveryAcquireJob=");
    } finally {
      stopWorker = true;
      await Promise.race([worker, sleep(1000)]);
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("notifies when an auto-reacquire job exhausts its retries", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const pushoverBodies: string[] = [];

    repo.updateSettings(
      defaultSettings({
        auth: { mode: "plex" },
        torznab: [],
        notifications: {
          pushover: {
            enabled: true,
            apiToken: "token",
            userKey: "user",
          },
        },
      })
    );

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const acquireJob = repo.createJob({
      type: "acquire",
      bookId: book.id,
      maxAttempts: 1,
      payload: {
        bookId: book.id,
        media: ["audio"],
        forceAgent: true,
        priorFailure: true,
        requireResult: true,
        notifyOnFailure: true,
        failureContext: "Auto-reacquire after stalled torrent for release 12",
      },
    });

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = String(input);
      if (url === "https://api.pushover.net/1/messages.json") {
        pushoverBodies.push(String(init?.body ?? ""));
        return new Response('{"status":1}', {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      throw new Error(`Unexpected fetch url: ${url}`);
    }) as unknown as typeof fetch;

    let stopWorker = false;
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
    });

    try {
      const started = Date.now();
      for (;;) {
        const current = repo.getJob(acquireJob.id);
        if (current?.status === "failed") break;
        if (Date.now() - started > 4000) {
          throw new Error("Timed out waiting for acquire failure notification");
        }
        await sleep(50);
      }

      expect(repo.getJob(acquireJob.id)?.status).toBe("failed");
      expect(pushoverBodies).toHaveLength(1);
      expect(pushoverBodies[0]).toContain("Auto-reacquire+after+stalled+torrent+for+release+12");
      expect(pushoverBodies[0]).toContain("Auto-acquire+found+no+usable+release");
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
        isActive: true,
        basePath: null,
        directory: null,
        isMultiFile: false,
        bytesDone: 90,
        sizeBytes: 100,
        leftBytes: 10,
        downRate: 1,
        message: null,
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
        isActive: true,
        basePath: null,
        directory: null,
        isMultiFile: false,
        bytesDone: 0,
        sizeBytes: 500,
        leftBytes: 500,
        downRate: 5,
        message: null,
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
        isActive: true,
        basePath: null,
        directory: null,
        isMultiFile: false,
        bytesDone: 50,
        sizeBytes: 100,
        leftBytes: 50,
        downRate: 0,
        message: null,
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
        isActive: true,
        basePath: null,
        directory: null,
        isMultiFile: false,
        bytesDone: 960,
        sizeBytes: 1000,
        leftBytes: null,
        downRate: 2,
        message: null,
      },
      5000
    );
    expect(pollMs).toBe(500);
  });
});
