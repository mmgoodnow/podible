import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { KindlingRepo } from "../../src/kindling/repo";
import { defaultSettings } from "../../src/kindling/settings";
import { runWorker } from "../../src/kindling/worker";
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
