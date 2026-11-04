import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { runWorker } from "../../src/worker";
import { processJob } from "../../src/worker/dispatcher";
import { createJobExecutionRuntime } from "../../src/worker/execution";

test("normal scheduler dispatches one durable observation per slot, including after restart", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "stewardship-scheduler-"));
  const db = new Database(path.join(root, "domain.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const book = repo.createBook({ title: "Owned library", author: "Fixture" });
  repo.bookTorrents.reserve({ infoHash: "a".repeat(40), bookId: book.id,
    clientId: "transmission:11111111-1111-4111-8111-111111111111", rootPath: root,
    rootToken: "22222222-2222-4222-8222-222222222222" });
  let stopped = false;
  const ctx = { repo, getSettings: () => repo.getSettings(), shouldStop: () => stopped, onLog() {},
    stewardshipAdapters: { connection: null } };
  const executionPath = path.join(root, "execution.sqlite");
  try {
    for (let iteration = 0; iteration < 2; iteration++) {
      stopped = false;
      const runtime = await createJobExecutionRuntime(ctx, executionPath, {
        processJob: (owned, job) => job.type === "torrent_stewardship_collect" ? processJob(owned, job) : Promise.resolve("done"),
      });
      const worker = runWorker(ctx, { runtime });
      try {
        const deadline = Date.now() + 5000;
        while (repo.listJobsByType("torrent_stewardship_collect")[0]?.status !== "succeeded") {
          if (Date.now() > deadline) throw new Error("Scheduled collection did not complete");
          await Bun.sleep(15);
        }
        await Bun.sleep(150);
        expect(repo.listJobsByType("torrent_stewardship_collect")).toHaveLength(1);
        expect(db.query("SELECT key FROM app_state WHERE key LIKE 'torrent-stewardship:v1:collection:%'").all()).toHaveLength(1);
        expect(repo.listJobsByType("acquire")).toHaveLength(0);
      } finally { stopped = true; await worker; }
    }
  } finally { db.close(); await rm(root, { recursive: true, force: true }); }
});
