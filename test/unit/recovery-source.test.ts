import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { inspectMetainfo } from "../../src/media/metainfo";
import { recoverMissingTorrentSource } from "../../src/media/recovery-source";
import { createFixtureMovieServices } from "../fixtures/movie-services";

test("source recovery rejects symlink/wrong-size ambiguity and waits past stale completion before accepting a recheck", async () => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "recovery-source-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), fixture = createFixtureMovieServices(root);
  const offer = fixture.addOffer("known", 1280), metainfo = await fixture.services.fetchMetainfo(offer);
  const metadata = inspectMetainfo(metainfo), sourceRoot = path.join(root, "downloads", "recovery");
  await fs.mkdir(sourceRoot, { recursive: true });
  const ref = await fixture.services.client.ensureAdded({ metainfo, location: { rootId: "movies", relativePath: "recovery" } });
  const full = path.join(sourceRoot, metadata.files[0]!.path);
  const ctx = { repo, getSettings: () => repo.getSettings(), execution: {
    key: "test", assertActive() {}, async run<T>(_name: string, action: () => T | Promise<T>) { return action(); }, async sleep() {},
  } };
  const input = { ctx, client: fixture.services.client, ref, metadata, sourceRoot, prefix: "recovery", active() {} };
  try {
    await fs.writeFile(full, "wrong size");
    await expect(recoverMissingTorrentSource(input)).rejects.toThrow("changed");
    expect(fixture.stats.rechecks).toBe(0);
    await fs.unlink(full); await fs.symlink(path.join(root, "absent"), full);
    await expect(recoverMissingTorrentSource(input)).rejects.toThrow("changed");
    expect(fixture.stats.rechecks).toBe(0);
    await fs.unlink(full);
    let observations = 0;
    const client = { ...fixture.services.client, async inspect() {
      observations++;
      return { ref, location: { rootId: "movies", relativePath: "recovery" }, state: observations < 3 ? "seeding" as const : "checking" as const,
        complete: true, bytesDone: metadata.size, sizeBytes: metadata.size, downloadBytesPerSecond: 0, message: null, observedAt: new Date().toISOString() };
    } };
    await recoverMissingTorrentSource({ ...input, client });
    expect(observations).toBe(3);
    expect(fixture.stats.rechecks).toBe(1);
  } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
});
