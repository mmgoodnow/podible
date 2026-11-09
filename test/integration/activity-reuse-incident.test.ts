import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { queueCrossSeed } from "../../src/media/cross-seed";
import { BooksRepo } from "../../src/repo";
import { createAdminWebRouter } from "../../src/rpc/admin-web-router";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

test("Activity identifies a reuse metainfo failure without exposing or guessing from raw errors", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "activity-reuse-"));
  const domain = path.join(root, "podible.sqlite"), execution = path.join(root, "execution.sqlite");
  const db = new Database(domain), storage = new SQLiteStorage(execution);
  try {
    runMigrations(db);
    finalizeJobJournalMigration(db);
    await storage.initialize();
    const repo = new BooksRepo(db), settings = repo.getSettings();
    settings.prowlarr = { url: "http://prowlarr.invalid:9696", apiKey: "settings-secret" };
    repo.updateSettings(settings);
    const user = repo.upsertUser({ provider: "local", providerUserId: "reuse-fixture", username: "Admin", isAdmin: true });
    const catalog = { ...fixtureTvEntry(), title: "Sense8" };
    const request = repo.tv.request(user.id, catalog, {}, [catalog.episodes[0]!.key]);
    const now = Date.now(), at = new Date(now).toISOString();
    const feed = repo.releaseInbox.ensureFeed("reuse-fixture", "PrivateHD", "tv", at);
    const [releaseId] = repo.releaseInbox.ingest(feed.id, [{ mediaType: "tv", title: "raw-release-title",
      provider: "PrivateHD", url: settings.prowlarr.url + "/download?apikey=offer-secret", guid: "candidate",
      infoHash: null, sizeBytes: 100, seeders: 1, leechers: 0, raw: {} }], at);
    const receipt = queueCrossSeed(repo, { operationId: randomUUID(), scope: { media: "tv", targetId: request.targetId }, releaseId });
    const runId = randomUUID(), step = "reuse:metainfo";
    await storage.createRun({ id: runId, workflow: "fixture", input: {}, status: "failed", idempotencyKey: runId,
      createdAt: now, updatedAt: now, startedAt: now, finishedAt: now, output: null, wakeAt: null,
      error: { stepName: step, code: null, message: "Metainfo retrieval returned HTTP 429" }, waitingFor: null });
    const stepId = randomUUID();
    await storage.saveStepResult({ id: stepId, runId, name: step, kind: "step", status: "failed",
      output: { name: "Error", code: null }, error: "Metainfo retrieval returned HTTP 429",
      attempts: 3, createdAt: now, updatedAt: now, startedAt: now, history: [] });
    repo.attachJobExecution(receipt.jobId, runId);

    using readOnly = new Database(domain, { readonly: true });
    using journal = new Database(execution);
    const route = createAdminWebRouter({ transmission: null }).routes.ops;
    const read = async () => {
      const result = await route.handler({ repo: new BooksRepo(readOnly), startTime: now, request: new Request("http://localhost/ops"),
        session: { user_id: user.id } as never }, { includeJobs: false });
      return result.incidents.find(incident => incident.jobId === receipt.jobId)!;
    };
    const legacy = await read();
    expect(legacy).toMatchObject({ title: "Sense8: cross-seeding", code: "provider-cooldown", recovery: "manual-retry" });
    expect(legacy.detail).toContain("Fetching torrent metadata from PrivateHD returned HTTP 429.");
    expect(legacy.detail).toContain("does not identify whether the indexer or a proxy");
    expect(legacy.detail).toContain("Check PrivateHD in Prowlarr and run its Test action before retrying.");

    const raw = "Upstream mentioned HTTP 429 https://private.invalid/?apikey=error-secret";
    journal.query("UPDATE workflow_steps SET error=? WHERE id=?").run(raw, stepId);
    const untyped = await read();
    expect(untyped.code).toBe("reuse-metainfo");
    expect(untyped.detail).toContain("could not fetch torrent metadata from PrivateHD");
    expect(untyped.detail).not.toContain("HTTP 429");

    journal.query("UPDATE workflow_steps SET output=? WHERE id=?").run(JSON.stringify({ name: "Error", code: "podible:metainfo-http:429" }), stepId);
    expect((await read()).detail).toBe(legacy.detail);
    expect(JSON.stringify(await read())).not.toMatch(/settings-secret|offer-secret|error-secret|raw-release-title|private\.invalid/);

    journal.query("UPDATE workflow_runs SET error=? WHERE id=?").run(JSON.stringify({ stepName: "reuse:prepare", message: "Storage unavailable" }), runId);
    const unrelated = await read();
    expect(unrelated.title).toBe("Sense8: cross-seeding");
    expect(unrelated.detail).not.toMatch(/PrivateHD|HTTP 429/);
  } finally {
    await storage.close();
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
