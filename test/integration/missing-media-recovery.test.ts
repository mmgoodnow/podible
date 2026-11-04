import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { RecoveryService } from "../../src/media/recovery";
import { queueMovieAcquisition, movieRequestView } from "../../src/movies/service";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createFixtureMovieServices, fixtureMovie } from "../fixtures/movie-services";

for (const loseSeed of [false, true]) test(`real Reflow restores a lost selected movie at equal quality; seed loss=${loseSeed}`, async () => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "missing-movie-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.movies.managedRoot = root; settings.movies.plexViewPath = "/fixture/movies";
  repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "loss", username: "loss", isAdmin: true });
  const requested = repo.movies.request(user.id, fixtureMovie, settings.movies.qualityPolicy);
  const fixture = createFixtureMovieServices(root); fixture.addOffer("only-release", 1280);
  const runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), movieServices: fixture.services, onLog() {} },
    path.join(root, "execution.sqlite"), { retryDelayMs: 1 });
  const until = async (check: () => boolean) => {
    const deadline = Date.now() + 10000;
    while (!check()) { await runtime.reconcile(); if (Date.now() > deadline) throw new Error("Recovery runtime timed out"); await Bun.sleep(15); }
  };
  try {
    const initial = queueMovieAcquisition(repo, requested.targetId, "initial").acquisition;
    await until(() => repo.getJob(initial.job_id!)?.status === "succeeded");
    const oldId = repo.movies.target(requested.targetId)!.active_version_id!, old = repo.movies.version(oldId)!;
    await fs.unlink(path.join(root, "objects", old.files[0]!.relativePath));
    if (loseSeed) for (const download of repo.movies.downloads(requested.targetId)) {
      const ref = { clientId: download.client_id, nativeId: download.native_id };
      for (const file of await fixture.services.client.files(ref)) await fs.unlink(file.path);
    }
    const service = new RecoveryService(repo), key = { namespace: "movie-target" as const, id: String(requested.targetId) };
    const preview = service.preview({ targets: [key] }).resources[0]!;
    expect(preview.action).toBe("queue-acquisition");
    const command = { operationId: randomUUID(), targets: [{ key, expectedRevision: preview.revision, action: "queue-acquisition" as const }] };
    const result = service.recover(command);
    expect(movieRequestView(repo, requested.requestId).available).toBe(false);
    expect(repo.movies.target(requested.targetId)!.active_version_id).toBeNull();
    await until(() => result.results[0]!.jobIds.every(id => repo.getJob(id)?.status === "succeeded"));
    const nextId = repo.movies.target(requested.targetId)!.active_version_id!, next = repo.movies.version(nextId)!;
    expect(nextId).not.toBe(oldId);
    expect(next.facts).toEqual(old.facts);
    expect(next.files[0]!.id).not.toBe(old.files[0]!.id);
    expect(await fs.readFile(path.join(root, "objects", next.files[0]!.relativePath), "utf8")).toContain("synthetic movie bytes");
    expect(movieRequestView(repo, requested.requestId).available).toBe(true);
    expect(repo.movies.requests(user.id).map(request => request.id)).toEqual([requested.requestId]);
    expect(service.recover(command)).toEqual(result);
    expect(fixture.stats.snatches).toBe(1);
    expect(fixture.stats.removals).toBe(0);
    expect(fixture.stats.rechecks).toBe(loseSeed ? 1 : 0);
  } finally { await runtime.stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); }
}, 15000);
