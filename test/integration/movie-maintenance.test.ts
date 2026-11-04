import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import {
  queueMovieAcquisition,
  movieRequestView,
} from "../../src/movies/service";
import { acquireMovie } from "../../src/movies/acquisition";
import { reconcileMovies } from "../../src/movies/maintenance";
import {
  createFixtureMovieServices,
  fixtureMovie,
} from "../fixtures/movie-services";
test("reconciliation does not search; explicit recovery and upgrades preserve current content", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-maintenance-")),
  );
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies = {
    ...settings.movies,
    managedRoot: root,
    plexViewPath: "/view",
  };
  repo.updateSettings(settings);
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "maintenance",
    username: "test",
    isAdmin: false,
  });
  const requested = repo.movies.request(
    user.id,
    fixtureMovie,
    settings.movies.qualityPolicy,
  );
  const fixture = createFixtureMovieServices(root);
  const ctx = {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
  };
  try {
    const first = queueMovieAcquisition(
      repo,
      requested.targetId,
      "initial",
    ).acquisition;
    await acquireMovie(ctx, repo.getJob(first.job_id!)!, fixture.services);
    expect(movieRequestView(repo, requested.requestId).state).toBe("blocked");
    fixture.addOffer("available", 1280);
    const recovery = queueMovieAcquisition(
      repo,
      requested.targetId,
      "recovery",
    ).acquisition;
    expect(movieRequestView(repo, requested.requestId).state).toBe("queued");
    await acquireMovie(ctx, repo.getJob(recovery.job_id!)!, fixture.services);
    const old = repo.movies.target(requested.targetId)!.active_version_id;
    fixture.addOffer("scheduled-upgrade", 1920);
    db.query(
      "UPDATE movie_acquisitions SET created_at='2020-01-01T00:00:00.000Z'",
    ).run();
    await reconcileMovies(ctx);
    expect(repo.movies.acquisitions(requested.targetId)).toHaveLength(2);
    expect(fixture.stats.searches).toBe(2);
    const upgrade = queueMovieAcquisition(repo, requested.targetId, "upgrade").acquisition;
    expect(upgrade.reason).toBe("upgrade");
    expect(movieRequestView(repo, requested.requestId).available).toBe(true);
    await acquireMovie(ctx, repo.getJob(upgrade.job_id!)!, fixture.services);
    expect(repo.movies.target(requested.targetId)!.active_version_id).not.toBe(
      old,
    );
    await reconcileMovies(ctx);
    expect(repo.movies.acquisitions(requested.targetId)).toHaveLength(3);
    repo.movies.withdrawRequest(requested.requestId);
    await reconcileMovies(ctx);
    expect(fixture.stats.quiesces).toBe(0);
    expect(fixture.stats.removals).toBe(0);
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
