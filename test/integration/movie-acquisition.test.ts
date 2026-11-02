import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { defaultMovieQualityPolicy as policy } from "../../src/movies/quality";
import { acquireMovie } from "../../src/movies/acquisition";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import {
  createFixtureMovieServices,
  fixtureMovie,
} from "../fixtures/movie-services";

async function setup() {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-acquire-")),
  );
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = defaultSettings();
  settings.movies = {
    ...settings.movies,
    enabled: true,
    managedRoot: root,
    plexViewPath: "/data/views/plex/movies",
  };
  repo.updateSettings(settings);
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "movie",
    username: "requester",
    isAdmin: false,
  });
  const request = repo.movies.request(user.id, fixtureMovie, policy);
  const fixture = createFixtureMovieServices(root);
  const context = {
    repo,
    getSettings: () => repo.getSettings(),
    onLog: () => {},
    movieServices: fixture.services,
  };
  const enqueue = (reason: "initial" | "upgrade" = "initial") =>
    repo.movies.startAcquisition(
      request.targetId,
      reason,
      (id) =>
        repo.createJob({
          type: "movie_acquire",
          payload: { acquisitionId: id },
          maxAttempts: 1,
        }).id,
    ).acquisition;
  return {
    root,
    db,
    repo,
    fixture,
    request,
    context,
    enqueue,
    close: async () => {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    },
  };
}
test("movie acquisition publishes, upgrades without losing old bytes, and retains equal-quality versions", async () => {
  const s = await setup();
  try {
    s.fixture.addOffer("first", 1280);
    await acquireMovie(
      s.context,
      s.repo.getJob(s.enqueue().job_id!)!,
      s.fixture.services,
    );
    const first = s.repo.movies.target(s.request.targetId)!.active_version_id!;
    expect(first).toBeGreaterThan(0);
    expect(s.repo.movies.events(s.request.targetId).at(-1)?.stage).toBe(
      "available",
    );
    s.fixture.addOffer("better", 1920);
    await acquireMovie(
      s.context,
      s.repo.getJob(s.enqueue("upgrade").job_id!)!,
      s.fixture.services,
    );
    expect(
      s.repo.movies.target(s.request.targetId)!.active_version_id,
    ).not.toBe(first);
    const old = s.repo.movies.version(first)!.files[0]!;
    expect(
      await fs.readFile(path.join(s.root, "objects", old.relativePath), "utf8"),
    ).toContain("first");
    const selected = s.repo.movies.target(
      s.request.targetId,
    )!.active_version_id;
    s.fixture.addOffer("equal", 1920);
    await acquireMovie(
      s.context,
      s.repo.getJob(s.enqueue("upgrade").job_id!)!,
      s.fixture.services,
    );
    expect(s.repo.movies.target(s.request.targetId)!.active_version_id).toBe(
      selected,
    );
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.fixture.stats.removals).toBe(0);
  } finally {
    await s.close();
  }
});
test("stalled movie downloads are retained and replaced, with visible recovery events", async () => {
  const s = await setup();
  try {
    s.fixture.addOffer("stalled", 1920, true);
    s.fixture.addOffer("working", 1280);
    await acquireMovie(
      s.context,
      s.repo.getJob(s.enqueue().job_id!)!,
      s.fixture.services,
    );
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.fixture.stats.quiesces).toBe(1);
    expect(s.fixture.stats.removals).toBe(0);
    expect(
      s.repo.movies
        .events(s.request.targetId)
        .some((e) => e.stage === "replacing"),
    ).toBe(true);
    expect(
      s.repo.movies.target(s.request.targetId)!.active_version_id,
    ).not.toBeNull();
  } finally {
    await s.close();
  }
});
test("movie Reflow replay after snatch does not exclude its own selected release or add twice", async () => {
  const s = await setup();
  let runtime:
    Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
  try {
    s.fixture.addOffer("resumed", 1280);
    const acquisition = s.enqueue();
    const checkpoints = new Map<string, unknown>();
    let interrupted = false;
    const context = {
      ...s.context,
      execution: {
        key: "fixture-replay",
        assertActive() {},
        sleep: async () => {},
        run: async <T>(
          name: string,
          action: () => T | Promise<T>,
        ): Promise<T> => {
          if (checkpoints.has(name)) return checkpoints.get(name) as T;
          const result = await action();
          checkpoints.set(name, structuredClone(result));
          if (name.endsWith(":snatch") && !interrupted) {
            interrupted = true;
            throw new Error("process died after saved snatch");
          }
          return result;
        },
      },
    };
    await expect(
      acquireMovie(
        context,
        s.repo.getJob(acquisition.job_id!)!,
        s.fixture.services,
      ),
    ).rejects.toThrow("process died");
    await acquireMovie(
      context,
      s.repo.getJob(acquisition.job_id!)!,
      s.fixture.services,
    );
    expect(s.fixture.stats.snatches).toBe(1);
    expect(
      s.repo.movies.target(s.request.targetId)!.active_version_id,
    ).not.toBeNull();
    const next = s.enqueue("upgrade");
    runtime = await createJobExecutionRuntime(
      s.context,
      path.join(s.root, "execution.sqlite"),
    );
    await runtime.reconcile();
    const deadline = Date.now() + 5000;
    while (s.repo.getJob(next.job_id!)!.status !== "succeeded") {
      if (Date.now() > deadline)
        throw new Error("Movie execution did not finish");
      await Bun.sleep(10);
    }
  } finally {
    await runtime?.stop();
    await s.close();
  }
}, 10_000);
