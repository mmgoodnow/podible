import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { syncSeerrRequests } from "../../src/movies/dual-write";
import { seerrInstance } from "../../src/movies/seerr";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createPodibleFetchHandler } from "../../src/http";
import { hashSessionToken } from "../../src/auth";
import type { WorkerContext } from "../../src/worker/context";
import { createFixtureMovieServices } from "../fixtures/movie-services";
import { createFixtureSeerrSource, seerrRequest } from "../fixtures/seerr";

async function setup() {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-seerr-")),
  );
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies.seerr = {
    url: "https://seerr.invalid/base",
    apiKey: "seerr-private-key",
    pollIntervalMinutes: 1,
  };
  settings.movies.managedRoot = root;
  settings.movies.plexViewPath = "/isolated/movies";
  repo.updateSettings(settings);
  const fixture = createFixtureMovieServices(root);
  const feed = createFixtureSeerrSource([seerrRequest(100)]);
  const ctx: WorkerContext = {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
    seerrSource: feed.source,
    onLog() {},
  };
  const instance = seerrInstance(settings.movies.seerr.url);
  const sync = async (payload = {}, context = ctx) => {
    const job = repo.createJob({ type: "movie_dual_write_sync", payload });
    await syncSeerrRequests(context, job);
    return job;
  };
  const count = (table: string) =>
    (db.query(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n;
  return {
    root,
    db,
    repo,
    fixture,
    feed,
    ctx,
    instance,
    sync,
    count,
    close: async () => {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    },
  };
}

test("Seerr activation never backfills history; later approvals of new pending requests are imported once", async () => {
  const s = await setup();
  try {
    await s.sync();
    expect(s.repo.movieSeerr.feed(s.instance)?.baselineId).toBe(100);
    expect(s.count("movie_requests")).toBe(0);
    s.feed.records.set(101, seerrRequest(101, 1));
    s.feed.records.set(102, seerrRequest(102, 3));
    s.feed.records.set(103, seerrRequest(103, 2, { type: "tv" }));
    await s.sync();
    expect(s.count("movie_requests")).toBe(0);
    s.feed.records.set(
      100,
      seerrRequest(100, 2, { updatedAt: "2026-11-02T15:00:00.000Z" }),
    );
    s.feed.records.set(
      101,
      seerrRequest(101, 2, { updatedAt: "2026-11-02T15:00:00.000Z" }),
    );
    await s.sync();
    await s.sync();
    expect(s.repo.movieSeerr.list().map((r) => r.externalRequestId)).toEqual([
      101,
    ]);
    expect(s.count("movie_requests")).toBe(1);
    expect(s.count("movie_acquisitions")).toBe(1);
    expect(s.repo.listJobsByType("movie_acquire")).toHaveLength(1);
    expect(s.feed.calls.movies).toHaveLength(1);
    const user = s.repo.listUsers()[0]!;
    expect(user).toMatchObject({
      provider: "local",
      provider_user_id: `seerr:${s.instance}:42`,
      is_admin: 0,
    });
    expect(s.repo.listUsers("plex")).toHaveLength(0);
    expect(s.count("sessions")).toBe(0);
  } finally {
    await s.close();
  }
});

test("explicit backfill is bounded, validates all IDs, and preserves existing Plex owner privileges", async () => {
  const s = await setup();
  try {
    const owner = s.repo.upsertUser({
      provider: "plex",
      providerUserId: "10332114",
      username: "owner",
      isAdmin: true,
    });
    s.feed.records.set(
      98,
      seerrRequest(98, 2, {
        requestedBy: { id: 1, plexId: 10332114, displayName: "owner" },
      }),
    );
    s.feed.records.set(99, seerrRequest(99, 1));
    await expect(s.sync({ backfillRequestIds: [98, 99] })).rejects.toThrow(
      "not an approved movie",
    );
    expect(s.count("movie_requests")).toBe(0);
    await expect(
      s.sync({ backfillRequestIds: [1, 2, 3, 4, 5, 6] }),
    ).rejects.toThrow();
    await expect(s.sync({ backfillRequestIds: [] })).rejects.toThrow();
    await s.sync({ backfillRequestIds: [98, 98, 100] });
    await s.sync({ backfillRequestIds: [98, 100] });
    expect(s.count("movie_request_origins")).toBe(2);
    expect(s.count("movie_requests")).toBe(2);
    expect(s.count("movie_acquisitions")).toBe(1);
    const origin = s.repo.movieSeerr.origin(s.instance, 98)!;
    expect(s.repo.movies.requestById(origin.requestId)?.user_id).toBe(owner.id);
    expect(s.repo.getUserById(owner.id)?.is_admin).toBe(1);
    expect(s.count("sessions")).toBe(0);
    expect(s.feed.calls.requests).not.toContain(1);
  } finally {
    await s.close();
  }
});

test("4K Seerr intent creates a 2160-only target without dropping explicit cut/language/HDR constraints", async () => {
  const s = await setup();
  try {
    const settings = s.repo.getSettings();
    settings.movies.qualityPolicy = {
      ...defaultMovieQualityPolicy,
      cut: "extended",
      audioLanguages: ["es"],
      allowHdr: false,
    };
    s.repo.updateSettings(settings);
    await s.sync();
    s.feed.records.set(101, seerrRequest(101, 4, { is4k: true }));
    s.feed.records.set(102, seerrRequest(102, 5));
    await s.sync();
    const origin = s.repo.movieSeerr.origin(s.instance, 101)!;
    const target = s.repo.movies.target(
      s.repo.movies.requestById(origin.requestId)!.target_id,
    )!;
    const policy = JSON.parse(target.policy_json);
    expect(
      policy.tiers.every((t: { resolution: number }) => t.resolution === 2160),
    ).toBe(true);
    expect(policy).toMatchObject({
      cut: "extended",
      audioLanguages: ["es"],
      allowHdr: false,
    });
    expect(s.count("movie_targets")).toBe(2);
    expect(s.count("movie_request_origins")).toBe(2);
  } finally {
    await s.close();
  }
});

test("failed job creation rolls back principal, origin, native request, acquisition and outbox atomically", async () => {
  const s = await setup();
  try {
    await s.sync();
    s.feed.records.set(101, seerrRequest(101));
    s.db.exec(
      "CREATE TRIGGER fail_movie_job BEFORE INSERT ON jobs WHEN NEW.type='movie_acquire' BEGIN SELECT RAISE(ABORT,'injected crash before job'); END",
    );
    await expect(s.sync()).rejects.toThrow("injected crash");
    for (const table of [
      "users",
      "movie_request_origins",
      "movie_requests",
      "movie_targets",
      "movie_acquisitions",
    ])
      expect(s.count(table)).toBe(0);
    expect(s.repo.movieSeerr.feed(s.instance)?.cursor).toBe(
      seerrRequest(100).createdAt,
    );
    s.db.exec("DROP TRIGGER fail_movie_job");
    await s.sync();
    expect(s.count("movie_request_origins")).toBe(1);
    expect(s.count("movie_acquisitions")).toBe(1);
    expect(s.db.query("PRAGMA foreign_key_check").all()).toHaveLength(0);
  } finally {
    await s.close();
  }
});

test("crash after atomic fanout but before cursor advancement retries without duplicates or orphans", async () => {
  const s = await setup();
  try {
    await s.sync();
    s.feed.records.set(101, seerrRequest(101));
    s.db.exec(
      "CREATE TRIGGER fail_cursor BEFORE UPDATE ON movie_request_feeds BEGIN SELECT RAISE(ABORT,'crash after fanout'); END",
    );
    await expect(s.sync()).rejects.toThrow("crash after fanout");
    expect(s.count("movie_request_origins")).toBe(1);
    expect(s.count("movie_acquisitions")).toBe(1);
    s.db.exec("DROP TRIGGER fail_cursor");
    await s.sync();
    expect(s.count("movie_request_origins")).toBe(1);
    expect(s.count("movie_requests")).toBe(1);
    expect(s.count("movie_acquisitions")).toBe(1);
    expect(s.feed.calls.movies).toHaveLength(1);
    expect(s.db.query("PRAGMA foreign_key_check").all()).toHaveLength(0);
  } finally {
    await s.close();
  }
});

test("Seerr feed baseline survives a crash before its checkpoint receipt", async () => {
  const s = await setup();
  try {
    const checkpoints = new Map<string, unknown>();
    let crashed = false;
    const context: WorkerContext = {
      ...s.ctx,
      execution: {
        key: "baseline-replay",
        assertActive() {},
        sleep: async () => {},
        run: async <T>(name: string, action: () => T | Promise<T>) => {
          if (checkpoints.has(name)) return checkpoints.get(name) as T;
          const result = await action();
          if (name === "seerr:feed" && !crashed) {
            crashed = true;
            throw new Error("process died before receipt");
          }
          checkpoints.set(name, structuredClone(result));
          return result;
        },
      },
    };
    const job = s.repo.createJob({ type: "movie_dual_write_sync" });
    await expect(syncSeerrRequests(context, job)).rejects.toThrow(
      "process died",
    );
    s.feed.records.set(101, seerrRequest(101));
    await syncSeerrRequests(context, job);
    expect(s.repo.movieSeerr.feed(s.instance)?.baselineId).toBe(100);
    expect(s.feed.calls.latest).toBe(1);
    expect(s.count("movie_request_origins")).toBe(1);
    const settings = s.repo.getSettings();
    settings.movies.seerr.url = "https://other-seerr.invalid";
    s.repo.updateSettings(settings);
    await expect(syncSeerrRequests(context, job)).rejects.toThrow(
      "connection changed",
    );
  } finally {
    await s.close();
  }
});

test("real Reflow Seerr fanout independently downloads, verifies and publishes with no external download adoption", async () => {
  const s = await setup();
  let runtime:
    | Awaited<ReturnType<typeof createJobExecutionRuntime>>
    | undefined;
  try {
    s.fixture.addOffer("independent-canary", 1920);
    s.repo.movieSeerr.start(s.instance, 100, seerrRequest(100).createdAt);
    s.feed.records.set(101, seerrRequest(101));
    const sync = s.repo.createJob({
      type: "movie_dual_write_sync",
      maxAttempts: 1,
    });
    runtime = await createJobExecutionRuntime(
      s.ctx,
      path.join(s.root, "execution.sqlite"),
    );
    const deadline = Date.now() + 7000;
    while (
      !s.repo.movies.targets()[0]?.active_version_id ||
      s.repo.getJob(sync.id)?.status !== "succeeded"
    ) {
      await runtime.reconcile();
      if (Date.now() > deadline)
        throw new Error(
          `Seerr fanout did not publish: ${JSON.stringify(s.repo.listRecentJobs(10))}`,
        );
      await Bun.sleep(10);
    }
    const target = s.repo.movies.targets()[0]!;
    const version = s.repo.movies.version(target.active_version_id!)!;
    expect(
      await fs.readFile(
        path.join(s.root, "objects", version.files[0]!.relativePath),
        "utf8",
      ),
    ).toContain("independent-canary");
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.fixture.stats.scans).toBeGreaterThan(0);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.repo.movies.events(target.id).at(-1)?.stage).toBe("available");
    expect(s.count("books")).toBe(0);
    expect(s.count("releases")).toBe(0);
    expect(s.count("sessions")).toBe(0);
    const acquisition = s.repo.movies.acquisitions(target.id)[0]!;
    const execution = await runtime.engine.getRunStatus(
      s.repo.getJobExecution(acquisition.job_id!)!.run_id,
    );
    expect(
      execution?.steps.some(
        (step) => step.name === "movie:attempt:0:search:request:0",
      ),
    ).toBe(true);
    expect(
      execution?.steps.some((step) => step.name === "movie:attempt:0:snatch"),
    ).toBe(true);
  } finally {
    await runtime?.stop();
    await s.close();
  }
}, 10_000);

test("Seerr control/status/progress RPCs are admin-only and omit source credentials and requester secrets", async () => {
  const s = await setup();
  try {
    for (const [token, isAdmin] of [
      ["admin", true],
      ["viewer", false],
    ] as const) {
      const user = s.repo.upsertUser({
        provider: "local",
        providerUserId: token,
        username: token,
        isAdmin,
      });
      s.repo.createSession(user.id, hashSessionToken(token), null, "browser");
    }
    const syncJob = await s.sync({ backfillRequestIds: [100] });
    const handler = createPodibleFetchHandler(s.repo, Date.now(), null, {
      movieServices: s.fixture.services,
    });
    const rpc = async (
      method: string,
      params: unknown = {},
      token = "admin",
    ) => {
      const response = await handler(
        new Request("http://localhost/rpc", {
          method: "POST",
          headers: {
            cookie: `podible_session=${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            method: method.includes(".") ? method : `movieDualWrite.${method}`,
            params,
          }),
        }),
      );
      return (await response.json()) as any;
    };
    for (const method of ["status", "list", "sync", "backfill"]) {
      const params = method === "backfill" ? { backfillRequestIds: [100] } : {};
      expect((await rpc(method, params, "viewer")).error.code).toBe(-32003);
      expect((await rpc(method, params, "invalid")).error.code).toBe(-32001);
    }
    expect((await rpc("backfill", {})).error.code).toBe(-32602);
    expect(
      (await rpc("backfill", { backfillRequestIds: [1, 2, 3, 4, 5, 6] })).error
        .code,
    ).toBe(-32602);
    const list = await rpc("list");
    expect(list.result.items[0].request.stage).toBe("requested");
    expect(list.result.items[0].requester).toBe("Katya");
    const status = await rpc("status");
    expect(status.result.keyConfigured).toBe(true);
    expect(JSON.stringify([list, status])).not.toContain("seerr-private-key");
    expect(JSON.stringify([list, status])).not.toContain("120148426");
    expect((await rpc("jobs.get", { jobId: syncJob.id })).result.job.type).toBe(
      "movie_dual_write_sync",
    );
    expect(
      (await rpc("jobs.list", { type: "movie_dual_write_sync" })).result.jobs[0]
        .id,
    ).toBe(syncJob.id);
    expect(
      (await rpc("jobs.list")).result.jobs.some(
        (job: { id: number }) => job.id === syncJob.id,
      ),
    ).toBe(true);
    const trpc = await handler(
      new Request(
        "http://localhost/api/trpc/movieDualWrite.list?input=" +
          encodeURIComponent("{}"),
        { headers: { cookie: "podible_session=viewer" } },
      ),
    );
    expect(trpc.status).toBe(403);
  } finally {
    await s.close();
  }
});

for (const checkpoint of ["seerr:metadata:101", "seerr:import:101"]) {
  test(`Seerr real process death after ${checkpoint} resumes fanout without duplicates or metadata refetch`, async () => {
    const s = await setup();
    const job = s.repo.createJob({
      type: "movie_dual_write_sync",
      maxAttempts: 1,
    });
    const launch = (stopAt = "") =>
      Bun.spawn(
        [
          process.execPath,
          "run",
          "test/fixtures/movie-seerr-process.ts",
          s.root,
          stopAt,
        ],
        {
          cwd: path.resolve(import.meta.dir, "../.."),
          stdout: "pipe",
          stderr: "pipe",
        },
      );
    const first = launch(checkpoint);
    let second: ReturnType<typeof launch> | undefined;
    try {
      const deadline = Date.now() + 5000;
      while (
        !(
          await fs.readFile(`${s.root}/events`, "utf8").catch(() => "")
        ).includes(checkpoint)
      ) {
        if (Date.now() > deadline)
          throw new Error("Seerr process fixture did not checkpoint");
        await Bun.sleep(10);
      }
      const runId = s.repo.getJobExecution(job.id)!.run_id;
      first.kill("SIGKILL");
      await first.exited;
      expect(s.count("movie_request_origins")).toBe(
        checkpoint.includes(":import:") ? 1 : 0,
      );
      second = launch();
      const exit = await Promise.race([
        second.exited,
        Bun.sleep(8000).then(() => -1),
      ]);
      expect(exit).toBe(0);
      expect(await new Response(second.stderr).text()).toBe("");
      expect(s.repo.getJob(job.id)?.status).toBe("succeeded");
      expect(s.repo.getJobExecution(job.id)?.run_id).toBe(runId);
      expect(s.count("movie_request_origins")).toBe(1);
      expect(s.count("movie_requests")).toBe(1);
      expect(s.count("movie_acquisitions")).toBe(1);
      expect(
        s.repo.movies.downloads(s.repo.movies.targets()[0]!.id),
      ).toHaveLength(1);
      expect(s.repo.movies.targets()[0]?.active_version_id).not.toBeNull();
      expect(
        s.db
          .query(
            "SELECT kind,COUNT(*) AS n FROM fixture_seerr_calls GROUP BY kind ORDER BY kind",
          )
          .all(),
      ).toEqual([
        { kind: "latest", n: 1 },
        { kind: "metadata", n: 1 },
      ]);
      expect(s.db.query("PRAGMA foreign_key_check").all()).toHaveLength(0);
    } finally {
      first.kill();
      second?.kill();
      if (second) await second.exited;
      await s.close();
    }
  }, 15_000);
}
