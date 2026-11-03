import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createPodibleFetchHandler } from "../../src/http";
import { hashSessionToken } from "../../src/auth";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import {
  createFixtureMovieServices,
  fixtureMovie,
} from "../fixtures/movie-services";

test("user-safe movie RPC shares acquisition, protects opaque offers and reports real worker readiness", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-rpc-")),
  );
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies = {
    ...settings.movies,
    enabled: true,
    managedRoot: root,
    plexViewPath: "/data/view",
  };
  repo.updateSettings(settings);
  const fixture = createFixtureMovieServices(root);
  fixture.addOffer("rpc-fixture", 1280);
  const users = ["one", "two"].map((username) => {
    const user = repo.upsertUser({
      provider: "local",
      providerUserId: username,
      username,
      isAdmin: false,
    });
    repo.createSession(user.id, hashSessionToken(username), null, "browser");
    return user;
  });
  const handler = createPodibleFetchHandler(repo, Date.now(), null, {
    movieServices: fixture.services,
  });
  async function rpc(method: string, params: unknown, token = "one") {
    return (await (
      await handler(
        new Request("http://localhost/rpc", {
          method: "POST",
          headers: {
            cookie: `podible_session=${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
        }),
      )
    ).json()) as any;
  }
  const runtime = await createJobExecutionRuntime(
    {
      repo,
      getSettings: () => repo.getSettings(),
      movieServices: fixture.services,
      onLog() {},
    },
    path.join(root, "execution.sqlite"),
  );
  try {
    const unauthorized = await rpc(
      "movies.search",
      { query: "fixture" },
      "invalid",
    );
    expect(unauthorized.error.code).toBe(-32001);
    const first = await rpc("movies.request", { tmdbId: fixtureMovie.tmdbId });
    const again = await rpc("movies.request", { tmdbId: fixtureMovie.tmdbId });
    const second = await rpc(
      "movies.request",
      { tmdbId: fixtureMovie.tmdbId },
      "two",
    );
    expect(first.result.request.id).toBe(again.result.request.id);
    expect(first.result.request.job.id).toBe(second.result.request.job.id);
    expect(first.result.request.available).toBe(false);
    expect(
      (await rpc("requests.get", { requestId: first.result.request.id }, "two"))
        .error.code,
    ).toBe(-32003);
    const search = (
      await rpc("requests.searchReleases", {
        requestId: first.result.request.id,
      })
    ).result;
    expect(search.results).toHaveLength(1);
    expect(JSON.stringify(search)).not.toContain("https:");
    expect(
      (
        await rpc(
          "requests.selectRelease",
          {
            requestId: second.result.request.id,
            searchId: search.searchId,
            resultIndex: 0,
          },
          "two",
        )
      ).error.code,
    ).toBe(-32602);
    await rpc("requests.withdraw", { requestId: first.result.request.id });
    expect(repo.getJob(first.result.request.job.id)?.status).toBe("queued");
    await runtime.reconcile();
    const deadline = Date.now() + 5000;
    let view: any;
    do {
      view = (
        await rpc(
          "requests.get",
          { requestId: second.result.request.id },
          "two",
        )
      ).result.request;
      if (view.available && view.stage === "available") break;
      if (Date.now() > deadline)
        throw new Error("Movie RPC did not become ready");
      await Bun.sleep(15);
    } while (true);
    expect(view.stage).toBe("available");
    expect(view.decisionNote).toContain("fixture");
    expect(JSON.stringify(view)).not.toContain(root);
    expect(JSON.stringify(view)).not.toContain("fixture.invalid");
    expect(fixture.stats.snatches).toBe(1);
    const trpc = await handler(
      new Request(
        "http://localhost/api/trpc/requests.get?input=" +
          encodeURIComponent(
            JSON.stringify({ requestId: second.result.request.id }),
          ),
        { headers: { cookie: "podible_session=two" } },
      ),
    );
    expect(trpc.status).toBe(200);
    expect(((await trpc.json()) as any).result.data.request.available).toBe(
      true,
    );
    const crossOrigin = await handler(
      new Request("http://localhost/api/trpc/movies.request", {
        method: "POST",
        headers: {
          origin: "https://evil.invalid",
          cookie: "podible_session=one",
        },
        body: "{}",
      }),
    );
    expect(crossOrigin.status).toBe(403);
    expect(users).toHaveLength(2);
  } finally {
    await runtime.stop();
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
}, 10_000);
