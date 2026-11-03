import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import {
  evaluateMovieShadow,
  shadowInventory,
  syncMovieShadow,
  type MovieShadowServices,
} from "../../src/movies/shadow";
import { RadarrObserver } from "../../src/movies/shadow-radarr";
import type { ShadowObservation } from "../../src/movies/shadow-types";
import { fixtureMovie } from "../fixtures/movie-services";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createPodibleFetchHandler } from "../../src/http";
import { hashSessionToken } from "../../src/auth";

test("shadow observes independently, deduplicates immutable inputs, and never modifies source files", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "shadow-")),
  );
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies.shadow = {
    ...settings.movies.shadow,
    enabled: true,
    pathMappings: [{ remoteRoot: "/downloads", localRoot: root }],
  };
  repo.updateSettings(settings);
  await fs.writeFile(path.join(root, "feature.mkv"), "unaltered fixture bytes");
  const observation: ShadowObservation = {
    key: "radarr:queue:1",
    movie: fixtureMovie,
    releaseTitle: "A Fixture Film 2024 WEB 1080p",
    sourcePaths: ["/downloads"],
    observedAt: new Date().toISOString(),
    radarr: { outcome: "blocked", messages: ["secret Radarr answer"] },
  };
  let calls = 0;
  const services: MovieShadowServices = {
    observe: async () => [observation],
    probe: async () => ({
      streams: [
        {
          codec_type: "video",
          codec_name: "h264",
          width: 1920,
          height: 1080,
          color_transfer: "bt709",
        },
        { codec_type: "audio", tags: { language: "eng" } },
      ],
    }),
    inspect: async (input) => {
      calls++;
      expect(JSON.stringify(input)).not.toContain("secret Radarr answer");
      expect(JSON.stringify(input)).not.toContain('"outcome"');
      expect(input.offer.key).toBe("shadow-candidate");
      return {
        fileIndex: 0,
        facts: {
          identityVerified: true,
          cut: "theatrical",
          source: "web",
          proper: false,
        },
        reason: "Target and feature file agree",
      };
    },
  };
  const ctx = {
    repo,
    getSettings: () => settings,
    movieShadowServices: services,
  };
  try {
    expect(settings.movies.enabled).toBe(false);
    await syncMovieShadow(ctx);
    const first = repo.movieShadow.list()[0]!;
    await evaluateMovieShadow(ctx, repo.getJob(first.jobId!)!);
    await syncMovieShadow(ctx);
    await evaluateMovieShadow(ctx, repo.getJob(first.jobId!)!);
    expect(calls).toBe(1);
    expect(repo.movieShadow.list()).toHaveLength(1);
    observation.libraryPath = "/library/a-different-path.mkv";
    await syncMovieShadow(ctx);
    expect(repo.movieShadow.list()).toHaveLength(1);
    expect(repo.movieShadow.get(first.id)!.result?.decision).toBe("import");
    expect(await fs.readFile(path.join(root, "feature.mkv"), "utf8")).toBe(
      "unaltered fixture bytes",
    );
    await fs.writeFile(path.join(root, "feature.mkv"), "different evidence");
    await syncMovieShadow(ctx);
    expect(repo.movieShadow.list()).toHaveLength(2);
    observation.sourcePaths = ["/downloads/missing.mkv"];
    await syncMovieShadow(ctx);
    const missing = repo.movieShadow.list()[0]!;
    await evaluateMovieShadow(ctx, repo.getJob(missing.jobId!)!);
    expect(repo.movieShadow.get(missing.id)!.result?.decision).toBe(
      "unobservable",
    );
    expect(calls).toBe(1);
    await fs.symlink("/etc/passwd", path.join(root, "escape.mkv"));
    observation.sourcePaths = ["/downloads/escape.mkv"];
    expect((await shadowInventory(observation, settings.movies)).files).toEqual(
      [],
    );
    expect(repo.movies.targets()).toEqual([]);
    observation.sourcePaths = ["/downloads/feature.mkv"];
    observation.libraryPath = "/downloads/feature.mkv";
    expect((await shadowInventory(observation, settings.movies)).source).toBe("library-fallback");
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Radarr base-path redirects fail explicitly without forwarding credentials", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const settings = new BooksRepo(db).ensureSettings();
  settings.movies.shadow.radarrUrl = "http://radarr:7878";
  settings.movies.shadow.radarrApiKey = "private";
  let calls = 0;
  const fetcher = (async (_url: unknown, init: RequestInit) => {
    calls++;
    expect(init.redirect).toBe("manual");
    return new Response(null, {
      status: 307,
      headers: { Location: "http://other-host/radarr" },
    });
  }) as typeof fetch;
  try {
    await expect(
      new RadarrObserver(settings.movies.shadow, fetcher).observe(),
    ).rejects.toThrow("include its configured URL base");
    expect(calls).toBe(1);
  } finally {
    db.close();
  }
});

test("shadow jobs execute through Reflow while native acquisition is disabled; RPC is admin-only", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "shadow-execution-"));
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies.shadow.enabled = true;
  repo.updateSettings(settings);
  for (const role of ["admin", "reader"]) {
    const user = repo.upsertUser({
      provider: "local",
      providerUserId: role,
      username: role,
      isAdmin: role === "admin",
    });
    repo.createSession(user.id, hashSessionToken(role), null, "browser");
  }
  const handler = createPodibleFetchHandler(repo, Date.now());
  const rpc = async (method: string, token: string, params = {}) =>
    (await (
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
  const runtime = await createJobExecutionRuntime(
    {
      repo,
      getSettings: () => repo.getSettings(),
      movieShadowServices: {
        observe: async () => [],
        inspect: async () => {
          throw new Error("No agent expected");
        },
        probe: async () => {
          throw new Error("No probe expected");
        },
      },
      onLog() {},
    },
    path.join(root, "execution.sqlite"),
  );
  try {
    for (const method of ["status", "list", "get", "sync", "retry", "review"])
      expect((await rpc(`movieShadow.${method}`, "reader")).error.code).toBe(
        -32003,
      );
    const status = await rpc("movieShadow.status", "admin");
    expect(status.result.enabled).toBe(true);
    expect(status.result.radarrApiKey).toBeUndefined();
    const queued = await rpc("movieShadow.sync", "admin");
    await runtime.reconcile();
    for (
      let i = 0;
      i < 100 && repo.getJob(queued.result.jobId)?.status !== "succeeded";
      i++
    )
      await Bun.sleep(20);
    expect(repo.getJob(queued.result.jobId)?.status).toBe("succeeded");
    expect(
      (await rpc("movieShadow.status", "admin")).result.lastSync.jobId,
    ).toBe(queued.result.jobId);
  } finally {
    await runtime.stop();
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Radarr observer is GET-only, covers blocked imports/history, and discards credential-bearing data", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const settings = new BooksRepo(db).ensureSettings();
  settings.movies.shadow.radarrUrl = "http://radarr:7878";
  settings.movies.shadow.radarrApiKey = "private-key";
  const requests: string[] = [];
  const fetcher = (async (url: URL | string | Request, init?: RequestInit) => {
    requests.push(String(url));
    expect(init?.method).toBe("GET");
    const movie = { id: 2, tmdbId: 101, title: "Film", year: 2024 };
    const record = String(url).includes("queue?")
      ? {
          id: 1,
          movie,
          sizeleft: 0,
          outputPath: "/downloads/film",
          errorMessage: "See https://example.com/?apikey=secret",
        }
      : {
          id: 2,
          movie,
          eventType: "downloadFolderImported",
          date: new Date().toISOString(),
          data: {
            importedPath: "/library/film.mkv",
            downloadUrl: "https://secret/?passkey=secret",
          },
        };
    return Response.json({ records: [record], totalRecords: 1 });
  }) as typeof fetch;
  try {
    const observations = await new RadarrObserver(
      settings.movies.shadow,
      fetcher,
    ).observe();
    expect(observations.map((o) => o.radarr.outcome)).toEqual([
      "blocked",
      "imported",
    ]);
    expect(JSON.stringify(observations)).not.toContain("secret");
    expect(requests).toHaveLength(2);
  } finally {
    db.close();
  }
});
