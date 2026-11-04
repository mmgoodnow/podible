import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import {
  movieRequestViewSchema,
  type MovieRequestView,
} from "../../src/movies/contracts";
import { BooksRepo } from "../../src/repo";
import { jobJournalReader } from "../../src/worker/job-journal";
import { fixtureMovie } from "../fixtures/movie-services";
import {
  checkpointFile,
  upgradeServerViewRoot,
  type UpgradeControl,
  type UpgradeFixtureState,
} from "../fixtures/movie-upgrade-process";

async function waitFor<T>(
  label: string,
  read: () => Promise<T | undefined>,
): Promise<T> {
  const deadline = Date.now() + 12_000;
  while (Date.now() < deadline) {
    const value = await read();
    if (value !== undefined) return value;
    await Bun.sleep(20);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function setup() {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-upgrade-acceptance-")),
  );
  const db = new Database(path.join(root, "app.sqlite"));
  db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  Object.assign(settings.movies, {
    managedRoot: root,
    plexViewPath: upgradeServerViewRoot,
    automaticUpgrades: false,
  });
  repo.updateSettings(settings);
  for (const token of ["owner", "stranger"]) {
    const user = repo.upsertUser({
      provider: "local", providerUserId: token, username: token, isAdmin: false,
    });
    repo.createSession(user.id, hashSessionToken(token), null, "browser");
  }
  let control: UpgradeControl = {
    offers: ["original"], plexVerified: true, pauseAt: [],
  };
  const configure = async (changes: Partial<UpgradeControl>) => {
    control = { ...control, ...changes };
    await fs.writeFile(
      path.join(root, "control.json.tmp"), JSON.stringify(control),
    );
    await fs.rename(
      path.join(root, "control.json.tmp"), path.join(root, "control.json"),
    );
  };
  await configure({});
  const launch = () => Bun.spawn(
    [
      process.execPath, "run",
      path.resolve(import.meta.dir, "../fixtures/movie-upgrade-process.ts"), root,
    ],
    {
      cwd: path.resolve(import.meta.dir, "../.."),
      stdin: "ignore", stdout: "ignore", stderr: "pipe",
    },
  );
  let worker:
    | { child: ReturnType<typeof launch>; stderr: Promise<string> }
    | undefined;
  let port = 0;
  const crash = async () => {
    if (!worker) return;
    worker.child.kill("SIGKILL");
    await worker.child.exited;
    expect(worker.child.signalCode).toBe("SIGKILL");
    const stderr = await worker.stderr;
    worker = undefined;
    expect(stderr).toBe("");
  };
  const start = async () => {
    await fs.rm(path.join(root, "ready.json"), { force: true });
    const child = launch();
    worker = { child, stderr: new Response(child.stderr).text() };
    const ready = await waitFor("child HTTP/RPC listener", async () => {
      if (child.exitCode !== null)
        throw new Error(`Movie worker exited: ${await worker!.stderr}`);
      const text = await fs.readFile(path.join(root, "ready.json"), "utf8")
        .catch((error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return undefined;
          throw error;
        });
      return text
        ? JSON.parse(text) as { port: number; pid: number }
        : undefined;
    });
    port = ready.port;
    repo.bindJobJournalReader(
      jobJournalReader(path.join(root, "execution.sqlite")),
    );
    return ready.pid;
  };
  const exchange = async (method: string, params: unknown, token = "owner") => {
    const response = await fetch(`http://127.0.0.1:${port}/rpc`, {
      method: "POST",
      headers: {
        cookie: `podible_session=${token}`, "Content-Type": "application/json",
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      signal: AbortSignal.timeout(3000),
    });
    return await response.json() as {
      result?: { request: unknown };
      error?: { code: number; message: string };
    };
  };
  const rpc = async (method: string, params: unknown) => {
    const body = await exchange(method, params);
    expect(body.error).toBeUndefined();
    return movieRequestViewSchema.parse(body.result?.request);
  };
  const view = (requestId: number) => rpc("requests.get", { requestId });
  const settled = (requestId: number, status = "succeeded") =>
    waitFor(`request job ${status}`, async () => {
      const current = await view(requestId);
      if (current.job?.status === "failed" && status !== "failed")
        throw new Error(JSON.stringify(current));
      return current.job?.status === status ? current : undefined;
    });
  const paused = (name: string) =>
    waitFor(`upgrade checkpoint ${name}`, async () => {
      if (!worker || worker.child.exitCode !== null)
        throw new Error("Movie worker exited before checkpoint");
      return await fs.readFile(checkpointFile(root, name), "utf8")
        .then((text) => JSON.parse(text) as { jobId: number; pid: number })
        .catch((error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return undefined;
          throw error;
        });
    });
  const release = (name: string) =>
    fs.writeFile(`${checkpointFile(root, name)}.continue`, "resume");
  const state = async (): Promise<UpgradeFixtureState> =>
    JSON.parse(await fs.readFile(path.join(root, "torrent-state.json"), "utf8"));
  const version = async (targetId: number) => {
    const id = repo.movies.target(targetId)!.active_version_id!;
    expect(id).toBeGreaterThan(0);
    const file = path.join(
      root, "objects", repo.movies.version(id)!.files[0]!.relativePath,
    );
    return { id, file, bytes: await fs.readFile(file) };
  };
  type ViewLink = { file: string; canonical: string; bytes: Buffer };
  const links = async (
    directory = path.join(root, "views", "plex", "movies"),
  ): Promise<ViewLink[]> => {
    const result: ViewLink[] = [];
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) result.push(...await links(file));
      else if (entry.isSymbolicLink())
        result.push({
          file, canonical: await fs.realpath(file), bytes: await fs.readFile(file),
        });
    }
    return result;
  };
  const oldAvailable = async (
    request: MovieRequestView,
    old: Awaited<ReturnType<typeof version>>,
    upgrading: boolean,
  ) => {
    const current = await view(request.id);
    expect(current.available).toBe(true);
    expect(current.state).toBe("available");
    expect(current.upgrading).toBe(upgrading);
    expect(repo.movies.target(request.targetId)!.active_version_id).toBe(old.id);
    expect(await fs.readFile(old.file)).toEqual(old.bytes);
    const originalLink = (await links()).find((link) => link.canonical === old.file);
    expect(originalLink?.bytes).toEqual(old.bytes);
    return current;
  };
  const retained = async (targetId: number, keys: string[]) => {
    const persisted = await state();
    expect(persisted.stats.removals).toBe(0);
    for (const key of keys) {
      const download = repo.movies.downloads(targetId)
        .find((item) => item.offer_key === key)!;
      expect(download).toBeDefined();
      const client = persisted.downloads
        .find(([hash]) => hash === download.native_id)![1];
      expect(client.resumed).toBe(true);
      expect(persisted.quiesced).not.toContain(download.native_id);
      expect(JSON.parse(download.observation_json!)).toMatchObject({
        complete: true, state: "seeding",
      });
      expect(await fs.readFile(
        path.join(root, "downloads", client.location.relativePath, `${key}.mkv`),
        "utf8",
      )).toBe(`synthetic movie bytes ${key}`);
    }
  };
  const reconcile = async () => {
    // Queue the actual maintenance worker, not a direct publication helper call.
    const job = repo.createJob({ type: "movie_reconcile", maxAttempts: 1 });
    await waitFor("movie reconcile worker", async () => {
      const current = repo.getJob(job.id)!;
      if (current.status === "failed") throw new Error(current.error!);
      return current.status === "succeeded" ? current : undefined;
    });
  };
  const count = (table: "movie_versions" | "managed_content_files") =>
    (db.query(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n;
  return {
    root, repo, configure, start, crash, exchange, rpc, settled, paused, release,
    state, version, links, oldAvailable, retained, reconcile, count,
    close: async () => {
      try {
        await crash();
      } finally {
        db.close();
        await fs.rm(root, { recursive: true, force: true });
      }
    },
  };
}

test("RPC/Reflow upgrade keeps the old movie through a stalled replacement, Plex rejection, and reconcile", async () => {
  const s = await setup();
  try {
    await s.start();
    const request = await s.rpc("movies.request", { tmdbId: fixtureMovie.tmdbId });
    await s.settled(request.id);
    const old = await s.version(request.targetId);
    const downloading = "movie:attempt:0:poll:1";
    const publishing = "movie:attempt:1:publish:0";
    await s.configure({
      offers: ["original", "stalled", "better"],
      plexVerified: false,
      pauseAt: [downloading, publishing],
    });
    const acquisitions = s.repo.movies.acquisitions(request.targetId).length;
    for (const [token, code] of [["invalid", -32001], ["stranger", -32003]] as const)
      expect((await s.exchange(
        "requests.checkUpgrade", { requestId: request.id }, token,
      )).error?.code).toBe(code);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(acquisitions);
    const upgrade = await s.rpc("requests.checkUpgrade", { requestId: request.id });
    await s.paused(downloading);
    const inProgress = await s.oldAvailable(request, old, true);
    expect(inProgress.stage).toBe("downloading");
    expect(inProgress.download).toMatchObject({ bytesDone: 0 });
    expect((await s.links()).map((link) => link.canonical)).toEqual([old.file]);
    expect((await s.rpc("requests.checkUpgrade", { requestId: request.id })).job?.id).toBe(upgrade.job!.id);
    expect((await s.rpc("movies.request", { tmdbId: fixtureMovie.tmdbId })).job?.id).toBe(upgrade.job!.id);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(acquisitions + 1);
    await s.release(downloading);
    await s.paused(publishing);
    const rejected = await s.oldAvailable(request, old, true);
    expect(rejected.stage).toBe("publishing");
    expect(rejected.events.some((event) => event.stage === "replacing")).toBe(true);
    const pending = s.repo.movies.publications()[0]!;
    expect(pending.version_id).not.toBe(old.id);
    const pendingFile = path.join(
      s.root, "objects",
      s.repo.movies.version(pending.version_id)!.files[0]!.relativePath,
    );
    expect((await s.links()).map((link) => link.canonical).sort())
      .toEqual([old.file, pendingFile].sort());
    expect((await s.state()).verifications.at(-1)?.accepted).toBe(false);
    await s.release(publishing);
    await s.settled(request.id, "failed");
    expect((await s.oldAvailable(request, old, false)).stage).toBe("blocked");
    expect(s.repo.movies.publications()).toHaveLength(1);
    expect(s.count("movie_versions")).toBe(2);
    const failed = await s.state();
    expect(failed.stats.snatches).toBe(3);
    expect(failed.addCalls).toHaveLength(3);
    expect(failed.stats.quiesces).toBe(1);
    await s.retained(request.targetId, ["original", "better"]);
    await s.reconcile();
    expect(s.repo.movies.publications()).toHaveLength(0);
    expect((await s.links()).map((link) => link.canonical)).toEqual([old.file]);
    await s.oldAvailable(request, old, false);
    await s.rpc("requests.withdraw", { requestId: request.id });
    await s.reconcile();
    await s.retained(request.targetId, ["original", "better"]);
    expect(await fs.readFile(old.file)).toEqual(old.bytes);
    expect(await fs.readFile(pendingFile, "utf8"))
      .toBe("synthetic movie bytes better");
    expect(s.count("managed_content_files")).toBe(2);
  } finally {
    await s.close();
  }
}, 40_000);

for (const boundary of ["movie:attempt:0:snatch", "movie:attempt:0:publish:0"]) {
  test(`authenticated upgrade survives SIGKILL at ${boundary} and suppresses repeated/equal upgrades`, async () => {
    const s = await setup();
    try {
      const firstPid = await s.start();
      const request = await s.rpc("movies.request", { tmdbId: fixtureMovie.tmdbId });
      await s.settled(request.id);
      const old = await s.version(request.targetId);
      await s.configure({ offers: ["original", "better"], plexVerified: false, pauseAt: [boundary] });
      const upgrade = await s.rpc("requests.checkUpgrade", { requestId: request.id });
      expect((await s.paused(boundary)).pid).toBe(firstPid);
      await s.oldAvailable(request, old, true);
      const acquisition = s.repo.movies.acquisitions(request.targetId)[0]!;
      const execution = s.repo.getJobExecution(upgrade.job!.id)!;
      const generation = s.repo.jobExecutionGeneration(upgrade.job!.id);
      expect(acquisition.reason).toBe("upgrade");
      expect(acquisition.finished_at).toBeNull();
      expect((await s.state()).stats.snatches).toBe(2);
      const pendingId = s.repo.movies.publications()[0]?.version_id;
      if (boundary.endsWith("publish:0")) {
        expect(pendingId).toBeGreaterThan(old.id);
        expect((await s.links())).toHaveLength(2);
        expect((await s.state()).verifications.at(-1)?.accepted).toBe(false);
      }
      await s.crash();
      expect(s.repo.movies.target(request.targetId)!.active_version_id).toBe(old.id);
      expect(await fs.readFile(old.file)).toEqual(old.bytes);
      // Remain unable to publish after restart, to inspect the recovered worker.
      const resumedBoundary = "movie:attempt:0:publish:1";
      await s.configure({ pauseAt: [resumedBoundary] });
      expect(await s.start()).not.toBe(firstPid);
      await s.paused(resumedBoundary);
      await s.oldAvailable(request, old, true);
      expect(s.repo.getJobExecution(upgrade.job!.id)!.run_id).toBe(execution.run_id);
      expect(s.repo.jobExecutionGeneration(upgrade.job!.id)).toBe(generation);
      expect(s.repo.movies.acquisitions(request.targetId)[0]!.id).toBe(acquisition.id);
      expect((await s.state()).addCalls).toHaveLength(2);
      expect(s.count("movie_versions")).toBe(2);
      expect(s.count("managed_content_files")).toBe(2);
      await s.configure({ plexVerified: true });
      await s.release(resumedBoundary);
      const available = await s.settled(request.id);
      expect(available.available).toBe(true);
      expect(available.upgrading).toBe(false);
      expect(available.stage).toBe("available");
      const better = await s.version(request.targetId);
      expect(better.id).not.toBe(old.id);
      if (pendingId) expect(better.id).toBe(pendingId);
      expect(better.bytes.toString()).toBe("synthetic movie bytes better");
      expect(await fs.readFile(old.file)).toEqual(old.bytes);
      expect(s.repo.movies.publications()).toHaveLength(0);
      expect((await s.links()).map((link) => link.canonical)).toEqual([better.file]);
      expect((await s.state()).verifications.at(-1)?.accepted).toBe(true);
      const before = await s.state();
      const revision = s.repo.movies.target(request.targetId)!.revision;
      for (const offers of [["original", "better"], ["original", "better", "equal"]]) {
        await s.configure({ offers, pauseAt: [] });
        const noOp = await s.rpc("requests.checkUpgrade", { requestId: request.id });
        expect(noOp.job!.id).not.toBe(upgrade.job!.id);
        const retained = await s.settled(request.id);
        expect(retained.available).toBe(true);
        expect(retained.stage).toBe("retained");
        expect(retained.decisionNote).toBe("No eligible improvement");
        if (offers.includes("equal")) {
          expect((await s.state()).equalProposals).toBe(1);
          expect(retained.events.some((event) =>
            event.message === "Release does not improve the current version",
          )).toBe(true);
        }
        expect(s.repo.movies.target(request.targetId)).toMatchObject({ active_version_id: better.id, revision });
      }
      await s.reconcile();
      const after = await s.state();
      expect(after.stats.searches).toBe(before.stats.searches + 3);
      expect(after.stats.snatches).toBe(2);
      expect(after.addCalls).toEqual(before.addCalls);
      expect(after.stats.probes).toBe(before.stats.probes);
      expect(after.verifications).toEqual(before.verifications);
      expect(s.count("movie_versions")).toBe(2);
      expect(s.count("managed_content_files")).toBe(2);
      expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(4);
      const eventIds = s.repo.movies.events(request.targetId).filter((event) => event.stage === "available");
      expect(eventIds).toHaveLength(2);
      await s.retained(request.targetId, ["original", "better"]);
      expect((await s.links()).map((link) => link.canonical)).toEqual([better.file]);
      expect(await fs.readFile(old.file)).toEqual(old.bytes);
    } finally {
      await s.close();
    }
  }, 40_000);
}
