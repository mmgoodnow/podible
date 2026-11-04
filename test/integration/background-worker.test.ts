import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { openDatabase } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { BackgroundWorker } from "../../src/runtime/background-worker";
import { createPodibleFetchHandler } from "../../src/http";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";

const entrypoint = new URL("../fixtures/background-worker.ts", import.meta.url);
async function until(check: () => boolean, timeout = 8000) {
  const deadline = Date.now() + timeout;
  while (!check()) { if (Date.now() > deadline) throw new Error("Background worker acceptance timed out"); await Bun.sleep(10); }
}
async function fixture(action: (f: {
  databasePath: string; db: ReturnType<typeof openDatabase>; repo: BooksRepo; worker: BackgroundWorker;
}) => Promise<void>) {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-worker-"));
  const databasePath = path.join(root, "app.sqlite"), db = openDatabase(databasePath);
  const worker = new BackgroundWorker({ databasePath, entrypoint });
  try {
    const ready = await worker.start();
    expect(ready.pid).toBe(process.pid);
    expect(ready.threadId).toBeGreaterThan(0);
    expect(ready.busyTimeoutMs).toBe(2000);
    const repo = new BooksRepo(db);
    await action({ databasePath, db, repo, worker });
  } finally { await worker.stop(); db.close(); await rm(root, { recursive: true, force: true }); }
}

test("real Bun worker migrates legacy journal before readiness and processes the durable main-thread outbox", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-migration-"));
  const databasePath = path.join(root, "app.sqlite"), db = openDatabase(databasePath);
  const legacy = new BooksRepo(db);
  legacy.ensureSettings();
  const old = legacy.createJob({ type: "metadata_hydration" });
  db.query("UPDATE jobs SET status='failed',error='historical',attempt_count=3 WHERE id=?").run(old.id);
  const worker = new BackgroundWorker({ databasePath, entrypoint });
  try {
    await worker.start();
    expect((db.query("PRAGMA table_info(jobs)").all() as { name: string }[]).some(c => c.name === "status")).toBe(false);
    const repo = new BooksRepo(db); // Same construction ordering as server.ts.
    expect(repo.getJob(old.id)).toMatchObject({ status: "failed", error: "historical", attempt_count: 3 });
    const job = repo.createJob({ type: "metadata_hydration" });
    await until(() => repo.getJob(job.id)?.status === "succeeded");
    expect(repo.getJsonState<boolean>(`finished:${job.id}`)).toBe(true);
    expect(repo.listJobCommands()).toEqual([]);
    expect(repo.getJobExecution(job.id)?.run_id).toBeTruthy();
    expect((db.query("PRAGMA busy_timeout").get() as { timeout: number }).timeout).toBe(2000);
  } finally { await worker.stop(); db.close(); await rm(root, { recursive: true, force: true }); }
});

for (const op of ["cpu", "lock"]) test(`real HTTP stays responsive during worker ${op} work`, () => fixture(async f => {
  const server = Bun.serve({ port: 0, fetch: createPodibleFetchHandler(f.repo, Date.now()) });
  try {
    const job = f.repo.createJob({ type: "metadata_hydration", payload: { op, ms: 900 } });
    await until(() => f.repo.getJsonState(`entered:${job.id}`) === true);
    const latencies: number[] = [];
    for (let i = 0; i < 6; i++) {
      const start = performance.now();
      const response = await fetch(`http://127.0.0.1:${server.port}/rpc/system/health`);
      expect(response.status).toBe(200);
      expect((await response.json() as { result: { ok: boolean } }).result.ok).toBe(true);
      latencies.push(performance.now() - start);
      await Bun.sleep(15);
    }
    expect(Math.max(...latencies)).toBeLessThan(400);
    expect(f.repo.getJsonState<boolean>(`finished:${job.id}`)).not.toBe(true);
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
  } finally { await server.stop(true); }
}));

test("abnormal worker exit preserves checkpoint, run/job/generation identity and recovers after confirmed replacement", () => fixture(async f => {
  const job = f.repo.createJob({ type: "metadata_hydration", payload: { op: "crash" } });
  await until(() => f.worker.state === "failed");
  const original = f.repo.getJobExecution(job.id)!;
  const generation = f.repo.jobExecutionGeneration(job.id);
  expect(f.repo.getJsonState<boolean>(`checkpoint:${job.id}`)).toBe(true);
  await f.worker.restart();
  await until(() => f.repo.getJob(job.id)?.status === "succeeded");
  expect(f.repo.getJobExecution(job.id)?.run_id).toBe(original.run_id);
  expect(f.repo.jobExecutionGeneration(job.id)).toBe(generation);
  expect(f.repo.getJsonState<boolean>(`finished:${job.id}`)).toBe(true);
  const storage = new SQLiteStorage(f.repo.executionDatabasePath());
  await storage.initialize();
  try {
    const runs = (await storage.listRuns()).filter(run => (run.input as { jobId?: number }).jobId === job.id);
    expect(runs).toHaveLength(1);
    expect((await storage.getStepResults(original.run_id)).find(step => step.name === "before-crash")?.status).toBe("completed");
  } finally { await storage.close(); }
}));

test("single-flight lifecycle refuses a second owner, replaces only after exit, and stops within deadlines", () => fixture(async f => {
  expect((await f.worker.start()).threadId).toBeGreaterThan(0);
  const duplicate = new BackgroundWorker({ databasePath: f.databasePath, entrypoint });
  await expect(duplicate.start()).rejects.toThrow("already owns");
  const [a, b] = await Promise.all([f.worker.restart(), f.worker.restart()]);
  expect(a.threadId).toBe(b.threadId);
  expect(f.worker.state).toBe("ready");
  const stopped = await f.worker.stop();
  expect(stopped.forced).toBe(false);
  expect(f.worker.state).toBe("stopped");
  await f.worker.start();
  expect(f.worker.state).toBe("ready");
}));

test("cancellation still blocks a stale worker callback", () => fixture(async f => {
  const job = f.repo.createJob({ type: "metadata_hydration", payload: { op: "wait", ms: 300 } });
  await until(() => f.repo.getJsonState(`entered:${job.id}`) === true);
  f.repo.markJobCancelled(job.id, "fixture cancellation");
  await until(() => f.repo.getJob(job.id)?.status === "cancelled");
  await Bun.sleep(400);
  expect(f.repo.getJsonState<boolean>(`finished:${job.id}`)).not.toBe(true);
}));

test("cancel then retry fences the old worker callback while only the new run/generation publishes", () => fixture(async f => {
  const job = f.repo.createJob({ type: "metadata_hydration", payload: { op: "wait", ms: 400 } });
  await until(() => f.repo.getJsonState(`entered:${job.id}`) === true);
  const original = f.repo.getJobExecution(job.id)!;
  const generation = f.repo.jobExecutionGeneration(job.id)!;
  f.repo.markJobCancelled(job.id, "fixture superseded execution");
  f.repo.retryJob(job.id);
  expect(f.repo.jobExecutionGeneration(job.id)).toBe(generation + 1);
  await until(() => f.repo.getJob(job.id)?.status === "succeeded");
  expect(f.repo.getJobExecution(job.id)?.run_id).not.toBe(original.run_id);
  expect(f.repo.getJsonState<boolean>(`finished:${job.id}:${generation}`)).not.toBe(true);
  expect(f.repo.getJsonState<boolean>(`finished:${job.id}:${generation + 1}`)).toBe(true);
  expect(f.repo.listJobCommands()).toEqual([]);
  const storage = new SQLiteStorage(f.repo.executionDatabasePath());
  await storage.initialize();
  try {
    expect((await storage.getRun(original.run_id))?.status).toBe("cancelled");
    const current = await storage.getRun(f.repo.getJobExecution(job.id)!.run_id);
    expect(current?.input).toEqual({ jobId: job.id, generation: generation + 1 });
    expect(current?.status).toBe("completed");
  } finally { await storage.close(); }
}));

test("app DB writes stop waiting at the configured 2000ms instead of an unbounded busy loop", () => fixture(async f => {
  expect((f.db.query("PRAGMA busy_timeout").get() as { timeout: number }).timeout).toBe(2000);
  const job = f.repo.createJob({ type: "metadata_hydration", payload: { op: "lock", ms: 3000 } });
  await until(() => f.repo.getJsonState(`entered:${job.id}`) === true);
  await Bun.sleep(30);
  const start = performance.now();
  expect(() => f.db.query("INSERT INTO app_state(key,value_json,updated_at) VALUES('contended','true','fixture')").run())
    .toThrow("database is locked");
  const elapsed = performance.now() - start;
  expect(elapsed).toBeGreaterThanOrEqual(1800);
  expect(elapsed).toBeLessThan(2800);
  await until(() => f.repo.getJob(job.id)?.status === "succeeded");
}), 10_000);

test("startup errors/timeouts release ownership only after the failed thread exits", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-failure-"));
  const databasePath = path.join(root, "app.sqlite");
  const missing = new BackgroundWorker({ databasePath, entrypoint: new URL("../fixtures/missing-worker.ts", import.meta.url),
    startupTimeoutMs: 1000, shutdownTimeoutMs: 100, terminateTimeoutMs: 1000 });
  const stalled = new BackgroundWorker({ databasePath, entrypoint: new URL("../fixtures/background-worker-never-ready.ts", import.meta.url),
    startupTimeoutMs: 100, shutdownTimeoutMs: 100, terminateTimeoutMs: 1000 });
  const healthy = new BackgroundWorker({ databasePath, entrypoint });
  try {
    await expect(missing.start()).rejects.toThrow();
    const start = performance.now();
    await expect(stalled.start()).rejects.toThrow("startup timed out");
    expect(performance.now() - start).toBeLessThan(2000);
    await healthy.start();
    expect(healthy.state).toBe("ready");
  } finally { await missing.stop(); await stalled.stop(); await healthy.stop(); await rm(root, { recursive: true, force: true }); }
});

test("a non-cooperative worker is terminated within a bound before its replacement starts", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-forced-"));
  const databasePath = path.join(root, "app.sqlite");
  const stuck = new BackgroundWorker({ databasePath, entrypoint: new URL("../fixtures/background-worker-ignore-stop.ts", import.meta.url),
    shutdownTimeoutMs: 100, terminateTimeoutMs: 1000 });
  const healthy = new BackgroundWorker({ databasePath, entrypoint });
  try {
    await stuck.start();
    const start = performance.now();
    expect((await stuck.stop()).forced).toBe(true);
    expect(performance.now() - start).toBeLessThan(2000);
    expect(stuck.state).toBe("stopped");
    await healthy.start();
    expect(healthy.state).toBe("ready");
  } finally { await stuck.stop(); await healthy.stop(); await rm(root, { recursive: true, force: true }); }
});

test("a runtime failure is reported once even before a non-cooperative thread exits", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-notify-"));
  let failures = 0;
  const worker = new BackgroundWorker({ databasePath: path.join(root, "app.sqlite"),
    entrypoint: new URL("../fixtures/background-worker-failed.ts", import.meta.url),
    shutdownTimeoutMs: 100, terminateTimeoutMs: 1000, onFailure: () => { failures++; } });
  try {
    await worker.start();
    await until(() => failures === 1);
    expect(worker.state).toBe("failed");
    expect((await worker.stop()).forced).toBe(true);
    expect(failures).toBe(1);
  } finally { await worker.stop(); await rm(root, { recursive: true, force: true }); }
});

async function boundedExit(child: ReturnType<typeof Bun.spawn>, milliseconds = 5000) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([child.exited, new Promise<number>(resolve => { timer = setTimeout(() => resolve(-1), milliseconds); })]); }
  finally { if (timer) clearTimeout(timer); }
}

test("production server exits unhealthy on background thread loss without spawning another engine", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-server-fail-"));
  const source = `process.on("worker", worker => {
    console.log("fixture-worker-created");
    setTimeout(() => worker.terminate(), 1000);
  });
  const { startServer } = await import(${JSON.stringify(path.resolve(import.meta.dir, "../../server.ts"))});
  await startServer();`;
  const child = Bun.spawn([process.execPath, "--eval", source], {
    cwd: path.resolve(import.meta.dir, "../.."), env: { ...process.env, CONFIG_DIR: root, PORT: "0", NODE_ENV: "test" },
    stdout: "pipe", stderr: "pipe",
  });
  try {
    expect(await boundedExit(child)).toBe(1);
    const stdout = await new Response(child.stdout).text();
    expect(stdout).toContain("Podible backend listening");
    expect(stdout.match(/fixture-worker-created/g)).toHaveLength(1);
    expect(await new Response(child.stderr).text()).toContain("execution worker failed");
  } finally { child.kill("SIGKILL"); await child.exited; await rm(root, { recursive: true, force: true }); }
}, 10_000);

test("HTTP bind failure cleans up the already-ready worker instead of leaving an orphan engine", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-background-server-port-"));
  const occupied = Bun.serve({ port: 0, fetch: () => new Response("fixture") });
  const child = Bun.spawn([process.execPath, "run", "server.ts"], {
    cwd: path.resolve(import.meta.dir, "../.."), env: { ...process.env, CONFIG_DIR: root, PORT: String(occupied.port), NODE_ENV: "test" },
    stdout: "pipe", stderr: "pipe",
  });
  try {
    const exitCode = await boundedExit(child);
    if (exitCode === -1) { child.kill("SIGKILL"); await child.exited; }
    const stdout = await new Response(child.stdout).text();
    const stderr = await new Response(child.stderr).text();
    expect({ exitCode, stdout, stderr }).toMatchObject({ exitCode: 1 });
    expect(stdout).not.toContain("Podible backend listening");
  } finally { child.kill("SIGKILL"); await child.exited; await occupied.stop(true); await rm(root, { recursive: true, force: true }); }
}, 10_000);
