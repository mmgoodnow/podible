import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { processAcquireJob } from "../../src/worker/acquire";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { startMockRtorrent } from "../mocks/rtorrent";

async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await check())) {
    if (Date.now() >= deadline) throw new Error("Indexer durability acceptance timed out");
    await Bun.sleep(15);
  }
}

function torrent(name: string) {
  return Buffer.from(`d4:infod4:name${name.length}:${name}6:lengthi10e12:piece lengthi16384e6:pieces20:12345678901234567890ee`);
}

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-indexer-execution-"));
  const db = new Database(path.join(root, "books.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const logs: string[] = [];
  const context = { repo, getSettings: () => repo.getSettings(), onLog: (line: string) => { logs.push(line); }, activeLocks: new Set<string>() };
  const start = () => createJobExecutionRuntime(context, path.join(root, "execution.sqlite"), {
    retryDelayMs: 1,
    processJob: (ctx, job) => job.type === "acquire" ? processAcquireJob(ctx, job) : Promise.resolve("done" as const),
  });
  return { repo, context, logs, start, async close() { db.close(); await rm(root, { recursive: true, force: true }); } };
}

test("a queued ebook outage survives Reflow restart while completed audio work is preserved", async () => {
  const f = await fixture();
  const rtorrent = startMockRtorrent({ byHash: {} });
  const calls = { audio: 0, ebook: 0, torrents: 0 };
  let recovered = false;
  const server = Bun.serve({ port: 0, fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/torrent/")) {
      calls.torrents++;
      return new Response(torrent(url.pathname.endsWith("audio") ? "dune.m4b" : "dune.epub"));
    }
    const media = url.searchParams.get("cat") === "3030" ? "audio" : "ebook";
    calls[media]++;
    if (media === "ebook" && !recovered) return new Response("upstream credential details must not escape", { status: 429, headers: { "Retry-After": "60" } });
    const download = new URL(`/torrent/${media}`, request.url);
    return new Response(`<rss><channel><item><title>Dune Frank Herbert [${media === "audio" ? "M4B" : "EPUB"}]</title><enclosure url="${download}" length="100" /></item></channel></rss>`);
  } });
  let runtime: Awaited<ReturnType<typeof f.start>> | undefined;
  let clock: ReturnType<typeof spyOn<typeof Date, "now">> | undefined;
  try {
    f.repo.updateSettings(defaultSettings({
      torznab: [{ name: "MAM", baseUrl: server.url.origin, apiKey: "private-key", categories: { audio: "3030", ebook: "7020" } }],
      rtorrent: { transport: "http-xmlrpc", url: rtorrent.url, username: "", password: "" },
    }));
    runtime = await f.start();
    const book = f.repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const job = f.repo.createJob({ type: "acquire", bookId: book.id, maxAttempts: 5, payload: { bookId: book.id, media: ["audio", "ebook"], requireResult: true } });
    await runtime.reconcile();
    await until(async () => {
      const link = f.repo.getJobExecution(job.id);
      return Boolean(link && (await runtime!.engine.getRunStatus(link.run_id))?.run.status === "sleeping");
    });
    const runId = f.repo.getJobExecution(job.id)!.run_id;
    const before = await runtime.engine.getRunStatus(runId);
    expect(Array.isArray(before!.steps.find((step) => step.name === "audio:search")!.output)).toBe(true);
    expect(before!.steps.find((step) => step.name === "ebook:search")!.output).toMatchObject({ kind: "indexer-search-deferred", results: [], failures: [{ status: 429 }] });
    expect(before!.steps.find((step) => step.name === "ebook:search")!.attempts).toBe(1);
    expect(before!.steps.find((step) => step.name === "ebook:search:wait:0")!.status).toBe("sleeping");
    expect(f.repo.listReleasesByBook(book.id)).toHaveLength(1);
    expect(rtorrent.loadCalls).toHaveLength(1);
    expect(f.context.activeLocks.size).toBe(0);
    expect(calls).toEqual({ audio: 1, ebook: 1, torrents: 1 });
    expect(JSON.stringify(before!.steps.filter((step) => step.name.startsWith("ebook:search")))).not.toContain("private-key");
    expect(f.logs.join("\n")).not.toContain("upstream credential details");

    await runtime.stop();
    runtime = await f.start();
    await runtime.reconcile();
    await Bun.sleep(650);
    expect((await runtime.engine.getRunStatus(runId))!.run.status).toBe("sleeping");
    expect(calls).toEqual({ audio: 1, ebook: 1, torrents: 1 });
    expect(f.repo.getJob(job.id)?.status).not.toBe("failed");

    recovered = true;
    const realNow = Date.now.bind(Date);
    clock = spyOn(Date, "now").mockImplementation(() => realNow() + 61_000);
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    const after = await runtime.engine.getRunStatus(runId);
    expect(after!.run.status).toBe("completed");
    expect(calls).toEqual({ audio: 1, ebook: 2, torrents: 2 });
    expect(rtorrent.loadCalls).toHaveLength(2);
    expect(new Set(rtorrent.loadCalls).size).toBe(2);
    expect(f.repo.listReleasesByBook(book.id)).toHaveLength(2);
    expect(f.repo.listManifestationsByBook(book.id)).toHaveLength(2);
    expect(f.repo.listJobsByType("download")).toHaveLength(2);
    expect(after!.steps.find((step) => step.name === "ebook:search:retry:1")!.attempts).toBe(1);
    expect(Array.isArray(after!.steps.find((step) => step.name === "ebook:search:retry:1")!.output)).toBe(true);
    expect(after!.steps.find((step) => step.name === "ebook:selection:retry:1")!.status).toBe("completed");
    expect(after!.steps.find((step) => step.name === "ebook:search:wait:0")!.status).toBe("completed");
  } finally {
    if (runtime) await runtime.stop();
    clock?.mockRestore();
    await server.stop(true);
    rtorrent.stop();
    await f.close();
  }
}, 15_000);

test("only-provider downtime queues both media with one upstream request, and remains cancellable", async () => {
  const f = await fixture();
  let calls = 0;
  const server = Bun.serve({ port: 0, fetch() { calls++; return new Response("private error", { status: 429 }); } });
  let runtime: Awaited<ReturnType<typeof f.start>> | undefined;
  try {
    f.repo.updateSettings(defaultSettings({ torznab: [{ name: "MAM", baseUrl: server.url.origin, apiKey: "secret" }] }));
    runtime = await f.start();
    const book = f.repo.createBook({ title: "Unloved", author: "Peyton Corinne" });
    const job = f.repo.createJob({ type: "acquire", bookId: book.id, payload: { bookId: book.id, requireResult: true } });
    await runtime.reconcile();
    await until(async () => {
      const link = f.repo.getJobExecution(job.id);
      return Boolean(link && (await runtime!.engine.getRunStatus(link.run_id))?.run.status === "sleeping");
    });
    const runId = f.repo.getJobExecution(job.id)!.run_id;
    const status = await runtime.engine.getRunStatus(runId);
    expect(calls).toBe(1);
    for (const media of ["audio", "ebook"]) {
      expect(status!.steps.find((step) => step.name === `${media}:search`)!.attempts).toBe(1);
      expect(status!.steps.find((step) => step.name === `${media}:search:wait:0`)!.status).toBe("sleeping");
      expect(status!.steps.some((step) => step.name === `${media}:selection`)).toBe(false);
    }
    expect(f.context.activeLocks.size).toBe(0);
    f.repo.markJobCancelled(job.id, "User cancelled queued indexer search");
    await runtime.reconcile();
    await until(async () => (await runtime!.engine.getRunStatus(runId))!.run.status === "cancelled");
    expect(f.repo.getJob(job.id)?.status).toBe("cancelled");
    expect(calls).toBe(1);
    expect(f.repo.listReleasesByBook(book.id)).toHaveLength(0);
  } finally {
    if (runtime) await runtime.stop();
    await server.stop(true);
    await f.close();
  }
}, 10_000);

test("configuration failures avoid generic retries and explicit retry probes again after credential repair", async () => {
  const f = await fixture();
  let calls = 0;
  let repaired = false;
  const server = Bun.serve({ port: 0, fetch() { calls++; return repaired ? new Response("<rss><channel></channel></rss>") : new Response("mam_id=private", { status: 403 }); } });
  let runtime: Awaited<ReturnType<typeof f.start>> | undefined;
  let clock: ReturnType<typeof spyOn<typeof Date, "now">> | undefined;
  try {
    f.repo.updateSettings(defaultSettings({ torznab: [{ name: "MAM", baseUrl: server.url.origin, apiKey: "private-key" }] }));
    runtime = await f.start();
    const book = f.repo.createBook({ title: "Unloved", author: "Peyton Corinne" });
    const job = f.repo.createJob({ type: "acquire", bookId: book.id, maxAttempts: 5, payload: { bookId: book.id, media: ["audio"] } });
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "failed");
    const runId = f.repo.getJobExecution(job.id)!.run_id;
    const status = await runtime.engine.getRunStatus(runId);
    expect(calls).toBe(1);
    expect(status!.steps.find((step) => step.name === "audio:search")!.attempts).toBe(1);
    expect(status!.steps.some((step) => step.name.includes(":wait:"))).toBe(false);
    expect(f.repo.getJob(job.id)?.error).toContain("HTTP 403");
    expect(f.repo.getJob(job.id)?.error).toContain("credentials");
    expect(f.repo.getJob(job.id)?.error).not.toContain("no usable release");
    expect(f.logs.join("\n")).not.toContain("private-key");
    expect(f.logs.join("\n")).not.toContain("mam_id=private");

    f.repo.retryJob(job.id);
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "failed");
    expect(calls).toBe(1); // Cached configuration failure prevents an immediate retry storm.
    expect((await runtime.engine.getRunStatus(runId))!.steps.find((step) => step.name === "audio:search:retry:1")!.attempts).toBe(1);

    repaired = true;
    const realNow = Date.now.bind(Date);
    clock = spyOn(Date, "now").mockImplementation(() => realNow() + 61_000);
    f.repo.retryJob(job.id);
    await runtime.reconcile();
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    expect(f.repo.getJobExecution(job.id)!.run_id).toBe(runId);
    expect(calls).toBe(2);
    expect((await runtime.engine.getRunStatus(runId))!.steps.find((step) => step.name === "audio:search:retry:2")!.output).toEqual([]);
  } finally {
    if (runtime) await runtime.stop();
    clock?.mockRestore();
    await server.stop(true);
    await f.close();
  }
}, 10_000);
