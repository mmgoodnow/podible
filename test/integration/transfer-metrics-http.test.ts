import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { transferMetricsPrometheus } from "../../src/media/transfer-metrics-prometheus";
import { transferMetricsStatusSchema } from "../../src/media/transfer-metrics-types";

test("metrics use admin authentication, expose no tracker editor, and reads have no daemon side effects", async () => {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db); repo.ensureSettings();
  try {
    const admin = repo.upsertUser({ provider: "local", providerUserId: "metrics-admin", username: "Administrator", isAdmin: true });
    const reader = repo.upsertUser({ provider: "local", providerUserId: "metrics-reader", username: "Reader", isAdmin: false });
    repo.createSession(admin.id, hashSessionToken("metrics-admin-token"), null, "app");
    repo.createSession(reader.id, hashSessionToken("metrics-reader-token"), null, "app");
    const fetch = createPodibleFetchHandler(repo, Date.now());
    const request = (pathname: string, token?: string, body?: unknown) => fetch(new Request(`http://fixture.invalid${pathname}`, {
      method: body ? "POST" : "GET", headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(body ? { "Content-Type": "application/json" } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    }));
    expect((await request("/metrics")).status).toBe(401);
    expect((await request("/metrics", "metrics-reader-token")).status).toBe(403);
    const scrape = await request("/metrics", "metrics-admin-token");
    expect(scrape.status).toBe(200); expect(scrape.headers.get("cache-control")).toBe("no-store");
    expect(scrape.headers.get("content-type")).toContain("text/plain");
    const text = await scrape.text(); expect(text).toContain("podible_transfer_metrics_fresh 0\n");
    expect(text).toContain("# TYPE podible_canonical_finalization_seconds histogram\n");
    expect(text).toMatch(/^podible_canonical_finalization_seconds_count\{media="tv",phase="proof_persist"\} \d+$/m);
    expect(text).not.toMatch(/^podible_seed_bytes /m);
    for (const [token, code] of [[undefined, -32001], ["metrics-reader-token", -32003]] as const) {
      for (const method of ["transferMetrics.status", "transferMetrics.published"]) {
        const response = await request("/rpc", token, { jsonrpc: "2.0", id: 1, method, params: {} });
        expect((await response.json() as any).error.code).toBe(code);
      }
    }
    const call = async (method: string, params: unknown) => (await (await request("/rpc", "metrics-admin-token", { jsonrpc: "2.0", id: 1, method, params })).json()) as any;
    expect((await call("transferMetrics.status", {})).result.summary).toBeNull();
    repo.createBook({ title: "Wanted book", author: "Reader" });
    expect((await call("transferMetrics.published", {})).result).toEqual({
      books: 0, ebooks: 0, audiobooks: 0, movies: 0, shows: 0, seasons: 0, episodes: 0,
    });
    expect((await call("transferMetrics.mapTracker", { host: "tracker.example", name: "Example" })).error.code).toBe(-32601);
    expect((await request("/api/trpc/transferMetrics.mapTracker", "metrics-admin-token", { host: "tracker.example", name: "Example" })).status).toBe(404);
    expect((await call("transferMetrics.status", { hours: 1 })).error).toBeDefined();
    expect(db.query("SELECT COUNT(*) AS n FROM jobs").get()).toEqual({ n: 0 });
  } finally { db.close(); }
});

test("Prometheus counters preserve units, escape bounded tracker labels, omit unknown gauges and never export content identities", () => {
  const status = transferMetricsStatusSchema.parse({ trackingSince: null, observedAt: "2026-11-06T00:00:00Z", stale: false, unavailableReason: null,
    summary: { seedBytes: 1024, seedingTorrents: 2, uploadedBytes: 4096, downloadedBytes: 512, uploadBytesPerSecond: 20, downloadBytesPerSecond: 0,
      crossSeedUploadedBytes: 2000, seedSeconds: 100, contentUnits: 1, averageCopies: 2 },
    trackers: [{ host: "tracker.example", name: 'A "tracker"\\label', seedBytes: 1024, seedingTorrents: 2, uploadedBytes: null, downloadedBytes: null, seedSeconds: 100, ratio: null }],
    history: [], mappings: [], warnings: [], downloads: [{ id: 12345, title: "Private book title", state: "downloading", bytesDone: 0, sizeBytes: 10, downloadBytesPerSecond: 0, href: "/book/12345" }],
    reuse: { attempts: 2, successful: 1, pending: 1, skipped: 0, failed: 0, stalled: 0, cancelled: 0,
      unsupported: 0, matchRate: 1, verifiedReusedBytes: 100, sidecarDownloadBytes: 2, contentRepairBytes: 1, tvMultiFileReusedBytes: 0 } });
  const text = transferMetricsPrometheus(status);
  expect(text).toContain("# TYPE podible_uploaded_bytes_total counter\npodible_uploaded_bytes_total 4096");
  expect(text).toContain('podible_tracker_seed_bytes{tracker="A \\"tracker\\"\\\\label",host="tracker.example"} 1024');
  expect(text).not.toMatch(/^podible_tracker_uploaded_bytes_total\{/m);
  expect(text).not.toContain("Private book"); expect(text).not.toContain("12345");
  status.stale = true; status.summary!.seedBytes = null;
  const stale = transferMetricsPrometheus(status);
  expect(stale).not.toMatch(/^podible_tracker_seed_bytes\{/m);
  expect(stale).not.toMatch(/^podible_seed_bytes /m);
  expect(stale).toContain("podible_uploaded_bytes_total 4096");
});

test("scrape token grants only metrics access and can be rotated without restarting", async () => {
  const dir = await mkdtemp(join(tmpdir(), "podible-metrics-auth-"));
  const digestPath = join(dir, "metrics-token.sha256");
  const db = new Database(":memory:");
  try {
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const fetch = createPodibleFetchHandler(repo, Date.now(), null, { metricsTokenDigestPath: digestPath });
    const token = randomBytes(32).toString("hex");
    const request = (path: string, body?: object) => fetch(new Request(`http://fixture.invalid${path}`, {
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      ...(body ? { method: "POST", body: JSON.stringify(body) } : {}),
    }));
    expect((await request("/metrics")).status).toBe(401);
    await writeFile(digestPath, createHash("sha256").update(token).digest("hex") + "\n");
    expect((await request("/metrics")).status).toBe(200);
    const rpc = await request("/rpc", { jsonrpc: "2.0", id: 1, method: "transferMetrics.status", params: {} });
    expect(await rpc.json()).toMatchObject({ error: { code: -32001 } });
    expect((await request("/api/trpc/transferMetrics.status?input=%7B%7D")).status).toBe(401);
    await writeFile(digestPath, createHash("sha256").update(randomBytes(32)).digest("hex"));
    expect((await request("/metrics")).status).toBe(401);
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("scrapes use one saved transfer sample and distinguish stale gauges from observed counters", () => {
  const db = new Database(":memory:");
  try {
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const now = Date.parse("2026-11-08T10:00:00Z");
    expect(repo.transferMetrics.scrapeSnapshot(now)).toBeNull();
    const sample = repo.transferMetrics.status({}, now);
    sample.observedAt = new Date(now).toISOString();
    sample.stale = false;
    sample.summary = { seedBytes: 500, seedingTorrents: 1, uploadedBytes: 300, downloadedBytes: 100,
      uploadBytesPerSecond: 10, downloadBytesPerSecond: 0, crossSeedUploadedBytes: 30,
      seedSeconds: 400, contentUnits: 1, averageCopies: 1 };
    db.query("INSERT INTO transfer_metric_samples VALUES('raw',?,?,?)")
      .run(sample.observedAt.slice(0, 16), sample.observedAt, JSON.stringify(sample));
    const fresh = transferMetricsPrometheus(repo.transferMetrics.scrapeSnapshot(now + 1), repo.transferMetrics.publishedCounts());
    expect(fresh).toContain("podible_seed_bytes 500\n");
    expect(fresh).toContain("podible_transfer_metrics_fresh 1\n");
    expect(fresh).toContain('podible_published_library_items{kind="episodes"} 0\n');
    const stale = transferMetricsPrometheus(repo.transferMetrics.scrapeSnapshot(now + 180_001));
    expect(stale).toContain("podible_transfer_metrics_fresh 0\n");
    expect(stale).toContain("podible_uploaded_bytes_total 300\n");
    expect(stale).not.toMatch(/^podible_(seed_bytes|upload_bytes_per_second) /m);
  } finally {
    db.close();
  }
});
