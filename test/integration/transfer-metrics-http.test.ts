import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
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
    expect(text).not.toMatch(/^podible_seed_bytes /m);
    for (const [token, code] of [[undefined, -32001], ["metrics-reader-token", -32003]] as const) {
      for (const method of ["transferMetrics.status"]) {
        const response = await request("/rpc", token, { jsonrpc: "2.0", id: 1, method, params: {} });
        expect((await response.json() as any).error.code).toBe(code);
      }
    }
    const call = async (method: string, params: unknown) => (await (await request("/rpc", "metrics-admin-token", { jsonrpc: "2.0", id: 1, method, params })).json()) as any;
    expect((await call("transferMetrics.status", {})).result.summary).toBeNull();
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
