import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { renderToStaticMarkup } from "react-dom/server";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { TransferMetricsRepo } from "../../src/media/transfer-metrics";
import type { TransferMetricsStatus } from "../../src/media/transfer-metrics-types";
import { crossSeedCommandSchema, crossSeedStateKey, type CrossSeedOutcome } from "../../src/media/cross-seed-state";
import { CrossSeedMetrics } from "../../web/src/transfer-metrics";

function fixture() {
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), jobs = new Map<number, { status: string }>();
  const metrics = new TransferMetricsRepo(db, () => {}, id => jobs.get(id) ?? null);
  let nextJob = 1;
  const add = (status: string | null, outcome?: CrossSeedOutcome["kind"], bytes = 0) => {
    const operationId = randomUUID(), jobId = nextJob++;
    if (status) jobs.set(jobId, { status });
    const command = crossSeedCommandSchema.parse({ operationId, scope: { media: "tv", targetId: 1 }, releaseId: 1 });
    repo.setJsonState(crossSeedStateKey(operationId), { command, jobId, commandHash: "fixture", createdAt: new Date().toISOString(),
      ...(outcome ? { outcome: { kind: outcome, at: new Date().toISOString(), reason: "Fixture", reusedBytes: bytes } } : {}) });
    return jobId;
  };
  return { db, metrics, add, jobs };
}

test("28 skipped candidates and five pending operations are not reported as 33 failures", () => {
  const f = fixture();
  try {
    for (let i = 0; i < 28; i++) f.add("succeeded", "skipped");
    for (let i = 0; i < 5; i++) f.add(i === 0 ? "running" : "queued");
    expect(f.metrics.status().reuse).toMatchObject({ attempts: 33, successful: 0, pending: 5, skipped: 28,
      failed: 0, stalled: 0, cancelled: 0, unsupported: 0, verifiedReusedBytes: 0 });
    const html = renderToStaticMarkup(<CrossSeedMetrics reuse={f.metrics.status().reuse} />);
    expect(html).toContain("<dt>Seeds created</dt><dd>0</dd>");
    expect(html).toContain("<dt>Pending</dt><dd>5</dd>");
    expect(html).toContain("<dt>Skipped</dt><dd>28</dd>");
    for (const absent of ["of 33", "Success rate", "Combined seed time", "<dt>Failed</dt>"]) expect(html).not.toContain(absent);
  } finally { f.db.close(); }
});

test("only validated seeding outcomes count as success; failures, stalls, cancellations and unsupported remain distinct", () => {
  const f = fixture();
  try {
    f.add("failed", "seeding", 20); // A persisted successful outcome survives later execution failure.
    f.add("succeeded", "skipped", 999);
    f.add("succeeded", "stalled", 999);
    f.add("succeeded", "unsupported", 999);
    f.add("failed"); f.add("cancelled");
    const pending = f.add("succeeded"); // Execution success alone proves no seeding.
    f.add(null);
    const reuse = f.metrics.status().reuse;
    expect(reuse).toMatchObject({ attempts: 8, successful: 1, pending: 2, skipped: 1, failed: 1,
      stalled: 1, cancelled: 1, unsupported: 1, verifiedReusedBytes: 20, matchRate: 1 / 6 });
    expect(reuse.successful + reuse.pending + reuse.skipped + reuse.failed + reuse.stalled + reuse.cancelled + reuse.unsupported).toBe(reuse.attempts);
    const html = renderToStaticMarkup(<CrossSeedMetrics reuse={reuse} />);
    for (const label of ["Failed", "Stalled", "Unsupported", "Cancelled"]) expect(html).toContain(`<dt>${label}</dt><dd>1</dd>`);
    f.jobs.set(pending, { status: "failed" });
    expect(f.metrics.status().reuse).toMatchObject({ failed: 2, pending: 1, successful: 1 });
  } finally { f.db.close(); }
});

test("the cross-seeding panel cannot attribute general torrent seed time to cross-seeds", () => {
  const reuse: TransferMetricsStatus["reuse"] = { attempts: 0, successful: 0, pending: 0, skipped: 0, failed: 0,
    stalled: 0, cancelled: 0, unsupported: 0, matchRate: null, verifiedReusedBytes: 0,
    sidecarDownloadBytes: null, contentRepairBytes: null, tvMultiFileReusedBytes: 0 };
  expect(renderToStaticMarkup(<CrossSeedMetrics reuse={reuse} />)).not.toContain("seed time");
});
