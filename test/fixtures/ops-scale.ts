import { Database } from "bun:sqlite";

export function seedActivityHistory(db: Database, executionPath: string) {
  const now = Date.now();
  const first = db.query<{ id: number }, []>("SELECT COALESCE(MAX(id), 0) + 1 AS id FROM jobs").get()!.id;
  const jobs = 40_000;
  db.query(`WITH RECURSIVE history(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM history WHERE n+1<?)
    INSERT INTO jobs(id, type, created_at, max_attempts, payload_json)
    SELECT ?+n, 'indexer_feed_sync', ?, 3, ? FROM history`).run(jobs, first, new Date(now).toISOString(), JSON.stringify({ fixture: "x".repeat(512) }));
  using execution = new Database(executionPath);
  execution.query(`WITH RECURSIVE history(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM history WHERE n+1<?)
    INSERT INTO workflow_runs(id, workflow, input, status, created_at, updated_at)
    SELECT 'ops-history:'||(?+n), 'media-indexer_feed_sync-v1', '{}',
      CASE WHEN n%40=0 THEN 'failed' ELSE 'completed' END, ?, ? FROM history`).run(jobs, first, now, now);
  execution.query(`WITH RECURSIVE steps(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM steps WHERE n<8)
    INSERT INTO workflow_steps(id, run_id, name, status, attempts, created_at, updated_at)
    SELECT r.id||':'||n, r.id, 'fixture:'||n, 'completed', 1, ?, ?
    FROM workflow_runs r CROSS JOIN steps WHERE r.id LIKE 'ops-history:%'`).run(now, now);
  db.query("INSERT INTO job_executions(job_id,run_id) SELECT id,'ops-history:'||id FROM jobs WHERE id>=?").run(first);

  // Match the production history order of magnitude without copying its data.
  const sample = db.query<{ snapshot_json: string }, []>(
    "SELECT snapshot_json FROM transfer_metric_samples WHERE resolution='raw' ORDER BY observed_at DESC LIMIT 1").get();
  if (!sample) throw new Error("Activity scale fixture needs a transfer sample");
  const snapshot = JSON.parse(sample.snapshot_json);
  snapshot.trackers = Array.from({ length: 10 }, (_, i) => ({
    ...snapshot.trackers[i % snapshot.trackers.length], name: `Fixture tracker ${i}`, host: `fixture-${i}.tracker.example`,
  }));
  db.transaction(() => {
    const insert = db.query("INSERT OR REPLACE INTO transfer_metric_samples VALUES('raw',?,?,?)");
    for (let i = 0; i < 1000; i++) {
      const at = new Date(Math.floor(now / 60_000) * 60_000 - i * 60_000).toISOString();
      insert.run(at.slice(0, 16), at, JSON.stringify(snapshot));
    }
  })();
}
