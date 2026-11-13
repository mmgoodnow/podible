import { Database, constants } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import type { JobRow, JobStatus } from "../app-types";
import type { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { activityJobIds } from "../activity-reads";

export type JobLifecycle = Pick<JobRow, "status" | "error" | "attempt_count" | "next_run_at" | "updated_at">;
export type JobWait = { reason: "provider-search"; media: "audio" | "ebook" };
export type ReuseFailure = { stage: "metainfo"; httpStatus: number | null } | { stage: "inventory" };
export type JobJournalReader = {
  read(runIds: string[]): Map<string, JobLifecycle>;
  readStatus?(runId: string): JobStatus | null;
  readStatuses?(runIds: string[]): Map<string, JobStatus>;
  assertLinks?(domainFilename: string): void;
  waits?(runIds: string[]): Map<string, JobWait[]>;
  activityJobIds?(domainFilename: string): number[];
  activityReuseFailure?(runId: string): ReuseFailure | null;
};
export type JobCommand = {
  id: number;
  job_id: number;
  kind: "enqueue" | "cancel" | "retry";
  generation: number;
  run_id: string | null;
  payload_json: string | null;
  created_at: string;
};

const statusMap: Record<string, JobStatus> = {
  pending: "queued",
  running: "running",
  sleeping: "queued",
  waiting: "queued",
  completed: "succeeded",
  failed: "failed",
  cancelled: "cancelled"
};
function errorMessage(value: string | null): string | null {
  if (!value) return null;
  try {
    return (JSON.parse(value) as { message?: string }).message ?? value;
  } catch {
    return value;
  }
}

/** Read-only, synchronous batch adapter for the pinned Reflow SQLite schema. */
export function jobJournalReader(filename: string): JobJournalReader {
  return {
    activityJobIds: domainFilename => activityJobIds(domainFilename, filename),
    readStatuses(runIds) {
      if (!runIds.length) return new Map();
      using db = new Database(filename, { readonly: true });
      db.exec("PRAGMA busy_timeout=5000");
      const rows = db.query<{ id: string; status: string }, [string]>(`SELECT r.id,r.status
        FROM json_each(?) ids JOIN workflow_runs r ON r.id=ids.value`).all(JSON.stringify(runIds));
      return new Map(rows.map(row => {
        if (!statusMap[row.status]) throw new Error(`Unknown Reflow status: ${row.status}`);
        return [row.id, statusMap[row.status]!];
      }));
    },
    activityReuseFailure(runId) {
      using db = new Database(filename, { readonly: true });
      // Use only the step that actually stopped this run, not an older attempt.
      const step = db.query<{ name: string; output: string | null; error: string | null }, [string]>(`SELECT s.name, s.output, s.error
        FROM workflow_runs r JOIN workflow_steps s ON s.run_id=r.id AND s.name=json_extract(r.error,'$.stepName')
        WHERE r.id=? AND r.status='failed' AND s.status='failed'
      `).get(runId);
      if (!step) return null;
      if (/^reuse:inventory:(?:torrent|files):[a-f0-9]{64}$/.test(step.name)) return { stage: "inventory" };
      if (step.name !== "reuse:metainfo") return null;
      const output: { code?: unknown } | null = JSON.parse(step.output ?? "null");
      const code = typeof output?.code === "string" ? /^podible:metainfo-http:([1-5][0-9]{2})$/.exec(output.code) : null;
      if (code) return { stage: "metainfo", httpStatus: Number(code[1]) };
      // Older builds did not save a code. Recognize only the exact app-owned
      // message at this known step, never a substring of an upstream error.
      return { stage: "metainfo", httpStatus: output?.code === null && step.error === "Metainfo retrieval returned HTTP 429" ? 429 : null };
    },
    readStatus(runId) {
      const db = new Database(filename, { readonly: true });
      db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=5000");
      try {
        const row = db.query("SELECT status FROM workflow_runs WHERE id=?").get(runId) as { status: string } | null;
        if (!row) return null;
        if (!statusMap[row.status]) throw new Error(`Unknown Reflow status: ${row.status}`);
        return statusMap[row.status];
      } finally { db.close(); }
    },
    assertLinks(domainFilename) {
      const db = new Database(domainFilename, constants.SQLITE_OPEN_READONLY | constants.SQLITE_OPEN_URI);
      db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=5000");
      try {
        const uri = pathToFileURL(filename); uri.searchParams.set("mode", "ro");
        db.query("ATTACH DATABASE ? AS execution_integrity").run(uri.toString());
        // Only inspect a job row when its run is missing; healthy history needs
        // just the small covering indexes, not every job's payload pages.
        const missing = db.query(`SELECT e.job_id,e.run_id FROM job_executions e
          LEFT JOIN execution_integrity.workflow_runs r ON r.id=e.run_id
          WHERE r.id IS NULL AND EXISTS(SELECT 1 FROM jobs j WHERE j.id=e.job_id) LIMIT 1`)
          .get() as { job_id: number; run_id: string } | null;
        if (missing) throw new Error(`Missing execution ${missing.run_id} for job ${missing.job_id}`);
      } finally { db.close(); }
    },
    waits(runIds) {
      const out = new Map<string, JobWait[]>();
      if (!runIds.length) return out;
      if (runIds.length > 100) throw new Error("Job wait observation budget exceeded");
      const db = new Database(filename, { readonly: true });
      db.exec("PRAGMA busy_timeout = 5000");
      try {
        const rows = db.query(`SELECT run_id,name FROM workflow_steps WHERE run_id IN (${runIds.map(() => "?").join(",")})
          AND status='sleeping' AND kind='sleep' LIMIT 1000`).all(...runIds) as { run_id: string; name: string }[];
        for (const row of rows) {
          const match = /^(audio|ebook):search:wait:\d+$/.exec(row.name);
          if (!match) continue;
          const waits = out.get(row.run_id) ?? [];
          const media = match[1] as "audio" | "ebook";
          if (!waits.some(wait => wait.media === media)) waits.push({ reason: "provider-search", media });
          out.set(row.run_id, waits);
        }
        return out;
      } finally { db.close(); }
    },
    read(runIds) {
      const out = new Map<string, JobLifecycle>();
      if (!runIds.length) return out;
      const db = new Database(filename, { readonly: true });
      db.exec("PRAGMA busy_timeout = 5000");
      try {
        for (let offset = 0; offset < runIds.length; offset += 500) {
          const ids = runIds.slice(offset, offset + 500);
          const rows = db
            .query(
              `SELECT r.id, r.status, r.error, r.wake_at, r.updated_at,
          MAX(COALESCE((SELECT MAX(s.attempts) FROM workflow_steps s WHERE s.run_id = r.id), 0),
            COALESCE((SELECT json_extract(e.payload, '$.legacyJob.attempt_count') FROM workflow_events e
              WHERE e.id = 'podible:legacy:' || r.id), 0)) AS attempts
          FROM workflow_runs r WHERE r.id IN (${ids.map(() => "?").join(",")})`
            )
            .all(...ids) as Array<{
            id: string;
            status: string;
            error: string | null;
            wake_at: number | null;
            updated_at: number;
            attempts: number;
          }>;
          for (const row of rows) {
            if (!statusMap[row.status]) throw new Error(`Unknown Reflow status: ${row.status}`);
            out.set(row.id, {
              status: statusMap[row.status],
              error: errorMessage(row.error),
              attempt_count: row.attempts,
              next_run_at: row.wake_at === null ? null : new Date(row.wake_at).toISOString(),
              updated_at: new Date(row.updated_at).toISOString()
            });
          }
        }
        return out;
      } finally {
        db.close();
      }
    }
  };
}

/** Legacy import never rewrites a linked run or any existing step/history. */
export async function importLegacyJob(
  storage: SQLiteStorage,
  job: JobRow,
  generation: number,
  filename: string,
  unsupported = false
) {
  const terminal = ["succeeded", "failed", "cancelled"].includes(job.status);
  const updatedAt = Date.parse(job.updated_at);
  const error =
    unsupported && !terminal
      ? { stepName: "enqueue", message: `Unsupported job type: ${job.type}`, code: null }
      : job.error
        ? { stepName: "legacy-history", message: job.error, code: null }
        : null;
  const result = await storage.createRun({
    id: randomUUID(),
    workflow: `media-${job.type}-v1`,
    input: { jobId: job.id, generation },
    idempotencyKey: `job:${job.id}:${generation}`,
    status:
      job.status === "succeeded"
        ? "completed"
        : job.status === "failed" || (unsupported && !terminal)
          ? "failed"
          : job.status === "cancelled"
            ? "cancelled"
            : job.next_run_at
              ? "sleeping"
              : "pending",
    createdAt: Date.parse(job.created_at),
    updatedAt,
    startedAt: job.attempt_count ? Date.parse(job.created_at) : null,
    finishedAt: terminal || unsupported ? updatedAt : null,
    output: job.status === "succeeded" ? { jobId: job.id } : null,
    error,
    wakeAt: terminal || unsupported || !job.next_run_at ? null : Date.parse(job.next_run_at),
    waitingFor: null
  });
  archiveLegacyJob(filename, result.run.id, job);
  return result.run;
}

export function archiveLegacyJob(filename: string, runId: string, job: JobRow) {
  const db = new Database(filename);
  try {
    db.exec("PRAGMA busy_timeout = 5000");
    // Metadata is an event, never a synthetic step: function-workflow replay
    // orders completed steps and an unvisited archival step can block replay.
    db.query(
      "INSERT OR IGNORE INTO workflow_events(id,run_id,event_name,payload,created_at) VALUES (?, ?, 'podible:legacy-job', ?, ?)"
    ).run(`podible:legacy:${runId}`, runId, JSON.stringify({ legacyJob: job }), Date.parse(job.updated_at));
  } finally {
    db.close();
  }
}

/** Atomically apply commands and their receipts inside the journal, then ack
 * the app outbox separately. Replaying an unacked command is therefore safe.
 * SQL follows Reflow 0.11's cancel/requeue primitives; no execution polling or
 * alternative retry state lives here. */
export class JournalCommands {
  private db: Database;
  constructor(filename: string) {
    this.db = new Database(filename);
    this.db.exec(
      "PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS podible_command_receipts (command_key TEXT PRIMARY KEY, run_id TEXT NOT NULL)"
    );
  }
  apply(commandKey: string, command: JobCommand, runId: string): void {
    this.db
      .transaction(() => {
        if (this.db.query("SELECT 1 FROM podible_command_receipts WHERE command_key = ?").get(commandKey)) return;
        const now = Date.now();
        if (command.kind === "retry") {
          const result = this.db
            .query(
              `UPDATE workflow_runs SET status = 'pending', lease_id = NULL, updated_at = ?, finished_at = NULL,
          output = NULL, error = NULL, wake_at = NULL, waiting_for = NULL WHERE id = ? AND status IN ('failed','cancelled')`
            )
            .run(now, runId);
          if (result.changes)
            this.db.query("DELETE FROM workflow_steps WHERE run_id = ? AND status IN ('retrying','failed')").run(runId);
        } else if (command.kind === "cancel") {
          const reason = command.payload_json ? (JSON.parse(command.payload_json) as { reason?: string }).reason : null;
          this.db
            .query(
              `UPDATE workflow_runs SET status = 'cancelled', lease_id = NULL, wake_at = NULL, waiting_for = NULL,
          updated_at = ?, finished_at = ?, error = ? WHERE id = ? AND status NOT IN ('completed','failed')`
            )
            .run(now, now, reason ? JSON.stringify({ stepName: "cancel", message: reason, code: null }) : null, runId);
        }
        this.db.query("INSERT INTO podible_command_receipts VALUES (?, ?)").run(commandKey, runId);
      })
      .immediate();
  }
  close() {
    this.db.close();
  }
}
