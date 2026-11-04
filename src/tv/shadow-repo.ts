import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import type { JobRow } from "../app-types";
import { tvShadowInputSchema, tvShadowResultSchema, type TvShadowInput, type TvShadowResult, type TvShadowCase } from "./shadow-types";

export const TV_SHADOW_SCHEMA_SQL = `CREATE TABLE tv_shadow_cases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  input_hash TEXT NOT NULL UNIQUE, input_json TEXT NOT NULL,
  job_id INTEGER REFERENCES jobs(id), result_json TEXT, created_at TEXT NOT NULL
);
CREATE TRIGGER tv_shadow_input_immutable BEFORE UPDATE OF input_hash,input_json,created_at ON tv_shadow_cases
BEGIN SELECT RAISE(ABORT, 'TV shadow evidence is immutable'); END;`;
type Row = { id: number; input_json: string; job_id: number | null; result_json: string | null; created_at: string };

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b))
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonical(entry)}`).join(",")}}`;
  return JSON.stringify(value);
}

export class TvShadowRepo {
  constructor(private db: Database, private beforeWrite: () => void, private getJob: (id: number) => JobRow | null) {}
  transaction<T>(fn: () => T): T {
    return this.db.transaction(() => { this.beforeWrite(); return fn(); })();
  }
  capture(raw: TvShadowInput) {
    const input = tvShadowInputSchema.parse(raw);
    const { observedAt, sonarr, libraryPath, ...observation } = input.observation;
    const hash = createHash("sha256").update(canonical({ ...input, observation })).digest("hex");
    return this.transaction(() => {
      const prior = this.db.query("SELECT id FROM tv_shadow_cases WHERE input_hash=?").get(hash) as { id: number } | null;
      if (prior) return { caseId: prior.id, created: false };
      const row = this.db.query("INSERT INTO tv_shadow_cases(input_hash,input_json,created_at) VALUES(?,?,?) RETURNING id")
        .get(hash, JSON.stringify(input), new Date().toISOString()) as { id: number };
      return { caseId: row.id, created: true };
    });
  }
  attachJob(id: number, jobId: number) {
    this.beforeWrite();
    const job = this.getJob(jobId);
    if (job?.type !== "tv_shadow_evaluate" || JSON.parse(job.payload_json ?? "{}").caseId !== id)
      throw new Error("TV shadow job does not own this case");
    const changed = this.db.query("UPDATE tv_shadow_cases SET job_id=? WHERE id=? AND (job_id IS NULL OR job_id=?)")
      .run(jobId, id, jobId).changes;
    if (!changed) throw new Error("TV shadow case already has a different execution");
  }
  finish(id: number, jobId: number, raw: TvShadowResult) {
    const result = JSON.stringify(tvShadowResultSchema.parse(raw));
    this.beforeWrite();
    const changed = this.db.query("UPDATE tv_shadow_cases SET result_json=? WHERE id=? AND job_id=? AND result_json IS NULL")
      .run(result, id, jobId).changes;
    if (!changed) {
      const row = this.db.query("SELECT result_json FROM tv_shadow_cases WHERE id=? AND job_id=?").get(id, jobId) as { result_json: string } | null;
      if (row?.result_json !== result) throw new Error("TV shadow result is immutable or execution changed");
    }
  }
  get(id: number): TvShadowCase | null {
    const row = this.db.query("SELECT * FROM tv_shadow_cases WHERE id=?").get(id) as Row | null;
    if (!row) return null;
    const job = row.job_id ? this.getJob(row.job_id) : null;
    return {
      id: row.id, createdAt: row.created_at, jobId: row.job_id,
      status: job?.status ?? "unattached",
      error: job?.error ? "TV audit execution failed; inspect server diagnostics or retry the evaluation" : null,
      input: tvShadowInputSchema.parse(JSON.parse(row.input_json)),
      result: row.result_json ? tvShadowResultSchema.parse(JSON.parse(row.result_json)) : null,
    };
  }
  list(before?: number, limit = 25, includeHistory = false): TvShadowCase[] {
    return (this.db.query(`SELECT c.id FROM tv_shadow_cases c WHERE c.id < ? AND (? OR NOT EXISTS (
      SELECT 1 FROM tv_shadow_cases newer WHERE newer.id>c.id
      AND json_extract(newer.input_json,'$.observation.key')=json_extract(c.input_json,'$.observation.key')
    )) ORDER BY c.id DESC LIMIT ?`).all(before ?? Number.MAX_SAFE_INTEGER, includeHistory ? 1 : 0, limit) as { id: number }[])
      .map(({ id }) => this.get(id)!);
  }
}
