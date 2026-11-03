import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import type { JobRow } from "../app-types";
import {
  shadowInputSchema,
  shadowResultSchema,
  shadowReviewSchema,
  type ShadowInput,
  type ShadowResult,
} from "./shadow-types";

export const MOVIE_SHADOW_SCHEMA_SQL = `CREATE TABLE movie_shadow_cases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  input_hash TEXT NOT NULL UNIQUE, input_json TEXT NOT NULL,
  job_id INTEGER REFERENCES jobs(id), result_json TEXT, review_json TEXT,
  created_at TEXT NOT NULL
);`;
type Row = {
  id: number;
  input_json: string;
  job_id: number | null;
  result_json: string | null;
  review_json: string | null;
  created_at: string;
};
export class MovieShadowRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void,
    private getJob: (id: number) => JobRow | null,
  ) {}
  transaction<T>(fn: () => T): T {
    return this.db.transaction(() => {
      this.beforeWrite();
      return fn();
    })();
  }
  capture(raw: ShadowInput) {
    const input = shadowInputSchema.parse(raw);
    // Poll time/outcome are not decision inputs; changing them must not repay the agent.
    const { observedAt, radarr, ...evidence } = input.observation;
    const hash = createHash("sha256")
      .update(JSON.stringify({ ...input, observation: evidence }))
      .digest("hex");
    return this.transaction(() => {
      const existing = this.db
        .query("SELECT id FROM movie_shadow_cases WHERE input_hash=?")
        .get(hash) as { id: number } | null;
      if (existing) return { caseId: existing.id, created: false };
      const row = this.db
        .query(
          "INSERT INTO movie_shadow_cases(input_hash,input_json,created_at) VALUES(?,?,?) RETURNING id",
        )
        .get(hash, JSON.stringify(input), new Date().toISOString()) as {
        id: number;
      };
      return { caseId: row.id, created: true };
    });
  }
  attachJob(id: number, jobId: number) {
    this.beforeWrite();
    this.db
      .query("UPDATE movie_shadow_cases SET job_id=? WHERE id=?")
      .run(jobId, id);
  }
  finish(id: number, result: ShadowResult) {
    this.beforeWrite();
    this.db
      .query("UPDATE movie_shadow_cases SET result_json=? WHERE id=?")
      .run(JSON.stringify(shadowResultSchema.parse(result)), id);
  }
  review(id: number, review: Parameters<typeof shadowReviewSchema.parse>[0]) {
    this.beforeWrite();
    this.db
      .query("UPDATE movie_shadow_cases SET review_json=? WHERE id=?")
      .run(JSON.stringify(shadowReviewSchema.parse(review)), id);
  }
  get(id: number) {
    const row = this.db
      .query("SELECT * FROM movie_shadow_cases WHERE id=?")
      .get(id) as Row | null;
    if (!row) return null;
    const job = row.job_id ? this.getJob(row.job_id) : null;
    return {
      id: row.id,
      createdAt: row.created_at,
      jobId: row.job_id,
      status: job?.status ?? "queued",
      error: job?.error ?? null,
      input: shadowInputSchema.parse(JSON.parse(row.input_json)),
      result: row.result_json
        ? shadowResultSchema.parse(JSON.parse(row.result_json))
        : null,
      review: row.review_json
        ? shadowReviewSchema.parse(JSON.parse(row.review_json))
        : null,
    };
  }
  list(before?: number, limit = 50) {
    return (
      this.db
        .query(
          "SELECT id FROM movie_shadow_cases WHERE id < ? ORDER BY id DESC LIMIT ?",
        )
        .all(before ?? Number.MAX_SAFE_INTEGER, limit) as { id: number }[]
    ).map(({ id }) => this.get(id)!);
  }
}
