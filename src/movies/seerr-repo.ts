import type { Database } from "bun:sqlite";
import type { SeerrRequest } from "./seerr";
export const MOVIE_SEERR_SCHEMA_SQL = `
CREATE TABLE movie_request_feeds(instance TEXT PRIMARY KEY, baseline_id INTEGER NOT NULL, cursor TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE movie_request_origins(
  instance TEXT NOT NULL REFERENCES movie_request_feeds(instance), external_request_id INTEGER NOT NULL,
  request_id INTEGER NOT NULL REFERENCES movie_requests(id), source_json TEXT NOT NULL, created_at TEXT NOT NULL,
  PRIMARY KEY(instance,external_request_id)
);`;
export class MovieSeerrRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void,
  ) {}
  feed(instance: string) {
    return this.db
      .query(
        "SELECT baseline_id AS baselineId,cursor,created_at AS createdAt FROM movie_request_feeds WHERE instance=?",
      )
      .get(instance) as {
      baselineId: number;
      cursor: string;
      createdAt: string;
    } | null;
  }
  start(instance: string, baselineId: number, cursor: string) {
    this.beforeWrite();
    this.db
      .query("INSERT OR IGNORE INTO movie_request_feeds VALUES(?,?,?,?)")
      .run(instance, baselineId, cursor, new Date().toISOString());
    return this.feed(instance)!;
  }
  advance(instance: string, cursor: string) {
    this.beforeWrite();
    this.db
      .query(
        "UPDATE movie_request_feeds SET cursor=MAX(cursor,?) WHERE instance=?",
      )
      .run(cursor, instance);
  }
  origin(instance: string, id: number) {
    return this.db
      .query(
        "SELECT request_id AS requestId FROM movie_request_origins WHERE instance=? AND external_request_id=?",
      )
      .get(instance, id) as { requestId: number } | null;
  }
  attach(instance: string, source: SeerrRequest, requestId: number) {
    this.beforeWrite();
    this.db
      .query("INSERT INTO movie_request_origins VALUES(?,?,?,?,?)")
      .run(
        instance,
        source.id,
        requestId,
        JSON.stringify(source),
        new Date().toISOString(),
      );
  }
  list(limit = 50, instance?: string) {
    return (
      this.db
        .query(
          "SELECT instance,external_request_id AS externalRequestId,request_id AS requestId,source_json,created_at AS createdAt FROM movie_request_origins WHERE (? IS NULL OR instance=?) ORDER BY created_at DESC,external_request_id DESC LIMIT ?",
        )
        .all(instance ?? null, instance ?? null, limit) as {
        instance: string;
        externalRequestId: number;
        requestId: number;
        source_json: string;
        createdAt: string;
      }[]
    ).map((r) => ({
      instance: r.instance,
      externalRequestId: r.externalRequestId,
      requestId: r.requestId,
      source: JSON.parse(r.source_json) as SeerrRequest,
      createdAt: r.createdAt,
    }));
  }
}
