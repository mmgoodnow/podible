import type { Database } from "bun:sqlite";
import { z } from "zod";
import type { JobRow } from "../app-types";
import type { RecoveryKey } from "./recovery-types";
import { RetentionRepo } from "./retention-repo";

export const CONTENT_LOSS_SCHEMA_SQL = `
CREATE TABLE media_content_losses (
  file_id TEXT PRIMARY KEY NOT NULL REFERENCES managed_content_files(id),
  namespace TEXT NOT NULL CHECK(namespace IN ('movie-target','tv-target')),
  target_id INTEGER NOT NULL,
  operation_id TEXT NOT NULL,
  observed_at TEXT NOT NULL
);`;

/** Filesystem absence is established by RecoveryService; this repository fences domain selection/demand. */
export class ContentLossRepo {
  constructor(private db: Database, private beforeWrite: () => void,
    private getJob: (id: number) => Pick<JobRow, "status"> | null) {}

  check(key: RecoveryKey, expectedRevision: number, fileIds: string[]): string | null {
    if (!["movie-target", "tv-target"].includes(key.namespace) || !/^[1-9]\d*$/.test(key.id) || !Number.isSafeInteger(Number(key.id)))
      throw new TypeError("Invalid loss target");
    z.array(z.string().uuid()).min(1).max(20).parse(fileIds);
    if (new Set(fileIds).size !== fileIds.length) throw new TypeError("Duplicate lost file");
    const id = Number(key.id), movie = key.namespace === "movie-target", prefix = movie ? "movie" : "tv";
    const target = this.db.query(`SELECT * FROM ${prefix}_targets WHERE id=?`).get(id) as { revision: number } | null;
    if (target?.revision !== expectedRevision) return "Selection changed during loss recovery";
    const intent = this.db.query("SELECT desired_presence FROM retention_resources WHERE namespace=? AND resource_id=?")
      .get(key.namespace, key.id) as { desired_presence: string } | null;
    if (intent?.desired_presence !== "present") return "Loss recovery requires retained present intent";
    if (!this.db.query(`SELECT 1 FROM ${prefix}_requests WHERE target_id=? AND withdrawn_at IS NULL LIMIT 1`).get(id))
      return "Loss recovery requires an active request";
    if (this.db.query(`SELECT 1 FROM ${prefix}_publications WHERE target_id=? LIMIT 1`).get(id))
      return "A publication still owns this selection";
    const acquisitions = this.db.query(`SELECT job_id,finished_at FROM ${prefix}_acquisitions WHERE target_id=? ORDER BY id DESC LIMIT 101`)
      .all(id) as { job_id: number | null; finished_at: string | null }[];
    if (acquisitions.length > 100) return "Loss recovery acquisition history exceeds its bound";
    if (acquisitions.some(a => { const job = a.job_id === null ? null : this.getJob(a.job_id);
      return job ? ["queued", "running"].includes(job.status) : !a.finished_at; })) return "An acquisition still owns this target";
    for (const fileId of fileIds) {
      const selected = movie
        ? this.db.query(`SELECT 1 FROM movie_targets t JOIN movie_version_files f ON f.version_id=t.active_version_id
            WHERE t.id=? AND f.file_id=? LIMIT 1`).get(id, fileId)
        : this.db.query(`SELECT 1 FROM tv_active_episodes a JOIN tv_version_file_episodes f
            ON f.version_id=a.version_id AND f.episode_id=a.episode_id WHERE a.target_id=? AND f.file_id=? LIMIT 1`).get(id, fileId);
      if (!selected) return "Lost file is not part of this exact selected target";
      const otherMovie = this.db.query(`SELECT 1 FROM movie_targets t JOIN movie_version_files f ON f.version_id=t.active_version_id
        WHERE f.file_id=? AND (?=0 OR t.id<>?) LIMIT 1`).get(fileId, movie ? 1 : 0, id);
      const otherTv = this.db.query(`SELECT 1 FROM tv_active_episodes a JOIN tv_version_file_episodes f
        ON f.version_id=a.version_id AND f.episode_id=a.episode_id WHERE f.file_id=? AND (?=0 OR a.target_id<>?) LIMIT 1`)
        .get(fileId, movie ? 0 : 1, id);
      if (otherMovie || otherTv) return "Another selected target shares this lost file; joint invalidation is required";
    }
    return null;
  }

  invalidate(key: RecoveryKey, expectedRevision: number, fileIds: string[], operationId: string) {
    z.string().uuid().parse(operationId);
    return this.db.transaction(() => {
      this.beforeWrite();
      const conflict = this.check(key, expectedRevision, fileIds);
      if (conflict) throw new Error(conflict);
      const now = new Date().toISOString(), id = Number(key.id);
      for (const fileId of fileIds) this.db.query(`INSERT INTO media_content_losses(file_id,namespace,target_id,operation_id,observed_at)
        VALUES(?,?,?,?,?) ON CONFLICT(file_id) DO NOTHING`).run(fileId, key.namespace, id, operationId, now);
      if (key.namespace === "movie-target") {
        this.db.query("UPDATE movie_targets SET active_version_id=NULL,revision=revision+1 WHERE id=? AND revision=?").run(id, expectedRevision);
      } else {
        // One lost multi-episode file invalidates its entire coverage, never a guessed subset.
        for (const fileId of fileIds) this.db.query(`DELETE FROM tv_active_episodes WHERE target_id=? AND EXISTS(
          SELECT 1 FROM tv_version_file_episodes f WHERE f.file_id=? AND f.version_id=tv_active_episodes.version_id
            AND f.episode_id=tv_active_episodes.episode_id)`).run(id, fileId);
        this.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=? AND revision=?").run(id, expectedRevision);
      }
      const retention = new RetentionRepo(this.db, this.beforeWrite), intent = retention.get(key)!;
      const remaining = key.namespace === "tv-target" && this.db.query("SELECT 1 FROM tv_active_episodes WHERE target_id=? LIMIT 1").get(id);
      retention.observe(key, { expectedRevision: intent.revision, actualPresence: remaining ? "unknown" : "missing",
        reason: "Confirmed canonical loss; original demand retained and only affected selections invalidated" });
    })();
  }

  /** Retry successful source releases whose adopted copy was lost, not previously rejected releases. */
  fileIds(key: RecoveryKey): string[] {
    return (this.db.query("SELECT file_id FROM media_content_losses WHERE namespace=? AND target_id=? ORDER BY file_id LIMIT 501")
      .all(key.namespace, Number(key.id)) as { file_id: string }[]).map(row => row.file_id);
  }

  retryDownloads(key: RecoveryKey): { offer_key: string; native_id: string }[] {
    const movie = key.namespace === "movie-target", prefix = movie ? "movie" : "tv";
    return this.db.query(`SELECT DISTINCT t.offer_key,t.native_id FROM media_torrents t
      JOIN ${prefix}_acquisition_downloads d ON d.torrent_id=t.id JOIN ${prefix}_versions v ON v.acquisition_id=d.acquisition_id
      JOIN ${prefix}_version_files f ON f.version_id=v.id JOIN media_content_losses l ON l.file_id=f.file_id
      JOIN ${prefix}_acquisitions a ON a.id=v.acquisition_id WHERE a.target_id=? AND a.finished_at IS NOT NULL LIMIT 100`)
      .all(Number(key.id)) as { offer_key: string; native_id: string }[];
  }
}

export function assertVersionContentAvailable(db: Database, kind: "movie" | "tv", versionId: number) {
  if (db.query(`SELECT 1 FROM ${kind}_version_files f JOIN media_content_losses l ON l.file_id=f.file_id WHERE f.version_id=? LIMIT 1`).get(versionId))
    throw new Error("Version contains confirmed missing content; reacquire into new canonical identities");
}
