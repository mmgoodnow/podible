import type { Database } from "bun:sqlite";
import type { JobRow } from "../app-types";
import { createHash } from "node:crypto";
import { parseSettings } from "../settings";
import { RetentionRepo } from "./retention-repo";
import { readTvRetentionInventory, tvRetentionDemandHash } from "./tv-retention-query";
import { tvRetentionPlanSchema, type TvRetentionPlan, type TvRetentionEviction } from "./tv-retention-types";

export const TV_RETENTION_SCHEMA_SQL = `
CREATE TABLE tv_retention_evictions (
  id TEXT PRIMARY KEY NOT NULL,
  target_id INTEGER NOT NULL REFERENCES tv_targets(id),
  command_hash TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id),
  created_at TEXT NOT NULL,
  finished_at TEXT,
  result_json TEXT
);`;

/** Parent-owned BooksRepo bridge supplies the connection, write ownership, and journal lifecycle reader. */
export class TvRetentionRepo {
  constructor(private readonly db: Database, private readonly beforeWrite: () => void,
    private readonly getJob: (id: number) => Pick<JobRow, "status"> | null) {}

  available(): boolean { return !!this.db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tv_retention_evictions'").get(); }
  inventory(scope: TvRetentionPlan["scope"]) { return readTvRetentionInventory(this.db, scope); }
  pressureCandidates(afterTargetId: number) {
    const rows = this.db.query(`SELECT t.id AS targetId,MIN(n.season) AS seasonNumber FROM tv_targets t
      JOIN tv_active_episodes a ON a.target_id=t.id JOIN tv_episode_numbering n
        ON n.entry_id=t.entry_id AND n.episode_id=a.episode_id AND n.scheme='tvdb:aired'
      WHERE t.id>? AND n.season IS NOT NULL GROUP BY t.id HAVING COUNT(DISTINCT n.season)=1
      ORDER BY t.id LIMIT 4`).all(afterTargetId) as { targetId: number; seasonNumber: number }[];
    return { scopes: rows.slice(0, 3).map(row => ({ ...row, scheme: "tvdb:aired" })), hasMore: rows.length > 3 };
  }
  selected(targetId: number) {
    return this.db.query("SELECT episode_id AS episodeId,version_id AS versionId FROM tv_active_episodes WHERE target_id=? ORDER BY episode_id LIMIT 51")
      .all(targetId) as TvRetentionPlan["selected"];
  }
  eviction(id: string): TvRetentionEviction | null {
    const row = this.db.query("SELECT * FROM tv_retention_evictions WHERE id=?").get(id) as { plan_json: string; job_id: number; finished_at: string | null; result_json: string | null } | null;
    return row ? { plan: tvRetentionPlanSchema.parse(JSON.parse(row.plan_json)), jobId: row.job_id, finishedAt: row.finished_at,
      result: row.result_json === null ? null : JSON.parse(row.result_json) } : null;
  }

  private check(plan: TvRetentionPlan, retired: boolean) {
    const inventory = this.inventory(plan.scope), retention = new RetentionRepo(this.db, this.beforeWrite);
    const intent = retention.get({ namespace: "tv-target", id: String(plan.scope.targetId) });
    if (inventory.overflow || inventory.target?.revision !== (retired ? plan.retiredRevision : plan.expectedTargetRevision) ||
        intent?.revision !== (retired ? plan.intentRevision : plan.expectedIntentRevision) || intent.desiredPresence !== (retired ? "evicted" : "present"))
      throw new Error("TV retention intent or selection changed");
    if (tvRetentionDemandHash(inventory) !== plan.demandHash || inventory.allRequestedEpisodeIds.some((id) => !plan.episodes.some((episode) => episode.id === id)))
      throw new Error("TV whole-season demand changed or another season was requested");
    const selected = this.selected(plan.scope.targetId);
    if (retired ? selected.length > 0 : JSON.stringify(selected) !== JSON.stringify(plan.selected)) throw new Error("TV season selection changed");
    if (inventory.publications.length || inventory.acquisitions.length > 20 || inventory.acquisitions.some((a) => {
      const job = a.jobId === null ? null : this.getJob(a.jobId);
      return job ? ["queued", "running"].includes(job.status) : !a.finishedAt;
    })) throw new Error("TV acquisition or publication owns this work");
    const settings = this.db.query("SELECT value_json FROM settings WHERE id=1").get() as { value_json: string } | null;
    // The same parsed settings projection is hashed by the service; raw JSON order is not configuration identity.
    if (!settings) throw new Error("TV storage configuration missing");
    const configuration = parseSettings(settings.value_json);
    if (createHash("sha256").update(JSON.stringify(configuration)).digest("hex") !== plan.settingsHash)
      throw new Error("TV retention configuration changed");
    const root = JSON.parse(settings.value_json).tv?.managedRoot;
    const ids = plan.files.map((file) => file.id), placeholders = ids.map(() => "?").join(",");
    const currentBindings = this.db.query(`SELECT DISTINCT t.id,t.client_id,t.native_id,t.location_json,f.file_id
      FROM media_torrents t JOIN tv_acquisition_downloads d ON d.torrent_id=t.id JOIN tv_versions v ON v.acquisition_id=d.acquisition_id
      JOIN tv_version_files f ON f.version_id=v.id WHERE f.file_id IN (${placeholders}) ORDER BY t.id,f.file_id LIMIT 401`).all(...ids) as
      { id: number; client_id: string; native_id: string; location_json: string; file_id: string }[];
    const expectedBindings = plan.downloads.flatMap((download) => download.fileIds.map((fileId) => ({ id: download.id,
      client_id: download.clientId, native_id: download.nativeId, location_json: download.locationJson, file_id: fileId })))
      .sort((a, b) => a.id - b.id || a.file_id.localeCompare(b.file_id));
    if (JSON.stringify(currentBindings) !== JSON.stringify(expectedBindings)) throw new Error("TV seed bindings changed");
    const tables = this.db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 257").all() as { name: string }[];
    if (tables.length > 256) throw new Error("TV reference schema exceeds budget");
    for (const { name } of tables) {
      if (["tv_version_files", "tv_version_file_episodes", "media_content_losses", "movie_version_files"].includes(name)) continue;
      const quoted = `"${name.replace(/"/g, '""')}"`;
      if ((this.db.query(`PRAGMA table_info(${quoted})`).all() as { name: string }[]).some((column) => column.name === "file_id") &&
          this.db.query(`SELECT 1 FROM ${quoted} WHERE file_id IN (${placeholders}) LIMIT 1`).get(...ids))
        throw new Error("TV object has another domain reference");
    }
    for (const file of plan.files) {
      const object = this.db.query("SELECT relative_path,size_bytes FROM managed_content_files WHERE id=?").get(file.id) as { relative_path: string; size_bytes: number } | null;
      if (object?.relative_path !== file.relativePath || object.size_bytes !== file.sizeBytes ||
          this.db.query("SELECT 1 FROM media_content_losses WHERE file_id=?").get(file.id)) throw new Error("TV retained canonical identity changed or was lost");
      if (this.db.query(`SELECT 1 FROM tv_version_files f JOIN tv_versions v ON v.id=f.version_id WHERE f.file_id=? AND v.target_id<>? LIMIT 1`)
        .get(file.id, plan.scope.targetId) || this.db.query("SELECT 1 FROM movie_version_files WHERE file_id=? LIMIT 1").get(file.id))
        throw new Error("TV object is shared across targets/domains");
      const coverage = this.db.query("SELECT DISTINCT episode_id FROM tv_version_file_episodes WHERE file_id=? LIMIT 51").all(file.id) as { episode_id: number }[];
      if (!coverage.length || coverage.length > 50 || coverage.some((row) => !plan.episodes.some((episode) => episode.id === row.episode_id)))
        throw new Error("TV file crosses the whole-season boundary");
      const canonical = `${root}/objects/${file.relativePath}`;
      if (this.db.query("SELECT 1 FROM asset_files WHERE path=? OR identity_path=? OR source_path=? LIMIT 1").get(canonical, canonical, canonical))
        throw new Error("TV object has a book reference");
    }
  }

  queue(raw: TvRetentionPlan, enqueue: (id: string) => number): TvRetentionEviction {
    const plan = tvRetentionPlanSchema.parse(raw);
    return this.db.transaction(() => {
      this.beforeWrite();
      const prior = this.eviction(plan.id);
      if (prior) { if (prior.plan.commandHash !== plan.commandHash) throw new Error("TV eviction operation conflict"); return prior; }
      this.check(plan, false);
      if (plan.retiredRevision !== plan.expectedTargetRevision + 1 || plan.intentRevision !== plan.expectedIntentRevision + 1)
        throw new Error("Invalid TV eviction revision plan");
      new RetentionRepo(this.db, this.beforeWrite).evict({ namespace: "tv-target", id: String(plan.scope.targetId) },
        { expectedRevision: plan.expectedIntentRevision, reason: plan.reason });
      this.db.query("DELETE FROM tv_active_episodes WHERE target_id=?").run(plan.scope.targetId);
      this.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=? AND revision=?").run(plan.scope.targetId, plan.expectedTargetRevision);
      const jobId = enqueue(plan.id);
      this.db.query("INSERT INTO tv_retention_evictions(id,target_id,command_hash,plan_json,job_id,created_at) VALUES(?,?,?,?,?,?)")
        .run(plan.id, plan.scope.targetId, plan.commandHash, JSON.stringify(plan), jobId, plan.createdAt);
      return this.eviction(plan.id)!;
    }).immediate();
  }

  assert(id: string) {
    const eviction = this.eviction(id);
    if (!eviction || eviction.finishedAt) throw new Error("Unknown or completed TV eviction");
    this.check(eviction.plan, true);
    return eviction;
  }
  withFence<T>(id: string, effect: () => T): T {
    if (effect.constructor.name === "AsyncFunction") throw new Error("TV eviction fence must be synchronous");
    return this.db.transaction(() => {
      this.beforeWrite(); this.assert(id);
      const result = effect();
      if (result && typeof (result as { then?: unknown }).then === "function") throw new Error("TV fence cannot return a promise");
      return result;
    }).immediate();
  }
  finish(id: string, result: unknown) {
    return this.db.transaction(() => {
      this.beforeWrite(); const prior = this.eviction(id);
      if (prior?.finishedAt) return prior;
      const eviction = this.assert(id), retention = new RetentionRepo(this.db, this.beforeWrite);
      retention.observe({ namespace: "tv-target", id: String(eviction.plan.scope.targetId) }, { expectedRevision: eviction.plan.intentRevision,
        actualPresence: "missing", reason: "Entire selected single-season target verified absent; owned seed store retained" });
      this.db.query("UPDATE tv_retention_evictions SET finished_at=?,result_json=? WHERE id=? AND finished_at IS NULL")
        .run(new Date().toISOString(), JSON.stringify(result), id);
      return this.eviction(id)!;
    })();
  }
  supersede(id: string, presentFileIds: string[]) {
    return this.db.transaction(() => {
      this.beforeWrite(); const eviction = this.eviction(id);
      if (!eviction || eviction.finishedAt) return eviction;
      const plan = eviction.plan, key = { namespace: "tv-target", id: String(plan.scope.targetId) };
      const retention = new RetentionRepo(this.db, this.beforeWrite), intent = retention.get(key);
      if (intent?.desiredPresence !== "present" || intent.revision <= plan.intentRevision) throw new Error("No superseding TV demand");
      const target = this.db.query("SELECT entry_id,revision FROM tv_targets WHERE id=?").get(plan.scope.targetId) as { entry_id: number; revision: number };
      const restoredEpisodeIds: number[] = [];
      if (target.revision === plan.retiredRevision && !this.selected(plan.scope.targetId).length) {
        const restorable = plan.selected.filter((selected) => {
          const files = this.db.query("SELECT file_id FROM tv_version_file_episodes WHERE version_id=? AND episode_id=? LIMIT 21")
            .all(selected.versionId, selected.episodeId) as { file_id: string }[];
          return files.length > 0 && files.length <= 20 && files.every((file) => presentFileIds.includes(file.file_id));
        });
        for (const selected of restorable) this.db.query("INSERT INTO tv_active_episodes(target_id,entry_id,episode_id,version_id) VALUES(?,?,?,?)")
          .run(plan.scope.targetId, target.entry_id, selected.episodeId, selected.versionId);
        if (restorable.length) this.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=? AND revision=?").run(plan.scope.targetId, plan.retiredRevision);
        restoredEpisodeIds.push(...restorable.map((episode) => episode.episodeId));
      }
      this.db.query("UPDATE tv_retention_evictions SET finished_at=?,result_json=? WHERE id=? AND finished_at IS NULL")
        .run(new Date().toISOString(), JSON.stringify({ outcome: "superseded", verifiedPresentFileIds: presentFileIds, restoredEpisodeIds }), id);
      return this.eviction(id)!;
    })();
  }
}
