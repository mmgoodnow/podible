import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { parseSettings } from "../settings";
import { RETENTION_REQUEST_GRACE_MS, retentionEvictionPlanSchema, type RetentionEviction, type RetentionEvictionPlan } from "./retention-policy-types";
import { dbSql, typedAll, typedGet } from "../db/typed";
import {
  validateResourceKey,
  type ActualPresence,
  type DesiredPresence,
  type ResourceKey,
  type RetentionAction,
  type RetentionEvent,
  type RetentionResource,
} from "./retention";

// Local compile-only table types keep this module usable before migration registration/codegen.
type ResourceRow = {
  namespace: string;
  resource_id: string;
  desired_presence: DesiredPresence;
  actual_presence: ActualPresence;
  revision: number;
  updated_at: string;
};
type EventRow = Omit<ResourceRow, "updated_at"> & {
  action: RetentionAction;
  reason: string;
  created_at: string;
};
const retentionSql = dbSql.withTables<{
  retention_resources: ResourceRow;
  retention_events: EventRow;
}>();

export interface TransactionalRepository {
  transaction<T>(fn: () => T): T;
}
export type RetentionChange = { expectedRevision: number; reason: string };

export class RetentionRevisionConflictError extends Error {
  constructor(
    readonly key: ResourceKey,
    readonly expectedRevision: number,
    readonly actualRevision: number,
  ) {
    super(`Retention revision conflict for ${key.namespace}:${key.id}: expected ${expectedRevision}, actual ${actualRevision}`);
    this.name = "RetentionRevisionConflictError";
  }
}

function resource(row: ResourceRow): RetentionResource {
  return {
    key: { namespace: row.namespace, id: row.resource_id },
    desiredPresence: row.desired_presence,
    actualPresence: row.actual_presence,
    revision: row.revision,
    updatedAt: row.updated_at,
  };
}

/** Intent/observations only. Caller owns the connection, migrations, and external effects. */
export class RetentionRepo implements TransactionalRepository {
  constructor(
    private readonly db: Database,
    private readonly beforeWrite: () => void = () => {},
  ) {}

  transaction<T>(fn: () => T): T {
    return this.db.transaction(() => {
      this.beforeWrite();
      const result = fn();
      if (result && typeof (result as { then?: unknown }).then === "function")
        throw new TypeError("Retention transactions must be synchronous");
      return result;
    })();
  }

  get(key: ResourceKey): RetentionResource | null {
    validateResourceKey(key);
    const row = typedGet(this.db, retentionSql.selectFrom("retention_resources").selectAll()
      .where("namespace", "=", key.namespace).where("resource_id", "=", key.id));
    return row ? resource(row) : null;
  }

  events(key: ResourceKey): RetentionEvent[] {
    validateResourceKey(key);
    return typedAll(this.db, retentionSql.selectFrom("retention_events").selectAll()
      .where("namespace", "=", key.namespace).where("resource_id", "=", key.id)
      .orderBy("revision")).map((row) => ({
        key: { namespace: row.namespace, id: row.resource_id },
        revision: row.revision,
        action: row.action,
        reason: row.reason,
        desiredPresence: row.desired_presence,
        actualPresence: row.actual_presence,
        createdAt: row.created_at,
      }));
  }

  request(key: ResourceKey, change: RetentionChange): RetentionResource {
    return this.change(key, change, "requested", "present");
  }

  evict(key: ResourceKey, change: RetentionChange): RetentionResource {
    return this.change(key, change, "evicted", "evicted");
  }

  observe(key: ResourceKey, change: RetentionChange & { actualPresence: ActualPresence }): RetentionResource {
    if (!["present", "missing", "unknown"].includes(change.actualPresence))
      throw new TypeError("Invalid retention actual presence");
    return this.change(key, change, "observed", undefined, change.actualPresence);
  }

  /** Withdrawal records history; it does not imply eviction, missing files, or loss of demand. */
  withdraw(key: ResourceKey, change: RetentionChange): RetentionResource {
    return this.change(key, change, "withdrawn");
  }

  eviction(id: string): RetentionEviction | null {
    const row = this.db.query("SELECT * FROM retention_evictions WHERE id=?").get(id) as {
      id: string; plan_json: string; job_id: number; finished_at: string | null; result_json: string | null;
    } | null;
    return row ? { id: row.id, jobId: row.job_id, plan: retentionEvictionPlanSchema.parse(JSON.parse(row.plan_json)),
      finishedAt: row.finished_at, result: row.result_json === null ? null : JSON.parse(row.result_json) } : null;
  }

  evictions(jobId: number): RetentionEviction[] {
    return (this.db.query("SELECT id FROM retention_evictions WHERE job_id=? ORDER BY created_at,id").all(jobId) as { id: string }[])
      .map((row) => this.eviction(row.id)!);
  }

  retiredFileIds(targetId: number): string[] {
    return [...new Set((this.db.query(`SELECT plan_json,result_json FROM retention_evictions
      WHERE namespace='movie-target' AND resource_id=? AND finished_at IS NOT NULL`).all(String(targetId)) as { plan_json: string; result_json: string | null }[])
      .flatMap((row) => {
        const plan = retentionEvictionPlanSchema.parse(JSON.parse(row.plan_json));
        const result = row.result_json === null ? null : JSON.parse(row.result_json);
        return result?.outcome === "superseded"
          ? plan.files.filter((file) => Array.isArray(result.verifiedAbsentObjectIds) && result.verifiedAbsentObjectIds.includes(file.id)).map((file) => file.id)
          : plan.files.map((file) => file.id);
      }))];
  }

  lastRequestAt(targetId: number): string | null {
    const row = this.db.query(`SELECT MAX(created_at) AS requested FROM (
      SELECT created_at FROM movie_requests WHERE target_id=?
      UNION ALL SELECT created_at FROM retention_events WHERE namespace='movie-target' AND resource_id=? AND action='requested'
    )`).get(targetId, String(targetId)) as { requested: string | null };
    return row.requested;
  }

  seedDownloads(targetId: number, fileIds: string[]) {
    const downloads = fileIds.flatMap((fileId) => this.db.query(`SELECT DISTINCT t.* FROM media_torrents t
      JOIN movie_acquisition_downloads d ON d.torrent_id=t.id JOIN movie_acquisitions a ON a.id=d.acquisition_id
      JOIN movie_versions v ON v.acquisition_id=a.id JOIN movie_version_files f ON f.version_id=v.id
      WHERE a.target_id=? AND f.file_id=?`).all(targetId, fileId) as {
        id: number; client_id: string; native_id: string; location_json: string; offer_key: string; observation_json: string | null;
      }[]);
    return [...new Map(downloads.map((download) => [download.id, download])).values()];
  }

  queueEviction(raw: RetentionEvictionPlan, enqueue: (id: string) => number): RetentionEviction {
    const plan = retentionEvictionPlanSchema.parse(raw);
    return this.transaction(() => {
      const prior = this.eviction(plan.id);
      if (prior) return prior;
      const key = { namespace: "movie-target", id: String(plan.targetId) };
      const resource = this.get(key);
      if (!resource || resource.revision !== plan.expectedRevision) throw new Error("Retention intent changed before eviction");
      this.guardSettings(plan);
      this.guardMovieReferences(plan);
      const target = this.db.query("SELECT active_version_id,revision FROM movie_targets WHERE id=?").get(plan.targetId) as { active_version_id: number | null; revision: number } | null;
      if (target?.revision !== plan.expectedTargetRevision || target.active_version_id !== plan.activeVersionId)
        throw new Error("Movie selection changed before eviction");
      const intent = this.evict(key, { expectedRevision: plan.expectedRevision, reason: plan.reason });
      if (intent.revision !== plan.intentRevision || plan.targetRevision !== target.revision + 1) throw new Error("Invalid eviction revision fence");
      this.db.query("UPDATE movie_targets SET active_version_id=NULL,revision=revision+1 WHERE id=? AND revision=?").run(plan.targetId, plan.expectedTargetRevision);
      const jobId = enqueue(plan.id);
      if (!Number.isSafeInteger(jobId) || jobId < 1) throw new Error("Invalid eviction job identity");
      this.db.query(`INSERT INTO retention_evictions(id,namespace,resource_id,plan_json,job_id,created_at)
        VALUES(?,'movie-target',?,?,?,?)`).run(plan.id, String(plan.targetId), JSON.stringify(plan), jobId, plan.createdAt);
      return this.eviction(plan.id)!;
    });
  }

  assertEviction(id: string): RetentionEviction {
    return this.transaction(() => {
      const eviction = this.eviction(id);
      if (!eviction || eviction.finishedAt) throw new Error("Unknown or completed eviction intent");
      const plan = eviction.plan, resource = this.get({ namespace: "movie-target", id: String(plan.targetId) });
      const target = this.db.query("SELECT active_version_id,revision FROM movie_targets WHERE id=?").get(plan.targetId) as { active_version_id: number | null; revision: number } | null;
      if (resource?.desiredPresence !== "evicted" || resource.revision !== plan.intentRevision ||
          target?.active_version_id !== null || target.revision !== plan.targetRevision) throw new Error("Eviction superseded by newer demand or selection");
      this.guardSettings(plan);
      this.guardMovieReferences(plan);
      return eviction;
    });
  }

  withEvictionFence<T>(id: string, effect: () => T): T {
    if (effect.constructor.name === "AsyncFunction") throw new TypeError("Eviction effects must be short synchronous filesystem operations");
    return this.db.transaction(() => {
      this.beforeWrite();
      this.assertEviction(id);
      const result = effect();
      if (result && typeof (result as { then?: unknown }).then === "function")
        throw new TypeError("Eviction fence callbacks must not return a thenable");
      return result;
    }).immediate();
  }

  finishEviction(id: string, result: unknown): RetentionEviction {
    return this.transaction(() => {
      const prior = this.eviction(id);
      if (prior?.finishedAt) return prior;
      const eviction = this.assertEviction(id);
      this.observe({ namespace: "movie-target", id: String(eviction.plan.targetId) }, {
        expectedRevision: eviction.plan.intentRevision, reason: "Canonical movie objects and owned presentation verified absent; seed store retained", actualPresence: "missing",
      });
      this.db.query("UPDATE retention_evictions SET finished_at=?,result_json=? WHERE id=? AND finished_at IS NULL")
        .run(new Date().toISOString(), JSON.stringify(result), id);
      return this.eviction(id)!;
    });
  }

  supersedeEviction(id: string, evidence: { verifiedPresentObjectIds: string[]; verifiedAbsentObjectIds: string[] }): RetentionEviction {
    return this.transaction(() => {
      const eviction = this.eviction(id);
      if (!eviction) throw new Error("Unknown eviction intent");
      if (eviction.finishedAt) return eviction;
      const { plan } = eviction, key = { namespace: "movie-target", id: String(plan.targetId) };
      const current = this.get(key);
      if (current?.desiredPresence !== "present" || current.revision <= plan.intentRevision) throw new Error("Eviction has no newer present intent");
      const all = [...evidence.verifiedPresentObjectIds, ...evidence.verifiedAbsentObjectIds];
      if (all.length !== plan.files.length || new Set(all).size !== all.length || plan.files.some((file) => !all.includes(file.id)))
        throw new Error("Supersession evidence must partition the exact planned object identities");
      const target = this.db.query("SELECT active_version_id,revision FROM movie_targets WHERE id=?").get(plan.targetId) as { active_version_id: number | null; revision: number } | null;
      if (!target) throw new Error("Unknown movie target");
      const required = (this.db.query("SELECT file_id FROM movie_version_files WHERE version_id=?").all(plan.activeVersionId) as { file_id: string }[]).map((row) => row.file_id);
      const available = required.length > 0 && required.every((fileId) => evidence.verifiedPresentObjectIds.includes(fileId));
      let selectionRestored = false;
      if (available && target.active_version_id === null && target.revision === plan.targetRevision &&
          !this.db.query("SELECT 1 FROM movie_publications WHERE target_id=? LIMIT 1").get(plan.targetId)) {
        this.db.query("UPDATE movie_targets SET active_version_id=?,revision=revision+1 WHERE id=? AND revision=?")
          .run(plan.activeVersionId, plan.targetId, plan.targetRevision);
        selectionRestored = true;
      }
      if (selectionRestored || target.active_version_id === plan.activeVersionId ||
          (target.active_version_id === null && !available)) {
        this.observe(key, { expectedRevision: current.revision, actualPresence: available ? "present" : "missing",
          reason: "Newer explicit request superseded eviction; canonical availability revalidated without changing desired presence" });
      }
      this.db.query("UPDATE retention_evictions SET finished_at=?,result_json=? WHERE id=? AND finished_at IS NULL")
        .run(new Date().toISOString(), JSON.stringify({ outcome: "superseded", ...evidence, selectionRestored,
          reason: "Preserved newer present intent; restored proven staging bytes where available, absent identities retained for recovery" }), id);
      return this.eviction(id)!;
    });
  }

  private guardSettings(plan: RetentionEvictionPlan) {
    const settings = this.db.query("SELECT value_json FROM settings WHERE id=1").get() as { value_json: string } | null;
    if (!settings || createHash("sha256").update(JSON.stringify(parseSettings(settings.value_json))).digest("hex") !== plan.settingsHash)
      throw new Error("Retention configuration changed");
  }

  private guardMovieReferences(plan: RetentionEvictionPlan) {
    const requestedAt = Date.parse(this.lastRequestAt(plan.targetId) ?? "");
    if (!Number.isFinite(requestedAt) || Date.now() - requestedAt < RETENTION_REQUEST_GRACE_MS) throw new Error("Fresh or unknown movie demand protects this resource");
    if (this.db.query("SELECT 1 FROM movie_acquisitions WHERE target_id=? AND finished_at IS NULL LIMIT 1").get(plan.targetId) ||
        this.db.query("SELECT 1 FROM movie_publications WHERE target_id=? LIMIT 1").get(plan.targetId))
      throw new Error("Movie acquisition or publication is still pending");
    const versions = (this.db.query(`SELECT v.id FROM movie_versions v JOIN movie_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=?`).all(plan.targetId) as { id: number }[]).map((row) => row.id);
    const references = this.db.query(`SELECT f.file_id,f.version_id FROM movie_version_files f
      JOIN movie_versions v ON v.id=f.version_id JOIN movie_acquisitions a ON a.id=v.acquisition_id WHERE a.target_id=?`).all(plan.targetId) as { file_id: string; version_id: number }[];
    const retired = new Set(this.retiredFileIds(plan.targetId));
    const actual = [...new Set(references.map((row) => row.file_id))].filter((id) => !retired.has(id) || plan.files.some((file) => file.id === id)).sort(), expected = plan.files.map((file) => file.id).sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error("Movie file inventory changed");
    const settings = parseSettings((this.db.query("SELECT value_json FROM settings WHERE id=1").get() as { value_json: string }).value_json);
    const tables = this.db.query("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[];
    const fileTables = tables.filter(({ name }) => name !== "movie_version_files" && /^[a-z_]+$/.test(name) &&
      (this.db.query(`PRAGMA table_info("${name}")`).all() as { name: string }[]).some((column) => column.name === "file_id"));
    for (const file of plan.files) {
      const row = this.db.query("SELECT relative_path,size_bytes FROM managed_content_files WHERE id=?").get(file.id) as { relative_path: string; size_bytes: number } | null;
      if (row?.relative_path !== file.relativePath || row.size_bytes !== file.sizeBytes) throw new Error("Canonical movie identity changed");
      const links = this.db.query("SELECT version_id FROM movie_version_files WHERE file_id=?").all(file.id) as { version_id: number }[];
      if (links.some((link) => !versions.includes(link.version_id))) throw new Error("Movie object is shared outside the eviction set");
      for (const { name } of fileTables) {
        if (this.db.query(`SELECT 1 FROM "${name}" WHERE file_id=? LIMIT 1`).get(file.id))
          throw new Error("Movie object is referenced by another media domain");
      }
      const canonical = path.join(settings.movies.managedRoot, "objects", file.relativePath);
      if (this.db.query("SELECT 1 FROM asset_files WHERE path=? OR identity_path=? OR source_path=? LIMIT 1").get(canonical, canonical, canonical))
        throw new Error("Movie object is referenced by book content");
    }
    for (const version of versions) if (this.db.query("SELECT 1 FROM movie_targets WHERE id<>? AND active_version_id=? LIMIT 1").get(plan.targetId, version) ||
      this.db.query("SELECT 1 FROM movie_publications WHERE version_id=? LIMIT 1").get(version)) throw new Error("Movie version remains selected outside the eviction set");
  }

  private change(
    key: ResourceKey,
    change: RetentionChange,
    action: RetentionAction,
    desired?: DesiredPresence,
    actual?: ActualPresence,
  ): RetentionResource {
    validateResourceKey(key);
    if (!Number.isSafeInteger(change.expectedRevision) || change.expectedRevision < 0 ||
        change.expectedRevision >= Number.MAX_SAFE_INTEGER)
      throw new TypeError("Invalid retention expected revision");
    if (typeof change.reason !== "string" || !change.reason.trim() || change.reason.length > 1000 ||
        /[\x00-\x1f\x7f]/.test(change.reason))
      throw new TypeError("Invalid retention event reason");
    return this.transaction(() => {
      const previous = this.get(key);
      if ((previous?.revision ?? 0) !== change.expectedRevision)
        throw new RetentionRevisionConflictError(key, change.expectedRevision, previous?.revision ?? 0);
      if (!previous && desired === undefined)
        throw new Error("Record request or eviction intent before observation/withdrawal");
      const next: ResourceRow = {
        namespace: key.namespace,
        resource_id: key.id,
        desired_presence: desired ?? previous!.desiredPresence,
        actual_presence: actual ?? previous?.actualPresence ?? "unknown",
        revision: change.expectedRevision + 1,
        updated_at: new Date().toISOString(),
      };
      const row = previous
        ? typedGet(this.db, retentionSql.updateTable("retention_resources").set(next)
          .where("namespace", "=", key.namespace).where("resource_id", "=", key.id)
          .where("revision", "=", change.expectedRevision).returningAll())
        : typedGet(this.db, retentionSql.insertInto("retention_resources").values(next)
          .onConflict((oc) => oc.columns(["namespace", "resource_id"]).doNothing()).returningAll());
      if (!row) throw new RetentionRevisionConflictError(key, change.expectedRevision, this.get(key)?.revision ?? 0);
      typedGet(this.db, retentionSql.insertInto("retention_events").values({
        namespace: next.namespace,
        resource_id: next.resource_id,
        revision: next.revision,
        action,
        reason: change.reason,
        desired_presence: next.desired_presence,
        actual_presence: next.actual_presence,
        created_at: next.updated_at,
      }).returning("revision"));
      return resource(row);
    });
  }
}
