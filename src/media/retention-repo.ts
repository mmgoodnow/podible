import type { Database } from "bun:sqlite";
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
