import type { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { admitTorrentUse, assertTorrentDatabaseWritable, finishTorrentUse, type TorrentRecordRef } from "./torrent-retirement-admission";

function store(db: Database, beforeWrite: () => void) {
  return {
    transaction<T>(action: () => T): T { return db.transaction(() => { beforeWrite(); return action(); })(); },
    getJsonState<T>(key: string): T | null {
      const row = db.query("SELECT value_json FROM app_state WHERE key=?").get(key) as { value_json: string } | null;
      return row ? JSON.parse(row.value_json) as T : null;
    },
    setJsonState(key: string, value: unknown) {
      beforeWrite();
      db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
        .run(key, JSON.stringify(value), new Date().toISOString());
    },
  };
}
const privateRef = (ref: TorrentRecordRef) => /^transmission:[a-f0-9-]{36}$/.test(ref.clientId);

/** Repository guard; called inside the same transaction as publishing a new binding. */
export function assertTorrentUseWritable(db: Database, ref: TorrentRecordRef) {
  if (privateRef(ref)) assertTorrentDatabaseWritable(db, ref);
}

/** Hold durable admission across async daemon I/O and binding acknowledgement. Unknown outcomes retain the token. */
export async function withTorrentUse<T>(repo: Parameters<typeof admitTorrentUse>[0], ref: TorrentRecordRef,
  action: () => Promise<T>): Promise<T> {
  if (!privateRef(ref)) return action();
  // Logical retries can overlap: each invocation must retain independent protection.
  const token = admitTorrentUse(repo, ref, randomUUID());
  const result = await action();
  finishTorrentUse(repo, ref, token);
  return result;
}

const registrations = new Map<string, Promise<unknown>>();
/** Serialize prepare/add/bind, not the download itself. Retirement admission alone is not a mutex. */
export async function withTorrentRegistration<T>(repo: Parameters<typeof admitTorrentUse>[0], ref: TorrentRecordRef,
  action: () => Promise<T>): Promise<T> {
  const key = JSON.stringify([ref.clientId, ref.infoHash]);
  const next = (registrations.get(key) ?? Promise.resolve()).catch(() => {}).then(() => withTorrentUse(repo, ref, action));
  registrations.set(key, next);
  try { return await next; }
  finally { if (registrations.get(key) === next) registrations.delete(key); }
}

export function withDatabaseTorrentUse<T>(db: Database, beforeWrite: () => void, ref: TorrentRecordRef,
  action: () => Promise<T>) {
  return withTorrentUse(store(db, beforeWrite), ref, action);
}
