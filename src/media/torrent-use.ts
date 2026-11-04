import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
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
  operation: string, action: () => Promise<T>): Promise<T> {
  if (!privateRef(ref)) return action();
  const h = createHash("sha256").update(JSON.stringify([ref, operation])).digest("hex");
  const operationId = `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
  const token = admitTorrentUse(repo, ref, operationId);
  const result = await action();
  finishTorrentUse(repo, ref, token);
  return result;
}

export function withDatabaseTorrentUse<T>(db: Database, beforeWrite: () => void, ref: TorrentRecordRef,
  operation: string, action: () => Promise<T>) {
  return withTorrentUse(store(db, beforeWrite), ref, operation, action);
}
