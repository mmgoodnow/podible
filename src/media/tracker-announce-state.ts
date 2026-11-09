import type { Database } from "bun:sqlite";
import { infoHashFromTorrentBytes, rewriteTorrentAnnounces } from "../library/torrent";
import { rotateTrackerAnnounces, TrackerRotationError, usableAnnounces } from "./tracker-rotation";

const prefix = "tracker-announces:v1:";
type Observation = { digest: string; hash: string; rowId: number };
type Rotation = {
  active: string; previous: string[]; donors: number[]; candidate?: Observation; pending: boolean;
};
type Donor = { key: string; provider: string; torrent_bytes: Uint8Array; updated_at: string };

export class TrackerAnnounceState {
  constructor(private db: Database, private beforeWrite: () => void = () => {}) {}

  private get(provider: string): Rotation | null {
    const row = this.db.query<{ value_json: string }, [string]>("SELECT value_json FROM app_state WHERE key=?").get(prefix + provider);
    return row ? JSON.parse(row.value_json) : null;
  }

  private save(provider: string, state: Rotation) {
    this.beforeWrite();
    this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
      .run(prefix + provider, JSON.stringify(state), new Date().toISOString());
  }

  private observation(bytes: Uint8Array, rowId: number): Observation | null {
    try { return { digest: usableAnnounces(bytes).digest, hash: infoHashFromTorrentBytes(bytes), rowId }; }
    catch { return null; }
  }

  /** Must run before replacing the cache row, in the same transaction as the fetch write. */
  initialize(provider: string, bytes: Uint8Array) {
    if (this.get(provider)) return;
    const fresh = this.observation(bytes, 0);
    if (!fresh) return;
    const old = this.db.query<{ rowid: number; torrent_bytes: Uint8Array }, [string]>(
      "SELECT rowid,torrent_bytes FROM torrent_cache WHERE provider=? ORDER BY updated_at DESC,rowid DESC LIMIT 1",
    ).get(provider);
    const baseline = old && this.observation(old.torrent_bytes, old.rowid);
    this.save(provider, { active: baseline?.digest ?? fresh.digest, donors: baseline ? [baseline.rowId] : [],
      previous: [], pending: false });
  }

  recordFetch(provider: string, key: string, bytes: Uint8Array) {
    const rowId = this.db.query<{ rowid: number }, [string]>("SELECT rowid FROM torrent_cache WHERE key=?").get(key)?.rowid;
    if (rowId === undefined) return;
    const fresh = this.observation(bytes, rowId);
    const state = this.get(provider);
    if (!fresh || !state) return;
    if (fresh.digest === state.active) {
      this.save(provider, { ...state, candidate: undefined, donors: [rowId, ...state.donors.filter(id => id !== rowId)].slice(0, 2) });
    } else if (!state.previous.includes(fresh.digest)) {
      const candidate = state.candidate;
      if (candidate?.digest !== fresh.digest || candidate.hash === fresh.hash) {
        this.save(provider, { ...state, candidate: fresh });
      } else {
        this.save(provider, { active: fresh.digest, pending: true,
          previous: [state.active, ...state.previous].slice(0, 8), donors: [...new Set([rowId, candidate.rowId])].filter(Boolean) });
      }
    }
  }

  private donor(provider: string, state: Rotation) {
    const byId = this.db.query<Donor, [number, string]>(
      "SELECT key,provider,torrent_bytes,updated_at FROM torrent_cache WHERE rowid=? AND provider=?",
    );
    for (const id of state.donors) {
      const row = byId.get(id, provider);
      if (!row) continue;
      try {
        const parsed = usableAnnounces(row.torrent_bytes);
        if (parsed.digest === state.active) return { row, ...parsed };
      } catch { /* Try the other corroborating cache row. */ }
    }
    return null;
  }

  currentMetainfo(provider: string | null | undefined, bytes: Uint8Array) {
    const state = provider && this.get(provider);
    if (!state || state.pending || !state.previous.length) return bytes;
    try {
      if (!state.previous.includes(usableAnnounces(bytes).digest)) return bytes;
      const donor = this.donor(provider, state);
      return donor ? rewriteTorrentAnnounces(bytes, donor.announces) : bytes;
    } catch { return bytes; }
  }

  pendingProvider() {
    const row = this.db.query<{ key: string }, [string, string]>(
      "SELECT key FROM app_state WHERE key>=? AND key<? AND json_extract(value_json,'$.pending')=1 ORDER BY updated_at,key LIMIT 1",
    ).get(prefix, "tracker-announces:v1;");
    return row?.key.slice(prefix.length) ?? null;
  }

  defer(provider: string) {
    this.beforeWrite();
    this.db.query("UPDATE app_state SET updated_at=? WHERE key=? AND json_extract(value_json,'$.pending')=1")
      .run(new Date().toISOString(), prefix + provider);
  }

  async reconcile(provider: string, options: Pick<Parameters<typeof rotateTrackerAnnounces>[0], "rpc" | "clientId" | "roots">) {
    const state = this.get(provider);
    if (!state?.pending) return null;
    const donor = this.donor(provider, state);
    if (!donor) return null;
    const assertCurrent = () => {
      this.beforeWrite();
      if (this.get(provider)?.active !== state.active) throw new TrackerRotationError("Confirmed tracker generation changed");
    };
    const result = await rotateTrackerAnnounces({ ...options, db: this.db, provider, apply: true, maxWrites: 20,
      confirmed: { donor: donor.row, digest: state.active, previous: state.previous, assertCurrent } });
    assertCurrent();
    this.db.query(`UPDATE app_state SET value_json=json_set(value_json,'$.pending',json(?)),updated_at=?
      WHERE key=? AND json_extract(value_json,'$.active')=?`)
      .run(JSON.stringify(result.remaining), new Date().toISOString(), prefix + provider, state.active);
    return result;
  }
}
