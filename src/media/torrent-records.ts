import type { Database } from "bun:sqlite";
import type { DownloadLocation, TorrentRef, TorrentSnapshot } from "./torrent-client";
import { assertTorrentUseWritable } from "./torrent-use";
import { assertTorrentRecordAgreement, torrentLocation, type TorrentOwnership, type TorrentRecord } from "./torrent-records-schema";
export type { TorrentOwnership, TorrentRecord } from "./torrent-records-schema";

/** One durable record per client/native identity. Domain associations live elsewhere. */
export class TorrentRecordsRepo {
  constructor(private db: Database, private beforeWrite: () => void = () => {}) {}

  get(clientId: string, nativeId: string): TorrentRecord | null {
    return this.db.query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?").get(clientId, nativeId) as TorrentRecord | null;
  }

  reserve(ref: TorrentRef, location: DownloadLocation, offerKey: string, ownership: Partial<TorrentOwnership> = {}): TorrentRecord {
    return this.db.transaction(() => {
      this.beforeWrite();
      assertTorrentUseWritable(this.db, { clientId: ref.clientId, infoHash: ref.nativeId });
      const canonical = torrentLocation(location);
      const expected: TorrentOwnership = { backend: ref.clientId.startsWith("transmission:") ? "transmission" : null,
        root_path: null, root_token: null, ownership_tag: null, ...ownership };
      const existing = this.get(ref.clientId, ref.nativeId);
      if (existing) {
        assertTorrentRecordAgreement(existing, canonical, expected);
        this.db.query(`UPDATE media_torrents SET backend=COALESCE(backend,?),root_path=COALESCE(root_path,?),
          root_token=COALESCE(root_token,?),ownership_tag=COALESCE(ownership_tag,?) WHERE id=?`)
          .run(expected.backend, expected.root_path, expected.root_token, expected.ownership_tag, existing.id);
        return this.get(ref.clientId, ref.nativeId)!;
      }
      return this.db.query(`INSERT INTO media_torrents(client_id,native_id,location_json,offer_key,backend,root_path,root_token,ownership_tag)
        VALUES(?,?,?,?,?,?,?,?) RETURNING *`).get(ref.clientId, ref.nativeId, JSON.stringify(canonical), offerKey,
          expected.backend, expected.root_path, expected.root_token, expected.ownership_tag) as TorrentRecord;
    }).immediate();
  }

  observe(id: number, snapshot: TorrentSnapshot) {
    this.db.transaction(() => {
      this.beforeWrite();
      const record = this.get(snapshot.ref.clientId, snapshot.ref.nativeId);
      if (!record || record.id !== id) throw new Error("Torrent health observation has another identity");
      assertTorrentRecordAgreement(record, torrentLocation(snapshot.location), { backend: null, root_path: null, root_token: null, ownership_tag: null });
      this.db.query("UPDATE media_torrents SET observation_json=? WHERE id=?").run(JSON.stringify(snapshot), id);
    })();
  }
}
