import path from "node:path";
import type { Database } from "bun:sqlite";
import { z } from "zod";
import type { DownloadLocation } from "./torrent-client";

export type TorrentOwnership = { backend: string | null; root_path: string | null; root_token: string | null; ownership_tag: string | null };
export type TorrentRecord = TorrentOwnership & {
  id: number; client_id: string; native_id: string; location_json: string; offer_key: string; observation_json: string | null;
};
const locationSchema = z.object({ rootId: z.string().min(1), relativePath: z.string().min(1) }).strict();
export function torrentLocation(value: unknown): DownloadLocation { return locationSchema.parse(value); }
export function assertTorrentRecordAgreement(existing: TorrentRecord, location: DownloadLocation, ownership: TorrentOwnership) {
  const old = torrentLocation(JSON.parse(existing.location_json));
  if (old.rootId !== location.rootId || old.relativePath !== location.relativePath)
    throw new Error("Torrent binding uses another layout");
  for (const field of ["backend", "root_path", "root_token", "ownership_tag"] as const)
    if (ownership[field] !== null && existing[field] !== null && ownership[field] !== existing[field])
      throw new Error(`Torrent ownership conflicts at ${field}`);
}

export const TORRENT_RECORDS_SCHEMA_SQL = `
ALTER TABLE media_torrents ADD COLUMN backend TEXT;
ALTER TABLE media_torrents ADD COLUMN root_path TEXT;
ALTER TABLE media_torrents ADD COLUMN root_token TEXT;
ALTER TABLE media_torrents ADD COLUMN ownership_tag TEXT;
CREATE TABLE book_torrent_bindings (
  torrent_id INTEGER PRIMARY KEY REFERENCES media_torrents(id),
  -- No book FK: ownership tombstones must survive deletion of the domain object.
  book_id INTEGER NOT NULL,
  release_id INTEGER UNIQUE REFERENCES releases(id) ON DELETE SET NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_book_torrent_bindings_book ON book_torrent_bindings(book_id);
`;

/** Called only within the migration transaction. No filesystem or daemon reads/writes. */
export function consolidateBookTorrentRecords(db: Database) {
  db.exec(TORRENT_RECORDS_SCHEMA_SQL);
  db.exec(`UPDATE media_torrents SET backend=CASE WHEN client_id LIKE 'transmission:%' THEN 'transmission' END,
    ownership_tag=CASE WHEN json_valid(location_json) THEN CASE json_extract(location_json,'$.rootId')
      WHEN 'movies' THEN 'Podible Movies' WHEN 'tv' THEN 'Podible TV' END END`);
  const books = db.query("SELECT * FROM book_torrents ORDER BY info_hash").all() as {
    info_hash: string; book_id: number; release_id: number | null; backend: string; client_id: string;
    root_path: string; root_token: string; root_id: string; relative_path: string; created_at: string;
  }[];
  for (const book of books) {
    if (!/^[a-f0-9]{40}$/.test(book.info_hash) || book.backend !== "transmission" || book.root_id !== "books" ||
        book.relative_path !== book.info_hash || !book.client_id || !path.isAbsolute(book.root_path) ||
        !/^[a-f0-9-]{36}$/.test(book.root_token)) throw new Error("Cannot consolidate an invalid legacy book torrent binding");
    const location = { rootId: book.root_id, relativePath: book.relative_path };
    const ownership: TorrentOwnership = { backend: book.backend, root_path: book.root_path, root_token: book.root_token, ownership_tag: "Podible Books" };
    const existing = db.query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?")
      .get(book.client_id, book.info_hash) as TorrentRecord | null;
    if (existing) assertTorrentRecordAgreement(existing, location, ownership);
    const record = existing ?? db.query(`INSERT INTO media_torrents(client_id,native_id,location_json,offer_key,backend,root_path,root_token,ownership_tag)
      VALUES(?,?,?,'',?,?,?,?) RETURNING *`).get(book.client_id, book.info_hash, JSON.stringify(location),
        ownership.backend, ownership.root_path, ownership.root_token, ownership.ownership_tag) as TorrentRecord;
    if (existing) db.query(`UPDATE media_torrents SET backend=COALESCE(backend,?),root_path=COALESCE(root_path,?),
      root_token=COALESCE(root_token,?),ownership_tag=COALESCE(ownership_tag,?) WHERE id=?`)
      .run(ownership.backend, ownership.root_path, ownership.root_token, ownership.ownership_tag, existing.id);
    db.query("INSERT INTO book_torrent_bindings(torrent_id,book_id,release_id,created_at) VALUES(?,?,?,?)")
      .run(record.id, book.book_id, book.release_id, book.created_at);
  }
  db.exec("DROP TABLE book_torrents");
}
