import type { Database, SQLQueryBindings } from "bun:sqlite";
import type { RetirementBinding } from "./torrent-retirement-types";

export type TorrentRetirementConsumers = { complete: boolean; count: number; jobIds: number[]; activePublications: number };

/** Exact, parameterized orphan/source queries. Unrelated table size never stands in for consumer evidence. */
export function readConsumerCount(db: Database, binding: RetirementBinding): TorrentRetirementConsumers {
  const get = (sql: string, ...params: SQLQueryBindings[]) => db.query(sql).get(...params) as Record<string, unknown> | null;
  const ids = (sql: string, ...params: SQLQueryBindings[]) => (db.query(sql).all(...params) as { job_id: number | null }[])
    .flatMap((row) => row.job_id === null ? [] : [row.job_id]);
  const number = (sql: string, ...params: SQLQueryBindings[]) => Number(get(sql, ...params)?.n ?? 0);
  try {
    let count = 0, jobIds: number[] = [], activePublications = 0;
    const directory = binding.directory, prefix = directory + "/", upper = directory + "0";
    count += number(`SELECT COUNT(*) AS n FROM asset_files WHERE
      path=? OR (path>=? AND path<?) OR source_path=? OR (source_path>=? AND source_path<?)
      OR identity_path=? OR (identity_path>=? AND identity_path<?)`, directory, prefix, upper, directory, prefix, upper, directory, prefix, upper);
    if (binding.media === "books") {
      const common = !!get("SELECT 1 FROM sqlite_master WHERE type='table' AND name='book_torrent_bindings'");
      const row = get(common ? `SELECT b.book_id,b.release_id FROM book_torrent_bindings b
        JOIN media_torrents t ON t.id=b.torrent_id WHERE t.client_id=? AND t.native_id=?`
        : "SELECT book_id,release_id FROM book_torrents WHERE client_id=? AND info_hash=?", binding.clientId, binding.hash);
      if (!row) return { complete: false, count, jobIds, activePublications };
      count += number("SELECT EXISTS(SELECT 1 FROM books WHERE id=?) AS n", Number(row.book_id));
      count += Number(row.release_id !== null);
      count += number("SELECT COUNT(*) AS n FROM releases WHERE info_hash=?", binding.hash);
      count += number(`SELECT COUNT(*) AS n FROM assets WHERE book_id=? OR source_release_id IN
        (SELECT id FROM releases WHERE info_hash=?)`, Number(row.book_id), binding.hash);
      jobIds = ids(`SELECT id AS job_id FROM jobs WHERE book_id=? OR release_id IN
        (SELECT id FROM releases WHERE info_hash=?) ORDER BY id LIMIT 101`, Number(row.book_id), binding.hash);
    } else {
      const row = get("SELECT id FROM media_torrents WHERE client_id=? AND native_id=?", binding.clientId, binding.hash);
      if (!row) return { complete: false, count, jobIds, activePublications };
      const id = Number(row.id);
      for (const media of ["movie", "tv"] as const) {
        count += number(`SELECT COUNT(*) AS n FROM ${media}_acquisition_downloads WHERE torrent_id=?`, id);
        activePublications += number(`SELECT COUNT(*) AS n FROM ${media}_publications p WHERE EXISTS (
          SELECT 1 FROM ${media}_acquisitions a JOIN ${media}_acquisition_downloads d ON d.acquisition_id=a.id
          WHERE d.torrent_id=? AND a.target_id=p.target_id)`, id);
        jobIds.push(...ids(`SELECT a.job_id FROM ${media}_acquisitions a JOIN ${media}_acquisition_downloads d ON d.acquisition_id=a.id
          WHERE d.torrent_id=? ORDER BY a.id LIMIT 101`, id));
        // Retain explicit version provenance even if its download link was independently lost.
        count += number(`SELECT COUNT(*) AS n FROM ${media}_versions WHERE json_valid(facts_json)
          AND json_extract(facts_json,'$.source.ref.clientId')=? AND json_extract(facts_json,'$.source.ref.nativeId')=?`, binding.clientId, binding.hash);
        if (number(`SELECT EXISTS(SELECT 1 FROM ${media}_versions WHERE NOT json_valid(facts_json)) AS n`))
          return { complete: false, count, jobIds: [], activePublications };
      }
    }
    const unique = [...new Set(jobIds)];
    return { complete: unique.length <= 100 && jobIds.length <= 200, count, jobIds: unique.slice(0, 100), activePublications };
  } catch {
    // Missing schemas, malformed metadata and unknown projections are blockers, never empty proofs.
    return { complete: false, count: 0, jobIds: [], activePublications: 0 };
  }
}
