import type { Database, SQLQueryBindings } from "bun:sqlite";
import type { RecoveryKey } from "./recovery-types";

type Row = Record<string, unknown>;
export type RecoveryDatabase = {
  target: Row | null; entry: Row | null; requests: Row[]; acquisitions: Row[];
  versions: Row[]; files: Row[]; links: Row[]; publications: Row[];
  activeEpisodes: Row[]; requestEpisodes: Row[]; episodeMetadata: Row[]; numbering: Row[]; coverage: Row[];
  overflow: boolean;
};
export type RecoveryReadInput = { namespace: RecoveryKey["namespace"]; targetId?: number; afterTargetId?: number; limit?: number; fence?: boolean };
/** Read-only target-scoped joins, with a hard LIMIT on every expanding result. No global row-count ceiling. */
export function readRecoveryInventory(db: Database, input: RecoveryReadInput) {
  // A read snapshot outside a writer, a nested savepoint when called by the short fence.
  return db.transaction(() => queryRecoveryInventory(db, input))();
}

function queryRecoveryInventory(db: Database, input: RecoveryReadInput) {
  const movie = input.namespace === "movie-target", prefix = movie ? "movie" : "tv";
  if (!["movie-target", "tv-target"].includes(input.namespace)) throw new TypeError("Invalid recovery namespace");
  const all = (sql: string, ...params: SQLQueryBindings[]) => db.query(sql).all(...params) as Row[];
  if (input.targetId === undefined) {
    const after = input.afterTargetId ?? 0, limit = input.limit ?? 20;
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 20)
      throw new TypeError("Invalid recovery page");
    const rows = all(`SELECT id FROM ${prefix}_targets WHERE id>? ORDER BY id LIMIT ?`, after, limit + 1);
    return { targetIds: rows.slice(0, limit).map((row) => Number(row.id)), hasMore: rows.length > limit, snapshot: null as RecoveryDatabase | null };
  }
  const id = input.targetId;
  if (!Number.isSafeInteger(id) || id < 1) throw new TypeError("Invalid recovery target");
  const target = all(`SELECT * FROM ${prefix}_targets WHERE id=?`, id)[0] ?? null;
  const entry = target ? all(movie ? "SELECT * FROM movies WHERE id=?" : "SELECT * FROM tv_entries WHERE id=?",
    Number(target[movie ? "movie_id" : "entry_id"]))[0] ?? null : null;
  const versionJoin = movie ? "JOIN movie_acquisitions a ON a.id=v.acquisition_id WHERE a.target_id=?" : "WHERE v.target_id=?";
  const fileJoin = `JOIN ${prefix}_version_files f ON f.version_id=v.id ${versionJoin}`;
  const historyLimit = input.fence ? 21 : 101, fileLimit = input.fence ? 21 : 501;
  const versions = all(`SELECT v.* FROM ${prefix}_versions v ${versionJoin} ORDER BY v.id LIMIT ${historyLimit}`, id);
  const links = all(`SELECT f.* FROM ${prefix}_versions v ${fileJoin} ORDER BY f.version_id,f.sequence LIMIT ${fileLimit}`, id);
  const files = all(`SELECT DISTINCT c.* FROM managed_content_files c JOIN ${prefix}_version_files f ON f.file_id=c.id
    JOIN ${prefix}_versions v ON v.id=f.version_id ${versionJoin} ORDER BY c.id LIMIT ${fileLimit}`, id);
  const requests = all(`SELECT * FROM ${prefix}_requests WHERE target_id=? ORDER BY id LIMIT 21`, id);
  const acquisitions = all(`SELECT * FROM ${prefix}_acquisitions WHERE target_id=? ORDER BY id DESC LIMIT ${historyLimit}`, id);
  const publications = all(`SELECT * FROM ${prefix}_publications WHERE target_id=? LIMIT 2`, id);
  const activeEpisodes = movie ? [] : all("SELECT * FROM tv_active_episodes WHERE target_id=? ORDER BY episode_id LIMIT 501", id);
  const requestEpisodes = movie ? [] : all(`SELECT e.* FROM tv_request_episodes e JOIN tv_requests r ON r.id=e.request_id
    WHERE r.target_id=? ORDER BY e.request_id,e.episode_id LIMIT 501`, id);
  const episodeMetadata = movie ? [] : all(`SELECT DISTINCT e.* FROM tv_episodes e WHERE e.id IN (
    SELECT a.episode_id FROM tv_active_episodes a WHERE a.target_id=? UNION
    SELECT q.episode_id FROM tv_request_episodes q JOIN tv_requests r ON r.id=q.request_id WHERE r.target_id=?) ORDER BY e.id LIMIT 501`, id, id);
  const numbering = movie ? [] : all(`SELECT n.* FROM tv_episode_numbering n JOIN tv_targets t ON t.entry_id=n.entry_id
    WHERE t.id=? AND n.scheme='tvdb:aired' AND n.episode_id IN (
      SELECT a.episode_id FROM tv_active_episodes a WHERE a.target_id=? UNION
      SELECT q.episode_id FROM tv_request_episodes q JOIN tv_requests r ON r.id=q.request_id WHERE r.target_id=?)
    ORDER BY n.episode_id LIMIT 501`, id, id, id);
  const coverage = movie ? [] : all(`SELECT f.* FROM tv_version_file_episodes f JOIN tv_versions v ON v.id=f.version_id
    WHERE v.target_id=? ORDER BY f.version_id,f.file_id,f.episode_id LIMIT 501`, id);
  const snapshot: RecoveryDatabase = { target, entry, requests, acquisitions, versions, files, links, publications,
    activeEpisodes, requestEpisodes, episodeMetadata, numbering, coverage,
    overflow: requests.length > 20 || versions.length > 100 || acquisitions.length > 100 ||
      [files, links, activeEpisodes, requestEpisodes, episodeMetadata, numbering, coverage].some((rows) => rows.length > 500) };
  return { targetIds: [], hasMore: false, snapshot };
}
