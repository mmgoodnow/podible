import type { Database, SQLQueryBindings } from "bun:sqlite";
import path from "node:path";
import { createHash } from "node:crypto";
import { tvRetentionSeasonSchema, type TvRetentionInventory, type TvRetentionSeason } from "./tv-retention-types";

type Row = Record<string, unknown>;
/** Parent may expose this through a read-only BooksRepo bridge. No migration number or write capability. */
export function readTvRetentionInventory(db: Database, input: TvRetentionSeason): TvRetentionInventory {
  const scope = tvRetentionSeasonSchema.parse(input);
  return db.transaction(() => {
    const all = (sql: string, ...params: SQLQueryBindings[]) => db.query(sql).all(...params) as Row[];
    const target = all("SELECT id,entry_id,revision FROM tv_targets WHERE id=?", scope.targetId)[0];
    const empty: TvRetentionInventory = { target: null, work: null, episodes: [], requests: [], selectedEpisodeIds: [], files: [],
      acquisitions: [], publications: [], downloads: [], lossInventoryKnown: false, overflow: false, lastRequestedAt: null,
      allSelectedEpisodeIds: [], allRequestedEpisodeIds: [] };
    if (!target) return empty;
    const entryId = Number(target.entry_id);
    const rawEntry = all("SELECT catalog_key,metadata_json FROM tv_entries WHERE id=?", entryId)[0]!;
    const entry = JSON.parse(String(rawEntry.metadata_json));
    const episodes = all(`SELECT e.id,e.episode_key,e.metadata_json FROM tv_episodes e
      JOIN tv_episode_numbering n ON n.episode_id=e.id WHERE n.entry_id=? AND n.scheme=? AND n.season=?
      ORDER BY e.id LIMIT 51`, entryId, scope.scheme, scope.seasonNumber);
    const ids = episodes.map((episode) => Number(episode.id)), marks = ids.map(() => "?").join(",") || "NULL";
    const requests = all(`SELECT DISTINCT r.* FROM tv_requests r JOIN tv_targets t ON t.id=r.target_id
      JOIN tv_request_episodes e ON e.request_id=r.id WHERE t.entry_id=? AND e.episode_id IN (${marks}) ORDER BY r.id LIMIT 101`, entryId, ...ids);
    const requestIds = requests.map((request) => Number(request.id)), requestMarks = requestIds.map(() => "?").join(",") || "NULL";
    const targetIds = [...new Set([scope.targetId, ...requests.map((request) => Number(request.target_id))])];
    const targetMarks = targetIds.map(() => "?").join(",");
    const seasonKeys = targetIds.map((id) => `${id}:${scope.scheme}:${scope.seasonNumber}`);
    const latest = all(`SELECT MAX(created_at) AS latest FROM retention_events WHERE action='requested' AND
      ((namespace='tv-target' AND resource_id IN (${targetMarks})) OR (namespace='tv-season' AND resource_id IN (${targetMarks})))`,
      ...targetIds.map(String), ...seasonKeys)[0];
    const requested = all(`SELECT request_id,episode_id FROM tv_request_episodes WHERE request_id IN (${requestMarks}) ORDER BY request_id,episode_id LIMIT 5001`, ...requestIds);
    const selected = all(`SELECT a.episode_id,f.file_id,a.version_id FROM tv_active_episodes a
      JOIN tv_version_file_episodes f ON f.version_id=a.version_id AND f.episode_id=a.episode_id
      WHERE a.target_id=? AND a.episode_id IN (${marks}) ORDER BY a.episode_id,f.file_id LIMIT 501`, scope.targetId, ...ids);
    const allSelected = all("SELECT episode_id FROM tv_active_episodes WHERE target_id=? ORDER BY episode_id LIMIT 501", scope.targetId);
    const allRequested = all(`SELECT DISTINCT e.episode_id FROM tv_request_episodes e JOIN tv_requests r ON r.id=e.request_id
      WHERE r.target_id=? ORDER BY e.episode_id LIMIT 501`, scope.targetId);
    const fileIds = [...new Set(selected.map((row) => String(row.file_id)))], fileMarks = fileIds.map(() => "?").join(",") || "NULL";
    const files = all(`SELECT * FROM managed_content_files WHERE id IN (${fileMarks}) ORDER BY id LIMIT 21`, ...fileIds);
    const coverage = all(`SELECT DISTINCT file_id,episode_id FROM tv_version_file_episodes
      WHERE file_id IN (${fileMarks}) ORDER BY file_id,episode_id LIMIT 1001`, ...fileIds);
    const associations = all(`SELECT DISTINCT f.file_id,v.target_id FROM tv_version_files f
      JOIN tv_versions v ON v.id=f.version_id WHERE f.file_id IN (${fileMarks}) LIMIT 1001`, ...fileIds);
    const acquisitions = all(`SELECT a.id,a.job_id,a.finished_at FROM tv_acquisitions a JOIN tv_targets t ON t.id=a.target_id
      WHERE t.entry_id=? ORDER BY a.id LIMIT 101`, entryId);
    const publications = all("SELECT p.target_id FROM tv_publications p JOIN tv_targets t ON t.id=p.target_id WHERE t.entry_id=? LIMIT 101", entryId);
    const downloads = all(`SELECT DISTINCT t.*,f.file_id FROM media_torrents t
      JOIN tv_acquisition_downloads d ON d.torrent_id=t.id JOIN tv_versions v ON v.acquisition_id=d.acquisition_id
      JOIN tv_version_files f ON f.version_id=v.id WHERE f.file_id IN (${fileMarks}) ORDER BY t.id,f.file_id LIMIT 401`, ...fileIds);
    const hasLossTable = !!all("SELECT 1 FROM sqlite_master WHERE type='table' AND name='media_content_losses'").length;
    const losses = hasLossTable ? all(`SELECT file_id FROM media_content_losses WHERE file_id IN (${fileMarks}) LIMIT 21`, ...fileIds) : [];
    const shared = new Set(associations.filter((row) => row.target_id !== scope.targetId).map((row) => String(row.file_id)));
    const tableNames = all("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 257");
    let referencesKnown = tableNames.length <= 256;
    for (const { name } of tableNames.slice(0, 256)) {
      const quoted = `"${String(name).replace(/"/g, '""')}"`;
      const columns = all(`PRAGMA table_info(${quoted})`).map((column) => column.name);
      if (["tv_version_files", "tv_version_file_episodes", "media_content_losses"].includes(String(name)) || !columns.includes("file_id")) continue;
      for (const row of all(`SELECT DISTINCT file_id FROM ${quoted} WHERE file_id IN (${fileMarks}) LIMIT 21`, ...fileIds)) shared.add(String(row.file_id));
    }
    const settings = all("SELECT value_json FROM settings WHERE id=1")[0];
    const root = settings ? JSON.parse(String(settings.value_json))?.tv?.managedRoot : null;
    if (typeof root !== "string" || !path.isAbsolute(root)) referencesKnown = false;
    else for (const file of files) {
      const canonical = path.resolve(root, "objects", String(file.relative_path));
      if (all("SELECT 1 FROM asset_files WHERE path=? OR identity_path=? OR source_path=? LIMIT 1", canonical, canonical, canonical).length)
        shared.add(String(file.id));
    }
    const grouped = new Map<number, TvRetentionInventory["downloads"][number]>();
    for (const download of downloads) {
      const id = Number(download.id), item = grouped.get(id) ?? { id, clientId: String(download.client_id), nativeId: String(download.native_id),
        locationJson: String(download.location_json), fileIds: [] };
      if (!item.fileIds.includes(String(download.file_id))) item.fileIds.push(String(download.file_id));
      grouped.set(id, item);
    }
    return { target: { id: Number(target.id), entryId, revision: Number(target.revision) },
      work: { key: String(rawEntry.catalog_key), title: String(entry.title), tmdbId: entry.tmdbId ?? null, tvdbId: entry.tvdbId ?? null },
      episodes: episodes.map((episode) => ({ id: Number(episode.id), key: String(episode.episode_key), airDate: JSON.parse(String(episode.metadata_json)).airDate ?? null })),
      requests: requests.map((request) => ({ id: Number(request.id), targetId: Number(request.target_id), createdAt: String(request.created_at),
        withdrawnAt: request.withdrawn_at === null ? null : String(request.withdrawn_at), episodeIds: requested.filter((row) => row.request_id === request.id).map((row) => Number(row.episode_id)) })),
      selectedEpisodeIds: [...new Set(selected.map((row) => Number(row.episode_id)))],
      allSelectedEpisodeIds: allSelected.map((row) => Number(row.episode_id)), allRequestedEpisodeIds: allRequested.map((row) => Number(row.episode_id)),
      files: files.map((file) => ({ id: String(file.id), relativePath: String(file.relative_path), sizeBytes: Number(file.size_bytes),
        episodeIds: coverage.filter((row) => row.file_id === file.id).map((row) => Number(row.episode_id)),
        references: shared.has(String(file.id)) ? "shared" as const : referencesKnown ? "verified-unshared" as const : "unknown" as const,
        lost: losses.some((row) => row.file_id === file.id) })),
      acquisitions: acquisitions.map((a) => ({ id: Number(a.id), jobId: a.job_id === null ? null : Number(a.job_id), finishedAt: a.finished_at === null ? null : String(a.finished_at) })),
      publications: publications.map((p) => Number(p.target_id)), downloads: [...grouped.values()], lossInventoryKnown: hasLossTable,
      lastRequestedAt: latest?.latest == null ? null : String(latest.latest),
      overflow: episodes.length > 50 || requests.length > 100 || requested.length > 5000 || files.length > 20 || fileIds.length > 20 ||
        coverage.length > 1000 || associations.length > 1000 || selected.length > 500 || allSelected.length > 500 || allRequested.length > 500 ||
        acquisitions.length > 100 || publications.length > 100 || grouped.size > 20 || downloads.length > 400,
    };
  })();
}

export function tvRetentionDemandHash(inventory: TvRetentionInventory) {
  return createHash("sha256").update(JSON.stringify([inventory.work, inventory.episodes, inventory.requests,
    inventory.allRequestedEpisodeIds, inventory.lastRequestedAt])).digest("hex");
}
