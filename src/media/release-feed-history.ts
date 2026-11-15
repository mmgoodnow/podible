import type { Database } from "bun:sqlite";
import type { IndexerMedia } from "./indexer-gateway";

export const releasePurposes = ["initial", "upgrade", "recovery", "cross-seed"] as const;
export type ReleasePurpose = typeof releasePurposes[number];
export type ReleaseAction = {
  targetKind: IndexerMedia; targetId: number; title: string | null; href: string | null;
  purpose: ReleasePurpose; status: "candidate" | "picked" | "cross-seeded" | "not-seeded";
  seedOrdinal: { min: number; max: number } | null;
};

// Keep the expression identical in the page/read queries so SQLite can seek the indexes.
export const crossSeedFeedReleaseId = "coalesce(json_extract(op.value_json,'$.command.releaseId'),json_extract(op.value_json,'$.feedReleaseId'))";

export const RELEASE_FEED_HISTORY_SQL = `
CREATE TABLE release_feed_picks (
  release_id INTEGER NOT NULL REFERENCES release_inbox(id),
  target_kind TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  purpose TEXT NOT NULL CHECK(purpose IN ('initial','upgrade','recovery')),
  PRIMARY KEY(release_id,target_kind,target_id,purpose)
) WITHOUT ROWID;
UPDATE app_state AS op
SET value_json=json_set(op.value_json,'$.feedReleaseId',json_extract(copy.value_json,'$.offer.inboxId'))
FROM app_state copy
WHERE op.key >= 'cross-seed:operation:' AND op.key < 'cross-seed:operation:~'
  AND copy.key='cross-seed:selected-copy:' || json_extract(op.value_json,'$.command.selectionCopyId')
  AND json_type(copy.value_json,'$.offer.inboxId')='integer';
CREATE INDEX release_feed_seed_operations ON app_state(${crossSeedFeedReleaseId.replaceAll("op.", "")})
  WHERE key >= 'cross-seed:operation:' AND key < 'cross-seed:operation:~';
CREATE INDEX release_feed_picked_seeds ON app_state(${crossSeedFeedReleaseId.replaceAll("op.", "")})
  WHERE key >= 'cross-seed:operation:' AND key < 'cross-seed:operation:~'
    AND json_extract(value_json,'$.outcome.kind')='seeding';
CREATE TABLE cross_seed_file_history (
  source_id TEXT NOT NULL,
  torrent_id INTEGER NOT NULL,
  operation_id TEXT NOT NULL,
  seeded_at TEXT NOT NULL,
  PRIMARY KEY(source_id,torrent_id)
) WITHOUT ROWID;
CREATE INDEX cross_seed_file_by_operation ON cross_seed_file_history(operation_id);
CREATE INDEX cross_seed_file_by_time ON cross_seed_file_history(source_id,seeded_at,torrent_id);

-- Only old receipts with exact saved file IDs can supply historical ordinals.
INSERT OR IGNORE INTO cross_seed_file_history
SELECT json_extract(file.value,'$.source.id'),json_extract(op.value_json,'$.outcome.torrentId'),
       json_extract(op.value_json,'$.command.operationId'),json_extract(op.value_json,'$.outcome.at')
FROM app_state op
JOIN app_state layout ON layout.key='cross-seed:optimistic:' || json_extract(op.value_json,'$.command.operationId')
  AND json_extract(layout.value_json,'$.torrentId')=json_extract(op.value_json,'$.outcome.torrentId')
CROSS JOIN json_each(layout.value_json,'$.plan.files') file
WHERE op.key >= 'cross-seed:operation:' AND op.key < 'cross-seed:operation:~'
  AND json_extract(op.value_json,'$.outcome.kind')='seeding'
  AND json_extract(file.value,'$.role')='content' AND json_type(file.value,'$.source.id')='text';

-- A saved selection alone is not an acquisition: require its download binding too.
WITH selected AS MATERIALIZED (
  SELECT json_extract(value_json,'$.selected.inboxId') AS release_id,
         json_extract(value_json,'$.selected.key') AS offer_key,
         substr(key,19,instr(substr(key,19),':')-1) AS media,
         cast(substr(key,19+instr(substr(key,19),':')) AS INTEGER) AS acquisition_id
  FROM app_state WHERE key >= 'release-selection:' AND key < 'release-selection:~'
    AND (key GLOB 'release-selection:movie:*' OR key GLOB 'release-selection:tv:*')
    AND json_type(value_json,'$.selected.inboxId')='integer'
)
INSERT OR IGNORE INTO release_feed_picks
SELECT i.id,'movie',a.target_id,a.reason FROM selected s
JOIN release_inbox i ON i.id=s.release_id
JOIN movie_acquisitions a ON a.id=s.acquisition_id
JOIN movie_acquisition_downloads d ON d.acquisition_id=a.id
JOIN media_torrents t ON t.id=d.torrent_id AND t.offer_key=s.offer_key
WHERE s.media='movie'
UNION ALL
SELECT i.id,'tv',a.target_id,a.reason FROM selected s
JOIN release_inbox i ON i.id=s.release_id
JOIN tv_acquisitions a ON a.id=s.acquisition_id
JOIN tv_acquisition_downloads d ON d.acquisition_id=a.id
JOIN media_torrents t ON t.id=d.torrent_id AND t.offer_key=s.offer_key
WHERE s.media='tv';
`;

export function releaseActions(db: Database, ids: number[]) {
  type Row = Omit<ReleaseAction, "href" | "seedOrdinal"> & {
    releaseId: number; tmdbId: number | null; entryId: number | null; minOrdinal: number | null; maxOrdinal: number | null;
  };
  const rows = db.query<Row, { $ids: string }>(`
    WITH ids(id) AS (SELECT value FROM json_each($ids)), activities AS (
      SELECT p.release_id,p.target_kind,p.target_id,p.purpose,'picked' AS status,NULL AS operation_id
      FROM ids CROSS JOIN release_feed_picks p ON p.release_id=ids.id
      UNION ALL
      SELECT c.release_id,c.target_kind,c.target_id,CASE
        WHEN c.target_kind IN ('movie','tv') THEN coalesce(ma.reason,ta.reason,'initial')
        WHEN json_extract(j.payload_json,'$.considerUpgrade')=1 THEN 'upgrade' ELSE 'initial' END,
        'candidate',NULL
      FROM ids CROSS JOIN release_considerations c ON c.release_id=ids.id
      LEFT JOIN movie_acquisitions ma ON c.target_kind='movie' AND ma.job_id=c.job_id
      LEFT JOIN tv_acquisitions ta ON c.target_kind='tv' AND ta.job_id=c.job_id
      LEFT JOIN jobs j ON c.target_kind IN ('book:audio','book:ebook') AND j.id=c.job_id
      WHERE c.target_kind IN ('movie','tv','book:audio','book:ebook')
      UNION ALL
      SELECT ids.id,json_extract(op.value_json,'$.command.scope.media'),json_extract(op.value_json,'$.command.scope.targetId'),
        'cross-seed',CASE WHEN json_extract(op.value_json,'$.outcome.kind')='seeding' THEN 'cross-seeded'
          WHEN json_extract(op.value_json,'$.outcome.kind') IS NULL THEN 'candidate' ELSE 'not-seeded' END,
        CASE WHEN json_extract(op.value_json,'$.outcome.kind')='seeding'
          THEN json_extract(op.value_json,'$.command.operationId') END
      FROM ids CROSS JOIN app_state op ON ${crossSeedFeedReleaseId}=ids.id
      WHERE op.key >= 'cross-seed:operation:' AND op.key < 'cross-seed:operation:~'
    ), deduped AS (
      SELECT release_id,replace(target_kind,'book:','') AS kind,target_id,purpose,status,operation_id
      FROM activities GROUP BY release_id,kind,target_id,purpose,status,operation_id
    ), ranks AS (
      SELECT a.operation_id,(
        SELECT count(*) FROM cross_seed_file_history earlier
        WHERE earlier.source_id=f.source_id AND (earlier.seeded_at,earlier.torrent_id)<=(f.seeded_at,f.torrent_id)
      ) AS ordinal
      FROM (SELECT DISTINCT operation_id FROM deduped WHERE status='cross-seeded') a
      CROSS JOIN cross_seed_file_history f USING(operation_id)
    ), ordinals AS (
      SELECT operation_id,min(ordinal) AS first,max(ordinal) AS last FROM ranks GROUP BY operation_id
    )
    SELECT a.release_id AS releaseId,a.kind AS targetKind,a.target_id AS targetId,a.purpose,a.status,
      coalesce(b.title,json_extract(m.metadata_json,'$.title'),json_extract(e.metadata_json,'$.title')) AS title,
      m.tmdb_id AS tmdbId,e.id AS entryId,o.first AS minOrdinal,o.last AS maxOrdinal
    FROM deduped a
    LEFT JOIN books b ON a.kind IN ('audio','ebook') AND b.id=a.target_id
    LEFT JOIN movie_targets mt ON a.kind='movie' AND mt.id=a.target_id
    LEFT JOIN movies m ON m.id=mt.movie_id
    LEFT JOIN tv_targets tt ON a.kind='tv' AND tt.id=a.target_id
    LEFT JOIN tv_entries e ON e.id=tt.entry_id
    LEFT JOIN ordinals o USING(operation_id)
    ORDER BY a.release_id,a.status IN ('picked','cross-seeded') DESC,a.target_id,a.purpose
  `).all({ $ids: JSON.stringify(ids) });
  const byRelease = new Map<number, ReleaseAction[]>();
  for (const { releaseId, tmdbId, entryId, minOrdinal, maxOrdinal, ...action } of rows) {
    const list = byRelease.get(releaseId) ?? [];
    list.push({ ...action, seedOrdinal: minOrdinal !== null && maxOrdinal !== null ? { min: minOrdinal, max: maxOrdinal } : null,
      href: tmdbId !== null ? `/movies/${tmdbId}` : entryId !== null ? `/tv/library/${entryId}`
        : action.title !== null && (action.targetKind === "audio" || action.targetKind === "ebook") ? `/book/${action.targetId}` : null });
    byRelease.set(releaseId, list);
  }
  for (const [id, list] of byRelease) byRelease.set(id, list.filter(action => action.status !== "candidate" ||
    !list.some(other => other.targetKind === action.targetKind && other.targetId === action.targetId &&
      other.purpose === action.purpose && (other.status === "picked" || other.status === "cross-seeded"))));
  return byRelease;
}
