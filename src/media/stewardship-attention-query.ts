import type { Database } from "bun:sqlite";
import { stewardshipIndexPrefix } from "./stewardship-binding";
import type { StewardshipInventoryPage, StewardshipInventoryInput } from "./stewardship-inventory";

export type StewardshipAttentionScope = { clientId: string | null; now: string;
  libraryRoot: string | null; movieRoot: string | null; tvRoot: string | null };
const json = (name: string) => `CASE WHEN json_valid(${name}) THEN ${name} ELSE '{}' END`;
const location = json("t.location_json");
const rootId = `json_extract(${location},'$.rootId')`, relative = `json_extract(${location},'$.relativePath')`;
const screenDomain = `(${rootId} IN ('movies','tv') OR t.id IN
  (SELECT torrent_id FROM movie_acquisition_downloads UNION SELECT torrent_id FROM tv_acquisition_downloads))`;
const privateHash = `t.client_id=$client AND length(t.native_id)=40 AND t.native_id NOT GLOB '*[^0-9a-f]*'`;
const uuid = `(length(t.root_token)=36 AND t.root_token GLOB '????????-????-????-????-????????????'
  AND length(replace(t.root_token,'-',''))=32
  AND replace(lower(t.root_token),'-','') NOT GLOB '*[^0-9a-f]*'
  AND ((substr(lower(t.root_token),15,1) GLOB '[1-8]' AND substr(lower(t.root_token),20,1) GLOB '[89ab]')
    OR lower(t.root_token) IN ('00000000-0000-0000-0000-000000000000','ffffffff-ffff-ffff-ffff-ffffffffffff')))`;

/** No JS binding walk or per-row state queries: native COUNT/filter precedes bounded row materialization. */
export function readStewardshipAttention(db: Database, input: StewardshipInventoryInput, scope: StewardshipAttentionScope): StewardshipInventoryPage {
  const sql = `WITH candidates AS (
    SELECT 'books' AS source,0 AS sort,t.id,t.native_id AS hash,${json("t.stewardship_binding_json")} AS binding_json,
      (${privateHash} AND $libraryRoot IS NOT NULL AND t.backend='transmission' AND ${rootId}='books'
        AND ${relative}=t.native_id AND substr(t.root_path,1,1)='/' AND ${uuid}) AS eligible,
      json_array('books',t.client_id,t.native_id,b.book_id,b.release_id,t.backend,t.root_path,t.root_token,${rootId},${relative},$libraryRoot) AS context,
      '${stewardshipIndexPrefix}books:'||t.client_id||':'||t.native_id AS index_key,
      json_object('info_hash',t.native_id,'book_id',b.book_id,'release_id',b.release_id,'backend',t.backend,'client_id',t.client_id,
        'root_path',t.root_path,'root_token',t.root_token,'root_id',${rootId},'relative_path',${relative}) AS row_json
    FROM book_torrent_bindings b JOIN media_torrents t ON t.id=b.torrent_id
    UNION ALL
    SELECT 'screen',1,t.id,t.native_id,${json("t.stewardship_binding_json")},
      (${privateHash} AND ${rootId} IN ('movies','tv') AND json_type(${location},'$.relativePath')='text'
        AND (SELECT count(*) FROM json_each(${location}))=2
        AND substr(CASE ${rootId} WHEN 'movies' THEN $movieRoot ELSE $tvRoot END,1,1)='/'
        AND length(${relative})>0 AND substr(${relative},1,1)<>'/' AND instr(${relative},'\\')=0
        AND instr(${relative},char(0))=0 AND ${relative} NOT GLOB ('*['||char(1)||'-'||char(31)||']*')
        AND instr('/'||${relative}||'/','//')=0 AND instr('/'||${relative}||'/','/./')=0
        AND instr('/'||${relative}||'/','/../')=0),
      json_array('screen',t.client_id,t.native_id,t.id,t.location_json,CASE ${rootId} WHEN 'movies' THEN $movieRoot ELSE $tvRoot END),
      '${stewardshipIndexPrefix}screen:'||t.client_id||':'||t.native_id,
      json_object('id',t.id,'client_id',t.client_id,'native_id',t.native_id,'location_json',t.location_json,'offer_key',t.offer_key)
    FROM media_torrents t WHERE NOT EXISTS(SELECT 1 FROM book_torrent_bindings b WHERE b.torrent_id=t.id) AND ${screenDomain}
  ), joined AS (
    SELECT c.*,${json("idx.value_json")} AS index_json,${json("obs.value_json")} AS history_json
    FROM candidates c LEFT JOIN app_state idx ON idx.key=c.index_key
    LEFT JOIN app_state obs ON obs.key='torrent-stewardship:v1:observation:'||json_extract(c.binding_json,'$.key')
    WHERE $source='all' OR c.source=$source
  ), verified AS (
    SELECT *,COALESCE(json_extract(index_json,'$.version')=1 AND json_extract(index_json,'$.contextJson')=context
      AND json_extract(binding_json,'$.contextJson')=context
      AND json_extract(index_json,'$.key')=json_extract(binding_json,'$.key')
      AND json_extract(index_json,'$.bindingDigest')=json_extract(binding_json,'$.bindingDigest')
      AND json_extract(index_json,'$.latestJson')=json_extract(history_json,'$.latest')
      AND json_extract(index_json,'$.bindingDigest')=json_extract(history_json,'$.latest.bindingDigest'),0) AS projection_verified
    FROM joined
  ), classified AS (
    SELECT *,COALESCE(eligible AND projection_verified
      AND CAST(round((julianday(json_extract(history_json,'$.latest.observedAt'))-2440587.5)*86400000) AS INTEGER) BETWEEN $nowMs-3600000 AND $nowMs
      AND json_extract(history_json,'$.latest.ownership')='verified-private-binding'
      AND json_extract(history_json,'$.latest.daemonError')='none'
      AND json_extract(history_json,'$.latest.trackerState')='healthy'
      AND json_extract(history_json,'$.latest.state') NOT IN ('unknown','paused-incomplete')
      AND json_extract(history_json,'$.latest.seedCounterRegressed')=0
      AND json_extract(history_json,'$.latest.actualSeedSeconds') IS NOT NULL
      AND json_extract(history_json,'$.latest.seedProtection') IN ('protected','minimum-met'),0) AS healthy
    FROM verified
  )`;
  const parameters = { $client: scope.clientId, $nowMs: Date.parse(scope.now), $libraryRoot: scope.libraryRoot,
    $movieRoot: scope.movieRoot, $tvRoot: scope.tvRoot, $source: input.source };
  return db.transaction(() => {
    const counts = db.query(`${sql} SELECT count(*) AS total,COALESCE(sum(COALESCE(eligible,0)),0) AS eligible,
      COALESCE(sum(healthy),0) AS healthy FROM classified`).get(parameters) as { total: number; eligible: number; healthy: number };
    const rows = db.query(`${sql} SELECT source,row_json,history_json,projection_verified,CASE WHEN projection_verified
        AND CAST(round((julianday(json_extract(history_json,'$.latest.observedAt'))-2440587.5)*86400000) AS INTEGER) BETWEEN $nowMs-3600000 AND $nowMs
        AND json_extract(history_json,'$.latest.ownership')='verified-private-binding'
        AND (json_extract(history_json,'$.latest.daemonError')='storage-error'
          OR json_extract(history_json,'$.latest.trackerState') IN ('confirmed-error','confirmed-unregistered')
          OR json_extract(history_json,'$.latest.state')='paused-incomplete') THEN 0 ELSE 1 END AS priority
      FROM classified WHERE eligible AND NOT healthy ORDER BY priority,sort,hash,id LIMIT $limit OFFSET $offset`)
      .all({ ...parameters, $limit: input.limit, $offset: input.offset }) as {
        source: "books" | "screen"; row_json: string; history_json: string; projection_verified: number;
      }[];
    return { rows: rows.map(row => ({ source: row.source, row: JSON.parse(row.row_json),
      historyJson: row.history_json, projectionVerified: !!row.projection_verified })), total: counts.total,
      summary: { scope: "current-private-bindings" as const, total: counts.eligible, healthy: counts.healthy,
        attention: counts.eligible - counts.healthy, excluded: counts.total - counts.eligible } };
  })();
}
