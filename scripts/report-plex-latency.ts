import { Database } from "bun:sqlite";
import { parseArgs } from "node:util";
import { savedCompletionTime } from "../src/media/acquisition-latency";

const { values } = parseArgs({ options: {
  db: { type: "string" }, limit: { type: "string", default: "10" }, since: { type: "string" },
} });
const limit = Number(values.limit);
const since = values.since ? Date.parse(values.since) : null;
if (!values.db || !Number.isSafeInteger(limit) || limit < 1 || limit > 100 || (since !== null && !Number.isFinite(since)))
  throw new Error("Usage: bun run scripts/report-plex-latency.ts --db PATH [--limit 10] [--since ISO_DATE]");

type Receipt = { acquisitionId: number; versionId: number; reason: string; queuedAt: string; readyAt: string; publishingAt: string | null; observation: string | null };
using db = new Database(values.db, { readonly: true });
const report = db.transaction(() => (["movie", "tv"] as const).map(media => {
  const source = media === "movie" ? `JOIN app_state pin ON pin.key='release-health:movie-version-source:'||v.id
    JOIN media_torrents t ON t.id=json_extract(pin.value_json,'$.id')`
    : `JOIN media_torrents t ON t.client_id=json_extract(v.facts_json,'$.source.ref.clientId')
      AND t.native_id=json_extract(v.facts_json,'$.source.ref.nativeId')`;
  const prefix = media === "movie" ? "published:" : "tv-published:";
  const tvAttempt = "json_extract(v.facts_json,'$.attemptKey')";
  const matchAttempt = media === "tv" ? `AND (${tvAttempt} IS NULL OR published.event_key =
    'tv-acquisition:'||a.id||':'||
    CASE WHEN ${tvAttempt} GLOB 'tv:attempt:*' THEN substr(${tvAttempt},12) ELSE ${tvAttempt} END||':publishing')` : "";
  const rows = db.query<Receipt, [string, string, string | null, number]>(`
    SELECT a.id AS acquisitionId,v.id AS versionId,a.reason,a.created_at AS queuedAt,ready.created_at AS readyAt,
      (SELECT CASE WHEN count(*)=1 THEN min(published.created_at) END FROM ${media}_events published
       WHERE published.target_id=a.target_id AND published.acquisition_id=a.id
         AND published.stage='publishing' AND published.id<ready.id ${matchAttempt}) AS publishingAt,
      t.observation_json AS observation
    FROM ${media}_acquisitions a JOIN ${media}_versions v ON v.acquisition_id=a.id
    JOIN acquisition_latency_receipts r ON r.media=?1 AND r.acquisition_id=a.id
    JOIN ${media}_events ready ON ready.event_key=?2||a.target_id||':'||v.id AND ready.stage='available'
    ${source}
    JOIN ${media}_acquisition_downloads d ON d.acquisition_id=a.id AND d.torrent_id=t.id
    WHERE (?3 IS NULL OR ready.created_at>=?3)
    ORDER BY ready.created_at DESC,ready.id DESC LIMIT ?4
  `).all(media, prefix, since === null ? null : new Date(since).toISOString(), limit * 10);
  const targetMs = media === "movie" ? 20_000 : 30_000;
  const samples = rows.flatMap(({ observation, ...row }) => {
    const completeAt = savedCompletionTime(observation);
    const complete = Date.parse(completeAt ?? ""), publish = Date.parse(row.publishingAt ?? ""), ready = Date.parse(row.readyAt);
    if (!(Date.parse(row.queuedAt) <= complete && complete <= publish && publish <= ready)) return [];
    return [{ ...row, completeAt, preparationMs: publish - complete, publicationMs: ready - publish,
      totalMs: ready - complete, underTarget: ready - complete < targetMs }];
  }).slice(0, limit);
  return { media, targetMs, candidatesExamined: rows.length, samples };
}))();
console.log(JSON.stringify({ kind: "recorded-download-to-verified-plex", generatedAt: new Date().toISOString(), report }, null, 2));
