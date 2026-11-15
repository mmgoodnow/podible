import { Database, constants } from "bun:sqlite";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { savedCompletionTime } from "../src/media/acquisition-latency";

const { values } = parseArgs({ options: {
  db: { type: "string" }, limit: { type: "string", default: "10" }, since: { type: "string" },
  "execution-db": { type: "string" },
} });
const limit = Number(values.limit);
const since = values.since ? Date.parse(values.since) : null;
if (!values.db || !Number.isSafeInteger(limit) || limit < 1 || limit > 100 || (since !== null && !Number.isFinite(since)))
  throw new Error("Usage: bun run scripts/report-plex-latency.ts --db PATH [--execution-db PATH] [--limit 10] [--since ISO_DATE]");

type Receipt = { acquisitionId: number; versionId: number; jobId: number | null; reason: string; queuedAt: string; readyAt: string; publishingAt: string | null; observation: string | null };
using db = new Database(values.db, constants.SQLITE_OPEN_READONLY | constants.SQLITE_OPEN_URI);
if (values["execution-db"]) {
  const uri = pathToFileURL(values["execution-db"]);
  uri.searchParams.set("mode", "ro");
  db.query("ATTACH DATABASE ? AS execution").run(uri.toString());
}
db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=2000");

const receiptPhases = {
  seal_files: new Set(["canonical-seal:read-reservation", "canonical-seal:preflight", "canonical-seal:daemon",
    "canonical-seal:verify", "canonical-seal:persist"]),
  publication_attempt: new Set(["publication:arm", "publication:mutex-wait", "publication:stage", "publication:reconcile",
    "publication:scan", "publication:verify", "publication:promote", "publication:rollback", "publication:cleanup", "publication:complete"]),
};

function readPhaseReceipt(output: string | null, phase: keyof typeof receiptPhases, complete: number, fromMs: number, toMs: number) {
  if (!output) return [];
  let receipt: { phases?: unknown } | null;
  try { receipt = JSON.parse(output); } catch { return []; }
  if (!Array.isArray(receipt?.phases)) return [];
  return receipt.phases.slice(0, 12).flatMap(item => {
    if (!item || typeof item.name !== "string" || !receiptPhases[phase].has(item.name) ||
      !Number.isSafeInteger(item.startedAt) || !Number.isSafeInteger(item.finishedAt)) return [];
    const start = Math.max(fromMs, item.startedAt - complete), end = Math.min(toMs, item.finishedAt - complete);
    return end > start ? [{ phase: item.name as string, fromMs: start, toMs: end }] : [];
  });
}

function readTimeline(job: number, complete: number, ready: number) {
  const steps = db.query<{ name: string; kind: string; start: number; end: number; output: string | null }, [number, number, number]>(`
    SELECT s.name,s.kind,s.started_at AS start,s.updated_at AS end,
      CASE WHEN s.kind='step' AND (s.name LIKE '%:seal-download' OR s.name GLOB '*:plex-publication:[0-9]*')
        AND length(s.output)<=8192 THEN s.output END AS output
    FROM job_executions j JOIN execution.workflow_steps s ON s.run_id=j.run_id
    WHERE j.job_id=? AND s.status='completed' AND s.updated_at>=? AND s.started_at<=?
    ORDER BY s.started_at,s.created_at LIMIT 401
  `).all(job, complete, ready);
  const segments = steps.slice(0, 400).flatMap(({ name, kind, start, end, output }) => {
    const phase = /:plex-publication:\d+$/.test(name) ? "publication_attempt"
      : /:plex-publication:\d+:event:\d+$/.test(name) && kind === "wait" ? "wait_for_plex_event"
      : /:completion:\d+:inspect$/.test(name) ? "read_torrent_completion"
      : /:seal-download$/.test(name) ? "seal_files"
      : /:probe:\d+$/.test(name) ? "probe_file"
      : /:inspect$/.test(name) ? "review_import"
      : /:adopt:\d+$/.test(name) ? "adopt_file"
      : /:version$/.test(name) ? "record_version"
      : /:cross-seed-copies$/.test(name) ? "queue_cross_seed_copies"
      : null;
    const fromMs = Math.max(complete, start) - complete;
    const toMs = Math.min(ready, end) - complete;
    if (!phase || toMs < fromMs) return [];
    const subphases = phase === "seal_files" || phase === "publication_attempt"
      ? readPhaseReceipt(output, phase, complete, fromMs, toMs) : [];
    return [{ phase, fromMs, toMs, ...(subphases.length ? { subphases } : {}) }];
  });
  return { segments, truncated: steps.length > 400 };
}

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
    SELECT a.id AS acquisitionId,v.id AS versionId,a.job_id AS jobId,a.reason,a.created_at AS queuedAt,ready.created_at AS readyAt,
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
  const samples = rows.flatMap(({ observation, jobId, ...row }) => {
    const completeAt = savedCompletionTime(observation);
    const complete = Date.parse(completeAt ?? ""), publish = Date.parse(row.publishingAt ?? ""), ready = Date.parse(row.readyAt);
    if (!(Date.parse(row.queuedAt) <= complete && complete <= publish && publish <= ready)) return [];
    return [{ ...row, completeAt, preparationMs: publish - complete, publicationMs: ready - publish,
      totalMs: ready - complete, underTarget: ready - complete < targetMs,
      ...(values["execution-db"] ? { timeline: jobId === null ? null : readTimeline(jobId, complete, ready) } : {}) }];
  }).slice(0, limit);
  return { media, targetMs, candidatesExamined: rows.length, samples };
}))();
console.log(JSON.stringify({ kind: "recorded-download-to-verified-plex", generatedAt: new Date().toISOString(), report }, null, 2));
