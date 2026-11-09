import { Database, constants } from "bun:sqlite";
import { pathToFileURL } from "node:url";

type AcquisitionRow = {
  id: number; job_id: number; target_id: number; title: string | null;
  latest: number; finished_at: string | null; stage: string | null;
  active_request: number; evicted: number; decision_json: string | null;
};
type MovieActivity = AcquisitionRow & { tmdb_id: number | null; available: number; target_stage: string | null };
type TvActivity = AcquisitionRow & {
  entry_id: number; request_user_id: number | null; request_id: number | null; available: number;
  missing_dates: string; publishing_acquisition: number | null;
};

/** Filter terminal history before fetching payloads or aggregating Reflow steps. */
export function activityJobIds(domainPath: string, executionPath: string): number[] {
  using db = new Database(domainPath, constants.SQLITE_OPEN_READONLY | constants.SQLITE_OPEN_URI);
  db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=5000");
  const uri = pathToFileURL(executionPath);
  uri.searchParams.set("mode", "ro");
  db.query("ATTACH DATABASE ? AS activity_execution").run(uri.toString());
  // Keep unlinked outbox jobs, as well as missing/invalid linked runs so the
  // normal journal hydration still reports integrity errors rather than hiding them.
  return db.query<{ id: number }, []>(`SELECT j.id FROM jobs j
    LEFT JOIN job_executions e ON e.job_id=j.id
    LEFT JOIN activity_execution.workflow_runs r ON r.id=e.run_id
    WHERE r.status IS NULL OR r.status NOT IN ('completed', 'cancelled')
    ORDER BY j.id`).all().map(row => row.id);
}

export class ActivityReads {
  constructor(private readonly db: Database) {}

  jobCommands() {
    return this.db.query<{ job_id: number; kind: "retry" | "cancel" }, []>(`SELECT c.job_id,c.kind
      FROM job_commands c JOIN jobs j ON j.id=c.job_id AND j.execution_generation=c.generation
      WHERE c.kind IN ('retry','cancel')`).all();
  }

  newerJobs(ids: number[]) {
    if (!ids.length) return new Set<number>();
    return new Set(this.db.query<{ id: number }, [string]>(`SELECT j.id
      FROM json_each(?) ids JOIN jobs j ON j.id=ids.value
      WHERE EXISTS (SELECT 1 FROM jobs n WHERE n.id>j.id AND n.type=j.type
        AND n.book_id IS j.book_id AND n.release_id IS j.release_id)`).all(JSON.stringify(ids)).map(row => row.id));
  }

  bookJobs(jobIds: number[]) {
    if (!jobIds.length) return new Map();
    const rows = this.db.query<{
      job_id: number; title: string | null; media_type: string | null; audio_ready: number; ebook_ready: number;
    }, [string]>(`SELECT j.id AS job_id,b.title,r.media_type,
      EXISTS(SELECT 1 FROM assets a JOIN manifestations m ON m.id=a.manifestation_id AND m.book_id=b.id
        WHERE a.book_id=b.id AND m.kind='audio' AND m.preferred_score>=0) AS audio_ready,
      EXISTS(SELECT 1 FROM assets a LEFT JOIN manifestations m ON m.id=a.manifestation_id
        WHERE a.book_id=b.id AND COALESCE(m.preferred_score,0)>=0
          AND a.mime IN ('application/epub+zip','application/pdf')) AS ebook_ready
      FROM json_each(?) ids JOIN jobs j ON j.id=ids.value
      LEFT JOIN books b ON b.id=j.book_id LEFT JOIN releases r ON r.id=j.release_id`).all(JSON.stringify(jobIds));
    return new Map(rows.map(row => [row.job_id, row]));
  }

  movieAcquisitions(jobIds: number[]) {
    return this.db.query<MovieActivity, [string]>(`SELECT a.id,a.job_id,a.target_id,a.finished_at,
      NULL AS decision_json, json_extract(m.metadata_json,'$.title') AS title, m.tmdb_id,
      t.active_version_id IS NOT NULL AS available, a.id=latest.id AS latest,
      EXISTS(SELECT 1 FROM movie_requests r WHERE r.target_id=t.id AND r.withdrawn_at IS NULL) AS active_request,
      COALESCE(rr.desired_presence='evicted',0) AS evicted,
      (SELECT e.stage FROM movie_events e WHERE e.target_id=t.id AND e.acquisition_id=a.id ORDER BY e.id DESC LIMIT 1) AS stage,
      (SELECT e.stage FROM movie_events e WHERE e.target_id=t.id ORDER BY e.id DESC LIMIT 1) AS target_stage
      FROM movie_targets t JOIN movie_acquisitions latest ON latest.id=(
        SELECT id FROM movie_acquisitions WHERE target_id=t.id ORDER BY id DESC LIMIT 1
      )
      JOIN movie_acquisitions a ON a.target_id=t.id
      JOIN movies m ON m.id=t.movie_id
      LEFT JOIN retention_resources rr ON rr.namespace='movie-target' AND rr.resource_id=CAST(t.id AS TEXT)
      WHERE a.job_id IN (SELECT value FROM json_each(?))
        OR (a.id=latest.id AND a.job_id IS NOT NULL AND a.finished_at IS NOT NULL AND t.active_version_id IS NULL)
      ORDER BY t.id,a.id DESC`).all(JSON.stringify(jobIds));
  }

  tvAcquisitions(jobIds: number[]) {
    return this.db.query<TvActivity, [string]>(`SELECT a.id,a.job_id,a.target_id,a.finished_at,a.decision_json,
      json_extract(e.metadata_json,'$.title') AS title, t.entry_id, r.user_id AS request_user_id, r.id AS request_id,
      a.id=latest.id AS latest,
      EXISTS(SELECT 1 FROM tv_requests live WHERE live.target_id=t.id AND live.withdrawn_at IS NULL) AS active_request,
      EXISTS(SELECT 1 FROM tv_active_episodes active WHERE active.entry_id=t.entry_id) AS available,
      COALESCE(rr.desired_presence='evicted',0) AS evicted, v.acquisition_id AS publishing_acquisition,
      (SELECT evt.stage FROM tv_events evt WHERE evt.target_id=t.id AND evt.acquisition_id=a.id ORDER BY evt.id DESC LIMIT 1) AS stage,
      (SELECT json_group_array(DISTINCT json_extract(ep.metadata_json,'$.airDate'))
        FROM tv_requests live JOIN tv_request_episodes re ON re.request_id=live.id JOIN tv_episodes ep ON ep.id=re.episode_id
        WHERE live.target_id=t.id AND live.withdrawn_at IS NULL AND NOT EXISTS (
          SELECT 1 FROM tv_active_episodes active WHERE active.target_id=t.id AND active.episode_id=re.episode_id
        )) AS missing_dates
      FROM tv_targets t JOIN tv_acquisitions latest ON latest.id=(
        SELECT id FROM tv_acquisitions WHERE target_id=t.id ORDER BY id DESC LIMIT 1
      )
      JOIN tv_acquisitions a ON a.target_id=t.id
      JOIN tv_entries e ON e.id=t.entry_id
      LEFT JOIN tv_requests r ON r.id=a.request_id AND r.target_id=t.id
      LEFT JOIN tv_publications p ON p.target_id=t.id
      LEFT JOIN tv_versions v ON v.id=p.version_id
      LEFT JOIN retention_resources rr ON rr.namespace='tv-target' AND rr.resource_id=CAST(t.id AS TEXT)
      WHERE EXISTS (SELECT 1 FROM tv_requests WHERE target_id=t.id)
        AND (a.job_id IN (SELECT value FROM json_each(?))
          OR (a.id=latest.id AND a.job_id IS NOT NULL AND a.finished_at IS NOT NULL))
      ORDER BY t.id,a.id DESC`).all(JSON.stringify(jobIds));
  }

  acquisitionTorrents(media: "movie" | "tv", jobIds: number[]) {
    if (!jobIds.length) return [];
    return this.db.query<{ id: number; client_id: string; native_id: string; location_json: string; job_id: number }, [string]>(
      `SELECT t.id,t.client_id,t.native_id,t.location_json,a.job_id
      FROM ${media}_acquisitions a JOIN ${media}_acquisition_downloads d ON d.acquisition_id=a.id
      JOIN media_torrents t ON t.id=d.torrent_id
      WHERE a.job_id IN (SELECT value FROM json_each(?)) ORDER BY a.id,t.id DESC`).all(JSON.stringify(jobIds));
  }

  tvTitle(entryId: number) {
    return this.db.query<{ title: string }, [number]>(
      "SELECT json_extract(metadata_json,'$.title') AS title FROM tv_entries WHERE id=?"
    ).get(entryId)?.title ?? null;
  }

  tvBatchScopes(batches: { jobId: number; entryId: number; episodeIds: number[] }[]) {
    const result = new Map<number, string>();
    if (!batches.length) return result;
    const rows = this.db.query<{ jobId: number; count: number; seasons: string }, [string]>(`SELECT
      json_extract(b.value,'$.jobId') AS jobId, COUNT(*) AS count, json_group_array(DISTINCT n.season) AS seasons
      FROM json_each(?) b, json_each(b.value,'$.episodeIds') ids
      JOIN tv_entry_episodes e ON e.episode_id=ids.value AND e.entry_id=json_extract(b.value,'$.entryId')
      LEFT JOIN tv_episode_numbering n ON n.entry_id=e.entry_id AND n.episode_id=e.episode_id AND n.scheme='tvdb:aired'
      GROUP BY jobId`).all(JSON.stringify(batches.map(batch => ({ ...batch, episodeIds: [...new Set(batch.episodeIds)] }))));
    for (const row of rows) {
      const seasons = (JSON.parse(row.seasons) as (number | null)[]).filter(season => season !== null).sort((a, b) => a - b);
      const label = seasons.length === 1 ? seasons[0] === 0 ? "Specials" : `Season ${seasons[0]}`
        : seasons.length ? `Seasons ${seasons.join(", ")}` : "Episode batch";
      result.set(row.jobId, `${label} · ${row.count} ${row.count === 1 ? "episode" : "episodes"}`);
    }
    return result;
  }
}
