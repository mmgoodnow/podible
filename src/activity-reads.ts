import { Database, constants } from "bun:sqlite";
import { pathToFileURL } from "node:url";
import type { JobRow } from "./app-types";

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

  hasNewerJob(job: JobRow): boolean {
    // The latest job supersedes an older incident regardless of its lifecycle.
    return this.db.query(`SELECT 1 FROM jobs
      WHERE id>? AND type=? AND book_id IS ? AND release_id IS ? LIMIT 1`)
      .get(job.id, job.type, job.book_id, job.release_id) !== null;
  }

  tvTitle(entryId: number) {
    return this.db.query<{ title: string }, [number]>(
      "SELECT json_extract(metadata_json,'$.title') AS title FROM tv_entries WHERE id=?"
    ).get(entryId)?.title ?? null;
  }

  tvMissingAirDates(targetId: number): (string | null)[] {
    return this.db.query<{ airDate: string | null }, [number]>(`SELECT DISTINCT
      json_extract(e.metadata_json,'$.airDate') AS airDate
      FROM tv_requests r
      JOIN tv_request_episodes re ON re.request_id=r.id
      JOIN tv_episodes e ON e.id=re.episode_id
      WHERE r.target_id=? AND r.withdrawn_at IS NULL AND NOT EXISTS (
        SELECT 1 FROM tv_active_episodes a WHERE a.target_id=r.target_id AND a.episode_id=re.episode_id
      )`).all(targetId).map(row => row.airDate);
  }

  tvBatchScope(entryId: number, episodeIds: number[]) {
    const batch = this.db.query<{ season: number | null }, [string, number]>(`SELECT n.season
      FROM json_each(?) ids
      JOIN tv_entry_episodes e ON e.episode_id=ids.value AND e.entry_id=?
      LEFT JOIN tv_episode_numbering n ON n.entry_id=e.entry_id AND n.episode_id=e.episode_id AND n.scheme='tvdb:aired'
    `).all(JSON.stringify([...new Set(episodeIds)]), entryId);
    const seasons = [...new Set(batch.flatMap(row => row.season === null ? [] : [row.season]))].sort((a, b) => a - b);
    return batch.length ? `${seasons.length === 1 ? seasons[0] === 0 ? "Specials" : `Season ${seasons[0]}` : seasons.length ? `Seasons ${seasons.join(", ")}` : "Episode batch"} · ${batch.length} ${batch.length === 1 ? "episode" : "episodes"}` : null;
  }
}
