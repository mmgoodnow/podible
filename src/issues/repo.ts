import type { Database, SQLQueryBindings } from "bun:sqlite";
import type { JobRow } from "../app-types";
import { immediateTransaction } from "../db-transaction";
import { screenIssueAssessmentSchema, screenIssueEvidenceSchema, type CreateScreenIssue, type ListScreenIssues,
  type ScreenIssue, type ScreenIssueAssessment, type ScreenIssueFinding, type ScreenIssueInboxPage, type ScreenIssuePage } from "./contracts";

type SubjectRow = { id: number; title: string; visible: 0 | 1 };
type ReportRow = {
  id: number; tmdbId: number | null; entryId: number | null; episodeId: number | null;
  title: string; episodeTitle: string | null; category: ScreenIssue["category"];
  description: string; finding: ScreenIssueFinding; createdAt: string;
  assessmentJson?: string | null;
};
const assessmentPrefix = "screen-issue-assessment:v1:";

function readAssessment(json: string | null | undefined): ScreenIssueAssessment | null {
  if (!json) return null;
  try { return screenIssueAssessmentSchema.safeParse(JSON.parse(json)).data ?? null; }
  catch { return null; }
}

function view(row: ReportRow): ScreenIssue {
  return {
    id: row.id, subject: row.entryId === null ? { media: "movie", tmdbId: row.tmdbId! }
      : { media: "tv", entryId: row.entryId, ...(row.episodeId === null ? {} : { episodeId: row.episodeId }) },
    title: row.title, episodeTitle: row.episodeTitle, category: row.category, description: row.description,
    status: "open", createdAt: row.createdAt, triage: { finding: row.finding, checkedAt: row.createdAt },
  };
}

export class ScreenIssuesRepo {
  constructor(private readonly db: Database, private readonly beforeWrite: () => void) {}

  create(userId: number, input: CreateScreenIssue, enqueueDiagnosis: (reportId: number) => JobRow): ScreenIssue | null {
    return immediateTransaction(this.db, this.beforeWrite, () => {
      const { subject, category, description } = input;
      const title = subject.media === "movie" ? this.movie(userId, subject.tmdbId) : this.show(userId, subject.entryId);
      if (!title?.visible) return null;
      const episodeId = subject.media === "tv" ? subject.episodeId ?? null : null;
      const episode = episodeId === null ? null : this.db.query<{ title: string }, [number, number]>(`
        SELECT json_extract(e.metadata_json,'$.title') AS title
        FROM tv_entry_episodes member JOIN tv_episodes e ON e.id=member.episode_id
        WHERE member.entry_id=? AND e.id=? AND e.episode_key NOT LIKE 'ephemeral:%'
          AND json_extract(e.metadata_json,'$.identity.stability')='provider'
          AND EXISTS (SELECT 1 FROM tv_episode_refs r WHERE r.episode_id=e.id)`
      ).get(title.id, episodeId);
      if (episodeId !== null && !episode) return null;

      const finding = this.triage(userId, subject.media, title.id, episodeId);
      const createdAt = new Date().toISOString();
      const { id } = this.db.query<{ id: number }, SQLQueryBindings[]>(`
        INSERT INTO screen_issues(user_id,movie_id,tv_entry_id,tv_episode_id,title,episode_title,category,description,triage_finding,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING id`
      ).get(userId, subject.media === "movie" ? title.id : null, subject.media === "tv" ? title.id : null,
        episodeId, title.title, episode?.title ?? null, category, description, finding, createdAt)!;
      enqueueDiagnosis(id);
      return { id, subject, title: title.title, episodeTitle: episode?.title ?? null, category, description,
        status: "open", createdAt, triage: { finding, checkedAt: createdAt } };
    });
  }

  listMine(userId: number, { title, beforeId, limit }: ListScreenIssues): ScreenIssuePage {
    const filters = ["i.user_id=?"];
    const bindings: SQLQueryBindings[] = [userId];
    if (title) {
      filters.push(title.media === "movie" ? "i.movie_id=(SELECT id FROM movies WHERE tmdb_id=?)" : "i.tv_entry_id=?");
      bindings.push(title.media === "movie" ? title.tmdbId : title.entryId);
    }
    const { rows, nextBeforeId } = this.page(filters, bindings, beforeId, limit, false);
    return { reports: rows.map(view), nextBeforeId };
  }

  inbox({ beforeId, limit }: Omit<ListScreenIssues, "title">): ScreenIssueInboxPage {
    const { rows, nextBeforeId } = this.page([], [], beforeId, limit, true);
    return { reports: rows.map(row => ({ ...view(row), assessment: readAssessment(row.assessmentJson) })), nextBeforeId };
  }

  private page(filters: string[], bindings: SQLQueryBindings[], beforeId: number | undefined, limit: number, inbox: boolean) {
    if (beforeId !== undefined) { filters.push("i.id<?"); bindings.push(beforeId); }
    const where = filters.length ? ` WHERE ${filters.join(" AND ")}` : "";
    const rows = this.db.query<ReportRow, SQLQueryBindings[]>(`
      SELECT i.id,m.tmdb_id AS tmdbId,i.tv_entry_id AS entryId,i.tv_episode_id AS episodeId,
        i.title,i.episode_title AS episodeTitle,i.category,i.description,i.triage_finding AS finding,i.created_at AS createdAt
        ${inbox ? ",s.value_json AS assessmentJson" : ""}
      FROM screen_issues i LEFT JOIN movies m ON m.id=i.movie_id
      ${inbox ? `LEFT JOIN app_state s ON s.key='${assessmentPrefix}' || i.id` : ""}
      ${where} ORDER BY i.id DESC LIMIT ?`)
      .all(...bindings, limit + 1);
    return { rows: rows.slice(0, limit), nextBeforeId: rows.length > limit ? rows[limit - 1]!.id : null };
  }

  diagnosisInput(reportId: number) {
    return this.db.transaction(() => {
      const row = this.db.query<{
        userId: number; movieId: number | null; entryId: number | null; episodeId: number | null;
        category: ScreenIssue["category"]; filedFinding: ScreenIssueFinding; assessmentJson: string | null;
      }, [string, number]>(`SELECT i.user_id AS userId,i.movie_id AS movieId,i.tv_entry_id AS entryId,
        i.tv_episode_id AS episodeId,i.category,i.triage_finding AS filedFinding,s.value_json AS assessmentJson
        FROM screen_issues i LEFT JOIN app_state s ON s.key=? WHERE i.id=?`).get(`${assessmentPrefix}${reportId}`, reportId);
      if (!row) return null;
      const saved = readAssessment(row.assessmentJson);
      if (saved) return { saved } as const;
      const media = row.movieId === null ? "tv" : "movie";
      const currentFinding = this.triage(row.userId, media, (row.movieId ?? row.entryId)!, row.episodeId);
      return { evidence: screenIssueEvidenceSchema.parse({
        scope: media === "movie" ? "movie" : row.episodeId === null ? "tv-show" : "tv-episode",
        category: row.category, filedFinding: row.filedFinding, currentFinding,
      }), checkedAt: new Date().toISOString() } as const;
    })();
  }

  saveAssessment(reportId: number, assessment: ScreenIssueAssessment): ScreenIssueAssessment | null {
    const key = `${assessmentPrefix}${reportId}`;
    return immediateTransaction(this.db, this.beforeWrite, () => {
      if (!this.db.query("SELECT 1 FROM screen_issues WHERE id=?").get(reportId)) return null;
      const saved = readAssessment(this.db.query<{ value_json: string }, [string]>(
        "SELECT value_json FROM app_state WHERE key=?").get(key)?.value_json);
      if (saved) return saved;
      this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
        .run(key, JSON.stringify(screenIssueAssessmentSchema.parse(assessment)), new Date().toISOString());
      return assessment;
    });
  }

  private movie(userId: number, tmdbId: number) {
    return this.db.query<SubjectRow, [number, number]>(`SELECT m.id,json_extract(m.metadata_json,'$.title') AS title,
      EXISTS (SELECT 1 FROM movie_targets t WHERE t.movie_id=m.id AND (
        EXISTS (SELECT 1 FROM movie_versions v WHERE v.id=t.active_version_id AND v.movie_id=m.id)
        OR EXISTS (SELECT 1 FROM movie_requests r WHERE r.target_id=t.id AND r.user_id=? AND r.withdrawn_at IS NULL))) AS visible
      FROM movies m WHERE m.tmdb_id=?`).get(userId, tmdbId);
  }

  private show(userId: number, entryId: number) {
    return this.db.query<SubjectRow, [number, number]>(`SELECT e.id,json_extract(e.metadata_json,'$.title') AS title,
      EXISTS (SELECT 1 FROM tv_targets t WHERE t.entry_id=e.id AND (
        EXISTS (SELECT 1 FROM tv_active_episodes a WHERE a.target_id=t.id)
        OR EXISTS (SELECT 1 FROM tv_requests r WHERE r.target_id=t.id AND r.user_id=? AND r.withdrawn_at IS NULL))) AS visible
      FROM tv_entries e WHERE e.id=? AND EXISTS (SELECT 1 FROM tv_entry_refs r WHERE r.entry_id=e.id)`).get(userId, entryId);
  }

  // Read only the committed publication projection, never Plex, the filesystem, or the report text.
  private triage(userId: number, media: "movie" | "tv", titleId: number, episodeId: number | null): ScreenIssueFinding {
    const published = media === "movie"
      ? this.db.query(`SELECT 1 FROM movie_targets t JOIN movie_versions v ON v.id=t.active_version_id AND v.movie_id=t.movie_id
          WHERE t.movie_id=? LIMIT 1`).get(titleId)
      : this.db.query(`SELECT 1 FROM tv_active_episodes a WHERE a.entry_id=? AND (? IS NULL OR a.episode_id=?) LIMIT 1`)
          .get(titleId, episodeId, episodeId);
    if (published) return "publication-recorded";

    // Loss recovery clears active selection. Consult historical markers only for the reporter's own active request.
    const lost = media === "movie"
      ? this.db.query(`SELECT 1 FROM movie_targets t JOIN media_content_losses l ON l.namespace='movie-target' AND l.target_id=t.id
          WHERE t.movie_id=? AND EXISTS (SELECT 1 FROM movie_requests r WHERE r.target_id=t.id AND r.user_id=? AND r.withdrawn_at IS NULL)
          LIMIT 1`).get(titleId, userId)
      : this.db.query(`SELECT 1 FROM tv_targets t JOIN media_content_losses l ON l.namespace='tv-target' AND l.target_id=t.id
          WHERE t.entry_id=? AND EXISTS (
            SELECT 1 FROM tv_requests r JOIN tv_request_episodes ep ON ep.request_id=r.id AND ep.entry_id=t.entry_id
            WHERE r.target_id=t.id AND r.user_id=? AND r.withdrawn_at IS NULL AND (? IS NULL OR ep.episode_id=?)
              AND EXISTS (SELECT 1 FROM tv_versions v JOIN tv_version_file_episodes f ON f.version_id=v.id
                WHERE v.target_id=t.id AND f.file_id=l.file_id AND f.episode_id=ep.episode_id))
          LIMIT 1`).get(titleId, userId, episodeId, episodeId);
    return lost ? "content-loss-recorded" : "no-publication-recorded";
  }
}
