import type { Database } from "bun:sqlite";
import { z } from "zod";
import { tvPosterUrlSchema } from "../tv/types";

const id = z.number().int().positive();
const card = z.object({ title: z.string(), posterUrl: z.string().nullable(), publishedAt: z.string() });
export const homeRecentSchema = z.object({
  books: z.array(card.extend({ id, author: z.string() })),
  movies: z.array(card.extend({ tmdbId: id, year: z.number().nullable(), posterUrl: tvPosterUrlSchema })),
  tv: z.array(card.extend({ entryId: id, year: z.number().nullable(), posterUrl: tvPosterUrlSchema })),
  episodes: z.array(z.object({ entryId: id, episodeId: id, title: z.string(), episodeTitle: z.string(),
    posterUrl: tvPosterUrlSchema, airDate: z.string(), season: z.number().nullable(), episode: z.number().nullable() })),
});
export type HomeRecentInput = { limit?: number; now?: string };

/** Four bounded read-only projections. Publication time is never inferred from row IDs or metadata updates. */
export function readHomeRecent(db: Database, input: HomeRecentInput = {}): z.infer<typeof homeRecentSchema> {
  const limit = z.number().int().min(1).max(20).parse(input.limit ?? 12);
  const now = z.iso.datetime().parse(input.now ?? new Date().toISOString());
  const books = db.query(`SELECT b.id,b.title,b.author,
      CASE WHEN b.cover_path IS NOT NULL THEN '/covers/'||b.id||'.jpg?v='||b.updated_at END AS posterUrl,
      MIN(a.created_at) AS publishedAt
    FROM books b JOIN assets a ON a.book_id=b.id
    WHERE julianday(a.created_at)<=julianday(?) GROUP BY b.id
    ORDER BY julianday(publishedAt) DESC,b.id DESC LIMIT ?`).all(now, limit);
  const movies = db.query(`SELECT m.tmdb_id AS tmdbId,json_extract(m.metadata_json,'$.title') AS title,
      json_extract(m.metadata_json,'$.year') AS year,json_extract(m.metadata_json,'$.posterUrl') AS posterUrl,
      MIN(e.created_at) AS publishedAt
    FROM movies m JOIN movie_targets t ON t.movie_id=m.id JOIN movie_events e ON e.target_id=t.id
    WHERE t.active_version_id IS NOT NULL AND e.stage='available'
      AND e.event_key LIKE 'published:'||t.id||':%' AND julianday(e.created_at)<=julianday(?)
      AND EXISTS(SELECT 1 FROM movie_version_files vf WHERE vf.version_id=t.active_version_id)
      AND NOT EXISTS(SELECT 1 FROM movie_version_files vf JOIN media_content_losses l ON l.file_id=vf.file_id WHERE vf.version_id=t.active_version_id)
    GROUP BY m.id ORDER BY julianday(publishedAt) DESC,m.id DESC LIMIT ?`).all(now, limit);
  const tv = db.query(`SELECT entry.id AS entryId,json_extract(entry.metadata_json,'$.title') AS title,
      json_extract(entry.metadata_json,'$.year') AS year,json_extract(entry.metadata_json,'$.posterUrl') AS posterUrl,
      MIN(e.created_at) AS publishedAt
    FROM tv_entries entry JOIN tv_targets t ON t.entry_id=entry.id JOIN tv_events e ON e.target_id=t.id
    WHERE e.stage='available' AND e.event_key LIKE 'tv-published:'||t.id||':%' AND julianday(e.created_at)<=julianday(?)
      AND EXISTS(SELECT 1 FROM tv_active_episodes a JOIN tv_version_file_episodes f
        ON f.version_id=a.version_id AND f.episode_id=a.episode_id AND f.entry_id=a.entry_id
        WHERE a.target_id=t.id AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.file_id))
    GROUP BY entry.id ORDER BY julianday(publishedAt) DESC,entry.id DESC LIMIT ?`).all(now, limit);
  const episodes = db.query(`SELECT DISTINCT entry.id AS entryId,ep.id AS episodeId,
      json_extract(entry.metadata_json,'$.title') AS title,json_extract(entry.metadata_json,'$.posterUrl') AS posterUrl,
      json_extract(ep.metadata_json,'$.title') AS episodeTitle,json_extract(ep.metadata_json,'$.airDate') AS airDate,
      n.season,n.episode
    FROM tv_active_episodes a JOIN tv_entries entry ON entry.id=a.entry_id JOIN tv_episodes ep ON ep.id=a.episode_id
    JOIN tv_version_file_episodes f ON f.version_id=a.version_id AND f.episode_id=a.episode_id AND f.entry_id=a.entry_id
    LEFT JOIN tv_episode_numbering n ON n.entry_id=a.entry_id AND n.episode_id=a.episode_id AND n.scheme='tvdb:aired'
    WHERE NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.file_id)
      AND length(json_extract(ep.metadata_json,'$.airDate'))=10
      AND date(json_extract(ep.metadata_json,'$.airDate'),'+0 days')=json_extract(ep.metadata_json,'$.airDate')
      AND julianday(json_extract(ep.metadata_json,'$.airDate')) BETWEEN julianday(date(?,'-30 days')) AND julianday(date(?))
    ORDER BY airDate DESC,entryId,episodeId LIMIT ?`).all(now, now, limit);
  return homeRecentSchema.parse({ books, movies, tv, episodes });
}
