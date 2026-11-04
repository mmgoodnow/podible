import type { Database } from "bun:sqlite";
import { z } from "zod";

const ids = z.array(z.number().int().positive().safe()).max(50);
const inputSchema = z.object({ books: ids, screen: ids }).strict().refine(input => input.books.length + input.screen.length <= 50);
export type StewardshipTitleInput = z.input<typeof inputSchema>;
export type StewardshipTitle = { title: string; href: string };
export type StewardshipTitleRow = { source: "books" | "screen"; id: number; titles: StewardshipTitle[]; truncated: boolean };

/** At most two metadata queries per status page; never hydrates catalog metadata or reads providers. */
export function readStewardshipTitles(db: Database, input: StewardshipTitleInput): StewardshipTitleRow[] {
  const parsed = inputSchema.parse(input), result: StewardshipTitleRow[] = [];
  const books = [...new Set(parsed.books)], screen = [...new Set(parsed.screen)];
  if (books.length) {
    const rows = db.query(`SELECT id,title FROM books WHERE id IN (${books.map(() => "?").join(",")}) LIMIT 50`).all(...books) as { id: number; title: string }[];
    for (const row of rows) if (row.title.trim()) result.push({ source: "books", id: row.id, titles: [{ title: row.title, href: `/book/${row.id}` }], truncated: false });
  }
  if (screen.length) {
    const slots = screen.map(() => "?").join(",");
    const rows = db.query(`WITH candidates AS (
      SELECT DISTINCT d.torrent_id AS id,'movies' AS media,t.id AS target_id,m.tmdb_id AS tmdb_id,NULL AS entry_id,
        CASE WHEN json_valid(m.metadata_json) THEN json_extract(m.metadata_json,'$.title') END AS title,1 AS published
      FROM movie_acquisition_downloads d JOIN movie_acquisitions a ON a.id=d.acquisition_id
      JOIN movie_targets t ON t.id=a.target_id JOIN movies m ON m.id=t.movie_id JOIN media_torrents mt ON mt.id=d.torrent_id
      WHERE d.torrent_id IN (${slots}) AND CASE WHEN json_valid(mt.location_json) THEN json_extract(mt.location_json,'$.rootId') END='movies'
      UNION
      SELECT DISTINCT d.torrent_id AS id,'tv' AS media,t.id AS target_id,
        CASE WHEN json_valid(e.metadata_json) THEN json_extract(e.metadata_json,'$.tmdbId') END AS tmdb_id,e.id AS entry_id,
        CASE WHEN json_valid(e.metadata_json) THEN json_extract(e.metadata_json,'$.title') END AS title,
        EXISTS(SELECT 1 FROM tv_active_episodes active WHERE active.target_id=t.id) AS published
      FROM tv_acquisition_downloads d JOIN tv_acquisitions a ON a.id=d.acquisition_id
      JOIN tv_targets t ON t.id=a.target_id JOIN tv_entries e ON e.id=t.entry_id JOIN media_torrents mt ON mt.id=d.torrent_id
      WHERE d.torrent_id IN (${slots}) AND CASE WHEN json_valid(mt.location_json) THEN json_extract(mt.location_json,'$.rootId') END='tv'
    ), bounded AS (SELECT *,row_number() OVER(PARTITION BY id ORDER BY media,target_id) AS n FROM candidates)
    SELECT * FROM bounded WHERE n<=4 ORDER BY id,n LIMIT 200`).all(...screen, ...screen) as {
      id: number; media: string; tmdb_id: number | null; entry_id: number | null; title: unknown; published: number; n: number;
    }[];
    const grouped = new Map<number, StewardshipTitleRow>();
    for (const row of rows) {
      const group = grouped.get(row.id) ?? { source: "screen" as const, id: row.id, titles: [], truncated: false };
      grouped.set(row.id, group);
      if (row.n > 3) { group.truncated = true; continue; }
      if (typeof row.title !== "string" || !row.title.trim()) continue;
      const href = row.media === "movies" && Number.isSafeInteger(row.tmdb_id) && row.tmdb_id! > 0 ? `/movies/${row.tmdb_id}`
        : row.media === "tv" && row.published ? `/tv/library/${row.entry_id}`
          : row.media === "tv" && Number.isSafeInteger(row.tmdb_id) && row.tmdb_id! > 0 ? `/tv/${row.tmdb_id}`
            : `/library?media=tv&q=${encodeURIComponent(row.title)}`;
      if (!group.titles.some(title => title.href === href)) group.titles.push({ title: row.title, href });
    }
    result.push(...grouped.values());
  }
  return result;
}
