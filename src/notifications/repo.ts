import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { immediateTransaction } from "../db-transaction";
import { NotificationOutbox, type EmailNotice } from "./outbox";

export type WatchTitle = { kind: "movie" | "tv"; tmdbId: number };
type Watch = { userId: number; tv?: { entryId: number; episodeIds: number[] } };
export type WatchStatus = "unavailable" | "eligible" | "watching";
type WatchUpdate = { key: string; watch: Watch };
type StateRow = { key: string; value_json: string; updated_at: string };
const watchPrefix = ({ kind, tmdbId }: WatchTitle) => `notifications:watch:v1:${kind}:${tmdbId}:`;

function prepareNotice(userId: number, identity: string, content: Pick<EmailNotice, "title" | "episode">): EmailNotice {
  const id = createHash("sha256").update(`${userId}:${identity}`).digest("hex");
  return { id, userId, ...content };
}

export class NotificationsRepo {
  constructor(private db: Database, private beforeWrite: () => void) {}

  get delivery() { return new NotificationOutbox(this.db, this.beforeWrite); }

  private row(key: string) {
    return this.db.query<StateRow, [string]>("SELECT key,value_json,updated_at FROM app_state WHERE key=?").get(key);
  }

  private remove(key: string) { this.db.query("DELETE FROM app_state WHERE key=?").run(key); }

  watchStatus(userId: number, title: WatchTitle): WatchStatus {
    if (this.row(watchPrefix(title) + userId)) return "watching";
    return this.eligibleWatch(userId, title) ? "eligible" : "unavailable";
  }

  setWatch(userId: number, title: WatchTitle, enabled: boolean): WatchStatus {
    return immediateTransaction(this.db, this.beforeWrite, () => {
      const key = watchPrefix(title) + userId;
      if (!enabled) {
        this.remove(key);
        return this.watchStatus(userId, title);
      }
      if (this.row(key)) return "watching";
      const watch = this.eligibleWatch(userId, title);
      if (!watch) return "unavailable";
      this.db.query("INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)")
        .run(key, JSON.stringify(watch), new Date().toISOString());
      return "watching";
    });
  }

  private eligibleWatch(userId: number, { kind, tmdbId }: WatchTitle): Watch | null {
    if (kind === "movie") {
      const pending = this.db.query(`SELECT 1 FROM movies m
        WHERE m.tmdb_id=? AND NOT EXISTS (SELECT 1 FROM movie_targets t WHERE t.movie_id=m.id AND t.active_version_id IS NOT NULL)
        AND EXISTS (SELECT 1 FROM movie_targets t JOIN movie_requests r ON r.target_id=t.id
          LEFT JOIN retention_resources p ON p.namespace='movie-target' AND p.resource_id=CAST(t.id AS TEXT)
          WHERE t.movie_id=m.id AND r.withdrawn_at IS NULL AND p.desired_presence IS NOT 'evicted')`).get(tmdbId);
      return pending ? { userId } : null;
    }
    const entries = this.db.query<{ id: number }, [number]>(`SELECT id FROM tv_entries
      WHERE json_extract(metadata_json,'$.tmdbId')=? LIMIT 2`).all(tmdbId);
    if (entries.length !== 1) return null;
    const entryId = entries[0]!.id;
    const episodes = this.db.query<{ id: number }, [number]>(`SELECT DISTINCT m.episode_id AS id
      FROM tv_targets t JOIN tv_requests r ON r.target_id=t.id JOIN tv_request_episodes m ON m.request_id=r.id AND m.entry_id=t.entry_id
      JOIN tv_episode_numbering n ON n.entry_id=m.entry_id AND n.episode_id=m.episode_id AND n.scheme='tvdb:aired'
      LEFT JOIN retention_resources p ON p.namespace='tv-target' AND p.resource_id=CAST(t.id AS TEXT)
      WHERE t.entry_id=? AND r.withdrawn_at IS NULL AND p.desired_presence IS NOT 'evicted'
        AND n.season IS NOT NULL AND n.episode IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM tv_active_episodes a WHERE a.entry_id=m.entry_id AND a.episode_id=m.episode_id)
      ORDER BY m.episode_id`).all(entryId);
    return episodes.length ? { userId, tv: { entryId, episodeIds: episodes.map(row => row.id) } } : null;
  }

  private watches(title: WatchTitle) {
    const prefix = watchPrefix(title);
    return this.db.query<StateRow, [string, string]>(`SELECT s.key,s.value_json,s.updated_at FROM app_state s
      JOIN users u ON u.id=json_extract(s.value_json,'$.userId') WHERE s.key>=? AND s.key<?`).all(prefix, prefix + "~")
      .map(row => ({ key: row.key, watch: JSON.parse(row.value_json) as Watch }));
  }

  private consumeWatches(remove: string[], updates: WatchUpdate[], now: string) {
    if (remove.length) this.db.query("DELETE FROM app_state WHERE key IN (SELECT value FROM json_each(?))")
      .run(JSON.stringify(remove));
    if (updates.length) this.db.query(`UPDATE app_state AS s SET value_json=json_extract(changes.value,'$.watch'),updated_at=?
      FROM json_each(?) AS changes WHERE s.key=json_extract(changes.value,'$.key')`).run(now, JSON.stringify(updates));
  }

  /** Called inside the Plex-verified promotion transaction when this target was unavailable. */
  moviePublished(targetId: number, revision: number) {
    const movie = this.db.query<{ tmdbId: number; title: string }, [number]>(`SELECT m.tmdb_id AS tmdbId,
      json_extract(m.metadata_json,'$.title') AS title FROM movie_targets t JOIN movies m ON m.id=t.movie_id WHERE t.id=?`).get(targetId)!;
    const owners = this.db.query<{ userId: number }, [number]>(`SELECT user_id AS userId FROM movie_requests r
      WHERE target_id=? AND withdrawn_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM movie_request_origins o WHERE o.request_id=r.id)`).all(targetId);
    const watches = this.watches({ kind: "movie", tmdbId: movie.tmdbId });
    const users = new Set([...owners.map(o => o.userId), ...watches.map(w => w.watch.userId)]);
    const now = new Date().toISOString();
    const content = { title: movie.title, episode: null };
    const identity = `movie:${targetId}:${revision}`;
    const notices = [...users].map(userId => prepareNotice(userId, identity, content));
    this.delivery.enqueue(identity, { title: "Movie available in Plex", message: movie.title }, notices, now);
    this.consumeWatches(watches.map(w => w.key), [], now);
  }

  /** Match newly verified episodes to exact request membership and to explicit, entry-scoped watches. */
  tvPublished(targetId: number, revision: number, episodeIds: number[]) {
    if (!episodeIds.length) return;
    const ids = JSON.stringify(episodeIds);
    const entry = this.db.query<{ id: number; tmdbId: number | null; title: string }, [number]>(`SELECT e.id,
      json_extract(e.metadata_json,'$.tmdbId') AS tmdbId,json_extract(e.metadata_json,'$.title') AS title
      FROM tv_targets t JOIN tv_entries e ON e.id=t.entry_id WHERE t.id=?`).get(targetId)!;
    const episodes = this.db.query<{ id: number; title: string; season: number; number: number }, [string, number]>(`
      SELECT e.id,json_extract(e.metadata_json,'$.title') AS title,n.season,n.episode AS number
      FROM tv_episode_numbering n JOIN tv_episodes e ON e.id=n.episode_id JOIN json_each(?) ids ON ids.value=e.id
      WHERE n.entry_id=? AND n.scheme='tvdb:aired' AND n.season IS NOT NULL AND n.episode IS NOT NULL
    `).all(ids, entry.id);
    const owners = this.db.query<{ userId: number; episodeId: number }, [number, string]>(`SELECT DISTINCT r.user_id AS userId,m.episode_id AS episodeId
      FROM tv_requests r JOIN tv_request_episodes m ON m.request_id=r.id
      WHERE r.target_id=? AND r.withdrawn_at IS NULL AND r.request_key GLOB 'native-seasons:*'
      AND m.episode_id IN (SELECT value FROM json_each(?))`).all(targetId, ids);
    const byId = new Map(episodes.map(ep => {
      const code = `S${String(ep.season).padStart(2, "0")}E${String(ep.number).padStart(2, "0")}`;
      return [ep.id, {
        title: entry.title, episode: ep.title ? `${code}: ${ep.title}` : code,
      }] as const;
    }));
    const now = new Date().toISOString();
    const notices = new Map<string, EmailNotice>();
    const queue = (userId: number, episodeId: number) => {
      const content = byId.get(episodeId);
      if (!content) return false;
      const recipient = `${userId}:${episodeId}`;
      if (!notices.has(recipient)) notices.set(recipient, prepareNotice(userId, `tv:${targetId}:${revision}:${episodeId}`, content));
      return true;
    };
    const remove: string[] = [], updates: WatchUpdate[] = [];
    for (const owner of owners) queue(owner.userId, owner.episodeId);
    for (const { key, watch } of entry.tmdbId ? this.watches({ kind: "tv", tmdbId: entry.tmdbId }) : []) {
      if (watch.tv?.entryId !== entry.id) continue;
      const remaining = watch.tv.episodeIds.filter(id => !queue(watch.userId, id));
      if (!remaining.length) remove.push(key);
      else if (remaining.length < watch.tv.episodeIds.length) updates.push({ key, watch: { ...watch, tv: { entryId: entry.id, episodeIds: remaining } } });
    }
    if (byId.size) {
      const episodes = [...byId.values()].map(ep => ep.episode).sort();
      const listed = episodes.slice(0, 5).join(", ");
      const more = episodes.length > 5 ? ` (+${episodes.length - 5} more)` : "";
      this.delivery.enqueue(`tv:${targetId}:${revision}`, {
        title: "TV available in Plex", message: `${entry.title}: ${listed}${more}`,
      }, [...notices.values()], now);
    }
    this.consumeWatches(remove, updates, now);
  }
}
