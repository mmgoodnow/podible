export const SCREEN_ISSUES_SCHEMA_SQL = `
CREATE TABLE screen_issues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  movie_id INTEGER REFERENCES movies(id),
  tv_entry_id INTEGER REFERENCES tv_entries(id),
  tv_episode_id INTEGER,
  title TEXT NOT NULL,
  episode_title TEXT,
  category TEXT NOT NULL CHECK(category IN ('playback','audio-subtitles','wrong-content','missing','other')),
  description TEXT NOT NULL CHECK(length(description) BETWEEN 1 AND 1000),
  triage_finding TEXT NOT NULL CHECK(triage_finding IN ('content-loss-recorded','publication-recorded','no-publication-recorded')),
  created_at TEXT NOT NULL,
  FOREIGN KEY(tv_entry_id,tv_episode_id) REFERENCES tv_entry_episodes(entry_id,episode_id),
  CHECK((movie_id IS NOT NULL AND tv_entry_id IS NULL AND tv_episode_id IS NULL)
    OR (movie_id IS NULL AND tv_entry_id IS NOT NULL)),
  CHECK((tv_episode_id IS NULL) = (episode_title IS NULL))
);
CREATE INDEX screen_issues_by_user ON screen_issues(user_id,id DESC);
CREATE INDEX screen_issues_by_movie ON screen_issues(user_id,movie_id,id DESC) WHERE movie_id IS NOT NULL;
CREATE INDEX screen_issues_by_tv ON screen_issues(user_id,tv_entry_id,id DESC) WHERE tv_entry_id IS NOT NULL;
`;
