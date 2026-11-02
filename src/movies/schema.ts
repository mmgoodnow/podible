export const MOVIES_SCHEMA_SQL = `
CREATE TABLE movies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tmdb_id INTEGER NOT NULL UNIQUE,
  metadata_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE movie_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  movie_id INTEGER NOT NULL REFERENCES movies(id),
  policy_key TEXT NOT NULL,
  policy_json TEXT NOT NULL,
  active_version_id INTEGER REFERENCES movie_versions(id),
  revision INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(movie_id, policy_key)
);
CREATE TABLE movie_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  target_id INTEGER NOT NULL REFERENCES movie_targets(id),
  created_at TEXT NOT NULL,
  UNIQUE(user_id, target_id)
);
CREATE TABLE movie_acquisitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL REFERENCES movie_targets(id),
  reason TEXT NOT NULL CHECK(reason IN ('initial', 'upgrade', 'recovery')),
  job_id INTEGER UNIQUE REFERENCES jobs(id),
  decision_json TEXT,
  finished_at TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX movie_acquisitions_target ON movie_acquisitions(target_id, id);
CREATE TABLE movie_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  movie_id INTEGER NOT NULL REFERENCES movies(id),
  acquisition_id INTEGER NOT NULL UNIQUE REFERENCES movie_acquisitions(id),
  facts_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE managed_content_files (
  id TEXT PRIMARY KEY,
  relative_path TEXT NOT NULL UNIQUE,
  size_bytes INTEGER NOT NULL,
  probe_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE movie_version_files (
  version_id INTEGER NOT NULL REFERENCES movie_versions(id),
  file_id TEXT NOT NULL REFERENCES managed_content_files(id),
  sequence INTEGER NOT NULL,
  PRIMARY KEY(version_id, file_id), UNIQUE(version_id, sequence)
);
CREATE TABLE movie_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL REFERENCES movie_targets(id),
  acquisition_id INTEGER REFERENCES movie_acquisitions(id),
  event_key TEXT NOT NULL UNIQUE,
  stage TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX movie_events_target ON movie_events(target_id, id);
`;
