export const RELEASE_INBOX_SCHEMA_SQL = `
CREATE TABLE indexer_feeds (
  id TEXT PRIMARY KEY NOT NULL,
  provider TEXT NOT NULL,
  media_type TEXT NOT NULL,
  head_key TEXT,
  last_polled_at TEXT,
  last_success_at TEXT,
  last_error TEXT,
  retry_at TEXT,
  gap_since TEXT,
  gap_note TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE release_inbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  feed_id TEXT NOT NULL REFERENCES indexer_feeds(id),
  release_key TEXT NOT NULL,
  content_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  UNIQUE(feed_id,release_key,content_key)
);
CREATE INDEX release_inbox_feed ON release_inbox(feed_id,id);
CREATE TABLE release_considerations (
  release_id INTEGER NOT NULL REFERENCES release_inbox(id),
  target_kind TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  target_revision TEXT NOT NULL,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  source_job_id INTEGER NOT NULL REFERENCES jobs(id),
  decision_note TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(release_id,target_kind,target_id,target_revision),
  UNIQUE(release_id,target_kind,target_id,source_job_id)
);
`;
