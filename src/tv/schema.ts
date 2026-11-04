// managed_content_files, users and jobs are shared application tables.
export const TV_SCHEMA_SQL = `
CREATE TABLE tv_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  catalog_key TEXT NOT NULL UNIQUE,
  metadata_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE tv_entry_refs (
  entry_id INTEGER NOT NULL REFERENCES tv_entries(id),
  provider TEXT NOT NULL,
  instance TEXT NOT NULL,
  external_id TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(entry_id,provider,instance,external_id)
);
CREATE INDEX tv_entry_refs_provider ON tv_entry_refs(provider,instance,external_id);
CREATE TABLE tv_episodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  episode_key TEXT NOT NULL UNIQUE,
  metadata_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE tv_entry_episodes (
  entry_id INTEGER NOT NULL REFERENCES tv_entries(id),
  episode_id INTEGER NOT NULL REFERENCES tv_episodes(id),
  PRIMARY KEY(entry_id,episode_id)
);
CREATE TABLE tv_episode_refs (
  episode_id INTEGER NOT NULL REFERENCES tv_episodes(id),
  provider TEXT NOT NULL,
  instance TEXT NOT NULL,
  external_id TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(episode_id,provider,instance,external_id)
);
CREATE TABLE tv_episode_numbering (
  entry_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  scheme TEXT NOT NULL,
  season INTEGER CHECK(season >= 0),
  episode INTEGER CHECK(episode >= 0),
  absolute INTEGER CHECK(absolute > 0),
  source TEXT NOT NULL,
  PRIMARY KEY(entry_id,episode_id,scheme),
  FOREIGN KEY(entry_id,episode_id) REFERENCES tv_entry_episodes(entry_id,episode_id),
  CHECK(season IS NOT NULL OR episode IS NOT NULL OR absolute IS NOT NULL)
);
CREATE TABLE tv_entry_relations (
  entry_id INTEGER NOT NULL REFERENCES tv_entries(id),
  related_key TEXT NOT NULL,
  title TEXT NOT NULL,
  relation TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(entry_id,related_key,relation,source)
);
CREATE TABLE tv_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL REFERENCES tv_entries(id),
  policy_key TEXT NOT NULL,
  policy_json TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
  created_at TEXT NOT NULL,
  UNIQUE(entry_id,policy_key), UNIQUE(id,entry_id)
);
CREATE TABLE tv_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  target_id INTEGER NOT NULL REFERENCES tv_targets(id),
  request_key TEXT NOT NULL,
  withdrawn_at TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(user_id,target_id,request_key), UNIQUE(id,target_id)
);
CREATE TABLE tv_request_episodes (
  request_id INTEGER NOT NULL REFERENCES tv_requests(id),
  entry_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  PRIMARY KEY(request_id,episode_id),
  FOREIGN KEY(entry_id,episode_id) REFERENCES tv_entry_episodes(entry_id,episode_id)
);
CREATE TABLE tv_request_origins (
  instance TEXT NOT NULL,
  external_request_id INTEGER NOT NULL,
  entry_id INTEGER NOT NULL REFERENCES tv_entries(id),
  request_id INTEGER NOT NULL UNIQUE REFERENCES tv_requests(id),
  source_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(instance,external_request_id,entry_id)
);
CREATE TABLE tv_acquisitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL REFERENCES tv_targets(id),
  request_id INTEGER,
  reason TEXT NOT NULL CHECK(reason IN ('initial','upgrade','recovery')),
  job_id INTEGER UNIQUE REFERENCES jobs(id),
  decision_json TEXT,
  finished_at TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(id,target_id),
  FOREIGN KEY(request_id,target_id) REFERENCES tv_requests(id,target_id)
);
CREATE INDEX tv_acquisitions_target ON tv_acquisitions(target_id,id);
CREATE TABLE tv_acquisition_downloads (
  acquisition_id INTEGER NOT NULL REFERENCES tv_acquisitions(id),
  torrent_id INTEGER NOT NULL REFERENCES media_torrents(id),
  PRIMARY KEY(acquisition_id,torrent_id)
);
CREATE TABLE tv_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL,
  target_id INTEGER NOT NULL,
  acquisition_id INTEGER NOT NULL UNIQUE,
  facts_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(id,entry_id), UNIQUE(id,target_id),
  FOREIGN KEY(target_id,entry_id) REFERENCES tv_targets(id,entry_id),
  FOREIGN KEY(acquisition_id,target_id) REFERENCES tv_acquisitions(id,target_id)
);
CREATE TABLE tv_version_files (
  version_id INTEGER NOT NULL,
  entry_id INTEGER NOT NULL,
  file_id TEXT NOT NULL REFERENCES managed_content_files(id),
  sequence INTEGER NOT NULL CHECK(sequence >= 0),
  PRIMARY KEY(version_id,file_id), UNIQUE(version_id,sequence),
  UNIQUE(version_id,file_id,entry_id),
  FOREIGN KEY(version_id,entry_id) REFERENCES tv_versions(id,entry_id)
);
CREATE TABLE tv_version_file_episodes (
  version_id INTEGER NOT NULL,
  file_id TEXT NOT NULL,
  entry_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  PRIMARY KEY(version_id,file_id,episode_id),
  FOREIGN KEY(version_id,file_id,entry_id) REFERENCES tv_version_files(version_id,file_id,entry_id),
  FOREIGN KEY(entry_id,episode_id) REFERENCES tv_entry_episodes(entry_id,episode_id)
);
CREATE TABLE tv_publications (
  target_id INTEGER PRIMARY KEY,
  version_id INTEGER NOT NULL,
  expected_revision INTEGER NOT NULL CHECK(expected_revision >= 0),
  created_at TEXT NOT NULL,
  FOREIGN KEY(version_id,target_id) REFERENCES tv_versions(id,target_id)
);
CREATE TABLE tv_active_episodes (
  target_id INTEGER NOT NULL,
  entry_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  PRIMARY KEY(target_id,episode_id),
  FOREIGN KEY(target_id,entry_id) REFERENCES tv_targets(id,entry_id),
  FOREIGN KEY(entry_id,episode_id) REFERENCES tv_entry_episodes(entry_id,episode_id),
  FOREIGN KEY(version_id,target_id) REFERENCES tv_versions(id,target_id)
);
CREATE TABLE tv_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL REFERENCES tv_targets(id),
  acquisition_id INTEGER,
  event_key TEXT NOT NULL UNIQUE,
  stage TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(acquisition_id,target_id) REFERENCES tv_acquisitions(id,target_id)
);
CREATE INDEX tv_events_target ON tv_events(target_id,id);
`;
