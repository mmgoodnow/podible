import { mkdirSync } from "node:fs";
import path from "node:path";

import { Database } from "bun:sqlite";

const SCHEMA_MIGRATION_ID = 1;
const GUID_MIGRATION_ID = 2;
const JOB_TYPE_MIGRATION_ID = 3;
const ASSET_FILE_SOURCE_PATH_MIGRATION_ID = 4;
const TORRENT_CACHE_MIGRATION_ID = 5;
const REMOVE_TRANSCODE_JOB_TYPE_MIGRATION_ID = 6;
const CHAPTER_ANALYSIS_MIGRATION_ID = 7;
const ASSET_TRANSCRIPTS_MIGRATION_ID = 8;
const USERS_AND_SESSIONS_MIGRATION_ID = 9;
const LOCAL_USERS_PROVIDER_MIGRATION_ID = 10;
const PLEX_LOGIN_ATTEMPTS_MIGRATION_ID = 11;
const BOOK_WORD_COUNT_MIGRATION_ID = 12;
const APP_AUTH_MIGRATION_ID = 13;
const APP_STATE_MIGRATION_ID = 14;
const ASSET_TRANSCRIPT_PATH_MIGRATION_ID = 15;
const MANIFESTATIONS_MIGRATION_ID = 16;

const BASE_SCHEMA_SQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS books (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  author TEXT NOT NULL,
  cover_path TEXT NULL,
  duration_ms INTEGER NULL,
  word_count INTEGER NULL,
  added_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  published_at TEXT NULL,
  description TEXT NULL,
  description_html TEXT NULL,
  language TEXT NULL,
  identifiers_json TEXT NULL
);

CREATE TABLE IF NOT EXISTS releases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  book_id INTEGER NOT NULL,
  provider TEXT NOT NULL,
  provider_guid TEXT NULL,
  title TEXT NOT NULL,
  media_type TEXT NOT NULL CHECK (media_type IN ('audio', 'ebook')),
  info_hash TEXT NOT NULL,
  size_bytes INTEGER NULL,
  url TEXT NOT NULL,
  snatched_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('snatched', 'downloading', 'downloaded', 'imported', 'failed')),
  error TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  book_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('single', 'multi', 'ebook')),
  mime TEXT NOT NULL,
  total_size INTEGER NOT NULL,
  duration_ms INTEGER NULL,
  source_release_id INTEGER NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
  FOREIGN KEY (source_release_id) REFERENCES releases(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS asset_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  source_path TEXT NULL,
  size INTEGER NOT NULL,
  start INTEGER NOT NULL,
  end INTEGER NOT NULL,
  duration_ms INTEGER NOT NULL,
  title TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('full_library_refresh', 'acquire', 'download', 'import', 'reconcile', 'chapter_analysis')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  book_id INTEGER NULL,
  release_id INTEGER NULL,
  payload_json TEXT NULL,
  error TEXT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_run_at TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
  FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chapter_analysis (
  asset_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
  source TEXT NOT NULL,
  algorithm_version TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  transcript_fingerprint TEXT NULL,
  chapters_json TEXT NULL,
  debug_json TEXT NULL,
  resolved_boundary_count INTEGER NOT NULL DEFAULT 0,
  total_boundary_count INTEGER NOT NULL DEFAULT 0,
  error TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS asset_transcripts (
  asset_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
  source TEXT NOT NULL,
  algorithm_version TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  transcript_path TEXT NULL,
  transcript_json TEXT NULL,
  error TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL CHECK (provider IN ('plex', 'local')),
  provider_user_id TEXT NOT NULL,
  username TEXT NOT NULL,
  display_name TEXT NULL,
  thumb_url TEXT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('browser', 'app')) DEFAULT 'browser',
  token_hash TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS plex_login_attempts (
  pin_id INTEGER PRIMARY KEY,
  client_identifier TEXT NOT NULL,
  public_jwk_json TEXT NOT NULL,
  private_key_pkcs8 TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  value_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS torrent_cache (
  key TEXT PRIMARY KEY,
  provider TEXT NULL,
  provider_guid TEXT NULL,
  url TEXT NOT NULL,
  info_hash TEXT NULL,
  torrent_bytes BLOB NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  id INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_books_added_at ON books(added_at);
CREATE INDEX IF NOT EXISTS idx_releases_book_status ON releases(book_id, status);
CREATE INDEX IF NOT EXISTS idx_releases_book_media ON releases(book_id, media_type);
CREATE INDEX IF NOT EXISTS idx_releases_info_hash ON releases(info_hash);
CREATE INDEX IF NOT EXISTS idx_releases_provider_guid ON releases(provider, provider_guid);
CREATE INDEX IF NOT EXISTS idx_releases_url ON releases(url);
CREATE INDEX IF NOT EXISTS idx_assets_book_created ON assets(book_id, created_at);
CREATE INDEX IF NOT EXISTS idx_asset_files_asset_start ON asset_files(asset_id, start);
CREATE INDEX IF NOT EXISTS idx_jobs_status_next_created ON jobs(status, next_run_at, created_at);
CREATE INDEX IF NOT EXISTS idx_torrent_cache_provider_guid ON torrent_cache(provider, provider_guid);
CREATE INDEX IF NOT EXISTS idx_torrent_cache_url ON torrent_cache(url);
CREATE UNIQUE INDEX IF NOT EXISTS ux_releases_info_hash ON releases(info_hash);
CREATE UNIQUE INDEX IF NOT EXISTS ux_releases_provider_guid ON releases(provider, provider_guid) WHERE provider_guid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_provider_user ON users(provider, provider_user_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_token_hash ON sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_plex_login_attempts_created_at ON plex_login_attempts(created_at);

CREATE TABLE IF NOT EXISTS app_login_attempts (
  id TEXT PRIMARY KEY,
  redirect_uri TEXT NOT NULL,
  state TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_codes (
  code_hash TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  attempt_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at TEXT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (attempt_id) REFERENCES app_login_attempts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_login_attempts_expires_at ON app_login_attempts(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_codes_attempt_id ON auth_codes(attempt_id);
CREATE INDEX IF NOT EXISTS idx_auth_codes_expires_at ON auth_codes(expires_at);

CREATE TABLE IF NOT EXISTS app_state (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`;

function hasColumn(db: Database, table: string, column: string): boolean {
  const rows = db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((row) => row.name === column);
}

function applyGuidMigration(db: Database): void {
  if (!hasColumn(db, "releases", "provider_guid")) {
    db.exec("ALTER TABLE releases ADD COLUMN provider_guid TEXT NULL");
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_releases_provider_guid ON releases(provider, provider_guid)");
  db.exec(
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_releases_provider_guid ON releases(provider, provider_guid) WHERE provider_guid IS NOT NULL"
  );
}

function applyJobTypeMigration(db: Database): void {
  db.exec(`
CREATE TABLE jobs_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('full_library_refresh', 'acquire', 'download', 'import', 'reconcile')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  book_id INTEGER NULL,
  release_id INTEGER NULL,
  payload_json TEXT NULL,
  error TEXT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_run_at TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
  FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
);

INSERT INTO jobs_new (
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
)
SELECT
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_created ON jobs(status, next_run_at, created_at);
`);
}

function applyAssetFileSourcePathMigration(db: Database): void {
  if (!hasColumn(db, "asset_files", "source_path")) {
    db.exec("ALTER TABLE asset_files ADD COLUMN source_path TEXT NULL");
  }
}

function applyTorrentCacheMigration(db: Database): void {
  db.exec(`
CREATE TABLE IF NOT EXISTS torrent_cache (
  key TEXT PRIMARY KEY,
  provider TEXT NULL,
  provider_guid TEXT NULL,
  url TEXT NOT NULL,
  info_hash TEXT NULL,
  torrent_bytes BLOB NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_torrent_cache_provider_guid ON torrent_cache(provider, provider_guid);
CREATE INDEX IF NOT EXISTS idx_torrent_cache_url ON torrent_cache(url);
`);
}

function applyRemoveTranscodeJobTypeMigration(db: Database): void {
  db.exec(`
CREATE TABLE jobs_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('full_library_refresh', 'acquire', 'download', 'import', 'reconcile')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  book_id INTEGER NULL,
  release_id INTEGER NULL,
  payload_json TEXT NULL,
  error TEXT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_run_at TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
  FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
);

INSERT INTO jobs_new (
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
)
SELECT
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
FROM jobs
WHERE type <> 'transcode';

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_created ON jobs(status, next_run_at, created_at);
`);
}

function applyChapterAnalysisMigration(db: Database): void {
  db.exec(`
CREATE TABLE jobs_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('full_library_refresh', 'acquire', 'download', 'import', 'reconcile', 'chapter_analysis')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  book_id INTEGER NULL,
  release_id INTEGER NULL,
  payload_json TEXT NULL,
  error TEXT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_run_at TEXT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE,
  FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
);

INSERT INTO jobs_new (
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
)
SELECT
  id, type, status, book_id, release_id, payload_json, error,
  attempt_count, max_attempts, next_run_at, created_at, updated_at
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_created ON jobs(status, next_run_at, created_at);

CREATE TABLE IF NOT EXISTS chapter_analysis (
  asset_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
  source TEXT NOT NULL,
  algorithm_version TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  transcript_fingerprint TEXT NULL,
  chapters_json TEXT NULL,
  debug_json TEXT NULL,
  resolved_boundary_count INTEGER NOT NULL DEFAULT 0,
  total_boundary_count INTEGER NOT NULL DEFAULT 0,
  error TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
);
`);
}

function applyAssetTranscriptsMigration(db: Database): void {
  if (!hasColumn(db, "chapter_analysis", "transcript_fingerprint")) {
    db.exec("ALTER TABLE chapter_analysis ADD COLUMN transcript_fingerprint TEXT NULL");
  }
  db.exec(`
CREATE TABLE IF NOT EXISTS asset_transcripts (
  asset_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
  source TEXT NOT NULL,
  algorithm_version TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  transcript_path TEXT NULL,
  transcript_json TEXT NULL,
  error TEXT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
);
`);
}

function applyAssetTranscriptPathMigration(db: Database): void {
  if (!hasColumn(db, "asset_transcripts", "transcript_path")) {
    db.exec("ALTER TABLE asset_transcripts ADD COLUMN transcript_path TEXT NULL");
  }
}

function applyUsersAndSessionsMigration(db: Database): void {
  db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL CHECK (provider IN ('plex', 'local')),
  provider_user_id TEXT NOT NULL,
  username TEXT NOT NULL,
  display_name TEXT NULL,
  thumb_url TEXT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('browser', 'app')) DEFAULT 'browser',
  token_hash TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_users_provider_user ON users(provider, provider_user_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_token_hash ON sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
`);
}

function applyLocalUsersProviderMigration(db: Database): void {
  db.exec("PRAGMA foreign_keys = OFF;");
  db.exec(`
CREATE TABLE users_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL CHECK (provider IN ('plex', 'local')),
  provider_user_id TEXT NOT NULL,
  username TEXT NOT NULL,
  display_name TEXT NULL,
  thumb_url TEXT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

INSERT INTO users_new (
  id, provider, provider_user_id, username, display_name, thumb_url, is_admin, created_at, updated_at
)
SELECT
  id, provider, provider_user_id, username, display_name, thumb_url, is_admin, created_at, updated_at
FROM users;

DROP TABLE users;
ALTER TABLE users_new RENAME TO users;

CREATE UNIQUE INDEX IF NOT EXISTS ux_users_provider_user ON users(provider, provider_user_id);
`);
  db.exec("PRAGMA foreign_keys = ON;");
}

function applyPlexLoginAttemptsMigration(db: Database): void {
  db.exec(`
CREATE TABLE IF NOT EXISTS plex_login_attempts (
  pin_id INTEGER PRIMARY KEY,
  client_identifier TEXT NOT NULL,
  public_jwk_json TEXT NOT NULL,
  private_key_pkcs8 TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plex_login_attempts_created_at ON plex_login_attempts(created_at);
`);
}

function applyBookWordCountMigration(db: Database): void {
  if (!hasColumn(db, "books", "word_count")) {
    db.exec("ALTER TABLE books ADD COLUMN word_count INTEGER NULL");
  }
}

function applyAppAuthMigration(db: Database): void {
  if (!hasColumn(db, "sessions", "kind")) {
    db.exec("ALTER TABLE sessions ADD COLUMN kind TEXT NOT NULL DEFAULT 'browser'");
  }
  db.exec(`
CREATE TABLE IF NOT EXISTS app_login_attempts (
  id TEXT PRIMARY KEY,
  redirect_uri TEXT NOT NULL,
  state TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_codes (
  code_hash TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  attempt_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at TEXT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (attempt_id) REFERENCES app_login_attempts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_login_attempts_expires_at ON app_login_attempts(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_codes_attempt_id ON auth_codes(attempt_id);
CREATE INDEX IF NOT EXISTS idx_auth_codes_expires_at ON auth_codes(expires_at);
`);
}

function applyAppStateMigration(db: Database): void {
  db.exec(`
CREATE TABLE IF NOT EXISTS app_state (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`);
}

// Introduces a `manifestations` table to model "playable editions" of a book
// distinct from the "containers" (assets) that make them up. Every existing
// asset is wrapped in a one-container manifestation, so behavior is unchanged
// until later steps swap readers over to query manifestations directly.
function applyManifestationsMigration(db: Database): void {
  db.exec(`
CREATE TABLE IF NOT EXISTS manifestations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  book_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('audio', 'ebook')),
  label TEXT NULL,
  edition_note TEXT NULL,
  duration_ms INTEGER NULL,
  total_size INTEGER NOT NULL,
  preferred_score INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_manifestations_book ON manifestations(book_id);
`);

  if (!hasColumn(db, "assets", "manifestation_id")) {
    db.exec("ALTER TABLE assets ADD COLUMN manifestation_id INTEGER NULL REFERENCES manifestations(id) ON DELETE CASCADE");
  }
  if (!hasColumn(db, "assets", "sequence_in_manifestation")) {
    db.exec("ALTER TABLE assets ADD COLUMN sequence_in_manifestation INTEGER NOT NULL DEFAULT 0");
  }
  db.exec(`
CREATE INDEX IF NOT EXISTS idx_assets_manifestation_seq
  ON assets(manifestation_id, sequence_in_manifestation);
`);

  // Backfill: every existing asset becomes a one-container manifestation.
  // We map the legacy asset.kind ('single' / 'multi' / 'ebook') to the new
  // manifestation.kind ('audio' / 'ebook'). Keeping it as a single statement
  // (per asset) so the asset's manifestation_id can be set in the same row.
  const orphanAssets = db
    .query("SELECT id, book_id, kind, mime, total_size, duration_ms, created_at, updated_at FROM assets WHERE manifestation_id IS NULL")
    .all() as Array<{
      id: number;
      book_id: number;
      kind: string;
      mime: string;
      total_size: number;
      duration_ms: number | null;
      created_at: string;
      updated_at: string;
    }>;
  const insertManifestation = db.prepare(
    `INSERT INTO manifestations (book_id, kind, label, edition_note, duration_ms, total_size, preferred_score, created_at, updated_at)
     VALUES (?, ?, NULL, NULL, ?, ?, 0, ?, ?)
     RETURNING id`
  );
  const linkAsset = db.prepare(
    "UPDATE assets SET manifestation_id = ?, sequence_in_manifestation = 0 WHERE id = ?"
  );
  for (const asset of orphanAssets) {
    const manifestationKind = asset.kind === "ebook" ? "ebook" : "audio";
    const result = insertManifestation.get(
      asset.book_id,
      manifestationKind,
      asset.duration_ms,
      asset.total_size,
      asset.created_at,
      asset.updated_at
    ) as { id: number };
    linkAsset.run(result.id, asset.id);
  }
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function openDatabase(dbPath: string): Database {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new Database(dbPath, { create: true });
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  runMigrations(db);
  return db;
}

export function runMigrations(db: Database): void {
  db.exec("CREATE TABLE IF NOT EXISTS schema_migrations (id INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)");

  const apply = (id: number, fn: () => void) => {
    const existing = db.query("SELECT id FROM schema_migrations WHERE id = ?").get(id) as { id: number } | null;
    if (existing) return;
    db.transaction(() => {
      fn();
      db.query("INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)").run(id, nowIso());
    })();
  };

  apply(SCHEMA_MIGRATION_ID, () => {
    db.exec(BASE_SCHEMA_SQL);
  });

  apply(GUID_MIGRATION_ID, () => {
    applyGuidMigration(db);
  });

  apply(JOB_TYPE_MIGRATION_ID, () => {
    applyJobTypeMigration(db);
  });

  apply(ASSET_FILE_SOURCE_PATH_MIGRATION_ID, () => {
    applyAssetFileSourcePathMigration(db);
  });

  apply(TORRENT_CACHE_MIGRATION_ID, () => {
    applyTorrentCacheMigration(db);
  });

  apply(REMOVE_TRANSCODE_JOB_TYPE_MIGRATION_ID, () => {
    applyRemoveTranscodeJobTypeMigration(db);
  });
  apply(CHAPTER_ANALYSIS_MIGRATION_ID, () => {
    applyChapterAnalysisMigration(db);
  });
  apply(ASSET_TRANSCRIPTS_MIGRATION_ID, () => {
    applyAssetTranscriptsMigration(db);
  });
  apply(USERS_AND_SESSIONS_MIGRATION_ID, () => {
    applyUsersAndSessionsMigration(db);
  });
  apply(LOCAL_USERS_PROVIDER_MIGRATION_ID, () => {
    applyLocalUsersProviderMigration(db);
  });
  apply(PLEX_LOGIN_ATTEMPTS_MIGRATION_ID, () => {
    applyPlexLoginAttemptsMigration(db);
  });
  apply(BOOK_WORD_COUNT_MIGRATION_ID, () => {
    applyBookWordCountMigration(db);
  });
  apply(APP_AUTH_MIGRATION_ID, () => {
    applyAppAuthMigration(db);
  });
  apply(APP_STATE_MIGRATION_ID, () => {
    applyAppStateMigration(db);
  });
  apply(ASSET_TRANSCRIPT_PATH_MIGRATION_ID, () => {
    applyAssetTranscriptPathMigration(db);
  });
  apply(MANIFESTATIONS_MIGRATION_ID, () => {
    applyManifestationsMigration(db);
  });
}
