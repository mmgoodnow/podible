export const BOOK_TORRENTS_SCHEMA_SQL = `
CREATE TABLE book_torrents (
  info_hash TEXT PRIMARY KEY NOT NULL
    CHECK (length(info_hash) = 40 AND info_hash NOT GLOB '*[^a-f0-9]*'),
  book_id INTEGER NOT NULL,
  release_id INTEGER UNIQUE REFERENCES releases(id) ON DELETE SET NULL,
  backend TEXT NOT NULL CHECK (backend = 'transmission'),
  client_id TEXT NOT NULL,
  root_path TEXT NOT NULL,
  root_token TEXT NOT NULL,
  root_id TEXT NOT NULL CHECK (root_id = 'books'),
  relative_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`;
