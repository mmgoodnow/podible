import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/db";

function tableExists(db: Database, name: string): boolean {
  const row = db.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
  return Boolean(row);
}

function indexExists(db: Database, name: string): boolean {
  const row = db.query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?").get(name);
  return Boolean(row);
}

describe("schema migrations", () => {
  test("creates required tables and indexes", () => {
    const db = new Database(":memory:");
    runMigrations(db);

    expect(tableExists(db, "books")).toBe(true);
    expect(tableExists(db, "releases")).toBe(true);
    expect(tableExists(db, "assets")).toBe(true);
    expect(tableExists(db, "asset_files")).toBe(true);
    expect(tableExists(db, "jobs")).toBe(true);
    expect(tableExists(db, "settings")).toBe(true);
    expect(tableExists(db, "torrent_cache")).toBe(true);

    expect(indexExists(db, "idx_books_added_at")).toBe(true);
    expect(indexExists(db, "idx_books_added_by_user")).toBe(true);
    expect(indexExists(db, "idx_releases_book_status")).toBe(true);
    expect(indexExists(db, "idx_releases_book_media")).toBe(true);
    expect(indexExists(db, "idx_releases_info_hash")).toBe(true);
    expect(indexExists(db, "idx_releases_provider_guid")).toBe(true);
    expect(indexExists(db, "idx_releases_url")).toBe(true);
    expect(indexExists(db, "idx_assets_book_created")).toBe(true);
    expect(indexExists(db, "idx_asset_files_asset_start")).toBe(true);
    expect(indexExists(db, "idx_jobs_status_next_created")).toBe(true);
    expect(indexExists(db, "idx_torrent_cache_provider_guid")).toBe(true);
    expect(indexExists(db, "idx_torrent_cache_url")).toBe(true);
    expect(indexExists(db, "idx_books_openlibrary_metadata_version")).toBe(true);

    db.close();
  });

  test("marks existing books stale and preserves jobs when adding metadata hydration", () => {
    const db = new Database(":memory:");
    db.exec(`
      CREATE TABLE schema_migrations (id INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
      CREATE TABLE books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author TEXT NOT NULL,
        cover_path TEXT NULL,
        duration_ms INTEGER NULL,
        word_count INTEGER NULL,
        added_by_user_id INTEGER NULL,
        added_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        published_at TEXT NULL,
        description TEXT NULL,
        description_html TEXT NULL,
        language TEXT NULL,
        identifiers_json TEXT NULL,
        series_json TEXT NULL
      );
      CREATE TABLE jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL CHECK (type IN ('full_library_refresh', 'acquire', 'download', 'import', 'reconcile', 'chapter_analysis', 'cover_generation')),
        status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
        book_id INTEGER NULL,
        release_id INTEGER NULL,
        payload_json TEXT NULL,
        error TEXT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        next_run_at TEXT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);
    const now = new Date().toISOString();
    for (let id = 1; id <= 27; id += 1) {
      db.query("INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)").run(id, now);
    }
    const book = db
      .query("INSERT INTO books (title, author, added_at, updated_at) VALUES (?, ?, ?, ?) RETURNING id")
      .get("Existing Book", "Existing Author", now, now) as { id: number };
    const job = db
      .query("INSERT INTO jobs (type, status, created_at, updated_at) VALUES ('reconcile', 'queued', ?, ?) RETURNING id")
      .get(now, now) as { id: number };

    runMigrations(db);

    expect(
      db
        .query("SELECT openlibrary_metadata_version, openlibrary_hydrated_at FROM books WHERE id = ?")
        .get(book.id)
    ).toEqual({ openlibrary_metadata_version: 0, openlibrary_hydrated_at: null });
    expect(db.query("SELECT type, status FROM jobs WHERE id = ?").get(job.id)).toEqual({
      type: "reconcile",
      status: "queued",
    });
    expect(() => {
      db.query("INSERT INTO jobs (type, status, created_at, updated_at) VALUES ('metadata_hydration', 'queued', ?, ?)").run(
        now,
        now
      );
    }).not.toThrow();

    db.close();
  });

  test("enforces globally unique release info_hash", () => {
    const db = new Database(":memory:");
    runMigrations(db);

    const now = new Date().toISOString();
    const bookA = db
      .query("INSERT INTO books (title, author, added_at, updated_at) VALUES (?, ?, ?, ?) RETURNING id")
      .get("Book A", "Author A", now, now) as { id: number };
    const bookB = db
      .query("INSERT INTO books (title, author, added_at, updated_at) VALUES (?, ?, ?, ?) RETURNING id")
      .get("Book B", "Author B", now, now) as { id: number };

    db.query(
      "INSERT INTO releases (book_id, provider, title, media_type, info_hash, size_bytes, url, snatched_at, status, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).run(bookA.id, "test", "A", "audio", "abc123", 100, "http://a", now, "snatched", now);

    expect(() => {
      db.query(
        "INSERT INTO releases (book_id, provider, title, media_type, info_hash, size_bytes, url, snatched_at, status, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      ).run(bookB.id, "test", "B", "ebook", "abc123", 200, "http://b", now, "snatched", now);
    }).toThrow();

    db.close();
  });

  test("prunes stale empty manifestations while preserving active import targets", () => {
    const db = new Database(":memory:");
    runMigrations(db);

    const now = new Date().toISOString();
    const book = db
      .query("INSERT INTO books (title, author, added_at, updated_at) VALUES (?, ?, ?, ?) RETURNING id")
      .get("Book", "Author", now, now) as { id: number };
    const stale = db
      .query(
        `INSERT INTO manifestations (book_id, kind, label, edition_note, duration_ms, total_size, preferred_score, created_at, updated_at)
         VALUES (?, 'audio', 'stale', NULL, 1000, 100, 0, ?, ?)
         RETURNING id`
      )
      .get(book.id, now, now) as { id: number };
    const active = db
      .query(
        `INSERT INTO manifestations (book_id, kind, label, edition_note, duration_ms, total_size, preferred_score, created_at, updated_at)
         VALUES (?, 'audio', 'active', NULL, 2000, 200, 0, ?, ?)
         RETURNING id`
      )
      .get(book.id, now, now) as { id: number };
    const release = db
      .query(
        `INSERT INTO releases (book_id, provider, title, media_type, info_hash, size_bytes, url, snatched_at, status, updated_at)
         VALUES (?, 'mock', 'Book Active', 'audio', ?, 200, 'https://example.com/active.torrent', ?, 'downloaded', ?)
         RETURNING id`
      )
      .get(book.id, "2222222222222222222222222222222222222222", now, now) as { id: number };
    db.query(
      `INSERT INTO jobs (type, status, book_id, release_id, payload_json, attempt_count, max_attempts, created_at, updated_at)
       VALUES ('import', 'queued', ?, ?, ?, 0, 5, ?, ?)`
    ).run(book.id, release.id, JSON.stringify({ manifestationId: active.id, sequenceInManifestation: 0 }), now, now);

    db.query("DELETE FROM schema_migrations WHERE id = 17").run();
    runMigrations(db);

    expect(db.query("SELECT id FROM manifestations WHERE id = ?").get(stale.id)).toBeNull();
    expect(db.query("SELECT id FROM manifestations WHERE id = ?").get(active.id)).not.toBeNull();

    db.close();
  });
});
