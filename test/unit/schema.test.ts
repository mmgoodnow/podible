import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";

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

    expect(indexExists(db, "idx_books_added_at")).toBe(true);
    expect(indexExists(db, "idx_releases_book_status")).toBe(true);
    expect(indexExists(db, "idx_releases_book_media")).toBe(true);
    expect(indexExists(db, "idx_releases_info_hash")).toBe(true);
    expect(indexExists(db, "idx_releases_url")).toBe(true);
    expect(indexExists(db, "idx_assets_book_created")).toBe(true);
    expect(indexExists(db, "idx_asset_files_asset_start")).toBe(true);
    expect(indexExists(db, "idx_jobs_status_next_created")).toBe(true);

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
});
