import { mkdirSync } from "node:fs";
import path from "node:path";

import { Database } from "bun:sqlite";

import { booksDbPath } from "../config";

type StateRow = {
  value_json: string;
};

let db: Database | null = null;

function nowIso(): string {
  return new Date().toISOString();
}

function stateDb(): Database {
  if (db) return db;
  mkdirSync(path.dirname(booksDbPath), { recursive: true });
  const next = new Database(booksDbPath, { create: true });
  next.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS app_state (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  db = next;
  return next;
}

export function loadJsonState<T>(key: string): T | null {
  const row = stateDb().query("SELECT value_json FROM app_state WHERE key = ?").get(key) as StateRow | null;
  if (!row) return null;
  try {
    return JSON.parse(row.value_json) as T;
  } catch {
    return null;
  }
}

export function saveJsonState(key: string, value: unknown): void {
  stateDb()
    .query(
      "INSERT INTO app_state (key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at"
    )
    .run(key, JSON.stringify(value), nowIso());
}

