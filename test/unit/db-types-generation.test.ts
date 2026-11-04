import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { finalizeJobJournalMigration, runMigrations } from "../../src/db";
import { generateDatabaseTypes, renderDatabaseTypes, updateDatabaseTypes } from "../../scripts/generate-db-types";

describe("migration-owned DB types", () => {
  test("committed output deterministically matches the actual finalized app schema", () => {
    const generated = generateDatabaseTypes();
    expect(generateDatabaseTypes()).toBe(generated);
    expect(readFileSync(new URL("../../src/db/schema.ts", import.meta.url), "utf8")).toBe(generated);
    using db = new Database(":memory:");
    runMigrations(db);
    expect((db.query("SELECT name FROM pragma_table_info('jobs')").all() as { name: string }[]).map((c) => c.name))
      .toContain("status");
    finalizeJobJournalMigration(db);
    expect(renderDatabaseTypes(db)).toBe(generated);
    expect(db.query("SELECT id FROM schema_migrations WHERE id = 34").get()).toEqual({ id: 34 });
    const tables = db.query<{ name: string }, []>(
      "SELECT name FROM sqlite_schema WHERE type = 'table' AND substr(name, 1, 7) <> 'sqlite_' ORDER BY name"
    ).all();
    const emittedTables = [...generated.matchAll(/^  (".*"): \{$/gm)].map((match) => JSON.parse(match[1]));
    expect(emittedTables).toEqual(tables.map((table) => table.name));
    expect(emittedTables).toContain("movie_request_origins");
    expect(emittedTables).toContain("job_commands");
    const jobType = generated.split('  "jobs": {')[1].split("  };")[0];
    for (const dropped of ["status", "error", "attempt_count", "next_run_at", "updated_at"]) {
      expect(jobType).not.toContain(`"${dropped}"`);
    }
  });

  test("detects added migration-shaped columns without a hardcoded version ceiling", () => {
    using db = new Database(":memory:");
    runMigrations(db);
    finalizeJobJournalMigration(db);
    const before = renderDatabaseTypes(db);
    db.exec("ALTER TABLE books ADD COLUMN future_default INTEGER NOT NULL DEFAULT 7");
    const after = renderDatabaseTypes(db);
    expect(after).not.toBe(before);
    expect(after).toContain('"future_default": Generated<number>;');
  });

  test("preserves defaults/nullability, true rowid aliases and nullable primary keys", () => {
    using db = new Database(":memory:");
    db.exec(`
      CREATE TABLE edge (
        id INTEGER PRIMARY KEY, required TEXT NOT NULL, optional TEXT,
        zero INTEGER NOT NULL DEFAULT 0, nullable_default TEXT DEFAULT 'yes', explicit_null TEXT DEFAULT NULL,
        timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, bytes BLOB, real_value REAL
      );
      CREATE TABLE text_key (id TEXT PRIMARY KEY, value TEXT);
      CREATE TABLE composite_key (a INTEGER, b TEXT, PRIMARY KEY (a, b));
      CREATE TABLE descending_key (id INTEGER PRIMARY KEY DESC, value TEXT);
      CREATE TABLE without_rowid (id INTEGER PRIMARY KEY, value TEXT) WITHOUT ROWID;
      CREATE TABLE strict_key (id TEXT PRIMARY KEY, value TEXT) STRICT;
    `);
    const generated = renderDatabaseTypes(db);
    expect(generated).toContain('"id": Generated<number>;');
    expect(generated).toContain('"required": string;');
    expect(generated).toContain('"optional": string | null;');
    expect(generated).toContain('"zero": Generated<number>;');
    expect(generated).toContain('"nullable_default": Generated<string | null>;');
    expect(generated).toContain('"explicit_null": Generated<string | null>;');
    expect(generated).toContain('"timestamp": Generated<string>;');
    expect(generated).toContain('"bytes": Uint8Array | null;');
    expect(generated).toContain('"real_value": number | null;');
    expect(generated).toContain('"text_key": {\n    "id": string | null;');
    expect(generated).toContain('"composite_key": {\n    "a": number | null;\n    "b": string | null;');
    expect(generated).toContain('"descending_key": {\n    "id": number | null;');
    expect(generated).toContain('"without_rowid": {\n    "id": number;');
    expect(generated).toContain('"strict_key": {\n    "id": string;');
    db.exec("INSERT INTO text_key DEFAULT VALUES; INSERT INTO composite_key DEFAULT VALUES; INSERT INTO descending_key DEFAULT VALUES;");
    expect(db.query("SELECT id FROM text_key").get()).toEqual({ id: null });
    expect(db.query("SELECT a, b FROM composite_key").get()).toEqual({ a: null, b: null });
    expect(db.query("SELECT id FROM descending_key").get()).toEqual({ id: null });
    expect(() => db.exec("INSERT INTO without_rowid DEFAULT VALUES")).toThrow();
    expect(() => db.exec("INSERT INTO strict_key DEFAULT VALUES")).toThrow();
  });

  test.each([
    ["CREATE TABLE unknown_type (value NUMERIC)", "Unsupported declared type"],
    ["CREATE TABLE generated (a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1))", "hidden/generated column"],
    ["CREATE TABLE base (id INTEGER); CREATE VIEW derived AS SELECT id FROM base", "Unsupported schema object"],
    ["CREATE VIRTUAL TABLE virtual USING fts5(value)", "Unsupported schema object"],
  ])("fails rather than silently guessing: %s", (ddl, message) => {
    using db = new Database(":memory:");
    db.exec(ddl);
    expect(() => renderDatabaseTypes(db)).toThrow(message);
  });

  test("check detects missing/stale output without writing it, generation repairs it", () => {
    const directory = mkdtempSync(path.join(tmpdir(), "podible-db-types-"));
    const file = pathToFileURL(path.join(directory, "schema.ts"));
    try {
      expect(() => updateDatabaseTypes(file, true)).toThrow("DB types are missing");
      writeFileSync(file, "// stale\n");
      expect(() => updateDatabaseTypes(file, true)).toThrow("DB types are stale");
      expect(readFileSync(file, "utf8")).toBe("// stale\n");
      updateDatabaseTypes(file, false);
      expect(() => updateDatabaseTypes(file, true)).not.toThrow();
      expect(readFileSync(file, "utf8")).toBe(generateDatabaseTypes());
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("CLI check ignores configured database paths and rejects unknown arguments", () => {
    const directory = mkdtempSync(path.join(tmpdir(), "podible-db-types-cli-"));
    const sentinel = path.join(directory, "do-not-open.sqlite");
    try {
      writeFileSync(sentinel, "not a database: must remain untouched");
      const result = Bun.spawnSync({
        cmd: [process.execPath, "run", "scripts/generate-db-types.ts", "--check"],
        cwd: new URL("../..", import.meta.url).pathname,
        env: { ...process.env, DB_PATH: sentinel, DATABASE_PATH: sentinel, DATA_DIR: directory },
      });
      expect(result.stderr.toString()).toBe("");
      expect(result.exitCode).toBe(0);
      expect(readFileSync(sentinel, "utf8")).toBe("not a database: must remain untouched");
      const invalid = Bun.spawnSync({
        cmd: [process.execPath, "run", "scripts/generate-db-types.ts", "--database", sentinel],
        cwd: new URL("../..", import.meta.url).pathname,
      });
      expect(invalid.exitCode).not.toBe(0);
      expect(invalid.stderr.toString()).toContain("Usage:");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
