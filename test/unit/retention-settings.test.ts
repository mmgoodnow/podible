import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { appSettingsSchema } from "../../src/rpc/schemas";
import { defaultSettings, parseSettings } from "../../src/settings";

test("retention observation never initializes absent settings", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const before = db.query("SELECT total_changes() AS n").get();
  expect(repo.readSettings()).toBeNull();
  expect(db.query("SELECT total_changes() AS n").get()).toEqual(before);
});

test("legacy configuration gains conservative retention defaults without a write or credential change", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const { retention: _, ...legacy } = defaultSettings({ agents: { apiKey: "fixture-secret" } });
  const raw = JSON.stringify(legacy);
  db.query("INSERT INTO settings(id,value_json) VALUES(1,?)").run(raw);
  const repo = new BooksRepo(db);
  const before = db.query("SELECT total_changes() AS n").get();
  expect(repo.readSettings()?.retention).toEqual({ minimumFreeBytes: 20 * 1024 ** 3, protectedMovieTargetIds: [] });
  expect(repo.readSettings()?.agents.apiKey).toBe("fixture-secret");
  expect(db.query("SELECT value_json FROM settings WHERE id=1").get()).toEqual({ value_json: raw });
  expect(db.query("SELECT total_changes() AS n").get()).toEqual(before);
});

test("retention settings round trip through typed RPC and persistence; unsafe overrides are rejected", () => {
  const settings = defaultSettings({ retention: { minimumFreeBytes: 100 * 1024 ** 3, protectedMovieTargetIds: [1, 7] } });
  expect(appSettingsSchema.parse(settings).retention).toEqual(settings.retention);
  expect(parseSettings(JSON.stringify(settings)).retention).toEqual(settings.retention);
  for (const retention of [
    { minimumFreeBytes: -1 }, { minimumFreeBytes: Number.MAX_SAFE_INTEGER + 1 },
    { protectedMovieTargetIds: [0] }, { protectedMovieTargetIds: Array(1001).fill(1) },
    { deleteSeedData: true }, { ignoreWatchHistory: true },
  ]) {
    expect(() => appSettingsSchema.parse({ ...settings, retention })).toThrow();
    expect(() => parseSettings(JSON.stringify({ ...settings, retention }))).toThrow();
  }
});
