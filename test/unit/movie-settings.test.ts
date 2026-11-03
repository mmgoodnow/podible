import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { movieSettingsSchema } from "../../src/movies/settings";
import { reconcileMovies } from "../../src/movies/maintenance";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";

test("movies have no feature flag; migration removes old flags without changing other settings", () => {
  for (const enabled of [false, true]) {
    const db = new Database(":memory:");
    runMigrations(db);
    const settings = defaultSettings();
    const raw = {
      ...settings,
      movies: {
        ...settings.movies,
        enabled,
        tmdbAccessToken: "preserve-token",
        shadow: { ...settings.movies.shadow, enabled: true },
      },
    };
    db.query("INSERT INTO settings(id,value_json) VALUES(1,?)").run(
      JSON.stringify(raw),
    );
    db.query("DELETE FROM schema_migrations WHERE id=39").run();
    runMigrations(db);
    const repo = new BooksRepo(db);
    expect(repo.getSettings().movies).not.toHaveProperty("enabled");
    expect(repo.getSettings().movies.tmdbAccessToken).toBe("preserve-token");
    expect(repo.getSettings().movies.shadow.enabled).toBe(true);
    expect(movieSettingsSchema.safeParse(raw.movies).success).toBe(false);
    db.close();
  }
});

test("movie maintenance does not require configuration when there are no native requests", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  try {
    expect(
      await reconcileMovies({ repo, getSettings: () => repo.getSettings() }),
    ).toBe("done");
  } finally {
    db.close();
  }
});
test("policy migration corrects untouched stock defaults but preserves customized constraints", () => {
  for (const custom of [false, true]) {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db),
      settings = repo.ensureSettings();
    settings.movies.qualityPolicy = {
      ...defaultMovieQualityPolicy,
      cut: "theatrical",
      audioLanguages: ["en"],
      allowHdr: false,
      minimumPreferenceGain: custom ? 30 : 10,
    };
    repo.updateSettings(settings);
    db.query("DELETE FROM schema_migrations WHERE id=40").run();
    runMigrations(db);
    expect(repo.getSettings().movies.qualityPolicy.cut).toBe(
      custom ? "theatrical" : null,
    );
    expect(repo.getSettings().movies.qualityPolicy.audioLanguages).toEqual(
      custom ? ["en"] : [],
    );
    db.close();
  }
});
