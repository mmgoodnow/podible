import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import {
  queueMovieAcquisition,
  movieRequestView,
} from "../../src/movies/service";
import { movieOfferFromResult, type MovieOffer } from "../../src/movies/offers";
import { acquireMovie } from "../../src/movies/acquisition";
import {
  createFixtureMovieServices,
  fixtureMovie,
} from "../fixtures/movie-services";

test("an actual failed RSS download can trigger bounded recovery search, never a periodic search", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "rss-recovery-")),
  );
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db),
    settings = repo.ensureSettings();
  Object.assign(settings.movies, {
    managedRoot: root,
    plexViewPath: "/fixture/movies",
  });
  repo.updateSettings(settings);
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "fixture",
    username: "fixture",
    isAdmin: false,
  });
  const request = repo.movies.request(
    user.id,
    fixtureMovie,
    settings.movies.qualityPolicy,
  );
  const fixture = createFixtureMovieServices(root);
  const normalize = (offer: MovieOffer) =>
    movieOfferFromResult({
      title: offer.title,
      provider: offer.provider,
      mediaType: "movie",
      sizeBytes: offer.sizeBytes,
      url: offer.url,
      guid: offer.key,
      infoHash: offer.infoHash,
      seeders: offer.seeders,
      leechers: null,
      raw: offer.attributes,
    });
  const stalled = normalize(fixture.addOffer("stalled-feed", 1920, true));
  const working = normalize(fixture.addOffer("working-recovery", 1280));
  let searches = 0;
  fixture.services.search = async () => {
    searches++;
    return { offers: [stalled, working], warnings: [] };
  };
  try {
    const acquisition = queueMovieAcquisition(
      repo,
      request.targetId,
      "initial",
      undefined,
      [stalled],
    ).acquisition;
    await acquireMovie(
      {
        repo,
        getSettings: () => repo.getSettings(),
        movieServices: fixture.services,
      },
      repo.getJob(acquisition.job_id!)!,
      fixture.services,
    );
    expect(searches).toBe(1);
    expect(fixture.stats.snatches).toBe(2);
    expect(fixture.stats.quiesces).toBe(1);
    expect(fixture.stats.removals).toBe(0);
    expect(movieRequestView(repo, request.requestId).available).toBe(true);
    expect(
      repo.movies
        .events(request.targetId)
        .some((event) => event.message.includes("failed feed download")),
    ).toBe(true);
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
