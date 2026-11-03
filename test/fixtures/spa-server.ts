import { Database } from "bun:sqlite";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createPodibleFetchHandler } from "../../src/http";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { createFixtureMovieServices } from "./movie-services";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { fixtureMovie } from "./movie-services";

const db = new Database(":memory:");
runMigrations(db);
const repo = new BooksRepo(db);
repo.ensureSettings();
const root = await fs.realpath(
  await fs.mkdtemp(path.join(os.tmpdir(), "movie-spa-")),
);
const settings = repo.getSettings();
settings.movies = {
  ...settings.movies,
  managedRoot: root,
  plexViewPath: "/data/view",
};
repo.updateSettings(settings);
if (process.env.SHADOW_FIXTURE) {
  settings.movies.shadow.enabled = true;
  settings.movies.shadow.radarrUrl = "http://radarr:7878";
  repo.updateSettings(settings);
  const { caseId } = repo.movieShadow.capture({ observation: { key: "fixture:history:1", movie: fixtureMovie, releaseTitle: "A Fixture Film 2024", sourcePaths: [], observedAt: new Date().toISOString(), radarr: { outcome: "imported", messages: [] } }, policy: settings.movies.qualityPolicy, files: [], source: "unavailable", warnings: ["Source is missing or has moved"] });
  repo.movieShadow.finish(caseId, { decision: "unobservable", selectedFile: null, reason: "No source files remain", facts: null, quality: null, probes: [], probeErrors: [], model: "fixture", evaluatedAt: new Date().toISOString() });
}
const fixture = createFixtureMovieServices(root);
const runtime = await createJobExecutionRuntime(
  {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
    movieShadowServices: { observe: async () => [], inspect: fixture.services.decider.inspect, probe: fixture.services.probe },
    onLog() {},
  },
  path.join(root, "execution.sqlite"),
);
let pumping = false;
const interval = setInterval(async () => {
  if (pumping) return;
  pumping = true;
  try {
    await runtime.reconcile();
  } finally {
    pumping = false;
  }
}, 30);
const user = repo.upsertUser({
  provider: "local",
  providerUserId: "spa-fixture",
  username: "reader",
  isAdmin: Boolean(process.env.SHADOW_FIXTURE),
});
repo.createSession(
  user.id,
  hashSessionToken("spa-browser-fixture"),
  null,
  "browser",
);
for (const [title, author] of [
  ["The Left Hand of Darkness", "Ursula K. Le Guin"],
  ["Piranesi", "Susanna Clarke"],
  ["The Odyssey", "Homer"],
])
  repo.createBook({ title: title!, author: author! });
const handler = createPodibleFetchHandler(repo, Date.now(), null, {
  movieServices: fixture.services,
});
const server = Bun.serve({
  hostname: "127.0.0.1",
  port: Number(process.env.PORT ?? 0),
  fetch(request) {
    const pathname = new URL(request.url).pathname;
    if (request.method === "POST" && pathname === "/__fixture/add-release") {
      fixture.addOffer("manual", 1280);
      return Response.json({ ok: true });
    }
    if (request.method === "POST" && pathname === "/__fixture/add-upgrade") {
      fixture.addOffer("upgrade", 1920);
      return Response.json({ ok: true });
    }
    return handler(request);
  },
});
console.log(`SPA_FIXTURE_URL=${server.url}`);
process.on("SIGTERM", async () => {
  clearInterval(interval);
  server.stop(true);
  await runtime.stop();
  db.close();
  await fs.rm(root, { recursive: true, force: true });
  process.exit(0);
});
