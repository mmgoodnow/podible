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
import { seerrInstance } from "../../src/movies/seerr";
import { tvSeerrFeedKey } from "../../src/tv/sync";
import { observedFiles } from "../../src/media/observed-files";
import { factsFromMovieProbe } from "../../src/movies/probe";
import { evaluateMovieQuality } from "../../src/movies/quality";
import type { TvCatalogEntry } from "../../src/tv/types";
import { tvShadowInputSchema } from "../../src/tv/shadow-types";

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
  settings.prowlarr={url:"https://fixture-prowlarr.invalid",apiKey:"fixture-private-key"};
  settings.movies.shadow.enabled = true;
  settings.movies.shadow.radarrUrl = "http://radarr:7878";
  repo.updateSettings(settings);
  const now=new Date().toISOString();
  repo.releaseInbox.ensureFeed("rss-fixture","Fixture indexer","movie",now);
  repo.releaseInbox.succeed("rss-fixture","head","Earlier releases are outside the available feed history.",now);
  repo.releaseInbox.fail("rss-fixture",{provider:"Fixture indexer",kind:"unavailable",status:429,retryAt:Date.now()+60_000,message:"Indexer Fixture indexer is temporarily unavailable (HTTP 429)."},now);
  const { caseId } = repo.movieShadow.capture({ observation: { key: "fixture:history:1", movie: fixtureMovie, releaseTitle: "A Fixture Film 2024", sourcePaths: [], observedAt: new Date().toISOString(), radarr: { outcome: "imported", messages: [] } }, policy: settings.movies.qualityPolicy, files: [], source: "unavailable", warnings: ["Source is missing or has moved"] });
  repo.movieShadow.finish(caseId, { decision: "unobservable", selectedFile: null, reason: "No source files remain", facts: null, quality: null, probes: [], probeErrors: [], model: "fixture", evaluatedAt: new Date().toISOString() });
}
if (process.env.TV_FIXTURE) {
  settings.movies.seerr = { ...settings.movies.seerr, url: "https://fixture-seerr.invalid", apiKey: "tv-fixture-seerr-private" };
  settings.agents.apiKey = "tv-fixture-agent-private";
  settings.tv = { ...settings.tv, sonarrUrl: "https://fixture-sonarr.invalid/sonarr", sonarrApiKey: "tv-fixture-sonarr-private",
    managedRoot: path.join(root, "tv-managed"), plexSectionId: "19", plexViewPath: "/fixture/plex/tv",
    pathMappings: [{ remoteRoot: "/tv-fixture", localRoot: root }] };
  repo.updateSettings(settings);
  const now = new Date().toISOString(), instance = seerrInstance(settings.movies.seerr.url);
  repo.setJsonState(tvSeerrFeedKey(instance), { baselineId: 726, cursor: now });
  const owner = repo.upsertUser({ provider: "local", providerUserId: "tv-fixture-viewer", username: "TV viewer", isAdmin: false });
  repo.createSession(owner.id, hashSessionToken("tv-reader-fixture"), null, "browser");
  const catalog: TvCatalogEntry = {
    key: "anidb:200", tvdbId: 100, tmdbId: null, title: "Fixture Sequel", aliases: ["Fixture Aggregate Series"], anime: true, year: 2020,
    source: "fixture:explicit-anime-map", refs: [],
    relations: [{ key: "anidb:199", title: "Fixture Prequel", relation: "PREQUEL", source: "fixture:exact-anilist-relation" }],
    episodes: [1, 2].map((n) => ({ key: `tvdb:${1000 + n}`, title: `Fixture episode ${n}`, airDate: n === 2 ? "2099-01-01" : "2020-01-01", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 2, episode: n, absolute: null, source: "fixture:sonarr:aired" },
        { scheme: "anidb", season: 1, episode: n, absolute: n, source: "fixture:explicit-anime-map" }] })),
  };
  const native = repo.tv.requestApproved({ userId: owner.id, entry: catalog, policy: settings.movies.qualityPolicy,
    episodeKeys: catalog.episodes.map((episode) => episode.key), origin: { instance, externalRequestId: 726, source: { fixture: true } } });
  const acquisition = repo.tv.startAcquisition(native.targetId, "initial", (id) => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId: id }, status: "succeeded", idempotencyKey: "tv-browser-seed",
  }).id, native.requestId).acquisition;
  const probe = { format: { duration: "1440" }, streams: [
    { codec_type: "video", codec_name: "h264", width: 1920, height: 1080, color_transfer: "bt709" },
    { codec_type: "audio", tags: { language: "jpn" } },
  ] };
  const facts = factsFromMovieProbe(probe, { identityVerified: true, source: "web", cut: null, proper: null });
  const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{
    id: "tv-browser-content", relativePath: "files/tv-browser-native.mkv", sizeBytes: 10, probe, episodeIds: [native.episodeIds[0]!],
  }]);
  // Synthetic persisted coverage, not a transfer or real Plex publication assertion.
  repo.tv.stagePublication(native.targetId, version, 0);
  repo.tv.promotePublication(native.targetId, version, 0);
  repo.tv.clearPublication(native.targetId, version);
  repo.tv.finishAcquisition(acquisition.id);
  repo.tv.decision(acquisition.id, { plan: { files: [{ name: "native-selected-file.mkv", episodeIds: [native.episodeIds[0]!] }] }, reason: "Fixture first episode available; future intent retained" });
  repo.tv.event(native.targetId, acquisition.id, "tv-browser-event", "retained", "One native episode available; future episode intent retained");
  await fs.writeFile(path.join(root, "tv-audit.mkv"), "synthetic TV audit evidence");
  const observation = { key: "sonarr:fixture:queue:1:entry:anidb:200", sourceKey: "sonarr:fixture:queue:1", entry: catalog,
    episodeKeys: ["tvdb:1002"], releaseTitle: "Fixture Sequel 02 WEB 1080p", observedAt: now,
    sourcePaths: ["/tv-fixture/tv-audit.mkv"], libraryPath: null,
    sonarr: { outcome: "blocked" as const, messages: ["Fixture Sonarr import blocked"] } };
  const captured = repo.tvShadow.capture(tvShadowInputSchema.parse({ observation, entry: repo.tv.entry(native.entryId)!, episodeIds: [native.episodeIds[1]!],
    policy: settings.movies.qualityPolicy, ...await observedFiles(observation, settings.tv.pathMappings) }));
  const auditJob = repo.createJob({ type: "tv_shadow_evaluate", payload: { caseId: captured.caseId }, status: "succeeded", idempotencyKey: "tv-browser-audit" });
  repo.tvShadow.attachJob(captured.caseId, auditJob.id);
  repo.tvShadow.finish(captured.caseId, auditJob.id, { decision: "import", reason: "Fixture independent episode and probe evidence agrees",
    files: [{ index: 0, name: "tv-audit.mkv", episodeIds: [native.episodeIds[1]!], facts,
      quality: evaluateMovieQuality(settings.movies.qualityPolicy, facts), reason: "Exact provider episode maps to the observed file" }],
    unmappedEpisodeIds: [], probes: [{ index: 0, name: "tv-audit.mkv", probe }], probeErrors: [], model: "fixture", evaluatedAt: now });
}
const fixture = createFixtureMovieServices(root);
const runtime = await createJobExecutionRuntime(
  {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
    movieShadowServices: { observe: async () => [], inspect: fixture.services.decider.inspect, probe: fixture.services.probe },
    indexerFeedSources: async () => [],
    onLog() {},
  },
  path.join(root, "execution.sqlite"),
);
let pumping = false;
const interval = setInterval(async () => {
  // TV browser actions exercise real queue receipts, never external acquisition.
  if (pumping || process.env.TV_FIXTURE) return;
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
  isAdmin: Boolean(process.env.SHADOW_FIXTURE || process.env.TV_FIXTURE),
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
