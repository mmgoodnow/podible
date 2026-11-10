import { Database } from "bun:sqlite";
import { buildSessionCookie, createSessionToken, hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createPodibleFetchHandler } from "../../src/http";
import { AcquisitionEvents, acquisitionEventsPath } from "../../src/runtime/acquisition-events";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { createFixtureMovieServices } from "./movie-services";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { fixtureMovie } from "./movie-services";
import { seerrInstance } from "../../src/movies/seerr";
import { tvSeerrFeedKey, tvSeerrMappingKey } from "../../src/tv/sync";
import { observedFiles } from "../../src/media/observed-files";
import { factsFromMovieProbe } from "../../src/movies/probe";
import { evaluateMovieQuality } from "../../src/movies/quality";
import type { TvCatalogEntry } from "../../src/tv/types";
import { queueTvAcquisition } from "../../src/tv/acquisition";
import { tvShadowInputSchema } from "../../src/tv/shadow-types";
import { createHash } from "node:crypto";
import { safeReturnPath } from "../../src/redirects";
import { seedTransferMetricsFixture } from "./transfer-metrics";
import { seedActivityHistory } from "./ops-scale";

const root = await fs.realpath(
  await fs.mkdtemp(path.join(os.tmpdir(), "movie-spa-")),
);
const db = new Database(process.env.OPS_SCALE_FIXTURE ? path.join(root, "podible.sqlite") : ":memory:");
runMigrations(db);
const repo = new BooksRepo(db);
repo.ensureSettings();
const settings = repo.getSettings();
settings.movies = {
  ...settings.movies,
  managedRoot: root,
  plexViewPath: "/data/view",
};
repo.updateSettings(settings);
if (process.env.ADMIN_FIXTURE) {
  // Exercise a healthy-disk preview independently of the CI runner's free space.
  settings.retention.minimumFreeBytes = 0;
  settings.feed.title = "Fixture library";
  settings.libraryRoot = path.join(root, "books");
  settings.tv.managedRoot = path.join(root, "tv-unavailable");
  settings.auth.plex = { ...settings.auth.plex, ownerToken: "admin-fixture-owner-secret", machineId: "fixture-mendel" };
  settings.agents.apiKey = "admin-fixture-agent-secret";
  settings.rtorrent.url = "http://admin-fixture.invalid/RPC2";
  await fs.mkdir(settings.libraryRoot);
  repo.updateSettings(settings);
  const servers = [
    { machineId: "fixture-mendel", name: "Mendel", product: "Plex Media Server", owned: true, sourceTitle: null },
    { machineId: "fixture-cyprus", name: "Cyprus", product: "Plex Media Server", owned: false, sourceTitle: "Fixture owner" },
  ];
  repo.setJsonState("plex_server_devices", {
    ownerTokenHash: createHash("sha256").update(settings.auth.plex.ownerToken).digest("hex"),
    fetchedAtMs: Date.now(), servers: servers.map((server) => ({ ...server, accessToken: "admin-fixture-server-secret" })),
  });
  const realFetch = globalThis.fetch;
  globalThis.fetch = Object.assign(async (...args: Parameters<typeof fetch>) => {
    const url = new URL(args[0] instanceof Request ? args[0].url : String(args[0]));
    if (url.origin === "https://plex.tv" && url.pathname === "/api/resources") return new Response(`<MediaContainer>${servers.map((server) =>
      `<Device clientIdentifier="${server.machineId}" name="${server.name}" product="${server.product}" owned="${server.owned ? "1" : "0"}" provides="server" accessToken="admin-fixture-server-secret" />`).join("")}</MediaContainer>`,
      { headers: { "Content-Type": "application/xml" } });
    throw new Error("Admin fixture forbids provider network access");
  }, { preconnect: realFetch.preconnect });
  const viewer = repo.upsertUser({ provider: "local", providerUserId: "admin-browser-reader", username: "Fixture reader", displayName: "Actual Reader", isAdmin: false });
  repo.createSession(viewer.id, hashSessionToken("admin-reader-fixture"), null, "browser");
  const activeBook = repo.createBook({ title: "Fixture Active Book", author: "Operational author" });
  repo.createJob({ type: "acquire", bookId: activeBook.id, payload: { media: ["audio"] }, status: "queued", nextRunAt: new Date(Date.now() + 600_000).toISOString() });
  const downloadBook = repo.createBook({ title: "Fixture Active Download", author: "Operational author" });
  const release = repo.createRelease({ bookId: downloadBook.id, provider: "Fixture provider", title: "Fixture audiobook", mediaType: "audio",
    infoHash: "a".repeat(40), url: "https://provider.invalid/private-torrent", status: "downloading" });
  repo.createJob({ type: "download", bookId: downloadBook.id, releaseId: release.id, status: "queued", nextRunAt: new Date(Date.now() + 600_000).toISOString() });
  const failedBook = repo.createBook({ title: "Fixture Import Failure", author: "Operational author" });
  const failed = repo.createJob({ type: "import", bookId: failedBook.id, status: "failed", maxAttempts: 1 });
  db.query("UPDATE jobs SET error=?,attempt_count=1 WHERE id=?").run("Invalid import from https://provider.invalid/?token=admin-fixture-provider-secret", failed.id);
  const delayedBook = repo.createBook({ title: "Fixture Automatic Retry", author: "Operational author" });
  const delayed = repo.createJob({ type: "acquire", bookId: delayedBook.id, nextRunAt: new Date(Date.now() + 300_000).toISOString(), payload: { media: ["audio"] } });
  db.query("UPDATE jobs SET error=?,attempt_count=1 WHERE id=?").run("HTTP 429 cooldown from https://provider.invalid/?token=admin-fixture-provider-secret", delayed.id);
  const waiting = repo.movies.request(viewer.id, { ...fixtureMovie, tmdbId: 202, title: "Fixture Waiting For Offers" }, settings.movies.qualityPolicy);
  const acquisition = repo.movies.startAcquisition(waiting.targetId, "initial", (id) =>
    repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id }, status: "succeeded" }).id).acquisition;
  repo.movies.event(waiting.targetId, acquisition.id, "admin-browser-waiting", "waiting", "No suitable release yet; monitoring feed offers automatically");
  repo.movies.finishAcquisition(acquisition.id);
  repo.releaseInbox.ensureFeed("admin-fixture-feed", "Fixture provider", "movie", new Date().toISOString());
  repo.releaseInbox.fail("admin-fixture-feed", { provider: "Fixture provider", kind: "unavailable", status: 429,
    retryAt: Date.now() + 180_000, message: "HTTP 429 cooldown; provider retry data recorded" }, new Date().toISOString());
  repo.releaseInbox.ensureFeed("admin-fixture-tv", "Fixture provider", "tv", new Date().toISOString());
  repo.releaseInbox.succeed("admin-fixture-tv", "fixture-head", null, new Date().toISOString());
  repo.releaseInbox.ensureFeed("admin-fixture-gap", "History provider", "movie", new Date().toISOString());
  repo.releaseInbox.succeed("admin-fixture-gap", "fixture-head", "Earlier history could not be verified", new Date().toISOString());
  seedTransferMetricsFixture(repo, activeBook.id);
  settings.movies.seerr.url = "http://seerr:5055"; repo.updateSettings(settings);
  repo.setJsonState(tvSeerrMappingKey(seerrInstance(settings.movies.seerr.url)), [{ requestId: 734,
    title: "Tom and Jerry", tmdbId: 340782, tvdbId: 72860, approvedSeasons: [1, 2, 3], catalogSeasons: [0, 1940, 1950, 1960],
    reason: "Synthetic unresolved provider mapping", firstSeenAt: new Date().toISOString(), lastAttemptAt: new Date().toISOString() }]);
}
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
let advanceTvFixture: ((gib: number) => void) | undefined;
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
  if (process.env.TV_SEARCH_FIXTURE) {
    const missingCatalog: TvCatalogEntry = { ...catalog, key: "tvdb:200", tvdbId: 200, title: "Fixture with gaps", relations: [],
      episodes: catalog.episodes.map((episode, index) => ({ ...episode, key: `tvdb:${2001 + index}`, airDate: "2020-01-01",
        numbering: [{ scheme: "tvdb:aired", season: index + 1, episode: 1, absolute: null, source: "fixture:aired" }] })) };
    const withGaps = repo.tv.request(owner.id, missingCatalog, settings.movies.qualityPolicy, missingCatalog.episodes.map(ep => ep.key));
    const batch = queueTvAcquisition(repo, withGaps.requestId, "initial", [withGaps.episodeIds[0]!]).acquisition;
    const ref = { clientId: "transmission:fixture", nativeId: "a".repeat(40) };
    const location = { rootId: "tv", relativePath: "fixture-download" };
    const torrentId = repo.tv.bindDownload(batch.id, ref, location, "fixture-transfer");
    repo.tv.event(withGaps.targetId, batch.id, "tv-browser-downloading", "downloading", "Downloading fixture release");
    repo.tv.observeDownload(torrentId, { ref, location, state: "downloading", complete: false,
      bytesDone: 3 * 1024 ** 3, sizeBytes: 8 * 1024 ** 3, downloadBytesPerSecond: 2 * 1024 ** 2,
      message: null, observedAt: new Date().toISOString() });
    advanceTvFixture = gib => repo.tv.observeDownload(torrentId, { ref, location, state: "downloading", complete: false,
      bytesDone: gib * 1024 ** 3, sizeBytes: 8 * 1024 ** 3, downloadBytesPerSecond: 2 * 1024 ** 2,
      message: null, observedAt: new Date().toISOString() });
  }
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
const runtime = process.env.TV_FIXTURE ? null : await createJobExecutionRuntime(
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
if (process.env.OPS_SCALE_FIXTURE) seedActivityHistory(db, repo.executionDatabasePath());
let pumping = false;
const interval = setInterval(async () => {
  // TV browser actions exercise real queue receipts, never external acquisition.
  if (!runtime || pumping || process.env.ADMIN_FIXTURE || process.env.AUTH_FIXTURE) return;
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
  isAdmin: Boolean(process.env.SHADOW_FIXTURE || process.env.TV_FIXTURE || process.env.ADMIN_FIXTURE),
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
let acquisitionEvents = new AcquisitionEvents();
const createHandler = () => createPodibleFetchHandler(repo, Date.now(), null, {
  movieServices: fixture.services,
  acquisitionEvents,
});
let handler = createHandler();
const authFixture = { mode: "success", starts: 0, callbacks: 0 };
const server = Bun.serve({
  hostname: "127.0.0.1",
  port: Number(process.env.PORT ?? 0),
  ...(process.env.FIXTURE_TLS_KEY ? { tls: { key: Bun.file(process.env.FIXTURE_TLS_KEY), cert: Bun.file(process.env.FIXTURE_TLS_CERT!) } } : {}),
  async fetch(request, httpServer) {
    const url = new URL(request.url), pathname = url.pathname;
    if (pathname === acquisitionEventsPath) httpServer.timeout(request, 0);
    if (advanceTvFixture && pathname === "/__fixture/tv-transfer" && request.method === "POST") {
      if (url.searchParams.has("reconnect")) {
        acquisitionEvents.close();
        advanceTvFixture(6);
        acquisitionEvents = new AcquisitionEvents();
        handler = createHandler();
      } else {
        advanceTvFixture(5);
        acquisitionEvents.notify();
      }
      return Response.json({ ok: true });
    }
    if (process.env.AUTH_FIXTURE) {
      if (pathname === "/__fixture/auth-mode" && request.method === "POST") {
        const input = await request.json() as { mode?: string };
        if (!["success", "error", "no-cookie", "start-error", "invalid-origin"].includes(input.mode ?? "")) return new Response("Invalid fixture mode", { status: 400 });
        authFixture.mode = input.mode!;
        return Response.json({ ok: true });
      }
      if (pathname === "/__fixture/auth-state" && request.method === "GET") return Response.json(authFixture);
      if (pathname === "/login/plex/start" && request.method === "POST") {
        authFixture.starts++;
        if (authFixture.mode === "start-error") return Response.json({ error: "Fixture could not start Plex sign-in" }, { status: 502, headers: { "Cache-Control": "no-store" } });
        const callback = new URL("/__fixture/auth-callback", url);
        callback.searchParams.set("mode", authFixture.mode);
        callback.searchParams.set("redirectTo", safeReturnPath(url.searchParams.get("redirectTo")) ?? "/");
        const auth = new URL(authFixture.mode === "invalid-origin" ? "https://untrusted.invalid/auth" : "https://app.plex.tv/auth");
        auth.hash = "?" + new URLSearchParams({ forwardUrl: callback.href, code: "fixture-only" });
        return Response.json({ pinId: 1, authUrl: auth.href }, { headers: { "Cache-Control": "no-store" } });
      }
      if (pathname === "/__fixture/auth-callback" && request.method === "GET") {
        authFixture.callbacks++;
        const mode = url.searchParams.get("mode"), ok = mode === "success" || mode === "no-cookie";
        const params = new URLSearchParams({ ok: ok ? "1" : "0", redirectTo: safeReturnPath(url.searchParams.get("redirectTo")) ?? "/" });
        if (!ok) params.set("error", "Fixture Plex sign-in denied");
        const headers = new Headers({ Location: `/login/plex/result?${params}`, "Cache-Control": "no-store" });
        if (mode === "success") {
          const token = createSessionToken();
          repo.createSession(user.id, hashSessionToken(token), null, "browser");
          headers.append("Set-Cookie", buildSessionCookie(token, request));
        }
        return new Response(null, { status: 303, headers });
      }
    }
    if (request.method === "POST" && pathname === "/__fixture/add-release") {
      fixture.addOffer("manual", 1280);
      return Response.json({ ok: true });
    }
    if (request.method === "POST" && pathname === "/__fixture/add-upgrade") {
      fixture.addOffer("upgrade", 1920);
      return Response.json({ ok: true });
    }
    if (process.env.FIXTURE_TLS_KEY) {
      // Model Caddy terminating HTTPS while forwarding HTTP with the public Host.
      const internal = new URL(request.url);
      internal.protocol = "http:";
      const headers = new Headers(request.headers);
      headers.set("x-forwarded-proto", "https");
      return handler(new Request(internal.href, { method: request.method, headers, body: request.body }));
    }
    return handler(request);
  },
});
console.log(`SPA_FIXTURE_URL=${server.url}`);
process.on("SIGTERM", async () => {
  clearInterval(interval);
  server.stop(true);
  await runtime?.stop();
  db.close();
  await fs.rm(root, { recursive: true, force: true });
  process.exit(0);
});
