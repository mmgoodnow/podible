import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { prepareMovieStorage } from "../../src/movies/storage";
import { movieViewEntries, reconcileMovieView } from "../../src/movies/publication";
import { fixtureFacts, fixtureMovie } from "./movie-services";
import { fixtureTvEntry } from "./tv-catalog";

const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "recovery-spa-")));
const db = new Database(":memory:");
runMigrations(db);
const repo = new BooksRepo(db);
const settings = repo.ensureSettings();
settings.movies.managedRoot = path.join(root, "movies");
settings.movies.plexViewPath = "/fixture/movies";
settings.tv.managedRoot = path.join(root, "unavailable-tv-mount");
repo.updateSettings(settings);
const admin = repo.upsertUser({ provider: "local", providerUserId: "recovery-admin", username: "Recovery admin", isAdmin: true });
const member = repo.upsertUser({ provider: "local", providerUserId: "recovery-reader", username: "Recovery reader", isAdmin: false });
repo.createSession(admin.id, hashSessionToken("recovery-admin"), null, "browser");
repo.createSession(member.id, hashSessionToken("recovery-reader"), null, "browser");
const contentRoot = path.join(settings.movies.managedRoot, "objects");
await prepareMovieStorage(repo, contentRoot);
const roots = { contentRoot, viewRoot: path.join(settings.movies.managedRoot, "views", "plex", "movies"), serverViewRoot: settings.movies.plexViewPath };
const requested = repo.movies.request(member.id, { ...fixtureMovie, title: "Repairable movie" }, settings.movies.qualityPolicy);
const acquisition = repo.movies.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition;
const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`;
const canonical = path.join(contentRoot, relativePath);
await fs.mkdir(path.dirname(canonical), { recursive: true });
await fs.writeFile(canonical, "fixture canonical bytes");
const before = await fs.stat(canonical);
const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id, relativePath, sizeBytes: before.size, probe: {} }]);
repo.movies.selectVersion(requested.targetId, version, 0);
repo.movies.finishAcquisition(acquisition.id);
const healthy = repo.movies.request(member.id, { ...fixtureMovie, tmdbId: 102, title: "Healthy owned movie" }, settings.movies.qualityPolicy);
const oldAcquisition = repo.movies.startAcquisition(healthy.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition;
const oldId = randomUUID();
repo.movies.recordVersion(oldAcquisition.id, fixtureFacts(), [{ id: oldId, relativePath: `files/${oldId.slice(0, 2)}/${oldId}.mkv`, sizeBytes: 1, probe: {} }]);
repo.movies.finishAcquisition(oldAcquisition.id);
const healthyAcquisition = repo.movies.startAcquisition(healthy.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition;
const healthyId = randomUUID(), healthyRelative = `files/${healthyId.slice(0, 2)}/${healthyId}.mkv`;
const healthyCanonical = path.join(contentRoot, healthyRelative);
await fs.mkdir(path.dirname(healthyCanonical), { recursive: true });
await fs.writeFile(healthyCanonical, "healthy selected bytes");
const healthyBefore = await fs.stat(healthyCanonical);
const healthyVersion = repo.movies.recordVersion(healthyAcquisition.id, fixtureFacts(), [{ id: healthyId, relativePath: healthyRelative, sizeBytes: healthyBefore.size, probe: {} }]);
repo.movies.selectVersion(healthy.targetId, healthyVersion, 0);
repo.movies.finishAcquisition(healthyAcquisition.id);
await reconcileMovieView(repo.movies, roots);
const view = path.join(roots.viewRoot, movieViewEntries(repo.movies, roots).find(entry => entry.contentPath === canonical)!.relativePath);
const healthyView = path.join(roots.viewRoot, movieViewEntries(repo.movies, roots).find(entry => entry.contentPath === healthyCanonical)!.relativePath);
await fs.unlink(view);
for (let index = 0; index < 19; index++) repo.movies.request(member.id, {
  ...fixtureMovie, tmdbId: 200 + index, title: index === 0 ? "Initial pending movie" : `Pending movie ${index}`,
}, settings.movies.qualityPolicy);
repo.tv.requestSeasons(member.id, fixtureTvEntry(), settings.movies.qualityPolicy, [1]);
const handler = createPodibleFetchHandler(repo, Date.now());
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async request => {
  if (new URL(request.url).pathname === "/__fixture/stewardship")
    return Response.json({ receipts: db.query("SELECT key FROM app_state WHERE key LIKE 'torrent-stewardship:v1:collection:%'").all().length });
  if (new URL(request.url).pathname === "/__fixture/verify") {
    const after = await fs.stat(canonical);
    const healthyAfter = await fs.stat(healthyCanonical);
    return Response.json({ repaired: await fs.realpath(view).catch(() => null) === canonical,
      preserved: before.ino === after.ino && before.size === after.size && before.mtimeMs === after.mtimeMs,
      healthyPreserved: healthyBefore.ino === healthyAfter.ino && healthyBefore.size === healthyAfter.size && healthyBefore.mtimeMs === healthyAfter.mtimeMs && await fs.realpath(healthyView) === healthyCanonical,
      tvMountStillMissing: !await fs.lstat(settings.tv.managedRoot).then(() => true).catch(() => false) });
  }
  return handler(request);
} });
console.log(`RECOVERY_FIXTURE=${JSON.stringify({ url: server.url.origin })}`);
process.on("SIGTERM", () => { server.stop(true); db.close(); void fs.rm(root, { recursive: true, force: true }).then(() => process.exit(0)); });
