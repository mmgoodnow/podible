import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { chromium } from "playwright";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { initializeManagedContent } from "../../src/media/library-view";
import { publishMovieVersion } from "../../src/movies/publication";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { BooksRepo } from "../../src/repo";
import { AcquisitionEvents, acquisitionEventsPath } from "../../src/runtime/acquisition-events";

const root = await realpath(await mkdtemp(path.join(os.tmpdir(), "podible-notifications-browser-")));
const db = new Database(":memory:"), events = new AcquisitionEvents();
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
let server: ReturnType<typeof Bun.serve> | undefined;
try {
  runMigrations(db);
  const repo = new BooksRepo(db);
  for (const name of ["requester", "watcher"]) {
    const user = repo.upsertUser({ provider: "local", providerUserId: name, username: name, isAdmin: false });
    repo.createSession(user.id, hashSessionToken(`browser-notifications-${name}`), null, "browser");
    if (name === "requester") repo.movies.request(user.id, { tmdbId: 902, title: "Availability Fixture", originalTitle: "Availability Fixture",
      year: 2025, overview: "", originalLanguage: "en", posterUrl: null, imdbId: null }, defaultMovieQualityPolicy);
  }
  const targetId = repo.movies.targets()[0]!.id;
  const roots = { contentRoot: path.join(root, "objects"), viewRoot: path.join(root, "view"), serverViewRoot: "/plex/movies" };
  await initializeManagedContent(roots.contentRoot);
  await writeFile(path.join(roots.contentRoot, "movie.mkv"), "movie", { mode: 0o444 });
  const acquisition = repo.movies.startAcquisition(targetId, "initial", () => repo.createJob({ type: "reconcile" }).id).acquisition;
  const versionId = repo.movies.recordVersion(acquisition.id, { identityVerified: true, cut: "theatrical", source: "web",
    width: 1920, height: 1080, videoCodec: "h264", dynamicRange: "sdr", audioLanguages: ["en"], proper: false },
    [{ id: "notification-movie", relativePath: "movie.mkv", sizeBytes: 5, probe: {} }]);
  const handler = createPodibleFetchHandler(repo, Date.now(), null, { acquisitionEvents: events });
  server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(request, http) {
    if (new URL(request.url).pathname === acquisitionEventsPath) http.timeout(request, 0);
    return handler(request);
  } });
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const openAs = async (name: string) => {
    const context = await browser!.newContext({ viewport: { width: 390, height: 840 } });
    context.setDefaultTimeout(10_000);
    await context.addCookies([{ name: "podible_session", value: `browser-notifications-${name}`, url: server!.url.href, httpOnly: true }]);
    return await context.newPage();
  };
  const watcher = await openAs("watcher"), errors: string[] = [];
  watcher.on("pageerror", error => errors.push(error.message));
  const connected = watcher.waitForResponse(response => new URL(response.url()).pathname === acquisitionEventsPath && response.ok());
  await watcher.goto(new URL("/movies/902", server.url).href);
  await watcher.getByText(/Get an email when this movie is ready in Plex/).waitFor();
  assert.equal(await watcher.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Notifications" }).count(), 0);
  await watcher.getByRole("button", { name: "Notify me" }).click();
  await watcher.getByRole("button", { name: "Stop watching" }).waitFor();
  await watcher.getByRole("button", { name: "Stop watching" }).click();
  await watcher.getByRole("button", { name: "Notify me" }).click();
  await watcher.getByText(/Subscribed to email alerts/).waitFor();
  await connected;

  await publishMovieVersion({ repo: repo.movies, targetId, versionId, roots, library: { scan: async () => {}, verify: async () => true } });
  events.notify();
  await watcher.getByRole("button", { name: "Stop watching" }).waitFor({ state: "detached" });
  await watcher.getByText("Available in Plex", { exact: true }).first().waitFor();
  assert.equal(await watcher.evaluate("document.documentElement.scrollWidth > innerWidth"), false);

  assert.equal((await watcher.request.get(new URL("/notifications", server.url).href)).status(), 404);
  assert.deepEqual(errors, []);
  console.log("Notifications: email subscription, live availability update, and removed inbox passed in Chrome.");
} finally {
  await browser?.close();
  events.close();
  server?.stop(true);
  db.close();
  await rm(root, { recursive: true, force: true });
}
