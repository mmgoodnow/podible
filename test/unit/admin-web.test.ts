import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { runMigrations, finalizeJobJournalMigration } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createAdminWebRouter, normalizedAdminError } from "../../src/rpc/admin-web-router";
import type { AdminTransmissionReader } from "../../src/rpc/admin-torrents";
import { executeMethod, flattenRouter } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";
import type { JobLifecycle } from "../../src/worker/job-journal";
import type { PlexServerDevice } from "../../src/plex";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { prepareMovieStorage } from "../../src/movies/storage";
import { tvRouter } from "../../src/rpc/tv-router";
import { catalogRouter } from "../../src/rpc/catalog-router";
import { fixtureTvEntry } from "../fixtures/tv-catalog";
import { tvSeerrMappingKey } from "../../src/tv/sync";
import { seerrInstance } from "../../src/movies/seerr";

const NOW = Date.parse("2026-11-04T12:00:00.000Z");
const ownerToken = "private-owner-credential";
const providerKey = "private-provider-key";
const secretUrl = `https://private.invalid/api?apikey=${providerKey}`;
const server = (machineId = "server-a", overrides: Partial<PlexServerDevice> = {}): PlexServerDevice => ({
  machineId, name: `Plex ${machineId}`, product: "Plex Media Server", owned: true, provides: ["server"],
  accessToken: "private-server-credential", sourceTitle: null, ...overrides,
});
async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "admin-web-")));
  const db = new Database(":memory:");
  runMigrations(db);
  finalizeJobJournalMigration(db);
  const repo = new BooksRepo(db);
  const lifecycle = new Map<string, JobLifecycle>();
  repo.bindJobJournalReader({ read: (ids) => new Map(ids.filter((key) => lifecycle.has(key)).map((key) => [key, lifecycle.get(key)!])) });
  const user = repo.upsertUser({ provider: "plex", providerUserId: "private-provider-user-id", username: "admin", displayName: "Administrator", isAdmin: true });
  repo.createSession(user.id, "private-session-hash", null, "browser");
  const settings = repo.getSettings();
  settings.auth.plex.ownerToken = ownerToken;
  settings.auth.plex.machineId = "server-a";
  settings.prowlarr = { url: "https://private.invalid", apiKey: providerKey };
  settings.libraryRoot = path.join(root, "books");
  settings.movies.managedRoot = path.join(root, "movies");
  settings.tv.managedRoot = path.join(root, "missing-tv-mount");
  await fs.mkdir(settings.libraryRoot);
  repo.updateSettings(settings);
  await prepareMovieStorage(repo, path.join(settings.movies.managedRoot, "objects"));
  const ctx: RpcContext = { repo, startTime: NOW, request: new Request("https://podible.invalid/rpc"),
    session: { user_id: user.id, is_admin: 1 } as RpcContext["session"] };
  let now = NOW;
  let fetches = 0;
  let fetcher: (input: typeof settings) => Promise<PlexServerDevice[]> = async () => [server(), server("server-b", { owned: false, sourceTitle: "Shared owner" })];
  const router = createAdminWebRouter({ now: () => now, transmission: null, fetchPlexServers: async (input) => { fetches++; return fetcher(input); } });
  const setLifecycle = (jobId: number, patch: Partial<JobLifecycle>) => {
    const key = repo.getJobExecution(jobId)?.run_id ?? `fixture:${jobId}`;
    const value = { status: "queued" as const, error: null, attempt_count: 0, next_run_at: null, updated_at: new Date(now).toISOString(), ...lifecycle.get(key), ...patch };
    lifecycle.set(key, value);
    repo.attachJobExecution(jobId, key);
  };
  const job = (input: Parameters<BooksRepo["createJob"]>[0], patch: Partial<JobLifecycle> = {}) => {
    const row = repo.createJob(input); setLifecycle(row.id, patch); return repo.getJob(row.id)!;
  };
  const cache = (ageMs = 0, token = ownerToken, devices = [server()]) => repo.setJsonState("plex_server_devices", {
    ownerTokenHash: createHash("sha256").update(token).digest("hex"), fetchedAtMs: now - ageMs, servers: devices,
  });
  return { root, db, repo, settings, user, ctx, router, job, setLifecycle, cache, lifecycle,
    fetches: () => fetches, setFetcher: (next: typeof fetcher) => { fetcher = next; }, setNow: (value: number) => { now = value; },
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
function movieAcquisition(f: Fixture, n: number, status: JobLifecycle["status"] = "running") {
  const requested = f.repo.movies.request(f.user.id, { tmdbId: n, title: `Movie ${n}`, originalTitle: `Movie ${n}`, year: 2024,
    overview: "", originalLanguage: "en", imdbId: null, posterUrl: null }, defaultMovieQualityPolicy);
  const acquisition = f.repo.movies.startAcquisition(requested.targetId, "initial", () => f.job({ type: "movie_acquire" }, { status }).id).acquisition;
  return { ...requested, acquisition };
}

const privateClientId = "transmission:11111111-1111-4111-8111-111111111111";
async function managedSource(f: Fixture, acquisitionId: number, media: "movie" | "tv", hash: string, relativePath: string, clientId = privateClientId) {
  const location = { rootId: media === "movie" ? "movies" : "tv", relativePath };
  const directory = path.join(media === "movie" ? f.settings.movies.managedRoot : f.settings.tv.managedRoot, "downloads", relativePath);
  await fs.mkdir(directory, { recursive: true });
  const repo = media === "movie" ? f.repo.movies : f.repo.tv;
  const ref = { clientId, nativeId: hash };
  const id = repo.bindDownload(acquisitionId, ref, location, `offer:${hash}`);
  repo.observeDownload(id, { ref, location, state: "downloading", complete: false, bytesDone: 0, sizeBytes: 100,
    downloadBytesPerSecond: 0, message: `Torrent engine reported a tracker or storage error ${secretUrl}`, observedAt: new Date(NOW - 600_000).toISOString() });
  return { id, directory, hash, location };
}
function liveTorrent(hashString: string, downloadDir: string, overrides: Record<string, unknown> = {}) {
  return { hashString, downloadDir, labels: ["Podible Movies"], status: 4, leftUntilDone: 60, sizeWhenDone: 100,
    rateDownload: 5, error: 0, errorString: "", ...overrides };
}
function liveInventory(f: Fixture, rows: unknown[], clientId = privateClientId) {
  const calls: { method: string; args: Record<string, unknown> }[] = [];
  const reader: AdminTransmissionReader = { clientId, rpc: { call: async (method, args = {}) => {
    calls.push({ method, args });
    return { torrents: rows };
  } } };
  const router = createAdminWebRouter({ now: () => NOW, transmission: reader });
  return { calls, read: () => executeMethod(router.routes.ops, f.ctx, {}) };
}

describe("admin web authorization and metadata", () => {
  test("only five scoped methods exist; every method rejects missing/non-admin sessions before effects", async () => {
    await using f = await fixture();
    expect(Object.keys(f.router.routes)).toEqual(["users", "plexServers", "selectPlexServer", "ops", "refresh"]);
    for (const [name, method] of Object.entries(flattenRouter(f.router))) {
      const input = name === "selectPlexServer" ? { machineId: "server-a" } : {};
      await expect(executeMethod(method, { ...f.ctx, session: null }, input)).rejects.toMatchObject({ code: -32001 });
      await expect(executeMethod(method, { ...f.ctx, session: { user_id: f.user.id, is_admin: 0 } as RpcContext["session"] }, input)).rejects.toMatchObject({ code: -32003 });
    }
    expect(f.fetches()).toBe(0);
    expect(f.repo.listJobCommands()).toEqual([]);
    expect(f.router.routes.users.readOnly).toBe(true);
    expect(f.router.routes.plexServers.readOnly).toBe(true);
    expect(f.router.routes.ops.readOnly).toBe(true);
    expect(f.router.routes.refresh.readOnly).toBe(false);
    expect(f.router.routes.selectPlexServer.readOnly).toBe(false);
    await expect(executeMethod(f.router.routes.ops, f.ctx, { table: "users" })).rejects.toMatchObject({ code: -32602 });
    await expect(executeMethod(f.router.routes.refresh, f.ctx, { type: "acquire", maxAttempts: 999 })).rejects.toMatchObject({ code: -32602 });
  });

  test("users returns explicit metadata only, with no account IDs, sessions, settings, or writes", async () => {
    await using f = await fixture();
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.users, f.ctx, {});
    expect(result.users).toHaveLength(1);
    expect(result.users[0]).toMatchObject({ id: f.user.id, provider: "plex", username: "admin", displayName: "Administrator", isAdmin: true });
    expect(Object.keys(result.users[0]!).sort()).toEqual(["createdAt", "displayName", "id", "isAdmin", "provider", "updatedAt", "username"]);
    expect(JSON.stringify(result)).not.toMatch(/private-|token|credential|session|provider_user_id/);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  });
});

describe("Plex discovery and exact selection", () => {
  test("current owner cache is reused, strips old cached access tokens, and preserves selected server", async () => {
    await using f = await fixture();
    f.cache();
    const result = await executeMethod(f.router.routes.plexServers, f.ctx, {});
    expect(result).toMatchObject({ ownerTokenConfigured: true, ownerTokenExpired: null, selectedMachineId: "server-a", source: "cache", error: null });
    expect(result.servers[0]).toEqual({ machineId: "server-a", name: "Plex server-a", product: "Plex Media Server", owned: true, sourceTitle: null });
    expect(f.fetches()).toBe(0);
    expect(JSON.stringify(result)).not.toMatch(/private-|accessToken|ownerTokenHash/);
  });

  test("fresh discovery uses existing owner settings, filters client devices, and stores metadata only", async () => {
    await using f = await fixture();
    f.setFetcher(async (settings) => {
      expect(settings.auth.plex.ownerToken).toBe(ownerToken);
      return [server(), server("client", { provides: ["player"] }), server("server-b", { name: `Name ${secretUrl}`, sourceTitle: ownerToken })];
    });
    const result = await executeMethod(f.router.routes.plexServers, f.ctx, {});
    expect(result.source).toBe("fresh");
    expect(result.servers).toHaveLength(2);
    expect(result.selectedMachineId).toBe("server-a");
    expect(JSON.stringify(result)).not.toContain(providerKey);
    expect(JSON.stringify(result)).not.toContain(ownerToken);
    expect(JSON.stringify(result)).not.toContain("https://");
    const cached = JSON.stringify(f.repo.getJsonState("plex_server_devices"));
    expect(cached).not.toContain("private-server-credential");
    expect(cached).not.toContain("accessToken");
  });

  test("failed discovery labels stale cache and never reuses another owner identity's cache", async () => {
    await using f = await fixture();
    f.cache(6 * 60_000);
    f.setFetcher(async () => { throw new Error(`HTTP 503 ${secretUrl} ${ownerToken}`); });
    let result = await executeMethod(f.router.routes.plexServers, f.ctx, {});
    expect(result.source).toBe("stale-cache");
    expect(result.error).toContain("discovery failed");
    expect(result.servers).toHaveLength(1);
    const settings = f.repo.getSettings(); settings.auth.plex.ownerToken = "rotated-owner"; f.repo.updateSettings(settings);
    result = await executeMethod(f.router.routes.plexServers, f.ctx, {});
    expect(result.source).toBe("unavailable");
    expect(result.servers).toEqual([]);
    expect(JSON.stringify(result)).not.toContain(providerKey);
    expect(JSON.stringify(result)).not.toContain("private.invalid");
  });

  test("absent token performs no lookup; token expiry is metadata, not an exposed credential", async () => {
    await using f = await fixture();
    let settings = f.repo.getSettings(); settings.auth.plex.ownerToken = ""; f.repo.updateSettings(settings);
    expect(await executeMethod(f.router.routes.plexServers, f.ctx, {})).toMatchObject({ source: "not-configured", ownerTokenConfigured: false, ownerTokenExpired: null });
    expect(f.fetches()).toBe(0);
    settings = f.repo.getSettings();
    settings.auth.plex.ownerToken = `e30.${Buffer.from(JSON.stringify({ exp: 1 })).toString("base64url")}.signature`;
    f.repo.updateSettings(settings);
    expect((await executeMethod(f.router.routes.plexServers, f.ctx, {})).ownerTokenExpired).toBe(true);
  });

  test("selection validates fresh exact server IDs, not arbitrary or cache-only IDs", async () => {
    await using f = await fixture();
    f.cache(0, ownerToken, [server("cache-only")]);
    f.setFetcher(async () => [server(), server("client-only", { provides: ["player"] })]);
    for (const machineId of ["cache-only", "client-only", "unknown", "server-a ", secretUrl])
      await expect(executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId })).rejects.toMatchObject({ code: -32602 });
    expect(f.repo.getSettings().auth.plex.machineId).toBe("server-a");
    await expect(executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId: "server-a", settings: {} })).rejects.toMatchObject({ code: -32602 });
    f.setFetcher(async () => [server(), server()]);
    await expect(executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId: "server-a" })).rejects.toMatchObject({ code: -32602 });
  });

  test("selection preserves unrelated concurrent settings edits and returns only selected ID", async () => {
    await using f = await fixture();
    f.setFetcher(async () => {
      const current = f.repo.getSettings(); current.feed.title = "Concurrent title";
      current.movies.plexToken = "shared-movie-plex-credential"; f.repo.updateSettings(current);
      return [server("server-b", { owned: false, sourceTitle: "Shared account" })];
    });
    expect(await executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId: "server-b" })).toEqual({ selectedMachineId: "server-b" });
    const settings = f.repo.getSettings();
    expect(settings.feed.title).toBe("Concurrent title");
    expect(settings.movies.plexToken).toBe("shared-movie-plex-credential");
    expect(settings.auth.plex.ownerToken).toBe(ownerToken);
    expect(settings.prowlarr).toEqual(f.settings.prowlarr);
    expect(settings.auth.plex.machineId).toBe("server-b");
  });

  test("owner rotation and a failed atomic cache write cannot commit a stale or partial selection", async () => {
    await using f = await fixture();
    f.setFetcher(async () => {
      const settings = f.repo.getSettings(); settings.auth.plex.ownerToken = "rotated-owner"; f.repo.updateSettings(settings);
      return [server("server-b")];
    });
    await expect(executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId: "server-b" })).rejects.toMatchObject({ code: -32000 });
    expect(f.repo.getSettings().auth.plex.machineId).toBe("server-a");
    expect(f.repo.getJsonState("plex_server_devices")).toBeNull();
    f.setFetcher(async () => [server("server-b")]);
    f.db.exec(`CREATE TRIGGER block_plex_cache BEFORE INSERT ON app_state WHEN NEW.key='plex_server_devices'
      BEGIN SELECT RAISE(ABORT, 'cache unavailable'); END`);
    await expect(executeMethod(f.router.routes.selectPlexServer, f.ctx, { machineId: "server-b" })).rejects.toThrow("cache unavailable");
    expect(f.repo.getSettings().auth.plex.machineId).toBe("server-a");
  });
});

describe("operational facts from the execution authority", () => {
  test("deferred TV season disagreement stays visible by show even after a healthy sync", async () => {
    await using f = await fixture();
    const settings = f.repo.getSettings(); settings.movies.seerr.url = "http://seerr:5055"; f.repo.updateSettings(settings);
    const key = tvSeerrMappingKey(seerrInstance(settings.movies.seerr.url));
    f.repo.setJsonState(key, [{ requestId: 734, title: "Tom and Jerry", tmdbId: 340782, tvdbId: 72860,
      approvedSeasons: [1, 2, 3], catalogSeasons: [0, 1940, 1950, 1960], reason: `Internal evidence ${secretUrl}`,
      firstSeenAt: new Date(NOW).toISOString(), lastAttemptAt: new Date(NOW).toISOString() }]);
    f.job({ type: "tv_sync" }, { status: "succeeded" });
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    const issue = result.incidents.find(item => item.title === "Tom and Jerry");
    expect(issue).toMatchObject({ code: "tv-season-mapping", jobId: null, href: "/tv/340782", recovery: "unknown" });
    expect(issue!.detail).toContain("1940, 1950, 1960");
    expect(issue!.detail).toContain("Other requests can continue");
    expect(JSON.stringify(result)).not.toContain(secretUrl);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    f.repo.setJsonState(key, []);
    expect((await executeMethod(f.router.routes.ops, f.ctx, {})).incidents.some(item => item.code === "tv-season-mapping")).toBe(false);
  });
  test("TV sync season-numbering failures explain the cause without exposing raw diagnostics", async () => {
    await using f = await fixture();
    const job = f.job({ type: "tv_sync" }, { status: "failed",
      error: `Seerr request 734 has no explicitly mapped episodes in its approved seasons ${secretUrl} ${ownerToken}` });
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.ops, f.ctx, { includeJobs: true });
    expect(result.incidents).toContainEqual(expect.objectContaining({ jobId: job.id, title: "TV request sync",
      code: "tv-season-mapping", detail: expect.stringContaining("do not match Sonarr's episode numbering"), recovery: "manual-retry" }));
    expect(result.incidents.find(item => item.jobId === job.id)!.detail).toContain("Existing downloads and Plex content are unchanged");
    expect(result.diagnostics.recentJobs.find(item => item.id === job.id)?.error?.code).toBe("tv-season-mapping");
    expect(JSON.stringify(result)).not.toContain(secretUrl);
    expect(JSON.stringify(result)).not.toContain(ownerToken);
    // Storage capacity can coincidentally contain these digits; inspect diagnostic text, not unrelated counters.
    expect(JSON.stringify({ incidents: result.incidents, diagnostics: result.diagnostics })).not.toContain("734");
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  });

  test("only the current TV sync failure is actionable and a newer successful sync clears it", async () => {
    await using f = await fixture();
    const error = "Seerr request 734 has no explicitly mapped episodes in its approved seasons";
    const historical = f.job({ type: "tv_sync" }, { status: "failed", error });
    const latest = f.job({ type: "tv_sync" }, { status: "failed", error });
    let result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.incidents.filter(item => item.code === "tv-season-mapping").map(item => item.jobId)).toEqual([latest.id]);
    expect(result.incidents.some(item => item.jobId === historical.id)).toBe(false);
    f.job({ type: "tv_sync" }, { status: "succeeded" });
    result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.incidents.some(item => item.code === "tv-season-mapping")).toBe(false);
  });

  test("Ops leads real active work and failures without treating saved torrent observations as current downloads", async () => {
    await using f = await fixture();
    const book = f.repo.createBook({ title: "Book in flight", author: "Writer" });
    const wake = new Date(NOW + 60_000).toISOString();
    f.job({ type: "acquire", bookId: book.id }, { status: "running" });
    const download = f.job({ type: "download", bookId: book.id }, { next_run_at: wake });
    const failed = f.job({ type: "import", bookId: book.id }, { status: "failed", error: `bad import ${secretUrl}`, attempt_count: 2 });
    const retry = f.job({ type: "metadata_hydration" }, { status: "queued", next_run_at: wake, error: `HTTP 503 ${secretUrl}`, attempt_count: 1 });
    const movie = movieAcquisition(f, 101);
    const torrentId = f.repo.movies.bindDownload(movie.acquisition.id, { clientId: "private-client", nativeId: "a".repeat(40) }, { rootId: "movies", relativePath: "owned" }, "offer");
    f.repo.movies.observeDownload(torrentId, { ref: { clientId: "private-client", nativeId: "a".repeat(40) }, location: { rootId: "movies", relativePath: "owned" },
      state: "downloading", complete: false, bytesDone: 40, sizeBytes: 100, downloadBytesPerSecond: 5, message: secretUrl, observedAt: new Date(NOW).toISOString() });
    f.repo.releaseInbox.ensureFeed("feed", `Provider ${secretUrl}`, "movie", new Date(NOW).toISOString());
    f.repo.releaseInbox.fail("feed", { provider: "Provider", kind: "unavailable", status: 429, retryAt: NOW + 120_000, message: `HTTP 429 ${secretUrl}` }, new Date(NOW).toISOString());
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.activeAcquisitions.map((item) => item.title)).toContain("Book in flight");
    expect(result.activeAcquisitions.map((item) => item.media)).toContain("movie");
    expect(result.downloads.find((item) => item.job.id === download.id)).toMatchObject({ telemetry: "unknown", bytesDone: null });
    expect(result.downloads.find((item) => item.media === "movie")).toBeUndefined();
    expect(result.diagnostics.warnings.join(" ")).toContain("recorded observations are not counted");
    expect(result.incidents.find((item) => item.jobId === failed.id)).toMatchObject({ recovery: "manual-retry", nextWakeAt: null });
    expect(result.incidents.find((item) => item.jobId === retry.id)).toMatchObject({ code: "provider-unavailable", recovery: "automatic-retry", nextWakeAt: wake });
    expect(result.feeds[0]).toMatchObject({ state: "cooldown", retryAt: new Date(NOW + 120_000).toISOString(), nextPollAt: null });
    expect(result.summary.nextWakeAt).toBe(wake);
    expect(result.diagnostics.recentJobs).toEqual([]);
    expect(JSON.stringify(result)).not.toContain(providerKey);
    expect(JSON.stringify(result)).not.toContain("https://");
    expect(JSON.stringify(result)).not.toContain("payload_json");
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect((f.db.query("PRAGMA table_info(jobs)").all() as { name: string }[]).map((column) => column.name)).not.toContain("status");
  });

  test("available movies, completed no-match waits, withdrawals and superseded failures are not incidents", async () => {
    await using f = await fixture();
    const waiting = movieAcquisition(f, 201, "succeeded");
    f.repo.movies.finishAcquisition(waiting.acquisition.id);
    f.repo.movies.event(waiting.targetId, waiting.acquisition.id, "waiting", "waiting", "No matching release; waiting for RSS");
    const available = movieAcquisition(f, 202, "failed");
    f.setLifecycle(available.acquisition.job_id!, { status: "failed", error: "Publication retry failed" });
    const version = f.repo.movies.recordVersion(available.acquisition.id, { identityVerified: true, cut: "theatrical", source: "web", width: 1920,
      height: 1080, videoCodec: "h264", dynamicRange: "sdr", audioLanguages: ["en"], proper: false },
    [{ id: randomUUID(), relativePath: "files/available.mkv", sizeBytes: 10, probe: {} }]);
    f.repo.movies.selectVersion(available.targetId, version, 0);
    const withdrawn = movieAcquisition(f, 203, "failed");
    f.setLifecycle(withdrawn.acquisition.job_id!, { status: "failed", error: "old failure" });
    f.repo.movies.withdrawRequest(withdrawn.requestId);
    const superseded = f.job({ type: "full_library_refresh" }, { status: "failed", error: "old refresh failure" });
    f.job({ type: "full_library_refresh" }, { status: "succeeded" });
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.summary.waiting).toBe(1);
    expect(result.incidents).toEqual([]);
    expect(result.activeAcquisitions).toEqual([]);
    expect(result.diagnostics.recentJobs).toEqual([]);
    expect(result.incidents.some((incident) => incident.jobId === superseded.id)).toBe(false);
  });

  test("only live owned tracker errors are actionable and recovery follows execution rather than inventing manual retry", async () => {
    await using f = await fixture();
    const movie = movieAcquisition(f, 401);
    const source = await managedSource(f, movie.acquisition.id, "movie", "d".repeat(40), "owned");
    const rows = [liveTorrent(source.hash, source.directory, { error: 1, errorString: `Tracker rejected: Your client version is not whitelisted. ${secretUrl}` })];
    const inventory = liveInventory(f, rows);
    let result = await inventory.read();
    expect(result.downloads[0]!.problem).toMatchObject({ code: "tracker-client-not-allowed" });
    expect(result.downloads[0]!.telemetry).toBe("live");
    expect(result.incidents[0]!.detail).toContain("allowed client/version list");
    expect(result.incidents[0]!.recovery).toBe("running");
    expect(JSON.stringify(result)).not.toContain(providerKey);
    rows[0]!.errorString = "Torrent engine reported a tracker or storage error";
    result = await inventory.read();
    expect(result.downloads[0]!.problem).toMatchObject({ code: "torrent-tracker-or-storage" });
    expect(result.downloads[0]!.problem!.message).toContain("Exact cause is unknown");
    expect(result.downloads[0]!.problem!.message).toContain("private managed client currently reports");
    expect(f.fetches()).toBe(0);
  });

  test("retained stopped sources cannot create an active download or manual-retry incident while a fallback is downloading", async () => {
    await using f = await fixture();
    const movie = movieAcquisition(f, 402);
    // The healthy fallback deliberately has the lower row ID and appears last in RPC order.
    const fallback = await managedSource(f, movie.acquisition.id, "movie", "9".repeat(40), "fallback");
    const retained = await managedSource(f, movie.acquisition.id, "movie", "1".repeat(40), "retained");
    const inventory = liveInventory(f, [
      liveTorrent(retained.hash, retained.directory, { status: 0, error: 0, errorString: `Old tracker rejection ${secretUrl}`, rateDownload: 0 }),
      liveTorrent(fallback.hash, fallback.directory, { leftUntilDone: 2 * 1024 ** 3, sizeWhenDone: 4 * 1024 ** 3,
        rateDownload: 2 * 1024 ** 2, errorString: "Stale error string with cleared error code" }),
    ]);
    const before = f.db.serialize();
    const result = await inventory.read();
    expect(result.downloads).toHaveLength(1);
    expect(result.downloads[0]).toMatchObject({ key: `movie:${movie.acquisition.id}:torrent:${fallback.id}`, telemetry: "live",
      bytesDone: 2 * 1024 ** 3, sizeBytes: 4 * 1024 ** 3, bytesPerSecond: 2 * 1024 ** 2, observedAt: new Date(NOW).toISOString(), problem: null });
    expect(result.summary.downloads).toBe(1);
    expect(result.incidents).toEqual([]);
    expect(result.activeAcquisitions[0]!.job.recovery).toBe("running");
    expect(inventory.calls).toHaveLength(1);
    expect(inventory.calls[0]!.method).toBe("torrent-get");
    expect(new Set(inventory.calls[0]!.args.ids as string[])).toEqual(new Set([retained.hash, fallback.hash]));
    expect(f.db.serialize()).toEqual(before);
    expect(JSON.stringify(result)).not.toContain(providerKey);
    expect(JSON.stringify(result)).not.toContain(retained.directory);
    expect(JSON.stringify(result)).not.toContain(privateClientId);
  });

  test("missing private identity and unavailable RPC leave historical activity and errors unverified, not actionable", async () => {
    await using f = await fixture();
    const movie = movieAcquisition(f, 403);
    await managedSource(f, movie.acquisition.id, "movie", "c".repeat(40), "unavailable");
    let calls = 0;
    const unavailable: AdminTransmissionReader = { clientId: privateClientId, rpc: { call: async () => {
      calls++; throw new Error(`HTTP 503 ${secretUrl} private-transmission-password`);
    } } };
    for (const transmission of [null, unavailable]) {
      const router = createAdminWebRouter({ now: () => NOW, transmission });
      const before = f.db.serialize();
      const result = await executeMethod(router.routes.ops, f.ctx, {});
      expect(result.downloads).toEqual([]);
      expect(result.incidents).toEqual([]);
      expect(result.activeAcquisitions).toHaveLength(1);
      expect(result.diagnostics.warnings.join(" ")).toContain("Current managed torrent activity could not be verified");
      expect(f.db.serialize()).toEqual(before);
      expect(JSON.stringify(result)).not.toMatch(/private-transmission-password|private-provider-key|https:\/\//);
    }
    expect(calls).toBe(1);
  });

  test("foreign clients, hashes, labels, roots, layouts and ambiguous or symlinked bindings fail closed", async () => {
    for (const failure of ["client", "hash", "label", "root", "traversal", "directory", "duplicate", "symlink"] as const) {
      await using f = await fixture();
      const movie = movieAcquisition(f, 404);
      const source = await managedSource(f, movie.acquisition.id, "movie", "d".repeat(40), "guarded",
        failure === "client" ? "transmission:22222222-2222-4222-8222-222222222222" : privateClientId);
      const rows = [liveTorrent(source.hash, source.directory, { error: 1, errorString: `Do not expose ${secretUrl}` })];
      if (failure === "hash") rows[0]!.hashString = "e".repeat(40);
      if (failure === "label") rows[0]!.labels = ["Podible TV"];
      if (failure === "directory") rows[0]!.downloadDir = path.join(f.settings.movies.managedRoot, "downloads", "another");
      if (failure === "duplicate") rows.push({ ...rows[0]! });
      if (failure === "root" || failure === "traversal")
        f.db.query("UPDATE media_torrents SET location_json=? WHERE id=?")
          .run(JSON.stringify(failure === "root" ? { rootId: "tv", relativePath: "guarded" } : { rootId: "movies", relativePath: "../guarded" }), source.id);
      if (failure === "symlink") {
        await fs.rename(source.directory, `${source.directory}-real`);
        await fs.symlink(`${source.directory}-real`, source.directory);
      }
      const inventory = liveInventory(f, rows);
      const before = f.db.serialize();
      const result = await inventory.read();
      expect(result.downloads).toEqual([]);
      expect(result.incidents).toEqual([]);
      expect(result.diagnostics.warnings.join(" ")).toContain("could not be verified");
      expect(JSON.stringify(result)).not.toContain(providerKey);
      expect(f.db.serialize()).toEqual(before);
      expect(inventory.calls.every((call) => call.method === "torrent-get")).toBe(true);
    }
  });

  test("live inventory stops at its global binding budget without choosing a newest source or issuing unbounded reads", async () => {
    await using f = await fixture();
    const movie = movieAcquisition(f, 405);
    for (let n = 0; n < 101; n++) f.repo.movies.bindDownload(movie.acquisition.id,
      { clientId: privateClientId, nativeId: n.toString(16).padStart(40, "0") }, { rootId: "movies", relativePath: `source-${n}` }, `offer-${n}`);
    const inventory = liveInventory(f, []);
    const result = await inventory.read();
    expect(inventory.calls).toEqual([]);
    expect(result.downloads).toEqual([]);
    expect(result.incidents).toEqual([]);
    expect(result.diagnostics.truncated).toBe(true);
    expect(result.diagnostics.warnings.join(" ")).toContain("bounded read budget");
  });

  test("TV future episodes remain informational while real TV acquisitions/downloads are visible", async () => {
    await using f = await fixture();
    const rows: ReturnType<typeof liveTorrent>[] = [];
    let currentRequestId = 0;
    for (const [n, date] of [[301, "2020-01-01"], [302, "2099-01-01"]] as const) {
      const request = f.repo.tv.request(f.user.id, { key: `tvdb:${n}`, tvdbId: n, tmdbId: null, title: `Series ${n}`, aliases: [], year: 2020, anime: false,
        episodes: [{ key: `tvdb:${n}01`, title: "Episode", airDate: date, runtimeMinutes: 30,
          numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null }] }], relations: [] }, {}, [`tvdb:${n}01`]);
      const acquisition = f.repo.tv.startAcquisition(request.targetId, "initial", () => f.job({ type: "tv_acquire" }, { status: n === 301 ? "running" : "succeeded" }).id, request.requestId).acquisition;
      if (n === 301) currentRequestId = request.requestId;
      if (n === 302) f.repo.tv.finishAcquisition(acquisition.id);
      else {
        const source = await managedSource(f, acquisition.id, "tv", "b".repeat(40), "owned");
        rows.push(liveTorrent(source.hash, source.directory, { labels: ["Podible TV"] }));
      }
    }
    const result = await liveInventory(f, rows).read();
    expect(result.summary.waiting).toBe(1);
    expect(result.activeAcquisitions.find((item) => item.media === "tv")).toMatchObject({ title: "Series 301", href: `/tv/requests/${currentRequestId}` });
    expect(result.downloads.find((item) => item.media === "tv")).toMatchObject({ telemetry: "live", bytesDone: 40, problem: null });
    expect(result.incidents).toEqual([]);
  });

  test("TV activity links use the actor's exact request, shared published work, or no inaccessible private destination", async () => {
    await using f = await fixture();
    const foreign = f.repo.upsertUser({ provider: "local", providerUserId: "activity-foreign", username: "Other viewer", isAdmin: false });
    const cases: { jobId: number; href: string | null; requestId: number; entryId: number; owner: boolean; published: boolean }[] = [];
    for (const [index, [owner, published]] of [[true, false], [true, true], [false, false], [false, true]].entries()) {
      const number = 900 + index;
      const episodes = [1, 2].map(episode => ({ key: `tvdb:${number}${episode}`, title: "Episode", airDate: "2020-01-01", runtimeMinutes: 30,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode, absolute: null }] }));
      const request = f.repo.tv.request(owner ? f.user.id : foreign.id, { key: `tvdb:${number}`, tvdbId: number, tmdbId: null,
        title: `Activity TV ${number}`, aliases: [], year: 2020, anime: false, episodes, relations: [] }, {}, episodes.map(episode => episode.key));
      const acquisition = f.repo.tv.startAcquisition(request.targetId, "initial", () => f.job({ type: "tv_acquire" }, { status: "running" }).id, request.requestId).acquisition;
      if (published) {
        const fileId = randomUUID();
        const version = f.repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id: fileId, relativePath: `files/${fileId}.mkv`,
          sizeBytes: 10, probe: {}, episodeIds: [request.episodeIds[0]!] }]);
        f.repo.tv.stagePublication(request.targetId, version, 0);
        f.repo.tv.promotePublication(request.targetId, version, 0);
        f.repo.tv.clearPublication(request.targetId, version);
      }
      cases.push({ jobId: acquisition.job_id!, requestId: request.requestId, entryId: request.entryId, owner, published,
        href: owner ? `/tv/requests/${request.requestId}` : published ? `/tv/library/${request.entryId}` : null });
    }
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    for (const entry of cases) {
      expect(result.activeAcquisitions.find(work => work.job.id === entry.jobId)?.href).toBe(entry.href);
      if (entry.owner) expect((await executeMethod(tvRouter.routes.myRequest, f.ctx, { requestId: entry.requestId })).request.requestId).toBe(entry.requestId);
      else await expect(executeMethod(tvRouter.routes.myRequest, f.ctx, { requestId: entry.requestId })).rejects.toMatchObject({ code: -32003 });
      if (entry.published) expect((await executeMethod(catalogRouter.routes.tvEntry, f.ctx, { entryId: entry.entryId })).availableEpisodes).toBe(1);
      else await expect(executeMethod(catalogRouter.routes.tvEntry, f.ctx, { entryId: entry.entryId })).rejects.toMatchObject({ code: -32602 });
      f.setLifecycle(entry.jobId, { status: "failed", error: "TV source unavailable" });
    }
    const failed = await executeMethod(f.router.routes.ops, f.ctx, {});
    for (const entry of cases) expect(failed.incidents.find(incident => incident.jobId === entry.jobId)?.href).toBe(entry.href);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  });

  test("pending cancellation cannot masquerade as active acquisition or a future automatic wake", async () => {
    await using f = await fixture();
    const job = f.job({ type: "acquire" }, { next_run_at: new Date(NOW + 60_000).toISOString(), error: "Old retry error" });
    f.repo.markJobCancelled(job.id, "Operator cancelled");
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.activeAcquisitions).toEqual([]);
    expect(result.incidents).toEqual([]);
    expect(result.summary.nextWakeAt).toBeNull();
  });

  test("storage separates capacity from mount identity, unknown/missing paths never become success", async () => {
    await using f = await fixture();
    let result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.storage.find((store) => store.media === "book")).toMatchObject({ capacityState: "known", identityState: "unknown" });
    expect(result.storage.find((store) => store.media === "movie")).toMatchObject({ capacityState: "known", identityState: "verified" });
    expect(result.storage.find((store) => store.media === "tv")).toMatchObject({ capacityState: "unknown", identityState: "unknown", totalBytes: null, availableBytes: null });
    await fs.writeFile(path.join(f.settings.movies.managedRoot, "objects", ".podible-content.json"), JSON.stringify({ id: randomUUID() }));
    result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.storage.find((store) => store.media === "movie")).toMatchObject({ capacityState: "known", identityState: "mismatch" });
    expect(result.incidents.some((item) => item.code === "storage-identity")).toBe(true);
  });

  test("recent diagnostics are lazy, bounded, normalized and include TV/feed job types", async () => {
    await using f = await fixture();
    for (let n = 0; n < 35; n++) f.job({ type: n % 2 ? "tv_feed_match" : "indexer_feed_sync" }, { status: "succeeded", error: null });
    f.job({ type: "tv_shadow_sync" }, { status: "failed", error: `Error ${secretUrl} ${ownerToken}` });
    let reads = 0;
    const original = f.repo.listRecentJobs.bind(f.repo);
    f.repo.listRecentJobs = (limit) => { reads++; return original(limit); };
    expect((await executeMethod(f.router.routes.ops, f.ctx, {})).diagnostics.recentJobs).toEqual([]);
    expect(reads).toBe(0);
    const result = await executeMethod(f.router.routes.ops, f.ctx, { includeJobs: true });
    expect(reads).toBe(1);
    expect(result.diagnostics.recentJobs).toHaveLength(30);
    expect(result.diagnostics.recentJobs[0]!.type).toBe("tv_shadow_sync");
    expect(result.diagnostics.recentJobs.some((job) => job.type === "tv_feed_match")).toBe(true);
    expect(JSON.stringify(result)).not.toContain(providerKey);
    expect(JSON.stringify(result)).not.toContain(ownerToken);
    expect(JSON.stringify(result)).not.toContain("payload_json");
  });

  test("failed comparisons name the show and distinguish audit impact from acquisition", async () => {
    await using f = await fixture();
    const catalog = { ...fixtureTvEntry(), title: "The Daily Show" };
    const entry = f.repo.tv.upsertEntry(catalog);
    const { caseId } = f.repo.tvShadow.capture({ entry, observation: {
      key: "fixture:daily-show", entry, episodeKeys: [entry.episodes[0]!.key],
      releaseTitle: "Private source title", sourcePaths: [], libraryPath: null,
      observedAt: new Date(NOW).toISOString(), sonarr: { outcome: "imported", messages: [] },
    }, episodeIds: [entry.episodes[0]!.id], policy: defaultMovieQualityPolicy,
      source: "unavailable", files: [], warnings: [] });
    const job = f.job({ type: "tv_shadow_evaluate", payload: { caseId } }, { status: "failed", error: `Invalid mapping ${secretUrl}` });
    f.repo.tvShadow.attachJob(caseId, job.id);
    const before = f.db.query("SELECT total_changes() AS n").get();
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.incidents).toContainEqual(expect.objectContaining({ title: "The Daily Show: Sonarr comparison",
      code: "comparison-failed", detail: expect.stringContaining("does not change downloads or your Plex library") }));
    expect(JSON.stringify(result)).not.toContain(secretUrl);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.repo.tv.targets()).toEqual([]);
  });

  test("scheduled book cards retain the action and use exact saved provider waits when available", async () => {
    await using f = await fixture();
    const book = f.repo.createBook({ title: "Salt, Fat, Acid, Heat", author: "Samin Nosrat" });
    const job = f.job({ type: "acquire", bookId: book.id, payload: { media: ["audio", "ebook"] } },
      { next_run_at: new Date(NOW + 60_000).toISOString() });
    let result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.activeAcquisitions[0]).toMatchObject({ title: book.title, stage: "Finding audiobook and ebook editions" });
    const runId = f.repo.getJobExecution(job.id)!.run_id;
    f.repo.bindJobJournalReader({ read: ids => new Map(ids.flatMap(id => f.lifecycle.has(id) ? [[id, f.lifecycle.get(id)!] as const] : [])),
      waits: () => new Map([[runId, [{ reason: "provider-search", media: "audio" }]]]) });
    result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.activeAcquisitions[0]).toMatchObject({ stage: "Waiting for providers before retrying the audiobook search.",
      job: { recovery: "scheduled-wake" } });
  });

  test("journal failure is explicitly unknown and read methods never initialize missing settings", async () => {
    await using f = await fixture();
    f.job({ type: "acquire" });
    f.repo.bindJobJournalReader({ read() { throw new Error(secretUrl); } });
    const result = await executeMethod(f.router.routes.ops, f.ctx, {});
    expect(result.incidents).toContainEqual(expect.objectContaining({ code: "execution-state-unknown", recovery: "unknown" }));
    expect(result.diagnostics.warnings.join(" ")).toContain("incomplete");
    expect(JSON.stringify(result)).not.toContain(providerKey);
    f.repo.bindJobJournalReader({ read: () => f.lifecycle });
    f.db.exec("DELETE FROM settings");
    const before = f.db.query("SELECT total_changes() AS n").get();
    await executeMethod(f.router.routes.users, f.ctx, {});
    await executeMethod(f.router.routes.ops, f.ctx, {});
    await executeMethod(f.router.routes.plexServers, f.ctx, {});
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.db.query("SELECT * FROM settings").all()).toEqual([]);
  });
});

describe("bounded transactional full-library refresh", () => {
  test("repeated requests share one bounded job and one enqueue intent, while completion allows a new refresh", async () => {
    await using f = await fixture();
    const results = await Promise.all(Array.from({ length: 8 }, () => executeMethod(f.router.routes.refresh, f.ctx, {})));
    expect(new Set(results.map((result) => result.jobId)).size).toBe(1);
    expect(results.filter((result) => !result.shared)).toHaveLength(1);
    const first = results[0]!.jobId;
    expect(f.repo.getJob(first)!.max_attempts).toBe(2);
    expect(f.repo.listJobCommands().filter((command) => command.kind === "enqueue")).toHaveLength(1);
    f.setLifecycle(first, { status: "succeeded" });
    const next = await executeMethod(f.router.routes.refresh, f.ctx, {});
    expect(next.shared).toBe(false);
    expect(next.jobId).not.toBe(first);
  });

  test("a retry already persisted in the outbox is shared rather than creating duplicate work", async () => {
    await using f = await fixture();
    const job = f.job({ type: "full_library_refresh" }, { status: "failed", error: "Failed refresh" });
    f.repo.retryJob(job.id);
    expect(await executeMethod(f.router.routes.refresh, f.ctx, {})).toEqual({ jobId: job.id, shared: true });
    const ops = await executeMethod(f.router.routes.ops, f.ctx, { includeJobs: true });
    expect(ops.diagnostics.recentJobs.find((item) => item.id === job.id)?.recovery).toBe("queued");
  });

  test("outbox failure rolls back the refresh job and a write guard prevents every refresh effect", async () => {
    await using f = await fixture();
    f.db.exec("CREATE TRIGGER block_enqueue BEFORE INSERT ON job_commands BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END");
    await expect(executeMethod(f.router.routes.refresh, f.ctx, {})).rejects.toThrow("outbox unavailable");
    expect(f.repo.listJobsByType("full_library_refresh")).toEqual([]);
    expect(f.repo.listJobCommands()).toEqual([]);
    const fenced = new BooksRepo(f.db, () => { throw new Error("stale execution"); });
    fenced.bindJobJournalReader({ read: () => f.lifecycle });
    await expect(executeMethod(f.router.routes.refresh, { ...f.ctx, repo: fenced }, {})).rejects.toThrow("stale execution");
  });
});

test("normalized errors never return raw messages or misclassify credential-bearing outage URLs", () => {
  expect(normalizedAdminError(`HTTP 503 ${secretUrl}`)?.code).toBe("provider-unavailable");
  expect(normalizedAdminError(`HTTP 429 ${secretUrl}; check credentials`)?.code).toBe("provider-cooldown");
  expect(normalizedAdminError(`HTTP 401 ${secretUrl}`)?.code).toBe("provider-configuration");
  expect(normalizedAdminError("Your client is not on the whitelist")?.code).toBe("tracker-client-not-allowed");
  for (const raw of [secretUrl, `Bearer ${ownerToken}`, `apiKey=${providerKey}`, "ENOSPC", "mount identity changed"])
    expect(JSON.stringify(normalizedAdminError(raw))).not.toContain(providerKey);
  expect(normalizedAdminError(null)).toBeNull();
});
