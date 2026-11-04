import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { prepareMovieStorage } from "../../src/movies/storage";
import { reconcileMovieView } from "../../src/movies/publication";
import {
  RetentionService, allowRecovery, ensurePresentIntent,
  retentionResourceDetailSchema, type RetentionServiceAdapters,
} from "../../src/media/retention-service";
import { createRetentionRouter } from "../../src/rpc/retention-router";
import { executeMethod } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";

const clientId = "transmission:9b0c9c0a-0bbb-4adc-8e74-4fe8bca6c66e";
const hash = "a".repeat(40);
const movie = { tmdbId: 42, title: "Owned Movie", originalTitle: "Owned Movie", year: 2024,
  overview: "Fixture", originalLanguage: "en", posterUrl: null, imdbId: null };
const facts = { identityVerified: true, cut: "theatrical", source: "web" as const, width: 1920,
  height: 1080, videoCodec: "h264", dynamicRange: "sdr" as const, audioLanguages: ["en"], proper: false };

async function fixture({ untracked = false }: { untracked?: boolean } = {}) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "retention-service-")));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "retention", username: "retention", isAdmin: true });
  const settings = repo.getSettings();
  settings.movies.managedRoot = path.join(root, "movies");
  settings.movies.plexUrl = "https://plex.invalid";
  settings.movies.plexToken = "private-plex-token";
  settings.movies.plexSectionId = "4";
  settings.movies.plexViewPath = "/server/movies";
  repo.updateSettings(settings);
  const contentRoot = path.join(settings.movies.managedRoot, "objects");
  await prepareMovieStorage(repo, contentRoot);
  const requested = repo.movies.request(user.id, movie, defaultMovieQualityPolicy);
  if (untracked) {
    // Simulate pre-retention data only in legacy fixtures, never in application behavior.
    db.query("DELETE FROM retention_events WHERE namespace='movie-target' AND resource_id=?").run(String(requested.targetId));
    db.query("DELETE FROM retention_resources WHERE namespace='movie-target' AND resource_id=?").run(String(requested.targetId));
  }
  const acquisition = repo.movies.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "reconcile" }).id).acquisition;
  const fileId = randomUUID();
  const relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`;
  const objectPath = path.join(contentRoot, relativePath);
  await fs.mkdir(path.dirname(objectPath), { recursive: true });
  await fs.writeFile(objectPath, Buffer.alloc(8192, 65));
  const versionId = repo.movies.recordVersion(acquisition.id, facts, [{ id: fileId, relativePath, sizeBytes: 8192, probe: {} }]);
  repo.movies.selectVersion(requested.targetId, versionId, 0);
  repo.movies.finishAcquisition(acquisition.id);
  db.query("UPDATE jobs SET status='succeeded' WHERE id=?").run(acquisition.job_id);
  const key = { namespace: "movie-target", id: String(requested.targetId) };
  const adapters: RetentionServiceAdapters = { transmission: null };
  return { db, repo, root, contentRoot, settings, user, requested, acquisition, fileId, relativePath, objectPath, versionId, key, adapters,
    service: () => new RetentionService(repo, adapters),
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

async function bindSeed(f: Fixture, input: { createFile?: boolean; seconds?: number | null; label?: string; client?: string; name?: string } = {}) {
  const seedDirectory = path.join(f.settings.movies.managedRoot, "downloads", hash);
  await fs.mkdir(seedDirectory, { recursive: true });
  if (input.createFile !== false) await fs.copyFile(f.objectPath, path.join(seedDirectory, "movie.mkv"));
  f.repo.movies.bindDownload(f.acquisition.id, { clientId: input.client ?? clientId, nativeId: hash }, { rootId: "movies", relativePath: hash }, "owned-offer");
  const calls: string[] = [];
  f.adapters.transmission = { clientId, rpc: { async call(method, args) {
    calls.push(method);
    expect(method).toBe("torrent-get");
    expect(args?.ids).toEqual([hash]);
    expect(args?.fields).toContain("secondsSeeding");
    expect(args?.fields).not.toContain("addedDate");
    return { torrents: [{ hashString: hash, downloadDir: seedDirectory, labels: [input.label ?? "Podible Movies"],
      files: [{ name: input.name ?? "movie.mkv", length: 8192, bytesCompleted: 8192 }],
      leftUntilDone: 0, secondsSeeding: input.seconds ?? null, error: 0, status: 6 }] };
  } } };
  return { seedDirectory, calls };
}
const blockerCodes = (result: Awaited<ReturnType<RetentionService["audit"]>>) => result.audit.blockers.map((b) => b.code);

describe("retention service owned presence and intent", () => {
  test("untracked owned movie is observed, but status never creates intent or events", async () => {
    await using f = await fixture({ untracked: true });
    const changes = f.db.query("SELECT total_changes() AS n").get();
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("present");
    expect(result.files[0]).toMatchObject({ ownership: "verified-owned", presence: "present", references: "verified-unshared", verifiedReclaimableBytes: null });
    expect(result.files[0]!.allocatedBytes).toBeGreaterThan(0);
    expect(result.verifiedReclaimableBytes).toBeNull();
    expect(result.desiredRecovery.action).toBe("inspect");
    expect(result.watch).toMatchObject({ state: "unknown", coverage: "unknown" });
    expect(blockerCodes(result)).toContain("watch-unknown");
    expect(blockerCodes(result)).toContain("seed-obligations-unknown");
    expect(result.deletionSupported).toBe(false);
    expect(retentionResourceDetailSchema.parse(result)).toEqual(result);
    expect(f.repo.retention.get(f.key)).toBeNull();
    expect(f.repo.retention.events(f.key)).toEqual([]);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(changes);
    expect(JSON.stringify(result)).not.toContain("private-plex-token");
  });

  test("missing requested versus intentionally absent versus intent-only eviction stay distinct", async () => {
    await using f = await fixture();
    expect(f.repo.retention.get(f.key)).toMatchObject({ desiredPresence: "present", revision: 1 });
    f.repo.retention.evict(f.key, { expectedRevision: 1, reason: "Legacy imported eviction intent" });
    const intentOnly = await f.service().audit(f.key);
    expect(intentOnly.displayState).toBe("eviction-intent-only");
    expect(intentOnly.observation.presence).toBe("present");
    expect(intentOnly.desiredRecovery.action).toBe("none");
    expect(allowRecovery(f.repo, f.key)).toBe(false);
    await fs.unlink(f.objectPath);
    const absent = await f.service().audit(f.key);
    expect(absent.displayState).toBe("intentional-absence");
    expect(absent.observation.presence).toBe("missing");
    expect(absent.desiredRecovery.action).toBe("none");
    expect(f.repo.retention.get(f.key)).toMatchObject({ desiredPresence: "evicted", actualPresence: "unknown", revision: 2 });
    // Only the explicit native request restores intent; earlier audit reads do not replay it.
    f.repo.movies.request(f.user.id, movie, defaultMovieQualityPolicy);
    const requested = await f.service().audit(f.key);
    expect(requested.displayState).toBe("missing");
    expect(requested.desiredRecovery.action).toBe("recover");
    expect(allowRecovery(f.repo, f.key)).toBe(true);
    expect(f.repo.retention.events(f.key).map((event) => event.action)).toEqual(["requested", "evicted", "requested"]);
    f.repo.movies.request(f.user.id, movie, defaultMovieQualityPolicy);
    expect(f.repo.retention.get(f.key)!.revision).toBe(3);
  });

  test("intent helper participates in parent rollback and missing intent never allows recovery", async () => {
    await using f = await fixture({ untracked: true });
    expect(allowRecovery(f.repo, f.key)).toBe(false);
    expect(() => f.repo.transaction(() => {
      ensurePresentIntent(f.repo, f.key, "Atomic explicit request");
      f.repo.createJob({ type: "reconcile" });
      throw new Error("Outbox transaction failed");
    })).toThrow("Outbox transaction failed");
    expect(f.repo.retention.get(f.key)).toBeNull();
    expect(f.repo.retention.events(f.key)).toEqual([]);
  });

  test("missing marker, replacement marker, and symlinked root are errors, not missing content", async () => {
    await using f = await fixture();
    ensurePresentIntent(f.repo, f.key, "Requested");
    const marker = path.join(f.contentRoot, ".podible-content.json");
    const original = await fs.readFile(marker);
    await fs.unlink(marker);
    expect((await f.service().audit(f.key)).observation.presence).toBe("unknown");
    await fs.writeFile(marker, JSON.stringify({ id: randomUUID() }));
    expect((await f.service().audit(f.key)).desiredRecovery.action).toBe("inspect");
    await fs.writeFile(marker, original);
    const moved = `${f.settings.movies.managedRoot}-moved`;
    await fs.rename(f.settings.movies.managedRoot, moved);
    await fs.symlink(moved, f.settings.movies.managedRoot);
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("unknown");
    expect(result.observation.reasons.join(" ")).toContain("inspection error, not verified absence");
    expect(result.files[0]!.ownership).toBe("unknown");
    expect(result.audit.eligible).toBe(false);
  });

  test("persisted path escape and symlinked object ancestors never become owned file observations", async () => {
    await using f = await fixture();
    const outside = path.join(f.root, "outside");
    await fs.mkdir(outside);
    await fs.rename(f.objectPath, path.join(outside, path.basename(f.objectPath)));
    await fs.rmdir(path.dirname(f.objectPath));
    await fs.symlink(outside, path.dirname(f.objectPath));
    expect((await f.service().audit(f.key)).files[0]).toMatchObject({ presence: "unknown", ownership: "unknown" });
    f.db.query("UPDATE managed_content_files SET relative_path=? WHERE id=?").run("../outside/foreign.mkv", f.fileId);
    const escaped = await f.service().audit(f.key);
    expect(escaped.files[0]!.reasons.join(" ")).toContain("no filesystem lookup was attempted");
    expect(escaped.observation.presence).toBe("unknown");
  });

  test("size changes and mount changes during an asynchronous adapter fence the observation", async () => {
    await using f = await fixture();
    await fs.appendFile(f.objectPath, "changed");
    expect((await f.service().audit(f.key)).files[0]!.presence).toBe("unknown");
    await fs.truncate(f.objectPath, 8192);
    f.adapters.watchHistory = async () => {
      await fs.writeFile(path.join(f.contentRoot, ".podible-content.json"), JSON.stringify({ id: randomUUID() }));
      return { state: "watched", coverage: "global-all-users" };
    };
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("unknown");
    expect(result.activeAcquisitions).toBeNull();
    expect(result.files[0]!.ownership).toBe("unknown");
    expect(result.files[0]!.verifiedReclaimableBytes).toBeNull();
  });

  test("canonical file loss or new hardlinks during an adapter invalidate earlier observations", async () => {
    await using f = await fixture();
    f.adapters.watchHistory = async () => {
      await fs.link(f.objectPath, path.join(f.root, "new-consumer.mkv"));
      return { state: "watched", coverage: "global-all-users" };
    };
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("unknown");
    expect(result.files[0]!.references).toBe("unknown");
    expect(result.verifiedReclaimableBytes).toBeNull();
    expect(result.audit.eligible).toBe(false);
  });

  test("newly published symlinks invalidate earlier unshared-reference evidence", async () => {
    await using f = await fixture();
    f.adapters.watchHistory = async () => {
      await reconcileMovieView(f.repo.movies, { contentRoot: f.contentRoot,
        viewRoot: path.join(f.settings.movies.managedRoot, "views", "plex", "movies"), serverViewRoot: "/server/movies" });
      return { state: "watched", coverage: "global-all-users" };
    };
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("unknown");
    expect(result.files[0]!.references).toBe("unknown");
    expect(result.audit.eligible).toBe(false);
  });
});

describe("retention service conservative storage evidence", () => {
  test("real inode links and cross-media database references yield zero reclaimable bytes", async () => {
    await using f = await fixture();
    const otherLink = path.join(f.root, "other-consumer.mkv");
    await fs.link(f.objectPath, otherLink);
    let result = await f.service().audit(f.key);
    expect(result.files[0]).toMatchObject({ linkCount: 2, references: "shared", verifiedReclaimableBytes: 0 });
    expect(result.verifiedReclaimableBytes).toBe(0);
    await fs.unlink(otherLink);
    f.db.exec("CREATE TABLE future_media_refs(file_id TEXT NOT NULL)");
    f.db.query("INSERT INTO future_media_refs VALUES (?)").run(f.fileId);
    result = await f.service().audit(f.key);
    expect(result.files[0]).toMatchObject({ linkCount: 1, databaseReferences: 2, references: "shared", verifiedReclaimableBytes: 0 });
    expect(blockerCodes(result)).toContain("shared-references");
  });

  test("another target and a real published view remain references even with one inode link", async () => {
    await using f = await fixture();
    const second = f.repo.movies.request(f.user.id, movie, { ...defaultMovieQualityPolicy, cut: "extended" });
    f.repo.movies.selectVersion(second.targetId, f.versionId, 0);
    expect((await f.service().audit(f.key)).files[0]!.references).toBe("shared");
    f.db.query("UPDATE movie_targets SET active_version_id=NULL WHERE id=?").run(second.targetId);
    await reconcileMovieView(f.repo.movies, { contentRoot: f.contentRoot,
      viewRoot: path.join(f.settings.movies.managedRoot, "views", "plex", "movies"), serverViewRoot: "/server/movies" });
    const result = await f.service().audit(f.key);
    expect(result.files[0]).toMatchObject({ linkCount: 1, references: "shared", verifiedReclaimableBytes: 0 });
  });

  test("actual allocation needs a server verifier; malformed or oversized claims are ignored", async () => {
    await using f = await fixture();
    f.adapters.reclaimableAllocation = async (file) => {
      expect(file.canonicalPath).toBe(f.objectPath);
      expect(file.inode).toMatch(/^\d+$/);
      return file.allocatedBytes;
    };
    let result = await f.service().audit(f.key);
    expect(result.verifiedReclaimableBytes).toBe(result.files[0]!.allocatedBytes);
    for (const value of [-1, NaN, Infinity, Number.MAX_SAFE_INTEGER]) {
      f.adapters.reclaimableAllocation = async () => value;
      result = await f.service().audit(f.key);
      expect(result.files[0]!.verifiedReclaimableBytes).toBeNull();
      expect(result.audit.reclaimableBytes).toBe(0);
    }
  });

  test("unknown seed store and unknown last-copy history stay blocked", async () => {
    await using f = await fixture();
    expect(blockerCodes(await f.service().audit(f.key))).toContain("seed-obligations-unknown");
    const seed = await bindSeed(f, { createFile: false, seconds: null });
    const result = await f.service().audit(f.key);
    expect(result.seeding.state).toBe("last-copy-or-unknown");
    expect(result.seeding.obligations[0]).toMatchObject({ remainingSeedableCopies: 0, verifiedSecondsSeeded: null, requiredSecondsSeeded: null });
    expect(blockerCodes(result)).toContain("seed-history-unknown");
    expect(blockerCodes(result)).toContain("seed-requirements-unknown");
    expect(seed.calls).toEqual(["torrent-get"]);
  });

  test("exact owned independent seed copies survive canonical-only scope, even with unknown past seed duration", async () => {
    await using f = await fixture();
    const seed = await bindSeed(f);
    const result = await f.service().audit(f.key);
    expect(result.scope).toBe("canonical-movie-objects-only");
    expect(result.seeding.state).toBe("verified-independent-seed-store");
    expect(result.seeding.obligations[0]).toMatchObject({ remainingSeedableCopies: 1, verifiedSecondsSeeded: null });
    expect(blockerCodes(result)).not.toContain("seed-history-unknown");
    expect(result.seeding.reason).toContain("No seed-store files or registrations would be removed");
    expect(await fs.readFile(path.join(seed.seedDirectory, "movie.mkv"))).toEqual(await fs.readFile(f.objectPath));
    expect(seed.calls).toEqual(["torrent-get"]);
  });

  test("wrong client, near-match ownership labels and escaping engine paths cannot supply seed evidence", async () => {
    await using f = await fixture();
    await bindSeed(f, { label: "Podible Movies foreign" });
    expect((await f.service().audit(f.key)).seeding.obligations[0]!.remainingSeedableCopies).toBeNull();
    await bindSeed(f, { name: "../foreign.mkv" });
    expect((await f.service().audit(f.key)).seeding.obligations[0]!.remainingSeedableCopies).toBeNull();
    const seed = await bindSeed(f);
    f.adapters.transmission!.clientId = `transmission:${randomUUID()}`;
    const result = await f.service().audit(f.key);
    expect(seed.calls).toEqual([]);
    expect(result.seeding.obligations[0]!.remainingSeedableCopies).toBeNull();
  });

  test("owner-only watch history is never all-user evidence; adapter errors stay unknown and sanitized", async () => {
    await using f = await fixture();
    f.adapters.watchHistory = async (input) => {
      expect(input.plexToken).toBe("private-plex-token");
      expect(input.plexUrl).toBe("https://plex.invalid");
      return { state: "watched", coverage: "owner-only" };
    };
    expect((await f.service().audit(f.key)).watch).toMatchObject({ state: "unknown", coverage: "unknown" });
    f.adapters.watchHistory = async () => { throw new Error("private-plex-token"); };
    const failed = await f.service().audit(f.key);
    expect(failed.watch.state).toBe("unknown");
    expect(JSON.stringify(failed)).not.toContain("private-plex-token");
    f.adapters.watchHistory = async () => ({ state: "unwatched", coverage: "global-all-users" });
    expect(blockerCodes(await f.service().audit(f.key))).toContain("unwatched");
  });

  test("active acquisition and incomplete bounded reference inventory fail conservative", async () => {
    await using f = await fixture();
    f.db.query("UPDATE jobs SET status='running' WHERE id=?").run(f.acquisition.job_id);
    expect(blockerCodes(await f.service().audit(f.key))).toContain("active-acquisitions");
    f.db.exec("CREATE TABLE unknown_file_refs(file_id TEXT)");
    f.db.exec("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<5001) INSERT INTO unknown_file_refs SELECT 'unknown' FROM n");
    const result = await f.service().audit(f.key);
    expect(result.observation.presence).toBe("unknown");
    expect(result.activeAcquisitions).toBeNull();
    expect(result.audit.eligible).toBe(false);
  });
});

describe("retention admin read-only router", () => {
  test("strict admin-only status/audit accept no file paths, seed claims or eviction mutation", async () => {
    await using f = await fixture();
    const router = createRetentionRouter(f.adapters);
    const ctx: RpcContext = { repo: f.repo, startTime: 0, request: new Request("https://podible.invalid/rpc"), session: null };
    expect(Object.keys(router.routes)).toEqual(["status", "audit"]);
    expect(router.routes.status.readOnly).toBe(true);
    expect(router.routes.audit.readOnly).toBe(true);
    await expect(executeMethod(router.routes.status, ctx, {})).rejects.toMatchObject({ code: -32001 });
    ctx.session = { user_id: f.user.id, is_admin: 0 } as RpcContext["session"];
    await expect(executeMethod(router.routes.audit, ctx, f.key)).rejects.toMatchObject({ code: -32003 });
    ctx.session = { user_id: f.user.id, is_admin: 1 } as RpcContext["session"];
    for (const extra of [{ filePath: f.objectPath }, { secondsSeeded: 9_000_000 }, { desiredPresence: "evicted" }])
      await expect(executeMethod(router.routes.audit, ctx, { ...f.key, ...extra })).rejects.toMatchObject({ code: -32602 });
    await expect(executeMethod(router.routes.audit, ctx, { namespace: "tv-request", id: "1" })).rejects.toMatchObject({ code: -32602 });
    await expect(executeMethod(router.routes.audit, ctx, { namespace: "movie-target", id: "999999999999999999999" })).rejects.toMatchObject({ code: -32602 });
    await expect(executeMethod(router.routes.audit, ctx, { namespace: "movie-target", id: "999999" })).rejects.toMatchObject({ code: -32602 });
    const before = f.db.query("SELECT total_changes() AS n").get();
    const beforeEvents = f.repo.retention.events(f.key);
    const result = await executeMethod(router.routes.status, ctx, { limit: 1 });
    expect(result).toMatchObject({ mode: "audit-only", deletionSupported: false, supportedNamespaces: ["movie-target"], unsupportedNamespaces: ["tv-request", "book-manifestation"] });
    expect(result.resources).toHaveLength(1);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.repo.retention.events(f.key)).toEqual(beforeEvents);
  });

  test("reading with absent settings never creates defaults, and pagination is bounded", async () => {
    await using f = await fixture();
    f.repo.movies.request(f.user.id, { ...movie, tmdbId: 43, title: "Other Movie" }, defaultMovieQualityPolicy);
    const first = await f.service().status({ limit: 1 });
    expect(first.nextAfterTargetId).toBe(f.requested.targetId);
    const second = await f.service().status({ limit: 1, afterTargetId: first.nextAfterTargetId! });
    expect(second.resources[0]!.title).toBe("Other Movie");
    expect(second.nextAfterTargetId).toBeNull();
    f.db.exec("DELETE FROM settings");
    const before = f.db.query("SELECT total_changes() AS n").get();
    expect((await f.service().audit(f.key)).observation.presence).toBe("unknown");
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.db.query("SELECT * FROM settings").all()).toEqual([]);
    await expect(f.service().status({ limit: 101 })).rejects.toThrow();
  });
});
