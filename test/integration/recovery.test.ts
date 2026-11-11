import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { unlinkSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { RecoveryService } from "../../src/media/recovery";
import { recoveryDigest } from "../../src/media/recovery-filesystem";
import { RECOVERY_OPERATION_LIMIT, recoveryPreviewSchema, type RecoveryKey } from "../../src/media/recovery-types";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { movieViewEntries, reconcileMovieView } from "../../src/movies/publication";
import { MovieRepo } from "../../src/movies/repo";
import { reconcileTvView, tvRoots, tvViewEntries } from "../../src/tv/publication";
import { TvRepo } from "../../src/tv/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { recoveryRouter } from "../../src/rpc/recovery-router";
import { executeMethod } from "../../src/rpc/framework";
import { handleRpcMethod } from "../../src/rpc";
import { appRouter } from "../../src/trpc";
import type { RpcContext } from "../../src/rpc/shared";

const movie = { tmdbId: 7, title: "Owned movie", originalTitle: "Owned movie", year: 2024, overview: "Fixture",
  originalLanguage: "en", posterUrl: null, imdbId: null };
const facts = { identityVerified: true, cut: null, source: "web" as const, width: 1920, height: 1080,
  videoCodec: "h264", dynamicRange: "sdr" as const, audioLanguages: ["en"], proper: false };
const tv = { key: "tvdb:77", tvdbId: 77, tmdbId: null, title: "Owned TV", aliases: [], year: 2023, anime: false, relations: [],
  episodes: [1, 2].map((number) => ({ key: `tvdb:${770 + number}`, title: `Episode ${number}`, airDate: "2023-01-01",
    runtimeMinutes: 30, numbering: [{ scheme: "tvdb:aired", season: 1, episode: number, absolute: null }] })),
};

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "recovery-")));
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.movies.managedRoot = path.join(root, "movies"); settings.movies.plexViewPath = "/server/movies";
  settings.tv.managedRoot = path.join(root, "tv"); settings.tv.plexViewPath = "/server/tv";
  settings.movies.plexToken = "private-plex-key";
  repo.updateSettings(settings);
  for (const kind of ["movies", "tv"] as const) await prepareMovieStorage(repo, path.join(settings[kind].managedRoot, "objects"));
  const user = repo.upsertUser({ provider: "local", providerUserId: "recovery", username: "recovery", isAdmin: true });
  const requestedMovie = repo.movies.request(user.id, movie, defaultMovieQualityPolicy);
  const requestedTv = repo.tv.request(user.id, tv, defaultMovieQualityPolicy, tv.episodes.map((episode) => episode.key));
  const keys: RecoveryKey[] = [
    { namespace: "movie-target", id: String(requestedMovie.targetId) }, { namespace: "tv-target", id: String(requestedTv.targetId) },
  ];
  repo.retention.request(keys[1]!, { expectedRevision: 0, reason: "Explicit fixture request" });
  const service = new RecoveryService(repo);
  const preview = (key: RecoveryKey) => service.preview({ targets: [key] }).resources[0]!;
  const command = (key: RecoveryKey, action: "repair-view" | "queue-acquisition", operationId = randomUUID()) =>
    ({ operationId, targets: [{ key, expectedRevision: preview(key).revision, action }] });
  const roots = (kind: "movies" | "tv") => ({ contentRoot: path.join(settings[kind].managedRoot, "objects"),
    viewRoot: path.join(settings[kind].managedRoot, "views", "plex", kind), serverViewRoot: settings[kind].plexViewPath });
  const addVersion = async (kind: "movies" | "tv", selected = true) => {
    const request = kind === "movies" ? requestedMovie : requestedTv;
    const acquisition = kind === "movies"
      ? repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition
      : repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id, request.requestId).acquisition;
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`, canonical = path.join(roots(kind).contentRoot, relativePath);
    await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, "verified fixture bytes");
    const file = { id, relativePath, sizeBytes: 22, probe: {} };
    const versionId = kind === "movies" ? repo.movies.recordVersion(acquisition.id, facts, [file])
      : repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ ...file, episodeIds: requestedTv.episodeIds }]);
    if (kind === "movies") {
      if (selected) repo.movies.selectVersion(request.targetId, versionId, repo.movies.target(request.targetId)!.revision);
      repo.movies.finishAcquisition(acquisition.id);
      if (selected) await reconcileMovieView(repo.movies, roots(kind));
    } else {
      if (selected) {
        const revision = repo.tv.target(request.targetId)!.revision;
        repo.tv.stagePublication(request.targetId, versionId, revision); repo.tv.promotePublication(request.targetId, versionId, revision);
        repo.tv.clearPublication(request.targetId, versionId);
      }
      repo.tv.finishAcquisition(acquisition.id);
      if (selected) await reconcileTvView(repo.tv, tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath));
    }
    const entry = selected ? (kind === "movies" ? movieViewEntries(repo.movies, roots(kind)) : tvViewEntries(repo.tv, roots(kind)))[0]! : null;
    return { canonical, file, versionId, view: entry ? path.join(roots(kind).viewRoot, entry.relativePath) : null };
  };
  const modern = async () => {
    const runtime = await createJobExecutionRuntime({ repo, getSettings: () => repo.getSettings(), onLog() {} }, path.join(root, "execution.sqlite"));
    await runtime.stop();
  };
  return { root, db, repo, service, settings, user, requestedMovie, requestedTv, keys, roots, preview, command, addVersion, modern,
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}
const codes = (resource: { reasons: { code: string }[] }) => resource.reasons.map((reason) => reason.code);

test("read-only inventory keeps cross-domain target/request IDs separate and never creates intent/settings", async () => {
  await using f = await fixture();
  expect(f.keys[0]!.id).toBe(f.keys[1]!.id);
  const before = f.db.query("SELECT total_changes() AS n").get();
  const preview = f.service.preview({ targets: f.keys });
  expect(recoveryPreviewSchema.parse(preview)).toEqual(preview);
  expect(preview.resources.map((resource) => resource.requestIds[0]!.namespace)).toEqual(["movie-request", "tv-request"]);
  expect(preview.resources.map((resource) => resource.action)).toEqual(["queue-acquisition", "queue-acquisition"]);
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
  expect(JSON.stringify(preview)).not.toContain("private-plex-key");
  f.db.exec("DELETE FROM settings; DELETE FROM retention_events; DELETE FROM retention_resources");
  const absent = f.db.query("SELECT total_changes() AS n").get();
  expect(f.preview(f.keys[0]!).action).toBe("blocked");
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(absent);
  expect(f.repo.readSettings()).toBeNull();
});

for (const [kind, index] of [["movies", 0], ["tv", 1]] as const) {
  test(`${kind}: surviving selected bytes repair only missing declarative links, with durable replay`, async () => {
    await using f = await fixture();
    const version = await f.addVersion(kind);
    await fs.unlink(version.view!);
    const key = f.keys[index]!;
    expect(f.preview(key)).toMatchObject({ action: "repair-view", canonical: { state: "present" }, view: { state: "missing" } });
    const bytes = await fs.readFile(version.canonical);
    const before = f.repo.retention.get(key);
    const command = f.command(key, "repair-view");
    const result = f.service.recover(command);
    expect(result.results[0]!.status).toBe("repaired");
    expect(await fs.realpath(version.view!)).toBe(version.canonical);
    expect(await fs.readFile(version.canonical)).toEqual(bytes);
    expect(f.repo.retention.get(key)).toEqual(before);
    const reopenedDb = new Database(path.join(f.root, "app.sqlite"));
    try {
      expect(new RecoveryService(new BooksRepo(reopenedDb)).recover(command)).toEqual(result);
    } finally { reopenedDb.close(); }
    expect(f.repo.listJobsByType(kind === "movies" ? "movie_acquire" : "tv_acquire")).toHaveLength(0);
    expect(f.preview(key).action).toBe("none");
  });

  test(`${kind}: missing/replaced markers block, never initialize an empty mount or queue a download`, async () => {
    await using f = await fixture();
    const key = f.keys[index]!, command = f.command(key, "queue-acquisition");
    const marker = path.join(f.roots(kind).contentRoot, ".podible-content.json");
    await fs.unlink(marker);
    expect(f.preview(key)).toMatchObject({ action: "blocked", mount: { state: "unavailable" }, canonical: { state: "unknown" } });
    expect(f.service.recover(command).results[0]!.status).toBe("blocked");
    await expect(fs.stat(marker)).rejects.toMatchObject({ code: "ENOENT" });
    await fs.writeFile(marker, JSON.stringify({ id: randomUUID() }));
    expect(codes(f.preview(key))).toContain("mount-unavailable");
    expect(f.service.recover(f.command(key, "queue-acquisition")).results[0]!.status).toBe("blocked");
    expect(f.repo.listJobsByType(kind === "movies" ? "movie_acquire" : "tv_acquire")).toHaveLength(0);
  });

  test(`${kind}: lost selected canonical bytes invalidate selection and atomically queue replacement without losing demand`, async () => {
    await using f = await fixture();
    const version = await f.addVersion(kind), key = f.keys[index]!;
    await fs.unlink(version.canonical);
    const resource = f.preview(key);
    expect(resource.canonical.state).toBe("missing");
    expect(resource.action).toBe("queue-acquisition");
    const command = f.command(key, "queue-acquisition"), result = f.service.recover(command);
    expect(result.results[0]!.status).toBe("queued");
    expect(f.service.recover(command)).toEqual(result);
    if (kind === "tv") {
      expect(f.repo.tv.missingEpisodes(f.requestedTv.requestId)).toHaveLength(2);
      expect(() => f.repo.tv.stagePublication(f.requestedTv.targetId, version.versionId)).toThrow("confirmed missing");
    } else {
      expect(f.repo.movies.target(f.requestedMovie.targetId)!.active_version_id).toBeNull();
      expect(() => f.repo.movies.selectVersion(f.requestedMovie.targetId, version.versionId, 2)).toThrow("confirmed missing");
      expect(() => f.repo.movies.stagePublication(f.requestedMovie.targetId, version.versionId)).toThrow("confirmed missing");
    }
    expect(f.repo.listJobsByType(kind === "movies" ? "movie_acquire" : "tv_acquire")).toHaveLength(1);
    expect(f.repo.retention.get(key)).toMatchObject({ desiredPresence: "present", actualPresence: "missing" });
    expect(f.db.query("SELECT file_id FROM media_content_losses").all()).toEqual([{ file_id: version.file.id }]);
    expect(kind === "movies" ? f.repo.movies.version(version.versionId) : f.repo.tv.version(version.versionId)).not.toBeNull();
  });

  test(`${kind}: intentional eviction and withdrawal never create new demand`, async () => {
    await using f = await fixture();
    const key = f.keys[index]!;
    const intent = f.repo.retention.get(key)!;
    f.repo.retention.evict(key, { expectedRevision: intent.revision, reason: "Explicit fixture eviction" });
    expect(codes(f.preview(key))).toContain("intentional-eviction");
    expect(f.service.recover(f.command(key, "queue-acquisition")).results[0]!.status).toBe("blocked");
    expect(f.repo.retention.get(key)!.desiredPresence).toBe("evicted");
    f.repo.retention.request(key, { expectedRevision: intent.revision + 1, reason: "Explicit re-request" });
    if (kind === "movies") f.repo.movies.withdrawRequest(f.requestedMovie.requestId);
    else f.repo.tv.withdrawRequest(f.requestedTv.requestId);
    expect(codes(f.preview(key))).toContain("no-active-requests");
  });

  test(`${kind}: a lost empty view tree is rebuilt without touching canonical bytes`, async () => {
    await using f = await fixture();
    const version = await f.addVersion(kind);
    const bytes = await fs.readFile(version.canonical);
    await fs.rm(f.roots(kind).viewRoot, { recursive: true });
    expect(f.service.recover(f.command(f.keys[index]!, "repair-view")).results[0]!.status).toBe("repaired");
    expect(await fs.readFile(version.canonical)).toEqual(bytes);
    expect(await fs.realpath(version.view!)).toBe(version.canonical);
  });
}

test("supported unselected movie/TV recovery atomically queues existing acquisitions/outbox and retains all IDs", async () => {
  await using f = await fixture();
  await f.modern();
  const requests = [f.repo.movies.requestById(f.requestedMovie.requestId), f.repo.tv.requestById(f.requestedTv.requestId)];
  const intents = f.keys.map((key) => f.repo.retention.get(key));
  const command = { operationId: randomUUID(), targets: f.keys.map((key) => ({ key, expectedRevision: f.preview(key).revision, action: "queue-acquisition" as const })) };
  const result = f.service.recover(command);
  expect(result.results.map((row) => row.status)).toEqual(["queued", "queued"]);
  expect(f.repo.movies.acquisitions(f.requestedMovie.targetId)).toHaveLength(1);
  expect(f.repo.tv.acquisitions(f.requestedTv.targetId)[0]).toMatchObject({ request_id: f.requestedTv.requestId, reason: "recovery" });
  expect(f.repo.listJobCommands()).toHaveLength(2);
  expect(f.repo.listJobCommands().map((row) => row.kind)).toEqual(["enqueue", "enqueue"]);
  expect([f.repo.movies.requestById(f.requestedMovie.requestId), f.repo.tv.requestById(f.requestedTv.requestId)]).toEqual(requests);
  expect(f.keys.map((key) => f.repo.retention.get(key))).toEqual(intents);
  expect(f.service.recover(command)).toEqual(result);
  expect(f.repo.listJobCommands()).toHaveLength(2);
  expect(codes(f.preview(f.keys[0]!))).toContain("active-acquisition");
  expect(codes(f.preview(f.keys[1]!))).toContain("active-acquisition");
  const conflict = { ...command, targets: command.targets.slice(0, 1) };
  expect(codes(f.service.recover(conflict).results[0]!)).toContain("operation-conflict");
});

test("a mixed confirmed-loss batch can be read back and resumed after a partial commit and reconnect", async () => {
  await using f = await fixture();
  const lostMovie = await f.addVersion("movies"), lostTv = await f.addVersion("tv");
  await f.modern();
  await fs.unlink(lostMovie.canonical); await fs.unlink(lostTv.canonical);
  const { resources } = f.service.preview({ targets: f.keys });
  expect(resources.map(({ canonical, action }) => [canonical.state, action]))
    .toEqual([["missing", "queue-acquisition"], ["missing", "queue-acquisition"]]);
  const command = { operationId: randomUUID(), targets: resources.map(({ key, revision }) =>
    ({ key, expectedRevision: revision, action: "queue-acquisition" as const })) };
  f.db.exec(`CREATE TRIGGER fail_second_recovery BEFORE INSERT ON job_commands
    WHEN EXISTS (SELECT 1 FROM jobs WHERE id=NEW.job_id AND type='tv_reconcile')
    BEGIN SELECT RAISE(ABORT,'second target outbox failure'); END`);
  expect(() => f.service.recover(command)).toThrow("second target outbox failure");
  expect(f.db.query("SELECT file_id FROM media_content_losses").all()).toEqual([{ file_id: lostMovie.file.id }]);
  expect(f.repo.listJobCommands()).toHaveLength(2);
  expect(f.repo.tv.activeEpisodes(f.requestedTv.targetId)).toHaveLength(2);
  f.db.exec("DROP TRIGGER fail_second_recovery");

  using reopened = new Database(path.join(f.root, "app.sqlite"));
  const repo = new BooksRepo(reopened), service = new RecoveryService(repo);
  const readInventory = repo.readRecoveryInventory.bind(repo);
  const read = spyOn(repo, "readRecoveryInventory").mockImplementation(input => {
    if (input.namespace === "movie-target") throw new Error("a committed target must not be inspected again");
    return readInventory(input);
  });
  try {
    const before = reopened.query("SELECT total_changes() AS n").get();
    const recorded = service.operation({});
    expect(recorded).toMatchObject({ status: "recorded", command, receipts: [{ key: f.keys[0], status: "queued" }] });
    if (recorded.status !== "recorded") throw new Error("Expected the stored command");
    expect(service.operation({ operationId: recorded.command.operationId })).toEqual(recorded);
    expect(read).not.toHaveBeenCalled();
    expect(reopened.query("SELECT total_changes() AS n").get()).toEqual(before);

    const resumed = service.recover(recorded.command);
    expect(resumed.results.map(result => result.status)).toEqual(["queued", "queued"]);
    expect(resumed.results[0]).toEqual(recorded.receipts[0]);
    expect(repo.listJobCommands()).toHaveLength(4);
    expect(reopened.query("SELECT file_id FROM media_content_losses ORDER BY namespace").all())
      .toEqual([{ file_id: lostMovie.file.id }, { file_id: lostTv.file.id }]);
    expect(service.operation({})).toEqual({ status: "recorded", command, receipts: resumed.results });
    expect(service.recover(recorded.command)).toEqual(resumed);
    expect(repo.listJobCommands()).toHaveLength(4);
  } finally { read.mockRestore(); }
});

test("legacy digest-only bindings are hydrated only by the original exact command", async () => {
  await using f = await fixture();
  expect(f.service.operation({})).toEqual({ status: "not-found", operationId: null });
  const command = f.command(f.keys[0]!, "queue-acquisition"), key = `recovery-operation:${command.operationId}`;
  const result = f.service.recover(command);
  f.repo.setJsonState(key, { descriptor: recoveryDigest(command.targets) });
  expect(f.service.operation({})).toEqual({ status: "legacy", operationId: command.operationId });
  const wrong = { ...command, targets: [{ ...command.targets[0]!, expectedRevision: "f".repeat(64) }] };
  expect(codes(f.service.recover(wrong).results[0]!)).toContain("operation-conflict");
  expect(f.service.operation({})).toEqual({ status: "legacy", operationId: command.operationId });
  expect(f.service.recover(command)).toEqual(result);
  expect(f.service.operation({})).toEqual({ status: "recorded", command, receipts: result.results });
});

test("outbox failure rolls back acquisition, job, and success receipt; exact operation retry remains usable", async () => {
  await using f = await fixture();
  await f.modern();
  const command = f.command(f.keys[0]!, "queue-acquisition");
  f.db.exec("CREATE TRIGGER reject_recovery_command BEFORE INSERT ON job_commands BEGIN SELECT RAISE(ABORT,'injected outbox outage'); END");
  expect(() => f.service.recover(command)).toThrow("injected outbox outage");
  expect(f.repo.movies.acquisitions(f.requestedMovie.targetId)).toHaveLength(0);
  expect(f.repo.listJobsByType("movie_acquire")).toHaveLength(0);
  expect(f.repo.listJobCommands()).toHaveLength(0);
  expect(f.repo.getJsonState(`recovery-operation:${command.operationId}:movie-target:${f.requestedMovie.targetId}`)).toBeNull();
  f.db.exec("DROP TRIGGER reject_recovery_command");
  expect(f.service.recover(command).results[0]!.status).toBe("queued");
  expect(f.repo.listJobCommands()).toHaveLength(1);
});

for (const [kind, index] of [["movies", 0], ["tv", 1]] as const) {
  test(`${kind}: loss invalidation, tombstones, presence observation and jobs roll back together on outbox failure`, async () => {
    await using f = await fixture();
    const version = await f.addVersion(kind), key = f.keys[index]!;
    await f.modern();
    await fs.unlink(version.canonical);
    const selection = kind === "movies" ? f.repo.movies.target(f.requestedMovie.targetId) : f.repo.tv.activeEpisodes(f.requestedTv.targetId);
    const intent = f.repo.retention.get(key), command = f.command(key, "queue-acquisition");
    f.db.exec("CREATE TRIGGER reject_loss_outbox BEFORE INSERT ON job_commands BEGIN SELECT RAISE(ABORT,'loss outbox unavailable'); END");
    expect(() => f.service.recover(command)).toThrow("loss outbox unavailable");
    expect(kind === "movies" ? f.repo.movies.target(f.requestedMovie.targetId) : f.repo.tv.activeEpisodes(f.requestedTv.targetId)).toEqual(selection);
    expect(f.repo.retention.get(key)).toEqual(intent);
    expect(f.db.query("SELECT * FROM media_content_losses").all()).toEqual([]);
    expect(f.repo.listJobCommands()).toHaveLength(0);
    f.db.exec("DROP TRIGGER reject_loss_outbox");
    expect(f.service.recover(command).results[0]!.status).toBe("queued");
    expect(f.repo.listJobCommands()).toHaveLength(2);
  });
}

test("a missing file restored before command is not invalidated or reacquired", async () => {
  await using f = await fixture();
  const version = await f.addVersion("movies"), bytes = await fs.readFile(version.canonical), key = f.keys[0]!;
  await fs.unlink(version.canonical);
  const command = f.command(key, "queue-acquisition");
  await fs.writeFile(version.canonical, bytes);
  expect(codes(f.service.recover(command).results[0]!)).toContain("stale-revision");
  const recorded = f.service.operation({ operationId: command.operationId });
  expect(recorded).toEqual({ status: "recorded", command, receipts: [] });
  if (recorded.status !== "recorded") throw new Error("Expected the stored command");
  expect(codes(f.service.recover(recorded.command).results[0]!)).toContain("stale-revision");
  expect(f.repo.movies.target(f.requestedMovie.targetId)!.active_version_id).toBe(version.versionId);
  expect(f.db.query("SELECT * FROM media_content_losses").all()).toEqual([]);
});

test("loss shared by another selected target blocks single-target invalidation", async () => {
  await using f = await fixture();
  const version = await f.addVersion("movies"), key = f.keys[0]!;
  const other = f.repo.movies.request(f.user.id, movie, { ...defaultMovieQualityPolicy, cut: "alternate" });
  f.repo.movies.selectVersion(other.targetId, version.versionId, f.repo.movies.target(other.targetId)!.revision);
  await fs.unlink(version.canonical);
  expect(codes(f.preview(key))).toContain("loss-invalidation-blocked");
  expect(f.service.recover(f.command(key, "queue-acquisition")).results[0]!.status).toBe("blocked");
  expect(f.repo.movies.target(f.requestedMovie.targetId)!.active_version_id).toBe(version.versionId);
  expect(f.db.query("SELECT * FROM media_content_losses").all()).toEqual([]);
});

test("revision fences reject changed policy, canonical bytes, withdrawal and intent without fallback", async () => {
  await using f = await fixture();
  const key = f.keys[0]!, version = await f.addVersion("movies");
  await fs.unlink(version.view!);
  const command = f.command(key, "repair-view");
  await fs.writeFile(version.canonical, "different fixture byte");
  expect(codes(f.service.recover(command).results[0]!)).toContain("stale-revision");
  await expect(fs.lstat(version.view!)).rejects.toMatchObject({ code: "ENOENT" });
  expect(f.service.recover(f.command(key, "queue-acquisition")).results[0]!.status).toBe("blocked");
  const another = f.command(f.keys[1]!, "queue-acquisition");
  f.repo.tv.withdrawRequest(f.requestedTv.requestId);
  expect(codes(f.service.recover(another).results[0]!)).toContain("stale-revision");
  expect(f.repo.listJobsByType("tv_acquire")).toHaveLength(0);
});

test("foreign view paths and symlinked canonical ancestors are blocked without deleting/replacing anything", async () => {
  await using f = await fixture();
  const version = await f.addVersion("movies"), key = f.keys[0]!;
  await fs.unlink(version.view!); await fs.writeFile(version.view!, "foreign bytes");
  expect(codes(f.preview(key))).toContain("view-unverified");
  expect(f.service.recover(f.command(key, "repair-view")).results[0]!.status).toBe("blocked");
  expect(await fs.readFile(version.view!, "utf8")).toBe("foreign bytes");
  const moved = `${path.dirname(version.canonical)}-moved`;
  await fs.rename(path.dirname(version.canonical), moved);
  await fs.symlink(moved, path.dirname(version.canonical));
  expect(f.preview(key).canonical.state).toBe("unknown");
  expect(codes(f.preview(key))).toContain("canonical-unverified");
});

test("surviving unselected versions are not redownloaded or falsely reported repaired", async () => {
  await using f = await fixture();
  await f.addVersion("movies", false); await f.addVersion("tv", false);
  for (const key of f.keys) {
    expect(codes(f.preview(key))).toContain("unsupported-repair");
    expect(f.service.recover(f.command(key, "queue-acquisition")).results[0]!.status).toBe("blocked");
  }
});

test("target-scoped recovery remains usable beyond 5000 unrelated TV episodes", async () => {
  await using f = await fixture();
  f.db.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<6000)
    INSERT INTO tv_episodes(episode_key,metadata_json,updated_at) SELECT 'unrelated:'||i,'{}','fixture' FROM n`);
  const noGlobalReads = spyOn(f.repo, "adminDbTablePage").mockImplementation(() => { throw new Error("Global scan forbidden"); });
  try {
    expect(f.preview(f.keys[1]!).action).toBe("queue-acquisition");
    expect(f.service.inventory({ namespace: "tv-target", limit: 1 }).resources).toHaveLength(1);
    expect(f.service.recover(f.command(f.keys[1]!, "queue-acquisition")).results[0]!.status).toBe("queued");
  } finally { noGlobalReads.mockRestore(); }
});

test("inspect uses bounded snapshot acquisitions/publications and never global domain accessors", async () => {
  await using f = await fixture();
  const movieVersion = await f.addVersion("movies"), tvVersion = await f.addVersion("tv");
  const movieSpies = (["publications", "acquisitions", "targets", "requests"] as const).map((method) =>
    spyOn(MovieRepo.prototype, method).mockImplementation(() => { throw new Error(`Forbidden movie ${method} read`); }));
  const tvSpies = (["publications", "acquisitions", "targets", "requests", "entries"] as const).map((method) =>
    spyOn(TvRepo.prototype, method).mockImplementation(() => { throw new Error(`Forbidden TV ${method} read`); }));
  try {
    expect(f.service.preview({ targets: f.keys }).resources.map((resource) => resource.action)).toEqual(["none", "none"]);
    f.db.query("INSERT INTO movie_publications(target_id,version_id,expected_revision,created_at) VALUES(?,?,1,'fixture')")
      .run(f.requestedMovie.targetId, movieVersion.versionId);
    f.db.query("INSERT INTO tv_publications(target_id,version_id,expected_revision,created_at) VALUES(?,?,1,'fixture')")
      .run(f.requestedTv.targetId, tvVersion.versionId);
    for (const resource of f.service.preview({ targets: f.keys }).resources) {
      expect(resource.action).toBe("blocked");
      expect(codes(resource)).toContain("active-publication");
    }
    for (const spy of [...movieSpies, ...tvSpies]) expect(spy).not.toHaveBeenCalled();
  } finally {
    for (const spy of [...movieSpies, ...tvSpies]) spy.mockRestore();
  }
});

test("the short writer fence excludes another WAL writer before filesystem effects", async () => {
  await using f = await fixture();
  f.db.exec("PRAGMA journal_mode=WAL");
  const version = await f.addVersion("movies"); await fs.unlink(version.view!);
  const command = f.command(f.keys[0]!, "repair-view");
  const other = new Database(path.join(f.root, "app.sqlite")); other.exec("PRAGMA busy_timeout=0");
  const transaction = f.repo.transaction.bind(f.repo), read = f.repo.readRecoveryInventory.bind(f.repo);
  let inside = false, checked = false;
  const transactionSpy = spyOn(f.repo, "transaction").mockImplementation((action) => transaction(() => {
    const prior = inside; inside = true;
    try { return action(); } finally { inside = prior; }
  }));
  const readSpy = spyOn(f.repo, "readRecoveryInventory").mockImplementation((input) => {
    if (inside && input.fence) {
      checked = true;
      expect(() => other.query("UPDATE retention_resources SET desired_presence='evicted' WHERE namespace='movie-target'").run()).toThrow(/locked|busy/i);
    }
    return read(input);
  });
  try {
    expect(f.service.recover(command).results[0]!.status).toBe("repaired");
    expect(checked).toBe(true);
    expect(f.repo.retention.get(f.keys[0]!)!.desiredPresence).toBe("present");
  } finally { readSpy.mockRestore(); transactionSpy.mockRestore(); other.close(); }
});

test("writer fence never performs full inspection and has one targeted read, no view/domain renderer", async () => {
  await using f = await fixture();
  const version = await f.addVersion("movies"); await fs.unlink(version.view!);
  const command = f.command(f.keys[0]!, "repair-view");
  const transaction = f.repo.transaction.bind(f.repo), read = f.repo.readRecoveryInventory.bind(f.repo);
  let inside = false, targeted = 0; const elapsed: number[] = [];
  const transactionSpy = spyOn(f.repo, "transaction").mockImplementation((action) => transaction(() => {
    const prior = inside, start = performance.now(); inside = true;
    try { return action(); } finally { inside = prior; elapsed.push(performance.now() - start); }
  }));
  const readSpy = spyOn(f.repo, "readRecoveryInventory").mockImplementation((input) => { if (inside) targeted++; return read(input); });
  const inspectSpy = spyOn(f.service as any, "inspect");
  try {
    expect(f.service.recover(command).results[0]!.status).toBe("repaired");
    expect(targeted).toBe(1);
    expect(inspectSpy).toHaveBeenCalledTimes(1);
    expect(Math.max(...elapsed)).toBeLessThan(250);
  } finally { readSpy.mockRestore(); transactionSpy.mockRestore(); inspectSpy.mockRestore(); }
});

test("durable receipt storage has a hard operation budget and never forgets receipts to admit new commands", async () => {
  await using f = await fixture();
  f.repo.setJsonState("recovery-operation-index", Array.from({ length: RECOVERY_OPERATION_LIMIT }, () => randomUUID()));
  const result = f.service.recover(f.command(f.keys[0]!, "queue-acquisition"));
  expect(codes(result.results[0]!)).toContain("operation-budget");
  expect(f.repo.movies.acquisitions(f.requestedMovie.targetId)).toHaveLength(0);
});

test("mutation file limits block before holding a writer for a large selected movie", async () => {
  await using f = await fixture();
  const selected = await f.addVersion("movies");
  for (let sequence = 1; sequence <= 20; sequence++) {
    const id = randomUUID(), relative = `files/${id.slice(0, 2)}/${id}.mkv`, canonical = path.join(f.roots("movies").contentRoot, relative);
    await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, "verified fixture bytes");
    f.db.query("INSERT INTO managed_content_files(id,relative_path,size_bytes,probe_json,created_at) VALUES(?,?,22,'{}','fixture')").run(id, relative);
    f.db.query("INSERT INTO movie_version_files(version_id,file_id,sequence) VALUES(?,?,?)").run(selected.versionId, id, sequence);
  }
  const key = f.keys[0]!;
  expect(f.preview(key).canonical.files).toHaveLength(21);
  expect(codes(f.preview(key))).toContain("mutation-limit");
  expect(f.service.recover(f.command(key, "repair-view")).results[0]!.status).toBe("blocked");
  expect(f.repo.listJobsByType("movie_acquire")).toHaveLength(0);
});

test("evidence changing during preview cannot be blessed by the final fresh fence", async () => {
  await using f = await fixture();
  const selected = await f.addVersion("movies"); await fs.unlink(selected.view!);
  const read = f.repo.readRecoveryInventory.bind(f.repo);
  let removed = false;
  const readSpy = spyOn(f.repo, "readRecoveryInventory").mockImplementation((input) => {
    if (input.fence && !removed) { removed = true; unlinkSync(selected.canonical); }
    return read(input);
  });
  try {
    const resource = f.preview(f.keys[0]!);
    expect(removed).toBe(true);
    expect(resource.action).toBe("blocked");
    expect(resource.canonical.state).toBe("unknown");
    expect(codes(resource)).toContain("stale-observation");
  } finally { readSpy.mockRestore(); }
});

test("standalone router is admin-only, bounded, strict, and preview stays read-only", async () => {
  await using f = await fixture();
  const ctx: RpcContext = { repo: f.repo, startTime: 0, request: new Request("https://podible.invalid/rpc"), session: null };
  for (const method of Object.values(recoveryRouter.routes))
    await expect(executeMethod<any, any>(method, ctx, {})).rejects.toMatchObject({ code: -32001 });
  ctx.session = { is_admin: 0, user_id: f.user.id } as RpcContext["session"];
  await expect(executeMethod(recoveryRouter.routes.preview, ctx, { targets: f.keys })).rejects.toMatchObject({ code: -32003 });
  await expect(appRouter.createCaller(ctx).recovery.operation({})).rejects.toMatchObject({ code: "FORBIDDEN" });
  ctx.session = { is_admin: 1, user_id: f.user.id } as RpcContext["session"];
  expect(recoveryRouter.routes.inventory.readOnly).toBe(true); expect(recoveryRouter.routes.preview.readOnly).toBe(true);
  expect(recoveryRouter.routes.recover.readOnly).toBe(false);
  expect(recoveryRouter.routes.operation.readOnly).toBe(true);
  for (const input of [{ operationId: "bad" }, { force: true }])
    await expect(executeMethod(recoveryRouter.routes.operation, ctx, input)).rejects.toMatchObject({ code: -32602 });
  for (const params of [{ namespace: "movie-target", limit: 21 }, { namespace: "movie-request" }, { namespace: "tv-target", path: "/tmp" }])
    await expect(executeMethod(recoveryRouter.routes.inventory, ctx, params)).rejects.toMatchObject({ code: -32602 });
  const before = f.db.query("SELECT total_changes() AS n").get();
  expect((await executeMethod(recoveryRouter.routes.preview, ctx, { targets: f.keys })).resources).toHaveLength(2);
  const operationId = randomUUID(), empty = { status: "not-found", operationId } as const;
  expect(await appRouter.createCaller(ctx).recovery.operation({ operationId })).toEqual(empty);
  expect(await (await handleRpcMethod("recovery.operation", { operationId }, ctx, { readOnly: true })).json())
    .toMatchObject({ result: empty });
  expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
});
