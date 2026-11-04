import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { observedFiles } from "../../src/media/observed-files";
import { evaluateTvShadow, projectTvShadowObservation, queueTvShadowCase, syncTvShadow, type TVShadowServices } from "../../src/tv/shadow";
import { TvShadowRepo } from "../../src/tv/shadow-repo";
import { tvShadowRouter } from "../../src/rpc/tv-shadow-router";
import { executeMethod, type RpcMethodDefinition } from "../../src/rpc/framework";
import type { RpcContext } from "../../src/rpc/shared";
import type { WorkerContext } from "../../src/worker/context";
import type { SonarrObservation } from "../../src/tv/sonarr";
import type { TvDecider } from "../../src/tv/agents";
import { createJobExecutionRuntime } from "../../src/worker/execution";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { while (cleanups.length) await cleanups.pop()!(); });
const probe = { format: { duration: "1440" }, streams: [
  { codec_type: "video", codec_name: "h264", width: 1920, height: 1080, color_transfer: "bt709" },
  { codec_type: "audio", tags: { language: "jpn" } },
] };
async function setup(names = ["episode.mkv"]) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-shadow-")));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.tv.sonarrUrl = "https://sonarr.invalid/sonarr";
  settings.tv.sonarrApiKey = "opaque-sonarr-private";
  settings.agents.apiKey = "opaque-agent-private";
  settings.tv.pathMappings = [{ remoteRoot: "/downloads", localRoot: root }];
  repo.updateSettings(settings);
  for (const name of names) await fs.writeFile(path.join(root, name), "unchanged fixture media");
  const observation: SonarrObservation = {
    key: "sonarr:fixture:queue:17", releaseTitle: "An Aggregate Series S02E01 WEB 1080p", observedAt: new Date().toISOString(),
    sourcePaths: ["/downloads"], libraryPath: null,
    sonarr: { outcome: "blocked", messages: ["upstream-answer-must-not-prime-the-verifier"] },
    episodeKeys: ["tvdb:1002"],
    entry: { key: "anidb:200", tvdbId: 100, tmdbId: null, title: "An Anime Sequel", aliases: ["An Aggregate Series"],
      anime: true, year: 2020, source: "fixture", refs: [],
      relations: [{ key: "anidb:199", title: "A Prequel", relation: "PREQUEL", source: "explicit-fixture-mapping" }],
      episodes: [1, 2].map((id) => ({ key: `tvdb:${1000 + id}`, title: `Episode ${id}`, airDate: "2020-01-01", runtimeMinutes: 24,
        numbering: [{ scheme: "tvdb:aired", season: 2, episode: id, absolute: null, source: "provider-fixture" },
          { scheme: "anidb", season: 1, episode: id, absolute: null, source: "explicit-fixture-mapping" }] })),
    },
  };
  const inspections: Parameters<TvDecider["inspect"]>[0][] = [], probes: string[] = [];
  const services: TVShadowServices = {
    observe: async () => [observation],
    probe: async (file) => { probes.push(file); return probe; },
    inspect: async (input) => {
      inspections.push(input);
      return { files: [{ fileIndex: input.files.at(-1)!.index, episodeIds: input.episodes.map((e) => e.id),
        facts: { identityVerified: true, source: "web", cut: null, proper: null }, reason: "Actual episode names, duration and provider mappings agree" }],
        reason: "Requested whole episodes independently established" };
    },
  };
  const ctx = { repo, getSettings: () => settings, tvShadowServices: services, onLog() {} } satisfies WorkerContext & { tvShadowServices: TVShadowServices };
  const runtime = await createJobExecutionRuntime(ctx, path.join(root, "execution.sqlite"));
  cleanups.push(async () => { await runtime.stop(); db.close(); await fs.rm(root, { recursive: true, force: true }); });
  const latest = () => repo.tvShadow.list()[0]!;
  const evaluate = async () => { const row = latest(); await evaluateTvShadow(ctx, repo.getJob(row.jobId!)!); return repo.tvShadow.get(row.id)!; };
  return { root, db, repo, settings, observation, services, inspections, probes, ctx, latest, evaluate, runtime };
}

describe("TV immutable audit capture", () => {
  test("the real Reflow dispatcher evaluates an atomic captured case without native acquisition", async () => {
    const f = await setup();
    const sync = f.repo.createJob({ type: "tv_shadow_sync", idempotencyKey: "tv-shadow-runtime-test" });
    for (let i = 0; i < 200; i++) {
      await f.runtime.reconcile();
      if (f.repo.getJob(sync.id)?.status === "succeeded" && f.latest()?.result) break;
      await Bun.sleep(10);
    }
    expect(f.repo.getJob(sync.id)?.status).toBe("succeeded");
    expect(f.latest().result?.decision).toBe("import");
    for (let i = 0; i < 100 && f.latest().status !== "succeeded"; i++) await Bun.sleep(10);
    expect(f.latest().status).toBe("succeeded");
    expect(f.repo.tv.targets()).toEqual([]);
    expect(f.db.query("SELECT COUNT(*) AS n FROM jobs WHERE type='tv_acquire'").get()).toEqual({ n: 0 });
  });

  test("captures stable episode IDs, atomically queues a job and independently maps files without mutating media", async () => {
    const f = await setup(["sample.mkv", "whole-episode.mkv"]);
    await syncTvShadow(f.ctx);
    const captured = f.latest(), row = await f.evaluate();
    expect(captured.input.episodeIds).toEqual([2]);
    expect(captured.input.entry.episodes.find((e) => e.id === 2)?.key).toBe("tvdb:1002");
    expect(row.result?.decision).toBe("import");
    expect(row.result?.files[0]?.index).toBe(1);
    expect(row.result?.files[0]?.episodeIds).toEqual([2]);
    expect(row.result?.files[0]?.facts.videoCodec).toBe("h264");
    expect(row.result?.files[0]?.facts.audioLanguages).toEqual(["jpn"]);
    expect(row.result?.files[0]?.quality.eligibility).toBe("eligible");
    expect(f.probes.every((file) => file.startsWith(f.root + path.sep))).toBe(true);
    expect(JSON.stringify(f.inspections)).not.toContain("upstream-answer-must-not-prime");
    expect(JSON.stringify(f.inspections)).not.toContain('"sourcePaths"');
    expect(JSON.stringify(f.inspections)).not.toContain(f.root);
    expect(f.inspections[0]?.episodes.map((e) => e.id)).toEqual([2]);
    expect(f.inspections[0]?.entry.relations[0]?.key).toBe("anidb:199");
    for (const name of ["sample.mkv", "whole-episode.mkv"])
      expect(await fs.readFile(path.join(f.root, name), "utf8")).toBe("unchanged fixture media");
    expect(f.repo.tv.targets()).toEqual([]);
    expect(f.repo.movies.targets()).toEqual([]);
    expect(f.db.query("SELECT COUNT(*) AS n FROM tv_acquisition_downloads").get()).toEqual({ n: 0 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 0 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM job_commands WHERE job_id=? AND kind='enqueue'").get(captured.jobId!)).toEqual({ n: 1 });
    expect(f.repo.getJob(captured.jobId!)?.status).toBe("queued");
  });

  test("poll timestamps and upstream answer changes dedupe; changed source stats create a new immutable case", async () => {
    const f = await setup();
    await syncTvShadow(f.ctx);
    const original = f.latest();
    f.observation.observedAt = new Date(Date.now() + 1000).toISOString();
    f.observation.sonarr = { outcome: "imported", messages: ["new upstream answer"] };
    f.observation.libraryPath = "/irrelevant/upstream/location.mkv";
    await syncTvShadow(f.ctx);
    expect(f.repo.tvShadow.list(undefined, 25, true)).toHaveLength(1);
    expect(f.repo.tvShadow.get(original.id)?.input.observation.sonarr.outcome).toBe("blocked");
    expect(queueTvShadowCase(f.repo, original.id).id).toBe(original.jobId!);
    expect(f.db.query("SELECT COUNT(*) AS n FROM jobs WHERE type='tv_shadow_evaluate'").get()).toEqual({ n: 1 });
    await fs.writeFile(path.join(f.root, "episode.mkv"), "new evidence bytes");
    await syncTvShadow(f.ctx);
    expect(f.repo.tvShadow.list(undefined, 25, true)).toHaveLength(2);
    expect(f.repo.tvShadow.list()).toHaveLength(1);
    expect(f.latest().input.episodeIds).toEqual(original.input.episodeIds);
    expect(f.repo.tvShadow.get(original.id)?.input.files).toEqual(original.input.files);
  });

  test.each(["jobs", "attachment"])("a failed %s write rolls catalog/case/job/outbox back together", async (stage) => {
    const f = await setup();
    f.db.exec(stage === "jobs"
      ? "CREATE TRIGGER fail_tv_job BEFORE INSERT ON jobs WHEN NEW.type='tv_shadow_evaluate' BEGIN SELECT RAISE(ABORT,'injected job failure'); END"
      : "CREATE TRIGGER fail_tv_attach BEFORE UPDATE OF job_id ON tv_shadow_cases BEGIN SELECT RAISE(ABORT,'injected attachment failure'); END");
    await expect(syncTvShadow(f.ctx)).rejects.toThrow("injected");
    for (const table of ["tv_entries", "tv_episodes", "tv_shadow_cases", "jobs", "job_commands"])
      expect(f.db.query(`SELECT COUNT(*) AS n FROM ${table}`).get()).toEqual({ n: 0 });
  });

  test("beforeWrite fences every captured case and result, with no execution-state columns", async () => {
    const f = await setup();
    const guarded = f.repo.forExecution(() => { throw new Error("stale execution"); });
    await expect(syncTvShadow({ ...f.ctx, repo: guarded })).rejects.toThrow("stale execution");
    expect(f.repo.tvShadow.list()).toEqual([]);
    await syncTvShadow(f.ctx);
    const row = await f.evaluate();
    expect(() => guarded.tvShadow.finish(row.id, row.jobId!, row.result!)).toThrow("stale execution");
    const columns = (f.db.query("PRAGMA table_info(tv_shadow_cases)").all() as { name: string }[]).map((column) => column.name);
    expect(columns).not.toContain("status");
    expect(columns).not.toContain("error");
    expect(columns).not.toContain("attempts");
    expect(row.status).toBe("queued");
    const authoritative = new TvShadowRepo(f.db, () => {}, (id) => ({ ...f.repo.getJob(id)!, status: "failed", error: "opaque-sonarr-private https://private.example" }));
    expect(authoritative.get(row.id)?.status).toBe("failed");
    expect(authoritative.get(row.id)?.error).not.toContain("opaque-sonarr-private");
    expect(() => f.db.query("UPDATE tv_shadow_cases SET input_json='{}' WHERE id=?").run(row.id)).toThrow("immutable");
    expect(() => f.repo.tvShadow.finish(row.id, row.jobId!, { ...row.result!, decision: "reject" })).toThrow("immutable");
  });

  test("observation and independent reasons never persist configured credentials or provider URLs", async () => {
    const f = await setup();
    f.observation.sonarr.messages = [`https://private.example?apiKey=${f.settings.tv.sonarrApiKey}`];
    const inspect = f.services.inspect;
    f.services.inspect = async (input) => ({ ...await inspect(input), reason: `apiKey=${f.settings.agents.apiKey}` });
    await syncTvShadow(f.ctx);
    const row = await f.evaluate(), json = JSON.stringify(row);
    expect(json).not.toContain(f.settings.tv.sonarrApiKey);
    expect(json).not.toContain(f.settings.agents.apiKey);
    expect(json).not.toContain("private.example");
    expect(json).toContain("[redacted");
  });
});

describe("exact anime observation projection", () => {
  test("projects selected provider episode keys to logical works while preserving original source identity", async () => {
    const f = await setup();
    const raw = { ...f.observation, entry: { ...f.observation.entry, key: "tvdb:100" }, episodeKeys: ["tvdb:1001", "tvdb:1002"] };
    const projected = await projectTvShadowObservation(raw, async (entry) => ({
      entries: [
        { ...entry, key: "anidb:199", title: "First work", episodes: entry.episodes.slice(0, 1) },
        { ...entry, key: "anidb:200", title: "Second work", episodes: entry.episodes.slice(1) },
      ], warnings: [], unmappedEpisodeKeys: [],
    }));
    expect(projected.map((o) => o.episodeKeys)).toEqual([["tvdb:1001"], ["tvdb:1002"]]);
    expect(projected.map((o) => o.sourceKey)).toEqual([raw.key, raw.key]);
    expect(new Set(projected.map((o) => o.key)).size).toBe(2);
    f.services.observe = async () => projected;
    await syncTvShadow(f.ctx);
    expect(f.repo.tvShadow.list()).toHaveLength(2);
    expect(f.repo.tv.entries().map((e) => e.key).sort()).toEqual(["anidb:199", "anidb:200"]);
    expect(f.repo.tvShadow.list().flatMap((row) => row.input.observation.episodeKeys).sort()).toEqual(raw.episodeKeys);
  });

  test("does not expand a selected observation into unrelated projected work episodes", async () => {
    const f = await setup();
    const projected = await projectTvShadowObservation(f.observation, async (entry) => ({
      entries: [{ ...entry, key: "anidb:200" }], warnings: [], unmappedEpisodeKeys: [],
    }));
    expect(projected[0]?.episodeKeys).toEqual(["tvdb:1002"]);
  });

  test("failed, incomplete or ambiguous enrichment keeps original evidence with an explicit warning", async () => {
    const f = await setup();
    const projections = [
      async () => { throw new Error("private provider details"); },
      async () => ({ entries: [], warnings: [], unmappedEpisodeKeys: f.observation.episodeKeys }),
      async () => ({ entries: [f.observation.entry, { ...f.observation.entry, key: "anidb:201" }], warnings: [], unmappedEpisodeKeys: [] }),
    ];
    for (const enrich of projections) {
      const [kept] = await projectTvShadowObservation(f.observation, enrich);
      expect(kept?.key).toBe(f.observation.key);
      expect(kept?.episodeKeys).toEqual(f.observation.episodeKeys);
      expect(kept?.warnings?.join(" ")).toContain("enrichment pending");
      expect(JSON.stringify(kept)).not.toContain("private provider details");
    }
  });
});

describe("TV missing and changed evidence", () => {
  test("a cached upstream snapshot cannot hide moved or rewritten filesystem evidence", async () => {
    const f = await setup();
    await syncTvShadow(f.ctx);
    const original = f.latest();
    await fs.writeFile(path.join(f.root, "episode.mkv"), "changed after capture");
    const row = await f.evaluate();
    expect(row.result?.decision).toBe("unobservable");
    expect(row.result?.reason).toContain("changed since capture");
    expect(f.probes).toEqual([]);
    expect(f.inspections).toEqual([]);
    await syncTvShadow(f.ctx);
    expect(f.latest().id).not.toBe(original.id);
    expect((await f.evaluate()).result?.decision).toBe("import");
  });

  test("source changes during inspection invalidate even a checkpointed valid-looking map", async () => {
    const f = await setup(), inspect = f.services.inspect;
    f.services.inspect = async (input) => {
      const plan = await inspect(input);
      await fs.writeFile(path.join(f.root, "episode.mkv"), "changed during inspection");
      return plan;
    };
    await syncTvShadow(f.ctx);
    const row = await f.evaluate();
    expect(row.result?.files).toHaveLength(1);
    expect(row.result?.decision).toBe("unobservable");
    expect(row.result?.reason).toContain("changed since capture");
  });

  test("resumed durable probe receipts cannot turn changed source into a rejection", async () => {
    const f = await setup(), saved = new Map<string, unknown>();
    const execution: NonNullable<WorkerContext["execution"]> = { key: "audit-case", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>) => {
        if (saved.has(name)) return saved.get(name) as T;
        const value = await action(); saved.set(name, value); return value;
      } };
    f.services.inspect = async () => { throw new Error("opaque-agent-private"); };
    await syncTvShadow(f.ctx);
    const job = f.repo.getJob(f.latest().jobId!)!;
    await expect(evaluateTvShadow({ ...f.ctx, execution }, job)).rejects.toThrow("no decision was recorded");
    expect(f.probes).toHaveLength(1);
    expect(f.latest().result).toBeNull();
    await fs.rename(path.join(f.root, "episode.mkv"), path.join(f.root, "moved.mkv"));
    await evaluateTvShadow({ ...f.ctx, execution }, job);
    expect(f.latest().result?.decision).toBe("unobservable");
    expect(f.probes).toHaveLength(1);
  });

  test.each(["missing", "probe-failed", "no-duration", "no-audio", "no-map", "identity-unknown", "partial-map"])("%s evidence is not a confident rejection", async (kind) => {
    const f = await setup();
    if (kind === "missing") f.observation.sourcePaths = ["/downloads/absent.mkv"];
    if (kind === "probe-failed") f.services.probe = async () => { throw new Error("opaque-sonarr-private"); };
    if (kind === "no-duration") f.services.probe = async () => ({ streams: probe.streams });
    if (kind === "no-audio") f.services.probe = async () => ({ ...probe, streams: probe.streams.slice(0, 1) });
    if (kind === "no-map") f.services.inspect = async () => ({ files: [], reason: "No episode identity established" });
    if (kind === "identity-unknown") f.services.inspect = async (input) => ({ files: [{ fileIndex: 0, episodeIds: input.episodes.map((e) => e.id),
      facts: { identityVerified: false, cut: null, source: "cam", proper: null }, reason: "Filename cannot establish identity" }], reason: "Identity not established" });
    if (kind === "partial-map") f.observation.episodeKeys = ["tvdb:1001", "tvdb:1002"];
    if (kind === "partial-map") f.services.inspect = async (input) => ({ files: [{ fileIndex: 0, episodeIds: [input.episodes[0]!.id],
      facts: { identityVerified: true, cut: null, source: "web", proper: null }, reason: "Only one episode identified" }], reason: "Partial coverage" });
    await syncTvShadow(f.ctx);
    const row = await f.evaluate();
    expect(row.result?.decision).toBe("unobservable");
    expect(JSON.stringify(row.result)).not.toContain("opaque-");
  });

  test("verified hard-policy failure is a rejection, and missing quality facts need verification", async () => {
    const f = await setup(), inspect = f.services.inspect;
    f.services.inspect = async (input) => {
      const plan = await inspect(input); plan.files[0]!.facts.source = "cam"; return plan;
    };
    await syncTvShadow(f.ctx);
    expect((await f.evaluate()).result?.decision).toBe("reject");
    await fs.writeFile(path.join(f.root, "episode.mkv"), "second policy snapshot");
    f.services.inspect = async (input) => {
      const plan = await inspect(input); plan.files[0]!.facts.source = null; return plan;
    };
    await syncTvShadow(f.ctx);
    expect((await f.evaluate()).result?.decision).toBe("needs-verification");
  });

  test("invented file/episode IDs are not persisted as decisions", async () => {
    const f = await setup();
    f.services.inspect = async () => ({ files: [{ fileIndex: 100, episodeIds: [999],
      facts: { identityVerified: true, cut: null, source: "web", proper: null }, reason: "Invented mapping" }], reason: "Invalid response" });
    await syncTvShadow(f.ctx);
    await expect(f.evaluate()).rejects.toThrow("invalid episode mapping");
    expect(f.latest().result).toBeNull();
  });
});

describe("mapped source boundaries and admin read model", () => {
  test("unmapped/relative/traversing paths and source symlinks fail closed", async () => {
    const f = await setup();
    await fs.symlink("/etc/passwd", path.join(f.root, "outside.mkv"));
    await fs.mkdir(path.join(f.root, "inside"));
    await fs.writeFile(path.join(f.root, "inside/episode.mkv"), "inside");
    await fs.symlink(path.join(f.root, "inside"), path.join(f.root, "inside-link"));
    for (const remote of ["/etc/passwd", "relative.mkv", "/downloads/../etc/passwd", "/downloads-other/episode.mkv",
      "/downloads/outside.mkv", "/downloads/inside-link/episode.mkv"])
      expect((await observedFiles({ sourcePaths: [remote] }, f.settings.tv.pathMappings)).files).toEqual([]);
    f.observation.sourcePaths = ["/downloads/inside-link/episode.mkv"];
    await syncTvShadow(f.ctx);
    expect((await f.evaluate()).result?.decision).toBe("unobservable");
    expect(f.probes).toEqual([]);
  });

  test("mapping precedence and explicit library fallback preserve evidence without arbitrary root access", async () => {
    const f = await setup();
    const result = await observedFiles({ sourcePaths: ["/downloads/missing.mkv", "/library/episode.mkv"], libraryPath: "/library/episode.mkv" },
      [...f.settings.tv.pathMappings, { remoteRoot: "/library", localRoot: f.root }]);
    expect(result.source).toBe("library-fallback");
    expect(result.files[0]?.path).toBe(path.join(f.root, "episode.mkv"));
    expect(result.warnings).toContain("Source is missing or has moved");
    const deeper = await observedFiles({ sourcePaths: ["/downloads/specific/episode.mkv"] },
      [...f.settings.tv.pathMappings, { remoteRoot: "/downloads/specific", localRoot: f.root }]);
    expect(deeper.files).toHaveLength(1);
  });

  test("bounded inventory is unavailable rather than a truncated confidently judged pack", async () => {
    const f = await setup([]);
    for (let i = 0; i < 51; i++) await fs.writeFile(path.join(f.root, `${i}.mkv`), "bounded");
    const inventory = await observedFiles(f.observation, f.settings.tv.pathMappings);
    expect(inventory.files).toEqual([]);
    expect(inventory.warnings.join(" ")).toContain("safety bounds");
  });

  test("every method is admin-only; mutations accept case IDs, never paths or provider credentials", async () => {
    const f = await setup();
    const sessions = new Map<boolean, NonNullable<RpcContext["session"]>>();
    const session = (isAdmin: boolean) => {
      if (sessions.has(isAdmin)) return sessions.get(isAdmin)!;
      const user = f.repo.upsertUser({ provider: "local", providerUserId: isAdmin ? "admin" : "reader", username: "Fixture", isAdmin });
      const value = f.repo.createSession(user.id, isAdmin ? "admin-token-hash" : "reader-token-hash", null);
      sessions.set(isAdmin, value);
      return value;
    };
    const ctx: RpcContext = { repo: f.repo, request: new Request("http://localhost/rpc"), startTime: Date.now(), session: null };
    for (const method of Object.values(tvShadowRouter.routes)) {
      await expect(executeMethod(method as RpcMethodDefinition, ctx, {})).rejects.toMatchObject({ code: -32001 });
      await expect(executeMethod(method as RpcMethodDefinition, { ...ctx, session: session(false) }, {})).rejects.toMatchObject({ code: -32003 });
    }
    const admin = { ...ctx, session: session(true) };
    const status = await executeMethod(tvShadowRouter.routes.status, admin, {});
    expect(status.sonarrConfigured).toBe(true);
    expect(JSON.stringify(status)).not.toContain("opaque-");
    expect(JSON.stringify(status)).not.toContain("sonarr.invalid");
    await expect(executeMethod(tvShadowRouter.routes.sync, admin, { path: "/etc/passwd" })).rejects.toMatchObject({ code: -32602 });
    await expect(executeMethod(tvShadowRouter.routes.get, admin, { caseId: 1, sourcePaths: ["/etc/passwd"] })).rejects.toMatchObject({ code: -32602 });
    const queued = await executeMethod(tvShadowRouter.routes.sync, admin, {});
    expect(f.repo.getJob(queued.jobId)?.type).toBe("tv_shadow_sync");
    await syncTvShadow(f.ctx);
    const result = await executeMethod(tvShadowRouter.routes.list, admin, {});
    expect(result.items).toHaveLength(1);
    expect((await executeMethod(tvShadowRouter.routes.get, admin, { caseId: result.items[0]!.id })).case.id).toBe(result.items[0]!.id);
    await expect(executeMethod(tvShadowRouter.routes.retry, admin, { caseId: result.items[0]!.id })).rejects.toMatchObject({ code: -32602 });
  });
});
