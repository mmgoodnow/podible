import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { TvRepo } from "../../src/tv/repo";
import { tvCatalogEntrySchema, type TvCatalogEntry, type TvContentFile } from "../../src/tv/types";
import type { TorrentSnapshot } from "../../src/media/torrent-client";

function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  db.exec("PRAGMA foreign_keys=ON");
  // These tests exercise the domain independently of Reflow's job journal.
  const users = ["one", "two"].map((name) => (db.query(`INSERT INTO users(provider,provider_user_id,username,is_admin,created_at,updated_at)
    VALUES('local',?,?,0,?,?) RETURNING id`).get(name, name, new Date().toISOString(), new Date().toISOString()) as { id: number }).id);
  let fenced = false;
  const repo = new TvRepo(db, () => { if (fenced) throw new Error("stale owner"); }, () => ({ status: "queued" }));
  const enqueue = () => (db.query(`INSERT INTO jobs(type,status,book_id,release_id,payload_json,error,attempt_count,max_attempts,next_run_at,created_at,updated_at)
    VALUES('reconcile','queued',NULL,NULL,NULL,NULL,0,3,NULL,?,?) RETURNING id`)
    .get(new Date().toISOString(), new Date().toISOString()) as { id: number }).id;
  return { db, repo, users, enqueue, fence: () => { fenced = true; } };
}

function catalog(key = "tvdb:307375", episodeKeys = ["tvdb:1001", "tvdb:1002", "tvdb:1003"]): TvCatalogEntry {
  return {
    key, tvdbId: 307375, tmdbId: null, title: "Mob Psycho 100", aliases: [], year: 2016, anime: true,
    source: "sonarr:fixture:/api/v3/series/1",
    episodes: episodeKeys.map((key, i) => ({
      key, title: "Episode", airDate: i === 2 ? "2099-01-01" : "2016-07-12", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 1, episode: i + 1, absolute: i + 1, source: "tvdb:307375:aired" }],
    })), relations: [],
  };
}

const policy = { minimumHeight: 720 };
const facts = { identityVerified: true, source: "web" };
function file(id: string, episodeIds: number[]): TvContentFile {
  return { id, relativePath: `files/${id}.mkv`, sizeBytes: 42, probe: { streams: [] }, episodeIds };
}
function selected(ctx: ReturnType<typeof setup>, input = catalog(), keys = input.episodes.map((e) => e.key)) {
  return ctx.repo.request(ctx.users[0]!, input, policy, keys);
}
function version(ctx: ReturnType<typeof setup>, requested: ReturnType<typeof selected>, files: TvContentFile[]) {
  const acquisition = ctx.repo.startAcquisition(requested.targetId, "initial", ctx.enqueue, requested.requestId).acquisition;
  const id = ctx.repo.recordVersion(acquisition.id, facts, files);
  ctx.repo.finishAcquisition(acquisition.id);
  return id;
}
function publish(ctx: ReturnType<typeof setup>, targetId: number, versionId: number) {
  const revision = ctx.repo.target(targetId)!.revision;
  ctx.repo.stagePublication(targetId, versionId, revision);
  ctx.repo.promotePublication(targetId, versionId, revision);
  ctx.repo.clearPublication(targetId, versionId);
}

test("anime works sharing a TVDB aggregate remain separate entries with sourced explicit relations", () => {
  const ctx = setup();
  try {
    const first = catalog("anidb:11826", ["tvdb:1001"]);
    const sequel = catalog("anilist:101338", ["tvdb:2001"]);
    sequel.episodes[0]!.numbering[0]!.season = 2;
    first.relations = [{ key: sequel.key, title: first.title, relation: "SEQUEL", source: "anilist:api:season-link" }];
    const a = ctx.repo.upsertEntry(first);
    const b = ctx.repo.upsertEntry(sequel);
    expect(a.id).not.toBe(b.id);
    expect(a.tvdbId).toBe(b.tvdbId);
    expect(a.episodes[0]!.id).not.toBe(b.episodes[0]!.id);
    expect(b.episodes[0]!.numbering[0]!.season).toBe(2);
    expect(a.relations).toEqual(first.relations);
    expect(b.relations).toEqual([]); // No inferred inverse or title-based relation.
    expect(ctx.repo.entries()).toHaveLength(2);
    expect(a.refs.some((r) => r.provider === "tvdb" && r.id === "307375" && r.source === first.source)).toBe(true);
  } finally { ctx.db.close(); }
});

test("renumbering preserves entry, episode, selected request and content identities", () => {
  const ctx = setup();
  try {
    const input = catalog();
    const request = selected(ctx, input);
    const before = ctx.repo.entry(request.entryId)!;
    const updated = structuredClone(input);
    updated.title = "Renamed";
    updated.episodes[0]!.title = "Renamed episode";
    updated.episodes[0]!.numbering = [{ scheme: "tvdb:dvd", season: 8, episode: 2, absolute: 99, source: "tvdb:dvd:refresh" }];
    const after = ctx.repo.upsertEntry(updated);
    expect(after.id).toBe(before.id);
    expect(after.episodes.map((e) => e.id)).toEqual(before.episodes.map((e) => e.id));
    expect(ctx.repo.requestedEpisodeIds(request.requestId)).toEqual(request.episodeIds);
    expect(after.episodes[0]!.numbering.map((n) => n.scheme)).toEqual(["tvdb:dvd"]);
    expect(after.episodes[0]!.numbering[0]!.source).toBe("tvdb:dvd:refresh");
  } finally { ctx.db.close(); }
});

test("neither titles nor provider ordinals merge entries or episode IDs", () => {
  const ctx = setup();
  try {
    const a = ctx.repo.upsertEntry(catalog("anidb:1", ["tvdb:101"]));
    const b = ctx.repo.upsertEntry(catalog("anidb:2", ["tvdb:102"]));
    const input = catalog("anidb:1", ["tvdb:101", "tvdb:103"]);
    input.episodes[1]!.numbering = structuredClone(input.episodes[0]!.numbering);
    const extended = ctx.repo.upsertEntry(input);
    expect(a.id).not.toBe(b.id);
    expect(extended.episodes[0]!.id).toBe(a.episodes[0]!.id);
    expect(extended.episodes[1]!.id).not.toBe(a.episodes[0]!.id);
    expect(extended.episodes[1]!.id).not.toBe(b.episodes[0]!.id);
  } finally { ctx.db.close(); }
});

test("explicit shared episode membership retains independent entry-scoped numbering", () => {
  const ctx = setup();
  try {
    const a = ctx.repo.upsertEntry(catalog("anidb:1", ["tvdb:101"]));
    const second = catalog("anilist:2", ["tvdb:101"]);
    second.episodes[0]!.numbering[0]!.season = 5;
    const b = ctx.repo.upsertEntry(second);
    expect(a.episodes[0]!.id).toBe(b.episodes[0]!.id);
    expect(ctx.repo.entry(a.id)!.episodes[0]!.numbering[0]!.season).toBe(1);
    expect(ctx.repo.entry(b.id)!.episodes[0]!.numbering[0]!.season).toBe(5);
  } finally { ctx.db.close(); }
});

test("ephemeral provider keys are explicit and never upgraded through a matching ordinal", () => {
  const ctx = setup();
  try {
    const input = catalog("sonarr:fixture:1", ["ephemeral:sonarr:fixture:series-1:slot-1"]);
    const initial = ctx.repo.upsertEntry(input).episodes[0]!;
    expect(initial.identity.stability).toBe("ephemeral");
    expect(initial.refs).toEqual([]);
    const stable = ctx.repo.upsertEntry(catalog("sonarr:fixture:1", ["tvdb:101"]));
    expect(stable.episodes.find((e) => e.key === "tvdb:101")!.id).not.toBe(initial.id);
    expect(tvCatalogEntrySchema.safeParse({ ...input, episodes: [{ ...input.episodes[0], identity: { stability: "provider", source: "bad" } }] }).success).toBe(false);
  } finally { ctx.db.close(); }
});

test("catalog ingestion rejects duplicate keys, ambiguous maps and contradictory primary IDs atomically", () => {
  const ctx = setup();
  try {
    const duplicate = catalog();
    duplicate.episodes[1]!.key = duplicate.episodes[0]!.key;
    expect(() => ctx.repo.upsertEntry(duplicate)).toThrow("Duplicate catalog");
    const maps = catalog();
    maps.episodes[0]!.numbering.push(maps.episodes[0]!.numbering[0]!);
    expect(() => ctx.repo.upsertEntry(maps)).toThrow("Duplicate episode numbering");
    expect(() => ctx.repo.upsertEntry({ ...catalog(), tvdbId: 1 })).toThrow("conflicts");
    expect(ctx.repo.entries()).toEqual([]);
  } finally { ctx.db.close(); }
});

test("selected request episodes retain future episodes and reject foreign or unresolved selections", () => {
  const ctx = setup();
  try {
    const request = selected(ctx, catalog(), ["tvdb:1001", "tvdb:1003"]);
    expect(ctx.repo.requestedEpisodes(request.requestId).map((e) => e.key)).toEqual(["tvdb:1001", "tvdb:1003"]);
    expect(ctx.repo.missingEpisodes(request.requestId)).toHaveLength(2);
    expect(() => selected(ctx, catalog(), ["tvdb:404"])).toThrow("not a member");
    expect(() => selected(ctx, catalog(), [])).toThrow("needs selected episodes");
    const other = ctx.repo.upsertEntry(catalog("anidb:2", ["tvdb:2001"]));
    expect(() => ctx.repo.requestEpisodes(ctx.users[0]!, request.entryId, policy, [other.episodes[0]!.id])).toThrow("another entry");
    expect(ctx.repo.requests()).toHaveLength(1);
  } finally { ctx.db.close(); }
});

test("Seerr origins fan out to logical anime entries and merge newly approved stable IDs idempotently", () => {
  const ctx = setup();
  try {
    const origin = { instance: "https://seerr.example", externalRequestId: 7, source: { seasons: [1, 2] } };
    const input = { userId: ctx.users[0]!, entry: catalog("anidb:1"), policy, episodeKeys: ["tvdb:1001"], origin };
    const a = ctx.repo.requestApproved(input);
    const b = ctx.repo.requestApproved({ ...input, entry: catalog("anidb:2", ["tvdb:2001"]), episodeKeys: ["tvdb:2001"] });
    expect(a.requestId).not.toBe(b.requestId);
    expect(a.targetId).not.toBe(b.targetId);
    expect(ctx.repo.origins(origin.instance, 7)).toHaveLength(2);
    expect(() => ctx.repo.origin(origin.instance, 7)).toThrow("multiple entries");
    expect(ctx.repo.origin(origin.instance, 7, a.entryId)!.requestId).toBe(a.requestId);
    expect(ctx.repo.requestApproved(input)).toEqual(a);
    expect(ctx.repo.requestApproved({ ...input, entry: catalog("anidb:2", ["tvdb:2001"]), episodeKeys: ["tvdb:2001"] })).toEqual(b);
    const expanded = ctx.repo.requestApproved({ ...input, episodeKeys: ["tvdb:1001", "tvdb:1002"] });
    expect(expanded.requestId).toBe(a.requestId);
    expect(expanded.episodeIds).toHaveLength(2);
    expect(ctx.repo.requestApproved(input).episodeIds).toEqual(expanded.episodeIds);
    expect(ctx.repo.requests()).toHaveLength(2);
    expect(ctx.repo.events(a.targetId)).toHaveLength(1);
  } finally { ctx.db.close(); }
});

test("origin instance and local principal scope prevent request hijacking but allow separate instances", () => {
  const ctx = setup();
  try {
    const input = { userId: ctx.users[0]!, entry: catalog(), policy, episodeKeys: ["tvdb:1001"],
      origin: { instance: "seerr-a", externalRequestId: 3, source: { approved: true } } };
    const a = ctx.repo.requestApproved(input);
    expect(() => ctx.repo.requestApproved({ ...input, userId: ctx.users[1]! })).toThrow("principal");
    expect(() => ctx.repo.requestApproved({ ...input, policy: { minimumHeight: 1080 } })).toThrow("policy");
    const b = ctx.repo.requestApproved({ ...input, origin: { ...input.origin, instance: "seerr-b" } });
    expect(b.targetId).toBe(a.targetId);
    expect(b.requestId).not.toBe(a.requestId);
    expect(ctx.repo.canAccessTarget(ctx.users[1]!, a.targetId)).toBe(false);
    expect(ctx.repo.activeRequestCount(a.targetId)).toBe(2);
    ctx.repo.withdrawRequest(a.requestId);
    ctx.repo.withdrawRequest(b.requestId);
    expect(ctx.repo.activeRequestCount(a.targetId)).toBe(0);
    expect(ctx.repo.canAccessTarget(ctx.users[0]!, a.targetId)).toBe(false);
  } finally { ctx.db.close(); }
});

test("requests share canonical policies and separate different users without title matching", () => {
  const ctx = setup();
  try {
    expect(ctx.repo.hasTargets()).toBe(false);
    const first = ctx.repo.request(ctx.users[0]!, catalog(), { a: 1, b: { c: 2 } }, ["tvdb:1001"]);
    const next = ctx.repo.request(ctx.users[1]!, catalog(), { b: { c: 2 }, a: 1 }, ["tvdb:1002"]);
    expect(first.targetId).toBe(next.targetId);
    expect(first.requestId).not.toBe(next.requestId);
    expect(ctx.repo.hasTargets()).toBe(true);
    expect(ctx.repo.requests(ctx.users[0]!)).toHaveLength(1);
  } finally { ctx.db.close(); }
});

test("request-scoped acquisitions enqueue atomically and continuation creates new work after finish", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    expect(() => ctx.repo.startAcquisition(request.targetId, "initial", () => { ctx.enqueue(); throw new Error("enqueue failed"); }, request.requestId)).toThrow("enqueue failed");
    expect(ctx.db.query("SELECT COUNT(*) AS n FROM jobs").get()).toEqual({ n: 0 });
    expect(ctx.repo.acquisitions(request.targetId)).toHaveLength(0);
    const a = ctx.repo.startAcquisition(request.targetId, "initial", ctx.enqueue, request.requestId);
    const again = ctx.repo.startAcquisition(request.targetId, "initial", ctx.enqueue, request.requestId);
    expect(again.acquisition.id).toBe(a.acquisition.id);
    expect(again.shared).toBe(true);
    ctx.repo.decision(a.acquisition.id, { selectedEpisodeIds: request.episodeIds });
    expect(JSON.parse(ctx.repo.acquisition(a.acquisition.id)!.decision_json!)).toEqual({ selectedEpisodeIds: request.episodeIds });
    ctx.repo.finishAcquisition(a.acquisition.id);
    expect(ctx.repo.startAcquisition(request.targetId, "recovery", ctx.enqueue, request.requestId).acquisition.id).not.toBe(a.acquisition.id);
    expect(ctx.db.query("SELECT COUNT(*) AS n FROM books").get()).toEqual({ n: 0 });
  } finally { ctx.db.close(); }
});

test("versions require verified owned files with explicit stable episode coverage and immutable replay", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const acquisition = ctx.repo.startAcquisition(request.targetId, "initial", ctx.enqueue).acquisition;
    const content = file("verified", request.episodeIds.slice(0, 2));
    expect(() => ctx.repo.recordVersion(acquisition.id, { identityVerified: false }, [content])).toThrow();
    expect(() => ctx.repo.recordVersion(acquisition.id, facts, [])).toThrow("needs content files");
    expect(() => ctx.repo.recordVersion(acquisition.id, facts, [{ ...content, episodeIds: [] }])).toThrow();
    expect(() => ctx.repo.recordVersion(acquisition.id, facts, [{ ...content, relativePath: "../escape.mkv" }])).toThrow();
    const id = ctx.repo.recordVersion(acquisition.id, facts, [content]);
    expect(ctx.repo.recordVersion(acquisition.id, facts, [content])).toBe(id);
    expect(() => ctx.repo.recordVersion(acquisition.id, facts, [{ ...content, sizeBytes: 43 }])).toThrow("different verified version");
    expect(ctx.repo.version(id)!.files).toEqual([content]);
    expect(ctx.repo.version(id)!.episodeIds).toEqual(content.episodeIds);
    expect(ctx.db.query("SELECT relative_path FROM managed_content_files WHERE id=?").get(content.id)).toEqual({ relative_path: content.relativePath });
  } finally { ctx.db.close(); }
});

test("partial packs only activate covered episodes and missing-episode continuation keeps earlier episodes", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const [one, two, three] = request.episodeIds as [number, number, number];
    const first = version(ctx, request, [file("first-pack", [one, two])]);
    ctx.repo.stagePublication(request.targetId, first, 0);
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual([]);
    expect(ctx.repo.missingEpisodes(request.requestId)).toHaveLength(3);
    ctx.repo.promotePublication(request.targetId, first, 0);
    ctx.repo.clearPublication(request.targetId, first);
    expect(ctx.repo.missingEpisodes(request.requestId).map((e) => e.id)).toEqual([three]);
    const second = version(ctx, request, [file("continuation", [three])]);
    publish(ctx, request.targetId, second);
    expect(ctx.repo.missingEpisodes(request.requestId)).toEqual([]);
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual([
      { episode_id: one, version_id: first }, { episode_id: two, version_id: first }, { episode_id: three, version_id: second },
    ]);
    expect(ctx.repo.target(request.targetId)!.revision).toBe(2);
  } finally { ctx.db.close(); }
});

test("publication requires staged ownership and exact revisions, with crash-safe promote replay", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const id = version(ctx, request, [file("one", [request.episodeIds[0]!])]);
    const other = selected(ctx, catalog("anidb:99", ["tvdb:999"]));
    expect(() => ctx.repo.stagePublication(other.targetId, id)).toThrow("another target");
    const otherPolicy = ctx.repo.request(ctx.users[0]!, catalog(), { minimumHeight: 1080 }, ["tvdb:1001"]);
    expect(() => ctx.repo.stagePublication(otherPolicy.targetId, id)).toThrow("another target");
    expect(() => ctx.repo.promotePublication(request.targetId, id, 0)).toThrow("not staged");
    expect(() => ctx.repo.stagePublication(request.targetId, id, 9)).toThrow("changed");
    ctx.repo.stagePublication(request.targetId, id, 0);
    expect(() => ctx.repo.clearPublication(request.targetId, id)).toThrow("unpromoted");
    expect(() => ctx.repo.promotePublication(request.targetId, id, 9)).toThrow("not staged");
    ctx.repo.promotePublication(request.targetId, id, 0);
    ctx.repo.stagePublication(request.targetId, id, 0);
    ctx.repo.promotePublication(request.targetId, id, 0);
    expect(ctx.repo.target(request.targetId)!.revision).toBe(1);
    expect(() => ctx.repo.abortPublication(request.targetId, id)).toThrow("active");
    ctx.repo.clearPublication(request.targetId, id);
    expect(ctx.repo.publications()).toEqual([]);
  } finally { ctx.db.close(); }
});

test("stale publication cannot change any episode or refresh its original revision ticket", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const id = version(ctx, request, [file("stale", request.episodeIds)]);
    ctx.repo.stagePublication(request.targetId, id, 0);
    ctx.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=?").run(request.targetId);
    ctx.repo.stagePublication(request.targetId, id);
    expect(ctx.repo.publications()[0]!.expected_revision).toBe(0);
    expect(() => ctx.repo.promotePublication(request.targetId, id, 0)).toThrow("changed");
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual([]);
    ctx.repo.abortPublication(request.targetId, id);
    expect(ctx.repo.publications()).toEqual([]);
  } finally { ctx.db.close(); }
});

test("multi-episode file partial replacement is rejected atomically; complete coverage replacement succeeds", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const [one, two, three] = request.episodeIds as [number, number, number];
    const old = version(ctx, request, [file("double", [one, two]), file("single", [three])]);
    publish(ctx, request.targetId, old);
    const partial = version(ctx, request, [file("replacement-one", [one])]);
    ctx.repo.stagePublication(request.targetId, partial, 1);
    const before = ctx.repo.activeEpisodes(request.targetId);
    expect(() => ctx.repo.promotePublication(request.targetId, partial, 1)).toThrow("partially invalidate");
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual(before);
    expect(ctx.repo.target(request.targetId)!.revision).toBe(1);
    ctx.repo.abortPublication(request.targetId, partial);
    const complete = version(ctx, request, [file("new-one", [one]), file("new-two", [two])]);
    publish(ctx, request.targetId, complete);
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual([
      { episode_id: one, version_id: complete }, { episode_id: two, version_id: complete }, { episode_id: three, version_id: old },
    ]);
  } finally { ctx.db.close(); }
});

test("content ownership, episode ownership and existing file coverage cannot be reassigned", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const first = version(ctx, request, [file("owned", [request.episodeIds[0]!])]);
    const other = selected(ctx, catalog("anidb:99", ["tvdb:999"]));
    const a = ctx.repo.startAcquisition(other.targetId, "initial", ctx.enqueue).acquisition;
    expect(() => ctx.repo.recordVersion(a.id, facts, [file("foreign", [request.episodeIds[0]!])])).toThrow("another entry");
    expect(() => ctx.repo.recordVersion(a.id, facts, [file("owned", other.episodeIds)])).toThrow("owned by another");
    const b = ctx.repo.startAcquisition(request.targetId, "recovery", ctx.enqueue).acquisition;
    expect(() => ctx.repo.recordVersion(b.id, facts, [file("owned", [request.episodeIds[1]!])])).toThrow("different episode coverage");
    const unchanged = ctx.repo.recordVersion(b.id, facts, [file("owned", [request.episodeIds[0]!])]);
    expect(unchanged).not.toBe(first);
    expect(ctx.db.query("SELECT COUNT(*) AS n FROM managed_content_files").get()).toEqual({ n: 1 });
  } finally { ctx.db.close(); }
});

test("TV torrent bindings share the neutral client table and preserve immutable layout", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const a = ctx.repo.startAcquisition(request.targetId, "initial", ctx.enqueue).acquisition;
    const ref = { clientId: "qbit", nativeId: "hash" };
    const location = { rootId: "downloads", relativePath: "tv/pack" };
    const id = ctx.repo.bindDownload(a.id, ref, location, "offer");
    expect(ctx.repo.bindDownload(a.id, ref, location, "offer")).toBe(id);
    expect(() => ctx.repo.bindDownload(a.id, ref, { ...location, relativePath: "other" }, "offer")).toThrow("another layout");
    const snapshot: TorrentSnapshot = {
      ref, location, state: "seeding", complete: true, bytesDone: 42, sizeBytes: 42,
      downloadBytesPerSecond: 0, message: null, observedAt: new Date().toISOString(),
    };
    ctx.repo.observeDownload(id, snapshot);
    expect(JSON.parse(ctx.repo.torrent(ref.clientId, ref.nativeId)!.observation_json!)).toEqual(snapshot);
    expect(ctx.repo.downloads(request.targetId)).toHaveLength(1);
    expect(ctx.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 1 });
    expect(ctx.db.query("SELECT COUNT(*) AS n FROM tv_acquisition_downloads").get()).toEqual({ n: 1 });
  } finally { ctx.db.close(); }
});

test("execution fence protects model writes, publications, events and torrent observations", () => {
  const ctx = setup();
  try {
    const request = selected(ctx);
    const a = ctx.repo.startAcquisition(request.targetId, "initial", ctx.enqueue).acquisition;
    const id = ctx.repo.recordVersion(a.id, facts, [file("fenced", request.episodeIds)]);
    ctx.repo.stagePublication(request.targetId, id, 0);
    ctx.fence();
    expect(() => ctx.repo.upsertEntry({ ...catalog(), title: "bad" })).toThrow("stale owner");
    expect(() => ctx.repo.promotePublication(request.targetId, id, 0)).toThrow("stale owner");
    expect(() => ctx.repo.decision(a.id, {})).toThrow("stale owner");
    expect(() => ctx.repo.finishAcquisition(a.id)).toThrow("stale owner");
    expect(() => ctx.repo.event(request.targetId, a.id, "stale-event", "available", "bad")).toThrow("stale owner");
    expect(() => ctx.repo.bindDownload(a.id, { clientId: "qbit", nativeId: "bad" }, { rootId: "dl", relativePath: "pack" }, "offer")).toThrow("stale owner");
    expect(ctx.repo.activeEpisodes(request.targetId)).toEqual([]);
    expect(ctx.repo.entry(request.entryId)!.title).not.toBe("bad");
    expect(ctx.repo.events(request.targetId)).toHaveLength(1);
  } finally { ctx.db.close(); }
});
