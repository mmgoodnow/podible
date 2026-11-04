import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { MovieIndexerError, type MovieOffer } from "../../src/movies/offers";
import { matchTvFeed, refreshTvFeedOffer, type TvFeedContext, type TvFeedMatcher } from "../../src/tv/feed";
import { queueTvAcquisition } from "../../src/tv/acquisition";
import type { TvCatalogEntry } from "../../src/tv/types";
import type { IndexerMedia, SearchResult } from "../../src/media/indexer-gateway";

function catalog(key = "anidb:5373", episodeBase = 100): TvCatalogEntry {
  return { key, tvdbId: 79525, tmdbId: null, title: "Independent anime work R2", aliases: ["Native release alias"],
    year: null, anime: true, refs: [], source: "explicit:fixture", relations: [
      { key: "anidb:4521", title: "Independent predecessor", relation: "PREQUEL", source: "anilist:2904" },
    ], episodes: [1, 2, 3, 4].map(i => ({ key: `tvdb:${episodeBase + i}`, title: `Episode ${i}`,
      airDate: i === 3 ? "2099-01-01" : i === 4 ? null : "2008-04-06", runtimeMinutes: 24,
      numbering: [{ scheme: "tvdb:aired", season: 2, episode: i + 12, absolute: i + 37, source: "provider:aired" },
        { scheme: "anidb", season: 1, episode: i, absolute: null, source: "anime-list:explicit" }],
    })) };
}
function release(id: string, mediaType: IndexerMedia = "tv"): SearchResult<IndexerMedia> {
  return { title: `Native release alias ${id} [pack]`, provider: "Shared TV provider", mediaType,
    guid: id, url: `https://private.invalid/${id}?apikey=private-feed-secret`, sizeBytes: 1000,
    infoHash: null, seeders: 2, leechers: null, raw: { attr: [
      { name: "tvdbid", value: "79525" }, { name: "anidbid", value: "5373" },
      { name: "description", value: "untrusted-private-description" },
    ] } };
}
function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  db.exec("PRAGMA foreign_keys=ON");
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  const users = ["first", "second"].map(username => repo.upsertUser({ provider: "local",
    providerUserId: username, username, isAdmin: false }));
  const input = catalog();
  const requested = repo.tv.request(users[0]!.id, input, settings.movies.qualityPolicy,
    [input.episodes[0]!.key, input.episodes[2]!.key, input.episodes[3]!.key]);
  const feedId = "tv-shared-fixture";
  repo.releaseInbox.ensureFeed(feedId, "Shared TV provider", "tv", new Date().toISOString());
  const ingest = (...results: SearchResult<IndexerMedia>[]) => repo.releaseInbox.ingest(feedId, results, new Date().toISOString());
  const ids = ingest(release("one"), release("two"));
  const job = (releaseIds = ids) => repo.createJob({ type: "tv_feed_match", payload: { releaseIds }, maxAttempts: 3 });
  const ctx: TvFeedContext = { repo, getSettings: () => settings };
  const matches: TvFeedMatcher = async ({ targets, offers }) => targets.map(t => ({
    targetId: t.targetId, releaseIndexes: offers.map((_, i) => i), reason: "Plausible native numbering or partial pack; verify downstream",
  }));
  const acquisitions = () => repo.tv.acquisitions(requested.targetId);
  const receipts = () => db.query("SELECT * FROM release_considerations ORDER BY release_id,target_revision").all() as {
    target_kind: string; target_id: number; target_revision: string; source_job_id: number; job_id: number; decision_note: string;
  }[];
  const decision = (index = 0) => JSON.parse(acquisitions()[index]!.decision_json!) as {
    feedOffers: MovieOffer[]; feedEvidence: { source: string; sourceJobId: number; requestId: number;
      entryKey: string; requestedEpisodeIds: number[]; releases: { inboxId: number; feedId: string }[];
      episodes: { id: number; key: string; numbering: unknown[] }[] };
  };
  return { db, repo, settings, users, input, requested, feedId, ids, ingest, job, ctx, matches, acquisitions, receipts, decision };
}
type Fixture = ReturnType<typeof setup>;
async function fixture(action: (f: Fixture) => Promise<void> | void) {
  const f = setup();
  try { await action(f); } finally { f.db.close(); }
}
function execution(f: Fixture, onSleep?: () => void) {
  const steps = new Map<string, unknown>();
  const sleeps: { name: string; delay: number }[] = [];
  f.ctx.execution = { key: "tv-feed-fixture", assertActive() {},
    async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
      if (steps.has(name)) return structuredClone(steps.get(name)) as T;
      const value = await handler(); steps.set(name, structuredClone(value)); return value;
    }, async sleep(name, delay) { sleeps.push({ name, delay }); onSleep?.(); } };
  return { sleeps, steps };
}
function publish(f: Fixture, episodeIds: number[]) {
  const a = f.repo.tv.startAcquisition(f.requested.targetId, "initial", id =>
    f.repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id } }).id, f.requested.requestId).acquisition;
  const versionId = f.repo.tv.recordVersion(a.id, { identityVerified: true, source: "web" }, [{ id: `content-${a.id}`,
    relativePath: `files/${a.id}.mkv`, sizeBytes: 42, probe: { streams: [] }, episodeIds }]);
  const revision = f.repo.tv.target(f.requested.targetId)!.revision;
  f.repo.tv.stagePublication(f.requested.targetId, versionId, revision);
  f.repo.tv.promotePublication(f.requested.targetId, versionId, revision);
  f.repo.tv.clearPublication(f.requested.targetId, versionId);
  f.repo.tv.finishAcquisition(a.id);
  return a.id;
}

test("TV feed routes only aired requested missing episodes, persists exact offers/evidence, never starts a search", () => fixture(async f => {
  let calls = 0;
  f.ctx.getSettings = () => { throw new Error("Feed routing must not initialize per-title search services"); };
  f.ctx.tvFeedMatcher = async ({ targets, offers }) => {
    calls++;
    expect(targets).toHaveLength(1);
    expect(targets[0]!.entry.key).toBe("anidb:5373");
    expect(targets[0]!.entry.tvdbId).toBe(79525);
    expect(targets[0]!.episodes.map(e => e.key)).toEqual(["tvdb:101"]);
    expect(targets[0]!.entry.episodes.map(e => e.key)).toEqual(["tvdb:101"]);
    expect(targets[0]!.episodes[0]!.numbering.map(n => [n.scheme, n.episode])).toEqual([["anidb", 1], ["tvdb:aired", 13]]);
    const modelInput = JSON.stringify({ targets, offers });
    expect(modelInput).not.toContain("private-feed-secret");
    expect(modelInput).not.toContain("untrusted-private-description");
    expect(offers[0]!.attributes).toEqual([{ name: "tvdbid", value: "79525" }, { name: "anidbid", value: "5373" }]);
    return [{ targetId: targets[0]!.targetId, releaseIndexes: [1, 0, 0], reason: "Native alias and partial pack may match" }];
  };
  const job = f.job();
  expect(await matchTvFeed(f.ctx, job)).toBe("done");
  expect(calls).toBe(1);
  expect(f.acquisitions()).toHaveLength(1);
  const decision = f.decision();
  expect(decision.feedOffers.map(o => o.inboxId)).toEqual([f.ids[1], f.ids[0]]);
  expect(decision.feedOffers[0]!.url).toContain("private-feed-secret");
  expect(decision.feedEvidence.sourceJobId).toBe(job.id);
  expect(decision.feedEvidence.requestId).toBe(f.requested.requestId);
  expect(decision.feedEvidence.requestedEpisodeIds).toEqual([f.requested.episodeIds[0]!]);
  expect(decision.feedEvidence.episodes[0]!.key).toBe("tvdb:101");
  expect(decision.feedEvidence.releases.map(r => r.feedId)).toEqual([f.feedId, f.feedId]);
  expect(f.receipts()).toHaveLength(2);
  expect(f.receipts().every(r => r.target_kind === "tv" && r.target_id === f.requested.targetId && r.source_job_id === job.id)).toBe(true);
  expect(f.repo.getJob(f.acquisitions()[0]!.job_id!)!.payload_json).toBe(JSON.stringify({ acquisitionId: f.acquisitions()[0]!.id }));
}));

test("one compatible target unions separate active requests without losing per-request episode ownership", () => fixture(async f => {
  const second = f.repo.tv.request(f.users[1]!.id, f.input, f.settings.movies.qualityPolicy, ["tvdb:102"]);
  let episodeKeys: string[] = [];
  await matchTvFeed(f.ctx, f.job(), async input => {
    expect(input.targets).toHaveLength(1);
    episodeKeys = input.targets[0]!.episodes.map(e => e.key);
    return f.matches(input);
  });
  expect(second.targetId).toBe(f.requested.targetId);
  expect(episodeKeys).toEqual(["tvdb:101", "tvdb:102"]);
  expect(f.acquisitions().map(a => a.request_id).sort()).toEqual([f.requested.requestId, second.requestId].sort());
  const evidence = f.acquisitions().map(a => JSON.parse(a.decision_json!).feedEvidence);
  expect(evidence.find(e => e.requestId === second.requestId).requestedEpisodeIds).toEqual(second.episodeIds);
  expect(f.receipts()).toHaveLength(2); // One receipt per release/target, not per user request.
}));

test("published coverage, withdrawn requests, future/undated episodes never trigger feed acquisitions", () => fixture(async f => {
  publish(f, [f.requested.episodeIds[0]!]);
  const before = f.acquisitions().length;
  let calls = 0;
  await matchTvFeed(f.ctx, f.job(), async () => { calls++; return []; });
  expect(calls).toBe(0);
  expect(f.acquisitions()).toHaveLength(before);
  expect(f.receipts()).toEqual([]);
  const requested = f.repo.tv.request(f.users[1]!.id, f.input, f.settings.movies.qualityPolicy, ["tvdb:102"]);
  f.repo.tv.withdrawRequest(requested.requestId);
  await matchTvFeed(f.ctx, f.job(), async () => { calls++; return []; });
  expect(calls).toBe(0);
}));

test("retention uses tv-target identity and desired eviction, including eviction after semantic routing", () => fixture(async f => {
  const key = { namespace: "tv-target", id: String(f.requested.targetId) };
  f.repo.retention.evict(key, { expectedRevision: 0, reason: "Explicit user eviction" });
  let calls = 0;
  await matchTvFeed(f.ctx, f.job(), async () => { calls++; return []; });
  expect(calls).toBe(0);
  f.repo.retention.request(key, { expectedRevision: 1, reason: "Wanted again" });
  await matchTvFeed(f.ctx, f.job(), async input => {
    calls++;
    f.repo.retention.evict(key, { expectedRevision: 2, reason: "Evicted while matching" });
    return f.matches(input);
  });
  expect(calls).toBe(1);
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
}));

test("withdrawal during matching cannot enqueue or consume a receipt", () => fixture(async f => {
  await matchTvFeed(f.ctx, f.job(), async input => {
    f.repo.tv.withdrawRequest(f.requested.requestId);
    return f.matches(input);
  });
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
}));

test("same-batch replay and a later source job do not overwrite offers or duplicate acquisition/receipts", () => fixture(async f => {
  const job = f.job();
  let calls = 0;
  const matcher: TvFeedMatcher = async input => { calls++; return f.matches(input); };
  await matchTvFeed(f.ctx, job, matcher);
  const first = structuredClone(f.acquisitions());
  await matchTvFeed(f.ctx, job, matcher); // Still queued: replay must not wait for its own dispatch.
  await matchTvFeed(f.ctx, f.job(), matcher);
  expect(f.acquisitions()).toEqual(first);
  expect(f.receipts()).toHaveLength(2);
  expect(calls).toBe(1);
}));

test("scope changes are semantically rechecked before dispatch and retain request-specific evidence", () => fixture(async f => {
  let calls = 0;
  await matchTvFeed(f.ctx, f.job(), async input => {
    calls++;
    if (calls === 1) f.repo.tv.request(f.users[1]!.id, f.input, f.settings.movies.qualityPolicy, ["tvdb:102"]);
    if (calls === 2) expect(input.targets[0]!.episodes.map(e => e.key)).toEqual(["tvdb:101", "tvdb:102"]);
    return f.matches(input);
  });
  expect(calls).toBe(2);
  expect(f.acquisitions()).toHaveLength(2);
}));

test("source-job receipts survive revision changes and prevent replay into a later target state", () => fixture(async f => {
  execution(f);
  const job = f.job();
  await matchTvFeed(f.ctx, job, f.matches);
  f.repo.tv.finishAcquisition(f.acquisitions()[0]!.id);
  f.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=?").run(f.requested.targetId);
  await matchTvFeed(f.ctx, job, f.matches);
  expect(f.acquisitions()).toHaveLength(1);
  await matchTvFeed(f.ctx, f.job(), f.matches);
  expect(f.acquisitions()).toHaveLength(2);
  expect(new Set(f.receipts().map(r => r.target_revision)).size).toBe(2);
}));

test("provider/matcher failure propagates with retry metadata, not a negative receipt", () => fixture(async f => {
  const failure = new MovieIndexerError("Provider temporarily unavailable", 90_000, true);
  const job = f.job();
  try { await matchTvFeed(f.ctx, job, async () => { throw failure; }); throw new Error("expected upstream failure"); }
  catch (error) { expect(error).toBe(failure); }
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
  await matchTvFeed(f.ctx, job, f.matches);
  expect(f.acquisitions()).toHaveLength(1);
}));

test("genuine no-match is distinct from an unavailable semantic matcher", () => fixture(async f => {
  await expect(matchTvFeed(f.ctx, f.job())).rejects.toThrow();
  expect(await matchTvFeed(f.ctx, f.job(), async () => [])).toBe("done");
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
}));

for (const bad of [
  { targetId: 999999, releaseIndexes: [0], reason: "Unknown target" },
  { targetId: 1, releaseIndexes: [99], reason: "Unknown release" },
  { targetId: 1, releaseIndexes: [-1], reason: "Negative index" },
  { targetId: 1, releaseIndexes: [0], reason: "Injected release", url: "https://attacker.invalid" },
]) test(`invalid matcher output is atomic: ${bad.reason}`, () => fixture(async f => {
  await expect(matchTvFeed(f.ctx, f.job(), async () => [
    { targetId: f.requested.targetId, releaseIndexes: [0], reason: "Valid first match" }, bad,
  ])).rejects.toThrow();
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
}));

test("mutating matcher projections cannot change server-owned offers or request identity", () => fixture(async f => {
  await matchTvFeed(f.ctx, f.job(), async input => {
    const match = await f.matches(input);
    input.targets[0]!.entry.key = "anidb:999";
    input.targets[0]!.episodes[0]!.key = "tvdb:999";
    Object.assign(input.offers[0]!, { title: "Changed", key: "attacker", url: "https://attacker.invalid", inboxId: 999 });
    return match;
  });
  expect(f.decision().feedEvidence.entryKey).toBe("anidb:5373");
  expect(f.decision().feedEvidence.episodes[0]!.key).toBe("tvdb:101");
  expect(f.decision().feedOffers[0]!.url).not.toContain("attacker");
  expect(f.decision().feedOffers[0]!.key).not.toBe("attacker");
}));

test("receipt insertion failure rolls back acquisition, immutable evidence, job and outbox atomically", () => fixture(async f => {
  const job = f.job();
  const before = f.repo.listJobsByType("tv_acquire").length;
  f.db.exec("CREATE TRIGGER reject_tv_receipt BEFORE INSERT ON release_considerations BEGIN SELECT RAISE(ABORT,'receipt fault'); END");
  await expect(matchTvFeed(f.ctx, job, f.matches)).rejects.toThrow("receipt fault");
  expect(f.acquisitions()).toEqual([]);
  expect(f.repo.listJobsByType("tv_acquire")).toHaveLength(before);
  expect(f.receipts()).toEqual([]);
  f.db.exec("DROP TRIGGER reject_tv_receipt");
  await matchTvFeed(f.ctx, job, f.matches);
  expect(f.acquisitions()).toHaveLength(1);
}));

test("busy targets defer without receipts, then dispatch after durable waiting", () => fixture(async f => {
  const active = queueTvAcquisition(f.repo, f.requested.requestId).acquisition;
  await expect(matchTvFeed(f.ctx, f.job(), f.matches)).rejects.toThrow("requires durable execution");
  expect(f.receipts()).toEqual([]);
  const run = execution(f, () => f.repo.tv.finishAcquisition(active.id));
  await matchTvFeed(f.ctx, f.job(), f.matches);
  expect(run.sleeps).toHaveLength(1);
  expect(run.sleeps[0]!.delay).toBe(60_000);
  expect(f.acquisitions()).toHaveLength(2);
  expect(f.receipts()).toHaveLength(2);
}));

test("execution ownership loss after semantic work prevents dispatch and receipts", () => fixture(async f => {
  execution(f);
  let lost = false;
  f.ctx.execution!.assertActive = () => { if (lost) throw new Error("stale execution"); };
  await expect(matchTvFeed(f.ctx, f.job(), async input => { lost = true; return f.matches(input); })).rejects.toThrow("stale execution");
  expect(f.acquisitions()).toEqual([]);
  expect(f.receipts()).toEqual([]);
}));

test("all bounded target batches must succeed before any dispatch; successful durable matcher work replays", () => fixture(async f => {
  for (let i = 1; i <= 100; i++) {
    const entry = catalog(`anidb:${10_000 + i}`, 10_000 + i * 10);
    f.repo.tv.request(f.users[0]!.id, entry, f.settings.movies.qualityPolicy, [entry.episodes[0]!.key]);
  }
  execution(f);
  const job = f.job();
  let calls = 0, fail = true;
  const matcher: TvFeedMatcher = async input => {
    calls++;
    expect(input.targets.length).toBeLessThanOrEqual(100);
    if (input.targets.length === 1 && fail) throw new MovieIndexerError("retryable semantic/provider failure", 10_000);
    return f.matches(input);
  };
  await expect(matchTvFeed(f.ctx, job, matcher)).rejects.toThrow("retryable");
  expect(f.repo.listJobsByType("tv_acquire")).toEqual([]);
  expect(f.receipts()).toEqual([]);
  fail = false;
  await matchTvFeed(f.ctx, job, matcher);
  expect(calls).toBe(3);
  expect(f.repo.listJobsByType("tv_acquire")).toHaveLength(101);
}));

test("TV feed refresh uses current provider URL but rejects identity drift or cross-domain rows", () => fixture(async f => {
  await matchTvFeed(f.ctx, f.job(), f.matches);
  const original = f.decision().feedOffers[0]!;
  const changedUrl = { ...release("one"), url: "https://private.invalid/renewed?apikey=new-secret" };
  expect(f.ingest(changedUrl)).toEqual([]);
  const fresh = refreshTvFeedOffer(f.repo, original);
  expect(fresh.key).toBe(original.key);
  expect(fresh.url).toBe(changedUrl.url);
  expect(f.decision().feedOffers[0]!.url).toBe(original.url);
  f.db.query("UPDATE release_inbox SET payload_json=? WHERE id=?").run(JSON.stringify({ ...changedUrl, title: "Different release identity" }), original.inboxId!);
  expect(() => refreshTvFeedOffer(f.repo, original)).toThrow("immutable identity");
  f.db.query("UPDATE release_inbox SET payload_json=? WHERE id=?").run(JSON.stringify(release("one", "movie")), original.inboxId!);
  expect(() => refreshTvFeedOffer(f.repo, original)).toThrow("missing from the inbox");
  expect(() => refreshTvFeedOffer(f.repo, { ...original, inboxId: 999999 })).toThrow("missing");
}));

test("invalid/cross-domain inbox batches fail before semantic routing", () => fixture(async f => {
  const movieIds = f.ingest(release("movie", "movie"));
  let calls = 0;
  const matcher: TvFeedMatcher = async () => { calls++; return []; };
  await expect(matchTvFeed(f.ctx, f.job([999999]), matcher)).rejects.toThrow("missing");
  await expect(matchTvFeed(f.ctx, f.job([...f.ids, ...movieIds]), matcher)).rejects.toThrow("Not a TV feed");
  await expect(matchTvFeed(f.ctx, { ...f.job(), payload_json: JSON.stringify({ releaseIds: f.ids, offers: [] }) }, matcher)).rejects.toThrow();
  await expect(matchTvFeed(f.ctx, f.job(Array.from({ length: 101 }, () => f.ids[0]!)), matcher)).rejects.toThrow();
  expect(calls).toBe(0);
}));
