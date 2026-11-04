import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { indexerFailure } from "../../src/media/indexer-errors";
import {
  IndexerGateway,
  type SearchResult,
} from "../../src/media/indexer-gateway";
import { boundedFeedBoundary, feedIdentity, feedReleaseKey } from "../../src/media/release-inbox";
import { movieOfferFromResult, type MovieOffer } from "../../src/movies/offers";
import {
  movieRequestView,
  queueMovieAcquisition,
} from "../../src/movies/service";
import { BooksRepo } from "../../src/repo";
import { runWorker } from "../../src/worker";
import type { WorkerContext } from "../../src/worker/context";
import { processJob } from "../../src/worker/dispatcher";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { syncIndexerFeeds } from "../../src/worker/indexer-feeds";
import {
  createFixtureMovieServices,
  fixtureMovie,
} from "../fixtures/movie-services";

const source = {
  name: "Feed Fixture",
  baseUrl: "https://feed.invalid/1/api",
  apiKey: "private-feed-key",
  supportsPagination: true,
};
const feedId = feedIdentity(source, "movie");
function result(key: string): SearchResult<"movie"> {
  return {
    title: `Fixture ${key}`,
    provider: source.name,
    mediaType: "movie",
    sizeBytes: 100,
    url: `https://feed.invalid/download/${key}`,
    guid: key,
    infoHash: null,
    seeders: 1,
    leechers: 0,
    raw: {},
  };
}
function fromOffer(offer: MovieOffer): SearchResult<"movie"> {
  return {
    ...result(offer.key),
    ...offer,
    guid: offer.key,
    raw: offer.attributes,
  };
}
function xml(results: SearchResult<"movie">[], offset: number, total: number) {
  return `<rss xmlns:newznab="urn:newznab"><channel><newznab:response offset="${offset}" total="${total}"/>${results
    .map(
      (item) =>
        `<item><title>${item.title}</title><guid>${item.guid}</guid><enclosure url="${item.url}" length="${item.sizeBytes}"/>${item.infoHash ? `<newznab:attr name="infohash" value="${item.infoHash}"/>` : ""}</item>`,
    )
    .join("")}</channel></rss>`;
}

async function until(check: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 8000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("Feed acceptance timed out");
    await Bun.sleep(15);
  }
}

async function setup() {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "indexer-feed-")),
  );
  const db = new Database(path.join(root, "app.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  Object.assign(settings.movies, {
    managedRoot: root,
    plexViewPath: "/fixture/movies",
    automaticUpgrades: true,
  });
  settings.prowlarr = {
    url: "https://feed.invalid",
    apiKey: "private-feed-key",
  };
  repo.updateSettings(settings);
  const fixture = createFixtureMovieServices(root);
  let targetedSearches = 0;
  fixture.services.search = async () => {
    targetedSearches++;
    throw new Error("Feed acquisition must not perform targeted searches");
  };
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "feeds",
    username: "feeds",
    isAdmin: false,
  });
  const context: WorkerContext = {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
    indexerFeedSources: async () => [source],
    onLog() {},
    movieFeedMatcher: async ({ targets, offers }) =>
      targets.map((target) => ({
        targetId: target.targetId,
        releaseIndexes: offers.map((_, index) => index),
        reason: "Fixture matches movie identity",
      })),
  };
  let rows: SearchResult<"movie">[] = [];
  const requests: URL[] = [];
  const gateway = (respond?: (url: URL) => Response) =>
    new IndexerGateway(async (url) => {
      requests.push(new URL(url));
      expect(url.searchParams.has("q")).toBe(false);
      expect(url.searchParams.get("cat")).toBe("2000");
      expect(url.searchParams.get("t")).toBe("search");
      if (respond) return respond(url);
      const offset = Number(url.searchParams.get("offset"));
      return new Response(
        xml(rows.slice(offset, offset + 100), offset, rows.length),
      );
    });
  const setRows = (next: SearchResult<"movie">[]) => {
    rows = next;
    context.indexerFeedGateway = gateway();
  };
  setRows([]);
  let runtime:
    | Awaited<ReturnType<typeof createJobExecutionRuntime>>
    | undefined;
  const start = async (
    options: Parameters<typeof createJobExecutionRuntime>[2] = {},
  ) => {
    runtime = await createJobExecutionRuntime(
      context,
      path.join(root, "execution.sqlite"),
      { retryDelayMs: 1, ...options },
    );
    return runtime;
  };
  const stop = async () => {
    await runtime?.stop();
    runtime = undefined;
  };
  const wait = async (predicate: () => boolean | Promise<boolean>) =>
    until(async () => {
      await runtime?.reconcile();
      const failed = repo
        .listExecutionJobs()
        .find((job) => job.status === "failed");
      if (failed)
        throw new Error(`Job ${failed.id} (${failed.type}): ${failed.error}`);
      return predicate();
    });
  const done = (id: number) =>
    wait(() => repo.getJob(id)?.status === "succeeded");
  const sync = async () => {
    const job = repo.createJob({ type: "indexer_feed_sync", maxAttempts: 1 });
    await done(job.id);
    await wait(() =>
      repo.listExecutionJobs().every((job) => job.status === "succeeded"),
    );
    return job;
  };
  const request = (tmdbId = fixtureMovie.tmdbId) =>
    repo.movies.request(
      user.id,
      { ...fixtureMovie, tmdbId },
      settings.movies.qualityPolicy,
    );
  const count = (
    table:
      | "release_inbox"
      | "release_considerations"
      | "jobs"
      | "job_commands"
      | "movie_versions",
  ) =>
    (db.query(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n;
  return {
    root,
    db,
    repo,
    context,
    fixture,
    requests,
    gateway,
    setRows,
    start,
    stop,
    wait,
    done,
    sync,
    request,
    count,
    searches: () => targetedSearches,
    close: async () => {
      await stop();
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    },
  };
}

test("feed sync shares one paginated inbox, stops at the previous head, and replays without new fanout", async () => {
  const s = await setup();
  try {
    s.setRows([result("old-head")]);
    await syncIndexerFeeds(s.context);
    expect(s.count("release_inbox")).toBe(1);
    s.requests.length = 0;
    s.setRows([
      ...Array.from({ length: 101 }, (_, i) => result(`new-${i}`)),
      result("old-head"),
      result("older"),
    ]);
    await syncIndexerFeeds(s.context);
    expect(s.requests.map((url) => url.searchParams.get("offset"))).toEqual([
      "0",
      "100",
    ]);
    expect(s.count("release_inbox")).toBe(103);
    expect(s.repo.releaseInbox.feed(feedId)).toMatchObject({
      head_key: feedReleaseKey(result("new-0")),
      gap_note: null,
    });
    const jobs = s.repo.listJobsByType("movie_feed_match");
    expect(jobs).toHaveLength(3);
    expect(
      jobs
        .map((job) => JSON.parse(job.payload_json!).releaseIds.length)
        .sort((a, b) => a - b),
    ).toEqual([1, 2, 100]);
    const beforeCommands = s.count("job_commands");
    s.requests.length = 0;
    await syncIndexerFeeds({ ...s.context, indexerFeedGateway: s.gateway() });
    expect(s.requests.map((url) => url.searchParams.get("offset"))).toEqual([
      "0",
    ]);
    expect(s.count("release_inbox")).toBe(103);
    expect(s.repo.listJobsByType("movie_feed_match")).toHaveLength(3);
    expect(s.count("job_commands")).toBe(beforeCommands);
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
});

test("feed catch-up follows provider-capped short pages when the reported total has more history", async () => {
  const s = await setup();
  try {
    s.setRows([result("old-head")]);
    await syncIndexerFeeds(s.context);
    const rows = [
      result("new-0"),
      result("new-1"),
      result("new-2"),
      result("old-head"),
    ];
    s.requests.length = 0;
    s.context.indexerFeedGateway = s.gateway((url) => {
      const offset = Number(url.searchParams.get("offset"));
      return new Response(
        xml(rows.slice(offset, offset + 2), offset, rows.length),
      );
    });
    await syncIndexerFeeds(s.context);
    expect(s.requests.map((url) => url.searchParams.get("offset"))).toEqual([
      "0",
      "2",
    ]);
    expect(s.count("release_inbox")).toBe(4);
    expect(s.repo.releaseInbox.feed(feedId)).toMatchObject({
      head_key: feedReleaseKey(result("new-0")),
      gap_note: null,
    });
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
});

for (const deleted of [1, 4]) {
  test(`five retained boundary anchors prevent a false gap when the first ${deleted} releases are deleted`, async () => {
    const s = await setup();
    try {
      const prior = Array.from({ length: 8 }, (_, i) => result(`anchor-${i}`));
      s.setRows(prior);
      await syncIndexerFeeds(s.context);
      expect(JSON.parse(s.repo.releaseInbox.feed(feedId)!.boundary_json!)).toEqual(prior.slice(0, 5).map(feedReleaseKey));
      const fresh = Array.from({ length: 101 }, (_, i) => result(`new-${i}`));
      s.requests.length = 0;
      s.setRows([...fresh, ...prior.slice(deleted)]);
      await syncIndexerFeeds(s.context);
      expect(s.requests.map(url => url.searchParams.get("offset"))).toEqual(["0", "100"]);
      const feed = s.repo.releaseInbox.feed(feedId)!;
      expect(feed.gap_note).toBeNull();
      expect(feed.gap_since).toBeNull();
      expect(feed.head_key).toBe(feedReleaseKey(fresh[0]!));
      expect(JSON.parse(feed.boundary_json!)).toEqual(fresh.slice(0, 5).map(feedReleaseKey));
      expect(s.count("release_inbox")).toBe(109);
      const before = s.count("job_commands");
      await syncIndexerFeeds({ ...s.context, indexerFeedGateway: s.gateway() });
      expect(s.count("job_commands")).toBe(before);
      expect(s.searches()).toBe(0);
    } finally { await s.close(); }
  });
}

test("missing every retained anchor signals bounded history uncertainty and later overlap cannot clear it", async () => {
  const s = await setup();
  try {
    const prior = Array.from({ length: 8 }, (_, i) => result(`old-anchor-${i}`));
    s.setRows(prior);
    await syncIndexerFeeds(s.context);
    const fresh = Array.from({ length: 1100 }, (_, i) => result(`new-history-${i}`));
    // Older captured rows are not substitutes for the explicit five-key boundary.
    s.setRows([...fresh, ...prior.slice(5)]);
    s.requests.length = 0;
    await syncIndexerFeeds(s.context);
    expect(s.requests.map(url => Number(url.searchParams.get("offset")))).toEqual(
      Array.from({ length: 10 }, (_, i) => i * 100),
    );
    const gap = s.repo.releaseInbox.feed(feedId)!;
    expect(gap.gap_since).not.toBeNull();
    expect(gap.gap_note).toContain("previous feed boundary was not found");
    expect(s.count("release_inbox")).toBe(1008);
    expect(JSON.parse(gap.boundary_json!)).toEqual(fresh.slice(0, 5).map(feedReleaseKey));
    s.setRows([result("later-head"), ...fresh.slice(0, 10)]);
    await syncIndexerFeeds(s.context);
    const recovered = s.repo.releaseInbox.feed(feedId)!;
    expect(recovered.head_key).toBe(feedReleaseKey(result("later-head")));
    expect(recovered.gap_since).toBe(gap.gap_since);
    expect(recovered.gap_note).toBe(gap.gap_note);
    expect(s.searches()).toBe(0);
  } finally { await s.close(); }
});

test("legacy feeds reconstruct anchors only from the persisted head's prior capture", async () => {
  const s = await setup();
  try {
    const prior = Array.from({ length: 8 }, (_, i) => result(`legacy-anchor-${i}`));
    s.setRows(prior);
    await syncIndexerFeeds(s.context);
    s.db.query("UPDATE indexer_feeds SET boundary_json=NULL WHERE id=?").run(feedId);
    const legacy = s.repo.releaseInbox.feed(feedId)!;
    expect(s.repo.releaseInbox.boundaryKeys(legacy)).toEqual(prior.slice(0, 5).map(feedReleaseKey));
    s.setRows([result("legacy-new"), ...prior.slice(4)]);
    await syncIndexerFeeds(s.context);
    expect(s.repo.releaseInbox.feed(feedId)!.gap_note).toBeNull();
    expect(JSON.parse(s.repo.releaseInbox.feed(feedId)!.boundary_json!)).toEqual(
      [result("legacy-new"), ...prior.slice(4)].slice(0, 5).map(feedReleaseKey),
    );
  } finally { await s.close(); }
});

test("an uncommitted legacy capture and provider dates cannot prove boundary overlap", async () => {
  const s = await setup();
  try {
    s.setRows([result("legacy-head")]);
    await syncIndexerFeeds(s.context);
    const candidate = { ...result("unproven"), raw: { pubDate: "1999-01-01T00:00:00Z" } };
    // Captured after the recorded success, with no corresponding successful cursor commit.
    s.repo.releaseInbox.ingest(feedId, [candidate], "2099-01-01T00:00:00.000Z");
    s.db.query("UPDATE indexer_feeds SET boundary_json=NULL WHERE id=?").run(feedId);
    const legacy = s.repo.releaseInbox.feed(feedId)!;
    expect(s.repo.releaseInbox.boundaryKeys(legacy)).toEqual([legacy.head_key!]);
    s.context.indexerFeedGateway = { page: async (_source, media) =>
      ({ results: [{ ...candidate, mediaType: media }], count: 1, total: 1, offset: 0 }) };
    await syncIndexerFeeds(s.context);
    expect(s.repo.releaseInbox.feed(feedId)!.gap_note).toContain("previous feed boundary was not found");
    expect(s.searches()).toBe(0);
  } finally { await s.close(); }
});

test("a stale same-head success cannot replace newer anchors or invent a gap", async () => {
  const s = await setup();
  try {
    const prior = Array.from({ length: 5 }, (_, i) => result(`same-head-${i}`));
    s.setRows(prior);
    await syncIndexerFeeds(s.context);
    const before = s.repo.releaseInbox.feed(feedId)!;
    const nextKeys = [before.head_key!, feedReleaseKey(result("another-captured-release"))];
    const later = new Date(Date.parse(before.last_success_at!) + 1000).toISOString();
    s.repo.transaction(() => {
      s.repo.releaseInbox.ingest(feedId, [result("another-captured-release")], later);
      s.repo.releaseInbox.succeed(feedId, before.head_key, null, later, before.head_key, nextKeys, before.boundary_json);
    });
    const committed = s.repo.releaseInbox.feed(feedId)!;
    s.repo.releaseInbox.succeed(feedId, before.head_key, "stale false gap", before.last_success_at!,
      before.head_key, JSON.parse(before.boundary_json!), before.boundary_json);
    expect(s.repo.releaseInbox.feed(feedId)).toEqual(committed);
    expect(JSON.parse(committed.boundary_json!).slice(0, 2)).toEqual(nextKeys);
    expect(committed.gap_note).toBeNull();
  } finally { await s.close(); }
});

test("boundary normalization is bounded, distinct, and preserves a useful older capture on empty success", async () => {
  expect(boundedFeedBoundary([null, "one", "one", "two", "three", "four", "five", "six"]))
    .toEqual(["one", "two", "three", "four", "five"]);
  expect(() => boundedFeedBoundary(["x".repeat(257)])).toThrow("boundary key");
  const s = await setup();
  try {
    s.setRows(Array.from({ length: 5 }, (_, i) => result(`empty-anchor-${i}`)));
    await syncIndexerFeeds(s.context);
    const before = s.repo.releaseInbox.feed(feedId)!;
    s.setRows([]);
    await syncIndexerFeeds(s.context);
    const after = s.repo.releaseInbox.feed(feedId)!;
    expect(after.head_key).toBe(before.head_key);
    expect(after.boundary_json).toBe(before.boundary_json);
    expect(after.gap_note).not.toBeNull();
  } finally { await s.close(); }
});

test("committed anchors survive restart and a lost persist checkpoint without duplicate fanout", async () => {
  const s = await setup();
  let reopened: Database | undefined;
  let failedAfterCommit = false;
  try {
    const prior = Array.from({ length: 5 }, (_, i) => result(`restart-anchor-${i}`));
    s.setRows(prior);
    await syncIndexerFeeds(s.context);
    const fresh = Array.from({ length: 101 }, (_, i) => result(`restart-new-${i}`));
    s.setRows([...fresh, prior[4]!]);
    const runtime = await s.start({ processJob: (ctx, job) => {
      if (job.type !== "indexer_feed_sync") return processJob(ctx, job);
      const execution = ctx.execution!;
      return processJob({ ...ctx, execution: { ...execution,
        run: <T>(name: string, action: () => T | Promise<T>): Promise<T> => execution.run(name, async () => {
          const value = await action();
          if (name.endsWith(":persist") && !failedAfterCommit) {
            failedAfterCommit = true;
            throw new Error("lost feed boundary checkpoint after commit");
          }
          return value;
        }),
      } }, job);
    } });
    const sync = s.repo.createJob({ type: "indexer_feed_sync", maxAttempts: 1 });
    await until(async () => { await runtime.reconcile(); return s.repo.getJob(sync.id)?.status === "failed"; });
    expect(failedAfterCommit).toBe(true);
    const runId = s.repo.getJobExecution(sync.id)!.run_id;
    const committed = s.repo.releaseInbox.feed(feedId)!;
    expect(committed.gap_note).toBeNull();
    expect(JSON.parse(committed.boundary_json!)).toEqual(fresh.slice(0, 5).map(feedReleaseKey));
    const count = s.count("release_inbox");
    const matches = s.repo.listJobsByType("movie_feed_match").length;
    expect(count).toBe(106);
    expect(matches).toBe(3);
    await s.stop();
    reopened = new Database(path.join(s.root, "app.sqlite"));
    const repo = new BooksRepo(reopened);
    expect(repo.releaseInbox.feed(feedId)).toEqual(committed);
    s.context.repo = repo;
    s.context.getSettings = () => repo.getSettings();
    let networkReads = 0;
    s.context.indexerFeedGateway = { page: async () => { networkReads++; throw new Error("Completed feed pages must replay locally"); } };
    await s.start();
    repo.retryJob(sync.id);
    await s.done(sync.id);
    expect(repo.getJobExecution(sync.id)!.run_id).toBe(runId);
    expect(networkReads).toBe(0);
    expect(repo.releaseInbox.feed(feedId)).toEqual(committed);
    expect(s.count("release_inbox")).toBe(count);
    expect(repo.listJobsByType("movie_feed_match")).toHaveLength(matches);
    expect(s.searches()).toBe(0);
  } finally { await s.stop(); reopened?.close(); await s.close(); }
}, 15_000);

for (const failureAt of ["second-fanout", "cursor"]) {
  test(`feed ${failureAt} failure atomically rolls back inbox, fanout/outbox, and cursor before safe replay`, async () => {
    const s = await setup();
    try {
      // Initialize journal-backed jobs, then hold dispatch so pending outbox rows are observable.
      await s.start();
      await s.stop();
      s.setRows([result("old-head")]);
      await syncIndexerFeeds(s.context);
      const before = s.repo.releaseInbox.feed(feedId)!;
      const jobs = s.count("jobs"),
        commands = s.count("job_commands");
      s.setRows([
        ...Array.from({ length: 200 }, (_, i) => result(`new-${i}`)),
        result("old-head"),
      ]);
      s.db.exec(
        failureAt === "second-fanout"
          ? `CREATE TRIGGER reject_feed BEFORE INSERT ON job_commands
           WHEN (SELECT type FROM jobs WHERE id=NEW.job_id)='movie_feed_match'
             AND (SELECT COUNT(*) FROM jobs WHERE type='movie_feed_match')>=3
           BEGIN SELECT RAISE(ABORT,'injected feed commit failure'); END`
          : `CREATE TRIGGER reject_feed BEFORE UPDATE OF head_key ON indexer_feeds
           WHEN NEW.head_key != OLD.head_key
           BEGIN SELECT RAISE(ABORT,'injected feed commit failure'); END`,
      );
      await expect(syncIndexerFeeds(s.context)).rejects.toThrow(
        "injected feed commit failure",
      );
      expect(s.count("release_inbox")).toBe(1);
      expect(s.count("jobs")).toBe(jobs);
      expect(s.count("job_commands")).toBe(commands);
      expect(s.repo.releaseInbox.feed(feedId)).toEqual(before);
      s.db.exec("DROP TRIGGER reject_feed");
      await syncIndexerFeeds(s.context);
      expect(s.count("release_inbox")).toBe(201);
      expect(s.count("jobs")).toBe(jobs + 2);
      expect(s.count("job_commands")).toBe(commands + 2);
      expect(s.repo.releaseInbox.feed(feedId)!.head_key).toBe(
        feedReleaseKey(result("new-0")),
      );
      await syncIndexerFeeds(s.context);
      expect(s.count("jobs")).toBe(jobs + 2);
      expect(s.count("release_inbox")).toBe(201);
    } finally {
      await s.close();
    }
  });
}

test("a failed later page preserves the prior head/batch and durable cooldown survives a fresh gateway and repo", async () => {
  const s = await setup();
  let reopened: Database | undefined;
  try {
    s.setRows(Array.from({ length: 5 }, (_, i) => result(`failure-anchor-${i}`)));
    await syncIndexerFeeds(s.context);
    const before = s.repo.releaseInbox.feed(feedId)!;
    const fresh = Array.from({ length: 100 }, (_, i) => result(`partial-${i}`));
    s.context.indexerFeedGateway = s.gateway((url) =>
      Number(url.searchParams.get("offset")) === 0
        ? new Response(xml(fresh, 0, 500))
        : new Response("private upstream credential details", {
            status: 429,
            headers: { "Retry-After": "300" },
          }),
    );
    await syncIndexerFeeds(s.context);
    const failed = s.repo.releaseInbox.feed(feedId)!;
    expect(failed.head_key).toBe(before.head_key);
    expect(failed.boundary_json).toBe(before.boundary_json);
    expect(failed.last_success_at).toBe(before.last_success_at);
    expect(failed.retry_at && Date.parse(failed.retry_at)).toBeGreaterThan(
      Date.now(),
    );
    expect(failed.last_error).toContain("429");
    expect(JSON.stringify(failed)).not.toContain("private upstream");
    expect(JSON.stringify(failed)).not.toContain("private-feed-key");
    expect(s.count("release_inbox")).toBe(5);
    expect(s.repo.listJobsByType("movie_feed_match")).toHaveLength(1);
    reopened = new Database(path.join(s.root, "app.sqlite"));
    const repo = new BooksRepo(reopened);
    const requests = s.requests.length;
    await syncIndexerFeeds({
      ...s.context,
      repo,
      getSettings: () => repo.getSettings(),
      indexerFeedGateway: s.gateway(() => {
        throw new Error("Persisted cooldown was ignored");
      }),
    });
    expect(s.requests).toHaveLength(requests);
    expect(repo.releaseInbox.feed(feedId)).toEqual(failed);
    repo.releaseInbox.fail(
      feedId,
      indexerFailure(source.name, 503, Date.now() - 1),
      new Date().toISOString(),
    );
    await syncIndexerFeeds({
      ...s.context,
      repo,
      getSettings: () => repo.getSettings(),
      indexerFeedGateway: s.gateway(() => new Response(xml([], 0, 0))),
    });
    expect(repo.releaseInbox.feed(feedId)).toMatchObject({
      head_key: before.head_key,
      retry_at: null,
      last_error: null,
    });
    expect(s.count("release_inbox")).toBe(5);
    expect(s.searches()).toBe(0);
  } finally {
    reopened?.close();
    await s.close();
  }
});

test("one failed provider does not prevent another provider from ingesting or invent per-title fallback searches", async () => {
  const s = await setup();
  try {
    s.setRows([result("prior")]);
    await syncIndexerFeeds(s.context);
    const before = s.repo.releaseInbox.feed(feedId)!;
    const healthy = {
      ...source,
      name: "Healthy",
      baseUrl: "https://feed.invalid/2/api",
    };
    s.context.indexerFeedSources = async () => [source, healthy];
    s.context.indexerFeedGateway = s.gateway((url) =>
      url.pathname === "/1/api"
        ? new Response("private failure", { status: 503 })
        : new Response(xml([result("healthy-new")], 0, 1)),
    );
    await syncIndexerFeeds(s.context);
    expect(s.repo.releaseInbox.feed(feedId)).toMatchObject({
      head_key: before.head_key,
      last_success_at: before.last_success_at,
    });
    expect(s.repo.releaseInbox.feed(feedId)!.last_error).toContain("503");
    expect(
      s.repo.releaseInbox.feed(feedIdentity(healthy, "movie")),
    ).toMatchObject({
      head_key: feedReleaseKey(result("healthy-new")),
      last_error: null,
    });
    expect(s.count("release_inbox")).toBe(2);
    expect(s.repo.listJobsByType("movie_feed_match")).toHaveLength(2);
    expect(s.repo.listJobsByType("movie_acquire")).toHaveLength(0);
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
});

test("bounded history records an observable gap without falling back to a movie search", async () => {
  const s = await setup();
  try {
    s.setRows([result("unreachable-old-head")]);
    await syncIndexerFeeds(s.context);
    const unpaged = { ...source, supportsPagination: false };
    s.context.indexerFeedSources = async () => [unpaged];
    s.setRows(Array.from({ length: 200 }, (_, i) => result(`recent-${i}`)));
    s.requests.length = 0;
    await syncIndexerFeeds(s.context);
    expect(s.requests).toHaveLength(1);
    expect(s.count("release_inbox")).toBe(101);
    const feed = s.repo.releaseInbox.feed(feedId)!;
    expect(feed.head_key).toBe(feedReleaseKey(result("recent-0")));
    expect(feed.gap_since).not.toBeNull();
    expect(feed.gap_note).toContain("previous feed boundary was not found");
    expect(feed.gap_note).toContain("no per-title search was started");
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
});

test("matching dispatch rolls back acquisition/outbox with its consideration and retries saved matching safely", async () => {
  const s = await setup();
  try {
    const request = s.request();
    let matches = 0;
    s.context.movieFeedMatcher = async ({ targets }) => {
      matches++;
      return targets.map((target) => ({
        targetId: target.targetId,
        releaseIndexes: [0],
        reason: "Saved fixture match",
      }));
    };
    const runtime = await s.start();
    s.db
      .exec(`CREATE TRIGGER reject_consideration BEFORE INSERT ON release_considerations
      BEGIN SELECT RAISE(ABORT,'injected consideration failure'); END`);
    s.setRows([fromOffer(s.fixture.addOffer("atomic-recovery", 1280))]);
    const sync = s.repo.createJob({
      type: "indexer_feed_sync",
      maxAttempts: 1,
    });
    await s.done(sync.id);
    await until(async () => {
      await runtime.reconcile();
      return s.repo.listJobsByType("movie_feed_match")[0]?.status === "failed";
    });
    const match = s.repo.listJobsByType("movie_feed_match")[0]!;
    const runId = s.repo.getJobExecution(match.id)!.run_id;
    expect(match.error).toContain("injected consideration failure");
    expect(matches).toBe(1);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(0);
    expect(s.repo.listJobsByType("movie_acquire")).toHaveLength(0);
    expect(s.count("release_considerations")).toBe(0);
    expect(s.fixture.stats.snatches).toBe(0);
    expect(s.repo.listJobCommands()).toHaveLength(0);
    s.db.exec("DROP TRIGGER reject_consideration");
    s.repo.retryJob(match.id);
    await s.done(match.id);
    await s.wait(() =>
      s.repo.listExecutionJobs().every((job) => job.status === "succeeded"),
    );
    expect(s.repo.getJobExecution(match.id)!.run_id).toBe(runId);
    expect(matches).toBe(1);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(1);
    expect(s.count("release_considerations")).toBe(1);
    expect(s.count("release_inbox")).toBe(1);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(movieRequestView(s.repo, request.requestId).available).toBe(true);
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
}, 15_000);

test("a lost dispatch checkpoint replay after successful promotion uses the source receipt instead of queueing another upgrade", async () => {
  const s = await setup();
  let release = () => {};
  try {
    const request = s.request();
    const original = s.fixture.addOffer("replay-original", 1280);
    const initial = queueMovieAcquisition(
      s.repo,
      request.targetId,
      "initial",
      undefined,
      [original],
    ).acquisition;
    let interrupted = false;
    let matches = 0;
    s.context.movieFeedMatcher = async ({ targets }) => {
      matches++;
      return targets.map((target) => ({
        targetId: target.targetId,
        releaseIndexes: [0],
        reason: "Causally stable fixture match",
      }));
    };
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const runtime = await s.start({
      processJob: (ctx, job) => {
        if (job.type !== "movie_feed_match") return processJob(ctx, job);
        const execution = ctx.execution!;
        return processJob(
          {
            ...ctx,
            execution: {
              ...execution,
              run: <T>(
                name: string,
                action: () => T | Promise<T>,
              ): Promise<T> =>
                execution.run(name, async () => {
                  const value = await action();
                  if (name.startsWith("rss:dispatch:") && !interrupted) {
                    // Commit the real domain transaction, but never checkpoint its result.
                    interrupted = true;
                    await gate;
                    throw new Error("lost dispatch checkpoint after commit");
                  }
                  return value;
                }),
            },
          },
          job,
        );
      },
    });
    await s.done(initial.job_id!);
    const originalVersion = s.repo.movies.target(
      request.targetId,
    )!.active_version_id!;
    s.setRows([fromOffer(s.fixture.addOffer("replay-upgrade", 1920))]);
    s.repo.createJob({ type: "indexer_feed_sync", maxAttempts: 1 });
    await s.wait(() => interrupted);
    const match = s.repo.listJobsByType("movie_feed_match")[0]!;
    const sourceRunId = s.repo.getJobExecution(match.id)!.run_id;
    const upgrade = s.repo.movies.acquisitions(request.targetId)[0]!;
    await s.done(upgrade.job_id!);
    const promoted = s.repo.movies.target(request.targetId)!;
    expect(promoted.active_version_id).not.toBe(originalVersion);
    const [releaseId] = JSON.parse(match.payload_json!).releaseIds as number[];
    expect(
      s.repo.releaseInbox.consideredForDispatch(
        releaseId!,
        "movie",
        request.targetId,
        match.id,
      ),
    ).toEqual({ job_id: upgrade.job_id! });
    const dispatchName = `rss:dispatch:${request.targetId}`;
    expect(
      (await runtime.engine.getRunStatus(sourceRunId))!.steps.find(
        (step) => step.name === dispatchName,
      )?.status,
    ).not.toBe("completed");
    release();
    await until(async () => {
      await runtime.reconcile();
      return s.repo.getJob(match.id)?.status === "failed";
    });
    expect(
      (await runtime.engine.getRunStatus(sourceRunId))!.steps.find(
        (step) => step.name === dispatchName,
      )?.status,
    ).toBe("failed");
    s.repo.retryJob(match.id);
    await s.done(match.id);
    await s.wait(() =>
      s.repo.listExecutionJobs().every((job) => job.status === "succeeded"),
    );
    expect(s.repo.getJobExecution(match.id)!.run_id).toBe(sourceRunId);
    expect(matches).toBe(1);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(2);
    expect(s.count("release_considerations")).toBe(1);
    expect(s.count("movie_versions")).toBe(2);
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.repo.movies.target(request.targetId)).toMatchObject({
      active_version_id: promoted.active_version_id,
      revision: promoted.revision,
    });
    expect(s.searches()).toBe(0);
  } finally {
    release();
    await s.close();
  }
}, 15_000);

test("RSS routes an upgrade through Reflow without searches, keeps old content until publication, and deduplicates intake", async () => {
  const s = await setup();
  let release = () => {};
  try {
    const request = s.request();
    const original = s.fixture.addOffer("original", 1280);
    const first = queueMovieAcquisition(
      s.repo,
      request.targetId,
      "initial",
      undefined,
      [original],
    ).acquisition;
    let paused = false;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    await s.start({
      onCheckpoint: async (jobId, name) => {
        const job = s.repo.getJob(jobId)!;
        if (job.type !== "movie_acquire" || name !== "movie:attempt:0:version")
          return;
        const { acquisitionId } = JSON.parse(job.payload_json!);
        if (s.repo.movies.acquisition(acquisitionId)?.reason !== "upgrade")
          return;
        paused = true;
        await gate;
      },
    });
    await s.done(first.job_id!);
    const oldId = s.repo.movies.target(request.targetId)!.active_version_id!;
    const oldFile = path.join(
      s.root,
      "objects",
      s.repo.movies.version(oldId)!.files[0]!.relativePath,
    );
    const bytes = await fs.readFile(oldFile);
    const better = s.fixture.addOffer("better", 1920);
    s.setRows([fromOffer(better)]);
    s.repo.createJob({ type: "indexer_feed_sync", maxAttempts: 1 });
    await s.wait(() => paused);
    expect(movieRequestView(s.repo, request.requestId)).toMatchObject({
      available: true,
      upgrading: true,
    });
    expect(s.repo.movies.target(request.targetId)!.active_version_id).toBe(
      oldId,
    );
    expect(await fs.readFile(oldFile)).toEqual(bytes);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(2);
    const upgrade = s.repo.movies.acquisitions(request.targetId)[0]!;
    expect(upgrade.reason).toBe("upgrade");
    const normalized = movieOfferFromResult({
      ...fromOffer(better),
      provider: source.name,
    });
    expect(JSON.parse(upgrade.decision_json!).feedOffers[0].key).toBe(
      normalized.key,
    );
    expect(normalized.key).not.toBe(better.key);
    release();
    await s.done(upgrade.job_id!);
    const selected = s.repo.movies.target(request.targetId)!.active_version_id!;
    expect(selected).not.toBe(oldId);
    expect(movieRequestView(s.repo, request.requestId)).toMatchObject({
      available: true,
      upgrading: false,
    });
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.count("release_considerations")).toBe(1);
    await s.sync();
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(2);
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.fixture.downloads.size).toBe(2);
    expect(await fs.readFile(oldFile)).toEqual(bytes);
    const maintenance = s.repo.createJob({
      type: "movie_reconcile",
      maxAttempts: 1,
    });
    await s.done(maintenance.id);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(2);
    expect(s.searches()).toBe(0);
  } finally {
    release();
    await s.close();
  }
}, 15_000);

test("a busy acquisition durably waits, then consumes its retained RSS candidate instead of losing it", async () => {
  const s = await setup();
  let release = () => {};
  try {
    const request = s.request();
    const original = s.fixture.addOffer("busy-original", 1280);
    const better = s.fixture.addOffer("busy-better", 1920);
    const acquisition = queueMovieAcquisition(
      s.repo,
      request.targetId,
      "initial",
      undefined,
      [original],
    ).acquisition;
    let paused = false;
    const waits: number[] = [];
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const runtime = await s.start({
      onCheckpoint: async (jobId, name) => {
        if (jobId === acquisition.job_id && name === "movie:attempt:0:poll:1") {
          paused = true;
          await gate;
        }
      },
      processJob: (ctx, job) => {
        if (job.type !== "movie_feed_match") return processJob(ctx, job);
        const execution = ctx.execution!;
        return processJob(
          {
            ...ctx,
            execution: {
              ...execution,
              sleep: (name, ms) => {
                waits.push(ms);
                // Keep the real durable sleep, shortening only this 60s busy-target wait.
                return execution.sleep(name, 100);
              },
            },
          },
          job,
        );
      },
    });
    await s.wait(() => paused);
    s.setRows([fromOffer(better)]);
    s.repo.createJob({ type: "indexer_feed_sync", maxAttempts: 1 });
    await s.wait(async () => {
      const match = s.repo.listJobsByType("movie_feed_match")[0];
      const link = match && s.repo.getJobExecution(match.id);
      return Boolean(
        link &&
          (await runtime.engine.getRunStatus(link.run_id))?.steps.some(
            (step) =>
              step.name === "rss:wait-for-targets:0" &&
              step.status === "sleeping",
          ),
      );
    });
    expect(waits[0]).toBe(60_000);
    expect(s.count("release_considerations")).toBe(0);
    expect(s.repo.movies.acquisitions(request.targetId)).toHaveLength(1);
    release();
    await s.wait(
      () =>
        s.repo.movies.acquisitions(request.targetId).length === 2 &&
        s.repo.listExecutionJobs().every((job) => job.status === "succeeded"),
    );
    expect(s.count("release_considerations")).toBe(1);
    expect(s.repo.movies.acquisitions(request.targetId)[0]!.reason).toBe(
      "upgrade",
    );
    expect(
      s.repo.movies.version(
        s.repo.movies.target(request.targetId)!.active_version_id!,
      )!.facts.width,
    ).toBe(1920);
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.searches()).toBe(0);
  } finally {
    release();
    await s.close();
  }
}, 15_000);

test("automatic upgrades disabled still allows feed recovery for missing movies, not existing versions", async () => {
  const s = await setup();
  try {
    const available = s.request();
    const original = s.fixture.addOffer("auto-off-original", 1280);
    const initial = queueMovieAcquisition(
      s.repo,
      available.targetId,
      "initial",
      undefined,
      [original],
    ).acquisition;
    await s.start();
    await s.done(initial.job_id!);
    const oldId = s.repo.movies.target(available.targetId)!.active_version_id;
    const settings = s.repo.getSettings();
    settings.movies.automaticUpgrades = false;
    s.repo.updateSettings(settings);
    const missing = s.request(102);
    const offeredTargets: number[] = [];
    s.context.movieFeedMatcher = async ({ targets }) => {
      offeredTargets.push(...targets.map((target) => target.targetId));
      return targets.map((target) => ({
        targetId: target.targetId,
        releaseIndexes: [0],
        reason: "Missing movie recovery",
      }));
    };
    s.setRows([fromOffer(s.fixture.addOffer("recovery", 1920))]);
    await s.sync();
    expect(offeredTargets).toEqual([missing.targetId]);
    expect(s.repo.movies.acquisitions(available.targetId)).toHaveLength(1);
    expect(s.repo.movies.target(available.targetId)!.active_version_id).toBe(
      oldId,
    );
    expect(s.repo.movies.acquisitions(missing.targetId)[0]!.reason).toBe(
      "recovery",
    );
    expect(movieRequestView(s.repo, missing.requestId).available).toBe(true);
    expect(s.fixture.stats.snatches).toBe(2);
    expect(s.searches()).toBe(0);
  } finally {
    await s.close();
  }
}, 15_000);

test("the periodic worker schedules one shared feed sync, never a search job per requested movie", async () => {
  const s = await setup();
  let stop = false;
  for (let id = 101; id < 106; id++) s.request(id);
  s.setRows([]);
  const worker = runWorker(
    { ...s.context, shouldStop: () => stop },
    { databasePath: path.join(s.root, "execution.sqlite"), retryDelayMs: 1 },
  );
  try {
    await until(
      () =>
        s.repo.listJobsByType("indexer_feed_sync")[0]?.status === "succeeded" &&
        s.repo.listJobsByType("movie_reconcile")[0]?.status === "succeeded",
    );
    await Bun.sleep(220);
    expect(s.repo.listJobsByType("indexer_feed_sync")).toHaveLength(1);
    expect(s.repo.listJobsByType("movie_acquire")).toHaveLength(0);
    expect(s.repo.listJobsByType("movie_feed_match")).toHaveLength(0);
    expect(s.requests).toHaveLength(1);
    expect(s.searches()).toBe(0);
  } finally {
    stop = true;
    await worker;
    await s.close();
  }
}, 15_000);
