import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { indexerFailure } from "../../src/media/indexer-errors";
import type { SearchResult } from "../../src/media/indexer-gateway";
import { feedIdentity, feedReleaseKey } from "../../src/media/release-inbox";
import { BooksRepo } from "../../src/repo";

const source = {
  name: "Fixture",
  baseUrl: "https://feed.invalid/1/api",
  apiKey: "old-key",
};
const now = "2026-11-03T12:00:00.000Z";
const release: SearchResult<"movie"> = {
  title: "Fixture Film 1080p",
  provider: source.name,
  mediaType: "movie",
  sizeBytes: 100,
  url: "https://feed.invalid/download/one",
  guid: "release-one",
  infoHash: "a".repeat(40),
  seeders: 3,
  leechers: 1,
  raw: {
    attr: [{ name: "tmdbid", value: "101" }],
    category: "2000",
    pubDate: now,
    description: "untrusted HTML",
  },
};

function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  return { db, repo: new BooksRepo(db) };
}

test("feed and release identities survive credential/health changes without conflating providers or media", () => {
  expect(feedIdentity(source, "movie")).toBe(
    feedIdentity({ ...source, apiKey: "new-key", name: "Renamed" }, "movie"),
  );
  expect(
    feedIdentity(
      { ...source, baseUrl: source.baseUrl + "?apikey=old" },
      "movie",
    ),
  ).toBe(
    feedIdentity(
      { ...source, baseUrl: source.baseUrl + "?apikey=new" },
      "movie",
    ),
  );
  expect(feedIdentity(source, "movie")).not.toBe(feedIdentity(source, "audio"));
  expect(feedIdentity(source, "movie")).not.toBe(
    feedIdentity({ ...source, baseUrl: "https://feed.invalid/2/api" }, "movie"),
  );
  expect(feedReleaseKey(release)).toBe(
    feedReleaseKey({
      ...release,
      infoHash: "b".repeat(40),
      url: "https://elsewhere.invalid/changed",
    }),
  );
  expect(feedReleaseKey({ ...release, guid: null })).toBe(
    feedReleaseKey({
      ...release,
      guid: null,
      url: "https://elsewhere.invalid/changed",
    }),
  );
  expect(feedReleaseKey({ ...release, guid: null, infoHash: null })).not.toBe(
    feedReleaseKey({
      ...release,
      guid: null,
      infoHash: null,
      url: "https://elsewhere.invalid/changed",
    }),
  );
});

test("migrated inbox deduplicates replay but retains changed content and provider-local identity", () => {
  const { db, repo } = setup();
  try {
    const inbox = repo.releaseInbox;
    inbox.ensureFeed("one", "Fixture", "movie", now);
    inbox.ensureFeed("two", "Other", "movie", now);
    const [first] = inbox.ingest("one", [release, release], now);
    expect(first).toBeGreaterThan(0);
    expect(
      inbox.ingest(
        "one",
        [
          {
            ...release,
            seeders: 100,
            url: "https://feed.invalid/rotated-token",
          },
        ],
        now,
      ),
    ).toEqual([]);
    const changed = inbox.ingest("one", [{ ...release, sizeBytes: 200 }], now);
    const other = inbox.ingest("two", [release], now);
    const entries = inbox.entries([other[0]!, changed[0]!, first!]);
    expect(entries).toHaveLength(3);
    expect(entries.map((entry) => entry.id)).toEqual([
      first!,
      changed[0]!,
      other[0]!,
    ]);
    expect(entries[0]!.result.raw).toEqual({
      attr: release.raw.attr,
      category: "2000",
      pubDate: now,
    });
    expect(entries[0]!.result.sizeBytes).toBe(100);
    expect(entries[1]!.result.sizeBytes).toBe(200);
    expect(entries[0]!.first_seen_at).toBe(now);
    expect(inbox.entries([])).toEqual([]);
    expect(() => inbox.entries([0])).toThrow("Invalid inbox entry IDs");
    expect(() =>
      inbox.entries(Array.from({ length: 1001 }, (_, i) => i + 1)),
    ).toThrow();
    expect(() => inbox.ingest("missing-feed", [release], now)).toThrow();
  } finally {
    db.close();
  }
});

test("feed failures preserve the successful boundary and gap; empty success clears cooldown without wiping the head", () => {
  const { db, repo } = setup();
  try {
    const inbox = repo.releaseInbox;
    inbox.ensureFeed("one", "Fixture", "movie", now);
    inbox.succeed(
      "one",
      "old-head",
      "History did not cover the old boundary",
      now,
    );
    const failureTime = "2026-11-03T12:01:00.000Z";
    const retry = Date.parse(failureTime) + 60_000;
    inbox.fail("one", indexerFailure("Fixture", 429, retry), failureTime);
    expect(inbox.feed("one")).toMatchObject({
      head_key: "old-head",
      last_success_at: now,
      last_polled_at: failureTime,
      retry_at: new Date(retry).toISOString(),
      gap_since: now,
    });
    inbox.ensureFeed("one", "Renamed", "movie", failureTime);
    expect(inbox.feed("one")!.head_key).toBe("old-head");
    inbox.succeed("one", null, null, failureTime);
    expect(inbox.feed("one")).toMatchObject({
      provider: "Renamed",
      head_key: "old-head",
      last_success_at: failureTime,
      last_error: null,
      retry_at: null,
      gap_since: now,
      gap_note: "History did not cover the old boundary",
    });
  } finally {
    db.close();
  }
});

test("considerations are revision-scoped, transactional, and respect execution fencing", () => {
  const { db, repo } = setup();
  try {
    const inbox = repo.releaseInbox;
    inbox.ensureFeed("one", "Fixture", "movie", now);
    const [id] = inbox.ingest("one", [release], now);
    const job = repo.createJob({
      type: "movie_feed_match",
      payload: { releaseIds: [id] },
    });
    inbox.consider(id!, "movie", 7, "policy:0", job.id, "Correct movie");
    expect(inbox.considered(id!, "movie", 7, "policy:0")).toEqual({
      job_id: job.id,
    });
    expect(inbox.consideredForDispatch(id!, "movie", 7, job.id)).toEqual({
      job_id: job.id,
    });
    expect(inbox.considered(id!, "movie", 7, "policy:1")).toBeNull();
    expect(() =>
      inbox.consider(id!, "movie", 7, "policy:0", job.id, "duplicate"),
    ).toThrow();
    expect(() =>
      inbox.consider(
        id!,
        "movie",
        7,
        "policy:1",
        job.id,
        "same causal dispatch",
      ),
    ).toThrow();
    const nextSource = repo.createJob({
      type: "movie_feed_match",
      payload: { releaseIds: [id] },
    });
    expect(() =>
      repo.transaction(() => {
        inbox.consider(
          id!,
          "movie",
          7,
          "policy:1",
          job.id,
          "rolled back",
          nextSource.id,
        );
        inbox.succeed("one", "uncommitted-head", null, now);
        throw new Error("abort dispatch");
      }),
    ).toThrow("abort dispatch");
    expect(inbox.considered(id!, "movie", 7, "policy:1")).toBeNull();
    expect(
      inbox.consideredForDispatch(id!, "movie", 7, nextSource.id),
    ).toBeNull();
    expect(inbox.feed("one")!.head_key).toBeNull();
    const fenced = repo.forExecution(() => {
      throw new Error("stale execution");
    });
    expect(() => fenced.releaseInbox.ingest("one", [release], now)).toThrow(
      "stale execution",
    );
    expect(() =>
      fenced.releaseInbox.fail(
        "one",
        indexerFailure("Fixture", 503, null),
        now,
      ),
    ).toThrow("stale execution");
    expect(fenced.releaseInbox.entries([id!])).toHaveLength(1);
  } finally {
    db.close();
  }
});

test("stale poll receipts cannot regress a newer cursor or overwrite its recovered health", () => {
  const { db, repo } = setup();
  try {
    const inbox = repo.releaseInbox;
    inbox.ensureFeed("one", "Fixture", "movie", now);
    inbox.succeed("one", "first", null, now, null);
    const later = "2026-11-03T12:01:00.000Z";
    inbox.succeed("one", "latest", null, later, "first");
    inbox.succeed("one", "stale", "not a real gap", now, "first");
    inbox.fail(
      "one",
      indexerFailure("Fixture", 503, Date.parse(later) + 60_000),
      now,
      now,
    );
    expect(inbox.feed("one")).toMatchObject({
      head_key: "latest",
      last_success_at: later,
      last_error: null,
      retry_at: null,
      gap_note: null,
    });
  } finally {
    db.close();
  }
});
