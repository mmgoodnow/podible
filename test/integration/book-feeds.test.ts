import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import type { MediaType } from "../../src/app-types";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import {
  IndexerGateway,
  type SearchResult,
} from "../../src/media/indexer-gateway";
import { feedIdentity } from "../../src/media/release-inbox";
import { inspectMetainfo } from "../../src/media/metainfo";
import type {
  DownloadLocation,
  TorrentClient,
} from "../../src/media/torrent-client";
import {
  syncIndexerFeeds,
  type IndexerFeedContext,
} from "../../src/worker/indexer-feeds";
import {
  bookFeedRevision,
  hasWantedBookMedia,
  matchBookFeed,
  type BookFeedContext,
} from "../../src/worker/book-feeds";
import { processAcquireJob } from "../../src/worker/acquire";
import { processDownloadJob } from "../../src/worker/downloads";
import { processImportJob } from "../../src/worker/imports";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { ProwlarrDiscovery } from "../../src/media/prowlarr";

const source = {
  name: "Book feed",
  baseUrl: "https://rss.invalid/1/api",
  apiKey: "private-key",
};
function release(
  guid: string,
  mediaType: MediaType = "audio",
): SearchResult<MediaType> {
  return {
    title: `Foundation Isaac Asimov ${guid}`,
    mediaType,
    provider: source.name,
    sizeBytes: 100,
    url: "https://rss.invalid/download/" + guid,
    guid,
    infoHash: null,
    seeders: 1,
    leechers: 0,
    raw: {},
  };
}
function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const ctx: BookFeedContext & IndexerFeedContext = {
    repo,
    getSettings: () => repo.getSettings(),
    onLog() {},
    bookFeedMatcher: async ({ targets, releases }) =>
      targets.flatMap((target) => {
        const releaseIndexes = releases.flatMap((release, index) =>
          release.mediaType === target.media ? [index] : [],
        );
        return releaseIndexes.length
          ? [
              {
                bookId: target.bookId,
                media: target.media,
                releaseIndexes,
                reason: "Title and author are plausible",
              },
            ]
          : [];
      }),
  };
  const book = (title = "Foundation") =>
    repo.createBook({ title, author: "Isaac Asimov" });
  const ingest = (items: SearchResult<MediaType>[]) => {
    const feed = feedIdentity(source, items[0].mediaType);
    repo.releaseInbox.ensureFeed(
      feed,
      source.name,
      items[0].mediaType,
      new Date().toISOString(),
    );
    return repo.releaseInbox.ingest(feed, items, new Date().toISOString());
  };
  const job = (releaseIds: number[]) =>
    repo.createJob({ type: "book_feed_match", payload: { releaseIds } });
  const ready = (bookId: number, media: MediaType) => {
    const manifestation = repo.addManifestation({
      bookId,
      kind: media,
      label: "Existing edition",
    });
    const asset = repo.addAsset({
      bookId,
      manifestationId: manifestation.id,
      kind: "single",
      mime: media === "ebook" ? "application/pdf" : "audio/mp4",
      totalSize: 100,
      files: [
        {
          path: `/fixture/${bookId}-${media}`,
          size: 100,
          start: 0,
          end: 100,
          durationMs: 1000,
        },
      ],
    });
    return { manifestation, asset };
  };
  const finish = (id: number) =>
    db.query("UPDATE jobs SET status='succeeded' WHERE id=?").run(id);
  return {
    db,
    repo,
    ctx,
    book,
    ingest,
    job,
    ready,
    finish,
    close: () => db.close(),
  };
}

test("shared intake polls audio and ebook category feeds without title searches, preserving movie-only fixture injection", async () => {
  const f = setup();
  try {
    f.book();
    const calls: URL[] = [];
    f.ctx.indexerFeedMediaSources = async (media) =>
      media === "audio" || media === "ebook" ? [source] : [];
    f.ctx.indexerFeedGateway = new IndexerGateway(async (url) => {
      calls.push(url);
      expect(url.searchParams.has("q")).toBe(false);
      const category = url.searchParams.get("cat");
      return new Response(
        `<rss><channel><item><title>Foundation ${category}</title><guid>${category}</guid><enclosure url="https://rss.invalid/${category}" length="100"/></item></channel></rss>`,
      );
    });
    await syncIndexerFeeds(f.ctx);
    expect(calls.map((url) => url.searchParams.get("cat"))).toEqual([
      "3030",
      "7020",
    ]);
    expect(
      f.repo.releaseInbox
        .feeds()
        .map((feed) => feed.media_type)
        .sort(),
    ).toEqual(["audio", "ebook"]);
    expect(f.repo.listJobsByType("book_feed_match")).toHaveLength(2);
    expect(f.repo.listJobsByType("movie_feed_match")).toHaveLength(0);
    await syncIndexerFeeds(f.ctx);
    expect(f.repo.listJobsByType("book_feed_match")).toHaveLength(2);
    delete f.ctx.indexerFeedMediaSources;
    f.ctx.indexerFeedSources = async () => [];
    await syncIndexerFeeds(f.ctx);
    expect(calls).toHaveLength(2); // Shared gateway coalesces/reuses the unchanged pages.
  } finally {
    f.close();
  }
});

test("category-aware RSS discovery retains audiobook/ebook sources, not search-only or movie-only indexers", async () => {
  const f = setup();
  try {
    f.book();
    const discovery = new ProwlarrDiscovery(
      { url: "https://discovery.invalid", apiKey: "key" },
      async (input) => {
        if (String(input).endsWith("indexerstatus")) return new Response("[]");
        return Response.json([
          {
            id: 1,
            name: "Audio",
            enable: true,
            protocol: "torrent",
            supportsRss: true,
            capabilities: { categories: [{ id: 3030 }] },
          },
          {
            id: 2,
            name: "Ebook",
            enable: true,
            protocol: "torrent",
            supportsRss: true,
            capabilities: { categories: [{ id: 7020 }] },
          },
          {
            id: 3,
            name: "Movie",
            enable: true,
            protocol: "torrent",
            supportsRss: true,
            capabilities: { categories: [{ id: 2000 }] },
          },
          {
            id: 4,
            name: "Search only",
            enable: true,
            protocol: "torrent",
            supportsRss: false,
            capabilities: { categories: [{ id: 3030 }, { id: 7020 }] },
          },
        ]);
      },
    );
    const requested: [string, string | null][] = [];
    f.ctx.indexerFeedMediaSources = async (media) =>
      media === "audio" || media === "ebook" ? discovery.sources(media, "rss") : [];
    f.ctx.indexerFeedGateway = new IndexerGateway(async (url) => {
      requested.push([url.pathname, url.searchParams.get("cat")]);
      return new Response("<rss><channel/></rss>");
    });
    await syncIndexerFeeds(f.ctx);
    expect(requested).toEqual([
      ["/1/api", "3030"],
      ["/2/api", "7020"],
    ]);
  } finally {
    f.close();
  }
});

test("book RSS without a quality reviewer only dispatches missing media and preserves every existing edition/container", async () => {
  const f = setup();
  try {
    const a = f.book(),
      b = f.book("Other book");
    const existing = f.ready(a.id, "audio");
    f.ready(b.id, "ebook");
    const audio = f.ingest([release("audio")]),
      ebook = f.ingest([release("ebook", "ebook")]);
    await matchBookFeed(f.ctx, f.job([...audio, ...ebook]));
    const jobs = f.repo.listJobsByType("acquire");
    expect(jobs).toHaveLength(2);
    expect(
      jobs
        .map((job) => ({
          bookId: job.book_id,
          media: JSON.parse(job.payload_json!).media,
        }))
        .sort((x, y) => x.bookId! - y.bookId!),
    ).toEqual([
      { bookId: a.id, media: ["ebook"] },
      { bookId: b.id, media: ["audio"] },
    ]);
    expect(f.repo.getManifestation(existing.manifestation.id)?.label).toBe(
      "Existing edition",
    );
    expect(f.repo.listAssetsByBook(a.id)).toEqual([{ ...existing.asset, manifestation_preferred_score: 0 }]);
    expect(hasWantedBookMedia(f.repo)).toBe(true);
    f.ready(a.id, "ebook");
    f.ready(b.id, "audio");
    expect(hasWantedBookMedia(f.repo)).toBe(false);
  } finally {
    f.close();
  }
});

test("dispatch and consideration receipts roll back together, and lost checkpoints cannot replay after state/preferences change", async () => {
  const f = setup();
  try {
    const book = f.book();
    const ids = f.ingest([release("one")]);
    const job = f.job(ids);
    f.db.exec(
      "CREATE TRIGGER reject_consideration BEFORE INSERT ON release_considerations BEGIN SELECT RAISE(ABORT,'receipt failure'); END",
    );
    await expect(matchBookFeed(f.ctx, job)).rejects.toThrow("receipt failure");
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
    f.db.exec("DROP TRIGGER reject_consideration");
    await matchBookFeed(f.ctx, job);
    const acquire = f.repo.listJobsByType("acquire")[0];
    expect(JSON.parse(acquire.payload_json!)).toMatchObject({
      feedCandidateIds: ids,
      forceAgent: true,
    });
    f.finish(acquire.id);
    const settings = f.repo.getSettings();
    settings.agents.editionPreference = "A changed preference";
    f.repo.updateSettings(settings);
    await matchBookFeed(f.ctx, job);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(1);
    expect(
      f.db
        .query(
          "SELECT target_kind, source_job_id, decision_note FROM release_considerations",
        )
        .get(),
    ).toEqual({
      target_kind: "book:audio",
      source_job_id: job.id,
      decision_note: "Title and author are plausible",
    });
    f.ready(book.id, "audio");
    await matchBookFeed(f.ctx, job);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(1);
  } finally {
    f.close();
  }
});

test("modern book RSS dispatch atomically rolls back its command outbox with the acquisition and receipt", async () => {
  const f = setup();
  try {
    f.repo.finishJobJournalMigration();
    f.repo.bindJobJournalReader({ read: () => new Map() });
    f.book();
    const ids = f.ingest([release("one")]);
    const matching = f.job(ids);
    const before = f.repo.listJobCommands().length;
    f.db.exec(
      "CREATE TRIGGER reject_book_receipt BEFORE INSERT ON release_considerations BEGIN SELECT RAISE(ABORT,'atomic receipt failure'); END",
    );
    await expect(matchBookFeed(f.ctx, matching)).rejects.toThrow(
      "atomic receipt failure",
    );
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
    expect(f.repo.listJobCommands()).toHaveLength(before);
    f.db.exec("DROP TRIGGER reject_book_receipt");
    await matchBookFeed(f.ctx, matching);
    const acquire = f.repo.listJobsByType("acquire")[0];
    expect(
      f.repo
        .listJobCommands()
        .filter(
          (command) =>
            command.job_id === acquire.id && command.kind === "enqueue",
        ),
    ).toHaveLength(1);
    expect(
      f.db
        .query("SELECT job_id, source_job_id FROM release_considerations")
        .get(),
    ).toEqual({ job_id: acquire.id, source_job_id: matching.id });
  } finally {
    f.close();
  }
});

test("same-revision receipts deduplicate a new matching job, while a new preference revision can reconsider missing media", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("one")]);
    const before = bookFeedRevision(f.ctx, book.id, "audio");
    await matchBookFeed(f.ctx, f.job(ids));
    f.finish(f.repo.listJobsByType("acquire")[0].id);
    await matchBookFeed(f.ctx, f.job(ids));
    expect(f.repo.listJobsByType("acquire")).toHaveLength(1);
    const settings = f.repo.getSettings();
    settings.agents.editionPreference = "Prefer dramatization";
    f.repo.updateSettings(settings);
    expect(bookFeedRevision(f.ctx, book.id, "audio")).not.toBe(before);
    await matchBookFeed(f.ctx, f.job(ids));
    expect(f.repo.listJobsByType("acquire")).toHaveLength(2);
  } finally {
    f.close();
  }
});

test("busy book acquisition waits durably and then dispatches, rather than losing feed offers", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("one")]);
    const busy = f.repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: { bookId: book.id, media: ["audio"] },
    });
    const sleeps: string[] = [];
    f.ctx.execution = {
      key: "test",
      assertActive() {},
      run: async (_name, handler) => handler(),
      sleep: async (name, ms) => {
        expect(ms).toBe(60_000);
        sleeps.push(name);
        f.finish(busy.id);
      },
    };
    await matchBookFeed(f.ctx, f.job(ids));
    expect(sleeps).toEqual(["book-rss:wait-for-targets:0"]);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(2);
  } finally {
    f.close();
  }
});

test("a queued ebook-only acquisition does not block RSS audio for the same book", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("audio")]);
    const ebookOutage = f.repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: {
        bookId: book.id,
        media: ["ebook"],
        failureContext: "Ebook provider unavailable",
      },
    });
    await matchBookFeed(f.ctx, f.job(ids));
    expect(f.repo.getJob(ebookOutage.id)?.status).toBe("queued");
    const acquisitions = f.repo.listJobsByType("acquire");
    expect(acquisitions).toHaveLength(2);
    expect(JSON.parse(acquisitions[0].payload_json!)).toMatchObject({
      media: ["audio"],
      feedCandidateIds: ids,
    });
  } finally {
    f.close();
  }
});

test("an acquisition with absent or empty media still blocks both formats", async () => {
  for (const media of [undefined, []]) {
    const f = setup();
    try {
      const book = f.book(),
        ids = f.ingest([release("audio")]);
      const busy = f.repo.createJob({
        type: "acquire",
        bookId: book.id,
        payload: { bookId: book.id, media },
      });
      let slept = false;
      f.ctx.execution = {
        key: "default-media",
        assertActive() {},
        run: async (_name, handler) => handler(),
        sleep: async () => {
          slept = true;
          f.finish(busy.id);
        },
      };
      await matchBookFeed(f.ctx, f.job(ids));
      expect(slept).toBe(true);
      expect(f.repo.listJobsByType("acquire")).toHaveLength(2);
    } finally {
      f.close();
    }
  }
});

test("an in-flight import waits, then a newly-ready book is skipped without another acquisition", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("one")]);
    const released = f.repo.createRelease({
      bookId: book.id,
      provider: "Old",
      title: "Old edition",
      mediaType: "audio",
      infoHash: "a".repeat(40),
      url: "https://old.invalid/torrent",
    });
    const busy = f.repo.createJob({
      type: "import",
      bookId: book.id,
      releaseId: released.id,
    });
    let sleeps = 0;
    f.ctx.execution = {
      key: "test",
      assertActive() {},
      run: async (_name, handler) => handler(),
      sleep: async () => {
        sleeps++;
        f.finish(busy.id);
        f.ready(book.id, "audio");
      },
    };
    await matchBookFeed(f.ctx, f.job(ids));
    expect(sleeps).toBe(1);
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
  } finally {
    f.close();
  }
});

test("busy book feed matching survives a real Reflow restart and consumes retained offers after cancellation", async () => {
  const f = setup();
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "book-rss-restart-"));
  let runtime:
    | Awaited<ReturnType<typeof createJobExecutionRuntime>>
    | undefined;
  let clock: ReturnType<typeof spyOn<typeof Date, "now">> | undefined;
  const until = async (check: () => boolean | Promise<boolean>) => {
    const deadline = performance.now() + 6000;
    while (!(await check())) {
      if (performance.now() > deadline)
        throw new Error("Book RSS restart timed out");
      await Bun.sleep(15);
    }
  };
  const start = () =>
    createJobExecutionRuntime(f.ctx, path.join(root, "execution.sqlite"), {
      retryDelayMs: 1,
      processJob: async (ctx, job) => {
        if (job.type === "book_feed_match") return matchBookFeed(ctx, job);
        if (
          job.type === "acquire" &&
          !JSON.parse(job.payload_json ?? "{}").feedCandidateIds
        )
          await ctx.execution!.sleep("initial-acquisition-pending", 3_600_000);
        return "done";
      },
    });
  try {
    const book = f.book(),
      ids = f.ingest([release("one")]);
    const busy = f.repo.createJob({
      type: "acquire",
      bookId: book.id,
      payload: { bookId: book.id, media: ["audio"] },
    });
    runtime = await start();
    const job = f.job(ids);
    await runtime.reconcile();
    await until(async () => {
      const execution = f.repo.getJobExecution(job.id);
      return Boolean(
        execution &&
          (await runtime!.engine.getRunStatus(execution.run_id))?.run.status ===
            "sleeping",
      );
    });
    expect(f.repo.listJobsByType("acquire")).toHaveLength(1);
    await runtime.stop();
    runtime = await start();
    f.repo.markJobCancelled(busy.id, "Original acquisition was cancelled");
    await runtime.reconcile();
    await until(() => f.repo.getJob(busy.id)?.status === "cancelled");
    const now = Date.now.bind(Date);
    clock = spyOn(Date, "now").mockImplementation(() => now() + 61_000);
    await until(() => f.repo.getJob(job.id)?.status === "succeeded");
    const acquisitions = f.repo.listJobsByType("acquire");
    expect(acquisitions).toHaveLength(2);
    expect(JSON.parse(acquisitions[0].payload_json!).feedCandidateIds).toEqual(
      ids,
    );
    expect(
      f.db.query("SELECT COUNT(*) AS n FROM release_considerations").get(),
    ).toEqual({ n: 1 });
  } finally {
    await runtime?.stop();
    clock?.mockRestore();
    f.close();
    await fs.rm(root, { recursive: true, force: true });
  }
}, 15_000);

test("book routing rejects invented targets, category crossover and missing evidence before creating work", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("one")]);
    for (const match of [
      {
        bookId: book.id + 5,
        media: "audio" as const,
        releaseIndexes: [0],
        reason: "Unknown book",
      },
      {
        bookId: book.id,
        media: "ebook" as const,
        releaseIndexes: [0],
        reason: "Wrong category",
      },
      {
        bookId: book.id,
        media: "audio" as const,
        releaseIndexes: [8],
        reason: "Unknown release",
      },
      {
        bookId: book.id,
        media: "audio" as const,
        releaseIndexes: [0],
        reason: "",
      },
    ]) {
      f.ctx.bookFeedMatcher = async () => [match];
      await expect(matchBookFeed(f.ctx, f.job(ids))).rejects.toThrow();
    }
    expect(f.repo.listJobsByType("acquire")).toHaveLength(0);
  } finally {
    f.close();
  }
});

function model(reply: () => unknown): ModelProvider {
  return {
    getModel: () => ({
      getResponse: async () => ({
        usage: new Usage(),
        output: [
          {
            type: "message",
            role: "assistant",
            status: "completed",
            content: [{ type: "output_text", text: JSON.stringify(reply()) }],
          },
        ],
      }),
      getStreamedResponse: async function* () {
        throw new Error("unused");
      },
    }),
  };
}

test("later RSS parts reuse earlier incomplete candidates and preserve agent part order in one manifestation", async () => {
  const f = setup();
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "book-rss-parts-")),
  );
  const torrents = [1, 2].map(
    (part) =>
      new Uint8Array(
        bencode.encode({
          info: {
            name: `Foundation-Part-${part}.m4b`,
            length: 100,
            "piece length": 16384,
            pieces: new Uint8Array(20),
          },
        }),
      ),
  );
  const locations = new Map<string, DownloadLocation>();
  const added: string[] = [];
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const part = Number(new URL(request.url).pathname.slice(1));
      return torrents[part - 1]
        ? new Response(torrents[part - 1])
        : new Response("Unexpected search", { status: 500 });
    },
  });
  const client: TorrentClient = {
    id: "rss-parts-fixture",
    capabilities: {
      pausedAdd: true,
      recheck: true,
      derivedLayouts: false,
      metainfoVersions: [1],
    },
    ensureAdded: async ({ metainfo, location }) => {
      const hash = inspectMetainfo(metainfo).infoHash;
      added.push(hash);
      locations.set(hash, location);
      return { clientId: client.id, nativeId: hash };
    },
    inspect: async (ref) => ({
      ref,
      location: locations.get(ref.nativeId)!,
      state: "seeding",
      complete: true,
      bytesDone: 100,
      sizeBytes: 100,
      downloadBytesPerSecond: 0,
      message: null,
      observedAt: new Date().toISOString(),
    }),
    files: async () => [],
    resume: async () => {},
    quiesce: async () => {},
    recheck: async () => {},
    removeRecord: async () => {
      throw new Error("Must preserve existing torrents");
    },
  };
  try {
    const book = f.book();
    f.repo.updateSettings(
      defaultSettings({
        libraryRoot: root,
        agents: { ...defaultSettings().agents, apiKey: "test-key" },
        torznab: [],
      }),
    );
    const offer = (part: number) => ({
      ...release(`Part ${part}`),
      title: `Foundation Isaac Asimov Part ${part} [M4B]`,
      seeders: part,
      url: server.url.origin + "/" + part,
      infoHash: inspectMetainfo(torrents[part - 1]).infoHash,
    });
    const firstIds = f.ingest([offer(1)]);
    f.ctx.bookTorrentClient = client;
    let selections = 0;
    f.ctx.modelProvider = model(() =>
      ++selections === 1
        ? {
            selections: [],
            confidence: 0.95,
            reason: "Part 1 alone is incomplete; wait for the second release",
          }
        : {
            selections: [
              {
                manifestation: {
                  label: "Two-part narration",
                  editionNote: "Ordered complete edition",
                },
                parts: [1, 0],
              },
            ],
            confidence: 0.95,
            reason:
              "The two releases jointly contain the full requested book, ordered part 1 then part 2",
          },
    );
    await matchBookFeed(f.ctx, f.job(firstIds));
    const incomplete = f.repo.listJobsByType("acquire")[0];
    await processAcquireJob(f.ctx, incomplete);
    f.finish(incomplete.id);
    expect(added).toHaveLength(0);
    const secondIds = f.ingest([offer(2)]);
    await matchBookFeed(f.ctx, f.job(secondIds));
    const acquisition = f.repo.listJobsByType("acquire")[0];
    expect(
      JSON.parse(acquisition.payload_json!).feedCandidateIds.sort(),
    ).toEqual([...firstIds, ...secondIds].sort());
    await processAcquireJob(f.ctx, acquisition);
    expect(selections).toBe(2);
    expect(added).toEqual(
      torrents.map((torrent) => inspectMetainfo(torrent).infoHash),
    );
    const manifestations = f.repo.listManifestationsByBook(book.id);
    expect(manifestations).toHaveLength(1);
    expect(manifestations[0].label).toBe("Two-part narration");
    const downloads = f.repo
      .listJobsByType("download")
      .reverse()
      .map((job) => JSON.parse(job.payload_json!));
    expect(downloads.map((payload) => payload.manifestationId)).toEqual([
      manifestations[0].id,
      manifestations[0].id,
    ]);
    expect(downloads.map((payload) => payload.sequenceInManifestation)).toEqual(
      [0, 1],
    );
    expect(
      f.db
        .query(
          "SELECT release_id, job_id FROM release_considerations ORDER BY release_id",
        )
        .all(),
    ).toEqual([
      { release_id: firstIds[0], job_id: incomplete.id },
      { release_id: secondIds[0], job_id: acquisition.id },
    ]);
    expect(
      f.repo.releaseInbox.consideredEntries("book:audio", book.id),
    ).toHaveLength(2);
    expect(
      f.repo.releaseInbox.consideredEntries("book:ebook", book.id),
    ).toHaveLength(0);
    expect(
      f.repo.releaseInbox.consideredEntries("book:audio", book.id + 1),
    ).toHaveLength(0);
  } finally {
    await server.stop(true);
    f.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("multipart RSS replay completes remaining parts after its first part has already imported", async () => {
  const f = setup();
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "book-rss-part-replay-")),
  );
  const torrents = [1, 2].map(
    (part) =>
      new Uint8Array(
        bencode.encode({
          info: {
            name: `Foundation-Part-${part}.m4b`,
            length: 100,
            "piece length": 16384,
            pieces: new Uint8Array(20),
          },
        }),
      ),
  );
  const hashes = torrents.map((torrent) => inspectMetainfo(torrent).infoHash);
  const locations = new Map<string, DownloadLocation>();
  const attempts: string[] = [];
  let failSecond = true,
    selections = 0;
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const part = Number(new URL(request.url).pathname.slice(1));
      return torrents[part - 1]
        ? new Response(torrents[part - 1])
        : new Response("Unexpected search", { status: 500 });
    },
  });
  const client: TorrentClient = {
    id: "rss-replay-fixture",
    capabilities: {
      pausedAdd: true,
      recheck: true,
      derivedLayouts: false,
      metainfoVersions: [1],
    },
    ensureAdded: async ({ metainfo, location }) => {
      const hash = inspectMetainfo(metainfo).infoHash;
      attempts.push(hash);
      if (hash === hashes[1] && failSecond)
        throw new Error("Second-part transport interrupted");
      locations.set(hash, location);
      return { clientId: client.id, nativeId: hash };
    },
    inspect: async (ref) => ({
      ref,
      location: locations.get(ref.nativeId)!,
      state: "seeding",
      complete: true,
      bytesDone: 100,
      sizeBytes: 100,
      downloadBytesPerSecond: 0,
      message: null,
      observedAt: new Date().toISOString(),
    }),
    files: async () => [],
    resume: async () => {},
    quiesce: async () => {},
    recheck: async () => {},
    removeRecord: async () => {
      throw new Error("Must not remove an existing torrent");
    },
  };
  try {
    const book = f.book();
    f.repo.updateSettings(
      defaultSettings({
        libraryRoot: root,
        agents: { ...defaultSettings().agents, apiKey: "test-key" },
        torznab: [],
      }),
    );
    const failed = f.repo.createRelease({
      bookId: book.id,
      provider: source.name,
      providerGuid: "Part 1",
      title: "Previously failed content at a reused download URL",
      mediaType: "audio",
      infoHash: "a".repeat(40),
      url: server.url.origin + "/1",
    });
    f.repo.setReleaseStatus(
      failed.id,
      "failed",
      "Wrong earlier torrent contents",
    );
    const ids = f.ingest(
      [1, 2].map((part) => ({
        ...release(`Part ${part}`),
        title: `Foundation Isaac Asimov Part ${part} [M4B]`,
        url: server.url.origin + "/" + part,
        infoHash: hashes[part - 1],
      })),
    );
    f.ctx.bookTorrentClient = client;
    f.ctx.modelProvider = model(() => {
      selections++;
      return {
        selections: [
          {
            manifestation: { label: "Complete edition", editionNote: null },
            parts: [0, 1],
          },
        ],
        confidence: 0.95,
        reason: "Both ordered parts form the complete book",
      };
    });
    await matchBookFeed(f.ctx, f.job(ids));
    const acquire = f.repo.listJobsByType("acquire")[0];
    const checkpoints = new Map<string, unknown>();
    f.ctx.execution = {
      key: "rss-part-replay",
      assertActive() {},
      run: async <T>(
        name: string,
        handler: () => T | Promise<T>,
      ): Promise<T> => {
        if (checkpoints.has(name))
          return structuredClone(checkpoints.get(name)) as T;
        const output = await handler();
        checkpoints.set(name, structuredClone(output));
        return output;
      },
      sleep: async () => {
        throw new Error("Unexpected wait");
      },
    };
    await expect(processAcquireJob(f.ctx, acquire)).rejects.toThrow(
      "Second-part transport interrupted",
    );
    const firstDownload = f.repo.listJobsByType("download")[0];
    const firstPayload = JSON.parse(firstDownload.payload_json!);
    f.repo.addAsset({
      bookId: book.id,
      sourceReleaseId: firstDownload.release_id!,
      manifestationId: firstPayload.manifestationId,
      kind: "single",
      mime: "audio/mp4",
      totalSize: 100,
      files: [
        {
          path: path.join(root, "part-1.m4b"),
          size: 100,
          start: 0,
          end: 100,
          durationMs: 1000,
        },
      ],
    });
    f.repo.setReleaseStatus(firstDownload.release_id!, "imported");
    expect(f.repo.getBook(book.id)?.audioStatus).toBe("imported");
    failSecond = false;
    await processAcquireJob(f.ctx, acquire);
    expect(attempts).toEqual([hashes[0], hashes[1], hashes[1]]);
    expect(selections).toBe(1);
    expect(f.repo.listManifestationsByBook(book.id)).toHaveLength(1);
    const downloads = f.repo.listJobsByType("download").reverse();
    expect(downloads).toHaveLength(2);
    expect(
      downloads.map(
        (job) => JSON.parse(job.payload_json!).sequenceInManifestation,
      ),
    ).toEqual([0, 1]);
    expect(
      f.repo
        .listReleasesByBook(book.id)
        .map((release) => release.info_hash)
        .sort(),
    ).toEqual([...hashes, failed.info_hash].sort());
    expect(f.repo.getRelease(failed.id)?.status).toBe("failed");
    expect(f.repo.findReleaseByInfoHash(hashes[0])?.url).toBe(failed.url);
    expect(f.repo.findReleaseByInfoHash(hashes[0])?.provider_guid).toBe(
      failed.provider_guid,
    );
  } finally {
    await server.stop(true);
    f.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("feed evidence rejects unrelated releases without a per-title search, and a ready book skips stale inbox work", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("wrong")]);
    const settings = defaultSettings({
      agents: { ...defaultSettings().agents, apiKey: "test-key" },
      torznab: [],
    });
    f.repo.updateSettings(settings);
    f.ctx.modelProvider = model(() => ({
      selections: [],
      confidence: 0.9,
      reason: "This is another book, not Foundation",
    }));
    await matchBookFeed(f.ctx, f.job(ids));
    const acquire = f.repo.listJobsByType("acquire")[0];
    await processAcquireJob(f.ctx, acquire);
    expect(f.repo.listReleasesByBook(book.id)).toHaveLength(0);
    expect(f.repo.listManifestationsByBook(book.id)).toHaveLength(0);
    f.ready(book.id, "audio");
    f.ctx.modelProvider = model(() => {
      throw new Error("Ready media must not invoke model");
    });
    await processAcquireJob(f.ctx, acquire);
    expect(f.repo.listManifestationsByBook(book.id)).toHaveLength(1);
  } finally {
    f.close();
  }
});

test("feed acquisition refuses missing, wrong-media and client-shaped candidate references", async () => {
  const f = setup();
  try {
    const book = f.book(),
      ids = f.ingest([release("one", "ebook")]);
    for (const payload of [
      { bookId: book.id, media: ["audio"], feedCandidateIds: ids },
      { bookId: book.id, media: ["audio"], feedCandidateIds: [9999] },
      {
        bookId: book.id,
        media: ["audio"],
        feedCandidateIds: ["https://arbitrary.invalid/torrent"],
      },
      { bookId: book.id, media: ["audio", "ebook"], feedCandidateIds: ids },
    ])
      await expect(
        processAcquireJob(
          f.ctx,
          f.repo.createJob({ type: "acquire", bookId: book.id, payload }),
        ),
      ).rejects.toThrow();
    expect(f.repo.listReleasesByBook(book.id)).toHaveLength(0);
  } finally {
    f.close();
  }
});

test("RSS ebook proceeds through selection, private-client snatch and import with fresh inbox URL and zero searches", async () => {
  const f = setup();
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "book-rss-import-")),
  );
  const bytes = Buffer.from("%PDF-1.4\nsynthetic ebook fixture\n%%EOF");
  const metainfo = new Uint8Array(
    bencode.encode({
      info: {
        name: "Foundation.pdf",
        length: bytes.length,
        "piece length": 16384,
        pieces: new Uint8Array(20),
      },
    }),
  );
  const hash = inspectMetainfo(metainfo).infoHash;
  let searches = 0,
    expiredFetches = 0,
    freshFetches = 0;
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const pathname = new URL(request.url).pathname;
      if (pathname === "/fresh") {
        freshFetches++;
        return new Response(metainfo);
      }
      if (pathname === "/expired") {
        expiredFetches++;
        return new Response("Expired", { status: 403 });
      }
      searches++;
      return new Response("RSS must not search", { status: 500 });
    },
  });
  const downloads = new Map<string, DownloadLocation>();
  const added: string[] = [];
  const client: TorrentClient = {
    id: "rss-private-fixture",
    capabilities: {
      pausedAdd: true,
      recheck: true,
      derivedLayouts: false,
      metainfoVersions: [1],
    },
    ensureAdded: async ({ metainfo: torrent, location }) => {
      const metadata = inspectMetainfo(torrent);
      added.push(metadata.infoHash);
      downloads.set(metadata.infoHash, location);
      const directory = path.join(
        root,
        ".podible",
        "downloads",
        location.relativePath,
      );
      await fs.mkdir(directory, { recursive: true });
      await fs.writeFile(path.join(directory, "Foundation.pdf"), bytes);
      return { clientId: client.id, nativeId: metadata.infoHash };
    },
    inspect: async (ref) => ({
      ref,
      location: downloads.get(ref.nativeId)!,
      state: "seeding",
      complete: true,
      sizeBytes: bytes.length,
      bytesDone: bytes.length,
      downloadBytesPerSecond: 0,
      message: null,
      observedAt: new Date().toISOString(),
    }),
    files: async (ref) => [
      {
        index: 0,
        path: path.join(
          root,
          ".podible",
          "downloads",
          ref.nativeId,
          "Foundation.pdf",
        ),
      },
    ],
    resume: async () => {},
    quiesce: async () => {},
    recheck: async () => {},
    removeRecord: async () => {
      throw new Error("Existing torrents must never be removed");
    },
  };
  try {
    const book = f.book();
    const settings = defaultSettings({
      libraryRoot: root,
      agents: { ...defaultSettings().agents, apiKey: "test-key" },
      torznab: [
        { name: "Must not search", baseUrl: server.url.origin, apiKey: "test" },
      ],
    });
    f.repo.updateSettings(settings);
    const offer = {
      ...release("one", "ebook"),
      title: "Foundation Isaac Asimov [PDF]",
      url: server.url.origin + "/expired",
      infoHash: hash,
    };
    const ids = f.ingest([offer]);
    f.ctx.bookTorrentClient = client;
    let selected = false;
    f.ctx.modelProvider = model(() => {
      if (!selected) {
        selected = true;
        // URL rotation during selection must be observed before the snatch.
        expect(
          f.ingest([{ ...offer, url: server.url.origin + "/fresh" }]),
        ).toEqual([]);
        return {
          selections: [
            {
              manifestation: { label: "PDF edition", editionNote: "Full text" },
              parts: [0],
            },
          ],
          confidence: 0.95,
          reason: "Correct title and author, PDF content",
        };
      }
      return {
        selectedIndices: [0],
        confidence: 0.95,
        reason: "The sole PDF is the requested book",
      };
    });
    await matchBookFeed(f.ctx, f.job(ids));
    const acquire = f.repo.listJobsByType("acquire")[0];
    await processAcquireJob(f.ctx, acquire);
    f.finish(acquire.id);
    const download = f.repo.listJobsByType("download")[0];
    await processDownloadJob(f.ctx, download);
    f.finish(download.id);
    const importing = f.repo.listJobsByType("import")[0];
    expect(importing).toBeDefined();
    await processImportJob(f.ctx, importing);
    f.finish(importing.id);
    expect(f.repo.getBook(book.id)?.ebookStatus).toBe("imported");
    const asset = f.repo.listAssetsByBook(book.id)[0];
    expect(await fs.readFile(f.repo.getAssetFiles(asset.id)[0].path)).toEqual(
      bytes,
    );
    expect(
      f.repo.listManifestationsByBook(book.id)[0].selection_note,
    ).toContain("Title and author are plausible");
    expect(
      f.repo.listManifestationsByBook(book.id)[0].selection_note,
    ).toContain("Correct title and author");
    expect(added).toEqual([hash]);
    expect({ searches, expiredFetches, freshFetches }).toEqual({
      searches: 0,
      expiredFetches: 0,
      freshFetches: 1,
    });
    await processAcquireJob(f.ctx, acquire);
    expect(added).toHaveLength(1);
  } finally {
    await server.stop(true);
    f.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
