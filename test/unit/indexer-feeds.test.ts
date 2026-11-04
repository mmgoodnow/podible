import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { runMigrations } from "../../src/db";
import { IndexerError, indexerFailure } from "../../src/media/indexer-errors";
import { feedIdentity } from "../../src/media/release-inbox";
import { BooksRepo } from "../../src/repo";
import type { WorkerContext } from "../../src/worker/context";
import { syncIndexerFeeds } from "../../src/worker/indexer-feeds";

const source = { name: "Fixture", baseUrl: "https://fixture.invalid/api", apiKey: "secret-key" };
const emptyPage = { results: [], count: 0, total: 0, offset: 0 };

function setup() {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const ctx: WorkerContext = {
    repo, getSettings: () => repo.getSettings(),
    indexerFeedSources: async () => [source],
    indexerFeedGateway: { page: async () => emptyPage },
  };
  return { db, repo, ctx };
}

function execution(): NonNullable<WorkerContext["execution"]> {
  return {
    key: "fixture", assertActive() {},
    run: async (_name, handler) => handler(),
    sleep: async () => { throw new Error("RSS must not sleep the whole job"); },
  };
}

test("expected failure resolves inside the checkpoint without serializing the Error or cause", async () => {
  const { db, repo, ctx } = setup();
  try {
    const failure = indexerFailure(source.name, 429, Date.now() + 60_000);
    const metadata = { ...failure, debugBody: "hidden sensitive diagnostic body" };
    const error = Object.assign(new IndexerError(metadata), {
      cause: new Error("https://secret.invalid/?apikey=secret-key"),
      body: "private upstream response", message: "hidden sensitive error body",
    });
    ctx.indexerFeedGateway = { page: async () => { throw error; } };
    const outputs: unknown[] = [];
    ctx.execution = { ...execution(), run: async (name, handler) => {
      const output = await handler();
      if (name.includes(":page:")) outputs.push(JSON.parse(JSON.stringify(output)));
      return output;
    } };
    expect(await syncIndexerFeeds(ctx)).toBe("done");
    expect(outputs).toEqual([{ kind: "failure", failure }]);
    const persisted = JSON.stringify({ outputs, feeds: repo.releaseInbox.feeds() });
    for (const secret of ["secret-key", "secret.invalid", "private upstream", "hidden sensitive"]) {
      expect(persisted).not.toContain(secret);
    }
  } finally { db.close(); }
});

for (const error of [new TypeError("invalid page implementation"), new LeaseExpiredError("fixture"), new RunCancelledError("fixture")]) {
  test(`RSS does not downgrade ${error.name} from the gateway into provider health`, async () => {
    const { db, repo, ctx } = setup();
    try {
      ctx.execution = execution();
      ctx.indexerFeedGateway = { page: async () => { throw error; } };
      await expect(syncIndexerFeeds(ctx)).rejects.toBe(error);
      expect(repo.releaseInbox.feed(feedIdentity(source, "movie"))).toMatchObject({
        last_error: null, retry_at: null, last_success_at: null,
      });
    } finally { db.close(); }
  });
}

for (const response of ["success", "failure"] as const) {
  for (const change of ["cancellation", "ownership", "configuration"] as const) {
    test(`${change} during a page ${response} prevents health/checkpoint writes`, async () => {
      const { db, repo, ctx } = setup();
      try {
        let changed = false;
        const control = change === "cancellation" ? new RunCancelledError("fixture") : new LeaseExpiredError("fixture");
        ctx.execution = { ...execution(), assertActive() {
          if (changed && change !== "configuration") throw control;
        } };
        ctx.indexerFeedGateway = { page: async () => {
          changed = true;
          if (change === "configuration") {
            const settings = repo.getSettings();
            settings.torznab = [source];
            repo.updateSettings(settings);
          }
          if (response === "failure") throw new IndexerError(indexerFailure(source.name, 429, Date.now() + 60_000));
          return emptyPage;
        } };
        const pending = syncIndexerFeeds(ctx);
        if (change === "configuration") await expect(pending).rejects.toThrow("Indexer configuration changed");
        else await expect(pending).rejects.toBe(control);
        expect(repo.releaseInbox.feed(feedIdentity(source, "movie"))).toMatchObject({
          last_error: null, retry_at: null, last_success_at: null,
        });
      } finally { db.close(); }
    });
  }
}

test("bare successful page checkpoints remain replayable without calling the gateway", async () => {
  const { db, repo, ctx } = setup();
  try {
    ctx.indexerFeedGateway = { page: async () => { throw new Error("Legacy page should replay"); } };
    ctx.execution = { ...execution(), run: async <T>(name: string, handler: () => T | Promise<T>): Promise<T> =>
      name.endsWith(":page:0") ? emptyPage as T : handler(),
    };
    expect(await syncIndexerFeeds(ctx)).toBe("done");
    expect(repo.releaseInbox.feed(feedIdentity(source, "movie"))!.last_success_at).not.toBeNull();
  } finally { db.close(); }
});

test("typed TV injection owns empty media too; legacy injection remains movie-only", async () => {
  const { db, repo, ctx } = setup();
  try {
    const media: string[] = [];
    const pages: string[] = [];
    ctx.indexerFeedMediaSources = async (kind) => {
      media.push(kind);
      return kind === "tv" ? [source] : [];
    };
    ctx.indexerFeedSources = async () => { throw new Error("Typed injection must own discovery"); };
    ctx.indexerFeedGateway = { page: async (_source, kind) => { pages.push(kind); return emptyPage; } };
    await syncIndexerFeeds(ctx);
    expect(media).toEqual(["movie", "tv", "audio", "ebook"]);
    expect(pages).toEqual(["tv"]);
    expect(repo.releaseInbox.feeds().map((feed) => feed.media_type)).toEqual(["tv"]);
    delete ctx.indexerFeedMediaSources;
    ctx.indexerFeedSources = async () => [source];
    await syncIndexerFeeds(ctx);
    expect(pages).toEqual(["tv", "movie"]);
  } finally { db.close(); }
});
