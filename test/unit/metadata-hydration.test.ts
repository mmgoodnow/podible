import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/db";
import {
  CURRENT_OPENLIBRARY_METADATA_VERSION,
  openLibraryMetadataStatus,
} from "../../src/library/hydration";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { handleJobFailure } from "../../src/worker/failure";
import {
  processMetadataHydrationJob,
  queueStaleMetadataHydration,
} from "../../src/worker/metadata-hydration";

describe("Open Library metadata hydration", () => {
  let db: Database;
  let repo: BooksRepo;
  let originalFetch: typeof fetch;

  beforeEach(() => {
    db = new Database(":memory:");
    runMigrations(db);
    repo = new BooksRepo(db);
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    db.close();
  });

  test("deduplicates startup jobs and skips the queue once all books are current", () => {
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    expect(openLibraryMetadataStatus(book)).toBe("never_hydrated");

    const first = queueStaleMetadataHydration(repo);
    const second = queueStaleMetadataHydration(repo);
    expect(first?.id).toBe(second?.id);
    expect(repo.listJobsByType("metadata_hydration")).toHaveLength(1);

    repo.updateBookMetadata(book.id, {
      openLibraryMetadataVersion: CURRENT_OPENLIBRARY_METADATA_VERSION,
      openLibraryHydratedAt: new Date().toISOString(),
    });
    repo.markJobSucceeded(first!.id);
    expect(queueStaleMetadataHydration(repo)).toBeNull();
    expect(openLibraryMetadataStatus(repo.getBookRow(book.id)!)).toBe("current");
  });

  test("hydrates successful books independently and retries unresolved books", async () => {
    const first = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const second = repo.createBook({ title: "Unknown Book", author: "Unknown Author" });
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.pathname === "/search.json") {
        const query = url.searchParams.get("q") ?? "";
        return Response.json({
          docs: query.includes("Dune")
            ? [{ key: "/works/OL123W", title: "Dune", author_name: ["Frank Herbert"], language: ["eng"] }]
            : [],
        });
      }
      if (url.pathname === "/works/OL123W.json") {
        return Response.json({ description: "A desert planet." });
      }
      if (url.pathname === "/works/OL123W/editions.json") {
        return Response.json({ entries: [] });
      }
      throw new Error(`Unexpected Open Library request: ${url}`);
    }) as typeof fetch;

    const queued = queueStaleMetadataHydration(repo)!;
    const running = repo.claimQueuedJob(queued.id)!;
    const ctx = { repo, getSettings: () => defaultSettings() };
    let failure: unknown;
    try {
      await processMetadataHydrationJob(ctx, running);
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(Error);
    await handleJobFailure(ctx, running, failure);

    const hydrated = repo.getBookRow(first.id)!;
    expect(hydrated.openlibrary_metadata_version).toBe(CURRENT_OPENLIBRARY_METADATA_VERSION);
    expect(hydrated.openlibrary_hydrated_at).not.toBeNull();
    expect(JSON.parse(hydrated.series_json ?? "[]")).toEqual([]);
    expect(openLibraryMetadataStatus(hydrated)).toBe("current");
    expect(openLibraryMetadataStatus(repo.getBookRow(second.id)!)).toBe("never_hydrated");
    expect(repo.getJob(queued.id)?.status).toBe("queued");
    expect(repo.getJob(queued.id)?.error).toContain(String(second.id));
  });
});
