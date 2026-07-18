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
    repo.updateBookMetadata(first.id, { language: "spa" });
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
    expect(hydrated.language).toBeNull();
    expect(JSON.parse(hydrated.series_json ?? "[]")).toEqual([]);
    expect(openLibraryMetadataStatus(hydrated)).toBe("current");
    expect(openLibraryMetadataStatus(repo.getBookRow(second.id)!)).toBe("never_hydrated");
    expect(repo.getJob(queued.id)?.status).toBe("queued");
    expect(repo.getJob(queued.id)?.error).toContain(String(second.id));
  });

  test("does not derive work series from a single unrelated edition", async () => {
    const book = repo.createBook({ title: "Wool", author: "Hugh Howey" });
    repo.updateBookMetadata(book.id, {
      identifiers: { openlibrary: "/works/OL16800608W" },
      series: [{ key: null, name: "InfiniTime", position: "006" }],
      language: "spa",
    });
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.pathname === "/search.json") {
        expect(url.searchParams.get("lang")).toBe("en");
        expect(url.searchParams.get("fields")).toContain("editions.language");
        return Response.json({
          docs: [
            {
              key: "/works/OL16800608W",
              title: "Wool",
              author_name: ["Hugh Howey"],
              language: ["spa", "chi", "fre", "ita", "eng"],
              editions: { docs: [{ language: ["eng"] }] },
            },
          ],
        });
      }
      if (url.pathname === "/works/OL16800608W.json") {
        return Response.json({ description: "They live beneath the earth.", series: [] });
      }
      if (url.pathname === "/works/OL16800608W/editions.json") {
        return Response.json({
          entries: [
            {
              key: "/books/OL27161382M",
              title: "羊毛記",
              series: ["InfiniTime -- 006"],
              languages: [{ key: "/languages/chi" }],
            },
          ],
        });
      }
      throw new Error(`Unexpected Open Library request: ${url}`);
    }) as typeof fetch;

    const job = queueStaleMetadataHydration(repo)!;
    await processMetadataHydrationJob(
      { repo, getSettings: () => defaultSettings() },
      repo.claimQueuedJob(job.id)!
    );

    expect(repo.getBook(book.id)?.series).toEqual([]);
    expect(repo.getBookRow(book.id)?.language).toBe("eng");
    expect(repo.getBookRow(book.id)?.openlibrary_metadata_version).toBe(CURRENT_OPENLIBRARY_METADATA_VERSION);
  });

  test("derives series name and position when editions independently agree", async () => {
    const book = repo.createBook({ title: "The Gate of the Feral Gods", author: "Matt Dinniman" });
    repo.updateBookMetadata(book.id, { identifiers: { openlibrary: "/works/OL24848267W" } });
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.pathname === "/search.json") {
        return Response.json({
          docs: [
            {
              key: "/works/OL24848267W",
              title: "The Gate of the Feral Gods",
              author_name: ["Matt Dinniman"],
              editions: { docs: [{ language: ["eng"] }] },
            },
          ],
        });
      }
      if (url.pathname === "/works/OL24848267W.json") {
        return Response.json({ description: "Welcome to the fifth floor.", series: [] });
      }
      if (url.pathname === "/works/OL24848267W/editions.json") {
        return Response.json({
          entries: [
            { key: "/books/OL62200143M", series: ["Dungeon Crawler Carl, Book 4"] },
            {
              key: "/books/OL60487327M",
              subtitle: "Dungeon Crawler Carl, Book 4",
              series: ["Dungeon Crawler Carl"],
            },
            { key: "/books/OL33026536M", series: ["Dungeon Crawler Carl"] },
          ],
        });
      }
      throw new Error(`Unexpected Open Library request: ${url}`);
    }) as typeof fetch;

    const job = queueStaleMetadataHydration(repo)!;
    await processMetadataHydrationJob(
      { repo, getSettings: () => defaultSettings() },
      repo.claimQueuedJob(job.id)!
    );

    expect(repo.getBook(book.id)?.series).toEqual([
      { key: null, name: "Dungeon Crawler Carl", position: "4" },
    ]);
  });

  test("keeps a corroborated series name but drops conflicting edition positions", async () => {
    const book = repo.createBook({ title: "Example", author: "Author" });
    repo.updateBookMetadata(book.id, { identifiers: { openlibrary: "/works/OL999W" } });
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.pathname === "/search.json") {
        return Response.json({
          docs: [{ key: "/works/OL999W", title: "Example", author_name: ["Author"] }],
        });
      }
      if (url.pathname === "/works/OL999W.json") return Response.json({ series: [] });
      if (url.pathname === "/works/OL999W/editions.json") {
        return Response.json({
          entries: [
            { key: "/books/OL1M", series: ["Example Series, Book 3"] },
            { key: "/books/OL2M", series: ["Example Series, Volume III"] },
            { key: "/books/OL3M", series: ["Example Series, Book 4"] },
            { key: "/books/OL4M", series: ["Example Series, Volume IV"] },
          ],
        });
      }
      throw new Error(`Unexpected Open Library request: ${url}`);
    }) as typeof fetch;

    const job = queueStaleMetadataHydration(repo)!;
    await processMetadataHydrationJob(
      { repo, getSettings: () => defaultSettings() },
      repo.claimQueuedJob(job.id)!
    );

    expect(repo.getBook(book.id)?.series).toEqual([
      { key: null, name: "Example Series", position: null },
    ]);
  });

  test("follows a redirected Open Library work before hydrating metadata", async () => {
    const book = repo.createBook({ title: "Sunrise on the Reaping", author: "Suzanne Collins" });
    repo.updateBookMetadata(book.id, {
      identifiers: { openlibrary: "/works/OL42360848W" },
      language: "ger",
    });
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.pathname === "/search.json") {
        const query = url.searchParams.get("q") ?? "";
        if (query.includes("OL43426400W")) {
          return Response.json({
            docs: [
              {
                key: "/works/OL43426400W",
                title: "Amanecer de la Cosecha",
                author_name: ["Suzanne Collins"],
                editions: { docs: [{ language: ["eng"] }] },
              },
            ],
          });
        }
        return Response.json({ docs: [] });
      }
      if (url.pathname === "/works/OL42360848W.json") {
        return Response.json({ location: "/works/OL43426400W", type: { key: "/type/redirect" } });
      }
      if (url.pathname === "/works/OL43426400W.json") {
        return Response.json({ description: "The second Quarter Quell approaches." });
      }
      throw new Error(`Unexpected Open Library request: ${url}`);
    }) as typeof fetch;

    const job = queueStaleMetadataHydration(repo)!;
    await processMetadataHydrationJob(
      { repo, getSettings: () => defaultSettings() },
      repo.claimQueuedJob(job.id)!
    );

    expect(repo.getBookRow(book.id)?.language).toBe("eng");
    expect(repo.getBook(book.id)?.identifiers.openlibrary).toBe("/works/OL43426400W");
  });
});
