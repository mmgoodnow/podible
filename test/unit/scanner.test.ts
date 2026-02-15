import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { KindlingRepo } from "../../src/kindling/repo";
import { scanLibraryRoot } from "../../src/kindling/scanner";

describe("library scanner metadata hydration", () => {
  test("hydrates work identifiers from Open Library for discovered books", async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "kindling-scan-"));
    const author = "Frank Herbert";
    const title = "Dune";
    const bookDir = path.join(tmpRoot, author, title);
    await mkdir(bookDir, { recursive: true });
    await writeFile(path.join(bookDir, "Dune.epub"), Buffer.from("epub-bytes"));

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (!url.startsWith("https://openlibrary.org/search.json")) {
        throw new Error(`Unexpected fetch url: ${url}`);
      }
      return new Response(
        JSON.stringify({
          docs: [
              {
                key: "/works/OL123W",
                first_publish_year: 1965,
                language: ["eng"],
              },
            ],
          }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    try {
      const result = await scanLibraryRoot(repo, tmpRoot);
      expect(result.booksCreated).toBe(1);
      expect(result.assetsCreated).toBe(1);

      const scanned = repo.findBookByTitleAuthor(title, author);
      expect(scanned).toBeTruthy();
      expect(scanned?.language).toBe("eng");
      expect(scanned?.published_at).toBe("1965-01-01T00:00:00.000Z");
      expect(JSON.parse(scanned?.identifiers_json ?? "{}")).toEqual({
        openlibrary: "/works/OL123W",
      });
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
      await rm(tmpRoot, { recursive: true, force: true });
    }
  });

  test("ignores hidden and app-bundle directories while scanning", async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "kindling-scan-ignore-"));
    const validBookDir = path.join(tmpRoot, "Frank Herbert", "Dune");
    await mkdir(validBookDir, { recursive: true });
    await writeFile(path.join(validBookDir, "Dune.epub"), Buffer.from("epub-bytes"));

    const hiddenDir = path.join(tmpRoot, ".idea", "dataSources");
    await mkdir(hiddenDir, { recursive: true });
    await writeFile(path.join(hiddenDir, "project.xml"), Buffer.from("xml"));

    const appBundleDir = path.join(tmpRoot, "Forecast.app", "Contents");
    await mkdir(appBundleDir, { recursive: true });
    await writeFile(path.join(appBundleDir, "Info.plist"), Buffer.from("plist"));

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (!url.startsWith("https://openlibrary.org/search.json")) {
        throw new Error(`Unexpected fetch url: ${url}`);
      }
      return new Response(
        JSON.stringify({
          docs: [
            {
              key: "/works/OL123W",
              first_publish_year: 1965,
              language: ["eng"],
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    try {
      const result = await scanLibraryRoot(repo, tmpRoot);
      expect(result.booksCreated).toBe(1);
      expect(result.assetsCreated).toBe(1);

      expect(repo.findBookByTitleAuthor("Dune", "Frank Herbert")).toBeTruthy();
      expect(repo.findBookByTitleAuthor("dataSources", ".idea")).toBeNull();
      expect(repo.findBookByTitleAuthor("Contents", "Forecast.app")).toBeNull();
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
      await rm(tmpRoot, { recursive: true, force: true });
    }
  });
});
