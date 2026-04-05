import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import JSZip from "jszip";

import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { scanLibraryRoot } from "../../src/scanner";

async function createMinimalEpub(filePath: string, text: string): Promise<void> {
  const zip = new JSZip();
  zip.file("mimetype", "application/epub+zip", { compression: "STORE" });
  zip.file(
    "META-INF/container.xml",
    `<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>`
  );
  zip.file(
    "OEBPS/content.opf",
    `<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Test Book</dc:title>
    <dc:creator>Test Author</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:test-book</dc:identifier>
  </metadata>
  <manifest>
    <item id="ch1" href="chapter1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="ch1"/>
  </spine>
</package>`
  );
  zip.file(
    "OEBPS/chapter1.xhtml",
    `<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body><p>${text}</p></body></html>`
  );
  await writeFile(filePath, Buffer.from(await zip.generateAsync({ type: "uint8array" })));
}

describe("library scanner metadata hydration", () => {
  test("hydrates work identifiers from Open Library for discovered books", async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "books-scan-"));
    const author = "Frank Herbert";
    const title = "Dune";
    const bookDir = path.join(tmpRoot, author, title);
    await mkdir(bookDir, { recursive: true });
    await createMinimalEpub(path.join(bookDir, "Dune.epub"), "Dune");

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
    const repo = new BooksRepo(db);

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
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "books-scan-ignore-"));
    const validBookDir = path.join(tmpRoot, "Frank Herbert", "Dune");
    await mkdir(validBookDir, { recursive: true });
    await createMinimalEpub(path.join(validBookDir, "Dune.epub"), "Dune");

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
    const repo = new BooksRepo(db);

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

  test("ignores hidden files inside book directories", async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), "books-scan-hidden-files-"));
    const bookDir = path.join(tmpRoot, "Sally Rooney", "Normal People");
    await mkdir(bookDir, { recursive: true });
    await createMinimalEpub(path.join(bookDir, "Normal People.epub"), "Normal People");
    await writeFile(path.join(bookDir, "._Normal People.epub"), Buffer.from("sidecar"));

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
              first_publish_year: 2018,
              language: ["eng"],
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    try {
      const result = await scanLibraryRoot(repo, tmpRoot);
      expect(result.booksCreated).toBe(1);
      expect(result.assetsCreated).toBe(1);

      const book = repo.findBookByTitleAuthor("Normal People", "Sally Rooney");
      expect(book).toBeTruthy();
      const assets = book ? repo.listAssetsByBook(book.id) : [];
      expect(assets.length).toBe(1);
      const assetFiles = assets[0] ? repo.getAssetFiles(assets[0].id) : [];
      expect(assetFiles.length).toBe(1);
      expect(assetFiles[0]?.path).toBe(path.join(bookDir, "Normal People.epub"));
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
      await rm(tmpRoot, { recursive: true, force: true });
    }
  });
});
