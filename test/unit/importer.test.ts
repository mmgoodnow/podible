import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import JSZip from "jszip";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { importReleaseFromPath } from "../../src/library/importer";
import { BooksRepo } from "../../src/repo";

const tempDirs: string[] = [];

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (!dir) continue;
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(prefix: string): string {
  const dir = mkdtempSync(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  return { db, repo: new BooksRepo(db) };
}

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
  writeFileSync(filePath, Buffer.from(await zip.generateAsync({ type: "uint8array" })));
}

describe("importer path collisions", () => {
  test("allocates a unique library path when a different source collides with an existing import path", async () => {
    const { db, repo } = setupRepo();
    const libraryRoot = tempDir("books-lib-");
    const srcA = tempDir("books-src-a-");
    const srcB = tempDir("books-src-b-");

    const sourceA = path.join(srcA, "book.epub");
    const sourceB = path.join(srcB, "book.epub");
    await createMinimalEpub(sourceA, "First ebook content");
    await createMinimalEpub(sourceB, "Second ebook content");

    const book = repo.createBook({ title: "Twilight", author: "Stephenie Meyer" });
    const release1 = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight [EPUB]",
      mediaType: "ebook",
      infoHash: "1111111111111111111111111111111111111111",
      url: "https://example.com/1.torrent",
      status: "downloaded",
    });
    const release2 = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Twilight (other source) [EPUB]",
      mediaType: "ebook",
      infoHash: "2222222222222222222222222222222222222222",
      url: "https://example.com/2.torrent",
      status: "downloaded",
    });

    const import1 = await importReleaseFromPath(repo, release1, sourceA, libraryRoot);
    const import2 = await importReleaseFromPath(repo, release2, sourceB, libraryRoot);

    expect(import1.linkedFiles).toHaveLength(1);
    expect(import2.linkedFiles).toHaveLength(1);
    expect(import1.linkedFiles[0]).not.toBe(import2.linkedFiles[0]);
    expect(path.basename(import1.linkedFiles[0] ?? "")).toBe("Twilight.epub");
    expect(path.basename(import2.linkedFiles[0] ?? "")).toBe("Twilight (2).epub");

    const assets = repo.listAssetsByBook(book.id);
    expect(assets).toHaveLength(2);
    const asset1 = assets.find((asset) => asset.source_release_id === release1.id);
    const asset2 = assets.find((asset) => asset.source_release_id === release2.id);
    const files1 = repo.getAssetFiles(asset1!.id);
    const files2 = repo.getAssetFiles(asset2!.id);
    expect(files1[0]?.path).toBe(import1.linkedFiles[0]);
    expect(files2[0]?.path).toBe(import2.linkedFiles[0]);
    expect(files1[0]?.path).not.toBe(files2[0]?.path);
    expect(files1[0]?.source_path).toBe(sourceA);
    expect(files2[0]?.source_path).toBe(sourceB);

    db.close();
  });

  test("attaches imported assets to a requested manifestation", async () => {
    const { db, repo } = setupRepo();
    const libraryRoot = tempDir("books-lib-");
    const src = tempDir("books-src-manifestation-");
    const source = path.join(src, "book.epub");
    await createMinimalEpub(source, "Manifestation ebook content");

    const book = repo.createBook({ title: "New Moon", author: "Stephenie Meyer" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: "ebook", label: "Annotated EPUB" });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "New Moon [EPUB]",
      mediaType: "ebook",
      infoHash: "3333333333333333333333333333333333333333",
      url: "https://example.com/3.torrent",
      status: "downloaded",
    });

    await importReleaseFromPath(repo, release, source, libraryRoot, {
      manifestationId: manifestation.id,
      sequenceInManifestation: 2,
      importNote: "Agent selected the EPUB file.",
    });

    const asset = repo.listAssetsByManifestation(manifestation.id)[0];
    expect(asset?.source_release_id).toBe(release.id);
    expect(asset?.manifestation_id).toBe(manifestation.id);
    expect(asset?.sequence_in_manifestation).toBe(2);
    expect(asset?.import_note).toBe("Agent selected the EPUB file.");

    db.close();
  });
});
