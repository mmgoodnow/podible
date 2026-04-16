import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import JSZip from "jszip";

import { runMigrations } from "../../src/db";
import {
  buildChunkPlan,
  extractGlossaryTerms,
  loadEpubEntries,
  loadStoredTranscriptPayload,
  normalizeTranscriptionLanguage,
  processChapterAnalysisJob,
  queueChapterAnalysisForBook,
} from "../../src/library/chapter-analysis";
import { buildChapters } from "../../src/library/media";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";

async function createMinimalEpub(filePath: string): Promise<void> {
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
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
    <item id="ch1" href="chapter1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ch2" href="chapter2.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
  </spine>
</package>`
  );
  zip.file(
    "OEBPS/toc.ncx",
    `<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head></head>
  <docTitle><text>Test Book</text></docTitle>
  <navMap>
    <navPoint id="navPoint-1" playOrder="1">
      <navLabel><text>Opening</text></navLabel>
      <content src="chapter1.xhtml"/>
    </navPoint>
    <navPoint id="navPoint-2" playOrder="2">
      <navLabel><text>Ending</text></navLabel>
      <content src="chapter2.xhtml"/>
    </navPoint>
  </navMap>
</ncx>`
  );
  zip.file(
    "OEBPS/chapter1.xhtml",
    `<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>Opening</h1><p>Alpha beta gamma delta epsilon zeta eta theta iota kappa lambda MuadDib mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega MuadDib alpha beta gamma delta epsilon zeta eta theta.</p></body></html>`
  );
  zip.file(
    "OEBPS/chapter2.xhtml",
    `<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>Ending</h1><p>Iota kappa lambda MuadDib mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega alpha beta gamma delta epsilon zeta eta theta MuadDib iota kappa lambda mu nu xi omicron pi rho.</p></body></html>`
  );

  const bytes = await zip.generateAsync({ type: "uint8array" });
  await writeFile(filePath, bytes);
}

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  return { db, repo: new BooksRepo(db) };
}

describe("chapter analysis", () => {
  test("loads EPUB chapters through parser adapter", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-epub-test-"));
    try {
      const epubPath = path.join(root, "book.epub");
      await createMinimalEpub(epubPath);
      const entries = await loadEpubEntries(epubPath);
      expect(entries.length).toBe(2);
      expect(entries[0]?.title).toBe("Opening");
      expect(entries[1]?.title).toBe("Ending");
      expect(entries[0]?.wordCount).toBeGreaterThan(6);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("queues one transcript job for preferred audio without requiring an epub", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 100,
        durationMs: 1000,
        files: [
          {
            path: "/tmp/audio-1.mp3",
            size: 100,
            start: 0,
            end: 99,
            durationMs: 1000,
            title: "Part 1",
          },
        ],
      });

      const first = await queueChapterAnalysisForBook(repo, book.id);
      const second = await queueChapterAnalysisForBook(repo, book.id);
      expect(first?.type).toBe("chapter_analysis");
      expect(second?.id).toBe(first?.id);
      expect(repo.listJobsByType("chapter_analysis").length).toBe(1);
    } finally {
      db.close();
    }
  });

  test("extracts compact glossary hints from epub text", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-epub-glossary-"));
    try {
      const epubPath = path.join(root, "book.epub");
      await createMinimalEpub(epubPath);
      const entries = await loadEpubEntries(epubPath);
      const glossary = extractGlossaryTerms(entries, new Set(["opening", "ending", "alpha", "beta"]));
      expect(glossary).toContain("MuadDib");
      expect(glossary).not.toContain("Opening");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("normalizes transcription language tags for OpenAI", () => {
    expect(normalizeTranscriptionLanguage("eng")).toBe("en");
    expect(normalizeTranscriptionLanguage("en-US")).toBe("en");
    expect(normalizeTranscriptionLanguage("zzzz")).toBeNull();
    expect(normalizeTranscriptionLanguage("")).toBeNull();
  });

  test("plans overlapping transcription chunks", () => {
    const chunks = buildChunkPlan(31 * 60_000);
    expect(chunks.length).toBe(2);
    expect(chunks[0]).toMatchObject({ index: 0, startMs: 0, trimStartMs: 0, trimEndMs: 15_000 });
    expect(chunks[1]?.startMs).toBe(29 * 60_000 + 30_000);
    expect(chunks[1]?.trimStartMs).toBe(15_000);
    expect(chunks[1]?.trimEndMs).toBe(0);
  });

  test("stores raw timestamp transcript payload and leaves chapter timings to audio metadata", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-transcript-test-"));
    const { db, repo } = setupRepo();
    try {
      await mkdir(root, { recursive: true });
      const audioPath = path.join(root, "audio.mp3");
      await writeFile(audioPath, "fake audio bytes");
      const book = repo.updateBookMetadata(repo.createBook({ title: "Dune", author: "Frank Herbert" }).id, {
        language: "eng",
      });
      const asset = repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 16,
        durationMs: 90_000,
        files: [
          {
            path: audioPath,
            size: 16,
            start: 0,
            end: 15,
            durationMs: 90_000,
            title: "Audio",
          },
        ],
      });
      const job = repo.createJob({
        type: "chapter_analysis",
        bookId: book.id,
        payload: { assetId: asset.id },
      });

      await processChapterAnalysisJob(
        {
          repo,
          getSettings: () =>
            defaultSettings({
              agents: {
                apiKey: "test-key",
                timeoutMs: 1000,
              },
            }),
          onLog: () => undefined,
        },
        job,
        {
          extractChunkClip: async () => audioPath,
          transcribeChunk: async () => [
            { startMs: 0, endMs: 500, raw: "The", token: "the" },
            { startMs: 500, endMs: 1200, raw: "spice", token: "spice" },
          ],
        }
      );

      const transcript = await loadStoredTranscriptPayload(repo, asset.id);
      expect(transcript?.text).toBe("The spice");
      expect(transcript?.words).toEqual([
        { startMs: 0, endMs: 500, text: "The", token: "the" },
        { startMs: 500, endMs: 1200, text: "spice", token: "spice" },
      ]);
      expect(transcript?.chunks?.length).toBe(1);
      expect(repo.getChapterAnalysis(asset.id)?.chapters_json).toBeNull();
      expect(await buildChapters(repo, asset, repo.getAssetFiles(asset.id))).toBeNull();
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});
