import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import JSZip from "jszip";

import { runMigrations } from "../../src/books/db";
import {
  buildChunkPlan,
  extractGlossaryTerms,
  loadEpubEntries,
  loadStoredChapterTimings,
  loadStoredTranscriptPayload,
  normalizeTranscriptionLanguage,
  processChapterAnalysisJob,
  queueChapterAnalysisForBook,
} from "../../src/books/chapter-analysis";
import { buildChapters } from "../../src/books/media";
import { BooksRepo } from "../../src/books/repo";
import { defaultSettings } from "../../src/books/settings";

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

function chapterWords(tokens: string[]): Array<{ text: string; token: string }> {
  return tokens.map((token) => ({ text: token, token }));
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

  test("queues one chapter analysis job for preferred audio + epub assets", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
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
      repo.addAsset({
        bookId: book.id,
        kind: "ebook",
        mime: "application/epub+zip",
        totalSize: 50,
        durationMs: null,
        files: [
          {
            path: "/tmp/book.epub",
            size: 50,
            start: 0,
            end: 49,
            durationMs: 0,
            title: null,
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

  test("extracts a compact glossary from epub text", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-epub-glossary-"));
    try {
      const epubPath = path.join(root, "book.epub");
      await createMinimalEpub(epubPath);
      const entries = await loadEpubEntries(epubPath);
      const glossary = extractGlossaryTerms(entries, new Set(["opening", "ending", "alpha", "beta"]));
      expect(glossary.length).toBeGreaterThan(0);
      expect(glossary).toContain("MuadDib");
      expect(glossary).not.toContain("Opening");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("skips front matter and section dividers when building glossary", () => {
    const glossary = extractGlossaryTerms(
      [
        {
          id: "copyright",
          title: "Copyright Page",
          href: "copyright.xhtml",
          text: "Frontmatterium Frontmatterium Frontmatterium Frontmatterium",
          words: chapterWords(["frontmatterium", "frontmatterium", "frontmatterium", "frontmatterium"]),
          tokens: ["frontmatterium", "frontmatterium", "frontmatterium", "frontmatterium"],
          wordCount: 4,
          cumulativeWords: 4,
          cumulativeRatio: 0.05,
        },
        {
          id: "note",
          title: "Author's Note",
          href: "note.xhtml",
          text: "Frontmatterium Frontmatterium Frontmatterium Frontmatterium",
          words: chapterWords(["frontmatterium", "frontmatterium", "frontmatterium", "frontmatterium"]),
          tokens: ["frontmatterium", "frontmatterium", "frontmatterium", "frontmatterium"],
          wordCount: 4,
          cumulativeWords: 8,
          cumulativeRatio: 0.1,
        },
        {
          id: "part",
          title: "Part One",
          href: "part.xhtml",
          text: "Dividerword Dividerword Dividerword Dividerword",
          words: chapterWords(["dividerword", "dividerword", "dividerword", "dividerword"]),
          tokens: ["dividerword", "dividerword", "dividerword", "dividerword"],
          wordCount: 4,
          cumulativeWords: 12,
          cumulativeRatio: 0.15,
        },
        {
          id: "one",
          title: "Chapter 1",
          href: "one.xhtml",
          text: "MuadDib Arrakis MuadDib Arrakis MuadDib Arrakis",
          words: chapterWords(["muaddib", "arrakis", "muaddib", "arrakis", "muaddib", "arrakis"]),
          tokens: ["muaddib", "arrakis", "muaddib", "arrakis", "muaddib", "arrakis"],
          wordCount: 6,
          cumulativeWords: 18,
          cumulativeRatio: 0.5,
        },
        {
          id: "two",
          title: "Chapter 2",
          href: "two.xhtml",
          text: "MuadDib Arrakis MuadDib Arrakis MuadDib Arrakis",
          words: chapterWords(["muaddib", "arrakis", "muaddib", "arrakis", "muaddib", "arrakis"]),
          tokens: ["muaddib", "arrakis", "muaddib", "arrakis", "muaddib", "arrakis"],
          wordCount: 6,
          cumulativeWords: 24,
          cumulativeRatio: 1,
        },
      ],
      new Set(["chapter"])
    );

    expect(glossary).toContain("MuadDib");
    expect(glossary).not.toContain("Frontmatterium");
    expect(glossary).not.toContain("Dividerword");
  });

  test("treats simple inflected dictionary words as ordinary glossary noise", () => {
    const glossary = extractGlossaryTerms(
      [
        {
          id: "one",
          title: "1",
          href: "one.xhtml",
          text: "Sensing Ignoring Mixing Reminded Pays Shes Arrakis MuadDib",
          words: chapterWords(["sensing", "ignoring", "mixing", "reminded", "pays", "shes", "arrakis", "muaddib"]),
          tokens: ["sensing", "ignoring", "mixing", "reminded", "pays", "shes", "arrakis", "muaddib"],
          wordCount: 8,
          cumulativeWords: 8,
          cumulativeRatio: 0.5,
        },
        {
          id: "two",
          title: "2",
          href: "two.xhtml",
          text: "Sensing Ignoring Mixing Reminded Pays Shes Arrakis MuadDib",
          words: chapterWords(["sensing", "ignoring", "mixing", "reminded", "pays", "shes", "arrakis", "muaddib"]),
          tokens: ["sensing", "ignoring", "mixing", "reminded", "pays", "shes", "arrakis", "muaddib"],
          wordCount: 8,
          cumulativeWords: 16,
          cumulativeRatio: 1,
        },
      ],
      new Set(["sense", "ignore", "mix", "remind", "pay", "she"])
    );

    expect(glossary).toContain("Arrakis");
    expect(glossary).toContain("MuadDib");
    expect(glossary).not.toContain("Sensing");
    expect(glossary).not.toContain("Ignoring");
    expect(glossary).not.toContain("Mixing");
    expect(glossary).not.toContain("Reminded");
    expect(glossary).not.toContain("Pays");
    expect(glossary).not.toContain("Shes");
  });

  test("folds apostrophe-s forms into their base glossary terms", () => {
    const glossary = extractGlossaryTerms(
      [
        {
          id: "one",
          title: "1",
          href: "one.xhtml",
          text: "She's Let's Edward's Esme's Carlisle's Volterra",
          words: chapterWords(["shes", "lets", "edwards", "esmes", "carlisles", "volterra"]),
          tokens: ["shes", "lets", "edwards", "esmes", "carlisles", "volterra"],
          wordCount: 6,
          cumulativeWords: 6,
          cumulativeRatio: 0.5,
        },
        {
          id: "two",
          title: "2",
          href: "two.xhtml",
          text: "She's Let's Edward's Esme's Carlisle's Volterra",
          words: chapterWords(["shes", "lets", "edwards", "esmes", "carlisles", "volterra"]),
          tokens: ["shes", "lets", "edwards", "esmes", "carlisles", "volterra"],
          wordCount: 6,
          cumulativeWords: 12,
          cumulativeRatio: 1,
        },
      ],
      new Set(["she", "let"])
    );

    expect(glossary).toContain("Edward");
    expect(glossary).toContain("Esme");
    expect(glossary).toContain("Carlisle");
    expect(glossary).toContain("Volterra");
    expect(glossary).not.toContain("She's");
    expect(glossary).not.toContain("Let's");
    expect(glossary).not.toContain("Edward's");
    expect(glossary).not.toContain("Esme's");
    expect(glossary).not.toContain("Carlisle's");
  });

  test("builds chunk plan with overlap trims", () => {
    const chunks = buildChunkPlan(70 * 60_000);
    expect(chunks.length).toBe(3);
    expect(chunks[0]?.trimStartMs).toBe(0);
    expect(chunks[0]?.trimEndMs).toBeGreaterThan(0);
    expect(chunks[1]?.trimStartMs).toBeGreaterThan(0);
    expect(chunks[1]?.trimEndMs).toBeGreaterThan(0);
    expect(chunks[2]?.trimStartMs).toBeGreaterThan(0);
    expect(chunks[2]?.trimEndMs).toBe(0);
  });

  test("normalizes book languages for transcription requests", () => {
    expect(normalizeTranscriptionLanguage("eng")).toBe("en");
    expect(normalizeTranscriptionLanguage("en-US")).toBe("en");
    expect(normalizeTranscriptionLanguage("EN_gb")).toBe("en");
    expect(normalizeTranscriptionLanguage("fr")).toBe("fr");
    expect(normalizeTranscriptionLanguage("zzz")).toBeNull();
    expect(normalizeTranscriptionLanguage(null)).toBeNull();
  });

  test("stores derived timings and chapters endpoint prefers cached analysis", async () => {
    const { db, repo } = setupRepo();
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-chapter-analysis-"));
    try {
      const settings = repo.ensureSettings();
      repo.updateSettings(
        defaultSettings({
          ...settings,
          auth: { mode: "local", key: "test" },
          agents: {
            ...settings.agents,
            apiKey: "test-key",
          },
        })
      );

      const audioPath = path.join(root, "audio.mp3");
      const epubPath = path.join(root, "book.epub");
      await mkdir(root, { recursive: true });
      await writeFile(audioPath, Buffer.from("audio"));
      await writeFile(epubPath, Buffer.from("epub"));

      const book = repo.createBook({ title: "Hyperion", author: "Dan Simmons" });
      const audio = repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 200,
        durationMs: 900_000,
        files: [
          {
            path: audioPath,
            size: 200,
            start: 0,
            end: 199,
            durationMs: 900_000,
            title: "Part 1",
          },
        ],
      });
      const epub = repo.addAsset({
        bookId: book.id,
        kind: "ebook",
        mime: "application/epub+zip",
        totalSize: 100,
        durationMs: null,
        files: [
          {
            path: epubPath,
            size: 100,
            start: 0,
            end: 99,
            durationMs: 0,
            title: null,
          },
        ],
      });
      const job = repo.createJob({
        type: "chapter_analysis",
        bookId: book.id,
        payload: {
          assetId: audio.id,
          ebookAssetId: epub.id,
        },
      });

      await processChapterAnalysisJob(
        {
          repo,
          getSettings: () => repo.getSettings(),
        },
        job,
        {
          loadEpubEntries: async () => [
            {
              id: "one",
              title: "One",
              href: "one.xhtml",
              text: "one one one",
              words: chapterWords(Array.from({ length: 30 }, (_, index) => `one${index}`)),
              tokens: Array.from({ length: 30 }, (_, index) => `one${index}`),
              wordCount: 30,
              cumulativeWords: 30,
              cumulativeRatio: 1 / 3,
            },
            {
              id: "two",
              title: "Two",
              href: "two.xhtml",
              text: "two two two",
              words: chapterWords(Array.from({ length: 30 }, (_, index) => `two${index}`)),
              tokens: Array.from({ length: 30 }, (_, index) => `two${index}`),
              wordCount: 30,
              cumulativeWords: 60,
              cumulativeRatio: 2 / 3,
            },
            {
              id: "three",
              title: "Three",
              href: "three.xhtml",
              text: "three three three",
              words: chapterWords(Array.from({ length: 30 }, (_, index) => `three${index}`)),
              tokens: Array.from({ length: 30 }, (_, index) => `three${index}`),
              wordCount: 30,
              cumulativeWords: 90,
              cumulativeRatio: 1,
            },
          ],
          extractChunkClip: async ({ tempDir, clipName }) => {
            const clipPath = path.join(tempDir, `${clipName}.mp3`);
            await mkdir(tempDir, { recursive: true });
            await writeFile(clipPath, Buffer.from("clip"));
            return clipPath;
          },
          transcribeChunk: async (_settings, _clipPath, _prompt, _book) =>
            Array.from({ length: 120 }, (_, index) => {
              const chapterIndex = Math.floor(index / 40);
              const prefix = chapterIndex === 0 ? "one" : chapterIndex === 1 ? "two" : "three";
              return {
                token: `${prefix}${index % 30}`,
                raw: `${prefix}${index % 30}`,
                startMs: index * 7_500,
                endMs: index * 7_500 + 500,
              };
            }),
        }
      );

      const stored = await loadStoredChapterTimings(repo, audio, repo.getAssetFiles(audio.id));
      const transcript = await loadStoredTranscriptPayload(repo, audio.id);
      expect(stored?.length).toBe(3);
      expect(stored?.[1]?.title).toBe("Two");
      expect(transcript?.words.length).toBeGreaterThan(0);
      expect(transcript?.text).toContain("one one one");
      expect(transcript?.segments?.length).toBe(3);
      expect(transcript?.segments?.[1]?.title).toBe("Two");
      expect(transcript?.segments?.[1]?.matchedWordCount).toBeGreaterThan(0);
      expect(transcript?.words.length).toBe(90);

      const chapters = await buildChapters(repo, audio, repo.getAssetFiles(audio.id));
      expect(chapters?.chapters.length).toBe(3);
      expect(chapters?.chapters[1]?.title).toBe("Two");
      expect(chapters?.chapters[1]?.startTime).toBeGreaterThan(0);
    } finally {
      await rm(root, { recursive: true, force: true });
      db.close();
    }
  });

  test("fuzzy boundary alignment tolerates substitutions insertions and omissions", async () => {
    const { db, repo } = setupRepo();
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-chapter-alignment-"));
    try {
      const settings = repo.ensureSettings();
      repo.updateSettings(
        defaultSettings({
          ...settings,
          auth: { mode: "local", key: "test" },
          agents: {
            ...settings.agents,
            apiKey: "test-key",
          },
        })
      );

      const audioPath = path.join(root, "audio.mp3");
      const epubPath = path.join(root, "book.epub");
      await mkdir(root, { recursive: true });
      await writeFile(audioPath, Buffer.from("audio"));
      await writeFile(epubPath, Buffer.from("epub"));

      const chapterOneTokens = Array.from({ length: 40 }, (_, index) => `alpha${index}`);
      const chapterTwoTokens = Array.from({ length: 40 }, (_, index) => `beta${index}`);

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const audio = repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 200,
        durationMs: 100_000,
        files: [
          {
            path: audioPath,
            size: 200,
            start: 0,
            end: 199,
            durationMs: 100_000,
            title: "Book",
          },
        ],
      });
      const epub = repo.addAsset({
        bookId: book.id,
        kind: "ebook",
        mime: "application/epub+zip",
        totalSize: 100,
        durationMs: null,
        files: [
          {
            path: epubPath,
            size: 100,
            start: 0,
            end: 99,
            durationMs: 0,
            title: null,
          },
        ],
      });
      const job = repo.createJob({
        type: "chapter_analysis",
        bookId: book.id,
        payload: {
          assetId: audio.id,
          ebookAssetId: epub.id,
        },
      });

      await processChapterAnalysisJob(
        {
          repo,
          getSettings: () => repo.getSettings(),
        },
        job,
        {
          loadEpubEntries: async () => [
            {
              id: "one",
              title: "One",
              href: "one.xhtml",
              text: chapterOneTokens.join(" "),
              words: chapterWords(chapterOneTokens),
              tokens: chapterOneTokens,
              wordCount: chapterOneTokens.length,
              cumulativeWords: chapterOneTokens.length,
              cumulativeRatio: 0.5,
            },
            {
              id: "two",
              title: "Two",
              href: "two.xhtml",
              text: chapterTwoTokens.join(" "),
              words: chapterWords(chapterTwoTokens),
              tokens: chapterTwoTokens,
              wordCount: chapterTwoTokens.length,
              cumulativeWords: chapterOneTokens.length + chapterTwoTokens.length,
              cumulativeRatio: 1,
            },
          ],
          extractChunkClip: async ({ tempDir, clipName }) => {
            const clipPath = path.join(tempDir, `${clipName}.mp3`);
            await mkdir(tempDir, { recursive: true });
            await writeFile(clipPath, Buffer.from("clip"));
            return clipPath;
          },
          transcribeChunk: async () => {
            const transcriptTokens = [
              ...chapterOneTokens.slice(0, 6),
              "filler0",
              ...chapterOneTokens.slice(6, 10).map((token, index) => (index === 1 ? `${token}x` : token)),
              ...chapterOneTokens.slice(10, 18),
              "filler1",
              ...chapterOneTokens.slice(19),
              "bridgeword",
              ...chapterTwoTokens.slice(0, 5).map((token, index) => (index === 2 ? `${token}x` : token)),
              "filler2",
              ...chapterTwoTokens.slice(5, 12),
              ...chapterTwoTokens.slice(13),
            ];
            return transcriptTokens.map((token, index) => ({
              token: token.toLowerCase(),
              raw: token,
              startMs: index * 1_000,
              endMs: index * 1_000 + 600,
            }));
          },
        }
      );

      const stored = await loadStoredChapterTimings(repo, audio, repo.getAssetFiles(audio.id));
      const transcript = await loadStoredTranscriptPayload(repo, audio.id);
      expect(stored?.length).toBe(2);
      expect(stored?.[1]?.startMs).toBeGreaterThan(20_000);
      expect(stored?.[1]?.startMs).toBeLessThan(70_000);
      expect(transcript?.segments?.length).toBe(2);
      expect(transcript?.segments?.[0]?.anchorCoverage).toBeGreaterThan(0.5);
      expect(transcript?.segments?.[1]?.anchorCoverage).toBeGreaterThan(0.5);
      expect(transcript?.segments?.[0]?.matchedWordCount).toBeGreaterThan(20);
      expect(transcript?.segments?.[1]?.matchedWordCount).toBeGreaterThan(20);
    } finally {
      await rm(root, { recursive: true, force: true });
      db.close();
    }
  });
});
