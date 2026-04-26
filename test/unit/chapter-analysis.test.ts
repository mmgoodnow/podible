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
  getBookTranscriptStatus,
  loadEpubEntries,
  loadStoredTranscriptPayload,
  mergeChunkSegments,
  normalizeTranscriptionLanguage,
  parseWhisperResponse,
  processChapterAnalysisJob,
  queueChapterAnalysisForBook,
  requestBookTranscription,
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

  test("queues one transcript job per preferred manifestation container", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio", label: "GraphicAudio dramatization" });
      const partOne = repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 100,
        durationMs: 1000,
        manifestationId: manifestation.id,
        sequenceInManifestation: 0,
        files: [{ path: "/tmp/red-rising-1.mp3", size: 100, start: 0, end: 99, durationMs: 1000, title: "Part 1" }],
      });
      const partTwo = repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 100,
        durationMs: 1000,
        manifestationId: manifestation.id,
        sequenceInManifestation: 1,
        files: [{ path: "/tmp/red-rising-2.mp3", size: 100, start: 0, end: 99, durationMs: 1000, title: "Part 2" }],
      });

      await queueChapterAnalysisForBook(repo, book.id);
      await queueChapterAnalysisForBook(repo, book.id);

      const jobs = repo.listJobsByType("chapter_analysis");
      expect(jobs.length).toBe(2);
      expect(jobs.map((job) => JSON.parse(job.payload_json ?? "{}").assetId).sort()).toEqual([partOne.id, partTwo.id].sort());
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

  test("snaps chunk boundaries to nearby chapter markers", () => {
    // 62-min audio with chapter markers at 29:30 and 58:45 (close to the next nominal end).
    const durationMs = 62 * 60_000;
    const boundaries = [29.5 * 60_000, 58.75 * 60_000];
    const chunks = buildChunkPlan(durationMs, boundaries);
    // First chunk snaps end from 30:00 to 29:30 (within ±60s window).
    expect(chunks[0]?.startMs).toBe(0);
    expect(chunks[0]?.durationMs).toBe(29.5 * 60_000);
    // Second chunk starts at 29:30 - 30s overlap = 29:00. Nominal end 59:00 snaps to 58:45.
    expect(chunks[1]?.startMs).toBe(29 * 60_000);
    expect(chunks[1]?.startMs + chunks[1]!.durationMs).toBe(58.75 * 60_000);
    // Third chunk runs to the end of the 62-min audio.
    expect(chunks[2]?.startMs).toBe(58.75 * 60_000 - 30_000);
    expect(chunks[2]?.trimEndMs).toBe(0);
    expect(chunks[2]!.startMs + chunks[2]!.durationMs).toBe(durationMs);
  });

  test("merges chunk segments by midpoint ownership", () => {
    // Two adjacent 30-min chunks with a 30s overlap (seam at 29:45 per trim).
    // Chunk 0 covers 0–30min, keep [0, 29:45]. Chunk 1 covers 29:30–60min, keep [29:45, 59:45].
    const chunk0Plan = { index: 0, startMs: 0, durationMs: 30 * 60_000, trimStartMs: 0, trimEndMs: 15_000 };
    const chunk1Plan = { index: 1, startMs: 29 * 60_000 + 30_000, durationMs: 30 * 60_000, trimStartMs: 15_000, trimEndMs: 0 };
    // A seam-straddling segment: 29:40 -> 29:50 (midpoint 29:45, on the seam).
    // In chunk 0's frame: absolute 29:40-29:50 => relative (29:40, 29:50).
    // In chunk 1's frame: absolute 29:40-29:50 => relative (10s, 20s).
    const chunk0Segments = [
      { startMs: 0, endMs: 60_000, text: "hello world" },
      { startMs: 29 * 60_000 + 40_000, endMs: 29 * 60_000 + 50_000, text: "seam utterance" },
    ];
    const chunk1Segments = [
      { startMs: 10_000, endMs: 20_000, text: "seam utterance" }, // same utterance, chunk-1 frame
      { startMs: 60_000, endMs: 120_000, text: "later" },
    ];
    const merged = mergeChunkSegments([
      { plan: chunk0Plan, segments: chunk0Segments },
      { plan: chunk1Plan, segments: chunk1Segments },
    ]);
    // Seam utterance should appear exactly once.
    const seamCount = merged.filter((s) => s.text === "seam utterance").length;
    expect(seamCount).toBe(1);
    // The other segments should also be present.
    expect(merged.find((s) => s.text === "hello world")).toBeDefined();
    expect(merged.find((s) => s.text === "later")).toBeDefined();
  });

  test("rescales Whisper timestamps to real audio time when audio was sped up", () => {
    // Whisper response represents a 2x sped-up clip. A word Whisper says ends
    // at 0.5s in its own frame is really at 1.0s in the original audio.
    const response = {
      words: [
        { word: "Hello", start: 0, end: 0.5 },
        { word: "world", start: 0.5, end: 1.0 },
      ],
      segments: [{ start: 0, end: 1.0, text: "Hello world" }],
    } as unknown as Parameters<typeof parseWhisperResponse>[0];
    const parsed = parseWhisperResponse(response, 2);
    expect(parsed.words).toEqual([
      { startMs: 0, endMs: 1000, token: "hello", raw: "Hello" },
      { startMs: 1000, endMs: 2000, token: "world", raw: "world" },
    ]);
    expect(parsed.segments).toEqual([{ startMs: 0, endMs: 2000, text: "Hello world" }]);
  });

  test("parseWhisperResponse with multiplier=1 leaves timestamps untouched", () => {
    const response = {
      words: [{ word: "Hi", start: 0.25, end: 0.5 }],
      segments: [{ start: 0, end: 0.5, text: "Hi" }],
    } as unknown as Parameters<typeof parseWhisperResponse>[0];
    const parsed = parseWhisperResponse(response, 1);
    expect(parsed.words).toEqual([{ startMs: 250, endMs: 500, token: "hi", raw: "Hi" }]);
    expect(parsed.segments).toEqual([{ startMs: 0, endMs: 500, text: "Hi" }]);
  });

  test("ignores chapter markers outside the snap window", () => {
    // Marker at 15:00 is far from the 30:00 nominal boundary — should not snap.
    const chunks = buildChunkPlan(35 * 60_000, [15 * 60_000]);
    expect(chunks[0]?.durationMs).toBe(30 * 60_000);
    expect(chunks[1]?.startMs).toBe(29 * 60_000 + 30_000);
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
          transcribeChunk: async () => ({
            words: [
              { startMs: 0, endMs: 500, raw: "The", token: "the" },
              { startMs: 500, endMs: 1200, raw: "spice", token: "spice" },
            ],
            segments: [{ startMs: 0, endMs: 1200, text: "The spice" }],
          }),
        }
      );

      const transcript = await loadStoredTranscriptPayload(repo, asset.id);
      expect(transcript?.text).toBe("The spice");
      expect(transcript?.words).toEqual([
        { startMs: 0, endMs: 500, text: "The", token: "the" },
        { startMs: 500, endMs: 1200, text: "spice", token: "spice" },
      ]);
      expect(transcript?.utterances).toEqual([
        { startMs: 0, endMs: 1200, text: "The spice" },
      ]);
      expect(transcript?.chunks?.length).toBe(1);
      expect(repo.getChapterAnalysis(asset.id)?.chapters_json).toBeNull();
      expect(await buildChapters(repo, asset, repo.getAssetFiles(asset.id))).toBeNull();
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("requestBookTranscription is idempotent and reports current/pending states", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-transcript-status-"));
    const { db, repo } = setupRepo();
    try {
      await mkdir(root, { recursive: true });
      const audioPath = path.join(root, "audio.mp3");
      await writeFile(audioPath, "fake audio bytes");
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 16,
        durationMs: 1000,
        files: [{ path: audioPath, size: 16, start: 0, end: 15, durationMs: 1000, title: null }],
      });

      const beforeApi = await getBookTranscriptStatus(repo, book.id, { apiKeyConfigured: false });
      expect(beforeApi.status).toBe("missing_config");
      expect(beforeApi.jobId).toBeNull();

      const first = await requestBookTranscription(repo, book.id, { apiKeyConfigured: true });
      expect(first.status).toBe("pending");
      expect(first.jobId).not.toBeNull();
      expect(repo.listJobsByType("chapter_analysis").length).toBe(1);

      const second = await requestBookTranscription(repo, book.id, { apiKeyConfigured: true });
      expect(second.status).toBe("pending");
      expect(second.jobId).toBe(first.jobId);
      expect(repo.listJobsByType("chapter_analysis").length).toBe(1);
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("requestBookTranscription returns missing_audio when no audio asset", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      const result = await requestBookTranscription(repo, book.id, { apiKeyConfigured: true });
      expect(result.status).toBe("missing_audio");
      expect(result.jobId).toBeNull();
      expect(repo.listJobsByType("chapter_analysis").length).toBe(0);
    } finally {
      db.close();
    }
  });
});
