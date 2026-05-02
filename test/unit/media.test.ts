import { spawn } from "node:child_process";
import { mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import JSZip from "jszip";

import { runMigrations } from "../../src/db";
import {
  applyTranscriptLabels,
  buildManifestationChapters,
  isGenericChapterLabel,
  pickTranscriptLabelForWindow,
  selectPreferredAudioAsset,
  selectPreferredAudioManifestation,
  streamAudioManifestation,
} from "../../src/library/media";
import { BooksRepo } from "../../src/repo";
import type { AssetRow, ManifestationRow } from "../../src/app-types";

function setupRepo(): { db: Database; repo: BooksRepo } {
  const db = new Database(":memory:");
  runMigrations(db);
  return { db, repo: new BooksRepo(db) };
}

async function runCommand(command: string, args: string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "ignore", "pipe"] });
    const stderr: Buffer[] = [];
    child.stderr.on("data", (chunk) => stderr.push(Buffer.from(chunk)));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} exited with code ${code}: ${Buffer.concat(stderr).toString("utf8")}`));
    });
  });
}

async function commandSucceeds(command: string, args: string[]): Promise<boolean> {
  try {
    await runCommand(command, args);
    return true;
  } catch {
    return false;
  }
}

function asset(overrides: Partial<AssetRow>): AssetRow {
  return {
    id: 1,
    book_id: 1,
    kind: "single",
    mime: "audio/mpeg",
    total_size: 100,
    duration_ms: 1000,
    source_release_id: null,
    manifestation_id: null,
    sequence_in_manifestation: 0,
    import_note: null,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function manifestation(overrides: Partial<ManifestationRow>): ManifestationRow {
  return {
    id: 1,
    book_id: 1,
    kind: "audio",
    label: null,
    edition_note: null,
    selection_note: null,
    duration_ms: 1000,
    total_size: 100,
    preferred_score: 0,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
    ...overrides,
  };
}

async function createChapterMarkerEpub(filePath: string): Promise<void> {
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
    <item id="ch3" href="chapter3.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
    <itemref idref="ch3"/>
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
      <navLabel><text>I: The Traveler</text></navLabel>
      <content src="chapter1.xhtml"/>
    </navPoint>
    <navPoint id="navPoint-2" playOrder="2">
      <navLabel><text>II: Red Royal</text></navLabel>
      <content src="chapter2.xhtml"/>
    </navPoint>
    <navPoint id="navPoint-3" playOrder="3">
      <navLabel><text>III: Grey Thief</text></navLabel>
      <content src="chapter3.xhtml"/>
    </navPoint>
  </navMap>
</ncx>`
  );
  zip.file("OEBPS/chapter1.xhtml", `<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>I: The Traveler</h1><p>One.</p></body></html>`);
  zip.file("OEBPS/chapter2.xhtml", `<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>II: Red Royal</h1><p>Two.</p></body></html>`);
  zip.file("OEBPS/chapter3.xhtml", `<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>III: Grey Thief</h1><p>Three.</p></body></html>`);

  const bytes = await zip.generateAsync({ type: "uint8array" });
  await writeFile(filePath, bytes);
}

describe("manifestation selection", () => {
  test("prefers a single-container m4b audio manifestation over a multi mp3 one", () => {
    const chosen = selectPreferredAudioManifestation([
      {
        manifestation: manifestation({ id: 100, kind: "audio" }),
        containers: [asset({ id: 1, kind: "multi", mime: "audio/mpeg", duration_ms: 5000, manifestation_id: 100 })],
      },
      {
        manifestation: manifestation({ id: 101, kind: "audio" }),
        containers: [asset({ id: 2, kind: "single", mime: "audio/mp4", duration_ms: 4000, manifestation_id: 101 })],
      },
    ]);
    expect(chosen?.manifestation.id).toBe(101);
  });

  test("ignores ebook manifestations entirely", () => {
    const chosen = selectPreferredAudioManifestation([
      {
        manifestation: manifestation({ id: 100, kind: "ebook" }),
        containers: [asset({ id: 1, kind: "ebook", mime: "application/epub+zip" })],
      },
    ]);
    expect(chosen).toBeNull();
  });

  test("uses preferred_score as the dominant tiebreak", () => {
    const chosen = selectPreferredAudioManifestation([
      {
        manifestation: manifestation({ id: 100, kind: "audio", preferred_score: 0 }),
        containers: [asset({ id: 1, kind: "single", mime: "audio/mp4", duration_ms: 4000 })],
      },
      {
        // Worse-scoring container, but a much higher preferred_score.
        manifestation: manifestation({ id: 101, kind: "audio", preferred_score: 1000 }),
        containers: [asset({ id: 2, kind: "multi", mime: "audio/mpeg", duration_ms: 1000 })],
      },
    ]);
    expect(chosen?.manifestation.id).toBe(101);
  });

  test("a two-container manifestation is selectable and reports both containers", () => {
    const chosen = selectPreferredAudioManifestation([
      {
        manifestation: manifestation({ id: 100, kind: "audio" }),
        containers: [
          asset({ id: 1, kind: "single", mime: "audio/mpeg", duration_ms: 30 * 60_000, sequence_in_manifestation: 0 }),
          asset({ id: 2, kind: "single", mime: "audio/mpeg", duration_ms: 45 * 60_000, sequence_in_manifestation: 1 }),
        ],
      },
    ]);
    expect(chosen?.containers.length).toBe(2);
    expect(chosen?.containers.map((c) => c.id)).toEqual([1, 2]);
  });

  test("an empty-containers manifestation is excluded", () => {
    const chosen = selectPreferredAudioManifestation([
      { manifestation: manifestation({ id: 100, kind: "audio" }), containers: [] },
    ]);
    expect(chosen).toBeNull();
  });
});

describe("media asset selection", () => {
  test("prefers single m4b-style audio over multi mp3", () => {
    const chosen = selectPreferredAudioAsset([
      asset({ id: 2, kind: "multi", mime: "audio/mpeg", duration_ms: 5000 }),
      asset({ id: 3, kind: "single", mime: "audio/mp4", duration_ms: 4000 }),
    ]);
    expect(chosen?.id).toBe(3);
  });

  test("returns null when only ebook assets exist", () => {
    const chosen = selectPreferredAudioAsset([
      asset({ id: 4, kind: "ebook", mime: "application/epub+zip", duration_ms: null }),
    ]);
    expect(chosen).toBeNull();
  });
});

describe("chapter label heuristics", () => {
  test("recognizes generic chapter labels", () => {
    expect(isGenericChapterLabel("Chapter 1")).toBe(true);
    expect(isGenericChapterLabel("Chapter One")).toBe(true);
    expect(isGenericChapterLabel("chapter 42")).toBe(true);
    expect(isGenericChapterLabel("Ch. 5")).toBe(true);
    expect(isGenericChapterLabel("001")).toBe(true);
    expect(isGenericChapterLabel("14")).toBe(true);
    expect(isGenericChapterLabel("Track 01")).toBe(true);
    expect(isGenericChapterLabel("Part 2")).toBe(true);
    expect(isGenericChapterLabel("Prologue")).toBe(false);
    expect(isGenericChapterLabel("Chapter 1: The Beginning")).toBe(false);
    expect(isGenericChapterLabel("")).toBe(false);
  });

  test("picks first utterance whose midpoint falls in the window", () => {
    const utterances = [
      { startMs: 0, endMs: 1500, text: "This is Audible." },
      { startMs: 4500, endMs: 7000, text: "Corina Press and Harper Audio present" },
      { startMs: 15_000, endMs: 20_000, text: "The Long Game includes mentions and descriptions of suicide and depression." },
      { startMs: 24_000, endMs: 28_000, text: "This book is for the Shane and Ilya fans." },
      { startMs: 32_000, endMs: 34_000, text: "Chapter 1 July" },
    ];
    expect(pickTranscriptLabelForWindow(utterances, 0, 15_700)).toBe("This is Audible");
    expect(pickTranscriptLabelForWindow(utterances, 15_700, 24_100)).toBe(
      "The Long Game includes mentions and descriptions of suicide…"
    );
    expect(pickTranscriptLabelForWindow(utterances, 24_100, 32_100)).toBe("This book is for the Shane and Ilya fans");
    expect(pickTranscriptLabelForWindow(utterances, 32_100, 840_800)).toBe("Chapter 1 July");
  });

  test("returns null when no utterance falls in the window", () => {
    const utterances = [{ startMs: 0, endMs: 1000, text: "Intro" }];
    expect(pickTranscriptLabelForWindow(utterances, 5000, 10_000)).toBeNull();
  });

  test("marks unlabeled generic chapters as 'Unknown (<original>)' when a transcript is present", () => {
    const timings = [
      { id: "ch0", title: "Chapter 1", startMs: 0, endMs: 10_000 },
      { id: "ch1", title: "Chapter 2", startMs: 10_000, endMs: 20_000 },
      { id: "ch2", title: "Prologue", startMs: 20_000, endMs: 30_000 }, // non-generic, should not be touched
    ];
    // Transcript only has content for chapter 2's window.
    const utterances = [{ startMs: 12_000, endMs: 14_000, text: "Hello world" }];
    const labeled = applyTranscriptLabels(timings, utterances);
    expect(labeled[0]?.title).toBe("Unknown (Chapter 1)");
    expect(labeled[1]?.title).toBe("Hello world");
    expect(labeled[2]?.title).toBe("Prologue");
  });

  test("leaves chapters untouched when no utterances are available at all", () => {
    const timings = [{ id: "ch0", title: "Chapter 1", startMs: 0, endMs: 10_000 }];
    const labeled = applyTranscriptLabels(timings, []);
    expect(labeled[0]?.title).toBe("Chapter 1");
  });
});

describe("manifestation media", () => {
  test("builds chapter boundaries across multiple containers", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio", label: "GraphicAudio dramatization" });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 10,
        durationMs: 1500,
        manifestationId: manifestation.id,
        sequenceInManifestation: 0,
        files: [{ path: "/tmp/part-one.mp3", size: 10, start: 0, end: 9, durationMs: 1500, title: "Part One" }],
      });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 20,
        durationMs: 2500,
        manifestationId: manifestation.id,
        sequenceInManifestation: 1,
        files: [{ path: "/tmp/part-two.mp3", size: 20, start: 0, end: 19, durationMs: 2500, title: "Part Two" }],
      });

      const target = repo.getManifestationWithContainers(manifestation.id);
      expect(target).toBeTruthy();
      const chapters = await buildManifestationChapters(repo, target!.manifestation, target!.containers);

      expect(chapters?.chapters).toEqual([
        { startTime: 0, title: "Part One" },
        { startTime: 1.5, title: "Part Two" },
      ]);
    } finally {
      db.close();
    }
  });

  test("uses chapter filenames when imported file titles are generic parts", async () => {
    const { db, repo } = setupRepo();
    try {
      const book = repo.createBook({ title: "Project Hail Mary", author: "Andy Weir" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 20,
        durationMs: 3000,
        manifestationId: manifestation.id,
        files: [
          { path: "/tmp/00 - Intro & Dedication.mp3", size: 10, start: 0, end: 9, durationMs: 1000, title: "Part 1" },
          { path: "/tmp/01 - Chapter 1.mp3", size: 10, start: 10, end: 19, durationMs: 2000, title: "Part 2" },
        ],
      });

      const target = repo.getManifestationWithContainers(manifestation.id);
      expect(target).toBeTruthy();
      const chapters = await buildManifestationChapters(repo, target!.manifestation, target!.containers);

      expect(chapters?.chapters).toEqual([
        { startTime: 0, title: "Intro & Dedication" },
        { startTime: 1, title: "Chapter 1" },
      ]);
    } finally {
      db.close();
    }
  });

  test("uses EPUB and stored transcript to build major manifestation chapter markers", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-chapter-marker-media-"));
    const { db, repo } = setupRepo();
    try {
      const epubPath = path.join(root, "book.epub");
      await createChapterMarkerEpub(epubPath);
      const book = repo.createBook({ title: "A Darker Shade of Magic", author: "V. E. Schwab" });
      const audioManifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
      const ebookManifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
      const audio = repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 30,
        durationMs: 3_000_000,
        manifestationId: audioManifestation.id,
        files: [
          { path: "/tmp/audio-0.mp3", size: 10, start: 0, end: 9, durationMs: 1_000_000, title: "001" },
          { path: "/tmp/audio-1.mp3", size: 10, start: 10, end: 19, durationMs: 1_000_000, title: "002" },
          { path: "/tmp/audio-2.mp3", size: 10, start: 20, end: 29, durationMs: 1_000_000, title: "003" },
        ],
      });
      repo.addAsset({
        bookId: book.id,
        kind: "ebook",
        mime: "application/epub+zip",
        totalSize: 100,
        manifestationId: ebookManifestation.id,
        files: [{ path: epubPath, size: 100, start: 0, end: 99, durationMs: 0, title: "EPUB" }],
      });
      repo.upsertAssetTranscript({
        assetId: audio.id,
        status: "succeeded",
        source: "test",
        algorithmVersion: "test",
        fingerprint: "test",
        transcriptJson: JSON.stringify({
          version: "1.5.0",
          text: "",
          words: [],
          utterances: [
            { startMs: 0, endMs: 1000, text: "This is audible." },
            { startMs: 120_000, endMs: 122_000, text: "Kell wore a very peculiar coat." },
            { startMs: 1_000_000, endMs: 1_001_000, text: "Two." },
            { startMs: 1_001_000, endMs: 1_002_000, text: "Red Royal." },
            { startMs: 2_000_000, endMs: 2_001_000, text: "Three. Grey Thief." },
            { startMs: 2_900_000, endMs: 2_901_000, text: "This concludes A Darker Shade of Magic." },
          ],
        }),
      });

      const target = repo.getManifestationWithContainers(audioManifestation.id);
      const chapters = await buildManifestationChapters(repo, target!.manifestation, target!.containers);

      expect(chapters?.chapters).toEqual([
        { startTime: 0, title: "Opening credits" },
        { startTime: 120, title: "I: The Traveler" },
        { startTime: 1000, title: "II: Red Royal" },
        { startTime: 2000, title: "III: Grey Thief" },
        { startTime: 2900, title: "Closing credits" },
      ]);
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("streams a multi-container manifestation as one virtual audio file", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-manifestation-stream-"));
    const { db, repo } = setupRepo();
    try {
      const partOnePath = path.join(root, "part-one.mp3");
      const partTwoPath = path.join(root, "part-two.mp3");
      await writeFile(partOnePath, "PARTONE");
      await writeFile(partTwoPath, "PARTTWO");

      const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio", label: "GraphicAudio dramatization" });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 7,
        durationMs: 1000,
        manifestationId: manifestation.id,
        sequenceInManifestation: 0,
        files: [{ path: partOnePath, size: 7, start: 0, end: 6, durationMs: 1000, title: "Part One" }],
      });
      repo.addAsset({
        bookId: book.id,
        kind: "multi",
        mime: "audio/mpeg",
        totalSize: 7,
        durationMs: 1000,
        manifestationId: manifestation.id,
        sequenceInManifestation: 1,
        files: [{ path: partTwoPath, size: 7, start: 0, end: 6, durationMs: 1000, title: "Part Two" }],
      });

      const target = repo.getManifestationWithContainers(manifestation.id);
      expect(target).toBeTruthy();
      const response = await streamAudioManifestation(new Request("http://localhost/stream"), repo, target!.manifestation, target!.containers);
      const text = new TextDecoder().decode(await response.arrayBuffer());

      expect(response.status).toBe(206);
      expect(text).toContain("PARTONE");
      expect(text).toContain("PARTTWO");
    } finally {
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("remuxes multi-container mp4 manifestations into a valid ranged m4b stream", async () => {
    if (!(await commandSucceeds("ffmpeg", ["-version"]))) return;

    const root = await mkdtemp(path.join(os.tmpdir(), "podible-manifestation-mp4-stream-"));
    const previousDerivedDir = process.env.PODIBLE_DERIVED_DIR;
    process.env.PODIBLE_DERIVED_DIR = path.join(root, "derived");
    const { db, repo } = setupRepo();
    try {
      const partOnePath = path.join(root, "part-one.m4a");
      const partTwoPath = path.join(root, "part-two.m4a");
      await runCommand("ffmpeg", [
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.1",
        "-c:a",
        "aac",
        partOnePath,
      ]);
      await runCommand("ffmpeg", [
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=660:duration=0.1",
        "-c:a",
        "aac",
        partTwoPath,
      ]);
      const partOneSize = (await stat(partOnePath)).size;
      const partTwoSize = (await stat(partTwoPath)).size;

      const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
      const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio", label: "Two-part M4B" });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mp4",
        totalSize: partOneSize,
        durationMs: 100,
        manifestationId: manifestation.id,
        sequenceInManifestation: 0,
        files: [{ path: partOnePath, size: partOneSize, start: 0, end: partOneSize - 1, durationMs: 100, title: "Part One" }],
      });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mp4",
        totalSize: partTwoSize,
        durationMs: 100,
        manifestationId: manifestation.id,
        sequenceInManifestation: 1,
        files: [{ path: partTwoPath, size: partTwoSize, start: 0, end: partTwoSize - 1, durationMs: 100, title: "Part Two" }],
      });

      const target = repo.getManifestationWithContainers(manifestation.id);
      expect(target).toBeTruthy();
      const response = await streamAudioManifestation(new Request("http://localhost/stream/m/1.m4a"), repo, target!.manifestation, target!.containers);
      const bytes = new Uint8Array(await response.arrayBuffer());
      const typeBox = new TextDecoder().decode(bytes.slice(4, 8));

      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("audio/mp4");
      expect(typeBox).toBe("ftyp");

      const rangeResponse = await streamAudioManifestation(
        new Request("http://localhost/stream/m/1.m4a", { headers: { Range: "bytes=0-15" } }),
        repo,
        target!.manifestation,
        target!.containers
      );
      const rangeBytes = new Uint8Array(await rangeResponse.arrayBuffer());
      expect(rangeResponse.status).toBe(206);
      expect(rangeResponse.headers.get("content-range")).toMatch(/^bytes 0-15\/\d+$/);
      expect(rangeBytes.byteLength).toBe(16);
      expect(new TextDecoder().decode(rangeBytes.slice(4, 8))).toBe("ftyp");
    } finally {
      if (previousDerivedDir === undefined) {
        delete process.env.PODIBLE_DERIVED_DIR;
      } else {
        process.env.PODIBLE_DERIVED_DIR = previousDerivedDir;
      }
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});
