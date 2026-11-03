import { Database } from "bun:sqlite";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";

export async function createBookSpaData() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-book-spa-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.libraryRoot = root;
  settings.agents.apiKey = "fixture-only-not-a-real-key";
  repo.updateSettings(settings);
  const admin = repo.upsertUser({
    provider: "local",
    providerUserId: "book-spa-admin",
    username: "librarian",
    isAdmin: true,
  });
  const reader = repo.upsertUser({
    provider: "local",
    providerUserId: "book-spa-reader",
    username: "reader",
    isAdmin: false,
  });
  repo.createSession(
    admin.id,
    hashSessionToken("book-spa-admin"),
    null,
    "browser",
  );
  repo.createSession(
    reader.id,
    hashSessionToken("book-spa-reader"),
    null,
    "browser",
  );
  const book = repo.createBook({
    title: "Piranesi",
    author: "Susanna Clarke",
    addedByUserId: admin.id,
  });
  repo.updateBookMetadata(book.id, {
    description:
      "The House is a world of infinite halls, a labyrinth of statues and tides. A story about the things we remember, and the worlds we make our home.",
    identifiers: { openlibrary: "/works/OL100W" },
    series: [{ key: "OL100L", name: "The House", position: "1" }],
    language: "eng",
  });
  const audioPath = path.join(root, "fixture-audio.mp3");
  const encoded = Bun.spawnSync([
    "ffmpeg",
    "-hide_banner",
    "-loglevel",
    "error",
    "-f",
    "lavfi",
    "-i",
    "sine=frequency=220:duration=1",
    "-c:a",
    "libmp3lame",
    audioPath,
  ]);
  if (encoded.exitCode !== 0)
    throw new Error(`Fixture audio encoding failed: ${encoded.stderr}`);
  const audioSize = (await Bun.file(audioPath).stat()).size;
  const manifestations = [];
  for (const label of ["Original narration", "Alternate narration"]) {
    const release = repo.createRelease({
      bookId: book.id,
      provider: "fixture",
      providerGuid: null,
      title: label,
      mediaType: "audio",
      infoHash:
        label === "Original narration" ? "a".repeat(40) : "b".repeat(40),
      url: "https://fixture.invalid/audio",
      status: "imported",
    });
    const manifestation = repo.addManifestation({
      bookId: book.id,
      kind: "audio",
      label,
    });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: audioSize,
      durationMs: 1000,
      sourceReleaseId: release.id,
      manifestationId: manifestation.id,
      files: [
        {
          path: audioPath,
          size: audioSize,
          start: 0,
          end: audioSize - 1,
          durationMs: 1000,
          title: label,
        },
      ],
    });
    repo.upsertChapterAnalysis({
      manifestationId: manifestation.id,
      status: "succeeded",
      source: "fixture",
      algorithmVersion: "fixture",
      fingerprint: "fixture",
      chaptersJson: JSON.stringify([
        { startTime: 0, title: `${label}: The House` },
        { startTime: 0.5, title: `${label}: The Tides` },
      ]),
      resolvedBoundaryCount: 2,
      totalBoundaryCount: 2,
      debugJson: JSON.stringify({ curation: {
        status: "completed",
        eventLogPath: "/config/chapter-curation/events.jsonl",
        traceDir: "/config/chapter-curation/traces",
        summary: { budget: { agentCallsStarted: 2, maxAgentCalls: 12 } },
      } }),
    });
    manifestations.push(manifestation);
  }
  for (const [format, mime] of [
    ["epub", "application/epub+zip"],
    ["pdf", "application/pdf"],
  ] as const) {
    const file = path.join(root, `Piranesi.${format}`);
    await writeFile(
      file,
      format === "epub"
        ? "fixture EPUB download"
        : "%PDF-1.4\nfixture PDF download",
    );
    const size = (await Bun.file(file).stat()).size;
    const release = repo.createRelease({
      bookId: book.id,
      provider: "fixture",
      providerGuid: null,
      title: `Piranesi ${format}`,
      mediaType: "ebook",
      infoHash: format === "epub" ? "c".repeat(40) : "d".repeat(40),
      url: "https://fixture.invalid/ebook",
      status: "imported",
    });
    const manifestation = repo.addManifestation({
      bookId: book.id,
      kind: "ebook",
      label: `${format.toUpperCase()} edition`,
    });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime,
      totalSize: size,
      sourceReleaseId: release.id,
      manifestationId: manifestation.id,
      files: [
        {
          path: file,
          size,
          start: 0,
          end: size - 1,
          durationMs: 0,
          title: "Piranesi",
        },
      ],
    });
  }
  const failedBook = repo.createBook({
    title: "Unloved",
    author: "Peyton Corinne",
  });
  const failed = repo.createJob({ type: "acquire", bookId: failedBook.id });
  repo.attachJobExecution(failed.id, "book-spa-failed");
  repo.bindJobJournalReader({
    read: (ids) =>
      new Map(
        ids.map((id) => [
          id,
          {
            status: "failed" as const,
            error:
              "ProwlarrHTTP429 https://provider.invalid/?apikey=NEVER-EXPOSE-THIS&mam_id=PRIVATE",
            attempt_count: 1,
            next_run_at: null,
            updated_at: failed.updated_at,
          },
        ]),
      ),
  });
  repo.finishJobJournalMigration();
  for (let index = 1; index <= 50; index++)
    repo.createBook({
      title: `Shelf story ${String(index).padStart(2, "0")}`,
      author: "The collection",
    });
  return {
    db,
    repo,
    root,
    book,
    failedBook,
    manifestations,
    admin,
    reader,
    audioPath,
    close: async () => {
      db.close();
      await rm(root, { recursive: true, force: true });
    },
  };
}
