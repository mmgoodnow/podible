import path from "node:path";

import { Database } from "bun:sqlite";

import { loadEpubEntries, readStoredTranscriptPayload, replayChapterBoundaryAnalysisFromStoredTranscript } from "../src/library/chapter-analysis";
import { BooksRepo } from "../src/repo";

type Options = {
  dbPath: string;
  epubsRoot: string;
  remoteLibraryRoot: string;
  includeSucceeded: boolean;
  titleFilter: string | null;
};

type ReplayRow = {
  audioAssetId: number;
  title: string;
  author: string;
  originalStatus: string;
  originalResolvedBoundaryCount: number;
  originalTotalBoundaryCount: number;
  originalError: string | null;
  remoteEpubPath: string;
};

function parseArgs(argv: string[]): Options {
  let dbPath = path.resolve("tmp/chapter-analysis-corpus/db/podible-debug.sqlite");
  let epubsRoot = path.resolve("tmp/chapter-analysis-corpus/epubs");
  let remoteLibraryRoot = "/media/MediaStorage/Media/podible";
  let includeSucceeded = false;
  let titleFilter: string | null = null;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--db") {
      dbPath = path.resolve(argv[index + 1] ?? "");
      index += 1;
      continue;
    }
    if (arg === "--epubs-root") {
      epubsRoot = path.resolve(argv[index + 1] ?? "");
      index += 1;
      continue;
    }
    if (arg === "--remote-library-root") {
      remoteLibraryRoot = argv[index + 1] ?? remoteLibraryRoot;
      index += 1;
      continue;
    }
    if (arg === "--include-succeeded") {
      includeSucceeded = true;
      continue;
    }
    if (arg === "--title") {
      titleFilter = (argv[index + 1] ?? "").trim() || null;
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return {
    dbPath,
    epubsRoot,
    remoteLibraryRoot,
    includeSucceeded,
    titleFilter,
  };
}

function relativeCorpusPath(remotePath: string, remoteLibraryRoot: string): string {
  if (!remotePath.startsWith(`${remoteLibraryRoot}/`)) {
    throw new Error(`EPUB path is outside configured library root: ${remotePath}`);
  }
  return remotePath.slice(remoteLibraryRoot.length + 1);
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

const options = parseArgs(process.argv.slice(2));
const db = new Database(options.dbPath, { readonly: true });
const repo = new BooksRepo(db);

try {
  const rows = db
    .query(
      `
      SELECT
        a.id AS audioAssetId,
        b.title AS title,
        b.author AS author,
        ca.status AS originalStatus,
        ca.resolved_boundary_count AS originalResolvedBoundaryCount,
        ca.total_boundary_count AS originalTotalBoundaryCount,
        ca.error AS originalError,
        eaf.path AS remoteEpubPath
      FROM chapter_analysis ca
      JOIN assets a ON a.id = ca.asset_id
      JOIN books b ON b.id = a.book_id
      JOIN assets ea ON ea.id = (
        SELECT value
        FROM jobs j, json_each(j.payload_json)
        WHERE j.book_id = b.id
          AND j.type = 'chapter_analysis'
          AND json_each.key = 'ebookAssetId'
        ORDER BY j.id DESC
        LIMIT 1
      )
      JOIN asset_files eaf ON eaf.asset_id = ea.id
      WHERE a.kind != 'ebook'
      ORDER BY b.author, b.title
      `
    )
    .all() as ReplayRow[];

  const filtered = rows.filter((row) => {
    if (!options.includeSucceeded && row.originalStatus === "succeeded") return false;
    if (options.titleFilter && row.title !== options.titleFilter) return false;
    return true;
  });

  if (filtered.length === 0) {
    console.log("No matching chapter-analysis rows found.");
    process.exit(0);
  }

  const results: Array<Record<string, string | number | boolean>> = [];
  for (const row of filtered) {
    const asset = repo.getAsset(row.audioAssetId);
    const transcriptRow = repo.getAssetTranscript(row.audioAssetId);
    const transcriptPayload = await readStoredTranscriptPayload(transcriptRow);
    const durationMs = asset?.duration_ms ?? 0;
    if (!asset || !transcriptPayload || durationMs <= 0) {
      results.push({
        title: row.title,
        author: row.author,
        originalStatus: row.originalStatus,
        replayStatus: "skipped",
        reason: !asset ? "missing audio asset" : !transcriptPayload ? "missing stored transcript" : "missing duration",
      });
      continue;
    }

    const localEpubPath = path.join(options.epubsRoot, relativeCorpusPath(row.remoteEpubPath, options.remoteLibraryRoot));
    const entries = await loadEpubEntries(localEpubPath);
    const replay = await replayChapterBoundaryAnalysisFromStoredTranscript(entries, durationMs, transcriptPayload);
    const matchCoverageValues =
      Array.isArray((replay.debug as { matches?: Array<{ previousCoverage?: number | null; nextCoverage?: number | null }> }).matches)
        ? (replay.debug as { matches: Array<{ previousCoverage?: number | null; nextCoverage?: number | null }> }).matches
            .flatMap((match) => [match.previousCoverage, match.nextCoverage])
            .filter((value): value is number => typeof value === "number")
        : [];
    const minMatchCoverage = matchCoverageValues.length > 0 ? Math.min(...matchCoverageValues) : 0;
    results.push({
      title: row.title,
      author: row.author,
      originalStatus: row.originalStatus,
      originalResolved: `${row.originalResolvedBoundaryCount}/${row.originalTotalBoundaryCount}`,
      replayResolved: `${replay.resolvedBoundaryCount}/${replay.totalBoundaryCount}`,
      oldThresholdPass: replay.totalBoundaryCount === 0 ? false : replay.resolvedBoundaryCount / replay.totalBoundaryCount >= 0.5,
      chapterCount: replay.chapters.length,
      avgMatchCoverage: Number(average(matchCoverageValues).toFixed(3)),
      minMatchCoverage: Number(minMatchCoverage.toFixed(3)),
    });
  }

  console.table(results);
} finally {
  db.close();
}
