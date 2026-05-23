/**
 * One-shot: recompute the manifestation transcript fingerprint to match what
 * processChapterAnalysisJob will compute from the live audio files, so the
 * recovered transcripts hit the cache instead of triggering re-transcription.
 *
 * Mirrors computeManifestationFingerprint / computeTranscriptFingerprint and
 * transcriptArtifactPath from src/library/chapter-analysis.ts. Keep in sync.
 */

import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { rename } from "node:fs/promises";

const DB_PATH = "/config/podible.sqlite";
const TRANSCRIPTS_DIR = "/config/transcripts";
const CHAPTER_ANALYSIS_ALGORITHM_VERSION = "2026-04-22-atempo-2x-v1";
const TIMESTAMP_TRANSCRIPTION_MODEL = "whisper-1";

type AssetRow = { id: number; duration_ms: number | null };
type FileRow = { path: string; size: number };

async function fileFingerprintData(files: FileRow[]) {
  return Promise.all(
    files.map(async (file) => {
      const stat = await Bun.file(file.path).stat();
      return { path: file.path, size: file.size, mtimeMs: stat.mtimeMs };
    })
  );
}

async function computeTranscriptFingerprint(asset: AssetRow, files: FileRow[]): Promise<string> {
  const hash = createHash("sha256");
  hash.update(
    JSON.stringify({
      version: CHAPTER_ANALYSIS_ALGORITHM_VERSION,
      kind: "transcript",
      model: TIMESTAMP_TRANSCRIPTION_MODEL,
      assetId: asset.id,
      durationMs: asset.duration_ms,
      files: await fileFingerprintData(files),
    })
  );
  return hash.digest("hex");
}

async function computeManifestationFingerprint(containers: Array<{ asset: AssetRow; files: FileRow[] }>): Promise<string> {
  const perAsset = await Promise.all(containers.map((c) => computeTranscriptFingerprint(c.asset, c.files)));
  return createHash("sha1")
    .update(containers.map((c, i) => `${c.asset.id}:${perAsset[i]}`).join("|"))
    .digest("hex");
}

function transcriptArtifactPath(manifestationId: number, fingerprint: string): string {
  const safe = fingerprint.replace(/[^a-zA-Z0-9._-]+/g, "_");
  return path.join(TRANSCRIPTS_DIR, `m${manifestationId}-${safe}.json`);
}

const db = new Database(DB_PATH);
db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");

// Recovered rows: manifestation_transcripts that were inserted by the recovery
// script (transcript_path like %/m<ID>-%) but whose fingerprint won't match the
// live computed one.
const rows = db.query<{ manifestation_id: number; fingerprint: string; transcript_path: string }, []>(
  "SELECT manifestation_id, fingerprint, transcript_path FROM manifestation_transcripts WHERE status = 'succeeded'"
).all();

for (const row of rows) {
  const assets = db.query<AssetRow & { mid: number }, [number]>(
    "SELECT id, duration_ms FROM assets WHERE manifestation_id = ? ORDER BY sequence_in_manifestation"
  ).all(row.manifestation_id);
  if (assets.length === 0) continue;

  const containers = assets.map((asset) => ({
    asset,
    files: db.query<FileRow, [number]>("SELECT path, size FROM asset_files WHERE asset_id = ? ORDER BY id").all(asset.id),
  }));
  if (containers.some((c) => c.files.length === 0)) {
    console.log(`m${row.manifestation_id}: missing asset_files, skipping`);
    continue;
  }

  const correctFp = await computeManifestationFingerprint(containers);
  if (correctFp === row.fingerprint) {
    console.log(`m${row.manifestation_id}: fingerprint already correct (${correctFp.slice(0, 12)})`);
    continue;
  }

  const newPath = transcriptArtifactPath(row.manifestation_id, correctFp);
  if (row.transcript_path !== newPath) {
    await rename(row.transcript_path, newPath);
  }
  db.query("UPDATE manifestation_transcripts SET fingerprint = ?, transcript_path = ? WHERE manifestation_id = ?")
    .run(correctFp, newPath, row.manifestation_id);
  // Keep chapter_analysis fingerprint aligned so the live status reads as current.
  db.query("UPDATE chapter_analysis SET fingerprint = ? WHERE manifestation_id = ?").run(correctFp, row.manifestation_id);
  console.log(`m${row.manifestation_id}: ${row.fingerprint.slice(0, 12)} -> ${correctFp.slice(0, 12)} (renamed)`);
}

console.log("Done.");
