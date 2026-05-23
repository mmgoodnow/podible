/**
 * One-shot: for manifestations whose combined transcript file exists on disk
 * (m<ID>-*.json) but whose manifestation_transcripts row is missing/failed/pending,
 * recompute the correct live fingerprint, rename the file to match, and upsert a
 * succeeded row. Lets recovered transcripts hit the cache instead of re-transcribing.
 *
 * Mirrors computeManifestationFingerprint / transcriptArtifactPath from
 * src/library/chapter-analysis.ts. Keep in sync.
 */

import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { rename, readdir } from "node:fs/promises";

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
const now = new Date().toISOString();

// Manifestations that need curation (succeeded chapter_analysis, null chapters)
// OR whose transcript row is not succeeded but a combined file exists on disk.
const targets = db.query<{ manifestation_id: number }, []>(`
  SELECT ca.manifestation_id
  FROM chapter_analysis ca
  WHERE ca.chapters_json IS NULL
`).all();

const dirEntries = await readdir(TRANSCRIPTS_DIR);

for (const { manifestation_id } of targets) {
  const existing = db.query<{ status: string }, [number]>(
    "SELECT status FROM manifestation_transcripts WHERE manifestation_id = ?"
  ).get(manifestation_id);
  if (existing?.status === "succeeded") {
    console.log(`m${manifestation_id}: already succeeded, skipping`);
    continue;
  }

  // Find a combined file on disk for this manifestation.
  const match = dirEntries.find((name) => name.startsWith(`m${manifestation_id}-`) && name.endsWith(".json"));
  if (!match) {
    console.log(`m${manifestation_id}: no combined transcript file on disk, skipping (will need re-transcription)`);
    continue;
  }

  const assets = db.query<AssetRow, [number]>(
    "SELECT id, duration_ms FROM assets WHERE manifestation_id = ? ORDER BY sequence_in_manifestation"
  ).all(manifestation_id);
  if (assets.length === 0) {
    console.log(`m${manifestation_id}: no assets, skipping`);
    continue;
  }
  const containers = assets.map((asset) => ({
    asset,
    files: db.query<FileRow, [number]>("SELECT path, size FROM asset_files WHERE asset_id = ? ORDER BY id").all(asset.id),
  }));
  if (containers.some((c) => c.files.length === 0)) {
    console.log(`m${manifestation_id}: missing asset_files, skipping`);
    continue;
  }

  const correctFp = await computeManifestationFingerprint(containers);
  const newPath = transcriptArtifactPath(manifestation_id, correctFp);
  const oldPath = path.join(TRANSCRIPTS_DIR, match);
  if (oldPath !== newPath) {
    await rename(oldPath, newPath);
  }

  const algoRow = db.query<{ algorithm_version: string }, [number]>(
    "SELECT algorithm_version FROM chapter_analysis WHERE manifestation_id = ?"
  ).get(manifestation_id);
  const algorithmVersion = algoRow?.algorithm_version ?? CHAPTER_ANALYSIS_ALGORITHM_VERSION;

  db.query(`
    INSERT INTO manifestation_transcripts
      (manifestation_id, status, source, algorithm_version, fingerprint, transcript_path, error, updated_at)
    VALUES (?, 'succeeded', 'whisper_transcript', ?, ?, ?, NULL, ?)
    ON CONFLICT(manifestation_id) DO UPDATE SET
      status = 'succeeded', source = 'whisper_transcript', algorithm_version = excluded.algorithm_version,
      fingerprint = excluded.fingerprint, transcript_path = excluded.transcript_path, error = NULL, updated_at = excluded.updated_at
  `).run(manifestation_id, algorithmVersion, correctFp, newPath, now);

  // Align chapter_analysis fingerprint; keep status succeeded+null-chapters so
  // listBookIdsNeedingCuration re-queues curation at startup.
  db.query("UPDATE chapter_analysis SET fingerprint = ? WHERE manifestation_id = ?")
    .run(correctFp, manifestation_id);

  console.log(`m${manifestation_id}: recovered -> fp ${correctFp.slice(0, 12)} (${match} renamed)`);
}

console.log("Done.");
