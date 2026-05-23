/**
 * One-shot migration: recover per-asset transcript files from disk and
 * insert manifestation_transcripts rows for manifestations that have
 * chapter_analysis succeeded but no manifestation_transcripts row.
 *
 * For single-asset manifestations: copy the per-asset file as-is.
 * For multi-asset manifestations: combine with time offsets (like
 * combineStoredTranscriptPayloads) and write a new combined file.
 */

import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { writeFile, readFile } from "node:fs/promises";

const DB_PATH = "/config/podible.sqlite";
const TRANSCRIPTS_DIR = "/config/transcripts";

type Payload = {
  version?: string;
  text: string;
  words: Array<{ startMs: number; endMs: number; [k: string]: unknown }>;
  utterances?: Array<{ startMs: number; endMs: number; text: string; [k: string]: unknown }>;
  chunks?: Array<{ index: number; startMs: number; wordStartIndex: number; wordEndIndex: number; [k: string]: unknown }>;
  rawText?: string;
  rawWords?: Array<unknown>;
};

function offsetPayload(payload: Payload, offsetMs: number, chunkOffset: number, wordOffset: number): Payload {
  return {
    ...payload,
    words: payload.words.map((w) => ({ ...w, startMs: w.startMs + offsetMs, endMs: w.endMs + offsetMs })),
    ...(payload.utterances ? {
      utterances: payload.utterances.map((u) => ({ ...u, startMs: u.startMs + offsetMs, endMs: u.endMs + offsetMs })),
    } : {}),
    ...(payload.chunks ? {
      chunks: payload.chunks.map((c) => ({
        ...c,
        index: c.index + chunkOffset,
        startMs: c.startMs + offsetMs,
        wordStartIndex: c.wordStartIndex + wordOffset,
        wordEndIndex: c.wordEndIndex + wordOffset,
      })),
    } : {}),
  };
}

function combinePayloads(parts: Array<{ payload: Payload; durationMs: number }>): Payload {
  if (parts.length === 0) throw new Error("No parts to combine");
  const combined: Payload[] = [];
  let offsetMs = 0, chunkOffset = 0, wordOffset = 0;
  for (const { payload, durationMs } of parts) {
    const p = offsetPayload(payload, offsetMs, chunkOffset, wordOffset);
    combined.push(p);
    offsetMs += durationMs;
    chunkOffset += p.chunks?.length ?? 0;
    wordOffset += p.words.length;
  }
  const first = combined[0]!;
  return {
    version: first.version,
    text: combined.map((p) => p.text).filter(Boolean).join("\n\n"),
    words: combined.flatMap((p) => p.words),
    ...(combined.some((p) => p.utterances?.length) ? { utterances: combined.flatMap((p) => p.utterances ?? []) } : {}),
    ...(combined.some((p) => p.chunks?.length) ? { chunks: combined.flatMap((p) => p.chunks ?? []) } : {}),
    rawText: combined.map((p) => (p as any).rawText ?? p.text).filter(Boolean).join("\n\n"),
    rawWords: combined.flatMap((p) => (p as any).rawWords ?? p.words),
  };
}

const db = new Database(DB_PATH);
db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");

const now = new Date().toISOString();

// Find manifestations with chapter_analysis succeeded but no manifestation_transcripts
const stuck = db.query<{ manifestation_id: number }, []>(`
  SELECT ca.manifestation_id
  FROM chapter_analysis ca
  LEFT JOIN manifestation_transcripts mt ON mt.manifestation_id = ca.manifestation_id
  WHERE ca.status = 'succeeded'
    AND ca.chapters_json IS NULL
    AND mt.manifestation_id IS NULL
`).all();

console.log(`Found ${stuck.length} stuck manifestations: ${stuck.map(r => r.manifestation_id).join(", ")}`);

for (const { manifestation_id } of stuck) {
  // Get assets in order
  const assets = db.query<{ id: number; duration_ms: number | null; manifestation_id: number }, [number]>(
    "SELECT id, duration_ms, manifestation_id FROM assets WHERE manifestation_id = ? ORDER BY sequence_in_manifestation"
  ).all(manifestation_id);

  if (assets.length === 0) {
    console.log(`  manifestation ${manifestation_id}: no assets, skipping`);
    continue;
  }

  // For each asset, find its transcript file on disk
  const parts: Array<{ payload: Payload; durationMs: number; fingerprint: string }> = [];
  let missing = false;

  for (const asset of assets) {
    // Find files matching this asset ID prefix
    const files = await Array.fromAsync(
      (async function* () {
        const glob = new Bun.Glob(`${asset.id}-*.json`);
        for await (const f of glob.scan(TRANSCRIPTS_DIR)) yield f;
      })()
    );

    if (files.length === 0) {
      console.log(`  manifestation ${manifestation_id} asset ${asset.id}: no transcript file found, skipping manifestation`);
      missing = true;
      break;
    }

    // If multiple files for same asset, pick the most recent by mtime
    let bestFile: string | null = null;
    let bestMtime = 0;
    for (const f of files) {
      const full = path.join(TRANSCRIPTS_DIR, f);
      const stat = await Bun.file(full).stat();
      if (stat.mtimeMs > bestMtime) {
        bestMtime = stat.mtimeMs;
        bestFile = full;
      }
    }

    const content = await readFile(bestFile!, "utf-8");
    const payload = JSON.parse(content) as Payload;
    const fingerprint = path.basename(bestFile!).replace(`${asset.id}-`, "").replace(".json", "");

    // Get duration from asset files if not on asset row
    let durationMs = asset.duration_ms ?? 0;
    if (!durationMs) {
      const fileRows = db.query<{ duration_ms: number }, [number]>(
        "SELECT duration_ms FROM asset_files WHERE asset_id = ?"
      ).all(asset.id);
      durationMs = fileRows.reduce((sum, r) => sum + r.duration_ms, 0);
    }

    parts.push({ payload, durationMs, fingerprint });
    console.log(`  manifestation ${manifestation_id} asset ${asset.id}: found ${bestFile} (${payload.words.length} words, ${durationMs}ms)`);
  }

  if (missing) continue;

  // Combine (or use as-is for single)
  const combined = parts.length === 1 ? parts[0]!.payload : combinePayloads(parts);

  // Compute a combined fingerprint
  const combinedFingerprint = parts.length === 1
    ? parts[0]!.fingerprint
    : createHash("sha256").update(parts.map(p => p.fingerprint).join(":")).digest("hex");

  // Write combined transcript file
  const outPath = path.join(TRANSCRIPTS_DIR, `m${manifestation_id}-${combinedFingerprint}.json`);
  await writeFile(outPath, JSON.stringify(combined));
  console.log(`  manifestation ${manifestation_id}: wrote ${outPath}`);

  // Get algorithm_version from chapter_analysis
  const ca = db.query<{ algorithm_version: string }, [number]>(
    "SELECT algorithm_version FROM chapter_analysis WHERE manifestation_id = ?"
  ).get(manifestation_id)!;

  // Insert manifestation_transcripts row
  db.query(`
    INSERT INTO manifestation_transcripts
      (manifestation_id, status, source, algorithm_version, fingerprint, transcript_path, error, updated_at)
    VALUES (?, 'succeeded', 'whisper_transcript', ?, ?, ?, NULL, ?)
  `).run(manifestation_id, ca.algorithm_version, combinedFingerprint, outPath, now);

  console.log(`  manifestation ${manifestation_id}: inserted manifestation_transcripts row`);
}

console.log("Done.");
