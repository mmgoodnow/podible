#!/usr/bin/env bun
import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { slugify } from "../src/utils/strings";

type Candidate = {
  book_id: number;
  title: string;
  author: string;
  ebook_manifestation_id: number;
  ebook_asset_id: number;
  ebook_path: string;
  audio_manifestation_id: number;
  audio_asset_id: number;
  audio_kind: string;
  audio_mime: string;
  audio_duration_ms: number | null;
  transcript_status: string | null;
  transcript_path: string | null;
  transcript_error: string | null;
  chapter_status: string | null;
  has_chapters: number;
};

type AssetFile = {
  id: number;
  asset_id: number;
  path: string;
  source_path: string | null;
  size: number;
  start: number;
  end: number;
  duration_ms: number | null;
  title: string | null;
};

type RawChapter = {
  id?: number;
  time_base?: string;
  start?: number;
  start_time?: string;
  end?: number;
  end_time?: string;
  tags?: Record<string, string>;
};

type ExportPayload = {
  candidate: Candidate;
  assetFiles: AssetFile[];
  rawChapters: RawChapter[];
  ebookBase64: string;
  transcriptBase64: string;
};

const host = process.env.PODIBLE_PROD_HOST?.trim() || "cyprus";
const container = process.env.PODIBLE_PROD_CONTAINER?.trim() || "podible";
const outputRoot = path.resolve(process.env.CORPUS_ROOT?.trim() || "tmp/chapter-cases/corpus");

function usage(): never {
  console.error(
    [
      "Usage:",
      "  bun run scripts/export-prod-corpus-case.ts --list",
      "  bun run scripts/export-prod-corpus-case.ts --manifestation <id> [--slug <slug>]",
      "",
      "Environment:",
      "  PODIBLE_PROD_HOST=cyprus PODIBLE_PROD_CONTAINER=podible CORPUS_ROOT=tmp/chapter-cases/corpus",
    ].join("\n")
  );
  process.exit(1);
}

function argValue(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) return null;
  return process.argv[index + 1] ?? null;
}

function hasArg(name: string): boolean {
  return process.argv.includes(name);
}

function remoteEval(source: string): string {
  return execFileSync("ssh", [host, "docker", "exec", "-i", container, "bun", "-"], {
    input: source,
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
}

function listCandidates(): Candidate[] {
  return JSON.parse(
    remoteEval(`
      import { Database } from "bun:sqlite";
      const db = new Database("/config/podible.sqlite", { readonly: true });
      const rows = db.query(\`
        SELECT
          b.id AS book_id,
          b.title,
          b.author,
          em.id AS ebook_manifestation_id,
          ea.id AS ebook_asset_id,
          af.path AS ebook_path,
          am.id AS audio_manifestation_id,
          (
            SELECT aa.id
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_asset_id,
          (
            SELECT aa.kind
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_kind,
          (
            SELECT aa.mime
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_mime,
          am.duration_ms AS audio_duration_ms,
          mt.status AS transcript_status,
          mt.transcript_path,
          mt.error AS transcript_error,
          ca.status AS chapter_status,
          ca.chapters_json IS NOT NULL AS has_chapters
        FROM books b
        JOIN manifestations em ON em.book_id = b.id AND em.kind = 'ebook'
        JOIN assets ea ON ea.manifestation_id = em.id AND ea.mime = 'application/epub+zip'
        JOIN asset_files af ON af.asset_id = ea.id
        JOIN manifestations am ON am.book_id = b.id AND am.kind = 'audio'
        LEFT JOIN manifestation_transcripts mt ON mt.manifestation_id = am.id
        LEFT JOIN chapter_analysis ca ON ca.manifestation_id = am.id
        WHERE mt.status = 'succeeded'
          AND mt.transcript_path IS NOT NULL
        GROUP BY am.id, ea.id
        ORDER BY b.title COLLATE NOCASE, am.id, ea.id
      \`).all();
      console.log(JSON.stringify(rows));
    `)
  ) as Candidate[];
}

function exportManifestation(manifestationId: number): ExportPayload {
  return JSON.parse(
    remoteEval(`
      import { Database } from "bun:sqlite";
      import { readFileSync } from "node:fs";
      import { spawnSync } from "node:child_process";
      const manifestationId = ${JSON.stringify(manifestationId)};
      const db = new Database("/config/podible.sqlite", { readonly: true });
      const candidate = db.query(\`
        SELECT
          b.id AS book_id,
          b.title,
          b.author,
          em.id AS ebook_manifestation_id,
          ea.id AS ebook_asset_id,
          af.path AS ebook_path,
          am.id AS audio_manifestation_id,
          (
            SELECT aa.id
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_asset_id,
          (
            SELECT aa.kind
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_kind,
          (
            SELECT aa.mime
            FROM assets aa
            WHERE aa.manifestation_id = am.id
            ORDER BY aa.sequence_in_manifestation ASC, aa.id ASC
            LIMIT 1
          ) AS audio_mime,
          am.duration_ms AS audio_duration_ms,
          mt.status AS transcript_status,
          mt.transcript_path,
          mt.error AS transcript_error,
          ca.status AS chapter_status,
          ca.chapters_json IS NOT NULL AS has_chapters
        FROM manifestations am
        JOIN books b ON b.id = am.book_id
        JOIN manifestations em ON em.book_id = b.id AND em.kind = 'ebook'
        JOIN assets ea ON ea.manifestation_id = em.id AND ea.mime = 'application/epub+zip'
        JOIN asset_files af ON af.asset_id = ea.id
        LEFT JOIN manifestation_transcripts mt ON mt.manifestation_id = am.id
        LEFT JOIN chapter_analysis ca ON ca.manifestation_id = am.id
        WHERE am.id = ?
          AND am.kind = 'audio'
          AND mt.status = 'succeeded'
          AND mt.transcript_path IS NOT NULL
        ORDER BY ea.id ASC
        LIMIT 1
      \`).get(manifestationId);
      if (!candidate) throw new Error("No runnable audio manifestation with EPUB and transcript: " + manifestationId);
      const assetFiles = db.query(\`
        SELECT af.*
        FROM assets a
        JOIN asset_files af ON af.asset_id = a.id
        WHERE a.manifestation_id = ?
        ORDER BY a.sequence_in_manifestation ASC, a.id ASC, af.start ASC, af.id ASC
      \`).all(manifestationId);
      const rawChapters = [];
      for (const file of assetFiles) {
        if (!file.path) continue;
        const result = spawnSync("ffprobe", ["-v", "error", "-print_format", "json", "-show_chapters", file.path], { encoding: "utf8" });
        if (result.status !== 0 || !result.stdout) continue;
        try {
          const parsed = JSON.parse(result.stdout);
          if (Array.isArray(parsed.chapters)) rawChapters.push(...parsed.chapters);
        } catch {}
      }
      const payload = {
        candidate,
        assetFiles,
        rawChapters,
        ebookBase64: readFileSync(candidate.ebook_path).toString("base64"),
        transcriptBase64: readFileSync(candidate.transcript_path).toString("base64"),
      };
      console.log(JSON.stringify(payload));
    `)
  ) as ExportPayload;
}

async function main(): Promise<void> {
  if (hasArg("--list")) {
    const rows = listCandidates();
    for (const row of rows) {
      console.log(
        `${row.audio_manifestation_id}\tbook=${row.book_id}\t${row.title} — ${row.author}\t${row.audio_kind}/${row.audio_mime}\tchapters=${row.has_chapters ? "yes" : "no"}`
      );
    }
    return;
  }

  const rawManifestationId = argValue("--manifestation");
  if (!rawManifestationId) usage();
  const manifestationId = Number(rawManifestationId);
  if (!Number.isInteger(manifestationId) || manifestationId <= 0) usage();

  const payload = exportManifestation(manifestationId);
  const slug = argValue("--slug")?.trim() || slugify(payload.candidate.title);
  const caseDir = path.join(outputRoot, slug);
  await mkdir(caseDir, { recursive: true });
  await writeFile(
    path.join(caseDir, "metadata.json"),
    `${JSON.stringify({ slug, ...payload.candidate }, null, 2)}\n`,
    "utf8"
  );
  await writeFile(path.join(caseDir, "asset-files.json"), `${JSON.stringify(payload.assetFiles, null, 2)}\n`, "utf8");
  await writeFile(path.join(caseDir, "raw-chapters.json"), `${JSON.stringify({ chapters: payload.rawChapters }, null, 2)}\n`, "utf8");
  await writeFile(path.join(caseDir, "book.epub"), Buffer.from(payload.ebookBase64, "base64"));
  await writeFile(path.join(caseDir, "transcript.json"), Buffer.from(payload.transcriptBase64, "base64"));
  console.log(JSON.stringify({ ok: true, slug, manifestationId, caseDir }, null, 2));
}

await main();
