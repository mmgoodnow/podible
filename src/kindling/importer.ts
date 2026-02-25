import { constants, promises as fs } from "node:fs";
import path from "node:path";

import { getDurationSeconds } from "../media/probe-cache";
import { normalizeAudioExt } from "../media/metadata";

import type { KindlingRepo } from "./repo";
import type { AssetKind, MediaType, ReleaseRow } from "./types";

/**
 * Release importer that materializes downloader output as library assets.
 *
 * Given an rTorrent base path, it discovers files, selects media candidates by
 * release type, hardlinks into `libraryRoot/Author/Title`, and writes
 * immutable asset + asset_file rows.
 */
type ImportResult = {
  assetId: number;
  linkedFiles: string[];
};

type FileInfo = {
  sourcePath: string;
  ext: string;
  size: number;
  mtimeMs: number;
};

export type ImportInspectionFile = {
  sourcePath: string;
  relativePath: string;
  ext: string;
  size: number;
  mtimeMs: number;
  supportedAudio: boolean;
  supportedEbook: boolean;
};

type ImportReleaseOptions = {
  selectedPaths?: string[];
};

function sanitizePathSegment(value: string): string {
  return value.replace(/[\\/:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
}

async function walkFiles(root: string): Promise<string[]> {
  const stat = await fs.stat(root);
  if (stat.isFile()) {
    return [root];
  }

  const entries = await fs.readdir(root, { withFileTypes: true });
  const out: string[] = [];
  for (const entry of entries) {
    const full = path.join(root, entry.name);
    if (entry.isFile()) {
      out.push(full);
    } else if (entry.isDirectory()) {
      out.push(...(await walkFiles(full)));
    }
  }
  return out;
}

async function collectFiles(basePath: string): Promise<FileInfo[]> {
  const files = await walkFiles(basePath);
  const out: FileInfo[] = [];
  for (const file of files) {
    const stat = await fs.stat(file);
    if (!stat.isFile()) continue;
    out.push({
      sourcePath: file,
      ext: path.extname(file).toLowerCase(),
      size: Number(stat.size),
      mtimeMs: Number(stat.mtimeMs),
    });
  }
  return out;
}

function supportsAudio(ext: string): boolean {
  return ext === ".mp3" || ext === ".m4b" || ext === ".m4a" || ext === ".mp4";
}

function supportsEbook(ext: string): boolean {
  return ext === ".epub" || ext === ".pdf";
}

function selectDiscoveredFiles(files: FileInfo[], selectedPaths: string[] | undefined, selectionRoot: string): FileInfo[] {
  if (!selectedPaths || selectedPaths.length === 0) {
    return files;
  }

  const discovered = new Map(files.map((file) => [path.resolve(file.sourcePath), file]));
  const chosen: FileInfo[] = [];
  const seen = new Set<string>();

  for (const selected of selectedPaths) {
    const resolved = path.isAbsolute(selected) ? path.resolve(selected) : path.resolve(selectionRoot, selected);
    if (seen.has(resolved)) continue;
    const file = discovered.get(resolved);
    if (!file) {
      throw new Error(`Selected path not found in source set: ${selected}`);
    }
    chosen.push(file);
    seen.add(resolved);
  }

  if (chosen.length === 0) {
    throw new Error("No selected files to import");
  }
  return chosen;
}

async function ensureHardlink(sourcePath: string, destinationPath: string): Promise<void> {
  await fs.mkdir(path.dirname(destinationPath), { recursive: true });
  try {
    await fs.link(sourcePath, destinationPath);
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    if (err.code === "EEXIST") {
      return;
    }
    if (err.code === "EXDEV") {
      throw new Error(`Hardlink failed with EXDEV: ${sourcePath} -> ${destinationPath}`);
    }
    throw err;
  }
}

function pickAudioCandidates(files: FileInfo[]): { kind: AssetKind; files: FileInfo[]; mime: string } | null {
  const m4 = files.filter((file) => [".m4b", ".m4a", ".mp4"].includes(file.ext));
  if (m4.length > 0) {
    const chosen = [...m4].sort((a, b) => b.size - a.size)[0];
    if (!chosen) return null;
    const ext = normalizeAudioExt(chosen.ext);
    return {
      kind: "single",
      files: [chosen],
      mime: ext === "m4a" ? "audio/mp4" : "audio/mpeg",
    };
  }

  const mp3s = files.filter((file) => file.ext === ".mp3").sort((a, b) => a.sourcePath.localeCompare(b.sourcePath));
  if (mp3s.length === 0) return null;

  if (mp3s.length === 1) {
    return {
      kind: "single",
      files: mp3s,
      mime: "audio/mpeg",
    };
  }

  return {
    kind: "multi",
    files: mp3s,
    mime: "audio/mpeg",
  };
}

function pickEbookCandidate(files: FileInfo[]): { kind: AssetKind; files: FileInfo[]; mime: string } | null {
  const epub = files.find((file) => file.ext === ".epub");
  if (epub) {
    return {
      kind: "ebook",
      files: [epub],
      mime: "application/epub+zip",
    };
  }
  const pdf = files.find((file) => file.ext === ".pdf");
  if (pdf) {
    return {
      kind: "ebook",
      files: [pdf],
      mime: "application/pdf",
    };
  }
  return null;
}

function chooseFilesForMedia(mediaType: MediaType, files: FileInfo[]): { kind: AssetKind; files: FileInfo[]; mime: string } {
  if (mediaType === "audio") {
    const picked = pickAudioCandidates(files);
    if (!picked) throw new Error("No supported audio files found for import");
    return picked;
  }
  const picked = pickEbookCandidate(files);
  if (!picked) throw new Error("No supported ebook files found for import");
  return picked;
}

/**
 * Build one asset from downloader output and attach it to the release's book.
 * Existing files are never moved; imported files are hardlinked into library.
 */
export async function importReleaseFromPath(
  repo: KindlingRepo,
  release: ReleaseRow,
  basePath: string,
  libraryRoot: string,
  options: ImportReleaseOptions = {}
): Promise<ImportResult> {
  const book = repo.getBookRow(release.book_id);
  if (!book) {
    throw new Error(`Book ${release.book_id} not found`);
  }

  const baseStat = await fs.stat(basePath);
  const discovered = await collectFiles(basePath);
  const selectionRoot = baseStat.isFile() ? path.dirname(basePath) : basePath;
  const candidateFiles = selectDiscoveredFiles(discovered, options.selectedPaths, selectionRoot);
  const selected = chooseFilesForMedia(release.media_type, candidateFiles);

  const authorDir = sanitizePathSegment(book.author || "Unknown");
  const titleDir = sanitizePathSegment(book.title || `book-${book.id}`);
  const root = path.join(libraryRoot, authorDir, titleDir);
  const linkedFiles: string[] = [];

  let cursor = 0;
  let durationMsTotal = 0;

  const assetFiles: Array<{
    path: string;
    sourcePath: string;
    size: number;
    start: number;
    end: number;
    durationMs: number;
    title?: string | null;
  }> = [];

  for (const [index, file] of selected.files.entries()) {
    const targetPath = (() => {
      if (selected.kind === "multi") {
        const multiDir = path.join(root, titleDir);
        return path.join(multiDir, path.basename(file.sourcePath));
      }
      const ext = file.ext;
      return path.join(root, `${titleDir}${ext}`);
    })();

    await ensureHardlink(file.sourcePath, targetPath);
    linkedFiles.push(targetPath);

    const durationSeconds =
      release.media_type === "audio"
        ? getDurationSeconds(targetPath, file.mtimeMs) ?? 0
        : 0;
    const durationMs = Math.max(0, Math.round(durationSeconds * 1000));
    const start = cursor;
    const end = Math.max(start, start + file.size - 1);
    cursor += file.size;
    durationMsTotal += durationMs;

    assetFiles.push({
      path: targetPath,
      sourcePath: file.sourcePath,
      size: file.size,
      start,
      end,
      durationMs,
      title: selected.kind === "multi" ? `Part ${index + 1}` : null,
    });
  }

  const totalSize = assetFiles.reduce((sum, item) => sum + item.size, 0);
  const asset = repo.addAsset({
    bookId: book.id,
    kind: selected.kind,
    mime: selected.mime,
    totalSize,
    durationMs: release.media_type === "audio" ? durationMsTotal : null,
    sourceReleaseId: release.id,
    files: assetFiles,
  });

  // Prefer first cover found in target root for /covers endpoint.
  const coverCandidates = [".jpg", ".jpeg", ".png"];
  const existingCover = await fs
    .readdir(root, { withFileTypes: true })
    .then((entries) =>
      entries.find((entry) => entry.isFile() && coverCandidates.includes(path.extname(entry.name).toLowerCase()))
    )
    .catch(() => null);
  if (existingCover) {
    repo.updateBookMetadata(book.id, {
      coverPath: path.join(root, existingCover.name),
      durationMs: release.media_type === "audio" ? durationMsTotal : book.duration_ms,
    });
  }

  return {
    assetId: asset.id,
    linkedFiles,
  };
}

export async function ensurePathReadable(value: string): Promise<void> {
  await fs.access(value, constants.R_OK);
}

export async function inspectImportPath(basePath: string): Promise<ImportInspectionFile[]> {
  await ensurePathReadable(basePath);
  const stat = await fs.stat(basePath);
  const root = stat.isFile() ? path.dirname(basePath) : basePath;
  const discovered = await collectFiles(basePath);
  return discovered
    .sort((a, b) => a.sourcePath.localeCompare(b.sourcePath))
    .map((file) => ({
      sourcePath: file.sourcePath,
      relativePath: path.relative(root, file.sourcePath) || path.basename(file.sourcePath),
      ext: file.ext,
      size: file.size,
      mtimeMs: file.mtimeMs,
      supportedAudio: supportsAudio(file.ext),
      supportedEbook: supportsEbook(file.ext),
    }));
}
