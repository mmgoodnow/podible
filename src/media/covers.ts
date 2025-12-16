import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";

import { dataDir, ensureDataDir } from "../config";
import { slugify } from "../utils/strings";

function coverOutputPath(sourcePath: string, mtimeMs: number, ext: "jpg" | "png" = "jpg"): string {
  const base = path.basename(sourcePath, path.extname(sourcePath));
  const safeName = slugify(base) || "cover";
  const hash = mtimeMs.toString(36);
  return path.join(dataDir, `cover-${safeName}-${hash}.${ext}`);
}

async function extractEmbeddedCover(sourcePath: string, mtimeMs: number): Promise<string | undefined> {
  await ensureDataDir();
  const output = coverOutputPath(sourcePath, mtimeMs, "jpg");
  const existing = await fs.stat(output).catch(() => null);
  if (existing && existing.isFile() && existing.size > 0) return output;
  const result = spawnSync(
    "ffmpeg",
    [
      "-y",
      "-nostdin",
      "-loglevel",
      "error",
      "-i",
      sourcePath,
      "-map",
      "0:v:0",
      "-an",
      "-c:v",
      "mjpeg",
      "-frames:v",
      "1",
      output,
    ],
    { stdio: "ignore" }
  );
  if (result.status === 0) {
    const stat = await fs.stat(output).catch(() => null);
    if (stat && stat.isFile() && stat.size > 0) return output;
  }
  await fs.rm(output, { force: true }).catch(() => {});
  return undefined;
}

async function extractCoverFromEpub(epubPath: string, mtimeMs: number): Promise<string | undefined> {
  const list = spawnSync("unzip", ["-Z1", epubPath], { encoding: "utf8" });
  if (list.status !== 0) return undefined;
  const entries = list.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const candidates = entries
    .filter((name) => /\.(jpg|jpeg|png)$/i.test(name))
    .sort((a, b) => {
      const aCover = /cover/i.test(a) ? 0 : 1;
      const bCover = /cover/i.test(b) ? 0 : 1;
      return aCover - bCover;
    });
  for (const name of candidates) {
    const lower = name.toLowerCase();
    const ext: "jpg" | "png" = lower.endsWith(".png") ? "png" : "jpg";
    const output = coverOutputPath(epubPath, mtimeMs, ext);
    const existing = await fs.stat(output).catch(() => null);
    if (existing && existing.isFile() && existing.size > 0) return output;
    const extracted = spawnSync("unzip", ["-p", epubPath, name], { encoding: "buffer" });
    if (extracted.status !== 0 || !extracted.stdout || (extracted.stdout as Buffer).length === 0) {
      await fs.rm(output, { force: true }).catch(() => {});
      continue;
    }
    try {
      await ensureDataDir();
      await fs.writeFile(output, extracted.stdout as Buffer);
      const stat = await fs.stat(output).catch(() => null);
      if (stat && stat.isFile() && stat.size > 0) return output;
    } catch {
      await fs.rm(output, { force: true }).catch(() => {});
    }
  }
  return undefined;
}

async function resolveCoverPath(
  bookDir: string,
  m4bs: string[],
  mp3s: string[],
  epubs: string[],
  pngs: string[],
  jpgs: string[]
): Promise<string | undefined> {
  if (m4bs.length > 0) {
    const filePath = path.join(bookDir, m4bs[0]);
    const stat = await fs.stat(filePath).catch(() => null);
    if (stat) {
      const embedded = await extractEmbeddedCover(filePath, stat.mtimeMs);
      if (embedded) return embedded;
    }
  }
  for (const mp3 of mp3s) {
    const filePath = path.join(bookDir, mp3);
    const stat = await fs.stat(filePath).catch(() => null);
    if (!stat) continue;
    const embedded = await extractEmbeddedCover(filePath, stat.mtimeMs);
    if (embedded) return embedded;
  }
  for (const epub of epubs) {
    const epubPath = path.join(bookDir, epub);
    const stat = await fs.stat(epubPath).catch(() => null);
    if (!stat) continue;
    const cover = await extractCoverFromEpub(epubPath, stat.mtimeMs);
    if (cover) return cover;
  }
  if (pngs.length > 0) return path.join(bookDir, pngs[0]);
  if (jpgs.length > 0) return path.join(bookDir, jpgs[0]);
  return undefined;
}

export { coverOutputPath, extractCoverFromEpub, extractEmbeddedCover, resolveCoverPath };
