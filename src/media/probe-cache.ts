import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

import { ensureDataDirSync, probeCachePath } from "../config";
import { ChapterTiming, FfprobeChapter, ProbeData } from "../types";
import { cleanMetaValue } from "../utils/strings";

const durationCache = new Map<
  string,
  {
    mtimeMs: number;
    duration?: number;
    failed?: boolean;
  }
>();

const probeCache = new Map<
  string,
  {
    mtimeMs: number;
    data: ProbeData | null;
  }
>();

let probeCacheLoaded = false;

function persistProbeCache() {
  try {
    ensureDataDirSync();
    const payload = Array.from(probeCache.entries()).map(([file, value]) => ({
      file,
      mtimeMs: value.mtimeMs,
      data: value.data,
    }));
    writeFileSync(probeCachePath, JSON.stringify(payload));
  } catch (err) {
    console.warn(`Failed to persist ffprobe cache: ${(err as Error).message}`);
  }
}

function probeData(filePath: string, mtimeMs: number): ProbeData | null {
  if (!probeCacheLoaded) {
    try {
      const content = readFileSync(probeCachePath, "utf8");
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        parsed.forEach((entry: any) => {
          if (!entry || typeof entry !== "object") return;
          if (typeof entry.file !== "string" || typeof entry.mtimeMs !== "number") return;
          probeCache.set(entry.file, { mtimeMs: entry.mtimeMs, data: entry.data ?? null });
        });
      }
    } catch {
      // ignore cache read errors
    }
    probeCacheLoaded = true;
  }

  const cached = probeCache.get(filePath);
  if (cached && cached.mtimeMs === mtimeMs) return cached.data;
  const result = spawnSync(
    "ffprobe",
    ["-v", "error", "-print_format", "json", "-show_format", "-show_chapters", filePath],
    { encoding: "utf8" }
  );
  if (result.error || result.status !== 0) {
    const message = result.error ? result.error.message : result.stderr || String(result.status);
    console.warn(`ffprobe failed for ${filePath}: ${message}`);
    probeCache.set(filePath, { mtimeMs, data: null });
    persistProbeCache();
    return null;
  }
  try {
    const parsed = JSON.parse(result.stdout);
    const format = parsed?.format ?? {};
    const durationStr: string | undefined = format.duration;
    const duration = durationStr ? Number.parseFloat(durationStr) : undefined;
    const tags = format.tags as Record<string, string> | undefined;
    const chapters = (parsed?.chapters ?? []) as FfprobeChapter[];
    const data: ProbeData = {
      duration: Number.isFinite(duration) ? duration : undefined,
      tags,
      chapters,
    };
    probeCache.set(filePath, { mtimeMs, data });
    persistProbeCache();
    return data;
  } catch (err) {
    console.warn(`Failed to parse ffprobe output for ${filePath}: ${(err as Error).message}`);
    probeCache.set(filePath, { mtimeMs, data: null });
    persistProbeCache();
    return null;
  }
}

function getDurationSeconds(filePath: string, mtimeMs: number): number | undefined {
  const cached = durationCache.get(filePath);
  if (cached && cached.mtimeMs === mtimeMs) {
    if (cached.failed) return undefined;
    return cached.duration;
  }
  const probed = probeData(filePath, mtimeMs);
  if (!probed || probed.duration === undefined) {
    durationCache.set(filePath, { mtimeMs, failed: true });
    return undefined;
  }
  durationCache.set(filePath, { mtimeMs, duration: probed.duration, failed: false });
  return probed.duration;
}

function readFfprobeChapters(filePath: string, mtimeMs: number): ChapterTiming[] | null {
  const probed = probeData(filePath, mtimeMs);
  const chapters = probed?.chapters;
  if (!chapters || chapters.length === 0) return null;
  const timings: ChapterTiming[] = [];
  chapters.forEach((chap, index) => {
    const start = Math.max(0, Math.round(Number.parseFloat(chap.start_time ?? "0") * 1000));
    const end = Math.max(start, Math.round(Number.parseFloat(chap.end_time ?? "0") * 1000));
    const tagTitle =
      chap.tags?.title ||
      chap.tags?.TITLE ||
      chap.tags?.name ||
      chap.tags?.NAME ||
      `Chapter ${index + 1}`;
    const title = cleanMetaValue(tagTitle) ?? `Chapter ${index + 1}`;
    timings.push({
      id: `ch${index}`,
      title,
      startMs: start,
      endMs: end,
    });
  });
  return timings;
}

export { getDurationSeconds, probeData, readFfprobeChapters };
