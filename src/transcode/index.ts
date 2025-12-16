import { promises as fs } from "node:fs";
import path from "node:path";

import { dataDir, ensureDataDir, transcodeStatusPath } from "../config";
import { slugify } from "../utils/strings";
import { JobChannel, TranscodeJob, TranscodeState, TranscodeStatus } from "../types";

function statusKey(source: string): string {
  return source;
}

function createJobChannel<T>(): JobChannel<T> {
  const queue: T[] = [];
  let wake: (() => void) | null = null;
  return {
    push(job: T) {
      queue.push(job);
      if (wake) {
        wake();
        wake = null;
      }
    },
    async *stream() {
      for (;;) {
        if (queue.length === 0) {
          await new Promise<void>((resolve) => (wake = resolve));
          continue;
        }
        const next = queue.shift();
        if (next !== undefined) yield next;
      }
    },
  };
}

const transcodeJobs = createJobChannel<TranscodeJob>();
const transcodeStatus = new Map<string, TranscodeStatus>();
const queuedSources = new Set<string>();

async function loadTranscodeStatus() {
  await ensureDataDir();
  try {
    const content = await fs.readFile(transcodeStatusPath, "utf8");
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) {
      parsed.forEach((entry: any) => {
        if (!entry || typeof entry !== "object") return;
        if (typeof entry.source !== "string" || typeof entry.target !== "string") return;
        if (typeof entry.mtimeMs !== "number" || typeof entry.state !== "string") return;
        const record: TranscodeStatus = {
          source: entry.source,
          target: entry.target,
          mtimeMs: entry.mtimeMs,
          state: entry.state as TranscodeState,
          error: typeof entry.error === "string" ? entry.error : undefined,
        };
        transcodeStatus.set(statusKey(record.source), record);
      });
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn("Failed to read transcode status:", (err as Error).message);
    }
  }
}

async function saveTranscodeStatus() {
  await ensureDataDir();
  await fs.writeFile(transcodeStatusPath, JSON.stringify(Array.from(transcodeStatus.values()), null, 2), "utf8");
}

function transcodeOutputPath(source: string, sourceStat: Awaited<ReturnType<typeof fs.stat>>): string {
  const extless = path.basename(source, path.extname(source));
  const safeName = slugify(extless) || "book";
  const hash = sourceStat.mtimeMs.toString(36);
  return path.join(dataDir, `${safeName}-${hash}.mp3`);
}

async function transcodeM4bToMp3(
  source: string,
  target: string,
  onProgress: (outTimeMs: number | undefined, speed: number | undefined) => void
): Promise<void> {
  await ensureDataDir();
  const proc = Bun.spawn({
    cmd: [
      "ffmpeg",
      "-y",
      "-nostdin",
      "-i",
      source,
      "-vn",
      "-map_metadata",
      "0",
      "-map_chapters",
      "0",
      "-write_id3v2",
      "1",
        "-id3v2_version",
        "3",
        "-codec:a",
        "libmp3lame",
        "-qscale:a",
        "2",
        "-threads",
        "1",
        "-progress",
        "pipe:1",
      "-stats_period",
      "1",
      "-loglevel",
      "error",
      target,
    ],
    stdout: "pipe",
    stderr: "inherit",
    stdin: "ignore",
  });

  let buffer = "";
  const reader = proc.stdout?.getReader();
  if (reader) {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += Buffer.from(value).toString("utf8");
      const parts = buffer.split(/\r?\n/);
      buffer = parts.pop() ?? "";
      let outTimeMs: number | undefined;
      let speed: number | undefined;
      parts
        .filter(Boolean)
        .forEach((line) => {
          const [key, valueStr] = line.split("=");
          if (key === "out_time_ms" || key === "out_time_us") {
            const parsed = Number(valueStr);
            if (Number.isFinite(parsed)) {
              outTimeMs = parsed / 1000; // microseconds -> ms
            }
          } else if (key === "speed") {
            const numeric = Number((valueStr || "").replace(/x$/, ""));
            if (Number.isFinite(numeric)) speed = numeric;
          }
        });
      if (outTimeMs !== undefined || speed !== undefined) {
        onProgress(outTimeMs, speed);
      }
    }
  }

  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    throw new Error(`ffmpeg failed with status ${exitCode}`);
  }
}

export {
  createJobChannel,
  loadTranscodeStatus,
  queuedSources,
  saveTranscodeStatus,
  statusKey,
  transcodeJobs,
  transcodeM4bToMp3,
  transcodeOutputPath,
  transcodeStatus,
};
