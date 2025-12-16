import { promises as fs } from "node:fs";

import { bookFromMeta, readyBooks, saveLibraryIndex } from "../library";
import { formatDurationAllowZero } from "../utils/time";
import {
  queuedSources,
  saveTranscodeStatus,
  statusKey,
  transcodeJobs,
  transcodeM4bToMp3,
  transcodeStatus,
} from "./index";

async function workerLoop() {
  for await (const job of transcodeJobs.stream()) {
    const status = transcodeStatus.get(statusKey(job.source));
    if (!status || status.mtimeMs !== job.mtimeMs) {
      queuedSources.delete(job.source);
      continue;
    }
    status.state = "working";
    status.error = undefined;
    if (!status.durationMs && job.meta.durationSeconds) {
      status.durationMs = job.meta.durationSeconds * 1000;
    }
    status.outTimeMs = status.outTimeMs ?? 0;
    status.speed = undefined;
    await saveTranscodeStatus();
    console.log(`[transcode] start source="${job.source}" -> target="${job.target}"`);
    try {
      let lastProgressPersist = 0;
      let lastProgressLogMs = 0;
      let lastOutReported = 0;
      await transcodeM4bToMp3(job.source, job.target, (outTimeMs, speed) => {
        status.outTimeMs = outTimeMs;
        status.speed = speed;
        const now = Date.now();
        if (outTimeMs && status.durationMs && outTimeMs > lastOutReported + 5000) {
          lastOutReported = outTimeMs;
          const ratio = Math.min(1, outTimeMs / status.durationMs);
          const pct = Math.round(ratio * 100);
          const elapsedText = formatDurationAllowZero(outTimeMs / 1000);
          const totalText = formatDurationAllowZero(status.durationMs / 1000);
          if (now - lastProgressLogMs > 1500) {
            lastProgressLogMs = now;
            console.log(
              `[transcode] progress source="${job.source}" ${elapsedText} / ${totalText} (${pct}%)${speed ? ` speed=${speed.toFixed(1)}x` : ""}`
            );
          }
        }
        if (now - lastProgressPersist > 2000) {
          lastProgressPersist = now;
          saveTranscodeStatus().catch(() => {});
        }
      });
      await fs.utimes(job.target, new Date(), new Date(job.mtimeMs));
      const outStat = await fs.stat(job.target);
      status.state = "done";
      status.target = job.target;
      status.outTimeMs = undefined;
      status.speed = undefined;
      await saveTranscodeStatus();
      const book = bookFromMeta(job.meta, job.target, outStat);
      readyBooks.set(book.id, book);
      await saveLibraryIndex();
      console.log(
        `[transcode] done source="${job.source}" -> target="${job.target}" size=${outStat.size} duration=${Math.round(book.durationSeconds ?? 0)}s`
      );
    } catch (err) {
      status.state = "failed";
      status.error = (err as Error).message;
      status.outTimeMs = undefined;
      status.speed = undefined;
      await saveTranscodeStatus();
      console.warn(`Failed to transcode ${job.source}:`, (err as Error).message);
    } finally {
      queuedSources.delete(job.source);
    }
  }
}

export { workerLoop };
