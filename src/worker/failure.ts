import { sendPushoverNotification } from "../notify";
import type { JobRow } from "../app-types";

import type { AcquirePayload } from "./acquire";
import { workerLog, type WorkerContext } from "./context";

async function notifyBestEffort(ctx: WorkerContext, title: string, message: string): Promise<boolean> {
  try {
    const result = await sendPushoverNotification(ctx.getSettings(), { title, message });
    if (!result.delivered) {
      workerLog(ctx, `[notify] skipped reason=${result.reason ?? "disabled"} title=${JSON.stringify(title)}`);
      return false;
    }
    workerLog(ctx, `[notify] delivered title=${JSON.stringify(title)}`);
    return true;
  } catch (error) {
    workerLog(ctx, `[notify] failed title=${JSON.stringify(title)} error=${JSON.stringify((error as Error).message)}`);
    return false;
  }
}

async function maybeNotifyAcquireFailure(ctx: WorkerContext, job: JobRow, message: string): Promise<void> {
  if (job.type !== "acquire") return;
  const payload = job.payload_json ? (JSON.parse(job.payload_json) as AcquirePayload) : {};
  if (payload.notifyOnFailure !== true) return;

  const media = payload.media && payload.media.length > 0 ? payload.media.join(",") : "audio,ebook";
  const book = job.book_id ? ctx.repo.getBookRow(job.book_id) : null;
  const title = "Podible recovery failed";
  const body = [
    `Book: ${book ? `${book.title} by ${book.author}` : `book ${job.book_id ?? "unknown"}`}`,
    `Media: ${media}`,
    `Context: ${payload.failureContext ?? "auto-recovery"}`,
    `Reason: ${message}`
  ].join("\n");
  await notifyBestEffort(ctx, title, body);
}

export async function notifyJobFailure(ctx: WorkerContext, job: JobRow, error: unknown): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await maybeNotifyAcquireFailure(ctx, job, message);
  workerLog(ctx, `[execution] job=${job.id} type=${job.type} failed error=${message}`);
}
