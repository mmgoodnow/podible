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

export async function handleJobFailure(
  ctx: WorkerContext,
  job: JobRow,
  error: unknown,
  expectedOwner: { runId: string; token: string | null },
  attempts = 1
): Promise<void> {
  const message = (error as Error).message;
  const failed = ctx.repo.transaction(() => {
    const current = ctx.repo.getJob(job.id);
    const execution = ctx.repo.getJobExecution(job.id);
    if (
      current?.status !== "running" ||
      execution?.run_id !== expectedOwner.runId ||
      execution.owner_token !== expectedOwner.token
    )
      return null;
    return ctx.repo.markJobFailed(job.id, message, attempts);
  });
  if (!failed) {
    workerLog(ctx, `[worker] job=${job.id} type=${job.type} failed but row missing (likely deleted) error=${message}`);
    return;
  }
  if (failed.status === "failed") {
    await maybeNotifyAcquireFailure(ctx, job, message);
  }
  workerLog(ctx, `[worker] job=${job.id} type=${job.type} failed status=${failed.status} error=${message}`);
}
