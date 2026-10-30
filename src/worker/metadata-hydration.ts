import type { JobRow } from "../app-types";
import { isRunControlError } from "reflow-ts";
import {
  CURRENT_OPENLIBRARY_METADATA_VERSION,
  hydrateBookFromOpenLibrary,
} from "../library/hydration";
import type { BooksRepo } from "../repo";

import { durableStep, workerLog, type WorkerContext } from "./context";

export function queueStaleMetadataHydration(repo: BooksRepo): JobRow | null {
  const stale = repo.listBooksWithStaleOpenLibraryMetadata(CURRENT_OPENLIBRARY_METADATA_VERSION);
  if (stale.length === 0) return null;

  const existing = repo
    .listJobsByType("metadata_hydration")
    .find((job) => job.status === "queued" || job.status === "running");
  if (existing) return existing;

  return repo.createJob({
    type: "metadata_hydration",
    payload: { targetVersion: CURRENT_OPENLIBRARY_METADATA_VERSION },
    maxAttempts: 20,
  });
}

export async function processMetadataHydrationJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const stale = await durableStep(ctx, "metadata-targets", () =>
    ctx.repo.listBooksWithStaleOpenLibraryMetadata(CURRENT_OPENLIBRARY_METADATA_VERSION).map((book) => ({ id: book.id }))
  );
  const failed: number[] = [];
  let fatalStepError: unknown;

  for (const row of stale) {
    const book = ctx.repo.getBook(row.id);
    if (!book) continue;
    try {
      if (!(await hydrateBookFromOpenLibrary(ctx.repo, book, { ctx, stepPrefix: `metadata:${row.id}`, requireMetadata: true }))) {
        failed.push(row.id);
        continue;
      }
    } catch (error) {
      if (isRunControlError(error)) throw error;
      ctx.execution?.assertActive();
      fatalStepError ??= error;
      failed.push(row.id);
      workerLog(ctx, `[metadata-hydration] book=${row.id} error=${String(error)}`);
      continue;
    }
    workerLog(ctx, `[metadata-hydration] book=${row.id} version=${CURRENT_OPENLIBRARY_METADATA_VERSION} hydrated=1`);
  }

  if (failed.length > 0) {
    // Preserve the engine's failed-step identity so an explicit retry can
    // re-execute that fetch instead of replaying it as a handled failure.
    if (fatalStepError) throw fatalStepError;
    throw new Error(`Open Library metadata hydration failed for book ids: ${failed.join(", ")}`);
  }

  ctx.repo.markJobSucceeded(job.id);
  workerLog(ctx, `[metadata-hydration] job=${job.id} hydrated=${stale.length}`);
  return "done";
}
