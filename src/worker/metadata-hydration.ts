import type { JobRow } from "../app-types";
import {
  CURRENT_OPENLIBRARY_METADATA_VERSION,
  hydrateBookFromOpenLibrary,
} from "../library/hydration";
import type { BooksRepo } from "../repo";

import { workerLog, type WorkerContext } from "./context";

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
  const stale = ctx.repo.listBooksWithStaleOpenLibraryMetadata(CURRENT_OPENLIBRARY_METADATA_VERSION);
  const failed: number[] = [];

  for (const row of stale) {
    const book = ctx.repo.getBook(row.id);
    if (!book || !(await hydrateBookFromOpenLibrary(ctx.repo, book))) {
      failed.push(row.id);
      continue;
    }
    workerLog(ctx, `[metadata-hydration] book=${row.id} version=${CURRENT_OPENLIBRARY_METADATA_VERSION} hydrated=1`);
  }

  if (failed.length > 0) {
    throw new Error(`Open Library metadata hydration failed for book ids: ${failed.join(", ")}`);
  }

  ctx.repo.markJobSucceeded(job.id);
  workerLog(ctx, `[metadata-hydration] job=${job.id} hydrated=${stale.length}`);
  return "done";
}
