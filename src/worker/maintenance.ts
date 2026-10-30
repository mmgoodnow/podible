import { queueMissingCoverGeneration } from "../library/cover-generation";
import { scanLibraryRoot } from "../library/scanner";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "./context";

export async function processReconcileJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const releases = await durableStep(ctx, "reconcile-targets", () => ctx.repo.findReleasesDownloadedWithoutAssets());
  for (const release of releases) {
    await durableStep(ctx, `reconcile-import:${release.id}`, () => ctx.repo.transaction(() => {
      ctx.execution?.assertActive();
      if (!ctx.repo.findReleasesDownloadedWithoutAssets().some((current) => current.id === release.id)) return;
      const existingImportJobs = ctx.repo
        .listJobsByType("import")
        .some(
          (candidate: JobRow) =>
            candidate.release_id === release.id && (candidate.status === "queued" || candidate.status === "running")
        );
      if (existingImportJobs) return;
      ctx.repo.createJob({
        type: "import",
        releaseId: release.id,
        bookId: release.book_id,
        payload: { reason: "reconcile" },
      });
    }));
  }
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

export async function processFullLibraryRefreshJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const settings = ctx.getSettings();
  await scanLibraryRoot(ctx.repo, settings.libraryRoot, ctx);
  const books = await durableStep(ctx, "refresh-cover-targets", () => ctx.repo.listAllBooks().map((book) => book.id));
  for (const bookId of books) {
    await durableStep(ctx, `refresh-cover:${bookId}`, () => ctx.repo.transaction(() => {
      ctx.execution?.assertActive();
      queueMissingCoverGeneration(ctx.repo, bookId);
    }));
  }
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}
