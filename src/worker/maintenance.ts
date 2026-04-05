import { scanLibraryRoot } from "../scanner";
import type { JobRow } from "../app-types";
import type { WorkerContext } from "./context";

export async function processReconcileJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const releases = ctx.repo.findReleasesDownloadedWithoutAssets();
  for (const release of releases) {
    const existingImportJobs = ctx.repo
      .listJobsByType("import")
      .some(
        (candidate: JobRow) =>
          candidate.release_id === release.id && (candidate.status === "queued" || candidate.status === "running")
      );
    if (existingImportJobs) continue;
    ctx.repo.createJob({
      type: "import",
      releaseId: release.id,
      bookId: release.book_id,
      payload: { reason: "reconcile" },
    });
  }
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}

export async function processFullLibraryRefreshJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const settings = ctx.getSettings();
  await scanLibraryRoot(ctx.repo, settings.libraryRoot);
  ctx.repo.markJobSucceeded(job.id);
  return "done";
}
