import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { processAcquireJob } from "../../src/worker/acquire";
import { processReconcileJob, processFullLibraryRefreshJob } from "../../src/worker/maintenance";
import { processMetadataHydrationJob } from "../../src/worker/metadata-hydration";
import { processCoverGenerationJob } from "../../src/library/cover-generation";
import { processChapterAnalysisJob } from "../../src/library/chapter-analysis";
import type { JobType, JobRow } from "../../src/app-types";
import type { WorkerContext } from "../../src/worker/context";

const handlers: Array<[JobType, (ctx: WorkerContext, job: JobRow) => Promise<"done">]> = [
  ["acquire", processAcquireJob],
  ["reconcile", processReconcileJob],
  ["full_library_refresh", processFullLibraryRefreshJob],
  ["metadata_hydration", processMetadataHydrationJob],
  ["cover_generation", processCoverGenerationJob],
  ["chapter_analysis", processChapterAnalysisJob],
];

test.each(handlers)("%s handler leaves completion to the execution engine", async (type, handler) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-handler-ownership-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.updateSettings({ ...repo.getSettings(), libraryRoot: root });
  const job = repo.createJob({ type });
  const guarded = new Proxy(repo, {
    get(target, key) {
      if (key === "markJobSucceeded" || key === "markJobFailed") {
        return () => { throw new Error("Domain handler tried to own execution lifecycle"); };
      }
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  try {
    expect(await handler({ repo: guarded, getSettings: () => repo.getSettings(), onLog() {} }, job)).toBe("done");
  } finally {
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
