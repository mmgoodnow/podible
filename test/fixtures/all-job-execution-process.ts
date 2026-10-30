import { appendFileSync } from "node:fs";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { processCoverGenerationJob } from "../../src/library/cover-generation";
import { processMetadataHydrationJob } from "../../src/worker/metadata-hydration";
import { processReconcileJob } from "../../src/worker/maintenance";

const [root, stopAt = "", domainMode = ""] = process.argv.slice(2);
const db = new Database(`${root}/books.sqlite`);
runMigrations(db);
db.exec("CREATE TABLE IF NOT EXISTS fixture_effects (job_id INTEGER, phase TEXT, PRIMARY KEY(job_id, phase))");
db.exec("CREATE TABLE IF NOT EXISTS fixture_attempts (job_id INTEGER, phase TEXT)");
const repo = new BooksRepo(db);
if (domainMode) {
  globalThis.fetch = (async (input: unknown) => {
    db.query("INSERT INTO fixture_attempts VALUES (0, 'metadata-fetch')").run();
    const url = new URL(String(input));
    if (url.pathname.endsWith("editions.json")) return Response.json({ entries: [] });
    if (url.pathname === "/search.json")
      return Response.json({ docs: [{ key: "/works/OL123W", title: "Fixture", author_name: ["Author"] }] });
    return Response.json({ description: "Saved domain description." });
  }) as typeof fetch;
}
const runtime = await createJobExecutionRuntime(
  { repo, getSettings: () => repo.getSettings(), onLog: () => {} },
  `${root}/execution.sqlite`,
  {
    leaseMs: 250,
    processJob: async (ctx, job) => {
      if (domainMode) {
        if (job.type === "cover_generation")
          return processCoverGenerationJob(ctx, job, {
            generateImage: async () => {
              db.query("INSERT INTO fixture_attempts VALUES (?, 'paid-cover')").run(job.id);
              return Buffer.from("fixture-image");
            },
            watermark: async (bytes) => Buffer.from(bytes)
          });
        if (job.type === "metadata_hydration") return processMetadataHydrationJob(ctx, job);
        if (job.type === "reconcile") return processReconcileJob(ctx, job);
        throw new Error(`Unsupported fixture domain ${job.type}`);
      }
      for (const phase of ["research", "publish"]) {
        await ctx.execution!.run(phase, () => {
          ctx.execution!.assertActive();
          db.transaction(() => {
            db.query("INSERT INTO fixture_attempts VALUES (?, ?)").run(job.id, phase);
            db.query("INSERT OR IGNORE INTO fixture_effects VALUES (?, ?)").run(job.id, phase);
          })();
        });
      }
      return "done";
    },
    onCheckpoint: async (jobId, name) => {
      if (name !== stopAt) return;
      appendFileSync(`${root}/events`, `${jobId}:${name}\n`);
      await new Promise(() => {});
    }
  }
);
while (repo.listExecutionJobs().some((job) => job.status !== "cancelled")) {
  await runtime.reconcile();
  await Bun.sleep(20);
}
await runtime.stop();
db.close();
