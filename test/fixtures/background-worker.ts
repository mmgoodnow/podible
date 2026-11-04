import { openDatabase } from "../../src/db";
import { bootBackgroundWorker } from "../../src/runtime/worker-bootstrap";
import { workerData } from "node:worker_threads";

await bootBackgroundWorker({ startupMaintenance: false, execution: {
  leaseMs: 400, retryDelayMs: 20,
  async processJob(ctx, job) {
    const payload = JSON.parse(job.payload_json ?? "{}");
    const generation = ctx.repo.jobExecutionGeneration(job.id);
    if (payload.op === "crash" && !ctx.repo.getJsonState(`crashed:${job.id}`)) {
      await ctx.execution!.run("before-crash", () => ctx.repo.setJsonState(`checkpoint:${job.id}`, true));
      ctx.repo.setJsonState(`crashed:${job.id}`, true);
      process.exit(7);
    }
    await ctx.execution!.run("fixture-work", async () => {
      ctx.repo.setJsonState(`entered:${job.id}`, true);
      if (payload.op === "cpu" || payload.op === "lock") {
        const db = openDatabase(workerData.databasePath);
        try {
          const work = () => {
            const end = Date.now() + Number(payload.ms ?? 1000);
            while (Date.now() < end) { /* Deliberately block only the worker's JS thread. */ }
          };
          if (payload.op === "lock") db.transaction(() => {
            db.query("UPDATE app_state SET updated_at=updated_at WHERE key=?").run(`entered:${job.id}`);
            work();
          }).immediate();
          else work();
        } finally { db.close(); }
      } else if (payload.op === "wait") await Bun.sleep(Number(payload.ms ?? 5000));
      ctx.execution!.assertActive();
      ctx.repo.setJsonState(`finished:${job.id}:${generation}`, true);
      ctx.repo.setJsonState(`finished:${job.id}`, true);
    });
    return "done";
  },
} });
