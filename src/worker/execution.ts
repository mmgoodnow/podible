import { randomUUID } from "node:crypto";
import { createEngine, createWorkflow, LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { z } from "zod";

import { processJob } from "./dispatcher";
import { handleJobFailure } from "./failure";
import { workerLog, type WorkerContext } from "./context";
import { jobLockKeys, lockSetsConflict } from "./locks";

export const EXECUTION_JOB_TYPES = ["acquire", "download", "import"] as const;

/** Job rows are an outbox and Activity projection; Reflow owns execution. */
export async function createJobExecutionRuntime(
  ctx: WorkerContext,
  databasePath: string,
  options: {
    leaseMs?: number;
    onCheckpoint?: (jobId: number, stepName: string) => Promise<void>;
  } = {}
) {
  const leaseMs = options.leaseMs ?? 30_000;
  const activeLocks = (ctx.activeLocks ??= new Set<string>());
  const storage = new SQLiteStorage(databasePath);
  const workflows = EXECUTION_JOB_TYPES.map((type) =>
    createWorkflow(
      {
        name: `media-${type}-v1` as const,
        concurrency: type === "acquire" ? 1 : type === "import" ? 2 : 16,
        input: z.object({ jobId: z.number().int().positive(), generation: z.number().int().nonnegative().default(0) }),
        maxOperations: 200_000
      },
      async ({ input, step, runId }) => {
        if (ctx.repo.jobExecutionGeneration(input.jobId) !== input.generation) {
          await engine.cancel(runId);
          throw new RunCancelledError(runId);
        }
        const token = randomUUID();
        ctx.repo.attachJobExecution(input.jobId, runId, input.generation);
        if (!ctx.repo.ownJobExecution(input.jobId, runId, token)) throw new RunCancelledError(runId);
        const signals = new Set<AbortSignal>();
        const assertActive = () => {
          for (const signal of signals) signal.throwIfAborted();
          const owner = ctx.repo.getJobExecution(input.jobId);
          if (owner?.run_id !== runId || owner.owner_token !== token) throw new LeaseExpiredError(runId);
          const job = ctx.repo.getJob(input.jobId);
          if (!job || job.status === "cancelled") throw new RunCancelledError(runId);
        };
        const execution: NonNullable<WorkerContext["execution"]> = {
          key: runId,
          assertActive,
          run: <T>(name: string, handler: () => T | Promise<T>): Promise<T> =>
            step.run<unknown>(name, {
              retry: { maxAttempts: 3, backoff: "exponential", initialDelay: 1000 },
              handler: async ({ signal }) => {
                signals.add(signal);
                const current = ctx.repo.getJob(input.jobId);
                const keys = current ? jobLockKeys(ctx.repo, current) : [];
                let locked = false;
                try {
                  assertActive();
                  while (lockSetsConflict(activeLocks, keys)) {
                    assertActive();
                    await Bun.sleep(30);
                  }
                  for (const key of keys) activeLocks.add(key);
                  locked = true;
                  return await handler();
                } finally {
                  if (locked) for (const key of keys) activeLocks.delete(key);
                  signals.delete(signal);
                }
              }
            }) as Promise<T>,
          sleep: (name, milliseconds) => step.sleep(name, milliseconds)
        };
        const job = await execution.run("job-input", () => ctx.repo.getJob(input.jobId)!);
        const ownedRepo = ctx.repo.forExecution(assertActive);
        await processJob({ ...ctx, repo: ownedRepo, execution }, job);
        await execution.run("operation-finish", () => {
          const current = ownedRepo.getJob(job.id);
          if (current?.status === "running") ownedRepo.markJobSucceeded(job.id);
        });
        return { jobId: job.id };
      }
    )
  );
  const engine = createEngine({
    storage,
    workflows,
    concurrency: 20,
    runLeaseDurationMs: leaseMs,
    heartbeatIntervalMs: Math.max(20, Math.floor(leaseMs / 3)),
    hooks: {
      onStepStart: ({ runId, stepName }) => workerLog(ctx, `[execution] run=${runId} step=${stepName}`),
      onStepComplete: async ({ runId, stepName }) => {
        if (options.onCheckpoint) {
          const status = await engine.getRunStatus(runId);
          const input = status?.run.input as { jobId: number } | undefined;
          if (input) await options.onCheckpoint(input.jobId, stepName);
        }
      }
    }
  });
  await engine.start(300);
  return {
    engine,
    async reconcile() {
      for (const job of ctx.repo.listExecutionJobs()) {
        const link = ctx.repo.getJobExecution(job.id);
        if (job.status === "cancelled") {
          if (link) await engine.cancel(link.run_id);
          continue;
        }
        if (job.status === "failed") continue;
        if (job.next_run_at && Date.parse(job.next_run_at) > Date.now()) continue;
        if (!link) {
          const name = `media-${job.type}-v1` as `media-${(typeof EXECUTION_JOB_TYPES)[number]}-v1`;
          const generation = ctx.repo.jobExecutionGeneration(job.id)!;
          const run = await engine.enqueue(
            name,
            { jobId: job.id, generation },
            { idempotencyKey: `job:${job.id}:${generation}` }
          );
          ctx.repo.attachJobExecution(job.id, run.id, generation);
          continue;
        }
        const status = await engine.getRunStatus(link.run_id);
        if (!status) throw new Error(`Missing execution ${link.run_id} for job ${job.id}`);
        if (status.run.status === "failed") {
          if (job.status === "running") {
            await handleJobFailure(ctx, job, new Error(status.run.error?.message ?? "Execution failed"), {
              runId: link.run_id,
              token: link.owner_token
            });
          } else {
            await engine.resume(link.run_id);
          }
        } else if (status.run.status === "cancelled" && job.status === "queued") {
          await engine.resume(link.run_id);
        } else if (status.run.status === "completed") {
          ctx.repo.transaction(() => {
            const current = ctx.repo.getJobExecution(job.id);
            if (current?.run_id === link.run_id && current.owner_token === link.owner_token)
              ctx.repo.markJobSucceeded(job.id);
          });
        }
      }
    },
    async stop() {
      await engine.stop();
      await storage.close();
    }
  };
}
