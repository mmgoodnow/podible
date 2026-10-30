import { randomUUID } from "node:crypto";
import { createEngine, createWorkflow, LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { z } from "zod";
import { Database } from "bun:sqlite";

import { processJob } from "./dispatcher";
import { notifyJobFailure } from "./failure";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { archiveLegacyJob, importLegacyJob, jobJournalReader, JournalCommands } from "./job-journal";
import { workerLog, type WorkerContext } from "./context";
import { jobLockKeys, lockSetsConflict } from "./locks";

export const EXECUTION_JOB_TYPES = [
  "acquire",
  "download",
  "import",
  "reconcile",
  "full_library_refresh",
  "chapter_analysis",
  "cover_generation",
  "metadata_hydration"
] as const;

export function executionRetryPolicy(maxAttempts: number, initialDelay?: number) {
  const longBudget = maxAttempts > 8;
  return {
    maxAttempts,
    backoff: longBudget ? ("linear" as const) : ("exponential" as const),
    initialDelay: initialDelay ?? (longBudget ? 30_000 : 1_000)
  };
}

/** Job rows are an outbox and Activity projection; Reflow owns execution. */
export async function createJobExecutionRuntime(
  ctx: WorkerContext,
  databasePath: string,
  options: {
    leaseMs?: number;
    retryDelayMs?: number;
    processJob?: (ctx: WorkerContext, job: import("../app-types").JobRow) => Promise<"done" | void>;
    afterHistoryCopy?: () => Promise<void>;
    onCheckpoint?: (jobId: number, stepName: string) => Promise<void>;
  } = {}
) {
  const leaseMs = options.leaseMs ?? 30_000;
  let stopping = false;
  const activeLocks = (ctx.activeLocks ??= new Set<string>());
  const laneOwners = new Map<string, { owner: string; count: number }>();
  if (databasePath === ":memory:") {
    const directory = mkdtempSync(path.join(os.tmpdir(), "podible-execution-"));
    databasePath = path.join(directory, "execution.sqlite");
    process.once("exit", () => rmSync(directory, { recursive: true, force: true }));
  }
  const storage = new SQLiteStorage(databasePath);
  await storage.initialize();
  const namespace = ctx.repo.jobJournalNamespace();
  const commands = new JournalCommands(databasePath);
  try {
    for (const job of ctx.repo.legacyJobRows()) {
      const link = ctx.repo.getJobExecution(job.id);
      if (link) {
        const run = await storage.getRun(link.run_id);
        if (!run) throw new Error(`Missing execution ${link.run_id} for job ${job.id}`);
        archiveLegacyJob(databasePath, link.run_id, job);
        const generation = ctx.repo.jobExecutionGeneration(job.id)!;
        // Before consolidation these mismatches represented unapplied control
        // requests, not stale display state. Preserve them before dropping it.
        const kind =
          job.status === "cancelled" && !["completed", "failed", "cancelled"].includes(run.status)
            ? "cancel"
            : job.status === "queued" && ["failed", "cancelled"].includes(run.status)
              ? "retry"
              : null;
        if (kind)
          commands.apply(
            `${namespace}:migration:${job.id}:${generation}:${kind}`,
            {
              id: 0,
              job_id: job.id,
              kind,
              generation,
              run_id: link.run_id,
              payload_json: JSON.stringify({ reason: job.error }),
              created_at: job.updated_at
            },
            link.run_id
          );
      } else {
        const generation = ctx.repo.jobExecutionGeneration(job.id)!;
        const run = await importLegacyJob(
          storage,
          job,
          generation,
          databasePath,
          !(EXECUTION_JOB_TYPES as readonly string[]).includes(job.type)
        );
        ctx.repo.attachJobExecution(job.id, run.id, generation);
      }
    }
    await options.afterHistoryCopy?.();
    const checkpoint = new Database(databasePath);
    try {
      checkpoint.exec("PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL");
      const result = checkpoint.query("PRAGMA wal_checkpoint(FULL)").get() as { busy: number };
      if (result.busy) throw new Error("Journal history could not be durably checkpointed");
    } finally {
      checkpoint.close();
    }
    ctx.repo.finishJobJournalMigration();
    ctx.repo.bindJobJournalReader(jobJournalReader(databasePath));
  } catch (error) {
    commands.close();
    await storage.close();
    throw error;
  }
  const workflows = EXECUTION_JOB_TYPES.map((type) =>
    createWorkflow(
      {
        name: `media-${type}-v1` as const,
        concurrency: type === "acquire" ? 1 : type === "import" || type === "chapter_analysis" ? 2 : 16,
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
        const cancelRequestedRun = async () => {
          await engine.cancel(runId);
          const command = ctx.repo
            .listJobCommands()
            .find(
              (command) =>
                command.kind === "cancel" && command.job_id === input.jobId && command.generation === input.generation
            );
          if (command) {
            commands.apply(`${namespace}:command:${command.id}`, command, runId);
            ctx.repo.acknowledgeJobCommand(command.id);
          }
        };
        const assertActive = () => {
          if (stopping) throw new LeaseExpiredError(runId);
          for (const signal of signals) signal.throwIfAborted();
          if (ctx.repo.isJobCancellationRequested(input.jobId)) {
            void cancelRequestedRun();
            throw new RunCancelledError(runId);
          }
          const owner = ctx.repo.getJobExecution(input.jobId);
          if (owner?.run_id !== runId || owner.owner_token !== token) throw new LeaseExpiredError(runId);
          const job = ctx.repo.getJob(input.jobId);
          if (!job || job.status === "cancelled" || ctx.repo.isJobCancellationRequested(input.jobId))
            throw new RunCancelledError(runId);
        };
        const execution: NonNullable<WorkerContext["execution"]> = {
          key: runId,
          assertActive,
          run: <T>(name: string, handler: () => T | Promise<T>): Promise<T> => {
            if (ctx.repo.isJobCancellationRequested(input.jobId)) {
              return cancelRequestedRun().then(() => {
                throw new RunCancelledError(runId);
              });
            }
            return step.run<unknown>(name, {
              retry: executionRetryPolicy(ctx.repo.getJob(input.jobId)?.max_attempts ?? 5, options.retryDelayMs),
              handler: async ({ signal }) => {
                signals.add(signal);
                const current = ctx.repo.getJob(input.jobId);
                // The execution key also makes jobs without domain lanes visible
                // to an exclusive filesystem refresh. Sibling callbacks share lanes.
                const keys = [...(current ? jobLockKeys(ctx.repo, current) : []), `execution:${token}`];
                let locked = false;
                try {
                  assertActive();
                  while (
                    lockSetsConflict(
                      new Set([...activeLocks].filter((key) => laneOwners.get(key)?.owner !== token)),
                      keys
                    )
                  ) {
                    assertActive();
                    await Bun.sleep(30);
                  }
                  for (const key of keys) {
                    const lane = laneOwners.get(key);
                    laneOwners.set(key, { owner: token, count: (lane?.count ?? 0) + 1 });
                    activeLocks.add(key);
                  }
                  locked = true;
                  return await handler();
                } finally {
                  if (locked)
                    for (const key of keys) {
                      const lane = laneOwners.get(key)!;
                      if (--lane.count === 0) {
                        laneOwners.delete(key);
                        activeLocks.delete(key);
                      }
                    }
                  signals.delete(signal);
                }
              }
            }) as Promise<T>;
          },
          sleep: (name, milliseconds) => step.sleep(name, milliseconds)
        };
        const job = await execution.run("job-input", () => ctx.repo.getJob(input.jobId)!);
        const ownedRepo = ctx.repo.forExecution(assertActive);
        const result = await (options.processJob ?? processJob)({ ...ctx, repo: ownedRepo, execution }, job);
        if (result !== "done" && result !== undefined) throw new Error(`Job ${job.id} returned an invalid result`);
        await execution.run("operation-finish", () => execution.assertActive());
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
      onRunFailed: async ({ runId, error }) => {
        const run = await storage.getRun(runId);
        const input = run?.input as { jobId: number } | undefined;
        const job = input ? ctx.repo.getJob(input.jobId) : null;
        if (job) await notifyJobFailure(ctx, job, error);
      },
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
      // Fail closed on broken linkage, including terminal jobs; never recreate
      // a lost journal from domain input and accidentally replay its effects.
      ctx.repo.listExecutionJobs();
      for (const command of ctx.repo.listJobCommands()) {
        const job = ctx.repo.getJob(command.job_id);
        if (!job) {
          ctx.repo.acknowledgeJobCommand(command.id);
          continue;
        }
        const generation = ctx.repo.jobExecutionGeneration(job.id);
        const obsolete = command.generation !== generation;
        if (obsolete && command.kind === "enqueue") {
          ctx.repo.acknowledgeJobCommand(command.id);
          continue;
        }
        let runId = command.run_id ?? (!obsolete ? ctx.repo.getJobExecution(job.id)?.run_id : undefined);
        if (obsolete && command.kind === "cancel" && !runId) {
          ctx.repo.acknowledgeJobCommand(command.id);
          continue;
        }
        if (command.kind === "enqueue") {
          const key = `${namespace}:enqueue:${command.id}`;
          const payload =
            (command.payload_json
              ? (JSON.parse(command.payload_json) as { notBefore?: string | null } | null)
              : null) ?? {};
          const now = Date.now();
          const unsupported = !(EXECUTION_JOB_TYPES as readonly string[]).includes(job.type);
          const result = await storage.createRun({
            id: randomUUID(),
            workflow: `media-${job.type}-v1`,
            input: { jobId: job.id, generation: command.generation },
            idempotencyKey: key,
            status: unsupported
              ? "failed"
              : payload.notBefore && Date.parse(payload.notBefore) > now
                ? "sleeping"
                : "pending",
            createdAt: Date.parse(command.created_at),
            updatedAt: now,
            startedAt: null,
            finishedAt: unsupported ? now : null,
            output: null,
            error: unsupported
              ? { stepName: "enqueue", message: `Unsupported job type: ${job.type}`, code: null }
              : null,
            wakeAt: payload.notBefore ? Date.parse(payload.notBefore) : null,
            waitingFor: null
          });
          runId = result.run.id;
          ctx.repo.attachJobExecution(job.id, runId, command.generation);
        }
        if (!runId || !(await storage.getRun(runId)))
          throw new Error(`Missing execution for command ${command.id}, job ${job.id}`);
        // Also notify the live engine so an in-process callback gets aborted.
        if (command.kind === "cancel") await engine.cancel(runId);
        commands.apply(`${namespace}:command:${command.id}`, command, runId);
        ctx.repo.acknowledgeJobCommand(command.id);
      }
    },
    async stop() {
      stopping = true;
      await engine.stop();
      commands.close();
      await storage.close();
    }
  };
}
