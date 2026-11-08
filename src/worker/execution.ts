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
import { JOB_TYPES } from "../job-types";
import { periodicPrefix } from "./periodic";

export const EXECUTION_JOB_TYPES = JOB_TYPES;

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
  const reservations = new Set<{ runId: string; jobId: number; token: string; entered: boolean; release: () => void }>();
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
        concurrency: ["torrent_reuse", "torrent_reuse_feed", "torrent_metrics_collect", "torrent_stewardship_collect", "indexer_feed_sync", "movie_feed_match", "book_feed_match", "acquire", "movie_reconcile", "movie_shadow_sync", "movie_dual_write_sync", "tv_sync", "tv_reconcile", "tv_shadow_sync", "tv_feed_match"].includes(type)
          ? 1 : ["import", "chapter_analysis", "movie_acquire", "movie_shadow_evaluate", "tv_acquire", "tv_shadow_evaluate"].includes(type) ? 2 : 16,
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
        const queuedJob = ctx.repo.getJob(input.jobId)!;
        const prefix = periodicPrefix({ ...queuedJob, dedupe_key: ctx.repo.jobDedupeKey(queuedJob.id) });
        const newer = prefix ? ctx.repo.latestJobByDedupePrefix(queuedJob.type, prefix) : null;
        if (newer && newer.id > queuedJob.id && (await storage.getStepResults(runId)).every(s => s.name === "job-input"))
          return { jobId: queuedJob.id, coalescedInto: newer.id };
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
          run: async <T>(name: string, handler: () => T | Promise<T>): Promise<T> => {
            if (ctx.repo.isJobCancellationRequested(input.jobId)) {
              await cancelRequestedRun();
              throw new RunCancelledError(runId);
            }
            const current = ctx.repo.getJob(input.jobId);
            const keys = [...(current ? jobLockKeys(ctx.repo, current) : []), `execution:${token}`];
            // Wait outside the handler: a durable sleep releases Reflow's run slot.
            // Reserve synchronously before entering step.run so another callback cannot race admission.
            for (let wait = 0; lockSetsConflict(
              new Set([...activeLocks].filter(key => laneOwners.get(key)?.owner !== token)), keys
            ); wait++) {
              assertActive();
              await step.sleep(`podible:lane:${name}:${wait}`, 1000);
            }
            assertActive();
            let reserved = false;
            const reservation = { runId, jobId: input.jobId, token, entered: false, release: () => release() };
            const reserve = () => {
              if (reserved) return;
              if (lockSetsConflict(new Set([...activeLocks].filter(key => laneOwners.get(key)?.owner !== token)), keys))
                throw new Error("Resource lane became busy during step retry");
              for (const key of keys) {
                const lane = laneOwners.get(key);
                laneOwners.set(key, { owner: token, count: (lane?.count ?? 0) + 1 });
                activeLocks.add(key);
              }
              reserved = true;
              reservations.add(reservation);
            };
            reserve();
            const release = () => {
              if (!reserved) return;
              reserved = false;
              reservations.delete(reservation);
              for (const key of keys) {
                const lane = laneOwners.get(key)!;
                if (--lane.count === 0) { laneOwners.delete(key); activeLocks.delete(key); }
              }
            };
            try { return await step.run<unknown>(name, {
              retry: executionRetryPolicy(ctx.repo.getJob(input.jobId)?.max_attempts ?? 5, options.retryDelayMs),
              handler: async ({ signal }) => {
                reservation.entered = true;
                signals.add(signal);
                try {
                  assertActive();
                  reserve();
                  return await handler();
                } finally {
                  signals.delete(signal);
                  // Suspended/cancelled Reflow operations need not settle their
                  // caller promise. Release at the actual side-effect boundary.
                  release();
                }
              }
            }) as T; }
            finally { release(); }
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
      // Cancellation/lease loss can cut off stepStart before the handler ever
      // enters. Reflow may leave that operation promise pending, so neither
      // caller nor handler finally is sufficient for an unentered reservation.
      for (const reservation of reservations) {
        if (reservation.entered) continue;
        const owner = ctx.repo.getJobExecution(reservation.jobId);
        const run = await storage.getRun(reservation.runId);
        if (!reservation.entered && (!run || run.status !== "running" || owner?.owner_token !== reservation.token ||
          ctx.repo.isJobCancellationRequested(reservation.jobId))) reservation.release();
      }
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
      for (const reservation of reservations) if (!reservation.entered) reservation.release();
      await engine.stop();
      commands.close();
      await storage.close();
    }
  };
}
