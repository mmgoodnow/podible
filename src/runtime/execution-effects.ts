import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";

type Effects = {
  jobId: number; runId: string; stepName: string; signal: AbortSignal;
  progress(): void; sequence: number;
};
const scope = new AsyncLocalStorage<Effects>();
const active = new Set<{ effects: Effects; startedAt: number; progressAt: number; controller: AbortController }>();

export class StepDeadlineError extends Error {
  constructor(message: string) { super(message); this.name = "StepDeadlineError"; }
}

export function currentEffects(): Effects | undefined { return scope.getStore(); }
export function assertWorkflowBoundary() {
  if (scope.getStore()) throw new Error("Workflow steps and durable sleeps must be called outside effect handlers");
}
export function effectCheckpoint() {
  const effects = scope.getStore();
  effects?.signal.throwIfAborted();
  effects?.progress();
}

/** Stable within a step replay, independent for siblings within the step. */
export function effectOperationId(namespace: string): string | undefined {
  const effects = scope.getStore();
  if (!effects) return;
  const digest = createHash("sha256").update(JSON.stringify([effects.runId, effects.stepName, namespace, effects.sequence++])).digest("hex");
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-4${digest.slice(13, 16)}-8${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

/** Abort is cooperative: do not race the callback or surrender its resources early. */
export async function withExecutionEffects<T>(input: {
  jobId: number; runId: string; stepName: string; signal: AbortSignal; timeoutMs?: number;
}, handler: () => T | Promise<T>): Promise<T> {
  const controller = new AbortController();
  const effects: Effects = { ...input, sequence: 0,
    signal: AbortSignal.any([input.signal, controller.signal]), progress: () => { record.progressAt = Date.now(); } };
  const record = { effects, startedAt: Date.now(), progressAt: Date.now(), controller };
  active.add(record);
  const timer = input.timeoutMs === undefined ? undefined : setTimeout(() =>
    controller.abort(new StepDeadlineError("Step exceeded its execution deadline")), input.timeoutMs);
  try {
    effects.signal.throwIfAborted();
    const result = await scope.run(effects, handler);
    effects.signal.throwIfAborted();
    return result;
  } finally {
    clearTimeout(timer);
    active.delete(record);
    controller.abort(new Error("Effect handler has exited"));
  }
}

/** Progress age, not merely an engine lease heartbeat. Unknown external effects stay fenced. */
export function executionDiagnostics(now = Date.now(), stallMs = 5 * 60_000) {
  return [...active].map(({ effects, startedAt, progressAt }) => {
    return { jobId: effects.jobId, runId: effects.runId, stepName: effects.stepName,
      startedAt, progressAt, aborted: effects.signal.aborted, stalled: now - progressAt >= stallMs };
  });
}
