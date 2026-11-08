import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";

type Effects = {
  jobId: number; runId: string; stepName: string; signal: AbortSignal;
  progress(): void; phase(name: string): void; sequence: number;
};
type Phase = { name: string; startedAt: number; finishedAt?: number };
const scope = new AsyncLocalStorage<Effects>();
const active = new Set<{ effects: Effects; startedAt: number; progressAt: number; controller: AbortController; phases: Phase[] }>();

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

/** Code-defined labels only: never put URLs, credentials or domain payloads here. */
export function effectPhase(name: string) {
  if (!/^[a-z][a-z0-9:-]{0,79}$/.test(name)) throw new Error("Invalid execution phase label");
  const effects = scope.getStore();
  effects?.signal.throwIfAborted();
  effects?.phase(name);
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
    signal: AbortSignal.any([input.signal, controller.signal]), progress: () => { record.progressAt = Date.now(); },
    phase: name => {
      const now = Date.now(), previous = record.phases.at(-1);
      if (previous) previous.finishedAt = now;
      record.phases.push({ name, startedAt: now });
      if (record.phases.length > 12) record.phases.shift();
      record.progressAt = now;
    } };
  const record = { effects, startedAt: Date.now(), progressAt: Date.now(), controller, phases: [] as Phase[] };
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
  return [...active].map(({ effects, startedAt, progressAt, phases }) => {
    return { jobId: effects.jobId, runId: effects.runId, stepName: effects.stepName,
      startedAt, progressAt, phases: phases.map(phase => ({ ...phase })),
      aborted: effects.signal.aborted, stalled: now - progressAt >= stallMs };
  });
}
