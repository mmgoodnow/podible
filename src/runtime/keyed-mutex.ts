import { AsyncLocalStorage } from "node:async_hooks";
import { currentEffects } from "./execution-effects";

const ranks = { registration: 10, storage: 20, publication: 30 } as const;
type Resource = { kind: keyof typeof ranks; key: string };
type Held = Resource & { active: boolean };
type Identity = { jobId: number | null; runId: string | null; stepName: string | null };
type Request = Identity & { requestedAt: number };
type Waiter = Request & { grant: () => void; reject: (error: unknown) => void; signal?: AbortSignal; abort?: () => void };
const held = new AsyncLocalStorage<Held[]>();
const locks = new Map<string, { resource: Resource; since: number; owner: Request & { acquiredAt: number }; queue: Waiter[] }>();
const identity = (resource: Resource) => JSON.stringify([resource.kind, resource.key]);
const compare = (a: Resource, b: Resource) => ranks[a.kind] - ranks[b.kind] || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0);

export class LockOrderError extends Error {
  constructor(message: string) { super(message); this.name = "LockOrderError"; }
}

export function assertNoMutexHeld() {
  if (held.getStore()?.some(lock => lock.active))
    throw new LockOrderError("Workflow operations cannot wait while holding a mutex");
}

/** All callers share one order: registration -> storage -> publication, then key. */
export async function withKeyedMutex<T>(resource: Resource, action: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  const effects = currentEffects();
  const request: Request = { jobId: effects?.jobId ?? null, runId: effects?.runId ?? null,
    stepName: effects?.stepName ?? null, requestedAt: Date.now() };
  signal ??= effects?.signal;
  signal?.throwIfAborted();
  const ancestors = (held.getStore() ?? []).filter(lock => lock.active);
  for (const lock of ancestors) {
    if (identity(lock) === identity(resource)) throw new LockOrderError(`Recursive ${resource.kind} mutex acquisition`);
    if (compare(lock, resource) >= 0) throw new LockOrderError(`Mutex order inversion: ${lock.kind} -> ${resource.kind}`);
  }
  const id = identity(resource);
  let current = locks.get(id);
  if (!current) {
    const acquiredAt = Date.now();
    current = { resource, since: acquiredAt, owner: { ...request, acquiredAt }, queue: [] };
    locks.set(id, current);
  } else {
    const waitingOn = current;
    await new Promise<void>((grant, reject) => {
      const waiter: Waiter = { ...request, grant, reject, signal };
      waiter.abort = () => {
        const index = waitingOn.queue.indexOf(waiter);
        if (index >= 0) waitingOn.queue.splice(index, 1);
        reject(signal!.reason);
      };
      waitingOn.queue.push(waiter);
      signal?.addEventListener("abort", waiter.abort, { once: true });
      if (signal?.aborted) waiter.abort();
    });
  }
  const ownership: Held = { ...resource, active: true };
  try {
    signal?.throwIfAborted();
    return await held.run([...ancestors, ownership], action);
  } finally {
    // Cancellation never unlocks a callback that is still running.
    ownership.active = false;
    const next = current.queue.shift();
    if (next) {
      next.signal?.removeEventListener("abort", next.abort!);
      current.since = Date.now();
      current.owner = { jobId: next.jobId, runId: next.runId, stepName: next.stepName,
        requestedAt: next.requestedAt, acquiredAt: current.since };
      next.grant();
    } else locks.delete(id);
  }
}

/** Internal diagnostics, not a user-facing torrent or filesystem API. */
export function mutexDiagnostics() {
  return [...locks.values()].map(({ resource, since, owner, queue }) => ({ ...resource, heldSince: since, waiters: queue.length,
    owner: { ...owner }, waiting: queue.map(({ jobId, runId, stepName, requestedAt }) => ({ jobId, runId, stepName, requestedAt })) }));
}
