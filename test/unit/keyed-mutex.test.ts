import { expect, test } from "bun:test";
import { LockOrderError, mutexDiagnostics, withKeyedMutex } from "../../src/runtime/keyed-mutex";
import { withExecutionEffects } from "../../src/runtime/execution-effects";

function gate() {
  let release!: () => void;
  return { promise: new Promise<void>(resolve => { release = resolve; }), release: () => release() };
}

test("same-key recursion and cross-domain inversions fail instead of waiting forever", async () => {
  const registration = { kind: "registration", key: "recursive" } as const;
  await expect(withKeyedMutex(registration, () => withKeyedMutex(registration, async () => {}))).rejects.toBeInstanceOf(LockOrderError);
  await expect(withKeyedMutex({ kind: "publication", key: "view" }, () =>
    withKeyedMutex(registration, async () => {}))).rejects.toThrow("order inversion");
  await expect(withKeyedMutex({ kind: "storage", key: "z" }, () =>
    withKeyedMutex({ kind: "storage", key: "a" }, async () => {}))).rejects.toThrow("order inversion");
  expect(mutexDiagnostics()).toEqual([]);
});

test("legal nested acquisition and parallel siblings complete", async () => {
  const visited: string[] = [];
  await withKeyedMutex({ kind: "registration", key: "ordered" }, async () => {
    await Promise.all(["a", "b"].map(key => withKeyedMutex({ kind: "storage", key }, async () => {
      visited.push(key);
      await withKeyedMutex({ kind: "publication", key }, async () => { visited.push(`view:${key}`); });
    })));
  });
  expect(visited.sort()).toEqual(["a", "b", "view:a", "view:b"]);
  expect(mutexDiagnostics()).toEqual([]);
});

test("FIFO waiters survive predecessor failure while unrelated keys make progress", async () => {
  const hold = gate(), order: number[] = [];
  const resource = { kind: "storage", key: "fifo" } as const;
  const first = withKeyedMutex(resource, async () => { await hold.promise; throw new Error("fixture"); });
  const caught = first.catch(() => {});
  const second = withKeyedMutex(resource, async () => { order.push(2); });
  const third = withKeyedMutex(resource, async () => { order.push(3); });
  await withKeyedMutex({ kind: "storage", key: "independent" }, async () => { order.push(1); });
  expect(mutexDiagnostics().find(lock => lock.key === "fifo")?.waiters).toBe(2);
  hold.release(); await Promise.all([caught, second, third]);
  expect(order).toEqual([1, 2, 3]);
  expect(mutexDiagnostics()).toEqual([]);
});

test("aborting a waiter removes only that waiter, never the active owner", async () => {
  const hold = gate(), controller = new AbortController();
  const resource = { kind: "storage", key: "cancel" } as const;
  const first = withKeyedMutex(resource, () => hold.promise);
  let entered = false;
  const waiting = withKeyedMutex(resource, async () => { entered = true; }, controller.signal);
  controller.abort(new Error("cancel fixture"));
  await expect(waiting).rejects.toThrow("cancel fixture");
  expect(entered).toBe(false);
  expect(mutexDiagnostics().find(lock => lock.key === "cancel")?.waiters).toBe(0);
  hold.release(); await first;
  expect(mutexDiagnostics()).toEqual([]);
});

test("aborted non-cooperative callback retains ownership until it actually exits", async () => {
  const hold = gate(), controller = new AbortController();
  const resource = { kind: "storage", key: "non-cooperative" } as const;
  const first = withKeyedMutex(resource, () => hold.promise, controller.signal);
  controller.abort(); let entered = false;
  const second = withKeyedMutex(resource, async () => { entered = true; });
  await Bun.sleep(10); expect(entered).toBe(false);
  hold.release(); await Promise.all([first, second]);
  expect(entered).toBe(true);
  expect(mutexDiagnostics()).toEqual([]);
});

test("diagnostics capture effect owners, FIFO wait identities, handoff timestamps and cancellation", async () => {
  const firstGate = gate(), secondGate = gate(), enteredSecond = gate();
  const resource = { kind: "publication", key: "diagnostic-view" } as const;
  const firstController = new AbortController(), cancelled = new AbortController();
  const effect = (jobId: number, signal: AbortSignal, action: () => Promise<void>) => withExecutionEffects({
    jobId, runId: `run-${jobId}`, stepName: `publish:${jobId}`, signal
  }, action);
  const first = effect(1, firstController.signal, () => withKeyedMutex(resource, () => firstGate.promise));
  // The owner rejection is observed immediately, even though it cannot exit yet.
  const firstResult = first.catch(error => error);
  const rejected = effect(2, cancelled.signal, () => withKeyedMutex(resource, async () => {}));
  const rejectedResult = rejected.catch(error => error);
  const second = effect(3, new AbortController().signal, () => withKeyedMutex(resource, async () => {
    enteredSecond.release(); await secondGate.promise;
  }));
  try {
    const initial = mutexDiagnostics().find(lock => lock.key === resource.key)!;
    expect(initial.owner).toMatchObject({ jobId: 1, runId: "run-1", stepName: "publish:1", acquiredAt: initial.heldSince });
    expect(initial.owner.requestedAt).toBeLessThanOrEqual(initial.heldSince);
    expect(initial.waiting.map(waiter => waiter.jobId)).toEqual([2, 3]);
    expect(initial.waiting.every(waiter => waiter.requestedAt >= initial.owner.requestedAt)).toBe(true);
    initial.owner.jobId = 999;
    firstController.abort(new Error("owner cancelled")); cancelled.abort(new Error("waiter cancelled"));
    expect((await rejectedResult).message).toBe("waiter cancelled");
    const held = mutexDiagnostics().find(lock => lock.key === resource.key)!;
    expect(held.owner.jobId).toBe(1);
    expect(held.waiting.map(waiter => waiter.jobId)).toEqual([3]);
    expect(held.waiters).toBe(1);
    firstGate.release(); await enteredSecond.promise;
    const handed = mutexDiagnostics().find(lock => lock.key === resource.key)!;
    expect(handed.owner).toMatchObject({ jobId: 3, runId: "run-3", stepName: "publish:3", acquiredAt: handed.heldSince });
    expect(handed.owner.requestedAt).toBe(initial.waiting[1]!.requestedAt);
    expect(handed.heldSince).toBeGreaterThanOrEqual(initial.heldSince);
    expect(handed.waiters).toBe(0);
  } finally {
    firstGate.release(); secondGate.release(); await Promise.all([firstResult, rejectedResult, second]);
  }
  expect(mutexDiagnostics()).toEqual([]);
});
