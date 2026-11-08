import { expect, test } from "bun:test";
import { LockOrderError, mutexDiagnostics, withKeyedMutex } from "../../src/runtime/keyed-mutex";

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
