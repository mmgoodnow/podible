import type { Database } from "bun:sqlite";

type ThenableResult = { then: (...args: never[]) => unknown };
export type SyncTransactionCallback<T> = () => T & (Extract<T, ThenableResult> extends never ? unknown : never);

const message = "Database transaction callbacks must be synchronous; async functions and thenables are not allowed";

function assertSynchronousFunction(fn: () => unknown): void {
  if (["[object AsyncFunction]", "[object AsyncGeneratorFunction]"].includes(Object.prototype.toString.call(fn)))
    throw new Error(message);
}

function assertSynchronousResult(value: unknown): void {
  if (value !== null && (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function") throw new Error(message);
}

/** Short synchronous DB work only. IMMEDIATE prevents WAL read-to-write upgrade races;
 * Bun uses savepoints when this is nested in an existing transaction. */
export function immediateTransaction<T>(db: Database, beforeWrite: () => void, action: SyncTransactionCallback<T>): T {
  assertSynchronousFunction(beforeWrite);
  assertSynchronousFunction(action);
  return db.transaction(() => {
    assertSynchronousResult(beforeWrite());
    const result = action();
    assertSynchronousResult(result);
    return result;
  }).immediate();
}
