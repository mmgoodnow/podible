import { expect, test } from "bun:test";
import { RunCancelledError } from "reflow-ts";
import { executionRetryPolicy } from "../../src/worker/execution";
import { rethrowChapterExecutionError } from "../../src/library/chapter-execution";

test("long retry budgets have a bounded practical horizon instead of exponential days", () => {
  const normal = executionRetryPolicy(5);
  expect(normal).toEqual({ maxAttempts: 5, backoff: "exponential", initialDelay: 1000 });
  const long = executionRetryPolicy(20);
  expect(long.backoff).toBe("linear");
  const waits = Array.from({ length: long.maxAttempts - 1 }, (_, index) => long.initialDelay * (index + 1));
  expect(waits.reduce((total, delay) => total + delay, 0)).toBe(95 * 60_000);
  expect(executionRetryPolicy(20, 1).initialDelay).toBe(1);
});

test("chapter error handling propagates engine control flow even while the owner is active", () => {
  const control = new RunCancelledError("run");
  expect(() => rethrowChapterExecutionError({}, control)).toThrow(control);
  expect(() => rethrowChapterExecutionError({}, new Error("ordinary failure"))).not.toThrow();
});
