import { expect, test } from "bun:test";
import { movieUpstreamStep } from "../../src/movies/upstream";
import { MovieIndexerError } from "../../src/movies/offers";
import type { WorkerContext } from "../../src/worker/context";

test("movie upstream 429 waits are durable and retry only the rate-limited request", async () => {
  const saved = new Map<string, unknown>();
  const sleeps: number[] = [];
  let calls = 0;
  const execution: NonNullable<WorkerContext["execution"]> = {
    key: "test",
    assertActive() {},
    sleep: async (_name, ms) => { sleeps.push(ms); },
    run: async <T>(name: string, action: () => T | Promise<T>) => {
      if (saved.has(name)) return saved.get(name) as T;
      const result = await action();
      saved.set(name, result);
      return result;
    },
  };
  const ctx = { execution } as WorkerContext;
  const action = async () => {
    calls++;
    if (calls === 1) throw new MovieIndexerError("HTTP 429", 1_800_000);
    return "result";
  };
  expect(await movieUpstreamStep(ctx, "search", action)).toBe("result");
  expect(sleeps).toEqual([1_800_000]);
  expect(await movieUpstreamStep(ctx, "search", action)).toBe("result");
  expect(calls).toBe(2);
});
