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
    sleep: async (_name, ms) => {
      sleeps.push(ms);
    },
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

test("durable upstream outages can retry beyond the source attempt budget without producing no-results", async () => {
  let calls = 0;
  let sleeps = 0;
  const execution: NonNullable<WorkerContext["execution"]> = {
    key: "long-outage",
    assertActive() {},
    run: async <T>(_name: string, action: () => T | Promise<T>) => action(),
    sleep: async () => {
      sleeps++;
    },
  };
  const ctx = { execution } as WorkerContext;
  const result = await movieUpstreamStep(ctx, "search", async () => {
    calls++;
    if (calls <= 10)
      throw new MovieIndexerError("Temporarily unavailable", 60_000);
    return "recovered";
  });
  expect(result).toBe("recovered");
  expect(calls).toBe(11);
  expect(sleeps).toBe(10);
});
