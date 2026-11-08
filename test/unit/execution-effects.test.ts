import { expect, test } from "bun:test";
import { assertWorkflowBoundary, currentEffects, effectCheckpoint, effectOperationId, executionDiagnostics, withExecutionEffects } from "../../src/runtime/execution-effects";

const input = { jobId: 1, runId: "fixture", stepName: "work", signal: new AbortController().signal };
test("effect scope rejects workflow nesting, propagates cancellation, and clears after exit", async () => {
  assertWorkflowBoundary();
  await withExecutionEffects(input, async () => {
    expect(() => assertWorkflowBoundary()).toThrow("outside effect handlers");
    expect(currentEffects()?.signal.aborted).toBe(false);
    effectCheckpoint();
  });
  expect(currentEffects()).toBeUndefined(); expect(executionDiagnostics()).toEqual([]);
});

test("replayed steps keep operation identities but sibling invocations get distinct identities", async () => {
  const get = () => withExecutionEffects(input, async () => [effectOperationId("use"), effectOperationId("use")]);
  const first = await get();
  expect(first).toEqual(await get()); expect(first[0]).not.toBe(first[1]);
  expect(effectOperationId("outside")).toBeUndefined();
});

test("deadline aborts cooperatively without settling a still-running callback", async () => {
  let release!: () => void, exited = false;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const pending = withExecutionEffects({ ...input, timeoutMs: 5 }, async () => { await hold; exited = true; });
  const caught = pending.catch(error => error);
  await Bun.sleep(15);
  expect(exited).toBe(false);
  expect(executionDiagnostics()[0]?.aborted).toBe(true);
  release(); expect((await caught).name).toBe("StepDeadlineError");
  expect(executionDiagnostics()).toEqual([]);
});

test("watchdog distinguishes checkpoints from lease heartbeats and never releases active effects", async () => {
  await withExecutionEffects(input, async () => {
    const first = executionDiagnostics()[0]!;
    expect(executionDiagnostics(first.progressAt + 11, 10)[0]?.stalled).toBe(true);
    expect(currentEffects()?.signal.aborted).toBe(false);
    effectCheckpoint();
    expect(executionDiagnostics()[0]?.stalled).toBe(false);
  });
  expect(executionDiagnostics()).toEqual([]);
});
