import { expect, test } from "bun:test";
import type { TorrentSnapshot } from "../../src/media/torrent-client";
import { waitForTorrentCompletion } from "../../src/worker/media-completion";
import type { WorkerContext } from "../../src/worker/context";

test("an initial check queues one deferred start before waiting for download completion", async () => {
  const ref = { clientId: "fixture", nativeId: "a".repeat(40) };
  const snapshot: TorrentSnapshot = {
    ref, location: { rootId: "tv", relativePath: "fixture" }, state: "checking", complete: false,
    bytesDone: 0, sizeBytes: 100, downloadBytesPerSecond: 0, observedAt: new Date().toISOString(), message: null,
  };
  const steps = new Map<string, unknown>();
  let started = 0, inspections = 0, waits = 0;
  const ctx: Pick<WorkerContext, "execution"> = { execution: {
    key: "fixture", assertActive() {},
    async sleep() { throw new Error("Completion must not poll"); },
    armCompletion() {}, clearCompletion() {},
    async run<T>(name: string, effect: () => T | Promise<T>): Promise<T> {
      if (!steps.has(name)) steps.set(name, await effect());
      return steps.get(name) as T;
    },
    async waitForCompletion() {
      expect(started).toBe(1);
      if (++waits === 1) throw new Error("Simulated process stop while waiting");
      return true;
    },
  } };
  const run = () => waitForTorrentCompletion(ctx, "fixture", ref, 10_000, async () => {
    return ++inspections === 1 ? snapshot : { ...snapshot, state: "seeding", bytesDone: 100, complete: true };
  }, seen => seen.complete, async () => { throw new Error("Immediate resume cannot run during verification"); },
  async () => { started++; });

  await expect(run()).rejects.toThrow("Simulated process stop");
  expect(await run()).toBe(true);
  expect(started).toBe(1);
  expect(inspections).toBe(2);
});
