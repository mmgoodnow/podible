import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { createEngine, createWorkflow } from "reflow-ts";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { z } from "zod";

const filename = process.argv[2];
assert(filename);
const effects: string[] = [];
const build = (expandedInventory: boolean) => {
  const storage = new SQLiteStorage(filename);
  const workflow = createWorkflow({ name: "reuse-inventory", input: z.null() }, async ({ step }) => {
    const extra = expandedInventory
      ? await step.run("new-inventory-page", async () => {
        // Let the progress check requested by awaiting the step see it running.
        await setImmediate();
        await setImmediate();
        effects.push("new-inventory-page");
        return ["new-file"];
      }) : [];
    const sources = await step.run("cached-torrent-inventory", async () => {
      effects.push("cached-torrent-inventory");
      return ["retained-file"];
    });
    return step.run("plan", async () => {
      if (!expandedInventory) throw new Error("The first worker could not assemble the plan");
      effects.push("plan");
      return [...extra, ...sources];
    });
  });
  const engine = createEngine({ storage, workflows: [workflow] });
  return { engine, stop: async () => { await engine.stop(); await storage.close(); } };
};

try {
  const first = build(false);
  await first.engine.start(10);
  const { id } = await first.engine.enqueue("reuse-inventory", null);
  const failed = await first.engine.waitForRun(id, { signal: AbortSignal.timeout(5_000) });
  assert.equal(failed.status, "failed");
  await first.stop();

  const retry = build(true);
  assert(await retry.engine.resume(id));
  await retry.engine.start(10);
  const result = await retry.engine.waitForRun(id, { signal: AbortSignal.timeout(5_000) }).catch(async cause => {
    const state = await retry.engine.getRunStatus(id);
    throw new Error("Replay did not complete: " + JSON.stringify({
      status: state?.run.status, steps: state?.steps.map(step => [step.name, step.status]),
    }), { cause });
  });
  assert.equal(result.id, id);
  assert.equal(result.status, "completed");
  assert.deepEqual(result.output, ["new-file", "retained-file"]);
  assert.deepEqual(effects, ["cached-torrent-inventory", "new-inventory-page", "plan"]);
  await retry.stop();
} catch (error) {
  console.error(error);
  process.exit(1);
}
