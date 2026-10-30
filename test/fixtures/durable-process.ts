import { appendFileSync, existsSync } from "node:fs";
import { Database } from "bun:sqlite";
import { createEngine, createWorkflow } from "reflow-ts";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { z } from "zod";

const [root, stopAt = ""] = process.argv.slice(2);
if (!root) throw new Error("Missing fixture directory");
const domain = new Database(`${root}/domain.sqlite`);
domain.exec("CREATE TABLE IF NOT EXISTS effects (id TEXT PRIMARY KEY, value TEXT NOT NULL)");
domain.exec("CREATE TABLE IF NOT EXISTS attempts (id TEXT NOT NULL)");
function effect(id: string) {
  domain.query("INSERT INTO attempts VALUES (?)").run(id);
  domain.query("INSERT OR IGNORE INTO effects VALUES (?, ?)").run(id, id);
}
async function checkpoint(name: string) {
  appendFileSync(`${root}/events`, `${name}\n`);
  if (stopAt === name && !existsSync(`${root}/resume`)) {
    await new Promise(() => {});
  }
}
const workflow = createWorkflow({ name: "acceptance", input: z.object({ id: z.string() }) }, async ({ step }) => {
  const search = await step.run("search", async () => { effect("search"); return ["a", "b"]; });
  await checkpoint("after-search");
  const choice = await step.run("select", async () => { effect("select"); return search; });
  await checkpoint("after-select");
  for (const part of choice) {
    await step.run(`snatch:${part}`, async () => {
      effect(`torrent:${part}`);
      await checkpoint(`after-external:${part}`);
      return { part };
    });
    await step.run(`import:${part}`, async () => {
      effect(`asset:${part}`);
      await checkpoint(`after-publication:${part}`);
      return part;
    });
    await checkpoint(`after-part:${part}`);
  }
  return choice;
});
const storage = new SQLiteStorage(`${root}/execution.sqlite`);
const engine = createEngine({ storage, workflows: [workflow], runLeaseDurationMs: 200, heartbeatIntervalMs: 50 });
await engine.start(20);
const run = await engine.enqueue("acceptance", { id: "book" }, { idempotencyKey: "book" });
await Bun.write(`${root}/run-id`, run.id);
const result = await engine.waitForRun(run.id, { pollIntervalMs: 20 });
await engine.stop();
await storage.close();
domain.close();
if (result.status !== "completed") throw new Error(`Unexpected ${result.status}`);
