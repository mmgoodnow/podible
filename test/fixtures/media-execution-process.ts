import { appendFileSync, existsSync, promises as fs } from "node:fs";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";

const [root, boundary = ""] = process.argv.slice(2);
if (!root) throw new Error("Missing fixture root");
const db = new Database(`${root}/domain.sqlite`);
db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
const repo = new BooksRepo(db);
async function checkpoint(name: string) {
  appendFileSync(`${root}/events`, `${name}\n`);
  while (name === boundary && !existsSync(`${root}/resume`)) await Bun.sleep(20);
}
const readdir = fs.readdir.bind(fs);
fs.readdir = (async (...args: Parameters<typeof fs.readdir>) => {
  const result = await readdir(...args);
  if (String(args[0]).includes(`${root}/library/`)) await checkpoint("before-publication");
  return result;
}) as typeof fs.readdir;
const transaction = BooksRepo.prototype.transaction;
const transactionDepth = new WeakMap<BooksRepo, number>();
BooksRepo.prototype.transaction = function <T>(fn: () => T): T {
  const depth = (transactionDepth.get(this) ?? 0) + 1;
  transactionDepth.set(this, depth);
  let value: T;
  try {
    value = transaction.call(this, fn) as T;
  } finally {
    transactionDepth.set(this, depth - 1);
  }
  if (depth === 1 && value && typeof value === "object" && "mime" in value && "source_release_id" in value) {
    appendFileSync(`${root}/events`, "asset-publication\n");
    if (boundary === "asset-publication" && !existsSync(`${root}/resume`)) process.kill(process.pid, "SIGSTOP");
  }
  return value;
};
const originalFetch = globalThis.fetch;
globalThis.fetch = (async (...args: Parameters<typeof fetch>) => {
  const response = await originalFetch(...args);
  if (String(args[1]?.body).includes("<methodName>load.raw_start</methodName>")) await checkpoint("external-snatch");
  return response;
}) as typeof fetch;
const modelProvider: ModelProvider = {
  getModel: () => ({
    getResponse: async (request) => {
      await checkpoint(JSON.stringify(request).includes("selectedIndices") ? "import-agent" : "search-agent");
      return {
        usage: new Usage(),
        output: [
          {
            type: "message",
            role: "assistant",
            status: "completed",
            content: [
              {
                type: "output_text",
                text: JSON.stringify({
                  selections: [{ manifestation: { label: "Two parts", editionNote: null }, parts: [0, 1] }],
                  selectedIndices: [0],
                  confidence: 0.99,
                  reason: "Fixture selection"
                })
              }
            ]
          }
        ]
      };
    },
    getStreamedResponse: async function* () {
      throw new Error("Streaming not used");
    }
  })
};
const runtime = await createJobExecutionRuntime(
  {
    repo,
    getSettings: () => repo.getSettings(),
    modelProvider,
    onLog: () => {}
  },
  `${root}/execution.sqlite`,
  {
    leaseMs: 300,
    onCheckpoint: async (_jobId, stepName) => checkpoint(stepName)
  }
);
const deadline = Date.now() + 12_000;
while (Date.now() < deadline) {
  await runtime.reconcile();
  const jobs = repo.listRecentJobs(100);
  const counts = await runtime.engine.countRuns();
  if (jobs.length === 1 && jobs[0]!.status === "cancelled" && counts.cancelled === 1) {
    await runtime.stop();
    db.close();
    process.exit(0);
  }
  if (
    jobs.length >= 5 &&
    jobs.every((job) => job.status === "succeeded" || job.status === "cancelled") &&
    counts.running === 0 &&
    counts.pending === 0
  ) {
    await runtime.stop();
    db.close();
    process.exit(0);
  }
  await Bun.sleep(30);
}
console.error(JSON.stringify(repo.listRecentJobs(100)));
await runtime.stop();
db.close();
process.exit(1);
