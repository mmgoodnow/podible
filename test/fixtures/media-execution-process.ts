import { appendFileSync, existsSync, promises as fs } from "node:fs";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { BooksRepo } from "../../src/repo";
import type { SyncTransactionCallback } from "../../src/db-transaction";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { connectBookTorrentFixture } from "../mocks/book-torrent";

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
  const directory = String(args[0]);
  if (directory.includes(`${root}/library/`) && !directory.includes("/.podible/"))
    await checkpoint("before-publication");
  return result;
}) as typeof fs.readdir;
const transaction = BooksRepo.prototype.transaction;
const transactionDepth = new WeakMap<BooksRepo, number>();
BooksRepo.prototype.transaction = function <T>(fn: SyncTransactionCallback<T>): T {
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
const bookTorrentClient = connectBookTorrentFixture(await fs.readFile(`${root}/book-torrent-url`, "utf8"));
const ensureAdded = bookTorrentClient.ensureAdded.bind(bookTorrentClient);
bookTorrentClient.ensureAdded = async (input) => {
  const ref = await ensureAdded(input);
  await checkpoint("external-snatch");
  return ref;
};
const modelProvider: ModelProvider = {
  getModel: () => ({
    getResponse: async (request) => {
      if (JSON.stringify(request).includes("primaryQuery")) {
        const input = typeof request.input === "string" ? request.input : request.input
          .flatMap(item => item.type === "message" && item.role === "user"
            ? typeof item.content === "string" ? [item.content] : item.content.flatMap(part => part.type === "input_text" ? [part.text] : [])
            : []).join("");
        return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
          content: [{ type: "output_text", text: JSON.stringify({ primaryQuery: JSON.parse(input).query, alternateQuery: null }) }] }] };
      }
      const importing = JSON.stringify(request).includes("selectedIndices");
      await checkpoint(importing ? "import-agent" : "search-agent");
      let selectedIndices: number[] = [];
      if (importing) {
        let prompt = "";
        if (typeof request.input === "string") prompt = request.input;
        else for (const item of request.input) {
          if (item.type !== "message" || item.role !== "user") continue;
          prompt += typeof item.content === "string" ? item.content : item.content
            .flatMap((content) => content.type === "input_text" ? [content.text] : []).join("\n");
        }
        const filesJson = /```json\n([\s\S]*?)\n```/.exec(prompt)?.[1];
        if (!filesJson) throw new Error("Missing fixture importable-file list");
        const files = JSON.parse(filesJson) as { index: number }[];
        selectedIndices = files.map((file) => file.index);
        if (!selectedIndices.length) throw new Error("Fixture has no importable files");
      }
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
                  selectedIndices,
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
    bookTorrentClient,
    onLog: (message) => appendFileSync(`${root}/worker.log`, `${message}\n`)
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
for (const job of repo.listRecentJobs(100)) {
  const execution = repo.getJobExecution(job.id);
  if (execution && job.status !== "succeeded") {
    const status = await runtime.engine.getRunStatus(execution.run_id);
    console.error(JSON.stringify({ jobId: job.id, runState: status?.run.status,
      steps: status?.steps.map(({ name, status, error, attempts }) => ({ name, status, error, attempts })) }));
  }
}
await runtime.stop();
db.close();
process.exit(1);
