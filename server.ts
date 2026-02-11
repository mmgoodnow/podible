import { ensureDataDir, kindlingDbPath, port } from "./src/config";
import { openDatabase } from "./src/kindling/db";
import { createKindlingFetchHandler } from "./src/kindling/http";
import { KindlingRepo } from "./src/kindling/repo";
import { runWorker } from "./src/kindling/worker";

const startTime = Date.now();

await ensureDataDir();
const db = openDatabase(kindlingDbPath);
const repo = new KindlingRepo(db);
const settings = repo.ensureSettings();

const rootArg = process.argv.slice(2).find((arg) => arg && !arg.startsWith("-"));
if (rootArg && rootArg !== settings.libraryRoot) {
  repo.updateSettings({
    ...settings,
    libraryRoot: rootArg,
  });
}

void runWorker({
  repo,
  getSettings: () => repo.getSettings(),
  onLog: (message) => console.log(message),
});

const server = Bun.serve({
  port,
  fetch: createKindlingFetchHandler(repo, startTime),
});

const localBase = `http://localhost${port === 80 ? "" : `:${port}`}`;
const current = repo.getSettings();

console.log(`Kindling backend listening on ${localBase}`);
console.log(`Library root: ${current.libraryRoot}`);
console.log(`Health: ${localBase}/health`);
if (current.auth.mode === "apikey") {
  console.log(`API key: ${current.auth.key}`);
}

process.on("SIGINT", () => {
  server.stop();
  db.close();
  process.exit(0);
});

process.on("SIGTERM", () => {
  server.stop();
  db.close();
  process.exit(0);
});
