import { ensureConfigDir, booksDbPath, port } from "./src/config";
import { openDatabase } from "./src/db";
import { createPodibleFetchHandler } from "./src/http";
import { BooksRepo } from "./src/repo";
import { runWorker } from "./src/worker";

const startTime = Date.now();

await ensureConfigDir();
const db = openDatabase(booksDbPath);
const repo = new BooksRepo(db);
repo.ensureSettings();
const current = repo.getSettings();

void runWorker({
  repo,
  getSettings: () => repo.getSettings(),
  onLog: (message) => console.log(message),
});

const server = Bun.serve({
  port,
  idleTimeout: 60,
  fetch: createPodibleFetchHandler(repo, startTime),
});

const localBase = `http://localhost${port === 80 ? "" : `:${port}`}`;

console.log(`Podible backend listening on ${localBase}`);
console.log(`Library root: ${current.libraryRoot}`);
console.log(`RPC endpoint: ${localBase}/rpc`);
console.log(`Home: ${localBase}/`);

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
