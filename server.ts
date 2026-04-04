import { ensureConfigDir, booksDbPath, port } from "./src/config";
import { openDatabase } from "./src/books/db";
import { createPodibleFetchHandler } from "./src/books/http";
import { BooksRepo } from "./src/books/repo";
import { runWorker } from "./src/books/worker";

const startTime = Date.now();

await ensureConfigDir();
const db = openDatabase(booksDbPath);
const repo = new BooksRepo(db);
repo.ensureSettings();

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
const current = repo.getSettings();

console.log(`Podible backend listening on ${localBase}`);
console.log(`Library root: ${current.libraryRoot}`);
console.log(`RPC endpoint: ${localBase}/rpc`);
console.log(`Home: ${localBase}/`);
if (current.auth.mode === "apikey") {
  console.log(`API key: ${current.auth.key}`);
  const authorizedHome = `${localBase}/?api_key=${encodeURIComponent(current.auth.key)}`;
  console.log(`Authorized home: ${authorizedHome}`);
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
