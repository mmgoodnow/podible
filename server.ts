import { dataDir, ensureDataDir, kindlingDbPath, port } from "./src/config";
import { openDatabase } from "./src/kindling/db";
import { createPodibleFetchHandler } from "./src/kindling/http";
import { KindlingRepo } from "./src/kindling/repo";
import { runWorker } from "./src/kindling/worker";
import { installProcessLogTee } from "./src/logging";

const startTime = Date.now();

await ensureDataDir();
const logFiles = await installProcessLogTee(dataDir);
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
  fetch: createPodibleFetchHandler(repo, startTime),
});

const localBase = `http://localhost${port === 80 ? "" : `:${port}`}`;
const current = repo.getSettings();

console.log(`Podible backend listening on ${localBase}`);
console.log(`Library root: ${current.libraryRoot}`);
console.log(`RPC endpoint: ${localBase}/rpc`);
console.log(`Home: ${localBase}/`);
console.log(`Log files: stdout=${logFiles.stdoutPath} stderr=${logFiles.stderrPath}`);
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
