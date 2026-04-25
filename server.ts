import { ensureConfigDir, booksDbPath, port } from "./src/config";
import { openDatabase } from "./src/db";
import { createPodibleFetchHandler } from "./src/http";
import { pingPlexOwnerToken } from "./src/plex";
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

// Keep the Plex owner token alive so shared-user lookups don't 401 a week
// after admin setup. Plex.tv extends a token's lifetime when /api/v2/ping is
// hit before the embedded `exp`. We do it at startup (cheap, catches a token
// that's about to expire) and every 24h thereafter. If it fails, we log; the
// admin recovery path is to log out and back in (which re-captures the token).
async function pingPlexAndLog(): Promise<void> {
  const settings = repo.getSettings();
  if (!settings.auth.plex.ownerToken) return;
  const ok = await pingPlexOwnerToken(settings);
  console.log(`[plex] owner token ping ${ok ? "ok" : "FAILED — admin must re-link Plex"}`);
}
void pingPlexAndLog();
const PLEX_PING_INTERVAL_MS = 24 * 60 * 60 * 1000;
const plexPingTimer = setInterval(() => void pingPlexAndLog(), PLEX_PING_INTERVAL_MS);

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
  clearInterval(plexPingTimer);
  server.stop();
  db.close();
  process.exit(0);
});

process.on("SIGTERM", () => {
  clearInterval(plexPingTimer);
  server.stop();
  db.close();
  process.exit(0);
});
