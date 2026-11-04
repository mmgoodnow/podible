import { ensureConfigDir, booksDbPath, port } from "./src/config";
import { openDatabase } from "./src/db";
import { createPodibleFetchHandler } from "./src/http";
import { pingPlexOwnerToken } from "./src/plex";
import { BooksRepo } from "./src/repo";
import { BackgroundWorker } from "./src/runtime/background-worker";
import type { HTMLBundle } from "bun";

export async function startServer(frontend?: HTMLBundle) {
  const startTime = Date.now();

  await ensureConfigDir();
  const db = openDatabase(booksDbPath);
  let server: ReturnType<typeof Bun.serve> | undefined;
  let stopping = false;
  let plexPingTimer: ReturnType<typeof setInterval> | undefined;
  const background = new BackgroundWorker({ databasePath: booksDbPath, onFailure: error => {
    console.error("[background] execution worker failed", error.message);
    void shutdown(1);
  } });
  async function shutdown(exitCode = 0): Promise<void> {
    if (stopping) return;
    stopping = true;
    clearInterval(plexPingTimer);
    const deadline = setTimeout(() => process.exit(exitCode || 1), 8_000);
    deadline.unref();
    try {
      const [, result] = await Promise.all([server?.stop(), background.stop()]);
      if (result.forced) console.warn("[background] worker terminated after shutdown deadline; durable leases will recover work");
      db.close();
      clearTimeout(deadline);
      process.exit(exitCode);
    } catch {
      console.error("[background] shutdown incomplete; exiting without starting another engine");
      process.exit(1);
    }
  }
  const onSignal = () => void shutdown();
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  try {
    await background.start();
    if (stopping) throw new Error("Server stopped during background startup");
    // Construct after the worker's durable journal migration, never cache legacy schema on main.
    const repo = new BooksRepo(db);
    repo.ensureSettings();
    const current = repo.getSettings();

    // Keep the Plex owner token alive; this existing asynchronous request does not run jobs.
    async function pingPlexAndLog(): Promise<void> {
      const settings = repo.getSettings();
      if (!settings.auth.plex.ownerToken) return;
      const ok = await pingPlexOwnerToken(settings);
      console.log(`[plex] owner token ping ${ok ? "ok" : "FAILED - admin must re-link Plex"}`);
    }
    void pingPlexAndLog();
    const PLEX_PING_INTERVAL_MS = 24 * 60 * 60 * 1000;
    plexPingTimer = setInterval(
      () => void pingPlexAndLog(),
      PLEX_PING_INTERVAL_MS,
    );

    const fetchHandler = createPodibleFetchHandler(repo, startTime);
    server = Bun.serve({
      port,
      idleTimeout: 60,
      ...(frontend
        ? {
            routes: {
              "/": frontend,
              "/library": frontend,
              "/add": frontend,
              "/book/:bookId": frontend,
              "/series": frontend,
              "/author": frontend,
              "/movies": frontend,
              "/movies/:id": frontend,
              "/requests": frontend,
              "/requests/:id": frontend,
              "/tv/:id": frontend,
              "/tv/requests/:id": frontend,
              "/tv/library/:id": frontend,
              "/recovery": frontend,
              "/stewardship": frontend,
              "/tv-retention": frontend,
              "/movie-dual-write": frontend,
            },
            development: { hmr: true, console: true },
          }
        : { development: false }),
      fetch: request => background.state === "ready" ? fetchHandler(request)
        : new Response("Background execution unavailable", { status: 503 }),
    });

    const localBase = `http://localhost${server.port === 80 ? "" : `:${server.port}`}`;
    console.log(`Podible backend listening on ${localBase}`);
    console.log(`Library root: ${current.libraryRoot}`);
    console.log(`RPC endpoint: ${localBase}/rpc`);
    console.log(`Home: ${localBase}/`);
    return server;
  } catch (error) {
    clearInterval(plexPingTimer);
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
    await server?.stop();
    await background.stop();
    db.close();
    throw error;
  }
}

if (import.meta.main) await startServer();
