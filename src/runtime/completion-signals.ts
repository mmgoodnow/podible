import { mkdirSync, readdirSync, unlinkSync, watch, type FSWatcher } from "node:fs";
import type { Database } from "bun:sqlite";

export const completionEvent = "podible-completion";

/** Subscriptions live beside the workflow journal, not in worker memory. */
export class CompletionSignals {
  private tail = Promise.resolve();
  constructor(private db: Database, private send: (runId: string, slot: string) => Promise<unknown>) {
    db.exec(`CREATE TABLE IF NOT EXISTS podible_completion_waits (
      run_id TEXT NOT NULL, slot TEXT NOT NULL, source TEXT NOT NULL,
      PRIMARY KEY (run_id, slot)
    );
    CREATE INDEX IF NOT EXISTS podible_completion_waits_source ON podible_completion_waits(source);`);
  }
  arm(runId: string, slot: string, source: string) {
    this.db.run("INSERT OR IGNORE INTO podible_completion_waits VALUES (?, ?, ?)", [runId, slot, source]);
  }
  clear(runId: string, slot?: string) {
    if (slot === undefined) this.db.run("DELETE FROM podible_completion_waits WHERE run_id = ?", [runId]);
    else this.db.run("DELETE FROM podible_completion_waits WHERE run_id = ? AND slot = ?", [runId, slot]);
  }
  signal(source: string, prefix = false) {
    const dispatch = this.tail.then(async () => {
      const pending = this.db.query<{ run_id: string; slot: string }, [string]>(
        prefix ? "SELECT run_id, slot FROM podible_completion_waits WHERE source GLOB ?" :
          "SELECT run_id, slot FROM podible_completion_waits WHERE source = ?"
      ).all(prefix ? source + "*" : source);
      for (const row of pending) {
        await this.send(row.run_id, row.slot);
        // A crash between send and clear can duplicate a wake, never lose it.
        this.clear(row.run_id, row.slot);
      }
    });
    this.tail = dispatch.catch(() => {});
    return dispatch;
  }
  drained() { return this.tail; }
}

const torrentEvent = /^([0-9a-f]{40}|[0-9a-f]{64})\.[A-Za-z0-9]+\.done$/;

/** Watch first, then drain, so an event written during startup cannot fall in a gap. */
export function watchTorrentCompletions(directory: string, signal: (key: string, prefix?: boolean) => Promise<void>,
  onError: () => void) {
  let watcher: FSWatcher | undefined;
  let stopped = false, dirty = false;
  let running: Promise<void> | undefined;
  let retry: ReturnType<typeof setTimeout> | undefined;
  const recover = () => {
    if (stopped || retry) return;
    onError();
    watcher?.close();
    retry = setTimeout(() => { retry = undefined; start(); }, 1000);
    retry.unref();
  };
  const drain = () => {
    dirty = true;
    if (running || stopped) return;
    running = (async () => {
      while (dirty && !stopped) {
        dirty = false;
        for (const name of readdirSync(directory)) {
          if (stopped) return;
          const match = torrentEvent.exec(name);
          if (match) await signal("torrent:" + match[1]);
          else if (/^reset\.[0-9a-f]{16}\.done$/.test(name)) await signal("torrent:", true);
          else continue;
          unlinkSync(directory + "/" + name);
        }
      }
    })().catch(recover).finally(() => {
      running = undefined;
      if (dirty && !retry) drain();
    });
  };
  const start = () => {
    try {
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      watcher = watch(directory, { persistent: false }, drain);
      watcher.on("error", recover);
      // The daemon may have stopped between completing the file and running its hook.
      void signal("torrent:", true).then(drain, recover);
    } catch { recover(); }
  };
  start();
  return async () => {
    stopped = true;
    clearTimeout(retry);
    watcher?.close();
    await running;
  };
}
