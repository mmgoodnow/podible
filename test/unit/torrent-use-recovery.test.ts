import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { withTorrentRegistration, withTorrentUse } from "../../src/media/torrent-use";
import { withExecutionEffects } from "../../src/runtime/execution-effects";

function fixture() {
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), ref = { clientId: `transmission:${randomUUID()}`, infoHash: "a".repeat(40) };
  const scope = { jobId: 1, runId: randomUUID(), stepName: "add", signal: new AbortController().signal };
  const users = () => (JSON.parse((db.query("SELECT value_json FROM app_state WHERE key LIKE 'torrent-retirement:admission:%'").get() as { value_json: string }).value_json) as { users: unknown[] }).users;
  return { db, repo, ref, scope, users };
}

test("durable retries and a new repository instance reconcile the original admission rather than exhausting 128 tokens", async () => {
  const f = fixture();
  try {
    for (let index = 0; index < 130; index++) {
      await withExecutionEffects(f.scope, () => withTorrentUse(new BooksRepo(f.db), f.ref, async () => {
        throw new Error("uncertain engine acknowledgement");
      })).catch(() => {});
      expect(f.users()).toHaveLength(1);
    }
    await withExecutionEffects(f.scope, () => withTorrentUse(f.repo, f.ref, async () => "reconciled"));
    expect(f.users()).toEqual([]);
  } finally { f.db.close(); }
});

test("siblings and cancelled generations never clear each other's uncertain effects", async () => {
  const f = fixture();
  try {
    await withExecutionEffects(f.scope, () => Promise.all([0, 1].map(() => withTorrentUse(f.repo, f.ref,
      async () => { throw new Error("uncertain"); }).catch(() => {}))));
    expect(f.users()).toHaveLength(2);
    await withExecutionEffects({ ...f.scope, runId: randomUUID() }, () => withTorrentUse(f.repo, f.ref, async () => "other generation"));
    expect(f.users()).toHaveLength(2);
    await withExecutionEffects(f.scope, () => Promise.all([0, 1].map(() => withTorrentUse(f.repo, f.ref, async () => "reconciled"))));
    expect(f.users()).toEqual([]);
  } finally { f.db.close(); }
});

test("registration recursion throws and releases the mutex while leaving uncertain admission fenced", async () => {
  const f = fixture();
  try {
    await expect(withExecutionEffects(f.scope, () => withTorrentRegistration(f.repo, f.ref,
      () => withTorrentRegistration(f.repo, f.ref, async () => {})))).rejects.toThrow("Recursive");
    expect(f.users()).toHaveLength(1);
    await withExecutionEffects(f.scope, () => withTorrentRegistration(f.repo, f.ref, async () => {}));
    expect(f.users()).toEqual([]);
  } finally { f.db.close(); }
});
