import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { ensureBookTorrentAdded } from "../../src/media/book-torrents";
import { createOrphanRetirementFence } from "../../src/media/torrent-retirement-admission";
import { stewardshipBinding } from "../../src/media/torrent-stewardship";
import { MockBookTorrentClient } from "../mocks/book-torrents";

test("actual private book producer holds durable admission across lost acknowledgements and honors orphan retirement tombstones", async () => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "torrent-use-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings(); settings.libraryRoot = root; repo.updateSettings(settings);
  const book = repo.createBook({ title: "Admission", author: "Fixture" });
  const client = new MockBookTorrentClient(path.join(root, ".podible", "downloads"), "transmission:11111111-1111-4111-8111-111111111111");
  const metainfo = bencode.encode({ info: { name: "book.mp3", length: 4, "piece length": 16384, pieces: Buffer.alloc(20) } });
  const states = () => (db.query("SELECT value_json FROM app_state WHERE key LIKE 'torrent-retirement:admission:v1:%'").all() as { value_json: string }[])
    .map(row => JSON.parse(row.value_json));
  const add = (id: number) => ensureBookTorrentAdded(repo, settings, id, metainfo, { bookTorrentClient: client });
  try {
    client.beforeAdd = () => expect(states()[0]!.users).toHaveLength(1);
    client.failResumeOnce = true;
    await expect(add(book.id)).rejects.toThrow("Lost resume acknowledgment");
    const token = states()[0]!.users[0].token;
    expect(states()[0]!.users).toHaveLength(1);
    const row = await add(book.id);
    expect(states()[0]!.users).toHaveLength(0);
    expect(states()[0]!.useRevision).toBeGreaterThan(1);
    expect(typeof token).toBe("string");
    db.query("DELETE FROM books WHERE id=?").run(book.id);
    const binding = stewardshipBinding(repo, "books", { ...row }, client.id)!;
    const fence = createOrphanRetirementFence(repo, { globalMaintenanceHeld: () => true, readIdentityDigest: () => "a".repeat(64) });
    const before = fence.inspect(binding), operation = randomUUID();
    expect(before.domainBindings).toBe(0);
    const claim = fence.claim(binding, operation, before);
    const next = repo.createBook({ title: "New demand", author: "Fixture" });
    await expect(add(next.id)).rejects.toThrow(/retir/i);
    expect(() => repo.updateSettings(settings)).toThrow("unresolved");
    expect(client.controls).toEqual([]);
    fence.finish(binding, operation, claim, "retired");
    repo.updateSettings(settings);
    await expect(add(next.id)).rejects.toThrow(/retir/i);
    expect(client.adds).toHaveLength(1);
  } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
});
