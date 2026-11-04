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

async function producerFixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "torrent-use-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings(); settings.libraryRoot = root; repo.updateSettings(settings);
  const book = repo.createBook({ title: "Admission", author: "Fixture" });
  const client = new MockBookTorrentClient(path.join(root, ".podible", "downloads"), "transmission:11111111-1111-4111-8111-111111111111");
  const metainfo = bencode.encode({ info: { name: "book.mp3", length: 4, "piece length": 16384, pieces: Buffer.alloc(20) } });
  const states = () => (db.query("SELECT value_json FROM app_state WHERE key LIKE 'torrent-retirement:admission:v1:%'").all() as { value_json: string }[])
    .map(row => JSON.parse(row.value_json));
  const add = (id: number) => ensureBookTorrentAdded(repo, settings, id, metainfo, { bookTorrentClient: client });
  const fence = createOrphanRetirementFence(repo, { globalMaintenanceHeld: () => true, readIdentityDigest: () => "a".repeat(64) });
  return { repo, book, client, settings, states, add, fence,
    binding: (hash: string) => stewardshipBinding(repo, "books", { ...repo.bookTorrents.get(hash)! }, client.id)!,
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("ambiguous private book invocation retains its independent admission after a successful logical retry", async () => {
  const s = await producerFixture();
  try {
    s.client.beforeAdd = () => expect(s.states()[0]!.users).toHaveLength(1);
    s.client.failResumeOnce = true;
    await expect(s.add(s.book.id)).rejects.toThrow("Lost resume acknowledgment");
    const ambiguous = structuredClone(s.states()[0]!.users[0]);
    s.client.beforeAdd = () => {
      const users = s.states()[0]!.users;
      expect(users).toHaveLength(2);
      expect(new Set(users.map((user: { token: string }) => user.token)).size).toBe(2);
      expect(new Set(users.map((user: { operationId: string }) => user.operationId)).size).toBe(2);
    };
    const row = await s.add(s.book.id);
    expect(s.states()[0]!.users).toEqual([ambiguous]);
    expect(s.states()[0]!.useRevision).toBeGreaterThan(1);
    expect(s.repo.deleteBook(s.book.id)).toBe(true);
    const binding = s.binding(row.info_hash), snapshot = s.fence.inspect(binding);
    expect(snapshot).toMatchObject({ completeInventory: true, domainBindings: 0, activeConsumers: 1 });
    expect(() => s.fence.claim(binding, randomUUID(), snapshot)).toThrow("zero-use proof");
    expect(s.client.adds).toHaveLength(1);
    expect(s.client.controls).toEqual([]);
  } finally { await s.close(); }
});

test("overlapping same-book invocations keep retirement blocked after A succeeds and deletion until B finishes", async () => {
  const s = await producerFixture();
  const gateA = Promise.withResolvers<void>(), gateB = Promise.withResolvers<void>();
  const enteredA = Promise.withResolvers<void>(), enteredB = Promise.withResolvers<void>();
  const pending: Promise<unknown>[] = [];
  try {
    const row = await s.add(s.book.id); // Establish ownership markers before concurrent calls.
    const original = s.client.ensureAdded.bind(s.client);
    let calls = 0, bFinished = false;
    s.client.ensureAdded = async input => {
      if (++calls === 1) { enteredA.resolve(); await gateA.promise; }
      else { enteredB.resolve(); await gateB.promise; }
      return original(input);
    };
    const a = s.add(s.book.id); pending.push(a); await enteredA.promise;
    const tokenA = s.states()[0]!.users[0].token;
    const b = s.add(s.book.id).then(value => { bFinished = true; return value; });
    pending.push(b); await enteredB.promise;
    const users = s.states()[0]!.users;
    expect(users).toHaveLength(2);
    const userB = users.find((user: { token: string }) => user.token !== tokenA);
    expect(userB).toBeDefined();
    gateA.resolve(); await a;
    expect(bFinished).toBe(false);
    expect(s.states()[0]!.users).toEqual([userB]);
    expect(s.repo.deleteBook(s.book.id)).toBe(true);
    const binding = s.binding(row.info_hash), before = s.fence.inspect(binding);
    expect(before).toMatchObject({ completeInventory: true, domainBindings: 0, activeConsumers: 1 });
    expect(() => s.fence.claim(binding, randomUUID(), before)).toThrow("zero-use proof");
    gateB.resolve(); await b;
    expect(bFinished).toBe(true);
    expect(s.states()[0]!.users).toHaveLength(0);
    const after = s.fence.inspect(binding), operation = randomUUID();
    expect(after).toMatchObject({ completeInventory: true, domainBindings: 0, activeConsumers: 0 });
    const claim = s.fence.claim(binding, operation, after);
    const next = s.repo.createBook({ title: "New demand", author: "Fixture" });
    await expect(s.add(next.id)).rejects.toThrow(/retir/i);
    expect(() => s.repo.updateSettings(s.settings)).toThrow("unresolved");
    s.fence.finish(binding, operation, claim, "retired");
    s.repo.updateSettings(s.settings);
    await expect(s.add(next.id)).rejects.toThrow(/retir/i);
    expect(s.client.adds).toHaveLength(1);
    expect(s.client.controls).toEqual([]);
  } finally {
    gateA.resolve(); gateB.resolve(); await Promise.allSettled(pending); await s.close();
  }
});
