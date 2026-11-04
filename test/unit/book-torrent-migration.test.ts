import { afterEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { inspectMetainfo } from "../../src/media/metainfo";
import {
  migrateBookTorrents, migrationRepository, openMigrationJournal,
  prepareMigrationTarget, selectCachedMetainfo,
  type LegacyMigrationState, type MigrationDependencies, type MigrationReport,
} from "../../src/media/book-torrent-migration";
import { parseMigrationArgs } from "../../scripts/migrate-book-torrents";
import { MockBookTorrentClient } from "../mocks/book-torrents";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup(); });

async function fixture(multi = false) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-migration-")));
  const library = path.join(root, "library");
  const sourceRoot = path.join(root, "rtorrent");
  await fs.mkdir(library);
  await fs.mkdir(sourceRoot);
  const contents = multi ? [Buffer.from("first"), Buffer.from("second!")] : [Buffer.from("raw audiobook")];
  const data = Buffer.concat(contents);
  const info = {
    name: multi ? "Release" : "book.mp3",
    ...(multi ? { files: contents.map((data, i) => ({ path: [`${i}.mp3`], length: data.length })) } : { length: data.length }),
    "piece length": 16384,
    pieces: createHash("sha1").update(data).digest(),
    private: 1,
  };
  const metainfo = bencode.encode({ announce: "https://tracker.invalid/SECRET/announce", info });
  const metadata = inspectMetainfo(metainfo);
  const files = metadata.files.map((file) => path.join(sourceRoot, file.path));
  for (let i = 0; i < files.length; i++) {
    await fs.mkdir(path.dirname(files[i]!), { recursive: true });
    await fs.writeFile(files[i]!, contents[i]!);
  }
  const db = new Database(":memory:");
  db.exec("PRAGMA foreign_keys = ON");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Book", author: "Author" });
  const release = repo.createRelease({
    bookId: book.id, title: "Book", provider: "fixture", mediaType: "audio",
    infoHash: metadata.infoHash, url: "https://indexer.invalid/SECRET", status: "imported",
  });
  repo.putTorrentCache({
    key: "fixture", provider: "fixture", url: release.url,
    infoHash: metadata.infoHash, torrentBytes: metainfo,
  });
  const legacy: LegacyMigrationState = {
    tag: "Podible", hashing: 0,
    state: {
      name: metadata.name, hash: metadata.infoHash.toUpperCase(), complete: true, isActive: true,
      basePath: multi ? path.join(sourceRoot, metadata.name) : files[0]!,
      directory: multi ? path.join(sourceRoot, metadata.name) : sourceRoot,
      isMultiFile: multi, sizeBytes: metadata.size, bytesDone: metadata.size,
      leftBytes: 0, downRate: 0, message: "",
    },
    source: { basePath: multi ? path.join(sourceRoot, metadata.name) : files[0]!, selectedPaths: files },
  };
  const events: string[] = [];
  let failAt: string | undefined;
  let clock = 0;
  let checkingPolls = 0;
  let hangRecheck = false;
  const hit = (name: string) => {
    events.push(name);
    if (failAt === name) throw new Error("SECRET fault https://tracker.invalid/SECRET");
  };
  const client = new MockBookTorrentClient(path.join(library, ".podible", "downloads"));
  const add = client.ensureAdded.bind(client);
  client.ensureAdded = async (input) => {
    hit("add");
    expect(repo.bookTorrents.get(metadata.infoHash)).toBeNull();
    return add(input);
  };
  const inspect = client.inspect.bind(client);
  client.inspect = async (ref) => {
    const snapshot = await inspect(ref);
    if (snapshot.state === "checking" && !hangRecheck && --checkingPolls <= 0) snapshot.state = "inactive";
    return snapshot;
  };
  const pause = client.quiesce.bind(client);
  client.quiesce = async (ref) => { hit("pause"); await pause(ref); };
  client.recheck = async (ref) => {
    hit("recheck");
    const record = client.records.get(ref.nativeId)!;
    const buffers = await Promise.all(record.files.map((file) => fs.readFile(file.path)));
    const actual = Buffer.concat(buffers);
    const matches = actual.equals(data);
    record.snapshot.complete = matches;
    record.snapshot.bytesDone = matches ? metadata.size : 0;
    record.snapshot.state = "checking";
    checkingPolls = 2;
  };
  const resume = client.resume.bind(client);
  client.resume = async (ref) => {
    hit("resume");
    expect(events).toContain("verify");
    expect(repo.bookTorrents.get(metadata.infoHash)?.release_id).toBe(release.id);
    return resume(ref);
  };
  const repository = migrationRepository(db);
  const commit = repository.commit;
  repository.commit = (release, binding, files) => { hit("commit"); commit(release, binding, files); };
  const persisted: MigrationReport[] = [];
  const deps: MigrationDependencies = {
    repository, client,
    legacy: {
      async inspect() { hit("inspect-source"); return structuredClone(legacy); },
      async stop() { hit("stop-source"); legacy.state.isActive = false; },
    },
    async prepareTarget() { hit("prepare-root"); return prepareMigrationTarget(library, client.id); },
    async assertVerified(ref, metadata) {
      hit("verify");
      const snapshot = await client.inspect(ref);
      if (!snapshot.complete || snapshot.bytesDone !== metadata.size || snapshot.state !== "inactive")
        throw new Error("Verification failed");
    },
    journal: {
      report: { version: 1, runId: randomUUID(), entries: {} },
      async save() {
        hit(`save:${deps.journal.report.entries[metadata.infoHash]?.phase}`);
        persisted.push(structuredClone(deps.journal.report));
      },
    },
    now: () => clock,
    async sleep(ms) { clock += ms; },
  };
  cleanups.push(async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); });
  return {
    root, library, sourceRoot, metadata, metainfo, files, contents, db, repo, book, release,
    deps, legacy, client, events, persisted,
    fail: (event?: string) => { failAt = event; },
    hang: () => { hangRecheck = true; },
    allowExistingOnRetry() { client.ensureAdded = add; },
    run: (options: Parameters<typeof migrateBookTorrents>[1] = { apply: true }) => migrateBookTorrents(deps, options),
  };
}

test("CLI defaults to dry-run and requires explicit apply, report and stopped workers", () => {
  expect(parseMigrationArgs([]).apply).toBe(false);
  expect(parseMigrationArgs([]).allowedLegacyTags).toEqual(["Podible", "LazyLibrarian"]);
  expect(() => parseMigrationArgs(["--apply"])).toThrow();
  expect(() => parseMigrationArgs(["--apply", "--report", "/tmp/report.json"])).toThrow();
  expect(() => parseMigrationArgs(["--apply", "--workers-stopped", "--report", "/tmp/report.json", "--dry-run"])).toThrow();
  expect(() => parseMigrationArgs(["--book-id", "0"])).toThrow();
  expect(() => parseMigrationArgs(["--hash", "not-a-hash"])).toThrow();
  expect(() => parseMigrationArgs(["--legacy-tag", "unowned"])).toThrow();
  expect(parseMigrationArgs(["--book-id", "2", "--book-id", "3", "--hash", "A".repeat(40)]))
    .toMatchObject({ bookIds: [2, 3], hashes: ["a".repeat(40)] });
});

test("dry-run reads cached metainfo and raw sources without writing, stopping, adding or binding", async () => {
  const f = await fixture();
  const result = await f.run({});
  expect(result[0]).toMatchObject({ phase: "planned", metainfoSource: "cache", oldRecord: "retained" });
  expect(f.events).toEqual(["inspect-source"]);
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  expect(await fs.readdir(f.library)).toEqual([]);
  expect(f.persisted).toEqual([]);
  expect(JSON.stringify(result)).not.toContain("SECRET");
});

test("book and hash filters intersect and do not touch unrelated releases", async () => {
  const f = await fixture();
  expect(await f.run({ apply: true, bookIds: [f.book.id + 1] })).toEqual([]);
  expect(await f.run({ apply: true, hashes: ["a".repeat(40)] })).toEqual([]);
  expect(f.events).toEqual([]);
});

for (const multi of [false, true]) {
  test(`verified ${multi ? "multi-file" : "single-file"} migration hardlinks raw bytes and binds only after recheck`, async () => {
    const f = await fixture(multi);
    const result = await f.run();
    expect(result[0]).toMatchObject({ phase: "complete", oldRecord: "stopped-retained" });
    expect(f.legacy.state.isActive).toBe(false);
    expect(f.events.indexOf("recheck")).toBeLessThan(f.events.indexOf("commit"));
    expect(f.events.indexOf("verify")).toBeLessThan(f.events.indexOf("commit"));
    expect(f.events.indexOf("commit")).toBeLessThan(f.events.indexOf("resume"));
    const binding = f.repo.bookTorrents.get(f.metadata.infoHash)!;
    expect(binding.release_id).toBe(f.release.id);
    for (let i = 0; i < f.files.length; i++) {
      const destination = path.join(binding.root_path, binding.relative_path, f.metadata.files[i]!.path);
      expect((await fs.stat(destination)).ino).toBe((await fs.stat(f.files[i]!)).ino);
      expect(await fs.readFile(f.files[i]!)).toEqual(f.contents[i]!);
    }
    expect(f.persisted.some((report) => report.entries[f.metadata.infoHash]?.phase === "adding-paused")).toBe(true);
    expect(JSON.stringify(f.persisted)).not.toContain("SECRET");
  });
}

test("LL-tagged completed failed DB releases qualify; unrelated tags and nonterminal releases do not", async () => {
  const f = await fixture();
  f.legacy.tag = "LazyLibrarian";
  f.repo.setReleaseStatus(f.release.id, "failed");
  expect((await f.run())[0]?.phase).toBe("complete");
  expect(f.repo.getRelease(f.release.id)?.status).toBe("failed");
  const other = await fixture();
  other.legacy.tag = "Other";
  expect((await other.run())[0]).toMatchObject({ phase: "skipped", reason: "legacy-tag-excluded" });
  expect(other.events).toEqual(["inspect-source", "save:skipped"]);
  other.legacy.tag = "LazyLibrarian";
  expect((await other.run({ apply: true, allowedLegacyTags: ["Podible"] }))[0]?.phase).toBe("skipped");
  other.repo.setReleaseStatus(other.release.id, "downloading");
  expect((await other.run())[0]?.reason).toBe("release-status-excluded");
});

test("strict cache validation rejects wrong hashes, corrupt bytes and unsafe metainfo", async () => {
  const f = await fixture();
  const release = f.deps.repository.releases()[0]!;
  expect(() => selectCachedMetainfo(release, [Buffer.from("not a torrent")])).toThrow();
  expect(() => selectCachedMetainfo({ ...release, infoHash: "a".repeat(40) }, [f.metainfo])).toThrow();
  f.db.query("DELETE FROM torrent_cache").run();
  expect((await f.run())[0]).toMatchObject({ phase: "failed", reason: "verified-metainfo-unavailable", failedAt: "load-cached-metainfo" });
  expect(f.events).not.toContain("stop-source");
  expect(f.client.adds).toEqual([]);
});

for (const field of ["complete", "hashing", "bytesDone", "leftBytes", "downRate"] as const) {
  test(`unknown/incomplete source ${field} fails before any write-side effect`, async () => {
    const f = await fixture();
    if (field === "hashing") f.legacy.hashing = null;
    else if (field === "complete") f.legacy.state.complete = false;
    else f.legacy.state[field] = field === "bytesDone" ? 0 : 1;
    expect((await f.run())[0]?.phase).toBe("failed");
    expect(f.events).not.toContain("stop-source");
    expect(await fs.readdir(f.library)).toEqual([]);
  });
}

test("source layout mismatch, missing bytes, and symlinks fail closed", async () => {
  const f = await fixture();
  f.legacy.source.selectedPaths = [path.join(f.sourceRoot, "different.mp3")];
  expect((await f.run())[0]?.reason).toBe("source-layout-mismatch");
  f.legacy.source.selectedPaths = f.files;
  await fs.rename(f.files[0]!, f.files[0]! + ".original");
  expect((await f.run())[0]?.phase).toBe("failed");
  await fs.symlink(f.files[0]! + ".original", f.files[0]!);
  expect((await f.run())[0]?.reason).toBe("source-file-invalid");
  expect(f.events).not.toContain("stop-source");
});

test("same-length corrupt raw bytes fail recheck and never bind or resume", async () => {
  const f = await fixture();
  const corrupt = Buffer.alloc(f.contents[0]!.length, 1);
  await fs.writeFile(f.files[0]!, corrupt);
  expect((await f.run())[0]).toMatchObject({ phase: "failed", reason: "recheck-incomplete", pauseConfirmed: true });
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  expect(f.events).not.toContain("commit");
  expect(f.events).not.toContain("resume");
  expect(await fs.readFile(f.files[0]!)).toEqual(corrupt);
});

for (const operation of ["stop-source", "add", "recheck", "verify", "commit", "resume"]) {
  test(`${operation} failure is redacted and never erases source or target`, async () => {
    const f = await fixture();
    f.fail(operation);
    const [result] = await f.run();
    expect(result?.phase).toBe("failed");
    expect(result?.failedAt).toBeTruthy();
    expect(JSON.stringify(result)).not.toContain("SECRET");
    expect(await fs.readFile(f.files[0]!)).toEqual(f.contents[0]!);
    if (operation !== "resume") expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
    if (["recheck", "verify", "commit", "resume"].includes(operation)) {
      expect(result?.pauseConfirmed).toBe(true);
      expect(f.client.records.get(f.metadata.infoHash)?.snapshot.state).toBe("inactive");
    }
  });
}

test("recheck timeout retains a paused target without cutover", async () => {
  const f = await fixture();
  f.hang();
  expect((await f.run({ apply: true, timeoutMs: 500 }))[0]).toMatchObject({
    phase: "failed", reason: "recheck-timeout", failedAt: "recheck-target", pauseConfirmed: true,
  });
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  expect(f.events).not.toContain("resume");
});

test("lost add acknowledgment is paused and reconciled on retry without duplicate bindings", async () => {
  const f = await fixture();
  const add = f.client.ensureAdded.bind(f.client);
  f.client.ensureAdded = async (input) => { await add(input); throw new Error("lost add acknowledgment SECRET"); };
  expect((await f.run())[0]).toMatchObject({ phase: "failed", pauseConfirmed: true });
  expect(f.client.records.size).toBe(1);
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  f.allowExistingOnRetry();
  f.deps.journal.report = structuredClone(f.persisted.at(-1)!);
  expect((await f.run())[0]?.phase).toBe("complete");
  expect(f.client.adds.length).toBe(1);
});

test("uncertain committed cutover is safely rechecked instead of trusting the report", async () => {
  const f = await fixture();
  const commit = f.deps.repository.commit;
  f.deps.repository.commit = (...args) => { commit(...args); throw new Error("lost commit receipt"); };
  expect((await f.run())[0]?.phase).toBe("failed");
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)?.release_id).toBe(f.release.id);
  f.deps.repository.commit = commit;
  f.allowExistingOnRetry();
  f.deps.journal.report = structuredClone(f.persisted.at(-1)!);
  expect((await f.run())[0]?.phase).toBe("complete");
  expect(f.events.filter((event) => event === "recheck").length).toBe(2);
});

test("lost resume acknowledgment is paused, retained and verified again on retry", async () => {
  const f = await fixture();
  const resume = f.client.resume.bind(f.client);
  f.client.resume = async (ref) => { await resume(ref); throw new Error("lost resume receipt"); };
  expect((await f.run())[0]).toMatchObject({ phase: "failed", pauseConfirmed: true });
  expect(f.client.records.get(f.metadata.infoHash)?.snapshot.state).toBe("inactive");
  f.client.resume = resume;
  f.allowExistingOnRetry();
  expect((await f.run())[0]?.phase).toBe("complete");
  expect(f.events.filter((event) => event === "recheck").length).toBe(2);
});

test("partial hardlink work is resumable but conflicting files are never replaced", async () => {
  const f = await fixture(true);
  f.fail("recheck");
  expect((await f.run())[0]?.phase).toBe("failed");
  const binding = f.deps.journal.report.entries[f.metadata.infoHash]!.binding!;
  const target = path.join(binding.root_path, binding.relative_path, f.metadata.files[1]!.path);
  await fs.unlink(target);
  await fs.writeFile(target, "unrelated");
  f.fail(); f.allowExistingOnRetry();
  expect((await f.run())[0]?.reason).toBe("hardlink-conflict");
  expect(await fs.readFile(target, "utf8")).toBe("unrelated");
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  await fs.unlink(target);
  expect((await f.run())[0]?.phase).toBe("complete");
});

test("journal write failure before add prevents external addition and binding", async () => {
  const f = await fixture();
  f.fail("save:adding-paused");
  expect((await f.run())[0]).toMatchObject({ phase: "failed", failedAt: "add-paused" });
  expect(f.client.adds).toEqual([]);
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
});

test("binding and matching source_path updates are atomic; canonical paths, identity paths, IDs and timestamps survive", async () => {
  const f = await fixture();
  const assetId = Number(f.db.query(`INSERT INTO assets
    (book_id, kind, mime, total_size, duration_ms, source_release_id, created_at, updated_at)
    VALUES (?, 'single', 'audio/mpeg', ?, 1000, ?, 'old', 'old')`).run(f.book.id, f.metadata.size, f.release.id).lastInsertRowid);
  f.db.query(`INSERT INTO asset_files
    (asset_id, path, source_path, identity_path, size, start, end, duration_ms, updated_at)
    VALUES (?, '/canonical/content.mp3', ?, '/original/paid-cache-identity.mp3', ?, 0, ?, 1000, 'old')`)
    .run(assetId, f.files[0]!, f.metadata.size, f.metadata.size);
  const before = f.db.query("SELECT * FROM asset_files WHERE asset_id = ?").get(assetId) as Record<string, unknown>;
  expect((await f.run())[0]?.phase).toBe("complete");
  const after = f.db.query("SELECT * FROM asset_files WHERE asset_id = ?").get(assetId) as Record<string, unknown>;
  const binding = f.repo.bookTorrents.get(f.metadata.infoHash)!;
  expect(after).toEqual({ ...before, source_path: path.join(binding.root_path, binding.relative_path, f.metadata.files[0]!.path) });
  expect(f.db.query("SELECT updated_at FROM assets WHERE id = ?").get(assetId)).toEqual({ updated_at: "old" });
});

test("release changes during verification abort the entire binding transaction", async () => {
  const f = await fixture();
  const verify = f.deps.assertVerified;
  f.deps.assertVerified = async (...args) => { await verify(...args); f.repo.setReleaseStatus(f.release.id, "failed"); };
  expect((await f.run())[0]?.reason).toBe("release-changed");
  expect(f.repo.bookTorrents.get(f.metadata.infoHash)).toBeNull();
  expect(f.events).not.toContain("resume");
});

test("durable journal is mode 0600, excludes concurrent writers and never overwrites on dry-run", async () => {
  const f = await fixture();
  const reportPath = path.join(f.root, "report.json");
  const handle = await openMigrationJournal(reportPath, true);
  try {
    await handle.journal.save();
    const original = await fs.readFile(reportPath, "utf8");
    expect((await fs.stat(reportPath)).mode & 0o777).toBe(0o600);
    await expect(openMigrationJournal(reportPath, true)).rejects.toThrow();
    const reader = await openMigrationJournal(reportPath, false);
    await expect(reader.journal.save()).rejects.toThrow();
    await reader.close();
    expect(await fs.readFile(reportPath, "utf8")).toBe(original);
  } finally { await handle.close(); }
  const reopened = await openMigrationJournal(reportPath, true);
  expect(reopened.journal.report.version).toBe(1);
  await reopened.close();
});
