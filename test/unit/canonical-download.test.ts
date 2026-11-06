import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { createHash, randomUUID } from "node:crypto";
import bencode from "bencode";
import { initializeManagedContent } from "../../src/media/library-view";
import { prepareCanonicalDownload, readCanonicalDownload, resolveDownloadFile, sealCanonicalDownload,
  finalizeCanonicalDownloadFile, finalizedCanonicalObject } from "../../src/media/canonical-download";
import { adoptContentFile } from "../../src/media/content-store";
import { adoptBookContent, prepareBookStorage } from "../../src/library/book-storage";
import { inspectImportPath } from "../../src/library/importer";
import type { TorrentClient } from "../../src/media/torrent-client";

async function fixture(book = false) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "canonical-download-")));
  const contentRoot = book ? await prepareBookStorage(root) : path.join(root, "objects");
  if (!book) await initializeManagedContent(contentRoot);
  const sourceRoot = path.join(root, "downloads"); await fs.mkdir(sourceRoot);
  const members = [{ path: "chapter01.mp3", text: "first chapter" }, { path: "chapter02.mp3", text: "second chapter" }, { path: "release.nfo", text: "sidecar" }];
  const bytes = Buffer.concat(members.map(member => Buffer.from(member.text)));
  const metainfo = bencode.encode({ info: { name: "Book", files: members.map(member => ({ length: Buffer.byteLength(member.text), path: [member.path] })),
    "piece length": 16384, pieces: createHash("sha1").update(bytes).digest() } });
  const input = { contentRoot, sourceRoot, metainfo, clientId: "fixture-client", allowCreate: true };
  const plan = (await prepareCanonicalDownload(input))!;
  const ref = { clientId: input.clientId, nativeId: plan.infoHash };
  const client = { id: ref.clientId, sealDownload: async () => {} } as unknown as TorrentClient;
  return { root, contentRoot, sourceRoot, input, members, plan, ref, client,
    async fill() { for (const member of members) await fs.writeFile(path.join(sourceRoot, "Book", member.path), member.text); },
    close: () => fs.rm(root, { recursive: true, force: true }) };
}

test("initial torrent is a persisted view of private canonical objects; import hashes in place and sidecars stay local", async () => {
  const f = await fixture();
  try {
    expect(f.plan.ready).toBe(true); expect(f.plan.sealed).toBe(false);
    const file = f.plan.files[0]!, view = path.join(f.sourceRoot, file.path), canonical = path.join(f.contentRoot, file.relativePath!);
    expect((await fs.lstat(view)).isSymbolicLink()).toBe(true);
    expect(await fs.readlink(view)).toBe(canonical);
    expect(await prepareCanonicalDownload(f.input)).toEqual(f.plan);
    await expect(finalizeCanonicalDownloadFile(f.contentRoot, f.sourceRoot, view, file.size)).rejects.toThrow("active writer");
    await f.fill(); const before = await fs.stat(canonical, { bigint: true });
    await sealCanonicalDownload(f.contentRoot, f.sourceRoot, f.client, f.ref);
    const result = await adoptContentFile({ contentRoot: f.contentRoot, sourceRoot: f.sourceRoot, sourcePath: view, fileId: randomUUID(), expectedSize: file.size });
    expect(result.id).toBe(file.id); expect(result.relativePath).toBe(file.relativePath!);
    expect(result.sha256).toBe(createHash("sha256").update(f.members[0]!.text).digest("hex"));
    expect((await fs.stat(canonical, { bigint: true })).ino).toBe(before.ino);
    expect(await finalizedCanonicalObject(f.contentRoot, canonical)).toEqual(result);
    expect(await adoptContentFile({ contentRoot: f.contentRoot, sourceRoot: f.sourceRoot, sourcePath: view, fileId: randomUUID(), expectedSize: file.size })).toEqual(result);
    const sidecar = path.join(f.sourceRoot, "Book", "release.nfo");
    expect((await fs.lstat(sidecar)).isFile()).toBe(true);
    expect(f.plan.files[2]!.relativePath).toBeNull();
    expect(await readCanonicalDownload(f.contentRoot, f.sourceRoot)).toMatchObject({ sealed: true });
    await fs.unlink(view);
    expect(await fs.readFile(canonical, "utf8")).toBe(f.members[0]!.text);
    expect(await prepareCanonicalDownload(f.input)).toMatchObject({ sealed: true });
    expect(await fs.readlink(view)).toBe(canonical);
  } finally { await f.close(); }
});

test("canonical layout survives reservation interruption without truncating bytes or changing identity", async () => {
  const f = await fixture();
  try {
    const file = f.plan.files[0]!, canonical = path.join(f.contentRoot, file.relativePath!);
    await f.fill();
    const before = await fs.stat(canonical, { bigint: true });
    expect(await prepareCanonicalDownload({ ...f.input, allowCreate: false })).toEqual(f.plan);
    expect(await fs.readFile(canonical, "utf8")).toBe(f.members[0]!.text);
    expect((await fs.stat(canonical, { bigint: true })).ino).toBe(before.ino);
    await fs.unlink(canonical);
    await expect(prepareCanonicalDownload(f.input)).rejects.toThrow("missing or was replaced");
    await expect(fs.lstat(canonical)).rejects.toMatchObject({ code: "ENOENT" });
  } finally { await f.close(); }
});

test("interruption before add and concurrent retries keep the original persisted reservation", async () => {
  const f = await fixture();
  try {
    const sourceRoot = path.join(f.root, "interrupted"); await fs.mkdir(sourceRoot);
    let calls = 0;
    await expect(prepareCanonicalDownload({ ...f.input, sourceRoot, assertActive() {
      if (++calls === 3) throw new Error("execution lost");
    } })).rejects.toThrow("execution lost");
    const pending = (await readCanonicalDownload(f.contentRoot, sourceRoot))!;
    expect(pending.ready).toBe(false);
    const file = pending.files[0]!, view = path.join(sourceRoot, file.path), canonical = path.join(f.contentRoot, file.relativePath!);
    await fs.writeFile(view, f.members[0]!.text);
    const before = await fs.stat(canonical, { bigint: true });
    const results = await Promise.all([prepareCanonicalDownload({ ...f.input, sourceRoot }), prepareCanonicalDownload({ ...f.input, sourceRoot })]);
    expect(results[0]).toEqual(results[1]); expect(results[0]!.files.map(file => file.id)).toEqual(pending.files.map(file => file.id));
    expect((await fs.stat(canonical, { bigint: true })).ino).toBe(before.ino);
    expect(await fs.readFile(canonical, "utf8")).toBe(f.members[0]!.text);
    expect((await fs.stat(canonical)).mode & 0o004).toBe(0o004);
  } finally { await f.close(); }
});

test("arbitrary links, replaced canonical objects, foreign roots and changed metainfo are not adopted", async () => {
  const f = await fixture();
  try {
    const file = f.plan.files[0]!, view = path.join(f.sourceRoot, file.path);
    await f.fill();
    await expect(prepareCanonicalDownload({ ...f.input, clientId: "another-client" })).rejects.toThrow("differs");
    await fs.unlink(view); await fs.symlink(path.join(f.sourceRoot, "Book", "chapter02.mp3"), view);
    await expect(resolveDownloadFile(f.contentRoot, f.sourceRoot, view, file.size)).rejects.toThrow("replaced");
    await expect(prepareCanonicalDownload(f.input)).rejects.toThrow("no longer points");
    await expect(resolveDownloadFile(f.contentRoot, f.sourceRoot, path.join(f.sourceRoot, "..", "escape.mp3"))).rejects.toThrow();
  } finally { await f.close(); }
});

test("book import and inspection reuse declared canonical objects, preserving ordered torrent filenames", async () => {
  const f = await fixture(true);
  try {
    await f.fill(); await sealCanonicalDownload(f.contentRoot, f.sourceRoot, f.client, f.ref);
    for (const file of f.plan.files.filter(file => file.relativePath)) {
      const view = path.join(f.sourceRoot, file.path), canonical = path.join(f.contentRoot, file.relativePath!);
      await finalizeCanonicalDownloadFile(f.contentRoot, f.sourceRoot, view, file.size);
      expect(await adoptBookContent(f.root, canonical)).toBe(canonical);
    }
    const inspected = await inspectImportPath(f.sourceRoot, f.root);
    expect(inspected.filter(file => file.supportedAudio).map(file => path.basename(file.sourcePath))).toEqual(["chapter01.mp3", "chapter02.mp3"]);
    const file = f.plan.files[0]!;
    await fs.writeFile(path.join(f.contentRoot, file.relativePath!), "x".repeat(file.size));
    await expect(adoptBookContent(f.root, path.join(f.contentRoot, file.relativePath!))).rejects.toThrow("changed");
  } finally { await f.close(); }
});

test("ordinary preexisting download layouts stay legacy rather than being silently converted", async () => {
  const f = await fixture();
  try {
    const sourceRoot = path.join(f.root, "legacy"); await fs.mkdir(sourceRoot); await fs.writeFile(path.join(sourceRoot, "unowned"), "old");
    expect(await prepareCanonicalDownload({ ...f.input, sourceRoot, allowCreate: false })).toBeNull();
    await expect(prepareCanonicalDownload({ ...f.input, sourceRoot })).rejects.toThrow("existing torrent layout");
  } finally { await f.close(); }
});
