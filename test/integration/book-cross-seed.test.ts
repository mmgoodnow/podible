import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import bencode from "bencode";
import { createHash, randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { adoptBookContent, bookContentRoot } from "../../src/library/book-storage";
import { prepareCanonicalDownload, sealCanonicalDownload, finalizeCanonicalDownloadFile } from "../../src/media/canonical-download";
import { crossSeedCommandSchema } from "../../src/media/cross-seed-state";
import { assertCrossSeedDemand, crossSeedOffer, crossSeedPlan, processCrossSeed, queueCrossSeed, queueSelectedCrossSeeds } from "../../src/media/cross-seed";
import { rememberSelectedCopies, selectedCopyOffer } from "../../src/media/selected-copies";
import { releaseOfferFromResult } from "../../src/media/release-offer";
import { materializeReuseLayout } from "../../src/media/reuse-layout";
import { inspectMetainfo } from "../../src/media/metainfo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { jobLockKeys, lockSetsConflict } from "../../src/worker/locks";
import type { DownloadLocation, TorrentClient } from "../../src/media/torrent-client";
import type { WorkerContext } from "../../src/worker/context";

async function fixture(media: "audio" | "ebook") {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-reuse-")));
  const db = new Database(path.join(root, "app.sqlite")); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.libraryRoot = path.join(root, "library"); repo.updateSettings(settings);
  const book = repo.createBook({ title: "Reusable book", author: "Writer" });
  const source = path.join(root, media === "audio" ? "original.mp3" : "original.epub");
  await fs.writeFile(source, "abcdefgh");
  const canonical = await adoptBookContent(settings.libraryRoot, source);
  const manifestation = repo.addManifestation({ bookId: book.id, kind: media });
  const asset = repo.addAsset({ bookId: book.id, manifestationId: manifestation.id,
    kind: "single", mime: media === "audio" ? "audio/mpeg" : "application/epub+zip",
    totalSize: 8, files: [{ path: canonical, sourcePath: source, size: 8, start: 0, end: 7, durationMs: 1000 }] });
  const other = repo.addManifestation({ bookId: book.id, kind: media === "audio" ? "ebook" : "audio" });
  repo.addAsset({ bookId: book.id, manifestationId: other.id, kind: "single", mime: "application/octet-stream",
    totalSize: 8, files: [{ path: "/foreign/same-sized-file", size: 8, start: 0, end: 7, durationMs: 0 }] });
  const bytes = Buffer.from("abcdefgh"), name = media === "audio" ? "renamed.mp3" : "renamed.epub";
  const metainfo = new Uint8Array(bencode.encode({ info: { name, length: 8, "piece length": 4,
    pieces: Buffer.concat([createHash("sha1").update(bytes.subarray(0, 4)).digest(), createHash("sha1").update(bytes.subarray(4)).digest()]) } }));
  const metadata = inspectMetainfo(metainfo), scope = { media, targetId: book.id };
  const result = { mediaType: media, provider: "Other", title: "Reusable book", guid: `copy-${media}`,
    url: "https://fixture.invalid/book-copy", infoHash: metadata.infoHash, sizeBytes: 8, seeders: 1, leechers: 0, raw: {} };
  const feed = repo.releaseInbox.ensureFeed(`book-${media}`, "Other", media, new Date().toISOString());
  const [releaseId] = repo.releaseInbox.ingest(feed.id, [result], new Date().toISOString());
  const command = crossSeedCommandSchema.parse({ operationId: randomUUID(), scope, releaseId });
  const downloadRoot = path.join(settings.libraryRoot, ".podible", "downloads");
  let location: DownloadLocation, running = false, checking = 0, complete = false, seedOnly = false, adds = 0, resumes = 0;
  const client: TorrentClient = {
    id: `transmission:${randomUUID()}`,
    capabilities: { pausedAdd: true, recheck: true, derivedLayouts: true, metainfoVersions: [1], readOnlySeeding: true },
    async ensureAdded(input) { adds++; location = input.location; seedOnly = Boolean(input.seedOnly); return { clientId: this.id, nativeId: metadata.infoHash }; },
    async inspect(ref) { return { ref, location, complete, sizeBytes: 8, bytesDone: complete ? 8 : 0,
      state: checking-- > 0 ? "checking" : running ? "seeding" : "inactive", seedOnly,
      message: null, observedAt: new Date().toISOString(), downloadBytesPerSecond: 0 }; },
    async files() { return [{ index: 0, path: path.join(downloadRoot, location.relativePath, name) }]; },
    async recheck() { checking = 1; complete = (await fs.readFile(path.join(downloadRoot, location.relativePath, name))).equals(bytes); },
    async resume() { throw new Error("Shared book bytes must never use writable resume"); },
    async resumeSeedOnly() { expect(seedOnly).toBe(true); resumes++; running = true; },
    async quiesce() { running = false; }, async removeRecord() { throw new Error("No removal"); },
  };
  const ctx: WorkerContext = { repo, getSettings: () => settings, crossSeedAdapters: {
    client: () => client, fetchMetainfo: async () => metainfo, pollMs: 1, maxPolls: 20,
    materialize: async input => input.capabilities.cow === "clone-force"
      ? { status: "unsupported", reason: "Fixture has no CoW" } : materializeReuseLayout(input),
  } };
  return { root, db, repo, settings, book, scope, asset, manifestation, canonical, source, metainfo, metadata, command, result, ctx, client,
    counts: () => ({ adds, resumes }), close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

for (const media of ["audio", "ebook"] as const) test(`${media} reuses owned hardlinked book content through the durable shared executor`, async () => {
  const f = await fixture(media);
  try {
    const before = f.repo.getAssetFiles(f.asset.id), manifests = f.repo.listManifestationsByBook(f.book.id);
    expect((await fs.lstat(f.canonical)).nlink).toBe(2);
    expect(f.repo.crossSeed.hasFiles(f.scope)).toBe(true);
    expect(f.repo.crossSeed.files(f.scope, [8])).toEqual([expect.objectContaining({ relative_path: f.canonical, size_bytes: 8 })]);
    const plan = await crossSeedPlan(f.repo, f.settings, f.command, f.metainfo);
    expect(plan.sources.map(source => source.path)).toEqual([f.canonical]);
    const receipt = queueCrossSeed(f.repo, f.command);
    const locks = jobLockKeys(f.repo, f.repo.getJob(receipt.jobId)!);
    expect(locks).toContain(`book:${f.book.id}:media:${media}`);
    expect(lockSetsConflict(new Set(locks), [`book:${f.book.id}:media:${media}`])).toBe(true);
    expect(lockSetsConflict(new Set(locks), [`book:${f.book.id}:media:${media === "audio" ? "ebook" : "audio"}`])).toBe(false);
    const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
      retryDelayMs: 1, processJob: (ctx, job) => processCrossSeed(ctx, job),
    });
    try {
      await runtime.reconcile();
      const end = Date.now() + 5000;
      while (!["failed", "succeeded"].includes(f.repo.getJob(receipt.jobId)!.status)) {
        if (Date.now() > end) throw new Error("Book reuse timed out");
        await Bun.sleep(10);
      }
      expect(f.repo.getJob(receipt.jobId)).toMatchObject({ status: "succeeded", error: null });
    } finally { await runtime.stop(); }
    const outcome = f.repo.crossSeed.receipt(f.command.operationId)!.outcome!;
    expect(outcome).toMatchObject({ kind: "seeding", reusedBytes: 8, downloadedBytes: 0 });
    expect(f.counts()).toEqual({ adds: 1, resumes: 1 });
    const record = f.repo.reuseTorrent({ clientId: f.client.id, nativeId: f.metadata.infoHash })!;
    expect(record.ownership_tag).toBe("Podible Books");
    expect(JSON.parse(record.location_json).rootId).toBe("books");
    const linked = (await f.client.files({ clientId: f.client.id, nativeId: f.metadata.infoHash }))[0]!.path;
    expect((await fs.lstat(linked)).isSymbolicLink()).toBe(true);
    expect(await fs.readlink(linked)).toBe(f.canonical);
    expect(await fs.readFile(f.source, "utf8")).toBe("abcdefgh");
    expect(await fs.readFile(f.canonical, "utf8")).toBe("abcdefgh");
    expect(f.repo.getAssetFiles(f.asset.id)).toEqual(before);
    expect(f.repo.listManifestationsByBook(f.book.id)).toEqual(manifests);
    expect(queueCrossSeed(f.repo, f.command)).toMatchObject({ jobId: receipt.jobId });
  } finally { await f.close(); }
});

for (const media of ["audio", "ebook"] as const) test(`${media} initial canonical UUID objects feed the same cross-seed executor`, async () => {
  const f = await fixture(media);
  try {
    const sourceRoot = path.join(f.root, "initial-view"); await fs.mkdir(sourceRoot);
    const contentRoot = bookContentRoot(f.settings.libraryRoot);
    const initial = bencode.encode({ info: { name: media === "audio" ? "initial.mp3" : "initial.epub", length: 8,
      "piece length": 16384, pieces: createHash("sha1").update("abcdefgh").digest() } });
    const plan = (await prepareCanonicalDownload({ contentRoot, sourceRoot, metainfo: initial, clientId: "original-client", allowCreate: true }))!;
    const file = plan.files[0]!, view = path.join(sourceRoot, file.path), canonical = path.join(contentRoot, file.relativePath!);
    await fs.writeFile(view, "abcdefgh");
    await sealCanonicalDownload(contentRoot, sourceRoot, { id: "original-client", sealDownload: async () => {} } as unknown as TorrentClient,
      { clientId: "original-client", nativeId: plan.infoHash });
    await finalizeCanonicalDownloadFile(contentRoot, sourceRoot, view, 8);
    f.db.query("UPDATE asset_files SET path=?,source_path=? WHERE asset_id=?").run(canonical, view, f.asset.id);
    expect((await crossSeedPlan(f.repo, f.settings, f.command, f.metainfo)).sources.map(source => source.path)).toEqual([canonical]);
    const receipt = queueCrossSeed(f.repo, f.command);
    const runtime = await createJobExecutionRuntime(f.ctx, path.join(f.root, "execution.sqlite"), {
      retryDelayMs: 1, processJob: (ctx, job) => processCrossSeed(ctx, job),
    });
    try {
      await runtime.reconcile();
      const deadline = Date.now() + 5000;
      while (!["failed", "succeeded"].includes(f.repo.getJob(receipt.jobId)!.status)) {
        if (Date.now() > deadline) throw new Error("Initial canonical book reuse timed out");
        await Bun.sleep(10);
      }
      expect(f.repo.getJob(receipt.jobId)).toMatchObject({ status: "succeeded", error: null });
    } finally { await runtime.stop(); }
    expect(f.repo.crossSeed.receipt(f.command.operationId)!.outcome).toMatchObject({ kind: "seeding", reusedBytes: 8, downloadedBytes: 0 });
    const linked = (await f.client.files({ clientId: f.client.id, nativeId: f.metadata.infoHash }))[0]!.path;
    expect(await fs.readlink(linked)).toBe(canonical);
    expect(await fs.readFile(canonical, "utf8")).toBe("abcdefgh");
    expect((await fs.stat(canonical)).nlink).toBe(1);
  } finally { await f.close(); }
});

test("book inventory rejects foreign paths and missing or changed ownership records without adopting them", async () => {
  const f = await fixture("audio");
  try {
    const setPath = (value: string) => f.db.query("UPDATE asset_files SET path=? WHERE asset_id=?").run(value, f.asset.id);
    setPath(f.source);
    await expect(crossSeedPlan(f.repo, f.settings, f.command, f.metainfo)).rejects.toThrow("registered canonical");
    setPath(f.canonical);
    const marker = path.join(path.dirname(f.canonical), ".podible-file.json"), record = await fs.readFile(marker, "utf8");
    await fs.unlink(marker);
    expect((await crossSeedPlan(f.repo, f.settings, f.command, f.metainfo)).sources).toHaveLength(0);
    expect(await fs.lstat(marker).catch(() => null)).toBeNull();
    await fs.writeFile(marker, record);
    await fs.writeFile(f.canonical, "xxxxabcd");
    await expect(crossSeedPlan(f.repo, f.settings, f.command, f.metainfo)).rejects.toThrow("identity changed");
    expect(f.counts().adds).toBe(0);
  } finally { await f.close(); }
});

test("book scopes enforce retained media, release association, deletion fencing, source ownership and revision replay", async () => {
  const f = await fixture("ebook");
  try {
    const selected = releaseOfferFromResult({ ...f.result, guid: "original", infoHash: "1".repeat(40) });
    const copy = releaseOfferFromResult(f.result);
    const release = f.repo.createRelease({ bookId: f.book.id, mediaType: "ebook", provider: "Original", title: "Reusable book",
      infoHash: "1".repeat(40), url: "https://fixture.invalid/original" });
    expect(() => rememberSelectedCopies(f.repo, { ...f.scope, media: "audio" }, release.id, selected, [copy])).toThrow("matching-format");
    const group = rememberSelectedCopies(f.repo, f.scope, release.id, selected, [copy]);
    expect(selectedCopyOffer(f.repo, f.scope, group.copies[0]!.id)).toEqual(copy);
    expect(() => selectedCopyOffer(f.repo, { ...f.scope, media: "audio" }, group.copies[0]!.id)).toThrow("another target");
    expect(() => crossSeedOffer(f.repo, { ...f.command, scope: { ...f.scope, media: "audio" } })).toThrow("media type");
    const queued = queueSelectedCrossSeeds(f.repo, f.scope, release.id, selected.key);
    expect(queued).toHaveLength(1);
    expect(queueSelectedCrossSeeds(f.repo, f.scope, release.id, selected.key)[0]!.jobId).toBe(queued[0]!.jobId);
    const revision = f.repo.crossSeed.contentRevision(f.scope);
    f.db.query("UPDATE asset_files SET path=path || '.changed' WHERE asset_id=?").run(f.asset.id);
    expect(f.repo.crossSeed.contentRevision(f.scope)).not.toBe(revision);
    f.repo.bookTorrents.reserve({ bookId: f.book.id, infoHash: release.info_hash, clientId: f.client.id,
      rootPath: path.join(f.settings.libraryRoot, ".podible", "downloads"), rootToken: randomUUID() });
    f.repo.bookTorrents.attach(release.info_hash, release.id);
    expect(f.repo.crossSeed.owns(f.scope, f.client.id, release.info_hash)).toBe(true);
    expect(f.repo.crossSeed.owns({ ...f.scope, media: "audio" }, f.client.id, release.info_hash)).toBe(false);
    f.db.query("DELETE FROM books WHERE id=?").run(f.book.id);
    expect(() => assertCrossSeedDemand(f.repo, f.scope)).toThrow("book is missing");
  } finally { await f.close(); }
});
