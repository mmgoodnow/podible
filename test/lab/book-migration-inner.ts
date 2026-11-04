import { Database } from "bun:sqlite";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { inspectMetainfo } from "../../src/media/metainfo";
import { prepareTransmission } from "../../src/media/managed-transmission";
import { TransmissionRpc } from "../../src/media/transmission-client";
import { reconcileBookStorage } from "../../src/library/book-publication";
import { runMigrationCli } from "../../scripts/migrate-book-torrents";
import { startMockRtorrent } from "../mocks/rtorrent";

const root = await fs.mkdtemp("/tmp/book-migration-lab-");
const library = path.join(root, "library");
const config = path.join(root, "config");
const engine = path.join(config, "transmission");
const identity = prepareTransmission(engine);
const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc",
  username: identity.username, password: identity.password });
const database = path.join(config, "podible.sqlite");
const db = new Database(database);
runMigrations(db);
const repo = new BooksRepo(db);
const fixtures = [];
for (const [index, tag] of ["Podible", "LazyLibrarian", "Podible"].entries()) {
  const name = `Bundle${index}`;
  const contents = [Buffer.from(`%PDF-1.4\nVerified legacy book ${index}\n`), Buffer.from(`NFO ${index}`)];
  const files = ["book.pdf", "release.nfo"];
  const torrent = new Uint8Array(bencode.encode({ info: { name, "piece length": 16384,
    pieces: createHash("sha1").update(Buffer.concat(contents)).digest(),
    files: files.map((file, i) => ({ length: contents[i]!.length, path: [file] })) } }));
  const metadata = inspectMetainfo(torrent);
  const source = path.join(root, "raw", name);
  await fs.mkdir(source, { recursive: true });
  for (const [i, file] of files.entries()) await fs.writeFile(path.join(source, file), index === 2 ? Buffer.alloc(contents[i]!.length, 0) : contents[i]!);
  const book = repo.createBook({ title: `Legacy ${index}`, author: "Synthetic" });
  const release = repo.createRelease({ bookId: book.id, provider: "Fixture", title: book.title,
    mediaType: "ebook", infoHash: metadata.infoHash, url: `https://fixture.invalid/${index}`, status: "imported" });
  repo.putTorrentCache({ key: metadata.infoHash, infoHash: metadata.infoHash, url: release.url, torrentBytes: torrent });
  const manifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
  const view = path.join(library, "Synthetic", book.title, "book.pdf");
  await fs.mkdir(path.dirname(view), { recursive: true });
  await fs.link(path.join(source, files[0]!), view);
  const asset = repo.addAsset({ bookId: book.id, manifestationId: manifestation.id,
    sourceReleaseId: release.id, kind: "single", mime: "application/pdf", totalSize: contents[0]!.length,
    files: [{ path: view, sourcePath: path.join(source, files[0]!), size: contents[0]!.length, start: 0, end: contents[0]!.length - 1, durationMs: 0 }] });
  fixtures.push({ tag, name, source, files, metadata, release, asset, contents });
}
const legacy = startMockRtorrent({ preloaded: fixtures.map((entry) => entry.metadata.infoHash),
  byHash: Object.fromEntries(fixtures.map((entry) => [entry.metadata.infoHash, {
    name: entry.name, basePath: entry.source, directory: entry.source,
    isMultiFile: true, filePaths: entry.files.map((file) => path.join(entry.source, file)),
    sizeBytes: entry.metadata.size, completeAfterPolls: 0, ownershipTag: entry.tag,
  }])) });
repo.updateSettings({ ...repo.ensureSettings(), libraryRoot: library,
  rtorrent: { ...repo.getSettings().rtorrent, url: legacy.url } });
await reconcileBookStorage(repo, library);
const stableFiles = fixtures.map((entry) => repo.getAssetFiles(entry.asset.id)[0]!);
let daemon: ReturnType<typeof Bun.spawn> | undefined;
async function start() {
  daemon = Bun.spawn(["transmission-daemon", "--foreground", "--config-dir", engine], { stdout: "ignore", stderr: "ignore" });
  for (let i = 0; i < 100; i++) {
    try { await rpc.call("session-get"); return; } catch { await Bun.sleep(100); }
  }
  throw new Error("Migration lab daemon did not start");
}
async function stop() { daemon?.kill("SIGTERM"); if (daemon) await daemon.exited; }
const args = ["--db", database, "--config-dir", config];
try {
  await start();
  if (await runMigrationCli(args) !== 0 || legacy.controlCalls.length) throw new Error("Dry-run was not read-only");
  const report = path.join(config, "migration.json");
  for (const entry of fixtures.slice(0, 2)) {
    if (await runMigrationCli([...args, "--apply", "--workers-stopped", "--report", report, "--hash", entry.metadata.infoHash]) !== 0)
      throw new Error("Verified migration failed");
    const row = repo.getAssetFiles(entry.asset.id)[0]!;
    const original = stableFiles[fixtures.indexOf(entry)]!;
    const target = path.join(library, ".podible", "downloads", entry.metadata.infoHash, entry.name, "book.pdf");
    if (row.path !== original.path || row.id !== original.id || row.identity_path !== original.identity_path || row.source_path !== target)
      throw new Error("Migration changed file identity or failed source-path cutover");
    if ((await fs.stat(target)).ino !== (await fs.stat(row.path)).ino) throw new Error("Migration copied book bytes");
    const observed = await rpc.call("torrent-get", { ids: [entry.metadata.infoHash], fields: ["hashString", "downloadedEver", "percentDone"] });
    const torrent = (observed.torrents as any[])[0];
    if (torrent.downloadedEver !== 0 || torrent.percentDone !== 1) throw new Error("Migration downloaded bytes");
  }
  const bad = fixtures[2]!;
  if (await runMigrationCli([...args, "--apply", "--workers-stopped", "--report", report, "--hash", bad.metadata.infoHash]) !== 1)
    throw new Error("Corrupt source was accepted");
  if (repo.bookTorrents.get(bad.metadata.infoHash)) throw new Error("Corrupt source cut over its binding");
  const rejected = await rpc.call("torrent-get", { ids: [bad.metadata.infoHash], fields: ["status", "downloadedEver"] });
  if ((rejected.torrents as any[])[0].status !== 0 || (rejected.torrents as any[])[0].downloadedEver !== 0)
    throw new Error("Corrupt migration resumed or downloaded");
  await stop(); prepareTransmission(engine); await start();
  if (await runMigrationCli([...args, "--apply", "--workers-stopped", "--report", report, "--hash", fixtures[0]!.metadata.infoHash]) !== 0)
    throw new Error("Restart/replay migration failed");
  if (legacy.controlCalls.includes("d.erase") || legacy.loadCalls.length) throw new Error("Migration erased or redownloaded a source");
  console.log("Book migration real-daemon acceptance passed: Podible + LL, full piece verification, zero downloaded bytes, source-path cutover, corrupt-data rejection, persistent restart/replay, retained source records.");
} finally {
  await stop(); legacy.stop(); db.close(); await fs.rm(root, { recursive: true, force: true });
}
