import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { managedTransmission, managedTvTransmission, managedBookTransmission, prepareTransmission } from "../../src/media/managed-transmission";
import { TransmissionRpc } from "../../src/media/transmission-client";
import { collectTransferMetrics } from "../../src/media/transfer-metrics-collect";
import { runSnatch } from "../../src/library/service";

const root = await fs.realpath(await fs.mkdtemp("/tmp/transfer-metrics-lab-"));
const engine = path.join(root, "engine"), identity = prepareTransmission(engine);
const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password });
const payloads = new Map<string, Buffer>(), torrents = new Map<string, Uint8Array>();
const webseed = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(request) {
  const pathname = new URL(request.url).pathname;
  if (torrents.has(pathname)) return new Response(torrents.get(pathname)!);
  const data = payloads.get(pathname); if (!data) return new Response(null, { status: 404 });
  const range = /bytes=(\d+)-(\d*)/.exec(request.headers.get("range") ?? "");
  if (!range) return new Response(data);
  const start = Number(range[1]), end = Math.min(data.length - 1, Number(range[2] || data.length - 1));
  return new Response(data.subarray(start, end + 1), { status: 206, headers: { "Content-Range": `bytes ${start}-${end}/${data.length}` } });
} });
function metainfo(name: string, byte: number) {
  const bytes = Buffer.alloc(65536, byte), pieces = [];
  for (let offset = 0; offset < bytes.length; offset += 16384) pieces.push(createHash("sha1").update(bytes.subarray(offset, offset + 16384)).digest());
  payloads.set(`/seed/${name}`, bytes);
  const data = new Uint8Array(bencode.encode({ "url-list": [`http://127.0.0.1:${webseed.port}/seed/${name}`],
    info: { name, length: bytes.length, "piece length": 16384, pieces: Buffer.concat(pieces) } }));
  torrents.set(`/torrents/${name}`, data); return data;
}
let daemon: ReturnType<typeof Bun.spawn> | undefined;
const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db);
const settings = repo.ensureSettings(); settings.libraryRoot = path.join(root, "books");
settings.movies.managedRoot = path.join(root, "movies"); settings.tv.managedRoot = path.join(root, "tv");
await fs.mkdir(settings.libraryRoot); repo.updateSettings(settings);
const until = async (check: () => Promise<boolean>) => {
  const end = Date.now() + 90000;
  while (!await check()) { if (Date.now() > end) throw new Error("Transfer metrics daemon fixture timed out"); await Bun.sleep(100); }
};
try {
  daemon = Bun.spawn(["transmission-daemon", "--foreground", "--config-dir", engine], { stdout: "ignore", stderr: "ignore" });
  await until(async () => { try { await rpc.call("session-get"); return true; } catch { return false; } });
  const movie = managedTransmission(settings.movies.managedRoot, engine), tv = managedTvTransmission(settings.tv.managedRoot, engine);
  const book = managedBookTransmission(path.join(settings.libraryRoot, ".podible", "downloads"), engine);
  const movieLocation = { rootId: "movies", relativePath: "fixture" }, tvLocation = { rootId: "tv", relativePath: "fixture" };
  const movieRef = await movie.ensureAdded({ metainfo: metainfo("fixture.mkv", 10), location: movieLocation });
  const tvRef = await tv.ensureAdded({ metainfo: metainfo("episode.mkv", 20), location: tvLocation });
  repo.reserveReuseTorrent(movieRef, movieLocation, "movie-offer", { media: "movie", targetId: 1 }, randomUUID());
  repo.reserveReuseTorrent(tvRef, tvLocation, "tv-offer", { media: "tv", targetId: 1 }, randomUUID());
  await movie.resume(movieRef); await tv.resume(tvRef);
  metainfo("fixture.pdf", 30);
  const bookRow = repo.createBook({ title: "Telemetry fixture", author: "Synthetic" });
  const snatch = await runSnatch(repo, settings, { bookId: bookRow.id, provider: "Local fixture", title: "Telemetry fixture PDF", mediaType: "ebook",
    url: `http://127.0.0.1:${webseed.port}/torrents/fixture.pdf`, sizeBytes: 65536 }, { bookTorrentClient: book });
  const bookRef = { clientId: movieRef.clientId, nativeId: snatch.release.info_hash };
  await until(async () => (await movie.inspect(movieRef)).complete && (await tv.inspect(tvRef)).complete && (await book.inspect(bookRef)).complete);
  const connection = { clientId: movieRef.clientId, get: (ids: string[], fields: readonly string[]) => rpc.call("torrent-get", { ids, fields: [...fields] }) };
  await until(async () => {
    const result = await connection.get([movieRef.nativeId, tvRef.nativeId, bookRef.nativeId], ["status"]);
    return Array.isArray(result.torrents) && result.torrents.length === 3 && result.torrents.every(row => row.status === 6);
  });
  await collectTransferMetrics(repo, { connection });
  let status = repo.transferMetrics.status();
  assert.equal(status.stale, false); assert.equal(status.summary!.seedingTorrents, 3); assert.equal(status.summary!.seedBytes, 3 * 65536);
  assert.ok(status.summary!.downloadedBytes! >= 3 * 65536);
  assert.equal(repo.transferMetrics.inventoryCount(), 3);
  await movie.quiesce(movieRef); await Bun.sleep(20); await collectTransferMetrics(repo, { connection });
  status = repo.transferMetrics.status(); assert.equal(status.summary!.seedingTorrents, 2);
  assert.equal(status.summary!.seedBytes, 2 * 65536);
  const traffic = status.summary!.downloadedBytes;
  daemon.kill("SIGTERM"); await daemon.exited; daemon = undefined;
  await Bun.sleep(20); await collectTransferMetrics(repo, { connection: null });
  status = repo.transferMetrics.status(); assert.equal(status.stale, true); assert.equal(status.summary!.seedBytes, null);
  assert.equal(status.summary!.downloadedBytes, traffic);
  console.log(JSON.stringify({ verified: "real stable Transmission collector", media: ["movie", "tv", "ebook"],
    registered: 3, seedingBeforePause: 3, seedingAfterPause: 2, downloadedBytes: traffic,
    outagePreservedCounters: true, outageIsUnknownNotZero: true }));
} finally {
  daemon?.kill("SIGTERM"); if (daemon) await daemon.exited;
  db.close(); webseed.stop(true); await fs.rm(root, { recursive: true, force: true });
}
