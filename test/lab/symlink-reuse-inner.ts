/** Real application reuse execution on a non-CoW Linux filesystem; no external services. */
import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import { constants } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { prepareTransmission, managedTransmission, managedBookTransmission } from "../../src/media/managed-transmission";
import { adoptBookContent, reconcileBookView } from "../../src/library/book-storage";
import type { CrossSeedScope } from "../../src/media/cross-seed-state";
import { TransmissionRpc } from "../../src/media/transmission-client";
import { rememberSelectedCopies } from "../../src/media/selected-copies";
import { queueSelectedCrossSeeds, processCrossSeed } from "../../src/media/cross-seed";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { inspectMetainfo } from "../../src/media/metainfo";

assert.equal(process.platform, "linux", "Run inside the isolated Linux lab with /tmp on tmpfs");
const base = await fs.mkdtemp("/tmp/podible-symlink-reuse-"), root = path.join(base, "media"), engine = path.join(base, "engine");
const db = new Database(path.join(base, "app.sqlite")); runMigrations(db);
const repo = new BooksRepo(db), settings = repo.ensureSettings();
const bookMedia = process.argv.includes("--audio") ? "audio" : process.argv.includes("--ebook") ? "ebook" : null;
settings.movies.managedRoot = root; settings.libraryRoot = root; repo.updateSettings(settings);
const data = Buffer.alloc(2 * 1024 ** 2 + 217, 43), pieceLength = 16384, pieces: Buffer[] = [];
for (let offset = 0; offset < data.length; offset += pieceLength)
  pieces.push(createHash("sha1").update(data.subarray(offset, offset + pieceLength)).digest());
let requests = 0;
const requestedPaths: string[] = [];
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(request) {
  const pathname = new URL(request.url).pathname;
  requestedPaths.push(pathname);
  if (pathname === "/announce") return new Response(bencode.encode({ interval: 60, peers: Buffer.alloc(0), complete: 1, incomplete: 0 }));
  requests++; return new Response(data);
} });
const endpoint = `http://127.0.0.1:${server.port}`;
const bytes = bencode.encode({ announce: endpoint + "/announce", "url-list": [endpoint + "/data"],
  info: { name: `renamed.${bookMedia === "ebook" ? "epub" : bookMedia === "audio" ? "m4b" : "mkv"}`, length: data.length, "piece length": pieceLength, pieces: Buffer.concat(pieces) } });
const metadata = inspectMetainfo(bytes);
let daemon: ReturnType<typeof Bun.spawn> | undefined;
let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
async function until(predicate: () => Promise<boolean>, message: string) {
  const deadline = Date.now() + 45_000;
  while (!await predicate()) { if (Date.now() > deadline) throw new Error(message); await Bun.sleep(100); }
}
try {
  await fs.mkdir(root);
  const identity = prepareTransmission(engine);
  const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password });
  const start = async () => {
    prepareTransmission(engine);
    daemon = Bun.spawn(["transmission-daemon", "--foreground", "--config-dir", engine], { stdout: "ignore", stderr: "ignore" });
    await until(async () => { try { await rpc.call("session-get"); return true; } catch { return false; } }, "Daemon did not start");
  };
  const stop = async () => { daemon?.kill("SIGTERM"); await daemon?.exited; daemon = undefined; };
  await start();
  const client = bookMedia ? managedBookTransmission(path.join(root, ".podible", "downloads"), engine) : managedTransmission(root, engine);
  let scope: CrossSeedScope, associationId: number, canonical: string, assertSelection: () => void;
  if (bookMedia) {
    const source = path.join(base, `source.${bookMedia === "audio" ? "m4b" : "epub"}`);
    await fs.writeFile(source, data);
    canonical = await adoptBookContent(root, source);
    const book = repo.createBook({ title: "Synthetic Book", author: "Synthetic Author" });
    const release = repo.createRelease({ bookId: book.id, provider: "First", title: book.title,
      mediaType: bookMedia, infoHash: "a".repeat(40), url: "https://fixture.invalid/original", status: "imported" });
    const manifestation = repo.addManifestation({ bookId: book.id, kind: bookMedia });
    repo.addAsset({ bookId: book.id, manifestationId: manifestation.id, sourceReleaseId: release.id, kind: "single",
      mime: bookMedia === "audio" ? "audio/mp4" : "application/epub+zip", totalSize: data.length,
      files: [{ path: canonical, size: data.length, start: 0, end: data.length, durationMs: 1000 }] });
    await reconcileBookView(repo, root);
    const before = JSON.stringify(repo.listManifestationsByBook(book.id));
    scope = { media: bookMedia, targetId: book.id }; associationId = release.id;
    assertSelection = () => assert.equal(JSON.stringify(repo.listManifestationsByBook(book.id)), before);
  } else {
    await prepareMovieStorage(repo, path.join(root, "objects"));
    const user = repo.upsertUser({ provider: "local", providerUserId: "symlink", username: "symlink", isAdmin: true });
    const request = repo.movies.request(user.id, fixtureMovie, settings.movies.qualityPolicy);
    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "movie_acquire" }).id).acquisition;
    const fileId = randomUUID(), relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`;
    canonical = path.join(root, "objects", relativePath);
    await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, data, { mode: 0o444 });
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id: fileId, relativePath, sizeBytes: data.length, probe: {} }]);
    repo.movies.selectVersion(request.targetId, version, repo.movies.target(request.targetId)!.revision);
    repo.movies.finishAcquisition(acquisition.id);
    scope = { media: "movie", targetId: request.targetId }; associationId = acquisition.id;
    assertSelection = () => assert.equal(repo.movies.target(request.targetId)!.active_version_id, version);
  }
  await assert.rejects(fs.copyFile(canonical, path.join(base, "clone-probe"), constants.COPYFILE_EXCL | constants.COPYFILE_FICLONE_FORCE));
  const primary = { key: "primary", title: "Fixture", provider: "First", sizeBytes: data.length, seeders: 1,
    url: "https://fixture.invalid/private", infoHash: null, attributes: {} };
  rememberSelectedCopies(repo, scope, associationId, primary, [{ ...primary, key: "other", provider: "Other", infoHash: metadata.infoHash }]);
  const [receipt] = queueSelectedCrossSeeds(repo, scope, associationId, primary.key);
  const context = { repo, getSettings: () => repo.getSettings(), crossSeedAdapters: {
    client: () => client, fetchMetainfo: async () => bytes, pollMs: 100, maxPolls: 500,
  } };
  runtime = await createJobExecutionRuntime(context, path.join(base, "execution.sqlite"), {
    processJob: (ctx, job) => job.type === "torrent_reuse" ? processCrossSeed(ctx, job) : Promise.resolve("done" as const),
  });
  await runtime.reconcile();
  await until(async () => ["succeeded", "failed"].includes(repo.getJob(receipt!.jobId)?.status ?? ""), "Reuse did not finish");
  assert.equal(repo.getJob(receipt!.jobId)?.error, null);
  assert.equal(repo.crossSeed.receipt(receipt!.command.operationId)?.outcome?.kind, "seeding");
  const ref = { clientId: client.id, nativeId: metadata.infoHash };
  const linked = (await client.files(ref))[0]!.path;
  assert.equal((await fs.lstat(linked)).isSymbolicLink(), true);
  assert.equal(await fs.readlink(linked), canonical);
  assert.deepEqual(await fs.readFile(canonical), data);
  assertSelection();
  await stop(); await start();
  await client.resumeSeedOnly(ref);
  await until(async () => (await client.inspect(ref)).state === "seeding", "Seed-only registration did not resume");
  assert.equal((await client.inspect(ref)).seedOnly, true);
  assert.equal(requests, 0, `Unexpected non-announce requests: ${JSON.stringify(requestedPaths)}`);
  console.log(JSON.stringify({ result: "passed", media: scope.media, cow: "unavailable", layout: "symlink", downloadedBytes: 0,
    restartedSeedOnly: true, canonicalUnchanged: true, selectedVersionUnchanged: true }));
} finally {
  await runtime?.stop(); daemon?.kill("SIGTERM"); await daemon?.exited;
  server.stop(true); db.close(); await fs.rm(base, { recursive: true, force: true });
}
