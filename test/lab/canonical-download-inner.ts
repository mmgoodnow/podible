/** Real isolated Transmission initial writes through canonical symlinks, including restart. */
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import bencode from "bencode";
import { TransmissionRpc } from "../../src/media/transmission-client";
import { prepareTransmission, managedTransmission } from "../../src/media/managed-transmission";
import { initializeManagedContent } from "../../src/media/library-view";
import { prepareCanonicalDownload, readCanonicalDownload, sealCanonicalDownload, finalizeCanonicalDownloadFile, type CanonicalDownload } from "../../src/media/canonical-download";
import { readSeedOnlyIntent, seedOnlyIntentDirectory } from "../../src/media/seed-only-state";
import { captureReuseFile, captureReuseRoot } from "../../src/media/reuse-layout";
import { hasVerifiedLocalSeasonPackPrefill } from "../../src/tv/season-pack-reuse";

assert.equal(process.platform, "linux", "Run in the disposable Linux Transmission lab image");
const binary = process.env.TRANSMISSION_BINARY ?? "transmission-daemon";
const version = Bun.spawnSync([binary, "--version"]);
assert.match(Buffer.from(version.stdout).toString() + Buffer.from(version.stderr).toString(), /4\.1\.4/);
const root = await fs.mkdtemp("/tmp/canonical-download-"), engine = path.join(root, "engine");
const downloads = path.join(root, "downloads", "initial"), canonicalRoot = path.join(root, "objects");
const pieceLength = 16 * 1024;
const files = [
  { name: "Pack/feature.mkv", bytes: randomBytes(1024 ** 2 + 413), canonical: true },
  { name: "Pack/tracker.nfo", bytes: randomBytes(239), canonical: false },
  { name: "Pack/nested/episode.mkv", bytes: randomBytes(1536 * 1024 + 71), canonical: true },
  { name: "Pack/nested/release.txt", bytes: randomBytes(311), canonical: false },
];
const singleFile = { name: "audiobook.m4b", bytes: randomBytes(512 * 1024 + 19) };
const payload = Buffer.concat(files.map(file => file.bytes)), pieceHashes: Buffer[] = [];
for (let start = 0; start < payload.length; start += pieceLength)
  pieceHashes.push(createHash("sha1").update(payload.subarray(start, start + pieceLength)).digest());
let offset = 0;
const boundaries = files.slice(0, -1).map(file => { offset += file.bytes.length; return offset; });
assert.ok(boundaries.every(boundary => boundary % pieceLength !== 0), "Fixture must cross content/sidecar file boundaries");
const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
let gate = false, requests = 0, servedBytes = 0;
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
  const pathname = decodeURIComponent(new URL(request.url).pathname);
  if (pathname === "/announce") return new Response(bencode.encode({ interval: 5, peers: Buffer.alloc(0), complete: 0, incomplete: 1 }));
  const fixturePath = pathname.replace(/^\/seed\/Season\//, "/seed/Pack/");
  const file = files.find(file => `/seed/${file.name}` === fixturePath) ?? (pathname === `/seed/${singleFile.name}` ? singleFile : undefined);
  if (!file) return new Response(null, { status: 404 });
  requests++;
  const range = /^bytes=(\d+)-(\d*)$/.exec(request.headers.get("range") ?? "");
  const start = range ? Number(range[1]) : 0;
  const end = range?.[2] ? Math.min(file.bytes.length - 1, Number(range[2])) : file.bytes.length - 1;
  if (start > end || start >= file.bytes.length) return new Response(null, { status: 416 });
  let cursor = start, cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      await Bun.sleep(25);
      while (gate && !cancelled && !request.signal.aborted) await Bun.sleep(10);
      if (cancelled || request.signal.aborted) { controller.close(); return; }
      const next = Math.min(end + 1, cursor + 16 * 1024);
      const chunk = file.bytes.subarray(cursor, next); cursor = next; servedBytes += chunk.length;
      controller.enqueue(chunk);
      if (cursor > end) controller.close();
    }, cancel() { cancelled = true; },
  });
  return new Response(body, { status: range ? 206 : 200, headers: { "content-length": String(end - start + 1),
    "accept-ranges": "bytes", ...(range ? { "content-range": `bytes ${start}-${end}/${file.bytes.length}` } : {}) } });
} });
const endpoint = `http://127.0.0.1:${server.port}`;
const info = { name: "Pack", "piece length": pieceLength, pieces: Buffer.concat(pieceHashes),
  files: files.map(file => ({ length: file.bytes.length, path: file.name.split("/").slice(1) })) };
const metainfo = bencode.encode({ announce: endpoint + "/announce", "url-list": [endpoint + "/seed/"], info });
const hash = createHash("sha1").update(bencode.encode(info)).digest("hex");
let rpc: TransmissionRpc, client: ReturnType<typeof managedTransmission>;
let daemon: ReturnType<typeof Bun.spawn> | undefined;
const daemonLog = path.join(root, "daemon.log");
async function until(check: () => Promise<boolean>, description: string, seconds = 60) {
  const end = Date.now() + seconds * 1000;
  while (!await check()) { if (Date.now() >= end) throw new Error(`Timed out: ${description}`); await Bun.sleep(50); }
}
async function start() {
  prepareTransmission(engine);
  daemon = Bun.spawn([binary, "--foreground", "--config-dir", engine], { stdout: "ignore", stderr: Bun.file(daemonLog) });
  await until(async () => { try { await rpc.call("session-get"); return true; } catch { return false; } }, "daemon startup", 10);
}
async function stop() {
  if (!daemon) return;
  const child = daemon; child.kill("SIGTERM");
  await Promise.race([child.exited, Bun.sleep(10_000).then(() => { child.kill("SIGKILL"); throw new Error("Daemon did not flush a graceful restart"); })]);
  assert.equal(await child.exited, 0); daemon = undefined;
}
type Snapshot = { hashString: string; status: number; downloadedEver: number; haveValid: number; haveUnchecked: number;
  totalSize: number; downloadDir: string; files: { name: string; length: number; bytesCompleted: number }[];
  labels: string[]; fileStats: { wanted: boolean }[] };
async function snapshot(torrentHash = hash): Promise<Snapshot> {
  const result = await rpc.call("torrent-get", { ids: [torrentHash], fields: ["hashString", "status", "downloadedEver", "haveValid", "haveUnchecked", "totalSize", "downloadDir", "files", "labels", "fileStats"] });
  const torrents = result.torrents as Snapshot[]; assert.equal(torrents.length, 1); return torrents[0]!;
}
const originals = new Map<string, { path: string; dev: number; ino: number; linkIno: number }>();
async function assertCanonicalLinks() {
  for (const [name, original] of originals) {
    const link = path.join(downloads, name), stat = await fs.lstat(link), content = await fs.stat(original.path);
    assert.equal(stat.isSymbolicLink(), true, `${name}: daemon replaced canonical symlink`);
    assert.equal(stat.ino, original.linkIno, `${name}: symlink inode changed`);
    assert.equal(await fs.readlink(link), original.path);
    assert.equal(content.dev, original.dev); assert.equal(content.ino, original.ino, `${name}: canonical inode changed`);
  }
}
async function assertNoPayloadCopies(directory: string = downloads) {
  for (const item of await fs.readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, item.name);
    assert.ok(!/\.(?:part|partial)$/i.test(item.name), `Unexpected partial file ${full}`);
    if (item.isDirectory()) await assertNoPayloadCopies(full);
    else if (item.isFile()) {
      const relative = path.relative(downloads, full);
      assert.ok(files.some(file => !file.canonical && file.name === relative), `Unexpected payload copy ${relative}`);
    } else assert.ok(item.isSymbolicLink(), `Unexpected filesystem object ${full}`);
  }
}
try {
  await fs.mkdir(engine); await fs.mkdir(downloads, { recursive: true }); await initializeManagedContent(canonicalRoot);
  await fs.writeFile(path.join(engine, "settings.json"), JSON.stringify({
    "download-dir": downloads, "peer-port": 25149, "port-forwarding-enabled": false,
    "dht-enabled": false, "pex-enabled": false, "lpd-enabled": false, "incomplete-dir-enabled": false,
    "rename-partial-files": false, "start-added-torrents": false, "download-queue-enabled": false,
    "seed-queue-enabled": false, "ratio-limit-enabled": false, "idle-seeding-limit-enabled": false,
  }));
  const identity = prepareTransmission(engine);
  rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password });
  client = managedTransmission(root, engine);
  assert.equal(client.capabilities.canonicalDownloads, true);
  const plan = await prepareCanonicalDownload({ contentRoot: canonicalRoot, sourceRoot: downloads,
    clientId: client.id, metainfo, allowCreate: true });
  assert.ok(plan?.ready); assert.equal(plan.sealed, false);
  for (const file of files) {
    const reserved: CanonicalDownload["files"][number] = plan.files.find(item => item.path === file.name)!;
    assert.equal(reserved.relativePath !== null, file.canonical);
    if (!reserved.relativePath) continue;
    const canonical = path.join(canonicalRoot, reserved.relativePath), link = path.join(downloads, file.name);
    const stat = await fs.stat(canonical);
    originals.set(file.name, { path: canonical, dev: stat.dev, ino: stat.ino, linkIno: (await fs.lstat(link)).ino });
    assert.equal(stat.size, 0, "Initial reservations must be empty, not full-sized sparse zero files");
  }
  console.log("Application helper reserved two truly empty canonical objects and writable symlinks; sidecars remain tracker-specific regular files");
  await start();
  const ref = await client.ensureAdded({ metainfo, location: { rootId: "movies", relativePath: "initial" } });
  assert.equal(ref.nativeId, hash); assert.equal((await client.inspect(ref)).state, "inactive");
  assert.equal((await snapshot()).haveValid, 0); assert.equal(requests, 0);
  await assert.rejects(finalizeCanonicalDownloadFile(canonicalRoot, downloads, path.join(downloads, files[0]!.name), files[0]!.bytes.length), /active writer/);
  await client.resume(ref);
  await until(async () => { const s = await snapshot(); return s.haveValid > 0 && s.haveValid < payload.length && s.downloadedEver > 0; }, "initial verified piece written through symlink");
  gate = true;
  const mid = await snapshot();
  assert.ok(mid.haveValid > 0 && mid.haveValid < payload.length);
  await assertCanonicalLinks(); await assertNoPayloadCopies();
  const midCanonical = await Promise.all([...originals.values()].map(async file => ({ path: path.basename(file.path), size: (await fs.stat(file.path)).size })));
  console.log(JSON.stringify({ stage: "mid-download", haveValid: mid.haveValid, totalSize: mid.totalSize, downloadedEver: mid.downloadedEver, midCanonical }));
  await stop();
  await assertCanonicalLinks(); await assertNoPayloadCopies();
  const flushed = Buffer.alloc(payload.length);
  let flushedOffset = 0;
  for (const file of files) {
    const present = await fs.readFile(path.join(downloads, file.name)).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error; return Buffer.alloc(0);
    });
    present.copy(flushed, flushedOffset, 0, file.bytes.length); flushedOffset += file.bytes.length;
  }
  let flushedVerifiedBytes = 0;
  for (let index = 0; index < pieceHashes.length; index++) {
    const piece = flushed.subarray(index * pieceLength, (index + 1) * pieceLength);
    if (createHash("sha1").update(piece).digest().equals(pieceHashes[index]!)) flushedVerifiedBytes += piece.length;
  }
  assert.ok(flushedVerifiedBytes >= mid.haveValid, "Graceful stop lost verified bytes instead of flushing canonical files");
  assert.ok((await Promise.all([...originals.values()].map(file => fs.stat(file.path)))).some(stat => stat.size > 0),
    "Verified partial bytes were not written to an initially empty canonical target");
  await start();
  const replayed = await prepareCanonicalDownload({ contentRoot: canonicalRoot, sourceRoot: downloads, clientId: client.id,
    metainfo, allowCreate: false });
  assert.deepEqual(replayed, plan, "Restart changed the application's canonical reservation");
  const restarted = await snapshot();
  assert.equal(restarted.hashString, hash); assert.equal(restarted.downloadDir, downloads);
  assert.ok(restarted.haveValid > 0 && restarted.haveValid < payload.length, "Restart did not retain partial verified progress");
  gate = false;
  await client.resume(ref);
  await until(async () => { const s = await snapshot(); return s.haveValid === payload.length && s.haveUnchecked === 0 && s.status === 6; }, "complete initial download after restart", 90);
  const complete = await snapshot();
  await assertCanonicalLinks(); await assertNoPayloadCopies();
  await sealCanonicalDownload(canonicalRoot, downloads, client, ref);
  assert.equal((await readCanonicalDownload(canonicalRoot, downloads))?.sealed, true);
  const sealed = await snapshot();
  assert.equal(sealed.fileStats.every(file => !file.wanted), true);
  assert.ok(sealed.labels.includes("Podible Seed Only"));
  assert.ok(readSeedOnlyIntent(seedOnlyIntentDirectory(engine), hash));
  const finalized: unknown[] = [];
  for (const file of files) {
    const target = path.join(downloads, file.name);
    assert.equal(sha256(await fs.readFile(target)), sha256(file.bytes), `${file.name}: final payload mismatch`);
    assert.equal((await fs.stat(target)).size, file.bytes.length);
    if (!file.canonical) assert.equal((await fs.lstat(target)).isFile(), true, "Sidecar must remain a tracker-specific regular file");
    const object = await finalizeCanonicalDownloadFile(canonicalRoot, downloads, target, file.bytes.length);
    if (file.canonical) {
      assert.equal(object?.sizeBytes, file.bytes.length);
      assert.equal(Object.hasOwn(object!, "sha256"), false);
      assert.deepEqual(await finalizeCanonicalDownloadFile(canonicalRoot, downloads, target, file.bytes.length), object);
      finalized.push(object);
    } else assert.equal(object, null);
  }
  const finalPayload = Buffer.concat(await Promise.all(files.map(file => fs.readFile(path.join(downloads, file.name)))));
  for (let index = 0; index < pieceHashes.length; index++)
    assert.equal(createHash("sha1").update(finalPayload.subarray(index * pieceLength, (index + 1) * pieceLength)).digest("hex"), pieceHashes[index]!.toString("hex"));
  await stop(); await assertCanonicalLinks(); await assertNoPayloadCopies();
  await start();
  const sealedRestart = await snapshot();
  assert.equal(sealedRestart.fileStats.every(file => !file.wanted), true, "Startup guard lost sealed unwanted flags");
  assert.ok(sealedRestart.labels.includes("Podible Seed Only"));
  await client.resume(ref);
  await until(async () => (await client.inspect(ref)).state === "seeding", "sealed seed-only restart");
  assert.equal((await snapshot()).downloadedEver, complete.downloadedEver, "Sealed restart downloaded replacement bytes");
  await assertCanonicalLinks(); await assertNoPayloadCopies();
  for (const file of files.filter(item => item.canonical))
    assert.equal((await finalizeCanonicalDownloadFile(canonicalRoot, downloads, path.join(downloads, file.name), file.bytes.length))?.sizeBytes, file.bytes.length);

  // Single-file metainfo has no top-level directory: exercise that distinct client path too.
  const singleRoot = path.join(root, "downloads", "single"); await fs.mkdir(singleRoot);
  const singlePieces: Buffer[] = [];
  for (let start = 0; start < singleFile.bytes.length; start += pieceLength)
    singlePieces.push(createHash("sha1").update(singleFile.bytes.subarray(start, start + pieceLength)).digest());
  const singleInfo = { name: singleFile.name, length: singleFile.bytes.length, "piece length": pieceLength, pieces: Buffer.concat(singlePieces) };
  const singleMetainfo = bencode.encode({ announce: endpoint + "/announce", "url-list": [endpoint + `/seed/${singleFile.name}`], info: singleInfo });
  const singlePlan = await prepareCanonicalDownload({ contentRoot: canonicalRoot, sourceRoot: singleRoot,
    clientId: client.id, metainfo: singleMetainfo, allowCreate: true });
  assert.ok(singlePlan?.ready); assert.equal(singlePlan.files.length, 1);
  const singleMember = singlePlan.files[0]!; assert.ok(singleMember.relativePath);
  assert.equal(singleMember.path, singleFile.name);
  const singleCanonical = path.join(canonicalRoot, singleMember.relativePath), singleView = path.join(singleRoot, singleFile.name);
  const originalSingle = await fs.stat(singleCanonical), originalSingleLink = await fs.lstat(singleView);
  assert.equal(originalSingle.size, 0); assert.ok(originalSingleLink.isSymbolicLink());
  assert.equal(await fs.readlink(singleView), singleCanonical);
  const singleRef = await client.ensureAdded({ metainfo: singleMetainfo, location: { rootId: "movies", relativePath: "single" } });
  assert.equal(singleRef.nativeId, singlePlan.infoHash); assert.equal((await snapshot(singleRef.nativeId)).haveValid, 0);
  await client.resume(singleRef);
  await until(async () => (await client.inspect(singleRef)).complete, "single-file initial audiobook download");
  await sealCanonicalDownload(canonicalRoot, singleRoot, client, singleRef);
  const singleFinalized = await finalizeCanonicalDownloadFile(canonicalRoot, singleRoot, singleView, singleFile.bytes.length);
  assert.equal(singleFinalized?.sizeBytes, singleFile.bytes.length);
  assert.equal(sha256(await fs.readFile(singleCanonical)), sha256(singleFile.bytes));
  const assertSingle = async () => {
    const canonical = await fs.stat(singleCanonical), link = await fs.lstat(singleView);
    assert.equal(canonical.dev, originalSingle.dev); assert.equal(canonical.ino, originalSingle.ino);
    assert.equal(link.ino, originalSingleLink.ino); assert.ok(link.isSymbolicLink());
    assert.equal(await fs.readlink(singleView), singleCanonical);
    assert.deepEqual(await fs.readdir(singleRoot), [singleFile.name], "Single-file view contains partial or duplicate payloads");
  };
  await assertSingle();
  const singleSealed = await snapshot(singleRef.nativeId);
  assert.ok(singleSealed.fileStats.every(file => !file.wanted));
  await stop(); await start();
  assert.ok((await snapshot(singleRef.nativeId)).fileStats.every(file => !file.wanted));
  await client.resume(singleRef);
  await until(async () => (await client.inspect(singleRef)).state === "seeding", "single-file sealed restart");
  assert.equal((await snapshot(singleRef.nativeId)).downloadedEver, singleSealed.downloadedEver);
  await assertSingle();
  assert.deepEqual(await finalizeCanonicalDownloadFile(canonicalRoot, singleRoot, singleView, singleFile.bytes.length), singleFinalized);
  await assertCanonicalLinks(); await assertNoPayloadCopies();

  const seasonRoot = path.join(root, "downloads", "season"); await fs.mkdir(seasonRoot);
  const reusable = files[2]!, missing = files[0]!, seasonPayload = Buffer.concat([reusable.bytes, missing.bytes]);
  const seasonPieces: Buffer[] = [];
  for (let start = 0; start < seasonPayload.length; start += pieceLength)
    seasonPieces.push(createHash("sha1").update(seasonPayload.subarray(start, start + pieceLength)).digest());
  const seasonInfo = { name: "Season", "piece length": pieceLength, pieces: Buffer.concat(seasonPieces),
    files: [reusable, missing].map(file => ({ length: file.bytes.length, path: file.name.split("/").slice(1) })) };
  const seasonMeta = bencode.encode({ announce: endpoint + "/announce", "url-list": [endpoint + "/seed/"], info: seasonInfo });
  const source = await captureReuseFile({ root: await captureReuseRoot({ rootPath: canonicalRoot, markerName: ".podible-content.json" }),
    sourceId: reusable.name, sourcePath: originals.get(reusable.name)!.path });
  const seasonPath = reusable.name.replace(/^Pack\//, "Season/");
  const seasonPlan = await prepareCanonicalDownload({ contentRoot: canonicalRoot, sourceRoot: seasonRoot, clientId: client.id,
    metainfo: seasonMeta, allowCreate: true, prefill: new Map([[seasonPath, source]]) });
  assert.ok(seasonPlan?.ready);
  assert.notEqual(String((await fs.stat(path.join(seasonRoot, seasonPath), { bigint: true })).ino), source.identity.inode);
  const seasonRef = await client.ensureAdded({ metainfo: seasonMeta, location: { rootId: "movies", relativePath: "season" } });
  if ((await client.inspect(seasonRef)).state === "inactive") await client.recheck(seasonRef);
  const checking = await client.inspect(seasonRef);
  assert.ok(checking.state === "checking" || hasVerifiedLocalSeasonPackPrefill(checking, seasonPayload.length));
  await until(async () => {
    const seen = await client.inspect(seasonRef);
    return hasVerifiedLocalSeasonPackPrefill(seen, seasonPayload.length);
  }, "paused season pack reuse verification");
  const prefilled = await snapshot(seasonRef.nativeId);
  assert.equal(prefilled.downloadedEver, 0, "No network transfer is permitted during preflight");
  assert.equal(prefilled.haveValid, Math.floor(reusable.bytes.length / pieceLength) * pieceLength);
  await client.resume(seasonRef);
  await until(async () => (await client.inspect(seasonRef)).complete, "download only missing season pieces");
  const seasonComplete = await snapshot(seasonRef.nativeId);
  assert.equal(seasonComplete.downloadedEver, seasonPayload.length - prefilled.haveValid);
  await sealCanonicalDownload(canonicalRoot, seasonRoot, client, seasonRef);
  assert.equal(sha256(await fs.readFile(source.path)), sha256(reusable.bytes));
  await assertCanonicalLinks();
  await stop();
  console.log(JSON.stringify({ result: "passed", version: "4.1.4", initiallyVerifiedCanonicalBytes: 0, totalBytes: payload.length,
    boundaryOffsets: boundaries, crossFilePieces: boundaries.length, flushedVerifiedBytes, restartHaveValid: restarted.haveValid,
    finalHaveValid: complete.haveValid, downloadedEver: complete.downloadedEver, requests, servedBytes,
    canonicalInodesPreserved: true, symlinkInodesPreserved: true, noPartialOrDuplicatePayloads: true,
    applicationManifestReplay: true, startupGuardPreservedSealedState: true, finalized,
    seasonPack: { bytes: seasonPayload.length, verifiedBeforeStart: prefilled.haveValid, downloadedEver: seasonComplete.downloadedEver },
    singleFile: { name: singleFile.name, bytes: singleFile.bytes.length, initiallyEmpty: true,
      canonicalInodePreserved: true, symlinkInodePreserved: true, sealedRestart: true,
      noPartialOrDuplicatePayloads: true, sha256: sha256(singleFile.bytes) },
    canonicalSha256: files.filter(file => file.canonical).map(file => ({ name: file.name, sha256: sha256(file.bytes) })),
    note: "Actual application helper, managed adapter, finalization and startup guard against Transmission 4.1.4." }, null, 2));
} catch (error) {
  console.error(await fs.readFile(daemonLog, "utf8").catch(() => "No daemon log")); throw error;
} finally {
  gate = false;
  if (daemon) { daemon.kill("SIGTERM"); await daemon.exited; }
  server.stop(true); await fs.rm(root, { recursive: true, force: true });
}
