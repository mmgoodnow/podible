/** Linux-only real Transmission probe. All files, trackers and peers are synthetic. */
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { createHash, randomBytes } from "node:crypto";
import bencode from "bencode";
import { persistSeedOnlyIntent, prepareSeedOnlyResumes, seedOnlyIntentDirectory } from "../../src/media/seed-only-state";

if (process.platform !== "linux") throw new Error("Run this lab inside the Linux Transmission 4.1.4 build image");
const binary = process.env.TRANSMISSION_BINARY ?? "/opt/transmission/bin/transmission-daemon";
const root = await fs.mkdtemp("/tmp/symlink-seeding-");
const bytes = randomBytes(2 * 1024 ** 2 + 213), pieceLength = 16384;
const digest = (data: Uint8Array) => createHash("sha256").update(data).digest("hex");
function encode(value: string | number | Uint8Array | unknown[] | Record<string, unknown>): Buffer {
  if (typeof value === "number") return Buffer.from(`i${value}e`);
  if (typeof value === "string" || value instanceof Uint8Array) {
    const data = Buffer.from(value); return Buffer.concat([Buffer.from(`${data.length}:`), data]);
  }
  if (Array.isArray(value)) return Buffer.concat([Buffer.from("l"), ...value.map(v => encode(v as Parameters<typeof encode>[0])), Buffer.from("e")]);
  return Buffer.concat([Buffer.from("d"), ...Object.keys(value).sort().flatMap(key => [encode(key), encode(value[key] as Parameters<typeof encode>[0])]), Buffer.from("e")]);
}
let webseedRequests = 0;
// Transmission intentionally filters loopback peers received from trackers.
const peerAddress = Object.values(os.networkInterfaces()).flat().find(address => address?.family === "IPv4" && !address.internal)?.address;
if (!peerAddress) throw new Error("Peer upload probe requires a disposable Docker bridge network (not --network none)");
const compactPeer = Buffer.from([...peerAddress.split(".").map(Number), 25143 >> 8, 25143 & 255]);
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(request) {
  const url = new URL(request.url);
  if (url.pathname === "/announce") return new Response(encode({ interval: 1, complete: 1, incomplete: 1,
    peers: compactPeer }));
  if (!url.pathname.endsWith("/movie.mkv")) return new Response(null, { status: 404 });
  webseedRequests++;
  const range = /bytes=(\d+)-(\d*)/.exec(request.headers.get("range") ?? "");
  if (!range) return new Response(bytes);
  const start = Number(range[1]), end = Math.min(bytes.length - 1, range[2] ? Number(range[2]) : bytes.length - 1);
  return new Response(bytes.subarray(start, end + 1), { status: 206,
    headers: { "content-range": `bytes ${start}-${end}/${bytes.length}`, "accept-ranges": "bytes" } });
} });
const endpoint = `http://127.0.0.1:${server.port}`;
const hashes: Buffer[] = [];
for (let start = 0; start < bytes.length; start += pieceLength)
  hashes.push(createHash("sha1").update(bytes.subarray(start, start + pieceLength)).digest());
function metainfo(name: string, webseed: boolean) {
  const info = { name, "piece length": pieceLength, pieces: Buffer.concat(hashes), files: [{ length: bytes.length, path: ["movie.mkv"] }] };
  return { hash: createHash("sha1").update(encode(info)).digest("hex"),
    data: encode({ announce: `${endpoint}/announce`, ...(webseed ? { "url-list": [`${endpoint}/seed/`] } : {}), info }) };
}
type Snapshot = { status: number; percentComplete: number; downloadedEver: number; uploadedEver: number;
  haveValid: number; leftUntilDone: number; fileStats: { wanted: boolean }[]; errorString: string;
  labels: string[]; downloadDir: string };
const fields = ["status", "percentComplete", "downloadedEver", "uploadedEver", "haveValid", "leftUntilDone", "fileStats", "errorString", "labels", "downloadDir"];
async function engine(name: string, rpcPort: number, peerPort: number) {
  const config = path.join(root, name), downloads = path.join(root, `${name}-downloads`);
  await fs.mkdir(config); await fs.mkdir(downloads);
  await fs.writeFile(path.join(config, "settings.json"), JSON.stringify({
    "rpc-enabled": true, "rpc-bind-address": "127.0.0.1", "rpc-port": rpcPort,
    "rpc-authentication-required": false, "rpc-whitelist-enabled": false, "rpc-host-whitelist-enabled": false,
    "peer-port": peerPort, "peer-port-random-on-start": false, "port-forwarding-enabled": false,
    "dht-enabled": false, "pex-enabled": false, "lpd-enabled": false,
    "download-dir": downloads, "incomplete-dir-enabled": false, "rename-partial-files": false,
    "start-added-torrents": false, "seed-queue-enabled": false, "download-queue-enabled": false,
    "ratio-limit-enabled": false, "idle-seeding-limit-enabled": false,
  }));
  let child: ReturnType<typeof Bun.spawn> | undefined, session = "";
  async function rpc(method: string, args: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    for (let attempt = 0; attempt < 2; attempt++) {
      const response = await fetch(`http://127.0.0.1:${rpcPort}/transmission/rpc`, { method: "POST",
        headers: { "content-type": "application/json", "X-Transmission-Session-Id": session },
        body: JSON.stringify({ method, arguments: args }), signal: AbortSignal.timeout(3000) });
      if (response.status === 409) { session = response.headers.get("X-Transmission-Session-Id")!; continue; }
      assert.equal(response.status, 200);
      const body = await response.json() as { result: string; arguments: Record<string, unknown> };
      assert.equal(body.result, "success", JSON.stringify(body)); return body.arguments;
    }
    throw new Error("RPC session negotiation failed");
  }
  return { config, downloads, rpc,
    async start(guard = false) {
      if (guard) assert.equal(prepareSeedOnlyResumes(config, seedOnlyIntentDirectory(config)).length, 1);
      child = Bun.spawn([binary, "--foreground", "--config-dir", config], { stdout: "ignore", stderr: "ignore" });
      for (let i = 0; i < 100; i++) {
        try { await rpc("session-get"); return; } catch { await Bun.sleep(50); }
      }
      throw new Error("Transmission failed to start");
    },
    async stop() {
      if (!child) return;
      child.kill("SIGTERM");
      await Promise.race([child.exited, Bun.sleep(10_000).then(() => { throw new Error("Daemon failed to flush resume state"); })]);
      child = undefined;
    },
    async snapshot(hash: string): Promise<Snapshot> {
      const result = await rpc("torrent-get", { ids: [hash], fields });
      const rows = result.torrents as Snapshot[]; assert.equal(rows.length, 1); return rows[0]!;
    },
  };
}
const seed = await engine("seed", 19091, 25143), leech = await engine("leech", 19092, 25144);
async function until(check: () => Promise<boolean>, description: string, timeout = 90_000) {
  const deadline = Date.now() + timeout;
  while (!await check()) { if (Date.now() > deadline) throw new Error(`Timed out: ${description}`); await Bun.sleep(100); }
}
const candidate = metainfo("Pack", true), canonical = path.join(root, "canonical", "movie.mkv");
try {
  const version = Bun.spawnSync([binary, "--version"]);
  assert.match(Buffer.from(version.stdout).toString() + Buffer.from(version.stderr).toString(), /4\.1\.4/);
  await fs.mkdir(path.dirname(canonical)); await fs.writeFile(canonical, bytes);
  await fs.mkdir(path.join(seed.downloads, "Pack"));
  const link = path.join(seed.downloads, "Pack", "movie.mkv"); await fs.symlink(canonical, link);
  assert.equal((await fs.lstat(link)).isSymbolicLink(), true);
  await seed.start();
  await seed.rpc("torrent-add", { metainfo: candidate.data.toString("base64"), "download-dir": seed.downloads, paused: true, "files-unwanted": [0] });
  await seed.rpc("torrent-set", { ids: [candidate.hash], "files-unwanted": [0] });
  await seed.rpc("torrent-verify", { ids: [candidate.hash] });
  await until(async () => (await seed.snapshot(candidate.hash)).haveValid === bytes.length, "verify exact symlink");
  await seed.rpc("torrent-start", { ids: [candidate.hash] });
  await until(async () => (await seed.snapshot(candidate.hash)).status === 6, "exact unwanted torrent seeding");
  const exact = await seed.snapshot(candidate.hash);
  assert.ok(exact.fileStats.every(file => !file.wanted)); assert.equal(exact.downloadedEver, 0);
  console.log("Exact symlink seeds with all files unwanted and zero downloaded bytes");
  // Confirm upload behavior, not merely the UI's seeding status.
  await leech.start();
  const peerCandidate = metainfo("Pack", false);
  await leech.rpc("torrent-add", { metainfo: peerCandidate.data.toString("base64"), "download-dir": leech.downloads, paused: false });
  await until(async () => (await leech.snapshot(candidate.hash)).percentComplete === 1, "peer download from unwanted seeder");
  assert.ok((await seed.snapshot(candidate.hash)).uploadedEver > 0);
  console.log("Independent leecher received the complete original file from the unwanted seeder");
  assert.equal(digest(await fs.readFile(path.join(leech.downloads, "Pack", "movie.mkv"))), digest(bytes));
  await leech.stop();
  await seed.stop(); await seed.start();
  const restarted = await seed.snapshot(candidate.hash);
  assert.ok(restarted.fileStats.every(file => !file.wanted), "unwanted flags must survive daemon restart before reapplying");
  console.log("Unwanted flags persisted across daemon restart");
  await seed.rpc("torrent-set", { ids: [candidate.hash], "files-unwanted": [0] });
  await seed.rpc("torrent-start", { ids: [candidate.hash] });
  await until(async () => (await seed.snapshot(candidate.hash)).status === 6, "seeding after restart");
  // Corrupt a previously complete shared target while the daemon is stopped.
  await seed.stop();
  const corrupted = Buffer.from(bytes); corrupted[corrupted.length - 1] ^= 1;
  await fs.writeFile(canonical, corrupted);
  const before = await fs.stat(canonical, { bigint: true }), wantedDigest = digest(corrupted), requestsBefore = webseedRequests;
  await seed.start();
  assert.ok((await seed.snapshot(candidate.hash)).fileStats.every(file => !file.wanted));
  await seed.rpc("torrent-set", { ids: [candidate.hash], "files-unwanted": [0] });
  await seed.rpc("torrent-verify", { ids: [candidate.hash] });
  await until(async () => { const s = await seed.snapshot(candidate.hash); return s.status !== 1 && s.status !== 2 && s.haveValid < bytes.length; }, "detect corrupted piece");
  await seed.rpc("torrent-start", { ids: [candidate.hash] });
  for (let i = 0; i < 50; i++) {
    const s = await seed.snapshot(candidate.hash);
    assert.ok(s.fileStats.every(file => !file.wanted)); assert.equal(s.downloadedEver, 0);
    assert.equal(digest(await fs.readFile(canonical)), wantedDigest); await Bun.sleep(100);
  }
  const wrong = await seed.snapshot(candidate.hash), after = await fs.stat(canonical, { bigint: true });
  assert.equal(webseedRequests, requestsBefore, "unwanted torrent must not request repair bytes");
  assert.equal(after.mtimeNs, before.mtimeNs); assert.equal(after.ctimeNs, before.ctimeNs);
  await seed.stop();
  await persistSeedOnlyIntent(seedOnlyIntentDirectory(seed.config), { infoHash: candidate.hash,
    destination: seed.downloads, fileCount: 1, ownershipTag: "Podible Lab" });
  const resumePath = path.join(seed.config, "resume", `${candidate.hash}.resume`);
  assert.ok((await fs.readdir(path.join(seed.config, "torrents"))).includes(`${candidate.hash}.torrent`), "actual daemon metadata must use the expected hash filename");
  const actualResume = bencode.decode(await fs.readFile(resumePath)) as Record<string, unknown>;
  console.log("Real daemon resume fields:", Object.keys(actualResume).sort().join(", "));
  const restoredCases: Record<string, Snapshot> = {};
  for (const damage of ["dnd-and-labels", "missing", "malformed"] as const) {
    if (damage === "missing") await fs.unlink(resumePath);
    else if (damage === "malformed") await fs.writeFile(resumePath, "not-bencoded-resume");
    else await fs.writeFile(resumePath, bencode.encode({ ...actualResume, dnd: [0], labels: [],
      destination: path.join(root, "incorrect-destination"), paused: 0 }));
    const requests = webseedRequests;
    await seed.start(true);
    // No RPC torrent-set/start here: the pre-spawn helper alone must restore protection.
    const restored = await seed.snapshot(candidate.hash);
    assert.ok(restored.fileStats.every(file => !file.wanted), `${damage}: flags were not restored before RPC`);
    assert.ok(restored.labels.includes("Podible Seed Only")); assert.ok(restored.labels.includes("Podible Lab"));
    assert.equal(restored.downloadDir, seed.downloads);
    if (damage !== "dnd-and-labels") assert.equal(restored.status, 0, `${damage}: reconstructed resume must remain paused`);
    await Bun.sleep(1500);
    const settled = await seed.snapshot(candidate.hash);
    assert.equal(settled.downloadedEver, 0);
    assert.equal(webseedRequests, requests, `${damage}: startup triggered a repair request`);
    assert.equal(digest(await fs.readFile(canonical)), wantedDigest);
    const current = await fs.stat(canonical, { bigint: true });
    assert.equal(current.mtimeNs, before.mtimeNs); assert.equal(current.ctimeNs, before.ctimeNs);
    restoredCases[damage] = settled;
    console.log(`Startup guard restored ${damage}; no repair download or canonical write`);
    await seed.stop();
  }
  console.log(JSON.stringify({ result: "passed", version: "4.1.4", exact, restarted, wrong,
    uploadedEver: restarted.uploadedEver, webseedRequests, restoredCases,
    canonicalUnchangedDuringRepairAttempt: true, flagsPersistedAcrossRestart: true,
    note: "Observed RPC behavior, not an OS-enforced write sandbox" }, null, 2));
} finally {
  await leech.stop(); await seed.stop(); server.stop(true); await fs.rm(root, { recursive: true, force: true });
}
