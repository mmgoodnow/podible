/** Real local daemon with host-native CoW storage. All media and trackers are synthetic. */
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { createHash, randomUUID } from "node:crypto";
import bencode from "bencode";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { prepareTransmission } from "../../src/media/managed-transmission";
import { TransmissionAdapter, TransmissionRpc } from "../../src/media/transmission-client";
import { inspectMetainfo } from "../../src/media/metainfo";
import { prepareMovieStorage } from "../../src/movies/storage";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { processCrossSeed, queueCrossSeed, queueSelectedCrossSeeds } from "../../src/media/cross-seed";
import { rememberSelectedCopies } from "../../src/media/selected-copies";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { reconcileLibraryView } from "../../src/media/library-view";

const base = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "podible-crossseed-lab-")));
const name = `podible-crossseed-${randomUUID().slice(0, 8)}`;
const original = Buffer.alloc(2 * 1024 ** 2 + 213, 42), desired = Buffer.from(original);
desired[desired.length - 1] = 43;
const files = [{ path: "Pack/renamed.mkv", bytes: desired }, { path: "Pack/tracker.nfo", bytes: Buffer.from("candidate tracker sidecar") }];
const root = path.join(base, "media"), engine = path.join(base, "engine");
await fs.mkdir(root);
const identity = prepareTransmission(engine);
const configPath = path.join(engine, "settings.json");
const settingsFile = JSON.parse(await fs.readFile(configPath, "utf8"));
Object.assign(settingsFile, { "rpc-bind-address": "0.0.0.0", "rpc-whitelist-enabled": false, "rpc-host-whitelist-enabled": false });
await fs.writeFile(configPath, JSON.stringify(settingsFile), { mode: 0o600 });
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(request) {
  const url = new URL(request.url);
  if (url.pathname === "/announce") return new Response(bencode.encode({ interval: 60, peers: Buffer.alloc(0), complete: 1, incomplete: 0 }));
  if (url.pathname === "/unavailable") return new Response(bencode.encode({ "failure reason": "Temporary tracker outage" }));
  const file = files.find(file => url.pathname === `/seed/${file.path}`);
  if (!file) return new Response(null, { status: 404 });
  const range = /bytes=(\d+)-(\d*)/.exec(request.headers.get("range") ?? "");
  if (!range) return new Response(file.bytes);
  const from = Number(range[1]), to = Math.min(file.bytes.length - 1, range[2] ? Number(range[2]) : file.bytes.length - 1);
  return new Response(file.bytes.subarray(from, to + 1), { status: 206, headers: { "content-range": `bytes ${from}-${to}/${file.bytes.length}` } });
} });
const endpoint = `http://host.docker.internal:${server.port}`;
function meta(name: string, entries: { path: string; bytes: Buffer }[]) {
  const data = Buffer.concat(entries.map(file => file.bytes)), hashes: Buffer[] = [];
  for (let i = 0; i < data.length; i += 16384) hashes.push(createHash("sha1").update(data.subarray(i, i + 16384)).digest());
  return new Uint8Array(bencode.encode({ announce: `${endpoint}/announce`,
    "announce-list": [[`${endpoint}/announce`], [`${endpoint}/unavailable`]], "url-list": [`${endpoint}/seed/`],
    info: { name, "piece length": 16384, pieces: Buffer.concat(hashes),
      files: entries.map(file => ({ path: [path.basename(file.path)], length: file.bytes.length })) } }));
}
async function docker(...args: string[]) {
  const child = Bun.spawn(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  const text = await new Response(child.stdout).text(), errors = await new Response(child.stderr).text();
  if (await child.exited) throw new Error(`Local daemon lab failed: ${errors}`);
  return text.trim();
}
const db = new Database(path.join(base, "app.sqlite")); runMigrations(db);
const repo = new BooksRepo(db), settings = repo.ensureSettings();
settings.movies.managedRoot = root; repo.updateSettings(settings);
let runtime: Awaited<ReturnType<typeof createJobExecutionRuntime>> | undefined;
try {
  await docker("run", "--rm", "--detach", "--name", name, "--publish", "127.0.0.1::9091", "--volume", `${base}:${base}`,
    "--entrypoint", "transmission-daemon", process.env.PODIBLE_LAB_IMAGE ?? "podible-ci-lab:verify", "--foreground", "--config-dir", engine);
  const published = await docker("port", name, "9091/tcp"), port = published.split(":").at(-1);
  const rpc = new TransmissionRpc({ url: `http://127.0.0.1:${port}/transmission/rpc`, username: identity.username, password: identity.password });
  for (let attempt = 0;; attempt++) {
    try { await rpc.call("session-get"); break; }
    catch { if (attempt >= 100) throw new Error("Local daemon was not ready"); await Bun.sleep(100); }
  }
  const client = new TransmissionAdapter({ clientId: `transmission:${identity.id}`, roots: { movies: path.join(root, "downloads") }, ownershipTag: "Podible Movies", rpc });
  await prepareMovieStorage(repo, path.join(root, "objects"));
  const user = repo.upsertUser({ provider: "local", providerUserId: "lab", username: "lab", isAdmin: true });
  const request = repo.movies.request(user.id, fixtureMovie, settings.movies.qualityPolicy);
  const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "movie_acquire" }).id).acquisition;
  const fileId = randomUUID(), relativePath = `files/${fileId.slice(0, 2)}/${fileId}.mkv`, canonical = path.join(root, "objects", relativePath);
  await fs.mkdir(path.dirname(canonical), { recursive: true }); await fs.writeFile(canonical, original, { mode: 0o444 });
  const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id: fileId, relativePath, sizeBytes: original.length, probe: {} }]);
  repo.movies.selectVersion(request.targetId, version, repo.movies.target(request.targetId)!.revision);
  repo.movies.finishAcquisition(acquisition.id);
  const viewRoot = path.join(root, "views");
  await reconcileLibraryView({ contentRoot: path.join(root, "objects"), viewRoot, entries: [{ relativePath: "Movie.mkv", contentPath: canonical }] });
  const oldLocation = { rootId: "movies", relativePath: "stalled" }, oldPath = path.join(root, "downloads/stalled/Original/old.mkv");
  await fs.mkdir(path.dirname(oldPath), { recursive: true });
  await fs.writeFile(oldPath, original.subarray(0, original.length - 16384));
  const old = await client.ensureAdded({ metainfo: meta("Original", [{ path: "old.mkv", bytes: original }]), location: oldLocation });
  repo.movies.bindDownload(acquisition.id, old, oldLocation, "stalled-original");
  const candidate = meta("Pack", files), metadata = inspectMetainfo(candidate);
  const scope = { media: "movie" as const, targetId: request.targetId };
  const discovery = process.env.PODIBLE_LAB_DISCOVERY ?? "rss";
  const receipt = (() => {
    if (discovery === "selection") {
      const selected = { key: "original", title: "Fixture film", provider: "Original tracker", sizeBytes: original.length,
        url: "https://fixture.invalid/original", infoHash: old.nativeId, seeders: 1, attributes: {} };
      rememberSelectedCopies(repo, scope, acquisition.id, selected, [{ ...selected, key: "other-tracker",
        provider: "Another tracker", sizeBytes: metadata.size, infoHash: metadata.infoHash }]);
      return queueSelectedCrossSeeds(repo, scope, acquisition.id, selected.key)[0]!;
    }
    if (discovery !== "rss") throw new Error("Unknown lab discovery mode");
    repo.releaseInbox.ensureFeed("lab", "Another tracker", "movie", new Date().toISOString());
    const [releaseId] = repo.releaseInbox.ingest("lab", [{ title: "Fixture pack on another tracker", provider: "Another tracker", mediaType: "movie",
      sizeBytes: metadata.size, url: "https://fixture.invalid/metainfo", guid: "lab-candidate", infoHash: metadata.infoHash, seeders: 1, leechers: 0, raw: {} }], new Date().toISOString());
    return queueCrossSeed(repo, { operationId: randomUUID(), scope, releaseId,
      policy: { minimumReuseRatio: 0.9, maximumExtraBytes: 64 * 1024 } });
  })();
  const command = receipt.command;
  const ctx = { repo, getSettings: () => repo.getSettings(), crossSeedAdapters: { client: () => client, fetchMetainfo: async () => candidate, pollMs: 100, maxPolls: 1200 } };
  runtime = await createJobExecutionRuntime(ctx, path.join(base, "execution.sqlite"), {
    processJob: (context, job) => job.type === "torrent_reuse" ? processCrossSeed(context, job) : Promise.resolve("done" as const) });
  await runtime.reconcile();
  const deadline = Date.now() + 180_000;
  while (!["succeeded", "failed"].includes(repo.getJob(receipt.jobId)?.status ?? "")) {
    if (Date.now() > deadline) throw new Error("Real cross-seeding transfer timed out"); await Bun.sleep(100);
  }
  const current = repo.crossSeed.receipt(command.operationId)!;
  if (repo.getJob(receipt.jobId)?.status !== "succeeded") throw new Error(String(repo.getJob(receipt.jobId)?.error));
  if (current.outcome?.kind !== "seeding") throw new Error(`Real daemon reuse did not seed: ${JSON.stringify(current.outcome)}`);
  if (!Buffer.from(await fs.readFile(canonical)).equals(original) || repo.movies.target(request.targetId)!.active_version_id !== version ||
    await fs.realpath(path.join(viewRoot, "Movie.mkv")) !== canonical) throw new Error("Reuse changed the existing canonical bytes or Plex selection");
  if ((await client.inspect(old)).complete) throw new Error("Expected the old incomplete source to remain incomplete, not be overwritten");
  const fields = await rpc.call("torrent-get", { ids: [metadata.infoHash], fields: ["downloadedEver", "totalSize", "trackerStats"] });
  const tracker = (fields.torrents as { trackerStats: { lastAnnounceSucceeded: boolean }[]; downloadedEver: number; totalSize: number }[])[0]!;
  if (tracker.downloadedEver >= original.length / 2) throw new Error("Candidate redownloaded the media instead of reusing it");
  console.log(JSON.stringify({ result: "passed", discovery, outcome: current.outcome, downloadedEver: tracker.downloadedEver,
    canonicalUnchanged: true, plexSelectionUnchanged: true, oldStalledRecordRetained: true, nativeCow: true,
    trackerAnnouncements: tracker.trackerStats.map(t => t.lastAnnounceSucceeded) }, null, 2));
} finally {
  await runtime?.stop(); server.stop(true);
  await docker("rm", "--force", name).catch(() => {});
  db.close(); await fs.rm(base, { recursive: true, force: true });
}
