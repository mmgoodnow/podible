import { promises as fs } from "node:fs";
import path from "node:path";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { runSnatch } from "../../src/library/service";
import { bookDownloadClient } from "../../src/media/book-torrents";
import { processDownloadJob } from "../../src/worker/downloads";
import { processImportJob } from "../../src/worker/imports";
import { bookViewEntries } from "../../src/library/book-storage";
import { reconcileBookStorage } from "../../src/library/book-publication";
import {
  prepareTransmission,
  managedTransmission,
  managedBookTransmission,
  managedTvTransmission,
  readTransmissionIdentity,
} from "../../src/media/managed-transmission";
import { TransmissionRpc } from "../../src/media/transmission-client";
import { infoHashFromTorrentBytes, rewriteTorrentAnnounces } from "../../src/library/torrent";
const root = await fs.mkdtemp("/tmp/transmission-acceptance-");
const directory = path.join(root, "engine");
const identity = prepareTransmission(directory);
const rpc = new TransmissionRpc({
  url: "http://127.0.0.1:9091/transmission/rpc",
  username: identity.username,
  password: identity.password,
});
let daemon: ReturnType<typeof Bun.spawn> | undefined;
const files = [
  { name: "Bundle/feature.bin", bytes: Buffer.alloc(80_000, 42) },
  { name: "Bundle/release.nfo", bytes: Buffer.from("fixture nfo") },
];
const torrentFiles = new Map<string, Uint8Array>();
const webseed = Bun.serve({
  hostname: "127.0.0.1",
  port: 18080,
  fetch(request) {
    const torrent = torrentFiles.get(new URL(request.url).pathname);
    if (torrent) return new Response(torrent);
    const file = files.find(
      (f) =>
        "/seed/" + f.name === decodeURIComponent(new URL(request.url).pathname),
    );
    if (!file) return new Response(null, { status: 404 });
    const range = /bytes=(\d+)-(\d*)/.exec(request.headers.get("range") ?? "");
    if (!range) return new Response(file.bytes);
    const start = Number(range[1]);
    const end = Math.min(
      file.bytes.length - 1,
      range[2] ? Number(range[2]) : file.bytes.length - 1,
    );
    return new Response(file.bytes.subarray(start, end + 1), {
      status: 206,
      headers: {
        "Content-Range": `bytes ${start}-${end}/${file.bytes.length}`,
      },
    });
  },
});
async function start() {
  daemon = Bun.spawn(
    ["transmission-daemon", "--foreground", "--config-dir", directory],
    { stdout: "ignore", stderr: "ignore" },
  );
  for (let n = 0; n < 100; n++) {
    try {
      await rpc.call("session-get");
      const session = await rpc.call("session-get", { fields: ["session-id"] });
      if (typeof session["session-id"] !== "string" || session["session-id"].length < 16)
        throw new Error("Stable Transmission does not expose a retirement session identity");
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("Daemon did not become ready");
}
async function stop() {
  daemon?.kill("SIGTERM");
  if (daemon) await daemon.exited;
}
const data = Buffer.concat(files.map((f) => f.bytes));
const pieces = [];
for (let i = 0; i < data.length; i += 16384)
  pieces.push(
    createHash("sha1")
      .update(data.subarray(i, i + 16384))
      .digest(),
  );
const originalAnnounce = "http://127.0.0.1:18080/old/announce";
const metainfo = bencode.encode({
  announce: originalAnnounce,
  "url-list": ["http://127.0.0.1:18080/seed/"],
  info: {
    name: "Bundle",
    "piece length": 16384,
    pieces: Buffer.concat(pieces),
    files: files.map((f) => ({
      length: f.bytes.length,
      path: [f.name.split("/").at(-1)],
    })),
  },
});
try {
  await start();
  const client = managedTransmission(root, directory);
  const ref = await client.ensureAdded({
    metainfo,
    location: { rootId: "movies", relativePath: "one" },
  });
  if ((await client.inspect(ref)).state !== "inactive")
    throw new Error("Add was not paused");
  await client.resume(ref);
  const deadline = Date.now() + 90_000;
  while (!(await client.inspect(ref)).complete) {
    if (Date.now() > deadline) throw new Error("Webseed transfer timed out");
    await Bun.sleep(200);
  }
  const paths = await client.files(ref);
  for (const [index, file] of files.entries())
    if (!(await fs.readFile(paths[index]!.path)).equals(file.bytes))
      throw new Error("Transferred bytes differ");
  {
    using rotationDb = new Database(":memory:");
    runMigrations(rotationDb);
    const rotationRepo = new BooksRepo(rotationDb);
    const primary = "http://127.0.0.1:18080/new/announce";
    const backup = "http://127.0.0.2:18080/new/announce";
    const donor = rewriteTorrentAnnounces(metainfo, { primary, tiers: [[primary], [backup]] });
    const independent = bencode.encode({ announce: primary, "announce-list": [[primary], [backup]], info: { name: "independent" } });
    for (const [key, bytes] of [["old", metainfo], ["new", donor], ["independent", independent]] as const)
      rotationRepo.putTorrentCache({ key, provider: "Loopback", url: "http://127.0.0.1:18080/torrent",
        infoHash: infoHashFromTorrentBytes(bytes), torrentBytes: bytes, observedFetch: key !== "old" });
    rotationDb.query(`INSERT INTO media_torrents(client_id,native_id,location_json,offer_key,backend,ownership_tag)
      VALUES(?,?,?,'','transmission','Podible Movies')`)
      .run(ref.clientId, ref.nativeId, JSON.stringify({ rootId: "movies", relativePath: "one" }));
    const provider = rotationRepo.trackerAnnounces.pendingProvider();
    const rotation = provider && await rotationRepo.trackerAnnounces.reconcile(provider,
      { rpc, clientId: ref.clientId, roots: { movies: path.join(root, "downloads") } });
    if (!rotation || rotation.cached !== 1 || rotation.registered !== 1)
      throw new Error("Announce rotation did not update the cached and registered torrent");
    await stop();
    prepareTransmission(directory);
    await start();
    const current = await rpc.call("torrent-get", { ids: [ref.nativeId], fields: ["trackerList"] });
    const trackers = (current.torrents as { trackerList: string }[])[0]?.trackerList.trim();
    if (trackers !== `${primary}\n\n${backup}` || !(await client.inspect(ref)).complete)
      throw new Error("Restart lost rotated tracker tiers or completed data");
  }
  const resumed = managedTransmission(root, directory);
  const same = await resumed.ensureAdded({
    metainfo,
    location: { rootId: "movies", relativePath: "one" },
  });
  if (
    JSON.stringify(same) !== JSON.stringify(ref) ||
    !(await resumed.inspect(same)).complete
  )
    throw new Error("Restart lost torrent state/identity");
  await resumed.removeRecord(same);
  if (!(await fs.readFile(paths[0]!.path)).equals(files[0]!.bytes))
    throw new Error("Record removal deleted content");

  const tv = managedTvTransmission(path.join(root, "tv"), directory);
  const tvRef = await tv.ensureAdded({ metainfo, location: { rootId: "tv", relativePath: "episode-pack" } });
  if ((await tv.inspect(tvRef)).state !== "inactive") throw new Error("TV add was not paused");
  let refused = false;
  try { await resumed.inspect(tvRef); } catch { refused = true; }
  if (!refused) throw new Error("Movie adapter accepted a TV-owned torrent");
  await tv.resume(tvRef);
  const tvDeadline = Date.now() + 90_000;
  while (!(await tv.inspect(tvRef)).complete) {
    if (Date.now() > tvDeadline) throw new Error("TV webseed transfer timed out");
    await Bun.sleep(200);
  }
  const tvFiles = await tv.files(tvRef);
  for (const [index, file] of files.entries())
    if (!(await fs.readFile(tvFiles[index]!.path)).equals(file.bytes)) throw new Error("TV bytes differ");
  await tv.removeRecord(tvRef);
  if (!(await fs.readFile(tvFiles[0]!.path)).equals(files[0]!.bytes)) throw new Error("TV record removal deleted data");
  console.log("TV private Transmission: paused add, owned root/tag, exact bytes, foreign adapter refusal, non-destructive removal verified");

  const libraryRoot = path.join(root, "books");
  await fs.mkdir(libraryRoot);
  const bookBytes = Buffer.from(
    "%PDF-1.4\nSynthetic book transmission fixture\n%%EOF\n",
  );
  files.push({ name: "book.pdf", bytes: bookBytes });
  const bookTorrent = new Uint8Array(
    bencode.encode({
      "url-list": ["http://127.0.0.1:18080/seed/book.pdf"],
      info: {
        name: "book.pdf",
        length: bookBytes.length,
        "piece length": 16384,
        pieces: createHash("sha1").update(bookBytes).digest(),
      },
    }),
  );
  torrentFiles.set("/torrent/book", bookTorrent);
  using bookDb = new Database(":memory:");
  runMigrations(bookDb);
  const bookRepo = new BooksRepo(bookDb);
  const settings = bookRepo.ensureSettings();
  settings.libraryRoot = libraryRoot;
  bookRepo.updateSettings(settings);
  const book = bookRepo.createBook({
    title: "Transmission fixture",
    author: "Synthetic",
  });
  const bookClient = managedBookTransmission(
    path.join(libraryRoot, ".podible", "downloads"),
    directory,
  );
  const snatch = await runSnatch(
    bookRepo,
    settings,
    {
      bookId: book.id,
      provider: "Loopback webseed",
      title: "Transmission fixture PDF",
      mediaType: "ebook",
      url: "http://127.0.0.1:18080/torrent/book",
      sizeBytes: bookBytes.length,
    },
    { bookTorrentClient: bookClient },
  );
  const downloadClient = bookDownloadClient(bookRepo, settings, bookClient);
  const bookDeadline = Date.now() + 90_000;
  while (
    !(await downloadClient.inspect(snatch.release.info_hash)).complete
  ) {
    if (Date.now() > bookDeadline)
      throw new Error("Book webseed transfer timed out");
    await Bun.sleep(200);
  }
  await processDownloadJob(
    {
      repo: bookRepo,
      getSettings: () => settings,
      bookTorrentClient: bookClient,
    },
    bookRepo.getJob(snatch.jobId)!,
  );
  const importJob = bookRepo.listJobsByType("import")[0]!;
  await processImportJob(
    {
      repo: bookRepo,
      getSettings: () => settings,
      bookTorrentClient: bookClient,
    },
    importJob,
  );
  const asset = bookRepo.listAssetsByBook(book.id)[0]!;
  const imported = bookRepo.getAssetFiles(asset.id)[0]!;
  const bookSource = (
    await downloadClient.getImportSource(snatch.release.info_hash)
  ).selectedPaths[0]!;
  const [sourceStat, importedStat] = await Promise.all([
    fs.stat(bookSource),
    fs.stat(imported.path),
  ]);
  if (
    sourceStat.ino !== importedStat.ino ||
    sourceStat.dev !== importedStat.dev ||
    !(await fs.readFile(imported.path)).equals(bookBytes)
  )
    throw new Error(
      "Book import did not preserve bytes with a same-filesystem hardlink",
    );
  if ((await fs.lstat(imported.path)).isSymbolicLink() || !imported.path.includes("/.podible/content/"))
    throw new Error("Book DB must reference canonical content, not a symlink view");
  const view = bookViewEntries(bookRepo, libraryRoot)[0]!;
  const viewPath = path.join(libraryRoot, view.relativePath);
  if (!(await fs.lstat(viewPath)).isSymbolicLink() || !(await fs.readFile(viewPath)).equals(bookBytes))
    throw new Error("Book view was not a readable symlink to canonical content");
  await fs.unlink(viewPath);
  await reconcileBookStorage(bookRepo, libraryRoot);
  if (!(await fs.readFile(viewPath)).equals(bookBytes)) throw new Error("Book view did not repair itself");
  await stop();
  prepareTransmission(directory);
  await start();
  const restartedBookClient = managedBookTransmission(
    path.join(libraryRoot, ".podible", "downloads"),
    directory,
  );
  if (
    !(
      await bookDownloadClient(
        bookRepo,
        settings,
        restartedBookClient,
      ).inspect(snatch.release.info_hash)
    ).complete
  )
    throw new Error("Daemon restart lost the book's download binding");
  const repeated = await runSnatch(
    bookRepo,
    settings,
    {
      bookId: book.id,
      provider: "Loopback webseed",
      title: "Transmission fixture PDF",
      mediaType: "ebook",
      url: "http://127.0.0.1:18080/torrent/book",
      infoHash: snatch.release.info_hash,
    },
    { bookTorrentClient: restartedBookClient },
  );
  if (!repeated.idempotent || repeated.release.id !== snatch.release.id)
    throw new Error("Book retry duplicated its release");
  console.log(
    "Book Transmission acceptance: real webseed transfer, private client binding, download/import handoff, zero-copy hardlink, restart and idempotency passed",
  );
  await stop();
  daemon = undefined;
  const config = path.join(root, "supervised");
  const supervisedRoot = path.join(root, "supervised-tv");
  let supervisedRef: Awaited<ReturnType<ReturnType<typeof managedTvTransmission>["ensureAdded"]>> | undefined;
  const supervisor = Bun.spawn(
    [process.execPath, "run", "scripts/container.ts"],
    {
      env: { ...process.env, CONFIG_DIR: config, PORT: "3187" },
      stdout: "inherit",
      stderr: "inherit",
    },
  );
  async function until(check: () => Promise<boolean>) {
    const end = Date.now() + 20_000;
    while (!(await check())) {
      if (Date.now() > end) throw new Error("Supervisor acceptance timed out");
      await Bun.sleep(100);
    }
  }
  const daemonPids = async () => {
    const result: number[] = [];
    for (const entry of await fs.readdir("/proc"))
      if (/^\d+$/.test(entry)) {
        const command = await fs
          .readFile(`/proc/${entry}/cmdline`, "utf8")
          .catch(() => "");
        if (command.split("\0")[0]?.endsWith("transmission-daemon"))
          result.push(Number(entry));
      }
    return result;
  };
  try {
    const healthy = async () => {
      try {
        const response = await fetch("http://127.0.0.1:3187/rpc/system/health");
        const data = (await response.json()) as { result?: { ok: boolean } };
        return data.result?.ok === true;
      } catch {
        return false;
      }
    };
    await until(healthy);
    const before = readTransmissionIdentity(path.join(config, "transmission"));
    const firstPid = (await daemonPids())[0];
    if (!firstPid) throw new Error("Private daemon is not running");
    process.kill(firstPid, "SIGKILL");
    await until(async () =>
      (await daemonPids()).some((pid) => pid !== firstPid),
    );
    const privateRpc = new TransmissionRpc({
      url: "http://127.0.0.1:9091/transmission/rpc",
      username: before.username,
      password: before.password,
    });
    await until(async () => {
      try {
        await privateRpc.call("session-get");
        return true;
      } catch {
        return false;
      }
    });
    if ((await fetch("http://127.0.0.1:9091/transmission/rpc")).status !== 401)
      throw new Error("Private RPC is not authenticated");
    if (
      readTransmissionIdentity(path.join(config, "transmission")).id !==
      before.id
    )
      throw new Error("Restart changed client identity");
    if (!(await healthy()))
      throw new Error("Daemon failure stopped application");
    // Add immediately before shutdown, before Transmission's periodic resume flush.
    supervisedRef = await managedTvTransmission(supervisedRoot, path.join(config, "transmission")).ensureAdded({
      metainfo, location: { rootId: "tv", relativePath: "fresh-before-stop" },
    });
    console.log(
      "Supervisor acceptance: real app readiness, independent daemon restart, private authenticated RPC, stable identity passed",
    );
  } finally {
    const shutdownStarted = Date.now();
    supervisor.kill("SIGTERM");
    const timeout = setTimeout(() => supervisor.kill("SIGKILL"), 15_000);
    await supervisor.exited;
    clearTimeout(timeout);
    if (Date.now() - shutdownStarted >= 10_000)
      throw new Error("Supervisor exceeded Docker's default total stop grace period");
  }
  const resumeFiles = await fs.readdir(path.join(config, "transmission", "resume"));
  if (!resumeFiles.some(name => name.endsWith(".resume")))
    throw new Error("Graceful shutdown did not persist the freshly added torrent's resume metadata");
  daemon = Bun.spawn(["transmission-daemon", "--foreground", "--config-dir", path.join(config, "transmission")],
    { stdout: "ignore", stderr: "ignore" });
  const restartedTv = managedTvTransmission(supervisedRoot, path.join(config, "transmission"));
  await until(async () => {
    try { return (await restartedTv.inspect(supervisedRef!)).location.relativePath === "fresh-before-stop"; }
    catch { return false; }
  });
  await stop(); daemon = undefined;
  console.log("Supervisor shutdown acceptance: fresh torrent resume, ownership label and exact directory survived restart within Docker's grace period");
  console.log(
    "Transmission acceptance: paused add, actual webseed transfer, multi-file paths, persistent restart, no-delete removal passed",
  );
} finally {
  await stop();
  webseed.stop(true);
  await fs.rm(root, { recursive: true, force: true });
}
