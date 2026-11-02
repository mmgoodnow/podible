import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import bencode from "bencode";
import { RtorrentAdapter } from "../../src/media/rtorrent-client";
import { startFixturePeer } from "./fixture-peer";

const root = path.resolve(import.meta.dir, "../..");
if (!process.env.LAB_INTERNAL) {
  const args = [
    "docker",
    "run",
    "--rm",
    "--network",
    "podible-movies-lab_default",
    "-e",
    "LAB_INTERNAL=1",
    "-e",
    `LAB_CASE=${process.env.LAB_CASE ?? "all"}`,
    "-v",
    "podible-movies-lab-content:/data",
    "-v",
    `${root}/src/media:/app/src/media:ro`,
    "-v",
    `${root}/src/rtorrent.ts:/app/src/rtorrent.ts:ro`,
    "-v",
    `${root}/test/lab:/app/test/lab:ro`,
    "podible-movies-poc:local",
    "bun",
    "test/lab/torrent-acceptance.ts",
  ];
  const child = Bun.spawn(args, { stdout: "inherit", stderr: "inherit" });
  process.exit(await child.exited);
}
const client = new RtorrentAdapter({
  clientId: "lab-rtorrent",
  ownershipTag: "Podible Movies Lab",
  roots: { lab: "/data/downloads" },
  settings: {
    transport: "http-xmlrpc",
    url: "http://rtorrent:8000",
    username: "",
    password: "",
  },
});
if (process.env.LAB_CASE !== "multi") {
  const directory = "/data/downloads/client-contract";
  await fs.mkdir(directory, { recursive: true });
  await fs.chmod(directory, 0o777);
  const file = `${directory}/fixture.bin`;
  const bytes = Buffer.from("movie data");
  await Bun.write(file, bytes);
  await fs.chmod(file, 0o666);
  const metainfo = new Uint8Array(
    bencode.encode({
      announce: "http://127.0.0.1:1/announce",
      info: {
        name: "fixture.bin",
        length: bytes.length,
        "piece length": 16384,
        pieces: createHash("sha1").update(bytes).digest(),
        private: 1,
      },
    }),
  );
  const ref = await client.ensureAdded({
    metainfo,
    location: { rootId: "lab", relativePath: "client-contract" },
  });
  try {
    const again = await client.ensureAdded({
      metainfo,
      location: { rootId: "lab", relativePath: "client-contract" },
    });
    if (
      ref.nativeId !== again.nativeId ||
      (await client.inspect(ref)).state === "downloading"
    )
      throw new Error("Paused/idempotent add failed");
    await client.recheck(ref);
    const deadline = Date.now() + 15_000;
    let snapshot = await client.inspect(ref);
    while (
      (!snapshot.complete || snapshot.state === "checking") &&
      Date.now() < deadline
    ) {
      await Bun.sleep(100);
      snapshot = await client.inspect(ref);
    }
    if (!snapshot.complete)
      throw new Error("Client did not verify the existing bytes");
    await client.resume(ref);
    await client.quiesce(ref);
    if ((await client.inspect(ref)).state !== "inactive")
      throw new Error("Quiescence not observed");
    if (
      !(await client.files(ref)).some((entry) =>
        entry.path.endsWith("fixture.bin"),
      )
    )
      throw new Error("File layout not reported");
    try {
      await client.inspect({ ...ref, clientId: "another-client" });
      throw new Error("Foreign client accepted");
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !error.message.includes("another client")
      )
        throw error;
    }
  } finally {
    await client.removeRecord(ref);
  }
  if ((await Bun.file(file).text()) !== bytes.toString())
    throw new Error("Removing the torrent record removed or changed content");
  console.log(
    "Real rTorrent contract passed: paused/idempotent add, inspect, recheck, resume, quiesce, files and record-only removal.",
  );
  const seed = await startFixturePeer();
  const transferDirectory = `/data/downloads/${seed.name}-layout`;
  await fs.mkdir(transferDirectory);
  await fs.chmod(transferDirectory, 0o777);
  let transferRef: Awaited<ReturnType<typeof client.ensureAdded>> | null = null;
  try {
    transferRef = await client.ensureAdded({
      metainfo: seed.metainfo,
      location: {
        rootId: "lab",
        relativePath: path.basename(transferDirectory),
      },
    });
    await client.resume(transferRef);
    const deadline = Date.now() + 45_000;
    let snapshot = await client.inspect(transferRef);
    while (!snapshot.complete && Date.now() < deadline) {
      await Bun.sleep(250);
      snapshot = await client.inspect(transferRef);
    }
    if (!snapshot.complete)
      throw new Error(
        `Real transfer did not complete: ${JSON.stringify({ snapshot, peer: seed.stats() })}`,
      );
    const reported = await client.files(transferRef);
    if (
      reported[0]?.index !== 0 ||
      !Buffer.from(await Bun.file(reported[0].path).arrayBuffer()).equals(
        seed.bytes,
      )
    )
      throw new Error("Transferred content/file mapping is incorrect");
    if (seed.stats().requests < 1 || seed.stats().announces < 1)
      throw new Error("Fixture peer was not actually used");
    console.log(
      `Real rTorrent transfer passed: downloaded and verified ${seed.bytes.length} bytes from the isolated fixture peer.`,
    );
  } finally {
    // Completed downloads are retained, just as acquisition/upgrades will retain
    // seeding records. Record-only removal is exercised separately above.
    if (transferRef) await client.quiesce(transferRef);
    await seed.close();
  }
}

const multiDirectory = `/data/downloads/multi-${Date.now()}`;
await fs.mkdir(multiDirectory);
await fs.chmod(multiDirectory, 0o777);
const parts = [
  Buffer.alloc(128 * 1024, 42),
  Buffer.from("tracker-specific note"),
];
const multiBytes = Buffer.concat(parts);
const pieces: Buffer[] = [];
for (let offset = 0; offset < multiBytes.length; offset += 16384)
  pieces.push(
    createHash("sha1")
      .update(multiBytes.subarray(offset, offset + 16384))
      .digest(),
  );
const multiMetainfo = new Uint8Array(
  bencode.encode({
    announce: "http://127.0.0.1:1/announce",
    info: {
      name: path.basename(multiDirectory),
      private: 1,
      "piece length": 16384,
      pieces: Buffer.concat(pieces),
      files: [
        { path: ["feature.mkv"], length: parts[0]!.length },
        { path: ["notes", "info.nfo"], length: parts[1]!.length },
      ],
    },
  }),
);
const multiRef = await client.ensureAdded({
  metainfo: multiMetainfo,
  location: { rootId: "lab", relativePath: path.basename(multiDirectory) },
});
try {
  const files = await client.files(multiRef);
  if (files.length !== 2)
    throw new Error("Multi-file client layout omitted files");
  for (const entry of files) {
    await fs.mkdir(path.dirname(entry.path), { recursive: true, mode: 0o777 });
    await fs.chmod(path.dirname(entry.path), 0o777);
    await fs.writeFile(entry.path, parts[entry.index]!);
    await fs.chmod(entry.path, 0o666);
  }
  await client.recheck(multiRef);
  const deadline = Date.now() + 15_000;
  let snapshot = await client.inspect(multiRef);
  while (
    (!snapshot.complete || snapshot.state === "checking") &&
    Date.now() < deadline
  ) {
    await Bun.sleep(100);
    snapshot = await client.inspect(multiRef);
  }
  if (!snapshot.complete)
    throw new Error(
      "Multi-file metainfo indexes do not map to verified client files",
    );
  console.log(
    "Real rTorrent multi-file layout passed: indexed feature and nested NFO map to verified metainfo bytes.",
  );
} finally {
  await client.removeRecord(multiRef);
}
