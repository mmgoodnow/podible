import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import bencode from "bencode";
import { RtorrentAdapter } from "../../src/media/rtorrent-client";

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
    if (!(error instanceof Error) || !error.message.includes("another client"))
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
