import { afterEach, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import bencode from "bencode";
import { inspectMetainfo } from "../../src/media/metainfo";
import { listSeedOnlyIntents, persistSeedOnlyIntent, prepareSeedOnlyResumes, readSeedOnlyIntent, seedOnlyIntentDirectory } from "../../src/media/seed-only-state";

const temporary: string[] = [];
afterEach(async () => {
  for (const directory of temporary.splice(0)) await fs.rm(directory, { recursive: true, force: true });
});
async function fixture() {
  const base = await fs.realpath(await fs.mkdtemp(path.join(tmpdir(), "podible-seed-only-")));
  temporary.push(base);
  const engine = path.join(base, "transmission"), destination = path.join(base, "data");
  await fs.mkdir(engine); await fs.mkdir(path.join(engine, "torrents")); await fs.mkdir(path.join(engine, "resume"));
  await fs.mkdir(destination);
  const metainfo = Buffer.from(bencode.encode({ announce: "https://private.invalid/secret", info: { name: "release",
    files: [{ path: ["movie.mkv"], length: 4 }, { path: ["empty.nfo"], length: 0 }],
    "piece length": 4, pieces: createHash("sha1").update("abcd").digest() } }));
  const metadata = inspectMetainfo(metainfo);
  const intent = { infoHash: metadata.infoHash, destination, fileCount: 2, ownershipTag: "Podible Movies" };
  const intents = seedOnlyIntentDirectory(engine), torrent = path.join(engine, "torrents", intent.infoHash + ".torrent"),
    resume = path.join(engine, "resume", intent.infoHash + ".resume");
  const save = (value: unknown) => fs.writeFile(resume, bencode.encode(value));
  const read = async () => bencode.decode(await fs.readFile(resume)) as Record<string, any>;
  return { base, engine, destination, metainfo, intent, intents, torrent, resume, save, read,
    register: async () => { await persistSeedOnlyIntent(intents, intent); await fs.writeFile(torrent, metainfo); } };
}

test("seed-only intent is durable, immutable, outside engine config, and contains no metainfo URLs", async () => {
  const f = await fixture();
  expect(f.intents).toBe(path.join(f.base, "transmission-seed-only"));
  const intent = await persistSeedOnlyIntent(f.intents, f.intent);
  expect(await persistSeedOnlyIntent(f.intents, f.intent)).toEqual(intent);
  expect(listSeedOnlyIntents(f.intents)).toEqual([intent]);
  const bytes = await fs.readFile(path.join(f.intents, f.intent.infoHash + ".json"), "utf8");
  expect(Object.keys(JSON.parse(bytes)).sort()).toEqual(["version", "infoHash", "destination", "fileCount", "ownershipTag"].sort());
  expect(bytes).not.toContain("secret");
  expect(bytes).not.toContain("https:");
  for (const change of [{ fileCount: 1 }, { ownershipTag: "Other" }, { destination: f.base }])
    await expect(persistSeedOnlyIntent(f.intents, { ...f.intent, ...change })).rejects.toThrow("immutable");
  expect(await fs.readdir(f.intents)).toEqual([f.intent.infoHash + ".json"]);
});

test("exact intent lookup is missing-safe and reads only the requested hash", async () => {
  const f = await fixture();
  expect(readSeedOnlyIntent(f.intents, f.intent.infoHash)).toBeNull();
  const saved = await persistSeedOnlyIntent(f.intents, f.intent);
  expect(readSeedOnlyIntent(f.intents, f.intent.infoHash)).toEqual(saved);
  expect(readSeedOnlyIntent(f.intents, "b".repeat(40))).toBeNull();
  await fs.writeFile(path.join(f.intents, "b".repeat(40) + ".json"), "unrelated corrupt marker");
  expect(readSeedOnlyIntent(f.intents, f.intent.infoHash)).toEqual(saved);
  for (const hash of ["../escape", "", f.intent.infoHash.toUpperCase(), "b".repeat(64)])
    expect(() => readSeedOnlyIntent(f.intents, hash)).toThrow();
  expect(() => readSeedOnlyIntent("relative", f.intent.infoHash)).toThrow("absolute");
});

test("exact intent lookup rejects malformed or mismatched markers rather than treating them as absent", async () => {
  const f = await fixture();
  const saved = await persistSeedOnlyIntent(f.intents, f.intent);
  const file = path.join(f.intents, f.intent.infoHash + ".json");
  for (const text of ["broken JSON", JSON.stringify({ ...saved, infoHash: "b".repeat(40) }),
    JSON.stringify({ ...saved, fileCount: 0 }), JSON.stringify({ ...saved, destination: "relative" }),
    JSON.stringify({ ...saved, url: "https://untrusted.invalid" })]) {
    await fs.writeFile(file, text);
    expect(() => readSeedOnlyIntent(f.intents, f.intent.infoHash)).toThrow();
  }
});

test("exact intent lookup rejects marker and directory symlinks, including dangling links", async () => {
  for (const kind of ["file", "directory", "dangling"]) {
    const f = await fixture(); await persistSeedOnlyIntent(f.intents, f.intent);
    const target = kind === "directory" ? f.intents : path.join(f.intents, f.intent.infoHash + ".json");
    const foreign = path.join(f.base, "foreign");
    await fs.rename(target, foreign);
    await fs.symlink(kind === "dangling" ? path.join(f.base, "absent") : foreign, target);
    expect(() => readSeedOnlyIntent(f.intents, f.intent.infoHash)).toThrow();
  }
});

test("startup restores unwanted flags for every file, preserves valid paused state, and fixes destination/labels", async () => {
  for (const paused of [0, 1]) for (const dnd of [undefined, [0, 0], [1], "broken"]) {
    const f = await fixture(); await f.register();
    await f.save({ name: "wrong", destination: "/wrong/default", paused, downloaded: 91, labels: ["Existing"], ...(dnd === undefined ? {} : { dnd }) });
    const result = prepareSeedOnlyResumes(f.engine, f.intents);
    expect(result.map(intent => intent.infoHash)).toEqual([f.intent.infoHash]);
    const resume = await f.read();
    expect(resume.paused).toBe(paused);
    expect(resume.dnd).toEqual([1, 1]);
    expect(Buffer.from(resume.destination).toString()).toBe(f.destination);
    expect(Buffer.from(resume.name).toString()).toBe("release");
    expect(resume.labels.map((label: Uint8Array) => Buffer.from(label).toString())).toEqual(["Existing", "Podible Movies", "Podible Seed Only"]);
    expect(resume.downloaded).toBe(91);
    // Re-run before every launch, including after a daemon rewrites its saved flags.
    await f.save({ ...resume, dnd: [0, 0] });
    prepareSeedOnlyResumes(f.engine, f.intents);
    expect((await f.read()).dnd).toEqual([1, 1]);
  }
});

test("missing or corrupt resume is rebuilt paused and all-unwanted, without inventing completeness", async () => {
  for (const bytes of [null, "garbage", "li1ee", "d6:pausedi0eejunk"]) {
    const f = await fixture(); await f.register();
    if (bytes !== null) await fs.writeFile(f.resume, bytes);
    prepareSeedOnlyResumes(f.engine, f.intents);
    const resume = await f.read();
    expect(resume.paused).toBe(1);
    expect(resume.dnd).toEqual([1, 1]);
    expect(Buffer.from(resume.destination).toString()).toBe(f.destination);
    expect(resume.progress).toBeUndefined();
    expect(resume.labels.map((label: Uint8Array) => Buffer.from(label).toString())).toEqual(["Podible Movies", "Podible Seed Only"]);
  }
});

test("ordinary resumes remain byte-for-byte untouched and an intent before torrent-add is skipped", async () => {
  const f = await fixture();
  const ordinary = path.join(f.engine, "resume", "b".repeat(40) + ".resume");
  const bytes = Buffer.from(bencode.encode({ paused: 0, dnd: [0, 0], destination: "ordinary" }));
  await fs.writeFile(ordinary, bytes);
  await persistSeedOnlyIntent(f.intents, f.intent);
  expect(prepareSeedOnlyResumes(f.engine, f.intents)).toEqual([]);
  expect(await fs.exists(f.resume)).toBe(false);
  expect(await fs.readFile(ordinary)).toEqual(bytes);
  await fs.writeFile(f.torrent, f.metainfo);
  prepareSeedOnlyResumes(f.engine, f.intents);
  expect(await fs.readFile(ordinary)).toEqual(bytes);
});

test("malformed metainfo or wrong actual hash/file count fails before changing a resume", async () => {
  for (const kind of ["malformed", "hash", "count"]) {
    const f = await fixture();
    await persistSeedOnlyIntent(f.intents, { ...f.intent, ...(kind === "count" ? { fileCount: 1 } : {}) });
    const other = bencode.encode({ info: { name: "other", length: 4, "piece length": 4, pieces: createHash("sha1").update("abcd").digest() } });
    await fs.writeFile(f.torrent, kind === "malformed" ? "invalid" : kind === "hash" ? other : f.metainfo);
    await f.save({ paused: 0, dnd: [0, 0] });
    const before = await fs.readFile(f.resume);
    expect(() => prepareSeedOnlyResumes(f.engine, f.intents)).toThrow();
    expect(await fs.readFile(f.resume)).toEqual(before);
  }
});

test("startup rejects intent, torrent, resume and parent-directory symlinks without changing targets", async () => {
  for (const kind of ["intent", "torrent", "resume", "resume-directory", "intent-directory"]) {
    const f = await fixture(); await f.register(); await f.save({ paused: 0, dnd: [0, 0] });
    const target = kind === "intent" ? path.join(f.intents, f.intent.infoHash + ".json") : kind === "torrent" ? f.torrent
      : kind === "resume" ? f.resume : kind === "resume-directory" ? path.dirname(f.resume) : f.intents;
    const foreign = path.join(f.base, "foreign");
    await fs.rename(target, foreign); await fs.symlink(foreign, target);
    expect(() => prepareSeedOnlyResumes(f.engine, f.intents)).toThrow();
    if (["intent", "torrent", "resume"].includes(kind)) expect((await fs.lstat(foreign)).isFile()).toBe(true);
  }
});

test("invalid destinations and filename/hash mismatches are not adopted", async () => {
  const f = await fixture();
  await expect(persistSeedOnlyIntent(f.intents, { ...f.intent, destination: "/" })).rejects.toThrow("bounded");
  await expect(persistSeedOnlyIntent(f.intents, { ...f.intent, destination: "relative" })).rejects.toThrow("bounded");
  const alias = path.join(f.base, "alias"); await fs.symlink(f.destination, alias);
  await expect(persistSeedOnlyIntent(f.intents, { ...f.intent, destination: alias })).rejects.toThrow("real");
  await f.register();
  const intentFile = path.join(f.intents, f.intent.infoHash + ".json");
  const intent = JSON.parse(await fs.readFile(intentFile, "utf8"));
  await fs.writeFile(intentFile, JSON.stringify({ ...intent, infoHash: "b".repeat(40) }));
  expect(() => listSeedOnlyIntents(f.intents)).toThrow("filename/hash");
  expect(() => prepareSeedOnlyResumes(f.engine, f.intents)).toThrow("filename/hash");
});
