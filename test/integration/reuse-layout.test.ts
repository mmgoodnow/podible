import { afterEach, expect, test } from "bun:test";
import { constants, promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import bencode from "bencode";
import { inspectMetainfo } from "../../src/media/metainfo";
import { planContentReuse, type ReuseCandidateManifest, type ReuseSourceFile } from "../../src/media/reuse-plan";
import {
  captureReuseFile, captureReuseRoot, materializeReuseLayout,
  prepareReuseLayoutRoot, verifyContentReuse,
} from "../../src/media/reuse-layout";

const temporary: string[] = [];
afterEach(async () => {
  for (const root of temporary.splice(0)) await fs.rm(root, { recursive: true, force: true });
});

function manifest(entries: Array<{ name: string; text: string; role?: "content" | "sidecar" }>, pieceLength = 4): ReuseCandidateManifest {
  const bytes = Buffer.concat(entries.map((entry) => Buffer.from(entry.text)));
  const pieces: Buffer[] = [];
  for (let offset = 0; offset < bytes.length; offset += pieceLength)
    pieces.push(createHash("sha1").update(bytes.subarray(offset, offset + pieceLength)).digest());
  const metainfo = bencode.encode({ info: {
    name: "pack", "piece length": pieceLength, pieces: Buffer.concat(pieces),
    files: entries.map((entry) => ({ path: entry.name.split("/"), length: Buffer.byteLength(entry.text) })),
  } });
  const inspected = inspectMetainfo(metainfo);
  return { identity: { v1: inspected.infoHash }, files: inspected.files.map((file, index) => ({ ...file, role: entries[index].role })),
    v1: { pieceLength, pieceHashes: pieces.map((piece) => piece.toString("hex")) } };
}

async function setup() {
  const base = await fs.realpath(await fs.mkdtemp(path.join(tmpdir(), "podible-reuse-")));
  temporary.push(base);
  const sourcesPath = path.join(base, "objects");
  await fs.mkdir(sourcesPath);
  await fs.writeFile(path.join(sourcesPath, ".podible-content.json"), JSON.stringify({ id: "owned-content" }));
  const sourcesRoot = await captureReuseRoot({ rootPath: sourcesPath, markerName: ".podible-content.json", ownerId: "owned-content" });
  const root = await prepareReuseLayoutRoot({ rootPath: path.join(base, "reuse"), ownerId: "owned-reuse" });
  async function source(id: string, name: string, text: string, hash = false) {
    const sourcePath = path.join(sourcesPath, name);
    await fs.mkdir(path.dirname(sourcePath), { recursive: true });
    await fs.writeFile(sourcePath, text, { mode: 0o444 });
    return captureReuseFile({ root: sourcesRoot, sourceId: id, sourcePath, sourceTorrentId: `torrent-${id}`, hash });
  }
  function plan(candidate: ReuseCandidateManifest, sources: ReuseSourceFile[], maxExtraDownloadBytes = 100) {
    return planContentReuse({ candidate, sources, policy: { version: 1, minReuseRatio: 0.5, maxExtraDownloadBytes } });
  }
  return { base, root, sourcesRoot, source, plan };
}

const writable = { symlinkMode: "never", cow: "clone-force" } as const;
const readOnly = { symlinkMode: "verified-read-only", cow: "unavailable" } as const;

test("real original metainfo verifies renamed media and combines episodes across torrent sources", async () => {
  const fixture = await setup();
  const first = await fixture.source("1", "release-one/old-name.mkv", "abcdefgh");
  const second = await fixture.source("2", "release-two/other.mkv", "ijklmnopqrstu");
  expect(first.identity.sha256).toBeUndefined();
  const candidate = manifest([{ name: "E01.mkv", text: "abcdefgh" }, { name: "E02.mkv", text: "ijklmnopqrstu" }]);
  const checked = await verifyContentReuse(fixture.plan(candidate, [first, second], 0));
  expect(checked.status).toBe("ready");
  expect(checked.files.map((file) => file.proof)).toEqual(["verified-full", "verified-full"]);
  expect(checked.summary.verifiedReuseBytes).toBe(21);
  expect(checked.pieces.every((piece) => piece.status === "verified")).toBe(true);
  const result = await materializeReuseLayout({ root: fixture.root, plan: checked, capabilities: readOnly });
  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("Expected read-only derived layout");
  expect((await fs.lstat(path.join(result.dataPath, "pack/E01.mkv"))).isSymbolicLink()).toBe(true);
  expect(await fs.readlink(path.join(result.dataPath, "pack/E01.mkv"))).toBe(first.path);
  expect(await fs.readFile(path.join(result.dataPath, "pack/E02.mkv"), "utf8")).toBe("ijklmnopqrstu");
});

test("an actual same-size changed tail detects the precise mismatching original piece", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "original.mkv", "abcdefgX");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original], 4);
  const checked = await verifyContentReuse(plan);
  expect(checked.files[0].proof).toBe("verified-partial");
  expect(checked.pieces.map((piece) => piece.status)).toEqual(["verified", "mismatch"]);
  expect(checked.summary.extraDownloadBytesUpperBound).toBe(4);
  expect(checked.status).toBe("ready");
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable, verification: "v1-pieces" });
  if (result.status === "unsupported") {
    expect(result.reason).toContain("CoW");
    expect(await fs.readFile(original.path, "utf8")).toBe("abcdefgX");
    return;
  }
  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("Expected isolated partial clone");
  const destination = path.join(result.dataPath, "pack/E01.mkv");
  expect((await fs.lstat(destination)).isFile()).toBe(true);
  expect((await fs.stat(destination)).ino).not.toBe((await fs.stat(original.path)).ino);
  await fs.writeFile(destination, "abcdefgh");
  expect(await fs.readFile(original.path, "utf8")).toBe("abcdefgX");
  expect(await fs.readFile(destination, "utf8")).toBe("abcdefgh");
});

test("large original pieces are read in bounded piece-sized chunks including a cross-file tail", async () => {
  const fixture = await setup(), text = "x".repeat(17 * 1024 * 1024 + 13);
  const first = await fixture.source("large", "large.mkv", text);
  const tail = await fixture.source("tail", "tail.mkv", "end");
  const candidate = manifest([{ name: "large.mkv", text }, { name: "tail.mkv", text: "end" }], 16 * 1024 * 1024);
  const open = fs.open.bind(fs);
  let reads = 0;
  fs.open = (async (...args: Parameters<typeof fs.open>) => {
    const handle = await open(...args);
    if ([first.path, tail.path].includes(String(args[0]))) {
      const read = handle.read.bind(handle);
      handle.read = ((...readArgs: unknown[]) => { reads++; return Reflect.apply(read, handle, readArgs); }) as typeof handle.read;
    }
    return handle;
  }) as typeof fs.open;
  try {
    const proof = await verifyContentReuse(fixture.plan(candidate, [first, tail], 0));
    expect(proof.summary.verifiedReuseBytes).toBe(text.length + 3);
    expect(proof.pieces.every(piece => piece.status === "verified")).toBe(true);
    expect(reads).toBe(3);
  } finally { fs.open = open; }
});

test("journal proof replays an exact seed-only layout without rereading payload, but rejects changed sources", async () => {
  const fixture = await setup();
  const source = await fixture.source("1", "canonical.mkv", "abcdefgh", true);
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [source], 0);
  const open = fs.open.bind(fs);
  let payloadReads = 0;
  fs.open = (async (...args: Parameters<typeof fs.open>) => {
    const handle = await open(...args);
    if (String(args[0]) === source.path) {
      const read = handle.read.bind(handle);
      handle.read = ((...readArgs: unknown[]) => {
        payloadReads++;
        return Reflect.apply(read, handle, readArgs);
      }) as typeof handle.read;
    }
    return handle;
  }) as typeof fs.open;
  try {
    const staged = await materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly });
    if (staged.status !== "ready") throw new Error("Expected exact seed-only layout");
    expect(payloadReads).toBeGreaterThan(0);
    const initialReads = payloadReads;
    const replay = await materializeReuseLayout({ root: fixture.root, plan: staged.plan, capabilities: readOnly,
      verification: "journal-v1-pieces" });
    expect(replay).toMatchObject({ status: "ready", replay: true, dataPath: staged.dataPath });
    expect(payloadReads).toBe(initialReads);
    const fabricated = structuredClone(staged.plan); fabricated.pieces[0]!.actualSha1 = "f".repeat(40);
    await expect(materializeReuseLayout({ root: fixture.root, plan: fabricated, capabilities: readOnly,
      verification: "journal-v1-pieces" })).rejects.toThrow("Invalid piece evidence");
    await fs.chmod(source.path, 0o600); await fs.writeFile(source.path, "abcdefgX");
    await expect(materializeReuseLayout({ root: fixture.root, plan: staged.plan, capabilities: readOnly,
      verification: "journal-v1-pieces" })).rejects.toThrow("identity or digest changed");
  } finally { fs.open = open; }
});

test("journal proof cannot create a replacement layout or accept a replaced symlink", async () => {
  const fixture = await setup();
  const source = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const proof = await verifyContentReuse(fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [source], 0));
  await expect(materializeReuseLayout({ root: fixture.root, plan: proof, capabilities: readOnly,
    verification: "journal-v1-pieces" })).rejects.toThrow("existing published layout");
  const staged = await materializeReuseLayout({ root: fixture.root, plan: proof, capabilities: readOnly });
  if (staged.status !== "ready") throw new Error("Expected seed-only layout");
  const view = path.join(staged.dataPath, "pack/E01.mkv");
  await fs.unlink(view); await fs.symlink(path.join(fixture.base, "foreign"), view);
  await expect(materializeReuseLayout({ root: fixture.root, plan: staged.plan, capabilities: readOnly,
    verification: "journal-v1-pieces" })).rejects.toThrow("Reuse symlink changed");
});

test("default likely staging skips target hashes, isolates client repair and keeps sidecars private", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgX");
  const candidate = manifest([{ name: "E01.mkv", text: "abcdefgh" }, { name: "info.nfo", text: "!!", role: "sidecar" }]);
  const plan = fixture.plan(candidate, [original], 2);
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable });
  if (result.status === "unsupported") { expect(result.reason).toContain("CoW"); return; }
  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("Expected optimistic writable clone");
  expect(result.plan.verification).toBe("pending");
  expect(result.plan.summary.verifiedReuseBytes).toBe(0);
  await fs.writeFile(path.join(result.dataPath, "pack/info.nfo"), "!!");
  await fs.writeFile(path.join(result.dataPath, "pack/E01.mkv"), "abcdefgh");
  const replay = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable });
  expect(replay).toMatchObject({ status: "ready", replay: true, dataPath: result.dataPath });
  expect(await fs.readFile(original.path, "utf8")).toBe("abcdefgX");
  expect(await fs.readFile(path.join(result.dataPath, "pack/info.nfo"), "utf8")).toBe("!!");
});

test("writable full exact reuse also requires CoW, even when hashes fully match", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original], 0);
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable });
  if (result.status === "unsupported") { expect(result.reason).toContain("CoW"); return; }
  expect(result.status).toBe("ready");
  if (result.status !== "ready") throw new Error("Expected writable full clone");
  const destination = path.join(result.dataPath, "pack/E01.mkv");
  expect((await fs.lstat(destination)).isSymbolicLink()).toBe(false);
  await fs.writeFile(destination, "XXXXXXXX");
  expect(await fs.readFile(original.path, "utf8")).toBe("abcdefgh");
});

test("CoW capability unavailable blocks safely, with no silent full copy or published layout", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
  const result = await materializeReuseLayout({ root: fixture.root, plan,
    capabilities: { symlinkMode: "never", cow: "unavailable" } });
  expect(result.status).toBe("unsupported");
  expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
  expect(await fs.readFile(original.path, "utf8")).toBe("abcdefgh");
});

test("tail mismatch cannot be exposed as a supposedly read-only verified symlink", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgX");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original], 4);
  expect((await materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly })).status).toBe("unsupported");
  expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
});

test("caller-supplied proof flags cannot turn mismatching bytes into a verified symlink", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgX");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original], 4);
  plan.files[0].proof = "verified-full";
  plan.files[0].verifiedBytes = 8;
  plan.verification = "v1-pieces";
  plan.summary.verifiedReuseBytes = 8;
  plan.status = "ready";
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly });
  expect(result.status).toBe("unsupported");
  expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
});

test("a supplied cached digest is checked rather than mistaken for current proof", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const cached = await captureReuseFile({ root: fixture.sourcesRoot, sourceId: "cached", sourcePath: original.path,
    sha256: "0".repeat(64) });
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [cached]);
  await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: writable })).rejects.toThrow("digest");
  expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
});

test("verified sidecar gaps count all cross-file pieces against the download budget", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdef");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdef" }, { name: "info.nfo", text: "!!", role: "sidecar" }]), [original], 2);
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable, verification: "v1-pieces" });
  expect(result.status).toBe("rejected");
  if (result.status !== "rejected") throw new Error("Expected bounded repair rejection");
  expect(result.plan.summary.extraDownloadBytesUpperBound).toBe(4);
  expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
});

test("read-only replay is idempotent and concurrent publications cannot overwrite one another", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original], 0);
  const results = await Promise.all([
    materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly }),
    materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly }),
  ]);
  expect(results.map((result) => result.status)).toEqual(["ready", "ready"]);
  const ready = results.filter((result) => result.status === "ready");
  expect(ready.map((result) => result.replay).sort()).toEqual([false, true]);
  expect(ready[0].layoutPath).toBe(ready[1].layoutPath);
  expect((await fs.readdir(fixture.root.path)).length).toBe(3);
});

test("foreign destination collisions are not overwritten", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
  const foreign = path.join(fixture.root.path, plan.id);
  await fs.mkdir(foreign);
  await fs.writeFile(path.join(foreign, "keep"), "operator data");
  await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly })).rejects.toThrow("collision");
  expect(await fs.readFile(path.join(foreign, "keep"), "utf8")).toBe("operator data");
});

test("source mutation, inode replacement and root marker replacement invalidate snapshots", async () => {
  for (const mutation of ["bytes", "inode", "marker", "root"] as const) {
    const fixture = await setup();
    const original = await fixture.source("1", "canonical.mkv", "abcdefgh", true);
    const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
    if (mutation === "bytes") { await fs.chmod(original.path, 0o600); await fs.writeFile(original.path, "abcdefgX"); }
    if (mutation === "inode") { await fs.unlink(original.path); await fs.writeFile(original.path, "abcdefgh"); }
    if (mutation === "marker") {
      const marker = path.join(fixture.sourcesRoot.path, ".podible-content.json");
      await fs.rename(marker, `${marker}.old`);
      await fs.writeFile(marker, JSON.stringify({ id: "owned-content" }));
    }
    if (mutation === "root") {
      await fs.rename(fixture.sourcesRoot.path, `${fixture.sourcesRoot.path}.old`);
      await fs.mkdir(fixture.sourcesRoot.path);
      await fs.writeFile(path.join(fixture.sourcesRoot.path, ".podible-content.json"), JSON.stringify({ id: "owned-content" }));
      await fs.writeFile(original.path, "abcdefgh");
    }
    await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly })).rejects.toThrow();
    expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
  }
});

test("owned markers, source symlinks and destination symlink roots fail closed", async () => {
  const fixture = await setup();
  await fs.mkdir(path.join(fixture.base, "foreign"));
  await fs.writeFile(path.join(fixture.base, "foreign/keep"), "foreign");
  await expect(prepareReuseLayoutRoot({ rootPath: path.join(fixture.base, "foreign"), ownerId: "ours" })).rejects.toThrow("nonempty");
  const source = await fixture.source("1", "canonical.mkv", "abcdefgh");
  await fs.symlink(source.path, path.join(fixture.sourcesRoot.path, "alias.mkv"));
  await expect(captureReuseFile({ root: fixture.sourcesRoot, sourceId: "alias", sourcePath: path.join(fixture.sourcesRoot.path, "alias.mkv") }))
    .rejects.toThrow("symlink");
  const alias = path.join(fixture.base, "alias");
  await fs.symlink(fixture.root.path, alias);
  await expect(prepareReuseLayoutRoot({ rootPath: alias, ownerId: "owned-reuse" })).rejects.toThrow("symlink");
});

test("destination root inode, mount identity and marker digest are fenced before any writes", async () => {
  for (const mutation of ["inode", "device", "marker"] as const) {
    const fixture = await setup();
    const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
    const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
    if (mutation === "inode") {
      await fs.rename(fixture.root.path, `${fixture.root.path}.old`);
      await prepareReuseLayoutRoot({ rootPath: fixture.root.path, ownerId: "owned-reuse" });
    }
    if (mutation === "device") fixture.root.device = "different-device";
    if (mutation === "marker") await fs.writeFile(path.join(fixture.root.path, ".podible-reuse-root.json"),
      JSON.stringify({ id: "owned-reuse", altered: true }));
    await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: writable })).rejects.toThrow("changed");
    expect(await fs.readdir(fixture.root.path)).toEqual([".podible-reuse-root.json"]);
  }
});

test("replay rejects client path substitution with a hardlink to canonical source", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable });
  if (result.status === "unsupported") { expect(result.reason).toContain("CoW"); return; }
  if (result.status !== "ready") throw new Error("Expected layout");
  const destination = path.join(result.dataPath, "pack/E01.mkv");
  await fs.unlink(destination);
  await fs.link(original.path, destination);
  await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: writable })).rejects.toThrow("identity");
});

test("default stat-only path fences source mutation on initial staging and replay", async () => {
  for (const stage of ["initial", "replay"] as const) {
    const fixture = await setup();
    const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
    const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
    if (stage === "replay") {
      const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: writable });
      if (result.status === "unsupported") { expect(result.reason).toContain("CoW"); continue; }
      expect(result.status).toBe("ready");
    }
    await fs.chmod(original.path, 0o600);
    await fs.writeFile(original.path, "abcdefgX");
    await expect(materializeReuseLayout({ root: fixture.root, plan, capabilities: writable })).rejects.toThrow("changed");
  }
});

test("interrupted unpublished generations are ignored and never treated as completed layouts", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  const plan = fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]);
  const abandoned = path.join(fixture.root.path, `.podible-reuse-${plan.id}-00000000-0000-0000-0000-000000000000`);
  await fs.mkdir(abandoned);
  await fs.writeFile(path.join(abandoned, "interrupted"), "not committed");
  const result = await materializeReuseLayout({ root: fixture.root, plan, capabilities: readOnly });
  expect(result).toMatchObject({ status: "ready", replay: false });
  expect(await fs.readFile(path.join(abandoned, "interrupted"), "utf8")).toBe("not committed");
});

test("native forced-clone availability is explicit, never emulated with a normal copy", async () => {
  const fixture = await setup();
  const original = await fixture.source("1", "canonical.mkv", "abcdefgh");
  let supported = true;
  try { await fs.copyFile(original.path, path.join(fixture.base, "probe"), constants.COPYFILE_FICLONE_FORCE | constants.COPYFILE_EXCL); }
  catch (error) {
    if (!["ENOTSUP", "EOPNOTSUPP", "EXDEV", "ENOSYS", "EINVAL"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
    supported = false;
  }
  const result = await materializeReuseLayout({ root: fixture.root,
    plan: fixture.plan(manifest([{ name: "E01.mkv", text: "abcdefgh" }]), [original]), capabilities: writable });
  expect(result.status).toBe(supported ? "ready" : "unsupported");
});
