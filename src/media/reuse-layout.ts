import { constants, promises as fs } from "node:fs";
import type { BigIntStats } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import {
  applyReusePieceEvidence, assertReusePath, planContentReuse, reuseDigest,
  type ReuseFileIdentity, type ReusePieceEvidence, type ReusePlan, type ReusePlanFile,
  type ReuseRootSnapshot, type ReuseSourceFile,
} from "./reuse-plan";

const ROOT_MARKER = ".podible-reuse-root.json";
const LAYOUT_MARKER = ".podible-reuse-layout.json";
const READ = constants.O_RDONLY | constants.O_NOFOLLOW;

function statIdentity(stat: BigIntStats) {
  if (!stat.isFile() || stat.size > BigInt(Number.MAX_SAFE_INTEGER))
    throw new Error("Reuse source is not a regular bounded file");
  return { device: String(stat.dev), inode: String(stat.ino), size: Number(stat.size),
    mtimeNs: String(stat.mtimeNs), ctimeNs: String(stat.ctimeNs) };
}

async function directory(directoryPath: string) {
  if (path.resolve(directoryPath) !== directoryPath || (await fs.realpath(directoryPath)) !== directoryPath)
    throw new Error("Reuse directory traverses a symlink");
  const stat = await fs.lstat(directoryPath, { bigint: true });
  if (!stat.isDirectory()) throw new Error("Reuse root is not a directory");
  return stat;
}

async function fileDigest(filePath: string) {
  const handle = await fs.open(filePath, READ);
  try {
    const before = statIdentity(await handle.stat({ bigint: true }));
    const hash = createHash("sha256");
    const buffer = Buffer.alloc(1024 * 1024);
    for (;;) {
      const { bytesRead } = await handle.read(buffer);
      if (!bytesRead) break;
      hash.update(buffer.subarray(0, bytesRead));
    }
    const after = statIdentity(await handle.stat({ bigint: true }));
    const current = statIdentity(await fs.lstat(filePath, { bigint: true }));
    if (reuseDigest(before) !== reuseDigest(after) || reuseDigest(before) !== reuseDigest(current))
      throw new Error("Reuse source changed while hashing");
    return { ...before, sha256: hash.digest("hex") } satisfies ReuseFileIdentity;
  } finally {
    await handle.close();
  }
}

async function readOwnedJson(filePath: string, maxBytes: number): Promise<{ value: unknown; identity: ReuseFileIdentity & { sha256: string } }> {
  const handle = await fs.open(filePath, READ);
  try {
    const before = statIdentity(await handle.stat({ bigint: true }));
    if (before.size > maxBytes) throw new Error("Ownership marker is too large");
    const bytes = await handle.readFile();
    const after = statIdentity(await handle.stat({ bigint: true }));
    const current = statIdentity(await fs.lstat(filePath, { bigint: true }));
    if (bytes.length !== before.size || reuseDigest(before) !== reuseDigest(after) ||
      reuseDigest(current) !== reuseDigest(before)) throw new Error("Ownership marker changed");
    return { value: JSON.parse(bytes.toString("utf8")), identity: {
      ...before, sha256: createHash("sha256").update(bytes).digest("hex"),
    } };
  } finally {
    await handle.close();
  }
}

export async function captureReuseRoot(input: {
  rootPath: string; markerName: string; ownerId?: string;
}): Promise<ReuseRootSnapshot> {
  assertReusePath(input.markerName);
  if (input.markerName.includes("/")) throw new Error("Ownership marker must be a root file");
  const stat = await directory(input.rootPath);
  const marker = await readOwnedJson(path.join(input.rootPath, input.markerName), 16 * 1024);
  const ownerId = (marker.value as { id?: unknown } | null)?.id;
  if (typeof ownerId !== "string" || !ownerId || ownerId.length > 256 ||
    (input.ownerId !== undefined && ownerId !== input.ownerId) || marker.identity.device !== String(stat.dev))
    throw new Error("Invalid reuse root ownership marker");
  return { path: input.rootPath, device: String(stat.dev), inode: String(stat.ino),
    marker: { name: input.markerName, ownerId, sha256: marker.identity.sha256,
      device: marker.identity.device, inode: marker.identity.inode } };
}

async function assertRoot(root: ReuseRootSnapshot) {
  const current = await captureReuseRoot({ rootPath: root.path, markerName: root.marker.name, ownerId: root.marker.ownerId });
  if (reuseDigest(current) !== reuseDigest(root)) throw new Error("Reuse root mount, inode or marker changed");
}

async function syncDirectory(directoryPath: string) {
  const handle = await fs.open(directoryPath, READ | constants.O_DIRECTORY);
  try { await handle.sync(); } finally { await handle.close(); }
}

/** Initializes only an empty, real directory; never claims pre-existing foreign data. */
export async function prepareReuseLayoutRoot(input: {
  rootPath: string; ownerId: string;
}): Promise<ReuseRootSnapshot> {
  if (!input.ownerId || input.ownerId.length > 256) throw new Error("Invalid reuse root owner");
  if (path.resolve(input.rootPath) !== input.rootPath) throw new Error("Reuse root must be an absolute normalized path");
  try { await fs.mkdir(input.rootPath, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  await directory(input.rootPath);
  const markerPath = path.join(input.rootPath, ROOT_MARKER);
  const existing = await fs.lstat(markerPath).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
  if (!existing) {
    if ((await fs.readdir(input.rootPath)).length) throw new Error("Refusing to claim nonempty reuse root");
    const handle = await fs.open(markerPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    try { await handle.writeFile(JSON.stringify({ version: 1, id: input.ownerId })); await handle.sync(); }
    finally { await handle.close(); }
    await syncDirectory(input.rootPath);
  }
  return captureReuseRoot({ rootPath: input.rootPath, markerName: ROOT_MARKER, ownerId: input.ownerId });
}

export async function captureReuseFile(input: {
  root: ReuseRootSnapshot; sourceId: string; sourcePath: string; sourceTorrentId?: string;
  /** Stat-only by default. A cached digest or explicit hash is optional evidence. */
  sha256?: string | null; hash?: boolean;
}): Promise<ReuseSourceFile> {
  await assertRoot(input.root);
  if (!input.sourceId || path.resolve(input.sourcePath) !== input.sourcePath ||
    !input.sourcePath.startsWith(`${input.root.path}${path.sep}`) ||
    input.sourcePath === path.join(input.root.path, input.root.marker.name) ||
    (await fs.realpath(input.sourcePath)) !== input.sourcePath)
    throw new Error("Reuse source escapes owned root or traverses a symlink");
  if (input.sha256 != null && !/^[a-f0-9]{64}$/.test(input.sha256)) throw new Error("Invalid cached source digest");
  const identity: ReuseFileIdentity = input.hash ? await fileDigest(input.sourcePath) : {
    ...await sourceStat(input.sourcePath), ...(input.sha256 !== undefined ? { sha256: input.sha256 } : {}),
  };
  if (input.hash && input.sha256 != null && identity.sha256 !== input.sha256)
    throw new Error("Cached reuse source digest differs from file");
  if (identity.device !== input.root.device) throw new Error("Reuse source is on a different mount");
  await assertRoot(input.root);
  return { id: input.sourceId, path: input.sourcePath, size: identity.size,
    sourceTorrentId: input.sourceTorrentId, root: structuredClone(input.root), identity };
}

async function assertSource(source: ReuseSourceFile, hash = true) {
  const current = await captureReuseFile({ root: source.root, sourceId: source.id,
    sourcePath: source.path, sourceTorrentId: source.sourceTorrentId,
    sha256: source.identity.sha256, hash: hash && source.identity.sha256 != null });
  if (reuseDigest(current) !== reuseDigest(source)) throw new Error("Owned reuse source identity or digest changed");
}

/** Prepopulate a private writable reservation; never give a downloader a shared inode. */
export async function prefillReuseFile(source: ReuseSourceFile, destination: string, active: () => void) {
  await assertSource(source, false);
  const temporary = `${destination}.${randomUUID()}.prefill`;
  try {
    try { await fs.copyFile(source.path, temporary, constants.COPYFILE_EXCL | constants.COPYFILE_FICLONE_FORCE); }
    catch (error) {
      if (!isCloneUnsupported(error)) throw error;
      await fs.copyFile(source.path, temporary, constants.COPYFILE_EXCL);
    }
    await assertSource(source, false);
    const handle = await fs.open(temporary, READ);
    try {
      const copy = statIdentity(await handle.stat({ bigint: true }));
      if (copy.size !== source.size || (copy.device === source.identity.device && copy.inode === source.identity.inode))
        throw new Error("Reuse prefill is not a complete private file");
      await handle.chmod(0o644);
      await handle.sync();
    } finally { await handle.close(); }
    if (await maybeStat(destination)) throw new Error("Prefill destination already exists");
    active();
    await fs.rename(temporary, destination);
    await syncDirectory(path.dirname(destination));
  } finally { await fs.unlink(temporary).catch(error => { if (error.code !== "ENOENT") throw error; }); }
}

async function sourceStat(filePath: string) {
  const handle = await fs.open(filePath, READ);
  try {
    const stat = statIdentity(await handle.stat({ bigint: true }));
    if (reuseDigest(statIdentity(await fs.lstat(filePath, { bigint: true }))) !== reuseDigest(stat))
      throw new Error("Reuse source changed while capturing identity");
    return stat;
  } finally { await handle.close(); }
}

function rebuildPlan(plan: ReusePlan) {
  const rebuilt = planContentReuse({ candidate: plan.candidate, sources: plan.sources,
    bindings: plan.bindings, policy: plan.policy });
  if (rebuilt.id !== plan.id || plan.version !== 1) throw new Error("Reuse plan identity changed");
  return rebuilt;
}

/** Hashes original v1 pieces across real selected files, including cross-file pieces. */
export async function verifyContentReuse(plan: ReusePlan): Promise<ReusePlan> {
  const rebuilt = rebuildPlan(plan);
  const selected = [...new Map(rebuilt.files.flatMap((file) => file.source ? [[file.source.id, file.source] as const] : [])).values()];
  for (const source of selected) await assertSource(source);
  const manifest = rebuilt.candidate.v1;
  if (!manifest) return applyReusePieceEvidence(rebuilt, []);
  const pieces: ReusePieceEvidence[] = [];
  const buffer = Buffer.alloc(Math.min(16 * 1024 * 1024, manifest.pieceLength));
  let opened: { source: ReuseSourceFile; handle: Awaited<ReturnType<typeof fs.open>> } | undefined;
  const checkHandle = async (entry: NonNullable<typeof opened>) => {
    const { sha256: _digest, ...expected } = entry.source.identity;
    if (reuseDigest(statIdentity(await entry.handle.stat({ bigint: true }))) !== reuseDigest(expected))
      throw new Error("Reuse source changed during piece verification");
  };
  const close = async () => {
    if (!opened) return;
    const entry = opened; opened = undefined;
    try { await checkHandle(entry); } finally { await entry.handle.close(); }
  };
  const sourceHandle = async (source: ReuseSourceFile) => {
    if (opened?.source.id !== source.id) {
      await close();
      opened = { source, handle: await fs.open(source.path, READ) };
      await checkHandle(opened);
    }
    return opened!.handle;
  };
  let fileIndex = 0;
  try {
    for (let index = 0; index < manifest.pieceHashes.length; index++) {
      const start = index * manifest.pieceLength;
      const end = Math.min(rebuilt.summary.totalBytes, start + manifest.pieceLength);
      const hash = createHash("sha1");
      let available = true;
      while (fileIndex < rebuilt.files.length &&
        rebuilt.files[fileIndex].offset + rebuilt.files[fileIndex].size <= start) fileIndex++;
      for (let i = fileIndex; i < rebuilt.files.length && rebuilt.files[i].offset < end; i++) {
        const file = rebuilt.files[i];
        const from = Math.max(start, file.offset) - file.offset;
        const until = Math.min(end, file.offset + file.size) - file.offset;
        if (until <= from) continue;
        if (!file.source || file.source.size < until) { available = false; break; }
        const handle = await sourceHandle(file.source);
        let position = from;
        while (position < until) {
          const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, until - position), position);
          if (!bytesRead) throw new Error("Reuse source truncated during verification");
          hash.update(buffer.subarray(0, bytesRead));
          position += bytesRead;
        }
      }
      if (!available) pieces.push({ index, status: "unavailable" });
      else {
        const actualSha1 = hash.digest("hex");
        pieces.push({ index, status: actualSha1 === manifest.pieceHashes[index] ? "verified" : "mismatch", actualSha1 });
      }
    }
  } finally { await close(); }
  for (const source of selected) await assertSource(source);
  return applyReusePieceEvidence(rebuilt, pieces);
}

export interface ReuseLayoutCapabilities {
  /** This means the adapter guarantees no repair/download writes, not just a paused torrent. */
  symlinkMode: "never" | "verified-read-only" | "optimistic-read-only";
  cow: "clone-force" | "unavailable";
}

export type ReuseLayoutResult =
  | { status: "ready"; layoutPath: string; dataPath: string; plan: ReusePlan; replay: boolean }
  | { status: "unsupported"; reason: string }
  | { status: "rejected"; reason: string; plan: ReusePlan };

interface LayoutReceipt {
  version: 1;
  planId: string;
  definitionDigest: string;
  capabilities: ReuseLayoutCapabilities;
  generation: { device: string; inode: string };
  files: Array<{ path: string; kind: "symlink" | "clone" | "missing"; device?: string; inode?: string }>;
}

async function regularDirectories(root: string, relative: string) {
  const parts = relative.split("/").slice(0, -1);
  let current = root;
  for (const part of parts) {
    current = path.join(current, part);
    try { await fs.mkdir(current, { mode: 0o700 }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    await directory(current);
  }
}

async function maybeStat(filePath: string) {
  return fs.lstat(filePath, { bigint: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
}

function isCloneUnsupported(error: unknown) {
  return ["ENOTSUP", "EOPNOTSUPP", "EXDEV", "ENOSYS", "EINVAL"].includes((error as NodeJS.ErrnoException).code ?? "");
}

/** Recover a published mode after a lost journal checkpoint; materialization still fully validates it. */
export async function publishedReuseLayoutCapabilities(root: ReuseRootSnapshot, plan: ReusePlan): Promise<ReuseLayoutCapabilities | null> {
  await assertRoot(root);
  const pointer = path.join(root.path, plan.id);
  if (!await maybeStat(pointer)) return null;
  const target = await fs.readlink(pointer);
  if (!new RegExp(`^\\.podible-reuse-${plan.id}-[a-f0-9-]{36}$`).test(target))
    throw new Error("Reuse layout collision: foreign generation");
  const generation = path.join(root.path, target);
  const stat = await directory(generation);
  const receipt = (await readOwnedJson(path.join(generation, LAYOUT_MARKER), 32 * 1024 * 1024)).value as LayoutReceipt;
  if (receipt.version !== 1 || receipt.planId !== plan.id ||
    receipt.generation.device !== String(stat.dev) || receipt.generation.inode !== String(stat.ino) ||
    !["never", "verified-read-only", "optimistic-read-only"].includes(receipt.capabilities?.symlinkMode) ||
    !["clone-force", "unavailable"].includes(receipt.capabilities?.cow) ||
    receipt.definitionDigest !== reuseDigest({ id: plan.id, root, capabilities: receipt.capabilities }))
    throw new Error("Reuse layout receipt or generation identity changed");
  return receipt.capabilities;
}

/** Publishes one fully staged generation through an exclusive deterministic pointer. */
export async function materializeReuseLayout(input: {
  plan: ReusePlan; root: ReuseRootSnapshot; capabilities: ReuseLayoutCapabilities;
  /** Client mode stages likely bytes without pre-reading them; parent enforces the post-check budget. */
  verification?: "client" | "v1-pieces" | "journal-v1-pieces" | "optimistic";
}): Promise<ReuseLayoutResult> {
  const { root, capabilities } = input;
  // Only the server's persisted staging result may supply this proof. It is
  // reusable while every captured source identity and the published layout agree.
  const journalProof = input.verification === "journal-v1-pieces";
  const optimistic = input.verification === "optimistic";
  const checkSource = (source: ReuseSourceFile) => assertSource(source, !journalProof && !optimistic);
  if (!["never", "verified-read-only", "optimistic-read-only"].includes(capabilities.symlinkMode) ||
    !["clone-force", "unavailable"].includes(capabilities.cow)) throw new Error("Invalid reuse client capabilities");
  await assertRoot(root);
  for (const source of input.plan.sources)
    if (source.root.path === root.path || source.root.path.startsWith(`${root.path}${path.sep}`) ||
      root.path.startsWith(`${source.root.path}${path.sep}`)) throw new Error("Reuse source and destination roots must be separate");
  if (capabilities.symlinkMode === "optimistic-read-only" && !optimistic)
    throw new Error("Optimistic links require the seed-only optimistic path");
  const plan = optimistic ? rebuildPlan(input.plan) : journalProof ? applyReusePieceEvidence(rebuildPlan(input.plan), input.plan.pieces) :
    input.verification === "v1-pieces" || capabilities.symlinkMode === "verified-read-only"
      ? await verifyContentReuse(input.plan) : rebuildPlan(input.plan);
  if (journalProof && (capabilities.symlinkMode !== "verified-read-only" || input.plan.verification !== "v1-pieces" ||
    reuseDigest(plan) !== reuseDigest(input.plan) || plan.summary.verifiedReuseBytes !== plan.summary.totalBytes))
    throw new Error("Journal reuse proof is not a complete verified staging result");
  if (plan.status === "rejected") return { status: "rejected", reason: plan.reasons.join("; "), plan };
  if (optimistic && plan.files.some(file => !file.source || file.source.size !== file.size))
    return { status: "unsupported", reason: "Optimistic seeding needs a complete exact-size layout; partial layouts require a client check" };
  const canLink = (file: ReusePlanFile) => capabilities.symlinkMode === "optimistic-read-only" ||
    (capabilities.symlinkMode === "verified-read-only" && (optimistic || file.proof === "verified-full"));
  for (const source of new Map(plan.files.flatMap((file) => file.source ? [[file.source.id, file.source] as const] : [])).values())
    await checkSource(source);
  const layoutPath = path.join(root.path, plan.id);
  const definitionDigest = reuseDigest({ id: plan.id, root, capabilities });

  async function replay(): Promise<ReuseLayoutResult | null> {
    const pointer = await maybeStat(layoutPath);
    if (!pointer) return null;
    if (!pointer.isSymbolicLink()) throw new Error("Reuse layout collision: destination is not an owned pointer");
    const target = await fs.readlink(layoutPath);
    if (!new RegExp(`^\\.podible-reuse-${plan.id}-[a-f0-9-]{36}$`).test(target))
      throw new Error("Reuse layout collision: foreign generation");
    const generation = path.join(root.path, target);
    const stat = await directory(generation);
    const receipt = (await readOwnedJson(path.join(generation, LAYOUT_MARKER), 32 * 1024 * 1024)).value as LayoutReceipt;
    if (receipt.version !== 1 || receipt.planId !== plan.id || receipt.definitionDigest !== definitionDigest ||
      receipt.generation.device !== String(stat.dev) || receipt.generation.inode !== String(stat.ino) ||
      !Array.isArray(receipt.files) || receipt.files.length !== plan.files.length)
      throw new Error("Reuse layout receipt or generation identity changed");
    const data = path.join(generation, "data");
    await directory(data);
    for (let index = 0; index < plan.files.length; index++) {
      const file = plan.files[index];
      const recorded = receipt.files[index];
      if (recorded.path !== file.candidatePath) throw new Error("Reuse layout receipt path changed");
      await checkParents(data, file.candidatePath);
      const destination = path.join(data, file.candidatePath);
      const output = await maybeStat(destination);
      if (recorded.kind === "missing" && !output) continue;
      if (!output) throw new Error("Reuse layout file disappeared");
      if (recorded.kind === "symlink") {
        if (!canLink(file) ||
          !output.isSymbolicLink() || (await fs.readlink(destination)) !== file.source?.path)
          throw new Error("Reuse symlink changed or is no longer verified");
      } else if ((recorded.kind !== "clone" && recorded.kind !== "missing") ||
        !output.isFile() || output.size !== BigInt(file.size) ||
        (recorded.kind === "clone" && (String(output.dev) !== recorded.device || String(output.ino) !== recorded.inode)) ||
        plan.sources.some((source) => source.identity.device === String(output.dev) && source.identity.inode === String(output.ino)))
        throw new Error("Reuse writable file identity changed or aliases source bytes");
    }
    await assertRoot(root);
    return { status: "ready", layoutPath, dataPath: path.join(layoutPath, "data"), plan, replay: true };
  }

  const existing = await replay();
  if (existing) return existing;
  if (journalProof) throw new Error("Journal reuse proof requires its existing published layout");
  if (plan.files.some((file) => file.source && !canLink(file)) && capabilities.cow === "unavailable")
    return { status: "unsupported", reason: "Writable reuse requires filesystem CoW cloning; full copies are not allowed" };

  const generation = path.join(root.path, `.podible-reuse-${plan.id}-${randomUUID()}`);
  await fs.mkdir(generation, { mode: 0o700 });
  let published = false;
  try {
    const generationStat = await directory(generation);
    if (String(generationStat.dev) !== root.device) throw new Error("Reuse generation crossed a mount");
    const data = path.join(generation, "data");
    await fs.mkdir(data, { mode: 0o700 });
    const files: LayoutReceipt["files"] = [];
    for (const file of plan.files) {
      await assertRoot(root);
      await regularDirectories(data, file.candidatePath);
      const destination = path.join(data, file.candidatePath);
      if (!file.source) { files.push({ path: file.candidatePath, kind: "missing" }); continue; }
      await checkSource(file.source);
      if (canLink(file)) {
        await fs.symlink(file.source.path, destination);
        files.push({ path: file.candidatePath, kind: "symlink" });
        continue;
      }
      try {
        await fs.copyFile(file.source.path, destination, constants.COPYFILE_EXCL | constants.COPYFILE_FICLONE_FORCE);
      } catch (error) {
        if (isCloneUnsupported(error)) return { status: "unsupported", reason: `Filesystem does not support forced CoW clone (${(error as NodeJS.ErrnoException).code})` };
        throw error;
      }
      const clone: ReuseFileIdentity = file.source.identity.sha256 != null
        ? await fileDigest(destination) : await sourceStat(destination);
      if ((clone.device === file.source.identity.device && clone.inode === file.source.identity.inode) ||
        (file.source.identity.sha256 != null && clone.sha256 !== file.source.identity.sha256) || clone.size !== file.source.size)
        throw new Error("CoW clone aliases source or copied different bytes");
      const permissions = await fs.open(destination, READ);
      try {
        const stat = await permissions.stat({ bigint: true });
        if (String(stat.dev) !== clone.device || String(stat.ino) !== clone.inode)
          throw new Error("CoW destination identity changed before chmod");
        await permissions.chmod(0o600);
      } finally { await permissions.close(); }
      const output = await fs.open(destination, constants.O_RDWR | constants.O_NOFOLLOW);
      try {
        const stat = await output.stat({ bigint: true });
        if (String(stat.dev) !== clone.device || String(stat.ino) !== clone.inode)
          throw new Error("CoW destination identity changed before write");
        await output.truncate(file.size);
        await output.chmod(0o600);
        await output.sync();
      } finally { await output.close(); }
      files.push({ path: file.candidatePath, kind: "clone", device: clone.device, inode: clone.inode });
    }
    for (const file of plan.files) if (file.source) await checkSource(file.source);
    const receipt: LayoutReceipt = { version: 1, planId: plan.id, definitionDigest, capabilities,
      generation: { device: String(generationStat.dev), inode: String(generationStat.ino) }, files };
    const marker = await fs.open(path.join(generation, LAYOUT_MARKER), "wx", 0o600);
    try { await marker.writeFile(JSON.stringify(receipt)); await marker.sync(); } finally { await marker.close(); }
    await syncTree(data);
    await syncDirectory(generation);
    await assertRoot(root);
    // symlink(2) is exclusive: unlike rename(2), it cannot replace an existing directory or layout.
    try { await fs.symlink(path.basename(generation), layoutPath, "dir"); published = true; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const winner = await replay();
      if (winner) return winner;
      throw new Error("Reuse layout publication race");
    }
    await syncDirectory(root.path);
    await assertRoot(root);
    return { status: "ready", layoutPath, dataPath: path.join(layoutPath, "data"), plan, replay: false };
  } finally {
    if (!published) {
      await assertRoot(root);
      await fs.rm(generation, { recursive: true, force: true });
    }
  }
}

async function checkParents(root: string, relative: string) {
  let current = root;
  for (const part of relative.split("/").slice(0, -1)) {
    current = path.join(current, part);
    await directory(current);
  }
}

async function syncTree(root: string): Promise<void> {
  for (const entry of await fs.readdir(root, { withFileTypes: true }))
    if (entry.isDirectory()) await syncTree(path.join(root, entry.name));
  await syncDirectory(root);
}
