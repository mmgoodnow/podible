import { constants, type BigIntStats } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import type { RetentionDisk, RetentionFilePlan } from "./retention-policy-types";
import type { PhysicalFile } from "./retention-service";

const EXT4 = 0xef53n;
const safe = (value: bigint) => value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER);

export async function observeRetentionDisk(root: string): Promise<RetentionDisk> {
  const observedAt = new Date().toISOString();
  try {
    if (!path.isAbsolute(root) || await fs.realpath(root) !== root) throw new Error("Noncanonical mount");
    const stat = await fs.lstat(root, { bigint: true }), disk = await fs.statfs(root, { bigint: true });
    const total = disk.blocks * disk.bsize, available = disk.bavail * disk.bsize;
    if (!stat.isDirectory() || !safe(total) || !safe(available) || total <= 0n || available > total) throw new Error("Invalid filesystem counters");
    return { state: "known", filesystem: `0x${disk.type.toString(16)}`, device: String(stat.dev), totalBytes: Number(total),
      availableBytes: Number(available), observedAt, reason: "Observed filesystem available blocks; not physical storage-pool savings." };
  } catch {
    return { state: "unknown", filesystem: null, device: null, totalBytes: null, availableBytes: null, observedAt,
      reason: "Canonical movie filesystem is unavailable or its counters are unknown." };
  }
}

/** ext4 has no in-filesystem reflink/snapshot sharing. External pool savings are never inferred. */
export async function ext4ReclaimableAllocation(file: PhysicalFile): Promise<number | null> {
  if (process.platform !== "linux") return null;
  try {
    const disk = await fs.statfs(file.canonicalPath, { bigint: true });
    if (disk.type !== EXT4 || file.linkCount !== 1) return null;
    const handle = await fs.open(file.canonicalPath, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const stat = await handle.stat({ bigint: true });
      if (!stat.isFile() || String(stat.dev) !== file.device || String(stat.ino) !== file.inode || stat.nlink !== 1n ||
          stat.size !== BigInt(file.sizeBytes) || String(stat.ctimeNs) !== file.ctimeNs || stat.blocks * 512n !== BigInt(file.allocatedBytes)) return null;
      return file.allocatedBytes;
    } finally { await handle.close(); }
  } catch { return null; }
}

export async function inspectRetentionFile(root: string, file: Pick<RetentionFilePlan, "relativePath">) {
  const relative = file.relativePath;
  if (path.isAbsolute(relative) || /[\\\x00-\x1f]/.test(relative) || relative.split("/").some((part) => !part || part === "." || part === ".."))
    throw new Error("Unsafe retention path");
  let current = root;
  for (const [index, part] of relative.split("/").entries()) {
    current = path.join(current, part);
    let stat;
    try { stat = await fs.lstat(current, { bigint: true }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
    if (stat.isSymbolicLink() || (index < relative.split("/").length - 1 ? !stat.isDirectory() : !stat.isFile()))
      throw new Error("Retention path is not a canonical regular file");
  }
  const handle = await fs.open(current, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat({ bigint: true });
    if (await fs.realpath(current) !== current || !stat.isFile()) throw new Error("Retention object changed during inspection");
    return { path: current, stat };
  } finally { await handle.close(); }
}

export function matchesRetentionFile(stat: BigIntStats, file: RetentionFilePlan, moved = false) {
  return String(stat.dev) === file.device && String(stat.ino) === file.inode && stat.size === BigInt(file.sizeBytes) &&
    stat.nlink === 1n && String(stat.mtimeNs) === file.mtimeNs && (moved || String(stat.ctimeNs) === file.ctimeNs) &&
    stat.blocks * 512n === BigInt(file.allocatedBytes);
}

export async function retentionDigest(file: string) {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const before = await handle.stat({ bigint: true }), hash = createHash("sha256"), bytes = Buffer.alloc(1024 * 1024);
    for (;;) { const read = await handle.read(bytes); if (!read.bytesRead) break; hash.update(bytes.subarray(0, read.bytesRead)); }
    const after = await handle.stat({ bigint: true });
    if (!before.isFile() || after.dev !== before.dev || after.ino !== before.ino || after.size !== before.size ||
        after.ctimeNs !== before.ctimeNs || after.mtimeNs !== before.mtimeNs) throw new Error("Retention object changed during byte verification");
    return hash.digest("hex");
  } finally { await handle.close(); }
}
