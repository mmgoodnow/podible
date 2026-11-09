import { promises as fs } from "node:fs";
import { torrentFileRole } from "./content-file-role";

const headroom = 64n * 1024n ** 2n;

/** Check actual target volumes before starting a new download. Does not reserve space. */
export async function downloadCapacityProblem(input: {
  sourceRoot: string; contentRoot: string; canonical: boolean;
  files: readonly { path: string; size: number }[];
}, read = async (root: string) => {
  const [stat, space] = await Promise.all([fs.stat(root, { bigint: true }), fs.statfs(root, { bigint: true })]);
  return { device: stat.dev, available: space.bavail * space.bsize };
}): Promise<string | null> {
  let canonicalBytes = 0n, downloadBytes = 0n;
  for (const file of input.files) {
    if (input.canonical && torrentFileRole(file.path) === "content") canonicalBytes += BigInt(file.size);
    else downloadBytes += BigInt(file.size);
  }
  const volumes = new Map<bigint, { required: bigint; available: bigint }>();
  for (const [root, bytes] of [[input.contentRoot, canonicalBytes], [input.sourceRoot, downloadBytes]] as const) {
    if (bytes === 0n) continue;
    const { device, available } = await read(root);
    const prior = volumes.get(device);
    volumes.set(device, { required: (prior?.required ?? 0n) + bytes,
      available: prior && prior.available < available ? prior.available : available });
  }
  for (const { required, available } of volumes.values())
    if (available < required + headroom)
      return `Not enough free disk space for this release: ${required + headroom} bytes needed including headroom, ${available} available`;
  return null;
}
