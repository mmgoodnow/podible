import { constants, promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { PhysicalFile } from "./retention-service";
import type { TvRetentionAudit, TvRetentionInventory, TvRetentionSeedProof } from "./tv-retention-types";
import { inspectRetentionFile } from "./retention-filesystem";
import { recoveryContained, recoveryDirectory } from "./recovery-filesystem";
import { readTransmissionIdentity } from "./managed-transmission";
import { TransmissionRpc } from "./transmission-client";

export type TvRetentionTransmission = { clientId: string; rpc: Pick<TransmissionRpc, "call"> };
export type TvRetentionHashBudget = { remainingBytes: number; deadline: number; onBytesRead?: (file: string, bytes: number) => void };
const count = z.number().int().nonnegative().safe();
const digestIdentity = (stat: import("node:fs").BigIntStats) => [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].map(String).join(":");

/** No cached byte proof, pathname/size equality, or torrent completion counter substitutes for this read. */
export async function tvRetentionDigest(file: string, budget: TvRetentionHashBudget) {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile() || before.size > BigInt(budget.remainingBytes)) throw new Error("TV retention byte-proof budget exhausted");
    const buffer = Buffer.alloc(1024 * 1024), hash = createHash("sha256");
    for (;;) {
      if (performance.now() >= budget.deadline) throw new Error("TV retention proof deadline exhausted");
      const read = await handle.read(buffer);
      if (!read.bytesRead) break;
      budget.remainingBytes -= read.bytesRead;
      budget.onBytesRead?.(file, read.bytesRead);
      if (budget.remainingBytes < 0) throw new Error("TV retention byte-proof budget exhausted");
      hash.update(buffer.subarray(0, read.bytesRead));
    }
    if (digestIdentity(await handle.stat({ bigint: true })) !== digestIdentity(before)) throw new Error("TV proof file changed");
    return hash.digest("hex");
  } finally { await handle.close(); }
}

export async function observeTvRetentionSeeds(input: {
  root: string; objects: PhysicalFile[]; downloads: TvRetentionInventory["downloads"];
  budget: TvRetentionHashBudget; transmission?: TvRetentionTransmission | null;
  memberProofs?: TvRetentionSeedProof[];
  onVerifiedMembers?: (proofs: TvRetentionSeedProof[]) => void;
}): Promise<TvRetentionAudit["seeding"]> {
  const obligations: TvRetentionAudit["seeding"]["obligations"] = input.downloads.slice(0, 20).map((download) => ({
    id: createHash("sha256").update(JSON.stringify([download.id, download.clientId, download.nativeId])).digest("hex"),
    remainingSeedableCopies: null, verifiedSecondsSeeded: null, requiredSecondsSeeded: null, otherRequirements: "unknown",
  }));
  const unknown = (): TvRetentionAudit["seeding"] => ({ state: "last-copy-or-unknown", obligations: obligations.map((o) => ({ ...o, remainingSeedableCopies: null })) });
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    let reader = input.transmission;
    if (reader === undefined) {
      const identity = readTransmissionIdentity();
      reader = { clientId: `transmission:${identity.id}`, rpc: new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc",
        username: identity.username, password: identity.password }) };
    }
    if (!reader || !/^transmission:[a-f0-9-]{36}$/.test(reader.clientId) || !input.objects.length || input.objects.length > 20 ||
        !input.downloads.length || input.downloads.length > 20) return unknown();
    const root = path.join(input.root, "downloads");
    if (!recoveryDirectory(root) || await fs.realpath(root) !== root) return unknown();
    if (input.downloads.some((download) => download.clientId !== reader.clientId || !/^[a-f0-9]{40}$/.test(download.nativeId) ||
        !download.fileIds.length || download.fileIds.some((id) => !input.objects.some((object) => object.id === id)))) return unknown();
    const hashes = new Map<string, string>();
    for (const object of input.objects) {
      const proof = object.sha256 ?? await tvRetentionDigest(object.canonicalPath, input.budget);
      if (!/^[a-f0-9]{64}$/.test(proof)) return unknown();
      hashes.set(object.id!, proof);
    }
    const remaining = Math.min(5000, input.budget.deadline - performance.now());
    if (remaining <= 0) return unknown();
    const expires = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("TV seed observation deadline")), remaining); });
    const result = await Promise.race([reader.rpc.call("torrent-get", { ids: [...new Set(input.downloads.map((download) => download.nativeId))],
      fields: ["hashString", "downloadDir", "labels", "files", "leftUntilDone", "secondsSeeding", "error", "status"] }), expires]);
    const torrents = z.array(z.object({ hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()).max(100),
      leftUntilDone: count, secondsSeeding: count.nullish(), error: count, status: count,
      files: z.array(z.object({ name: z.string(), length: count, bytesCompleted: count })).min(1).max(500),
    })).max(20).parse(result.torrents);
    const covered = new Set<string>(), verified: TvRetentionSeedProof[] = [];
    for (const [index, download] of input.downloads.entries()) {
      const location = z.object({ rootId: z.literal("tv"), relativePath: z.string() }).strict().parse(JSON.parse(download.locationJson));
      const directory = recoveryContained(root, location.relativePath);
      if (!recoveryDirectory(directory)) return unknown();
      const matches = torrents.filter((torrent) => torrent.hashString.toLowerCase() === download.nativeId);
      if (matches.length !== 1) return unknown();
      const torrent = matches[0]!;
      obligations[index]!.verifiedSecondsSeeded = torrent.secondsSeeding ?? null;
      if (!torrent.labels.includes("Podible TV") || torrent.downloadDir !== directory || torrent.leftUntilDone !== 0 ||
          torrent.error !== 0 || ![0, 5, 6].includes(torrent.status)) return unknown();
      const matchesForBinding = new Set<string>();
      if (new Set(torrent.files.map((file) => file.name)).size !== torrent.files.length || torrent.files.some((file) => file.bytesCompleted !== file.length)) return unknown();
      const proofs = input.memberProofs?.filter((proof) => proof.bindingId === download.id && download.fileIds.includes(proof.fileId));
      if (proofs && download.fileIds.some((id) => !proofs.some((proof) => proof.fileId === id && proof.sha256 === hashes.get(id)))) return unknown();
      const requestedMembers = proofs ? new Set(proofs.map((proof) => proof.memberName)) : null;
      if (requestedMembers && [...requestedMembers].some((name) => !torrent.files.some((file) => file.name === name))) return unknown();
      // Audit discovers exact members once; execution never rehashes the whole pack per object.
      for (const file of torrent.files.filter((file) => !requestedMembers || requestedMembers.has(file.name))) {
        const found = await inspectRetentionFile(root, { relativePath: `${location.relativePath}/${file.name}` });
        if (!found || found.stat.size !== BigInt(file.length) || input.objects.some((object) => object.device === String(found.stat.dev) && object.inode === String(found.stat.ino))) return unknown();
        if (!input.objects.some((object) => object.sizeBytes === file.length)) continue;
        const hash = await tvRetentionDigest(found.path, input.budget);
        const after = await inspectRetentionFile(root, { relativePath: `${location.relativePath}/${file.name}` });
        if (!after || digestIdentity(after.stat) !== digestIdentity(found.stat)) return unknown();
        for (const id of download.fileIds) if (!matchesForBinding.has(id) && hashes.get(id) === hash && (!proofs || proofs.some((proof) => proof.fileId === id && proof.memberName === file.name && proof.sha256 === hash))) {
          matchesForBinding.add(id);
          verified.push({ fileId: id, bindingId: download.id, memberName: file.name, sha256: hash });
        }
      }
      if (download.fileIds.some((id) => !matchesForBinding.has(id))) return unknown();
      obligations[index]!.remainingSeedableCopies = 1;
      for (const id of matchesForBinding) covered.add(id);
    }
    if (input.objects.some((object) => !covered.has(object.id!))) return unknown();
    input.onVerifiedMembers?.(verified);
    return { state: "verified-independent-seed-store", obligations };
  } catch { return unknown(); }
  finally { if (timer !== undefined) clearTimeout(timer); }
}
