import { constants, closeSync, fsyncSync, fstatSync, linkSync, lstatSync, mkdirSync, openSync,
  readFileSync, readdirSync, realpathSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import type { BigIntStats } from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";
import bencode from "bencode";
import { z } from "zod";
import { inspectMetainfo } from "./metainfo";

const intentSchema = z.object({ version: z.literal(1), infoHash: z.string().regex(/^[a-f0-9]{40}$/),
  destination: z.string().min(1).max(4096), fileCount: z.number().int().positive().max(100_000),
  ownershipTag: z.string().trim().min(1).max(256) }).strict();
export type SeedOnlyIntent = z.infer<typeof intentSchema>;
export const seedOnlyIntentDirectory = (engineDirectory: string) =>
  path.join(path.dirname(engineDirectory), "transmission-seed-only");
const readFlags = constants.O_RDONLY | constants.O_NOFOLLOW;
const missing = (error: unknown) => (error as NodeJS.ErrnoException).code === "ENOENT";
function exists(value: string) {
  try { lstatSync(value); return true; } catch (error) { if (missing(error)) return false; throw error; }
}

function directory(value: string) {
  if (!path.isAbsolute(value) || path.resolve(value) !== value || value === "/" ||
    realpathSync(value) !== value || !lstatSync(value).isDirectory())
    throw new Error("Seed-only state requires a real bounded directory");
}
function sameFile(a: BigIntStats, b: BigIntStats) {
  return a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
}
function readRegular(file: string, limit: number): { bytes: Buffer; stat: BigIntStats } | null {
  let fd: number;
  try { fd = openSync(file, readFlags); }
  catch (error) { if (missing(error)) return null; throw error; }
  try {
    const stat = fstatSync(fd, { bigint: true });
    if (!stat.isFile() || stat.size > BigInt(limit) || stat.nlink !== 1n)
      throw new Error("Seed-only state path is not a bounded owned regular file");
    const bytes = readFileSync(fd);
    if (bytes.length !== Number(stat.size) || !sameFile(stat, fstatSync(fd, { bigint: true })) ||
      !sameFile(stat, lstatSync(file, { bigint: true }))) throw new Error("Seed-only state changed while reading");
    return { bytes, stat };
  } finally { closeSync(fd); }
}
function syncDirectory(value: string) {
  const fd = openSync(value, readFlags | constants.O_DIRECTORY);
  try { fsyncSync(fd); } finally { closeSync(fd); }
}
function parseIntent(value: unknown, fileHash?: string) {
  const intent = intentSchema.parse(value);
  if (!path.isAbsolute(intent.destination) || path.resolve(intent.destination) !== intent.destination || intent.destination === "/")
    throw new Error("Seed-only destination must be a bounded absolute path");
  if (fileHash && intent.infoHash !== fileHash) throw new Error("Seed-only intent filename/hash mismatch");
  return intent;
}

export function listSeedOnlyIntents(intentDirectory: string): SeedOnlyIntent[] {
  if (!exists(intentDirectory)) return [];
  directory(intentDirectory);
  return readdirSync(intentDirectory).sort().flatMap(name => {
    if (!/^[a-f0-9]{40}\.json$/.test(name)) return [];
    const file = readRegular(path.join(intentDirectory, name), 16 * 1024);
    if (!file) throw new Error("Seed-only intent disappeared");
    return [parseIntent(JSON.parse(file.bytes.toString("utf8")), name.slice(0, 40))];
  });
}

export function readSeedOnlyIntent(intentDirectory: string, infoHash: string): SeedOnlyIntent | null {
  intentSchema.shape.infoHash.parse(infoHash);
  if (!path.isAbsolute(intentDirectory) || path.resolve(intentDirectory) !== intentDirectory || intentDirectory === "/")
    throw new Error("Seed-only intent directory must be a bounded absolute path");
  if (!exists(intentDirectory)) return null;
  directory(intentDirectory);
  const file = readRegular(path.join(intentDirectory, infoHash + ".json"), 16 * 1024);
  return file ? parseIntent(JSON.parse(file.bytes.toString("utf8")), infoHash) : null;
}

/** Persist before torrent-add. The per-hash command cannot later change destination or ownership. */
export async function persistSeedOnlyIntent(intentDirectory: string, input: Omit<SeedOnlyIntent, "version">): Promise<SeedOnlyIntent> {
  const intent = parseIntent({ version: 1, ...input });
  directory(intent.destination);
  directory(path.dirname(intentDirectory));
  try { mkdirSync(intentDirectory, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  directory(intentDirectory);
  const target = path.join(intentDirectory, intent.infoHash + ".json");
  const temporary = path.join(intentDirectory, `.${intent.infoHash}-${randomUUID()}.tmp`);
  const fd = openSync(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { writeFileSync(fd, JSON.stringify(intent)); fsyncSync(fd); }
  finally { closeSync(fd); }
  try {
    try { linkSync(temporary, target); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const existing = readRegular(target, 16 * 1024);
      if (!existing || JSON.stringify(parseIntent(JSON.parse(existing.bytes.toString("utf8")), intent.infoHash)) !== JSON.stringify(intent))
        throw new Error("Seed-only intent is immutable for this hash");
    }
  } finally { unlinkSync(temporary); }
  syncDirectory(intentDirectory);
  syncDirectory(path.dirname(intentDirectory));
  return intent;
}

function resumeMap(bytes: Buffer | undefined): Record<string, unknown> {
  if (!bytes) return {};
  try {
    const value = bencode.decode(bytes);
    const consumed = (bencode.decode as typeof bencode.decode & { position: number }).position;
    if (consumed === bytes.length && value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array))
      return value as Record<string, unknown>;
  } catch { /* Damaged daemon state is rebuilt solely from independently persisted intent. */ }
  return {};
}
function labels(value: unknown) {
  if (!Array.isArray(value)) return [];
  return value.flatMap(item => {
    if (!(item instanceof Uint8Array)) return [];
    try {
      const label = new TextDecoder("utf-8", { fatal: true }).decode(item);
      return label && label.length <= 256 ? [label] : [];
    } catch { return []; }
  });
}

/** Supervisor-only, while the daemon is stopped, before EVERY spawn. Never starts or proves a torrent complete. */
export function prepareSeedOnlyResumes(engineDirectory: string, intentDirectory: string): SeedOnlyIntent[] {
  const intents = listSeedOnlyIntents(intentDirectory);
  if (!intents.length) return [];
  directory(engineDirectory);
  const torrentDirectory = path.join(engineDirectory, "torrents"), resumeDirectory = path.join(engineDirectory, "resume");
  if (exists(torrentDirectory)) directory(torrentDirectory);
  if (exists(resumeDirectory)) directory(resumeDirectory);
  // Validate every affected identity before modifying any resume. Intent may precede a crashed torrent-add.
  const prepared = intents.flatMap(intent => {
    const metainfo = readRegular(path.join(torrentDirectory, intent.infoHash + ".torrent"), 8 * 1024 ** 2);
    if (!metainfo) return [];
    const metadata = inspectMetainfo(metainfo.bytes);
    if (metadata.infoHash !== intent.infoHash || metadata.files.length !== intent.fileCount)
      throw new Error("Seed-only metainfo hash or file count differs from intent");
    directory(intent.destination);
    const target = path.join(resumeDirectory, intent.infoHash + ".resume");
    const previous = readRegular(target, 32 * 1024 ** 2);
    const resume = resumeMap(previous?.bytes);
    const next = { ...resume, name: metadata.name, destination: intent.destination,
      paused: resume.paused === 0 || resume.paused === 1 ? resume.paused : 1,
      dnd: Array.from({ length: intent.fileCount }, () => 1),
      labels: [...new Set([...labels(resume.labels), intent.ownershipTag, "Podible Seed Only"])] };
    return [{ intent, target, previous, bytes: Buffer.from(bencode.encode(next)) }];
  });
  if (!prepared.length) return [];
  try { mkdirSync(resumeDirectory, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  directory(resumeDirectory);
  for (const item of prepared) {
    const temporary = path.join(resumeDirectory, `.${item.intent.infoHash}-${randomUUID()}.tmp`);
    const fd = openSync(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    try { writeFileSync(fd, item.bytes); fsyncSync(fd); } finally { closeSync(fd); }
    try {
      const current = readRegular(item.target, 32 * 1024 ** 2);
      if (item.previous ? !current || !sameFile(item.previous.stat, current.stat) : current !== null)
        throw new Error("Seed-only resume changed during startup preparation");
      renameSync(temporary, item.target);
      syncDirectory(resumeDirectory);
    } finally { try { unlinkSync(temporary); } catch (error) { if (!missing(error)) throw error; } }
  }
  syncDirectory(engineDirectory);
  return prepared.map(item => item.intent);
}
