import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import type { BooksRepo } from "../repo";
import type { TorrentClient, TorrentSnapshot } from "../media/torrent-client";
import { inspectMetainfo } from "../media/metainfo";
import { resolveDownloadFile } from "../media/canonical-download";

const relative = z.string().min(1).refine((value) => !path.isAbsolute(value) && !/[\\\x00-\x1f\x7f]/.test(value) &&
  value.split("/").every((part) => part && part !== "." && part !== ".."));
const count = z.number().int().nonnegative().safe();
const identity = z.object({ mtimeMs: z.number(), ctimeMs: z.number(), device: count, inode: count });
export const tvSourceSchema = z.object({
  ref: z.object({ clientId: z.string().min(1), nativeId: z.string().regex(/^[a-f0-9]{40}$/) }),
  location: z.object({ rootId: z.literal("tv"), relativePath: relative }),
  offerKey: z.string().min(1), title: z.string(), provider: z.string(),
  manifest: z.object({ infoHash: z.string().regex(/^[a-f0-9]{40}$/), name: relative, size: count,
    files: z.array(z.object({ path: relative, size: count, identity: identity.optional() })).min(1).max(300) }),
}).refine((source) => source.ref.nativeId === source.manifest.infoHash &&
  source.manifest.files.reduce((sum, file) => sum + file.size, 0) === source.manifest.size &&
  new Set(source.manifest.files.map((file) => file.path.normalize("NFC").toLowerCase())).size === source.manifest.files.length);
export type TvSource = z.infer<typeof tvSourceSchema>;
const labels = z.object({ identityVerified: z.literal(true), releaseTitle: z.string(), provider: z.string() });

function bound(source: TvSource, downloads: ReturnType<BooksRepo["tv"]["downloads"]>) {
  return downloads.some((download) => {
    if (download.client_id !== source.ref.clientId || download.native_id !== source.ref.nativeId || download.offer_key !== source.offerKey) return false;
    try { const location = JSON.parse(download.location_json); return location.rootId === "tv" && location.relativePath === source.location.relativePath; }
    catch { return false; }
  });
}

type VersionSource = ReturnType<BooksRepo["tv"]["versionSources"]>[number];
async function legacyVersionSource(repo: BooksRepo, row: VersionSource, journal: SQLiteStorage) {
  const { acquisition, version, downloads } = row;
  if (acquisition.job_id === null) return null;
  const link = repo.getJobExecution(acquisition.job_id);
  if (!link) return null;
  const run = await journal.getRun(link.run_id);
  if (run?.workflow !== "media-tv_acquire-v1" || !z.object({ jobId: z.literal(acquisition.job_id) }).safeParse(run.input).success) return null;
  const steps = (await journal.getStepResults(link.run_id)).filter((step) => step.status === "completed");
  const versions = steps.filter((step) => /^tv:attempt:\d+:version$/.test(step.name) && step.output === version.id);
  if (versions.length !== 1) return null;
  const prefix = versions[0]!.name.slice(0, -":version".length);
  const snatch = steps.find((step) => step.name === `${prefix}:snatch`);
  const intent = steps.find((step) => step.name === `${prefix}:download-intent`);
  const metainfo = steps.filter((step) => new RegExp(`^${prefix}:metainfo:request:\\d+$`).test(step.name))
    .map((step) => z.object({ ok: z.literal(true), value: z.string().max(12 * 1024 ** 2) }).safeParse(step.output))
    .filter((result) => result.success);
  if (!snatch || !intent || metainfo.length !== 1) return null;
  const manifest = inspectMetainfo(Buffer.from(metainfo[0]!.data!.value, "base64"));
  const ref = z.object({ clientId: z.string(), nativeId: z.string() }).parse(snatch.output);
  const binding = downloads.find((download) => download.client_id === ref.clientId && download.native_id === ref.nativeId);
  if (!binding) return null;
  const prior = labels.parse(version.facts);
  const intentOutput = intent.output as { location?: unknown } | null;
  const source = tvSourceSchema.parse({ ref, location: intentOutput?.location ?? intent.output, manifest, offerKey: binding.offer_key, title: prior.releaseTitle, provider: prior.provider });
  return bound(source, downloads) ? { source, prefix, version } : null;
}

export async function recordedLegacyTvSource(repo: BooksRepo, targetId: number, acquisitionId: number) {
  const row = repo.tv.versionSources(targetId).find((row) => row.acquisition.id === acquisitionId);
  if (!row || repo.executionDatabasePath() === ":memory:") return null;
  let journal: SQLiteStorage | undefined;
  try {
    journal = new SQLiteStorage(repo.executionDatabasePath(), { readOnly: true });
    const proof = await legacyVersionSource(repo, row, journal);
    const link = row.acquisition.job_id === null ? null : repo.getJobExecution(row.acquisition.job_id);
    if (!proof || !link) return null;
    const plan = z.object({ plan: z.object({ files: z.array(z.object({
      fileIndex: count, episodeIds: z.array(z.number().int().positive()),
    })) }) }).parse(proof.version.facts);
    const steps = (await journal.getStepResults(link.run_id)).filter((step) => step.status === "completed");
    const adoptions = proof.version.files.map((file) => {
      const mappings = plan.plan.files.filter((mapping) => mapping.episodeIds.length === file.episodeIds.length &&
        mapping.episodeIds.every((id) => file.episodeIds.includes(id)));
      if (mappings.length !== 1) throw new Error("Ambiguous legacy file mapping");
      const fileIndex = mappings[0]!.fileIndex;
      const receipts = steps.filter((step) => step.name === `${proof.prefix}:adopt:${fileIndex}`);
      if (receipts.length !== 1) throw new Error("Missing legacy adoption proof");
      const receipt = z.object({ id: z.string(), relativePath: relative, sizeBytes: count,
        sha256: z.string().regex(/^[a-f0-9]{64}$/) }).parse(receipts[0]!.output);
      if (receipt.id !== file.id || receipt.relativePath !== file.relativePath || receipt.sizeBytes !== file.sizeBytes)
        throw new Error("Legacy adoption proof belongs to another file");
      return { fileId: file.id, fileIndex, sha256: receipt.sha256 };
    });
    return { ...proof, adoptions };
  } catch { return null; }
  finally { await journal?.close(); }
}

/** Recover old provenance from one exact successful attempt; never infer it from sibling downloads. */
export async function reusableTvSources(repo: BooksRepo, targetId: number, acquisitionId: number, missingEpisodeIds: number[]) {
  const sources = new Map<string, { source: TvSource; accounted: Map<number, Set<number>> }>();
  let journal: SQLiteStorage | undefined;
  try {
    for (const row of repo.tv.versionSources(targetId)) {
      const { version, acquisition, downloads } = row;
      if (acquisition.id === acquisitionId || !acquisition.finished_at) continue;
      const facts = z.object({ source: tvSourceSchema }).safeParse(version.facts);
      let source = facts.success ? facts.data.source : null;
      if (!source && labels.safeParse(version.facts).success && acquisition.job_id !== null) {
        try {
          if (repo.executionDatabasePath() === ":memory:") continue;
          journal ??= new SQLiteStorage(repo.executionDatabasePath(), { readOnly: true });
          source = (await legacyVersionSource(repo, row, journal))?.source ?? null;
        } catch { continue; }
      }
      if (source && bound(source, downloads)) {
        const key = `${source.ref.clientId}:${source.ref.nativeId}`;
        const saved = sources.get(key) ?? { source, accounted: new Map<number, Set<number>>() };
        const plan = z.object({ plan: z.object({ files: z.array(z.object({ fileIndex: count, episodeIds: z.array(z.number().int().positive()) })) }) }).safeParse(version.facts);
        if (plan.success) for (const mapping of plan.data.plan.files) {
          if (!version.files.some((file) => file.episodeIds.length === mapping.episodeIds.length && file.episodeIds.every((id) => mapping.episodeIds.includes(id)))) continue;
          const assigned = saved.accounted.get(mapping.fileIndex) ?? new Set<number>();
          for (const id of mapping.episodeIds) assigned.add(id);
          saved.accounted.set(mapping.fileIndex, assigned);
        }
        sources.set(key, saved);
      }
    }
  } finally { await journal?.close(); }
  const useful: { source: TvSource; skipFileIndexes: number[] }[] = [];
  let consumed = 0;
  for (const { source, accounted } of sources.values()) {
    const skipFileIndexes = [...accounted].filter(([, ids]) => !missingEpisodeIds.some((id) => ids.has(id))).map(([index]) => index);
    if (source.manifest.files.every((file, index) => !/\.(mkv|mp4|m4v|avi|mov|ts)$/i.test(file.path) || skipFileIndexes.includes(index))) consumed++;
    else useful.push({ source, skipFileIndexes });
  }
  return { sources: useful, consumed };
}

export function completeTvSource(source: TvSource, observation: TorrentSnapshot) {
  return observation.ref.clientId === source.ref.clientId && observation.ref.nativeId === source.ref.nativeId &&
    observation.location.rootId === source.location.rootId && observation.location.relativePath === source.location.relativePath &&
    observation.complete && observation.state !== "checking" && observation.message === null &&
    observation.bytesDone === source.manifest.size && observation.sizeBytes === source.manifest.size;
}

/** Capture all metainfo members, including unadopted pack files, before trusting saved probe receipts. */
export async function tvSourceFiles(source: TvSource, client: TorrentClient, managedRoot: string) {
  if (source.ref.clientId !== client.id) throw new Error("TV source belongs to another client");
  const root = path.resolve(managedRoot, "downloads"), sourceRoot = path.resolve(root, source.location.relativePath);
  if (!sourceRoot.startsWith(root + path.sep) || await fs.realpath(root) !== root || await fs.realpath(sourceRoot) !== sourceRoot)
    throw new Error("TV source layout is not canonical");
  const files = await client.files(source.ref);
  if (files.length !== source.manifest.files.length || new Set(files.map((file) => file.index)).size !== files.length)
    throw new Error("TV client file manifest changed");
  const manifest = structuredClone(source.manifest);
  for (const file of files) {
    const expected = source.manifest.files[file.index];
    if (!Number.isInteger(file.index) || !expected || path.resolve(file.path) !== path.resolve(sourceRoot, expected.path) ||
      !path.resolve(file.path).startsWith(sourceRoot + path.sep))
      throw new Error("TV client file escaped metainfo layout");
    const resolved = await resolveDownloadFile(path.join(managedRoot, "objects"), sourceRoot, file.path, expected.size);
    const stat = await fs.lstat(resolved.path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== expected.size) throw new Error("Completed TV file size differs from metainfo");
    const current = { mtimeMs: stat.mtimeMs, ctimeMs: stat.ctimeMs, device: stat.dev, inode: stat.ino };
    if (expected.identity && Object.keys(current).some((key) => current[key as keyof typeof current] !== expected.identity![key as keyof typeof current]))
      throw new Error("Previously verified TV source file changed");
    manifest.files[file.index]!.identity = current;
  }
  return { files, sourceRoot, source: tvSourceSchema.parse({ ...source, manifest }) };
}
