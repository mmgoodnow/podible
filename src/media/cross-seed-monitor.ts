import path from "node:path";
import { constants, promises as fs } from "node:fs";
import { z } from "zod";
import { durableStep, workerLog, type WorkerContext } from "../worker/context";
import { managedBookTransmission, managedTransmission, managedTvTransmission } from "./managed-transmission";
import { TorrentNotFoundError, type TorrentClient } from "./torrent-client";
import { isBookCrossSeedScope, type CrossSeedScope, type OptimisticRegistration } from "./cross-seed-state";
import { captureReuseFile, captureReuseRoot, publishedReuseLayoutCapabilities } from "./reuse-layout";
import { reuseDigest } from "./reuse-plan";
import { withTorrentRegistration } from "./torrent-use";

const cursorKey = "cross-seed:optimistic-monitor-cursor";
export type CrossSeedMonitorAdapters = { client?: (scope: CrossSeedScope) => TorrentClient | Promise<TorrentClient> };

const layoutReceiptSchema = z.object({
  version: z.literal(1), planId: z.string(), definitionDigest: z.string(),
  capabilities: z.object({ cow: z.enum(["clone-force", "unavailable"]),
    symlinkMode: z.enum(["never", "verified-read-only", "optimistic-read-only"]) }),
  generation: z.object({ device: z.string(), inode: z.string() }),
  files: z.array(z.object({ path: z.string(), kind: z.enum(["symlink", "clone", "missing"]),
    device: z.string().optional(), inode: z.string().optional() })).max(100_000),
});

async function readLayoutReceipt(generation: string) {
  const marker = path.join(generation, ".podible-reuse-layout.json");
  const handle = await fs.open(marker, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile() || before.nlink !== 1n || before.size > 32n * 1024n * 1024n)
      throw new Error("Invalid layout receipt file");
    const bytes = await handle.readFile();
    const after = await handle.stat({ bigint: true }), current = await fs.lstat(marker, { bigint: true });
    const identity = (stat: typeof before) => [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].map(String).join(":");
    if (BigInt(bytes.length) !== before.size || identity(before) !== identity(after) || identity(before) !== identity(current))
      throw new Error("Layout receipt changed");
    return layoutReceiptSchema.parse(JSON.parse(bytes.toString("utf8")));
  } finally { await handle.close(); }
}

/** Opens only ownership metadata and stats. Never hashes or reads media payloads. */
async function validateLayout(ctx: WorkerContext, registration: OptimisticRegistration) {
  const receipt = ctx.repo.crossSeed.receipt(registration.operationId)!;
  const settings = ctx.getSettings(), scope = receipt.command.scope;
  const managed = await fs.realpath(isBookCrossSeedScope(scope) ? settings.libraryRoot
    : scope.media === "tv" ? settings.tv.managedRoot : settings.movies.managedRoot);
  const downloads = isBookCrossSeedScope(scope) ? path.join(managed, ".podible", "downloads") : path.join(managed, "downloads");
  const expectedRoot = isBookCrossSeedScope(scope) ? "books" : scope.media === "tv" ? "tv" : "movies";
  if (registration.location.rootId !== expectedRoot || path.isAbsolute(registration.location.relativePath) ||
    registration.location.relativePath.split(/[\\/]/).some(part => !part || part === "." || part === ".."))
    throw new Error("Optimistic registry location is not bounded");
  const root = await captureReuseRoot({ rootPath: path.join(downloads, "reuse"),
    markerName: ".podible-reuse-root.json", ownerId: registration.ref.clientId });
  const capabilities = await publishedReuseLayoutCapabilities(root, registration.plan);
  if (!capabilities) throw new Error("Optimistic layout ownership changed");
  const data = await fs.realpath(path.join(root.path, registration.planId, "data"));
  if (data !== path.resolve(downloads, registration.location.relativePath) || !data.startsWith(root.path + path.sep))
    throw new Error("Optimistic layout differs from its registered location");
  const generation = path.dirname(data), generationStat = await fs.lstat(generation, { bigint: true });
  const layout = await readLayoutReceipt(generation);
  if (!generationStat.isDirectory() || layout.planId !== registration.planId ||
    layout.generation.device !== String(generationStat.dev) || layout.generation.inode !== String(generationStat.ino) ||
    reuseDigest(layout.capabilities) !== reuseDigest(capabilities) ||
    layout.definitionDigest !== reuseDigest({ id: registration.planId, root, capabilities }) ||
    layout.files.length !== registration.plan.files.length) throw new Error("Optimistic layout receipt changed");
  for (const [index, file] of registration.plan.files.entries()) {
    ctx.execution!.assertActive();
    if (!file.source || file.size !== file.source.size) throw new Error("Optimistic member is not fully mapped");
    const current = await captureReuseFile({ root: file.source.root, sourceId: file.source.id, sourcePath: file.source.path,
      sourceTorrentId: file.source.sourceTorrentId, sha256: file.source.identity.sha256, hash: false });
    if (reuseDigest(current) !== reuseDigest(file.source)) throw new Error("Optimistic source identity changed");
    const leaf = path.resolve(data, file.candidatePath);
    if (!leaf.startsWith(data + path.sep) || await fs.realpath(path.dirname(leaf)) !== path.dirname(leaf))
      throw new Error("Optimistic member left its captured source");
    const recorded = layout.files[index], output = await fs.lstat(leaf, { bigint: true });
    if (recorded.path !== file.candidatePath) throw new Error("Optimistic member receipt changed");
    if (recorded.kind === "symlink") {
      if (capabilities.symlinkMode === "never" || !output.isSymbolicLink() || await fs.readlink(leaf) !== file.source.path)
        throw new Error("Optimistic member left its captured source");
    } else if (recorded.kind !== "clone" || capabilities.cow !== "clone-force" || !output.isFile() ||
      output.size !== BigInt(file.size) || output.nlink !== 1n || String(output.dev) !== recorded.device ||
      String(output.ino) !== recorded.inode || registration.plan.sources.some(source =>
        source.identity.device === String(output.dev) && source.identity.inode === String(output.ino)))
      throw new Error("Optimistic clone identity changed or aliases source bytes");
  }
  return downloads;
}

/** Scheduled stewardship invokes this after completed acquisition jobs have left execution. */
export async function processOptimisticCrossSeeds(ctx: WorkerContext, adapters: CrossSeedMonitorAdapters = {}): Promise<void> {
  if (!ctx.execution) throw new Error("Optimistic monitoring requires durable execution");
  const cursor = ctx.repo.getJsonState<string>(cursorKey) ?? "";
  const batch = await durableStep(ctx, "optimistic-monitor:batch", () => ctx.repo.crossSeed.optimisticRegistrations(25, cursor));
  for (const registration of batch) {
    await durableStep(ctx, `optimistic-monitor:${registration.operationId}`, () => withTorrentRegistration(ctx.repo,
      { clientId: registration.ref.clientId, infoHash: registration.ref.nativeId }, async () => {
        const active = () => ctx.execution!.assertActive();
        active();
        const current = ctx.repo.crossSeed.optimisticRegistration(registration.operationId);
        const receipt = ctx.repo.crossSeed.receipt(registration.operationId);
        if (!current || !receipt || ["queued", "running"].includes(ctx.repo.getJobStatus(receipt.jobId) ?? "") ||
          !ctx.repo.crossSeed.ownsOptimisticRegistration(registration.operationId)) return;
        try {
          const downloads = await validateLayout(ctx, current), scope = receipt.command.scope, settings = ctx.getSettings();
          const client = await (adapters.client?.(scope) ?? (isBookCrossSeedScope(scope) ? managedBookTransmission(downloads)
            : scope.media === "tv" ? managedTvTransmission(settings.tv.managedRoot) : managedTransmission(settings.movies.managedRoot)));
          if (client.id !== current.ref.clientId || !client.capabilities.optimisticSeeding || !client.capabilities.readOnlySeeding)
            throw new Error("Optimistic engine identity or capability changed");
          const owned = () => { active(); if (!ctx.repo.crossSeed.ownsOptimisticRegistration(current.operationId))
            throw new Error("Optimistic registration ownership changed"); };
          const inspect = async () => {
            owned(); const observed = await client.inspect(current.ref);
            if (observed.ref.clientId !== current.ref.clientId || observed.ref.nativeId !== current.ref.nativeId ||
              reuseDigest(observed.location) !== reuseDigest(current.location) || observed.seedOnly !== true ||
              observed.sizeBytes !== current.plan.summary.totalBytes) throw new Error("Optimistic daemon layout or protection changed");
            return observed;
          };
          let observed;
          try { observed = await inspect(); }
          catch (error) {
            if (!(error instanceof TorrentNotFoundError) || current.status !== "removing" ||
              !ctx.repo.crossSeed.isIncompatible(current.ref.nativeId, current.planId)) throw error;
            await client.forgetSeedOnlyIntent?.(current.ref, current.location);
            owned(); ctx.repo.crossSeed.completeOptimisticRemoval(current.operationId); return;
          }
          if (observed.integrityError !== "piece-mismatch" && current.status !== "removing") return;
          owned();
          ctx.repo.crossSeed.markIncompatible(current.ref.nativeId, current.planId, "Candidate failed lazy piece verification; shared source files were not modified");
          owned(); await client.quiesce(current.ref);
          await validateLayout(ctx, current); await inspect(); owned();
          await client.removeRecord(current.ref);
          try { await inspect(); throw new Error("Optimistic record removal is not yet confirmed"); }
          catch (error) {
            if (!(error instanceof TorrentNotFoundError)) throw error;
            await client.forgetSeedOnlyIntent?.(current.ref, current.location);
            owned(); ctx.repo.crossSeed.completeOptimisticRemoval(current.operationId);
          }
        } catch {
          active(); workerLog(ctx, `[cross-seed-monitor] operation=${registration.operationId} retained for ownership/layout/removal reconciliation`);
        }
      }));
  }
  await durableStep(ctx, "optimistic-monitor:cursor", () => {
    ctx.execution!.assertActive(); ctx.repo.setJsonState(cursorKey, batch.length === 25 ? batch.at(-1)!.operationId : "");
  });
}
