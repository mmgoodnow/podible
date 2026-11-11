import { constants, lstatSync, readFileSync, realpathSync, renameSync, unlinkSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { assertMovieStorage } from "../movies/storage";
import { reconcileMovieView } from "../movies/publication";
import { createPlexWatchHistoryReader } from "./plex-watch-history";
import { RetentionService } from "./retention-service";
import { inspectRetentionFile, matchesRetentionFile, observeRetentionDisk, retentionDigest } from "./retention-filesystem";
import { previewRetention, prepareMovieEviction, retentionAdapters, retentionConfiguration, type RetentionAdapters } from "./retention-policy";
import { RETENTION_MAX_TARGETS, type RetentionDisk, type RetentionEviction } from "./retention-policy-types";

const freshWatch = createPlexWatchHistoryReader({ cacheTtlMs: 0 });

async function syncDirectory(directory: string) {
  const handle = await fs.open(directory, constants.O_RDONLY | constants.O_NOFOLLOW);
  try { await handle.sync(); } finally { await handle.close(); }
}

async function checkStagingDirectory(directory: string, contentId: string) {
  const stat = await fs.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || await fs.realpath(directory) !== directory) throw new Error("Retention staging is not canonical");
  const marker = path.join(directory, ".owner.json");
  const owned = await fs.lstat(marker);
  if (!owned.isFile() || owned.isSymbolicLink() || owned.size > 1024 || JSON.parse(await fs.readFile(marker, "utf8")).contentId !== contentId)
    throw new Error("Retention staging ownership is unknown");
}

async function stagingDirectory(root: string, contentId: string, adapters: RetentionAdapters) {
  const directory = path.join(root, ".podible-retention");
  const exists = async () => {
    try { await fs.lstat(directory); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
    await checkStagingDirectory(directory, contentId);
    return true;
  };
  if (await exists()) return directory;
  // Publish only a fully initialized directory. A crash can leave an unused private
  // initializer, never an unmarked staging directory that blocks every retry.
  const temporary = await fs.mkdtemp(path.join(root, ".podible-retention-init-"));
  await adapters.afterCheckpoint?.("staging-created");
  const marker = await fs.open(path.join(temporary, ".owner.json"), "wx", 0o600);
  try { await marker.writeFile(JSON.stringify({ contentId })); await marker.sync(); } finally { await marker.close(); }
  await syncDirectory(temporary);
  await adapters.afterCheckpoint?.("staging-initialized");
  if (await exists()) return directory;
  await fs.rename(temporary, directory);
  await syncDirectory(root);
  await checkStagingDirectory(directory, contentId);
  return directory;
}

type Proofs = Map<string, string>;
const stagedPath = (eviction: RetentionEviction, file: RetentionEviction["plan"]["files"][number]) =>
  `.podible-retention/${eviction.id}-${file.id}${path.extname(file.relativePath)}`;

async function verifyMount(ctx: WorkerContext, eviction: RetentionEviction, root: string) {
  ctx.execution!.assertActive();
  await assertMovieStorage(ctx.repo, root);
  if (JSON.parse(await fs.readFile(path.join(root, ".podible-content.json"), "utf8")).id !== eviction.plan.contentId ||
      String((await fs.lstat(root, { bigint: true })).dev) !== eviction.plan.device) throw new Error("Retention mount changed");
}

async function restoreSupersededDemand(ctx: WorkerContext, eviction: RetentionEviction, adapters: RetentionAdapters, root: string, proofs: Proofs) {
  const key = { namespace: "movie-target", id: String(eviction.plan.targetId) }, demand = ctx.repo.retention.get(key);
  if (demand?.desiredPresence !== "present" || demand.revision <= eviction.plan.intentRevision) return null;
  const active = () => {
    ctx.execution!.assertActive();
    const current = ctx.repo.retention.get(key);
    if (current?.desiredPresence !== "present" || current.revision !== demand.revision) throw new Error("Recovery demand changed");
  };
  await verifyMount(ctx, eviction, root);
  const verifiedPresentObjectIds: string[] = [], verifiedAbsentObjectIds: string[] = [];
  for (const file of eviction.plan.files) {
    active();
    let canonical = await inspectRetentionFile(root, file);
    const staged = await inspectRetentionFile(root, { relativePath: stagedPath(eviction, file) });
    if (staged) {
      await checkStagingDirectory(path.dirname(staged.path), eviction.plan.contentId);
      const ownRecoveryLink = canonical && canonical.stat.dev === staged.stat.dev && canonical.stat.ino === staged.stat.ino &&
        canonical.stat.nlink === 2n && staged.stat.nlink === 2n;
      if (canonical && !ownRecoveryLink) throw new Error("Recovery refuses to overwrite a canonical object");
      const normalized = ownRecoveryLink ? { ...staged.stat, nlink: 1n } : staged.stat;
      if (!matchesRetentionFile(normalized, file, true) || await retentionDigest(staged.path) !== proofs.get(file.id))
        throw new Error("Staged recovery bytes differ from the original durable proof");
      active(); await verifyMount(ctx, eviction, root);
      if (!canonical) {
        // link is exclusive: never replace a newer canonical path. A crash with
        // both names is recoverable only when they are these exact two inode links.
        await fs.link(staged.path, path.join(root, file.relativePath));
        await syncDirectory(path.dirname(path.join(root, file.relativePath)));
        await adapters.afterCheckpoint?.("file-restored-linked");
      }
      canonical = await inspectRetentionFile(root, file);
      const latest = await inspectRetentionFile(root, { relativePath: stagedPath(eviction, file) });
      if (!canonical || !latest || canonical.stat.dev !== latest.stat.dev || canonical.stat.ino !== latest.stat.ino ||
          canonical.stat.nlink !== 2n || latest.stat.nlink !== 2n ||
          !matchesRetentionFile({ ...latest.stat, nlink: 1n }, file, true) || await retentionDigest(latest.path) !== proofs.get(file.id))
        throw new Error("Recovery object changed before staging retirement");
      active(); await fs.unlink(latest.path); await syncDirectory(path.dirname(latest.path));
      canonical = await inspectRetentionFile(root, file);
    }
    if (!canonical) verifiedAbsentObjectIds.push(file.id);
    else {
      if (!matchesRetentionFile(canonical.stat, file, true) || await retentionDigest(canonical.path) !== proofs.get(file.id))
        throw new Error("Canonical recovery identity or bytes changed");
      verifiedPresentObjectIds.push(file.id);
    }
  }
  active(); await verifyMount(ctx, eviction, root);
  const result = ctx.repo.retention.supersedeEviction(eviction.id, { verifiedPresentObjectIds, verifiedAbsentObjectIds });
  await adapters.afterCheckpoint?.("demand-superseded");
  await reconcileMovieView(ctx.repo.movies, { contentRoot: root, viewRoot: path.join(path.dirname(root), "views", "plex", "movies"),
    serverViewRoot: retentionConfiguration(ctx.repo).movies.plexViewPath }, () => ctx.execution!.assertActive());
  return result;
}

async function executeEvictionEffects(ctx: WorkerContext, input: RetentionEviction, adapters: RetentionAdapters, before: RetentionDisk, proofs: Proofs) {
  const raw = ctx.repo.retention.eviction(input.id);
  if (!raw) throw new Error("Unknown eviction plan");
  if (!ctx.execution) throw new Error("Retention deletion requires durable execution");
  const { plan } = raw, repo = ctx.repo, configuration = retentionConfiguration(repo);
  const root = path.join(configuration.movies.managedRoot, "objects");
  const roots = { contentRoot: root, viewRoot: path.join(configuration.movies.managedRoot, "views", "plex", "movies"), serverViewRoot: configuration.movies.plexViewPath };
  if (raw.finishedAt) {
    await verifyMount(ctx, raw, root);
    await reconcileMovieView(repo.movies, roots, () => ctx.execution!.assertActive());
    return raw;
  }
  const active = () => { ctx.execution!.assertActive(); repo.retention.assertEviction(raw.id); };
  const mount = async () => {
    active(); await verifyMount(ctx, raw, root);
  };
  const fenced = (effect: () => void) => repo.retention.withEvictionFence(raw.id, () => {
    ctx.execution!.assertActive();
    const marker = path.join(root, ".podible-content.json"), stat = lstatSync(root, { bigint: true });
    if (!stat.isDirectory() || realpathSync(root) !== root || String(stat.dev) !== plan.device ||
        realpathSync(marker) !== marker || JSON.parse(readFileSync(marker, "utf8")).id !== plan.contentId)
      throw new Error("Retention mount changed before filesystem effect");
    effect();
  });
  const actionAdapters = retentionAdapters({ ...adapters, watchHistory: adapters.watchHistory ?? freshWatch });
  const service = new RetentionService(repo, actionAdapters);
  const target = repo.movies.target(plan.targetId), movie = target && repo.movies.movie(target.movie_id);
  if (!movie) throw new Error("Movie identity is unavailable");
  const revalidate = async () => {
    await mount();
    const detail = await service.audit({ namespace: "movie-target", id: String(plan.targetId) }, { wholeResource: true });
    if (detail.activeAcquisitions !== 0 || detail.files.some((file) => file.presence === "unknown" ||
        (file.presence === "present" && file.references !== "verified-unshared"))) throw new Error("Retention file or acquisition evidence changed");
    if (detail.watch.state !== "watched" || detail.watch.coverage !== "global-all-users" || detail.watch.activePlayback !== "none" ||
        !detail.watch.playCount || !detail.watch.lastViewedAt) throw new Error("Retention watch or playback evidence is no longer eligible");
    const seeding = await service.seedEvidence(plan.targetId, configuration.movies.managedRoot, plan.files.map((file) => ({
      canonicalPath: path.join(root, file.relativePath), device: file.device, inode: file.inode, sizeBytes: file.sizeBytes,
      allocatedBytes: file.allocatedBytes, linkCount: 1, ctimeNs: file.ctimeNs, mtimeNs: file.mtimeNs,
      sha256: proofs.get(file.id)!,
    })));
    if (seeding.state !== "verified-independent-seed-store") throw new Error("Independent seed-store evidence is no longer verified");
    // Seed hashing can take minutes. Playback must be observed after that work,
    // not inherited from the earlier audit's history observation.
    const watch = await actionAdapters.watchHistory!({ tmdbId: movie.tmdbId, imdbId: movie.imdbId,
      plexUrl: configuration.movies.plexUrl, plexToken: configuration.movies.plexToken, plexSectionId: configuration.movies.plexSectionId });
    if (watch.state !== "watched" || watch.coverage !== "global-all-users" || watch.activePlayback !== "none" ||
        !Number.isSafeInteger(watch.playCount) || (watch.playCount ?? 0) < 1 || !watch.lastViewedAt || !Number.isFinite(Date.parse(watch.lastViewedAt)))
      throw new Error("Fresh post-hash watch or playback evidence is no longer eligible");
    active();
  };
  await mount();
  await reconcileMovieView(repo.movies, roots, active);
  await adapters.afterCheckpoint?.("view-retired");
  const staging = await stagingDirectory(root, plan.contentId, adapters);
  for (const file of plan.files) {
    const stagedRelative = stagedPath(raw, file);
    const source = await inspectRetentionFile(root, file), staged = await inspectRetentionFile(root, { relativePath: stagedRelative });
    if (!source && !staged) continue;
    if (source && staged) throw new Error("Both canonical and staged retention objects exist");
    if (source && !matchesRetentionFile(source.stat, file)) throw new Error("Canonical retention identity changed");
    if (staged && !matchesRetentionFile(staged.stat, file, true)) throw new Error("Staged retention identity changed");
    await revalidate();
    if (source) {
      active();
      const latest = await inspectRetentionFile(root, file);
      if (!latest || !matchesRetentionFile(latest.stat, file)) throw new Error("Canonical retention identity changed before staging");
      const destination = path.join(staging, path.basename(stagedRelative));
      fenced(() => {
        const current = lstatSync(latest.path, { bigint: true });
        if (!current.isFile() || realpathSync(latest.path) !== latest.path || !matchesRetentionFile(current, file))
          throw new Error("Canonical retention identity changed at staging fence");
        try { lstatSync(destination); throw new Error("Staging destination already exists"); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
        if (realpathSync(staging) !== staging) throw new Error("Retention staging changed");
        renameSync(latest.path, destination);
      });
      await syncDirectory(path.dirname(latest.path)); await syncDirectory(staging);
      await adapters.afterCheckpoint?.("file-staged");
    }
    const owned = await inspectRetentionFile(root, { relativePath: stagedRelative });
    if (!owned || !matchesRetentionFile(owned.stat, file, true) || await retentionDigest(owned.path) !== proofs.get(file.id))
      throw new Error("Staged retention bytes differ from the original durable proof");
    const reclaimable = await actionAdapters.reclaimableAllocation!({ canonicalPath: owned.path, device: String(owned.stat.dev), inode: String(owned.stat.ino),
      sizeBytes: Number(owned.stat.size), allocatedBytes: Number(owned.stat.blocks * 512n), linkCount: Number(owned.stat.nlink),
      ctimeNs: String(owned.stat.ctimeNs), mtimeNs: String(owned.stat.mtimeNs) });
    if (reclaimable === null || !Number.isSafeInteger(reclaimable) || reclaimable < file.reclaimableBytes || reclaimable > file.allocatedBytes)
      throw new Error("Filesystem reclaimability changed before deletion");
    await revalidate();
    const latest = await inspectRetentionFile(root, { relativePath: stagedRelative });
    if (!latest || !matchesRetentionFile(latest.stat, file, true) || latest.stat.ctimeNs !== owned.stat.ctimeNs)
      throw new Error("Staged retention object changed before unlink");
    fenced(() => {
      const current = lstatSync(latest.path, { bigint: true });
      if (!current.isFile() || realpathSync(latest.path) !== latest.path || !matchesRetentionFile(current, file, true) ||
          current.ctimeNs !== latest.stat.ctimeNs) throw new Error("Staged retention identity changed at unlink fence");
      unlinkSync(latest.path);
    });
    await syncDirectory(staging);
    await adapters.afterCheckpoint?.("file-unlinked");
  }
  await mount();
  for (const file of plan.files) if (await inspectRetentionFile(root, file) ||
    await inspectRetentionFile(root, { relativePath: `.podible-retention/${plan.id}-${file.id}${path.extname(file.relativePath)}` }))
    throw new Error("Eviction is not verified absent");
  await reconcileMovieView(repo.movies, roots, active);
  const after = await (adapters.observeDisk ?? observeRetentionDisk)(configuration.movies.managedRoot);
  active();
  return repo.retention.finishEviction(plan.id, { verifiedAbsentObjectIds: plan.files.map((file) => file.id), diskBefore: before, diskAfter: after,
    observedAvailableDeltaBytes: before.availableBytes !== null && after.availableBytes !== null ? after.availableBytes - before.availableBytes : null,
    attributedFreedBytes: null, reason: "Measured filesystem availability; concurrent volume activity prevents attributing the delta to this eviction alone." });
}

async function executeEviction(ctx: WorkerContext, eviction: RetentionEviction, adapters: RetentionAdapters) {
  if (!ctx.execution) throw new Error("Retention deletion requires durable execution");
  const configuration = retentionConfiguration(ctx.repo), root = path.join(configuration.movies.managedRoot, "objects");
  const proofIntent = () => {
    ctx.execution!.assertActive();
    const current = ctx.repo.retention.get({ namespace: "movie-target", id: String(eviction.plan.targetId) });
    // New demand may arrive before proof capture, while every canonical name is
    // still present. Read-only proofs are needed to safely restore that selection.
    if (current?.desiredPresence === "present" && current.revision > eviction.plan.intentRevision) return;
    ctx.repo.retention.assertEviction(eviction.id);
  };
  // Reflow does not allow nested step.run calls. Capture every original byte proof
  // before entering the single, globally locked destructive/recovery step.
  const before = await durableStep(ctx, `retention:${eviction.id}:disk-before`, async () => {
    proofIntent(); await verifyMount(ctx, eviction, root);
    return (adapters.observeDisk ?? observeRetentionDisk)(configuration.movies.managedRoot);
  });
  const proofs: Proofs = new Map();
  for (const file of eviction.plan.files) {
    const proof = await durableStep(ctx, `retention:${eviction.id}:proof:${file.id}`, async () => {
      proofIntent(); await verifyMount(ctx, eviction, root);
      const source = await inspectRetentionFile(root, file);
      if (!source || !matchesRetentionFile(source.stat, file)) throw new Error("Canonical retention identity changed before proof");
      const sha256 = await retentionDigest(source.path), after = await inspectRetentionFile(root, file);
      if (!after || !matchesRetentionFile(after.stat, file)) throw new Error("Canonical retention identity changed during proof");
      return { sha256 };
    });
    proofs.set(file.id, z.string().regex(/^[a-f0-9]{64}$/).parse(proof.sha256));
  }
  return durableStep(ctx, `retention:${eviction.id}:execute`, async () => {
    try { return await executeEvictionEffects(ctx, eviction, adapters, before, proofs); }
    catch (error) {
      const restored = await restoreSupersededDemand(ctx, eviction, adapters, root, proofs);
      if (restored) return restored;
      throw error;
    }
  });
}

export async function runRetentionCleanup(ctx: WorkerContext, job: JobRow, adapters: RetentionAdapters = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("Retention cleanup requires durable execution");
  const payload = z.object({ evictionId: z.string().uuid().optional() }).strict().parse(JSON.parse(job.payload_json ?? "{}"));
  if (payload.evictionId) {
    const eviction = ctx.repo.retention.eviction(payload.evictionId);
    if (!eviction || eviction.jobId !== job.id) throw new Error("Unknown retention execution intent");
    await executeEviction(ctx, eviction, adapters); return "done";
  }
  const existing = ctx.repo.retention.evictions(job.id);
  for (const eviction of existing) await executeEviction(ctx, eviction, adapters);
  for (let iteration = existing.length; iteration < RETENTION_MAX_TARGETS; iteration++) {
    ctx.execution?.assertActive();
    const cursor = ctx.repo.getJsonState<number>("retention-pressure-cursor") ?? 0;
    const preview = await previewRetention(ctx.repo, adapters, { afterTargetId: cursor });
    if (preview.pressure !== true) return "done";
    if (preview.nextAfterTargetId !== null) ctx.repo.setJsonState("retention-pressure-cursor", preview.nextAfterTargetId);
    const candidate = preview.candidates.find((candidate) => candidate.eligible);
    if (!candidate || candidate.expectedRevision === null) return "done";
    const eviction = await durableStep(ctx, `retention:plan:${iteration}`, async () => {
      const plan = await prepareMovieEviction(ctx.repo, { targetId: candidate.targetId, expectedRevision: candidate.expectedRevision!,
        reason: "Automatic disk pressure: oldest low-watch eligible owned movie", mode: "pressure" },
      { ...adapters, watchHistory: adapters.watchHistory ?? freshWatch });
      ctx.execution?.assertActive();
      return ctx.repo.retention.queueEviction(plan, () => job.id);
    });
    await adapters.afterCheckpoint?.("intent-committed");
    await executeEviction(ctx, eviction, adapters);
  }
  return "done";
}
