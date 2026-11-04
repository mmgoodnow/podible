import { constants, promises as fs, lstatSync, readFileSync, realpathSync, renameSync, unlinkSync, readlinkSync as readFileLink, openSync, fsyncSync, closeSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import { z } from "zod";
import type { JobRow } from "../app-types";
import type { WorkerContext } from "../worker/context";
import { durableStep } from "../worker/context";
import { observeRecoveryMount, recoveryContained, recoveryDirectory, observeRecoveryView, repairRecoveryView } from "./recovery-filesystem";
import { inspectRetentionFile, matchesRetentionFile, ext4ReclaimableAllocation, observeRetentionDisk } from "./retention-filesystem";
import { TvRetentionService, type TvRetentionAdapters } from "./tv-retention";
import { tvRetentionDigest, observeTvRetentionSeeds } from "./tv-retention-seeds";
import { createTvRetentionPlexReader } from "./tv-retention-plex";
import type { TvRetentionPlan } from "./tv-retention-types";
import { TV_RETENTION_DEFAULT_HASH_BYTES, TV_RETENTION_DEFAULT_TIMEOUT_MS } from "./tv-retention-types";
import type { TvRetentionRepo } from "./tv-retention-repo";
import { tvRoots, tvViewEntries } from "../tv/publication";

const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const plex = createTvRetentionPlexReader();
const staged = (plan: TvRetentionPlan, file: TvRetentionPlan["files"][number]) => `.podible-tv-retention/${plan.id}-${file.id}${path.extname(file.relativePath)}`;

async function syncDirectory(directory: string) {
  const handle = await fs.open(directory, constants.O_RDONLY | constants.O_NOFOLLOW);
  try { await handle.sync(); } finally { await handle.close(); }
}
function assertStagingOwner(contentRoot: string, contentId: string) {
  try {
    const directory = path.join(contentRoot, ".podible-tv-retention"), marker = path.join(directory, ".owner.json");
    if (!recoveryDirectory(directory)) throw new Error();
    const stat = lstatSync(marker);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 || JSON.parse(readFileSync(marker, "utf8")).contentId !== contentId) throw new Error();
  } catch { throw new Error("TV retention staging ownership changed or is unverified"); }
}
async function ensureStaging(root: string, contentId: string, adapters: TvRetentionAdapters) {
  const directory = path.join(root, ".podible-tv-retention");
  const verify = async () => {
    if (!recoveryDirectory(directory)) return false;
    const marker = path.join(directory, ".owner.json"), stat = await fs.lstat(marker);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 || JSON.parse(await fs.readFile(marker, "utf8")).contentId !== contentId)
      throw new Error("TV retention staging ownership is unverified");
    return true;
  };
  if (await verify()) return directory;
  const temporary = await fs.mkdtemp(path.join(root, ".podible-tv-retention-init-"));
  await adapters.afterCheckpoint?.("staging-created");
  const marker = await fs.open(path.join(temporary, ".owner.json"), "wx", 0o600);
  try { await marker.writeFile(JSON.stringify({ contentId })); await marker.sync(); } finally { await marker.close(); }
  await syncDirectory(temporary);
  if (!(await verify())) { await fs.rename(temporary, directory); await syncDirectory(root); }
  await adapters.afterCheckpoint?.("staging-initialized");
  if (!(await verify())) throw new Error("TV staging initialization failed");
  return directory;
}

function retireView(plan: TvRetentionPlan, root: string, fence: (action: () => void) => void) {
  const viewRoot = path.join(root, "views", "plex", "tv"), manifestPath = path.join(viewRoot, ".podible-view.json");
  if (!recoveryDirectory(viewRoot)) return;
  const stat = lstatSync(manifestPath);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 ** 2) throw new Error("TV view manifest is unverified");
  const manifest = z.object({ contentId: z.string().uuid(), files: z.record(z.string(), z.array(z.string())) }).strict().parse(JSON.parse(readFileSync(manifestPath, "utf8")));
  if (manifest.contentId !== plan.contentId) throw new Error("TV view mount changed");
  try { lstatSync(path.join(viewRoot, ".podible-view-pending.json")); throw new Error("TV view publication is pending"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  for (const view of plan.views) {
    const destination = recoveryContained(viewRoot, view.relativePath), content = recoveryContained(path.join(root, "objects"), view.contentRelativePath);
    if (!recoveryDirectory(path.dirname(destination))) continue;
    let found;
    try { found = lstatSync(destination); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
    if (!found.isSymbolicLink() || path.resolve(path.dirname(destination), readFileLink(destination)) !== content ||
        !manifest.files[view.relativePath]?.includes(view.contentRelativePath)) throw new Error("TV view path is foreign or changed");
    fence(() => {
      if (!recoveryDirectory(path.dirname(destination)) || !lstatSync(destination).isSymbolicLink() ||
          path.resolve(path.dirname(destination), readFileLink(destination)) !== content) throw new Error("TV view changed at retirement fence");
      unlinkSync(destination);
    });
    const directory = openSync(path.dirname(destination), "r");
    try { fsyncSync(directory); } finally { closeSync(directory); }
  }
  // Retain ownership entries during eviction. Missing links are replayable and can be restored
  // after newer demand; unrelated manifest entries are never rewritten or retired here.
}

/** Parent dispatches this job under the same global '*' resource lane as movie retention. */
export async function runTvRetentionCleanup(ctx: WorkerContext, job: JobRow, adapters: TvRetentionAdapters = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("TV retention requires durable execution");
  const repository = adapters.repository ?? (ctx.repo as WorkerContext["repo"] & { tvRetention?: TvRetentionRepo }).tvRetention;
  if (!repository?.available()) throw new Error("TV retention repository is not integrated");
  const { evictionId } = z.object({ evictionId: z.string().uuid() }).strict().parse(JSON.parse(job.payload_json ?? "{}"));
  const eviction = repository.eviction(evictionId);
  if (!eviction || eviction.jobId !== job.id) throw new Error("TV eviction does not belong to this durable job");
  const plan = eviction.plan, root = ctx.repo.readSettings()?.tv.managedRoot;
  if (!root) throw new Error("TV storage configuration missing");
  const contentRoot = path.join(root, "objects");
  const verifyMount = () => {
    ctx.execution!.assertActive();
    if (hash(ctx.repo.readSettings()) !== plan.settingsHash) throw new Error("TV retention configuration changed");
    const mount = observeRecoveryMount(ctx.repo, root, "tv");
    if (mount.state !== "verified" || mount.contentId !== plan.contentId || String(lstatSync(contentRoot, { bigint: true }).dev) !== plan.device)
      throw new Error("TV retention mount changed");
  };
  const fence = (action: () => void) => repository.withFence(plan.id, () => { verifyMount(); action(); });
  const active = () => { verifyMount(); repository.assert(plan.id); };
  const budget = () => ({ remainingBytes: Math.max(0, Math.min(TV_RETENTION_DEFAULT_HASH_BYTES, adapters.maxHashBytes ?? TV_RETENTION_DEFAULT_HASH_BYTES)),
    deadline: performance.now() + Math.max(1, Math.min(120_000, adapters.timeoutMs ?? TV_RETENTION_DEFAULT_TIMEOUT_MS)), onBytesRead: adapters.onHashBytes });
  const proofs = new Map<string, string>();
  const superseded = () => {
    const intent = ctx.repo.retention.get({ namespace: "tv-target", id: String(plan.scope.targetId) });
    return intent?.desiredPresence === "present" && intent.revision > plan.intentRevision;
  };
  const restoreViews = () => {
    const target = ctx.repo.tv.target(plan.scope.targetId);
    if (!target || !superseded()) return;
    const scoped = new Proxy(ctx.repo.tv, { get(repo, property) {
      if (property === "targets") return () => [target]; if (property === "publications") return () => [];
      const value = Reflect.get(repo, property); return typeof value === "function" ? value.bind(repo) : value;
    } });
    const settings = ctx.repo.readSettings()!, entries = tvViewEntries(scoped, tvRoots(root, settings.tv.plexViewPath));
    const mount = observeRecoveryMount(ctx.repo, root, "tv"), view = observeRecoveryView(mount, entries);
    if (entries.length && ["present", "missing"].includes(view.state)) repairRecoveryView(mount, entries, view);
  };
  if (eviction.finishedAt) { verifyMount(); if ((eviction.result as { outcome?: string })?.outcome === "superseded") restoreViews(); return "done"; }
  const before = await durableStep(ctx, `tv-retention:${plan.id}:disk-before`, async () => { verifyMount(); return observeRetentionDisk(root); });
  for (const file of plan.files) {
    const proof = await durableStep(ctx, `tv-retention:${plan.id}:proof:${file.id}`, async () => {
      verifyMount(); if (!superseded()) repository.assert(plan.id);
      const source = await inspectRetentionFile(contentRoot, file);
      if (!source || !matchesRetentionFile(source.stat, file)) throw new Error("TV object changed before durable proof");
      const sha256 = await tvRetentionDigest(source.path, budget()), after = await inspectRetentionFile(contentRoot, file);
      if (!after || !matchesRetentionFile(after.stat, file) || !plan.seedProofs.some((proof) => proof.fileId === file.id && proof.sha256 === sha256))
        throw new Error("TV object changed during durable proof");
      return { sha256 };
    });
    proofs.set(file.id, z.string().regex(/^[a-f0-9]{64}$/).parse(proof.sha256));
  }
  const restore = async () => {
    if (!superseded()) return false;
    verifyMount(); const present: string[] = [];
    for (const file of plan.files) {
      let canonical = await inspectRetentionFile(contentRoot, file);
      const moved = await inspectRetentionFile(contentRoot, { relativePath: staged(plan, file) });
      if (moved) {
        assertStagingOwner(contentRoot, plan.contentId);
        const ownRecoveryLink = canonical && canonical.stat.dev === moved.stat.dev && canonical.stat.ino === moved.stat.ino &&
          canonical.stat.nlink === 2n && moved.stat.nlink === 2n;
        if (!matchesRetentionFile(ownRecoveryLink ? { ...moved.stat, nlink: 1n } : moved.stat, file, true) || await tvRetentionDigest(moved.path, budget()) !== proofs.get(file.id))
          throw new Error("TV staged restoration proof changed");
        if (canonical && !ownRecoveryLink) throw new Error("TV restoration refuses to overwrite a canonical path");
        verifyMount();
        if (!canonical) {
          await fs.link(moved.path, path.join(contentRoot, file.relativePath)); await syncDirectory(path.dirname(path.join(contentRoot, file.relativePath)));
          await adapters.afterCheckpoint?.("file-restored-linked");
        }
        canonical = await inspectRetentionFile(contentRoot, file);
        if (!canonical || canonical.stat.ino !== moved.stat.ino || canonical.stat.dev !== moved.stat.dev || canonical.stat.nlink !== 2n)
          throw new Error("TV restoration identity changed");
        await fs.unlink(moved.path); await syncDirectory(path.dirname(moved.path));
        canonical = await inspectRetentionFile(contentRoot, file);
      }
      if (canonical) {
        if (!matchesRetentionFile(canonical.stat, file, true) || await tvRetentionDigest(canonical.path, budget()) !== proofs.get(file.id))
          throw new Error("TV restored canonical proof changed");
        present.push(file.id);
      }
    }
    verifyMount(); repository.supersede(plan.id, present); restoreViews();
    await adapters.afterCheckpoint?.("demand-superseded"); return true;
  };
  await durableStep(ctx, `tv-retention:${plan.id}:execute`, async () => {
    try {
      active(); retireView(plan, root, fence); await adapters.afterCheckpoint?.("view-retired");
      const staging = await ensureStaging(contentRoot, plan.contentId, adapters);
      const revalidate = async (currentFile: TvRetentionPlan["files"][number]) => {
        active();
        const membership = await new TvRetentionService(ctx.repo, adapters).membership(plan.work, plan.scope);
        if (!membership.complete || !membership.fullyAired || membership.workKey !== plan.work.key || membership.episodeKeys.length !== plan.episodes.length ||
            plan.episodes.some((episode) => !membership.episodeKeys.includes(episode.key))) throw new Error("Whole TV season membership changed");
        const seeds = await observeTvRetentionSeeds({ root, objects: [currentFile].map((file) => ({ ...file, canonicalPath: path.join(contentRoot, file.relativePath),
          sizeBytes: file.sizeBytes, allocatedBytes: file.allocatedBytes, linkCount: 1, sha256: proofs.get(file.id)! })),
          downloads: plan.downloads.filter((download) => download.fileIds.includes(currentFile.id)).map((download) => ({ ...download, fileIds: [currentFile.id] })),
          memberProofs: plan.seedProofs.filter((proof) => proof.fileId === currentFile.id), budget: budget(), transmission: adapters.transmission });
        if (seeds.state !== "verified-independent-seed-store") throw new Error("TV independent seed bytes no longer verified");
        const settings = ctx.repo.readSettings()!;
        const watch = await (adapters.watchHistory ?? plex)({ tvdbId: plan.work.tvdbId ?? 0,
          episodes: plan.episodes.map((episode) => ({ ...episode, providerId: /^tvdb:([1-9]\d*)$/.exec(episode.key)?.[1] ?? "" })),
          plexUrl: settings.movies.plexUrl, plexToken: settings.movies.plexToken, plexSectionId: settings.tv.plexSectionId });
        if (watch.state !== "watched" || watch.coverage !== "global-all-users" || watch.activePlayback !== "none" || watch.episodes.length !== plan.episodes.length ||
            plan.episodes.some((episode) => !watch.episodes.some((watched) => watched.episodeId === episode.id && watched.episodeKey === episode.key &&
              watched.state === "watched" && Number.isSafeInteger(watched.playCount) && (watched.playCount ?? 0) > 0 && watched.lastViewedAt &&
              Number.isFinite(Date.parse(watched.lastViewedAt)) && Date.parse(watched.lastViewedAt) <= Date.now() + 300_000)))
          throw new Error("Whole-season global history/playback is no longer eligible");
        active();
      };
      for (const file of plan.files) {
        active();
        let source = await inspectRetentionFile(contentRoot, file), moved = await inspectRetentionFile(contentRoot, { relativePath: staged(plan, file) });
        if (!source && !moved) continue;
        if (source && moved) throw new Error("TV canonical and staged paths both exist");
        if (source && !matchesRetentionFile(source.stat, file)) throw new Error("TV canonical identity changed");
        if (moved && !matchesRetentionFile(moved.stat, file, true)) throw new Error("TV staged identity changed");
        await revalidate(file);
        if (source) {
          const destination = path.join(staging, path.basename(staged(plan, file)));
          fence(() => {
            assertStagingOwner(contentRoot, plan.contentId);
            if (!recoveryDirectory(path.dirname(source!.path)) || !matchesRetentionFile(lstatSync(source!.path, { bigint: true }), file)) throw new Error("TV object changed at staging fence");
            try { lstatSync(destination); throw new Error("TV staging path already exists"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
            if (!recoveryDirectory(staging)) throw new Error("TV staging directory changed");
            renameSync(source!.path, destination);
          });
          await syncDirectory(path.dirname(source.path)); await syncDirectory(staging); await adapters.afterCheckpoint?.("file-staged");
        }
        moved = await inspectRetentionFile(contentRoot, { relativePath: staged(plan, file) });
        if (!moved || !matchesRetentionFile(moved.stat, file, true) || await tvRetentionDigest(moved.path, budget()) !== proofs.get(file.id)) throw new Error("TV staged bytes differ from durable proof");
        const allocation = await (adapters.reclaimableAllocation ?? ext4ReclaimableAllocation)({ ...file, canonicalPath: moved.path,
          sizeBytes: file.sizeBytes, allocatedBytes: Number(moved.stat.blocks * 512n), linkCount: Number(moved.stat.nlink),
          device: String(moved.stat.dev), inode: String(moved.stat.ino), mtimeNs: String(moved.stat.mtimeNs), ctimeNs: String(moved.stat.ctimeNs) });
        if (allocation === null || !Number.isSafeInteger(allocation) || allocation < file.reclaimableBytes || allocation > file.allocatedBytes)
          throw new Error("TV staged reclaimability changed");
        await revalidate(file);
        const latest = await inspectRetentionFile(contentRoot, { relativePath: staged(plan, file) });
        if (!latest || !matchesRetentionFile(latest.stat, file, true) || latest.stat.ctimeNs !== moved.stat.ctimeNs) throw new Error("TV staged object changed before unlink");
        fence(() => {
          assertStagingOwner(contentRoot, plan.contentId);
          const current = lstatSync(latest.path, { bigint: true });
          if (!current.isFile() || !matchesRetentionFile(current, file, true) || current.ctimeNs !== latest.stat.ctimeNs || realpathSync(latest.path) !== latest.path)
            throw new Error("TV staged object changed at unlink fence");
          unlinkSync(latest.path);
        });
        await syncDirectory(staging); await adapters.afterCheckpoint?.("file-unlinked");
      }
      active();
      for (const file of plan.files) if (await inspectRetentionFile(contentRoot, file) || await inspectRetentionFile(contentRoot, { relativePath: staged(plan, file) }))
        throw new Error("TV whole-season absence is not verified");
      const after = await observeRetentionDisk(root); active();
      repository.finish(plan.id, { outcome: "evicted", verifiedAbsentFileIds: plan.files.map((file) => file.id),
        diskBefore: before, diskAfter: after, attributedFreedBytes: null });
    } catch (error) { if (!(await restore())) throw error; }
  });
  return "done";
}
