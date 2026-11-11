import { createHash } from "node:crypto";
import type { z } from "zod";
import type { BooksRepo } from "../repo";
import path from "node:path";
import { tvViewEntries, tvRoots } from "../tv/publication";
import { configuredTvCatalog, type TvCatalog } from "../tv/catalog";
import { observeRecoveryFile, observeRecoveryMount, observeRecoveryView } from "./recovery-filesystem";
import { ext4ReclaimableAllocation, inspectRetentionFile } from "./retention-filesystem";
import type { PhysicalFile } from "./retention-service";
import { auditDeletion } from "./retention";
import { RETENTION_REQUEST_GRACE_MS } from "./retention-policy-types";
import { createTvRetentionPlexReader, unknownTvRetentionWatch, type TvRetentionWatchInput, type TvRetentionWatch } from "./tv-retention-plex";
import { observeTvRetentionSeeds, type TvRetentionTransmission } from "./tv-retention-seeds";
import {
  tvRetentionSeasonSchema, tvRetentionPreviewParamsSchema, TV_RETENTION_MAX_EPISODES, TV_RETENTION_MAX_FILES,
  tvRetentionEvictParamsSchema, tvRetentionPlanSchema, type TvRetentionSeason, type TvRetentionInventory, type TvRetentionAudit,
  type TvRetentionSeedProof, TV_RETENTION_DEFAULT_HASH_BYTES, TV_RETENTION_DEFAULT_TIMEOUT_MS,
} from "./tv-retention-types";
import type { TvRetentionRepo } from "./tv-retention-repo";
import { tvRetentionDemandHash } from "./tv-retention-query";

export type TvRetentionMembership = { complete: boolean; fullyAired: boolean; workKey: string; source: string; episodeKeys: string[] };
export type TvRetentionAdapters = {
  readInventory?: (scope: TvRetentionSeason) => TvRetentionInventory;
  membership?: (work: NonNullable<TvRetentionInventory["work"]>, scope: TvRetentionSeason) => Promise<TvRetentionMembership>;
  catalog?: Pick<TvCatalog, "resolve"> | null;
  watchHistory?: (input: TvRetentionWatchInput) => Promise<TvRetentionWatch>;
  reclaimableAllocation?: (file: PhysicalFile) => Promise<number | null>;
  transmission?: TvRetentionTransmission | null;
  maxHashBytes?: number; timeoutMs?: number; now?: () => number;
  repository?: TvRetentionRepo;
  afterCheckpoint?: (name: string) => void | Promise<void>;
  onHashBytes?: (file: string, bytes: number) => void;
};
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const readPlex = createTvRetentionPlexReader();
const executionBlockers = [
  { code: "durable-repository-unsupported", message: "The single-season whole-target retention repository/schema is unavailable. Removal is disabled." },
  { code: "season-eviction-executor-unsupported", message: "The supported whole-target executor requires atomic coverage retirement, durable receipts and replay fences. No files or requests will be changed without that integration." },
];

/** Explicit, bounded read-only audit. Passing evidence is never represented as removal permission. */
export class TvRetentionService {
  private readonly auditContexts = new WeakMap<TvRetentionAudit, {
    baseline: string; inventory: TvRetentionInventory; settingsHash: string; mountHash: string;
    targetRevision: number; intentRevision: number; selected: { episodeId: number; versionId: number }[]; seedProofs: TvRetentionSeedProof[];
  }>();
  constructor(private readonly repo: BooksRepo, private readonly adapters: TvRetentionAdapters = {}) {}

  async preview(input: z.input<typeof tvRetentionPreviewParamsSchema>) {
    const params = tvRetentionPreviewParamsSchema.parse(input);
    const resources: TvRetentionAudit[] = [];
    for (const season of params.seasons) resources.push(await this.audit(season));
    return { mode: "audit-only" as const, removalSupported: resources.some((resource) => resource.removalSupported), resources };
  }

  repository(): TvRetentionRepo | null {
    return this.adapters.repository ?? (this.repo as BooksRepo & { tvRetention?: TvRetentionRepo }).tvRetention ?? null;
  }

  private inventory(scope: TvRetentionSeason): TvRetentionInventory {
    if (this.adapters.readInventory) return this.adapters.readInventory(scope);
    if (this.repository()) return this.repository()!.inventory(scope);
    const bridge = this.repo as BooksRepo & { readTvRetentionInventory?: (scope: TvRetentionSeason) => TvRetentionInventory };
    if (bridge.readTvRetentionInventory) return bridge.readTvRetentionInventory(scope);
    throw new Error("Target-scoped whole-season repository bridge unavailable");
  }

  async membership(work: NonNullable<TvRetentionInventory["work"]>, scope: TvRetentionSeason): Promise<TvRetentionMembership> {
    if (this.adapters.membership) return this.adapters.membership(work, scope);
    const settings = this.repo.readSettings();
    const catalog = this.adapters.catalog === undefined && settings ? configuredTvCatalog(settings) : this.adapters.catalog;
    if (!catalog || !work.tmdbId) throw new Error("Authoritative season membership unavailable");
    const entry = await catalog.resolve(work.tmdbId);
    if (entry.episodes.length > 5000 || entry.key !== work.key || entry.tvdbId !== work.tvdbId || entry.tmdbId !== work.tmdbId)
      throw new Error("Canonical work identity changed");
    const episodes = entry.episodes.filter((episode) => episode.numbering.some((n) => n.scheme === scope.scheme && n.season === scope.seasonNumber));
    if (episodes.some((episode) => episode.identity?.stability === "ephemeral" || episode.key.startsWith("ephemeral:")) ||
        new Set(episodes.map((episode) => episode.key)).size !== episodes.length) throw new Error("Unstable season identity");
    return { complete: episodes.length > 0 && episodes.length <= TV_RETENTION_MAX_EPISODES, workKey: entry.key,
      source: entry.source ?? entry.key, episodeKeys: episodes.map((episode) => episode.key),
      fullyAired: episodes.every((episode) => episode.airDate && Number.isFinite(Date.parse(episode.airDate)) && Date.parse(episode.airDate) <= (this.adapters.now?.() ?? Date.now())) };
  }

  async audit(input: z.input<typeof tvRetentionSeasonSchema>): Promise<TvRetentionAudit> {
    const season = tvRetentionSeasonSchema.parse(input), now = this.adapters.now?.() ?? Date.now();
    const settings = this.repo.readSettings();
    const key = { namespace: "tv-season" as const, id: `${season.targetId}:${season.scheme}:${season.seasonNumber}` };
    const mount = observeRecoveryMount(this.repo, settings?.tv.managedRoot ?? "", "tv");
    const result: TvRetentionAudit = {
      key, season, title: `TV target ${season.targetId}`, workKey: null, mode: "audit-only", removalSupported: false, eligible: false,
      safetyEligible: false, revision: "0".repeat(64), observedAt: new Date(now).toISOString(), targetRevision: null, desiredPresence: "unknown",
      membership: { state: "unknown", source: "Unverified complete canonical season membership", wholeSeasonRequested: false, expectedEpisodeCount: null, episodeIds: [] },
      mount: { state: mount.state, reason: mount.reason }, watch: unknownTvRetentionWatch({ episodes: [] }), files: [],
      seeding: { state: "last-copy-or-unknown", obligations: [] }, verifiedReclaimableBytes: null, planReclaimableBytes: 0,
      blockers: [], executionBlockers: executionBlockers.map((blocker) => ({ ...blocker })),
    };
    const block = (code: string, message: string) => { if (!result.blockers.some((blocker) => blocker.code === code)) result.blockers.push({ code, message }); };
    let inventory: TvRetentionInventory | null = null;
    let membership: TvRetentionMembership | null = null;
    const physical: PhysicalFile[] = [];
    let baseline: string | null = null;
    try {
      inventory = this.inventory(season);
      if (inventory.overflow || inventory.episodes.length > 50 || inventory.files.length > 20 || inventory.requests.length > 100 ||
          inventory.acquisitions.length > 100 || inventory.downloads.length > 20) throw new Error("Whole-season inventory exceeds bounds");
      if (!inventory.target || !inventory.work) { block("not-found", "Owned TV target not found."); throw new Error("Unknown target"); }
      result.title = inventory.work.title; result.workKey = inventory.work.key; result.targetRevision = inventory.target.revision;
      const targetKey = { namespace: "tv-target", id: String(season.targetId) };
      const targetIntent = this.repo.retention.get(targetKey), seasonIntent = this.repo.retention.get(key);
      const selectedAtAudit = this.repository()?.selected(season.targetId) ?? [];
      result.desiredPresence = seasonIntent?.desiredPresence ?? targetIntent?.desiredPresence ?? "unknown";
      baseline = hash([inventory, settings, targetIntent, seasonIntent, mount.evidence]);
      if (!targetIntent) block("intent-unknown", "No retained target intent; missing files do not establish season demand.");
      if (targetIntent?.desiredPresence === "evicted" || seasonIntent?.desiredPresence === "evicted") block("already-evicted", "Existing intentional absence is not permission for another removal.");
      if (mount.state !== "verified") block("mount-unavailable", mount.reason);
      if (!inventory.lossInventoryKnown) block("loss-inventory-unknown", "Content-loss tombstones cannot be verified; old lost files must not be treated as eviction candidates.");
      const episodes = inventory.episodes;
      const members = new Set(episodes.map((episode) => episode.id));
      if (!members.size || members.size !== episodes.length || new Set(episodes.map((episode) => episode.key)).size !== episodes.length)
        block("season-membership-unknown", "Season episode identities are missing or duplicated.");
      try {
        membership = await this.membership(inventory.work, season);
        result.membership.expectedEpisodeCount = membership.episodeKeys.length;
        if (!membership.complete || membership.workKey !== inventory.work.key || membership.episodeKeys.length !== episodes.length ||
            new Set(membership.episodeKeys).size !== membership.episodeKeys.length || episodes.some((episode) => !membership!.episodeKeys.includes(episode.key)))
          block("season-membership-incomplete", "The entire current canonical season is not represented by exact persisted episode IDs; a subset is not a whole-season candidate.");
        else { result.membership.state = "complete"; result.membership.source = membership.source; }
        if (!membership.fullyAired || episodes.some((episode) => !episode.airDate || !Number.isFinite(Date.parse(episode.airDate)) || Date.parse(episode.airDate) > now))
          block("season-not-fully-aired", "Future or unknown air dates protect the whole season.");
      } catch { block("season-membership-unknown", "Fresh authoritative whole-season membership is unavailable; persisted requested episodes alone are not completeness proof."); }
      result.membership.episodeIds = episodes.map((episode) => episode.id);
      const singleSeason = inventory.allSelectedEpisodeIds.length > 0 && inventory.allRequestedEpisodeIds.length > 0 &&
        [...inventory.allSelectedEpisodeIds, ...inventory.allRequestedEpisodeIds].every((id) => members.has(id));
      if (!singleSeason) result.executionBlockers = [{ code: "multi-season-target-unsupported", message: "All selected and requested episodes must belong to this one complete season. Multi-season targets remain protected until season-level intent gates exist." }];
      else if (this.repository()?.available() && inventory.acquisitions.length <= 20 && inventory.requests.length <= 20) {
        result.removalSupported = true; result.executionBlockers = [];
      }
      const activeRequests = inventory.requests.filter((request) => request.withdrawnAt === null);
      const requested = new Set(activeRequests.filter((request) => request.targetId === season.targetId).flatMap((request) => request.episodeIds));
      result.membership.wholeSeasonRequested = members.size > 0 && [...members].every((id) => requested.has(id));
      if (!result.membership.wholeSeasonRequested) block("partial-or-absent-season-demand", "Active original requests do not cover the entire season for this target.");
      const dates = [...activeRequests.map((request) => request.createdAt), ...(inventory.lastRequestedAt ? [inventory.lastRequestedAt] : [])].map(Date.parse);
      if (!dates.length || dates.some((date) => !Number.isFinite(date)) || now - Math.max(...dates) < RETENTION_REQUEST_GRACE_MS)
        block("fresh-or-unknown-request", "Fresh/unknown demand for any season episode or policy target protects the entire season for 30 days.");
      if ([...members].some((id) => !inventory!.selectedEpisodeIds.includes(id))) block("partial-season-selection", "Not every canonical season episode has selected owned content.");
      if (inventory.publications.length) block("active-publication", "A TV publication for this work is pending.");
      let active = 0;
      for (const acquisition of inventory.acquisitions) {
        const job = acquisition.jobId ? this.repo.getJob(acquisition.jobId) : null;
        if (job ? ["queued", "running"].includes(job.status) : !acquisition.finishedAt) active++;
      }
      if (active) block("active-acquisition", "An acquisition for this work or another policy target is active or unowned.");
      if (!inventory.files.length) block("no-files", "There is no complete selected canonical season file inventory.");
      const covered = new Set<number>();
      for (const file of inventory.files) {
        const observed = observeRecoveryFile(mount, file);
        const detail: TvRetentionAudit["files"][number] = { id: file.id, relativePath: file.relativePath, episodeIds: file.episodeIds,
          presence: observed.presence, ownership: observed.presence === "present" ? "verified-owned" : "unknown", references: file.references,
          lost: file.lost, physicalId: null, linkCount: null, mtimeNs: null, ctimeNs: null, allocatedBytes: null, verifiedReclaimableBytes: null };
        result.files.push(detail);
        if (file.lost) block("content-loss", "A selected object has a recorded loss tombstone; it cannot be an eviction candidate even if its path reappears.");
        if (!file.episodeIds.length || file.episodeIds.some((id) => !members.has(id))) block("cross-season-file", "A selected file contains episodes outside this exact whole-season removal set.");
        for (const id of file.episodeIds) covered.add(id);
        if (observed.presence !== "present") { block("presence-unverified", "At least one required canonical object is missing, replaced, unsafe, or unverified."); continue; }
        const found = await inspectRetentionFile(mount.contentRoot, { relativePath: file.relativePath });
        if (!found || found.stat.size !== BigInt(file.sizeBytes)) throw new Error("Canonical file changed");
        detail.physicalId = `${found.stat.dev}:${found.stat.ino}`; detail.linkCount = Number(found.stat.nlink); detail.allocatedBytes = Number(found.stat.blocks * 512n);
        detail.mtimeNs = String(found.stat.mtimeNs); detail.ctimeNs = String(found.stat.ctimeNs);
        if (detail.linkCount !== 1 || detail.references !== "verified-unshared") block("shared-or-unverified-reference", "Cross-target/domain references or surviving inode links block whole-season removal.");
        const object: PhysicalFile = { id: file.id, canonicalPath: found.path, device: String(found.stat.dev), inode: String(found.stat.ino),
          sizeBytes: file.sizeBytes, allocatedBytes: detail.allocatedBytes, linkCount: detail.linkCount, mtimeNs: String(found.stat.mtimeNs), ctimeNs: String(found.stat.ctimeNs) };
        physical.push(object);
        const allocation = await (this.adapters.reclaimableAllocation ?? ext4ReclaimableAllocation)(object);
        if (allocation !== null && Number.isSafeInteger(allocation) && allocation >= 0 && allocation <= object.allocatedBytes) detail.verifiedReclaimableBytes = allocation;
        else block("allocation-unknown", "Actual reclaimable allocation is unknown; file length/blocks alone are not storage savings.");
      }
      if ([...members].some((id) => !covered.has(id))) block("partial-season-files", "The selected files do not cover every canonical season episode.");
      const noPriorBlockers = result.blockers.length === 0;
      let seedProofs: TvRetentionSeedProof[] = [];
      if (noPriorBlockers) result.seeding = await observeTvRetentionSeeds({ root: mount.root, objects: physical, downloads: inventory.downloads,
        transmission: this.adapters.transmission, onVerifiedMembers: (proofs) => { seedProofs = proofs; },
        budget: { remainingBytes: Math.max(0, Math.min(TV_RETENTION_DEFAULT_HASH_BYTES, this.adapters.maxHashBytes ?? TV_RETENTION_DEFAULT_HASH_BYTES)),
          deadline: performance.now() + Math.max(1, Math.min(120_000, this.adapters.timeoutMs ?? TV_RETENTION_DEFAULT_TIMEOUT_MS)), onBytesRead: this.adapters.onHashBytes } });
      const watchEpisodes = episodes.map((episode) => ({ id: episode.id, key: episode.key, providerId: /^tvdb:([1-9]\d*)$/.exec(episode.key)?.[1] ?? "" }));
      const watchInput = { tvdbId: inventory.work.tvdbId ?? 0, episodes: watchEpisodes, plexUrl: settings?.movies.plexUrl ?? "",
        plexToken: settings?.movies.plexToken ?? "", plexSectionId: settings?.tv.plexSectionId ?? "" };
      result.watch = unknownTvRetentionWatch(watchInput);
      // Playback/history is observed AFTER expensive seed hashing, never inherited from a pre-hash read.
      if (result.membership.state === "complete") {
        try { result.watch = await (this.adapters.watchHistory ?? readPlex)(watchInput); }
        catch { /* An upstream response body/cause is never returned as an audit fact. */ }
      }
      if (result.watch.coverage !== "global-all-users" || result.watch.episodes.length !== episodes.length ||
          new Set(result.watch.episodes.map((episode) => episode.episodeId)).size !== episodes.length || episodes.some((episode) =>
            !result.watch.episodes.some((watched) => watched.episodeId === episode.id && watched.episodeKey === episode.key &&
              watched.state === "watched" && Number.isSafeInteger(watched.playCount) && (watched.playCount ?? 0) > 0 &&
              watched.lastViewedAt !== null && Number.isFinite(Date.parse(watched.lastViewedAt)) && Date.parse(watched.lastViewedAt) <= now + 300_000)))
        block("whole-season-watch-unverified", "Every exact canonical episode needs retained global Plex history; owner counters or a watched subset cannot authorize the whole season.");
      if (result.watch.state !== "watched" || result.watch.activePlayback !== "none") block("playback-active-or-unknown", "Global playback must be freshly verified inactive for every season member and duplicate copy.");
      if (result.seeding.state !== "verified-independent-seed-store") block("seed-safety-unverified", "Exact owned seed bindings and independent matching bytes for every object are unverified or exceed the proof budget; last-copy deletion remains blocked.");
      const current = this.inventory(season);
      const latestMount = observeRecoveryMount(this.repo, mount.root, "tv");
      if (hash([current, this.repo.readSettings(), this.repo.retention.get(targetKey), this.repo.retention.get(key), latestMount.evidence]) !== baseline)
        throw new Error("Season inventory/intent/configuration/mount changed");
      for (const object of physical) {
        const file = inventory.files.find((file) => file.id === object.id)!;
        const found = await inspectRetentionFile(mount.contentRoot, { relativePath: file.relativePath });
        if (!found || String(found.stat.dev) !== object.device || String(found.stat.ino) !== object.inode || found.stat.size !== BigInt(object.sizeBytes) ||
            String(found.stat.ctimeNs) !== object.ctimeNs || String(found.stat.mtimeNs) !== object.mtimeNs || Number(found.stat.nlink) !== object.linkCount)
          throw new Error("Canonical season bytes changed during observation");
      }
      const audit = auditDeletion({ resource: targetIntent ? { ...targetIntent, key, desiredPresence: "evicted", actualPresence: "present" } : null,
        files: result.files.map((file) => ({ fileId: file.physicalId ?? file.id, ownership: file.ownership, references: file.references, verifiedReclaimableBytes: file.verifiedReclaimableBytes })),
        activeAcquisitions: active, watchState: result.watch.state, seeding: { kind: "obligations", obligations: result.seeding.obligations } });
      for (const blocker of audit.blockers) block(blocker.code, blocker.message);
      result.safetyEligible = result.blockers.length === 0 && audit.eligible;
      result.eligible = result.safetyEligible && result.removalSupported;
      result.verifiedReclaimableBytes = result.files.length && result.files.every((file) => file.verifiedReclaimableBytes !== null)
        ? result.files.reduce((total, file) => total + file.verifiedReclaimableBytes!, 0) : null;
      if (result.verifiedReclaimableBytes !== null && !Number.isSafeInteger(result.verifiedReclaimableBytes)) result.verifiedReclaimableBytes = null;
      result.planReclaimableBytes = result.safetyEligible ? audit.reclaimableBytes : 0;
      if (result.eligible && targetIntent && inventory.target) this.auditContexts.set(result, {
        baseline, inventory, settingsHash: hash(settings), mountHash: hash(mount.evidence), targetRevision: inventory.target.revision,
        intentRevision: targetIntent.revision, selected: selectedAtAudit, seedProofs,
      });
    } catch {
      block("inventory-or-evidence-unverified", "Bounded season inventory, ownership, stable evidence, or required repository capability could not be verified; no removal is supported.");
      result.safetyEligible = false; result.eligible = false; result.planReclaimableBytes = 0; result.verifiedReclaimableBytes = null;
      for (const file of result.files) { file.presence = "unknown"; file.ownership = "unknown"; file.verifiedReclaimableBytes = null; }
      result.seeding = { state: "last-copy-or-unknown", obligations: result.seeding.obligations.map((o) => ({ ...o, remainingSeedableCopies: null })) };
    }
    result.revision = hash([inventory, membership, settings, { ...result, observedAt: null,
      seeding: { ...result.seeding, obligations: result.seeding.obligations.map((o) => ({ ...o, verifiedSecondsSeeded: null })) } }]);
    return result;
  }

  async evict(input: z.input<typeof tvRetentionEvictParamsSchema>) {
    const params = tvRetentionEvictParamsSchema.parse(input), repository = this.repository();
    const commandHash = hash(params);
    if (!repository?.available()) return { status: "blocked" as const, evictionId: null, jobId: null, blockers: executionBlockers };
    const prior = repository.eviction(params.operationId);
    if (prior) {
      if (prior.plan.commandHash !== commandHash) throw new Error("TV eviction operation conflict");
      return { status: "queued" as const, evictionId: prior.plan.id, jobId: prior.jobId, blockers: [] };
    }
    const scope = tvRetentionSeasonSchema.parse({ targetId: params.targetId, scheme: params.scheme, seasonNumber: params.seasonNumber });
    const audit = await this.audit(scope);
    if (!audit.eligible) return { status: "blocked" as const, evictionId: null, jobId: null, blockers: [...audit.blockers, ...audit.executionBlockers] };
    if (audit.revision !== params.expectedRevision) return { status: "blocked" as const, evictionId: null, jobId: null,
      blockers: [{ code: "stale-revision", message: "TV retention evidence changed; preview the complete season again." }] };
    const frozen = this.auditContexts.get(audit);
    if (!frozen) throw new Error("TV eviction audit context is unavailable");
    await this.adapters.afterCheckpoint?.("audit-complete");
    const settings = this.repo.readSettings()!, inventory = this.inventory(scope), mount = observeRecoveryMount(this.repo, settings.tv.managedRoot, "tv");
    const target = this.repo.tv.target(scope.targetId)!, intent = this.repo.retention.get({ namespace: "tv-target", id: String(scope.targetId) })!;
    const stale = () => ({ status: "blocked" as const, evictionId: null, jobId: null,
      blockers: [{ code: "stale-revision", message: "Demand, selection, storage or evidence changed after the audited snapshot; no eviction was queued." }] });
    const unchanged = () => hash([this.inventory(scope), this.repo.readSettings(), this.repo.retention.get({ namespace: "tv-target", id: String(scope.targetId) }),
      this.repo.retention.get(audit.key), observeRecoveryMount(this.repo, settings.tv.managedRoot, "tv").evidence]) === frozen.baseline;
    if (!unchanged() || target.revision !== frozen.targetRevision || intent.revision !== frozen.intentRevision || hash(settings) !== frozen.settingsHash || hash(mount.evidence) !== frozen.mountHash)
      return stale();
    const rendered = tvViewEntries(new Proxy(this.repo.tv, { get(targetRepo, property) {
      if (property === "targets") return () => [target];
      if (property === "publications") return () => [];
      const value = Reflect.get(targetRepo, property);
      return typeof value === "function" ? value.bind(targetRepo) : value;
    } }), tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath));
    const view = observeRecoveryView(mount, rendered);
    if (!["present", "missing"].includes(view.state) || rendered.length > 20) throw new Error("TV view ownership is unverified");
    const files = [];
    for (const file of audit.files) {
      const found = await inspectRetentionFile(mount.contentRoot, { relativePath: file.relativePath });
      if (!found || `${found.stat.dev}:${found.stat.ino}` !== file.physicalId || found.stat.nlink !== 1n ||
          String(found.stat.mtimeNs) !== file.mtimeNs || String(found.stat.ctimeNs) !== file.ctimeNs ||
          Number(found.stat.blocks * 512n) !== file.allocatedBytes) throw new Error("TV canonical identity changed before queue");
      files.push({ id: file.id, relativePath: file.relativePath, sizeBytes: Number(found.stat.size), device: String(found.stat.dev), inode: String(found.stat.ino),
        mtimeNs: String(found.stat.mtimeNs), ctimeNs: String(found.stat.ctimeNs), allocatedBytes: file.allocatedBytes!, reclaimableBytes: file.verifiedReclaimableBytes! });
      await this.adapters.afterCheckpoint?.("queue-file-inspected");
    }
    if (!unchanged()) return stale();
    const plan = tvRetentionPlanSchema.parse({ id: params.operationId, commandHash, scope, reason: params.reason, settingsHash: hash(settings),
      demandHash: tvRetentionDemandHash(frozen.inventory), expectedTargetRevision: frozen.targetRevision, retiredRevision: frozen.targetRevision + 1,
      expectedIntentRevision: frozen.intentRevision, intentRevision: frozen.intentRevision + 1, contentId: mount.contentId,
      device: files[0]!.device, createdAt: new Date(this.adapters.now?.() ?? Date.now()).toISOString(), work: inventory.work,
      episodes: frozen.inventory.episodes, selected: frozen.selected, files, downloads: frozen.inventory.downloads, seedProofs: frozen.seedProofs,
      views: rendered.map((entry) => ({ relativePath: entry.relativePath, contentRelativePath: path.relative(mount.contentRoot, entry.contentPath) })),
    });
    const queued = repository.queue(plan, (id) => this.repo.createJob({ type: "tv_retention_cleanup" as import("../app-types").JobType,
      payload: { evictionId: id }, idempotencyKey: `tv-retention:${id}`, maxAttempts: 5 }).id);
    return { status: "queued" as const, evictionId: queued.plan.id, jobId: queued.jobId, blockers: [] };
  }
}
