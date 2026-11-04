import path from "node:path";
import type { BooksRepo } from "../repo";
import { movieViewEntries } from "../movies/publication";
import type { MovieAcquisition } from "../movies/repo";
import { queueMovieAcquisition } from "../movies/service";
import { tvRoots, tvViewEntries } from "../tv/publication";
import { queueTvAcquisition } from "../tv/acquisition";
import type { LibraryViewEntry } from "./library-view";
import {
  observeRecoveryFile, observeRecoveryMount, observeRecoveryView, recoveryDigest, recoveryViewFence, repairRecoveryView,
  type RecoveryMount, type RecoveryView,
} from "./recovery-filesystem";
import {
  RECOVERY_FILE_LIMIT, RECOVERY_OPERATION_LIMIT, recoveryInventoryParamsSchema, recoveryPreviewParamsSchema,
  recoveryRecoverParamsSchema, recoveryResultSchema,
  type RecoveryKey, type RecoveryResource, type RecoveryResult,
} from "./recovery-types";
import type { z } from "zod";

type File = { id: string; relativePath: string; sizeBytes: number };
type Snapshot = {
  resource: RecoveryResource; mount: RecoveryMount; view: RecoveryView; entries: LibraryViewEntry[];
  queueRequestIds: number[]; fence: string; files: File[];
};
const keyString = (key: RecoveryKey) => `${key.namespace}:${key.id}`;
const reason = (code: string, message: string) => ({ code, message });

/** Use the domain's actual declarative renderer, scoped to exactly one target. */
function scoped<T extends object>(repo: T, overrides: Partial<T>): T {
  return new Proxy(repo, { get(target, property) {
    if (property in overrides) return Reflect.get(overrides, property);
    const value = Reflect.get(target, property);
    return typeof value === "function" ? value.bind(target) : value;
  } });
}

export class RecoveryService {
  constructor(private readonly repo: BooksRepo) {}

  inventory(input: z.input<typeof recoveryInventoryParamsSchema>) {
    const params = recoveryInventoryParamsSchema.parse(input);
    const { targetIds: page, hasMore } = this.repo.readRecoveryInventory(params);
    return { resources: page.map((id) => this.inspect({ namespace: params.namespace, id: String(id) }).resource),
      nextAfterTargetId: hasMore ? page.at(-1)! : null };
  }

  preview(input: z.input<typeof recoveryPreviewParamsSchema>) {
    const params = recoveryPreviewParamsSchema.parse(input);
    return { resources: params.targets.map((key) => this.inspect(key).resource) };
  }

  recover(input: z.input<typeof recoveryRecoverParamsSchema>) {
    const params = recoveryRecoverParamsSchema.parse(input);
    const operationKey = `recovery-operation:${params.operationId}`;
    const descriptor = recoveryDigest(params.targets);
    const accepted = this.repo.transaction(() => {
      const prior = this.repo.getJsonState<{ descriptor: string }>(operationKey);
      if (prior) return prior.descriptor === descriptor;
      const index = this.repo.getJsonState<string[]>("recovery-operation-index") ?? [];
      if (!Array.isArray(index) || index.some((id) => typeof id !== "string")) throw new Error("Invalid recovery receipt inventory");
      if (index.length >= RECOVERY_OPERATION_LIMIT) return "limit" as const;
      this.repo.setJsonState("recovery-operation-index", [...index, params.operationId]);
      this.repo.setJsonState(operationKey, { descriptor });
      return true;
    });
    const results = params.targets.map((command): RecoveryResult => {
      const receiptKey = `${operationKey}:${keyString(command.key)}`;
      const current = this.inspect(command.key);
      const blocked = (code: string, message: string): RecoveryResult => ({
        key: command.key, status: "blocked", revision: current.resource.revision, jobIds: [], reasons: [reason(code, message)],
      });
      if (accepted !== true) return accepted === "limit"
        ? blocked("operation-budget", "Durable recovery operation budget is full. Existing operations remain retryable; receipts are never silently forgotten.")
        : blocked("operation-conflict", "Operation ID is already bound to a different exact batch. Preview and use a new ID.");
      return this.repo.transaction(() => {
        const saved = this.repo.getJsonState<RecoveryResult>(receiptKey);
        if (saved) return recoveryResultSchema.parse(saved);
        const fresh = current;
        if (fresh.resource.revision !== command.expectedRevision)
          return { ...blocked("stale-revision", "Recovery evidence or intent changed. Preview again; no action was taken."),
            reasons: [reason("stale-revision", "Recovery evidence or intent changed. Preview again; no action was taken."), ...fresh.resource.reasons] };
        if (fresh.resource.action === "blocked") return { ...blocked("blocked", "Recovery is blocked."), reasons: fresh.resource.reasons };
        if (fresh.resource.action === "none") {
          const result: RecoveryResult = { key: command.key, status: "unchanged", revision: fresh.resource.revision, jobIds: [], reasons: fresh.resource.reasons };
          this.repo.setJsonState(receiptKey, result);
          return result;
        }
        if (fresh.resource.action !== command.action)
          return blocked("action-mismatch", "Command does not match the current preview action; no fallback/download was attempted.");
        // Take the application writer before filesystem effects (also in WAL mode).
        // A deferred read transaction alone would allow a concurrent intent commit.
        this.repo.setJsonState(operationKey, { descriptor });
        // Full inventory/rendering is outside the writer. This fence is target-scoped,
        // <=20 exact canonical/view paths, and does not hash media or call the network.
        if (this.quickFence(fresh) !== fresh.fence)
          return blocked("stale-revision", "Targeted recovery fence changed; no action was taken.");
        const jobIds: number[] = [];
        const missingSelected = fresh.resource.canonical.files.filter(file => file.selected && file.presence === "missing").map(file => file.id);
        if (command.action === "queue-acquisition" && missingSelected.length) {
          this.repo.contentLoss.invalidate(command.key, fresh.resource.targetRevision!, missingSelected, params.operationId);
          jobIds.push(this.repo.createJob({
            type: command.key.namespace === "movie-target" ? "movie_reconcile" : "tv_reconcile",
            idempotencyKey: `recovery-view:${params.operationId}:${keyString(command.key)}`, maxAttempts: 3,
          }).id);
        }
        if (command.action === "repair-view") {
          repairRecoveryView(fresh.mount, fresh.entries, fresh.view);
        } else if (command.key.namespace === "movie-target") {
          const queued = queueMovieAcquisition(this.repo, Number(command.key.id), "recovery");
          if (!queued.acquisition.job_id) throw new Error("Recovery acquisition has no durable job");
          jobIds.push(queued.acquisition.job_id);
        } else {
          for (const requestId of fresh.queueRequestIds) {
            const queued = queueTvAcquisition(this.repo, requestId, "recovery");
            if (!queued.acquisition.job_id) throw new Error("Recovery acquisition has no durable job");
            jobIds.push(queued.acquisition.job_id);
          }
          if (!jobIds.length) throw new Error("Recovery has no actionable TV request");
        }
        const mount = observeRecoveryMount(this.repo, fresh.mount.root, command.key.namespace === "movie-target" ? "movies" : "tv");
        if (mount.state !== "verified" || JSON.stringify(mount.evidence) !== JSON.stringify(fresh.mount.evidence))
          throw new Error("Recovery mount changed before acknowledgement");
        const result: RecoveryResult = {
          key: command.key, status: command.action === "repair-view" ? "repaired" : "queued", revision: fresh.resource.revision, jobIds,
          reasons: [reason(command.action === "repair-view" ? "view-repaired" : "acquisition-queued",
            command.action === "repair-view" ? "Missing declarative links repaired using surviving selected owned content; Plex indexing was not verified."
              : "Existing requests retained; recovery acquisition and job command committed together. No network/download was performed by this command.")],
        };
        this.repo.setJsonState(receiptKey, result);
        return result;
      });
    });
    return { operationId: params.operationId, results };
  }

  private quickFence(snapshot: Pick<Snapshot, "resource" | "mount" | "files" | "entries">) {
    const key = snapshot.resource.key;
    const database = this.repo.readRecoveryInventory({ namespace: key.namespace, targetId: Number(key.id), fence: true }).snapshot;
    if (!database || database.overflow || database.files.length > 20 || database.links.length > 20 || database.acquisitions.length > 20 || database.versions.length > 20)
      throw new Error("Recovery mutation fence exceeds budget");
    const jobs = database.acquisitions.map((acquisition) => {
      const job = acquisition.job_id ? this.repo.getJob(Number(acquisition.job_id)) : null;
      return [acquisition.id, job?.status ?? null];
    });
    const mount = observeRecoveryMount(this.repo, snapshot.mount.root, key.namespace === "movie-target" ? "movies" : "tv");
    return JSON.stringify([database, this.repo.readSettings(), this.repo.retention.get(key), jobs, mount.evidence,
      snapshot.files.map((file) => observeRecoveryFile(mount, file)), recoveryViewFence(mount, snapshot.entries)]);
  }

  private inspect(key: RecoveryKey): Snapshot {
    const movie = key.namespace === "movie-target", id = Number(key.id), kind = movie ? "movies" : "tv";
    const settings = this.repo.readSettings();
    const mount = observeRecoveryMount(this.repo, settings?.[kind].managedRoot ?? "", kind);
    const emptyView: RecoveryView = { state: "not-selected", files: [], evidence: null, manifest: null };
    const resource: RecoveryResource = {
      key, title: keyString(key), revision: "0".repeat(64), targetRevision: null, intentRevision: null,
      desiredPresence: "unknown", requestIds: [], mount: { state: mount.state, contentId: mount.contentId, reason: mount.reason },
      canonical: { state: "unknown", files: [] }, view: { state: "unknown", files: [] }, activeAcquisitions: [], action: "blocked", reasons: [],
    };
    let entries: LibraryViewEntry[] = [], view = emptyView, queueRequestIds: number[] = [], filesToFence: File[] = [];
    let dbEvidence: unknown = null, fileEvidence: unknown = null;
    let expectedFence: unknown = null;
    const block = (code: string, message: string) => resource.reasons.push(reason(code, message));
    try {
      const database = this.repo.readRecoveryInventory({ namespace: key.namespace, targetId: id }).snapshot!;
      if (database.overflow) { block("inventory-limit", "This target exceeds the bounded preview budget; unrelated library size is not restricted."); throw new InspectionBlocked(); }
      const requests = database.requests;
      const versionRows = database.versions;
      const target = movie ? this.repo.movies.target(id) : this.repo.tv.target(id);
      if (!target) { block("not-found", "Owned target not found in this namespace."); throw new InspectionBlocked(); }
      const intent = this.repo.retention.get(key);
      resource.targetRevision = target.revision;
      resource.intentRevision = intent?.revision ?? null;
      resource.desiredPresence = intent?.desiredPresence ?? "unknown";
      resource.requestIds = requests.map((request) => ({ namespace: movie ? "movie-request" : "tv-request",
        id: String(request.id), active: request.withdrawn_at === null }));
      const activeRequests = requests.filter((request) => request.withdrawn_at === null);
      if (requests.length > 20) { block("inventory-limit", "More than 20 requests belong to this target; bounded recovery requires inspection."); throw new InspectionBlocked(); }
      // Both domains share these acquisition fields; use only the bounded SQL snapshot.
      const acquisitions = database.acquisitions as MovieAcquisition[];
      const acquisitionIds = new Set(acquisitions.map((acquisition) => acquisition.id));
      const ownedVersions = versionRows.filter((version) => movie ? acquisitionIds.has(Number(version.acquisition_id)) : version.target_id === id);
      if (ownedVersions.length > 100) { block("inventory-limit", "Version history exceeds the bounded recovery budget."); throw new InspectionBlocked(); }
      const jobs = acquisitions.map((acquisition) => ({ acquisition, job: acquisition.job_id ? this.repo.getJob(acquisition.job_id) : null }));
      resource.activeAcquisitions = jobs.filter(({ acquisition, job }) =>
        job ? ["queued", "running"].includes(job.status) : !acquisition.finished_at)
        .map(({ acquisition, job }) => ({ id: acquisition.id, jobId: acquisition.job_id, status: job?.status ?? "unknown" }));
      const publications = database.publications;
      const versions = ownedVersions.map((row) => movie ? this.repo.movies.version(Number(row.id))! : this.repo.tv.version(Number(row.id))!);
      const files = [...new Map(versions.flatMap((version) => version.files).map((file) => [file.id, file])).values()];
      filesToFence = files;
      if (files.length > RECOVERY_FILE_LIMIT) { block("inventory-limit", "Canonical file inventory exceeds the bounded recovery budget."); throw new InspectionBlocked(); }
      let selected = false;
      let missingDemand = false;
      let requestScope: unknown = null;
      if (movie) {
        const selectedTarget = this.repo.movies.target(id)!;
        const metadata = this.repo.movies.movie(selectedTarget.movie_id)!;
        resource.title = metadata.title;
        selected = selectedTarget.active_version_id !== null;
        if (selected && !ownedVersions.some((version) => version.id === selectedTarget.active_version_id))
          throw new Error("Selected movie version is not owned by this target");
        entries = movieViewEntries(scoped(this.repo.movies, { targets: () => [selectedTarget], publications: () => [] }), {
          contentRoot: mount.contentRoot, viewRoot: mount.viewRoot, serverViewRoot: settings?.movies.plexViewPath ?? "",
        });
        missingDemand = !selected;
        requestScope = metadata;
      } else {
        const selectedTarget = this.repo.tv.target(id)!;
        const entry = { ...JSON.parse(String(database.entry!.metadata_json)), id: selectedTarget.entry_id, episodes: [], refs: [], relations: [] };
        resource.title = entry.title;
        const active = this.repo.tv.activeEpisodes(id);
        selected = active.length > 0;
        if (active.some((episode) => !ownedVersions.some((version) => version.id === episode.version_id)))
          throw new Error("Selected TV version is not owned by this target");
        entries = tvViewEntries(scoped(this.repo.tv, { targets: () => [selectedTarget], publications: () => [], entry: () => entry }),
          tvRoots(mount.root, settings?.tv.plexViewPath ?? ""));
        requestScope = activeRequests.map((request) => ({ requestId: request.id,
          episodes: this.repo.tv.requestedEpisodes(Number(request.id)), missing: this.repo.tv.missingEpisodes(Number(request.id)),
        }));
        queueRequestIds = activeRequests.filter((request) => this.repo.tv.missingEpisodes(Number(request.id))
          .some((episode) => episode.airDate && Date.parse(episode.airDate) <= Date.now())).map((request) => Number(request.id));
        missingDemand = queueRequestIds.length > 0;
      }
      if (selected && !entries.length) throw new Error("Selected version has no declarative files");
      const selectedPaths = new Set(entries.map((entry) => path.relative(mount.contentRoot, entry.contentPath)));
      const observations = files.map((file: File) => ({ file, observation: observeRecoveryFile(mount, file) }));
      const selectedFiles = observations.filter(({ file }) => selectedPaths.has(file.relativePath));
      if (selectedFiles.length !== selectedPaths.size) throw new Error("Selected canonical inventory is incomplete");
      resource.canonical.files = observations.map(({ file, observation }) => ({ id: file.id, relativePath: file.relativePath,
        sizeBytes: file.sizeBytes, presence: observation.presence, selected: selectedPaths.has(file.relativePath) }));
      resource.canonical.state = mount.state !== "verified" ? "unknown" : !selected ? "not-selected"
        : selectedFiles.some(({ observation }) => observation.presence === "unknown") ? "unknown"
        : selectedFiles.some(({ observation }) => observation.presence === "missing") ? "missing" : "present";
      const missingSelectedIds = selectedFiles.filter(item => item.observation.presence === "missing").map(item => item.file.id);
      if (missingSelectedIds.length) {
        const conflict = this.repo.contentLoss.check(key, target.revision, missingSelectedIds);
        if (conflict) block("loss-invalidation-blocked", conflict);
        if (!movie) {
          const missing = new Set(missingSelectedIds);
          const lostEpisodes = new Set(versions.flatMap(version => "episodeIds" in version
            ? version.files.filter(file => missing.has(file.id)).flatMap(file => file.episodeIds) : []));
          queueRequestIds = [...new Set([...queueRequestIds, ...activeRequests.filter(request =>
            this.repo.tv.requestedEpisodes(Number(request.id)).some(ep => lostEpisodes.has(ep.id) && ep.airDate && Date.parse(ep.airDate) <= Date.now()))
            .map(request => Number(request.id))])];
          missingDemand = queueRequestIds.length > 0;
        } else missingDemand = true;
      }
      fileEvidence = observations.map(({ file, observation }) => [file.id, file.relativePath, file.sizeBytes, observation]);
      const viewFence = recoveryViewFence(mount, entries);
      view = observeRecoveryView(mount, entries);
      resource.view = { state: view.state, files: view.files };
      dbEvidence = [settings, target, intent, requests, jobs.map(({ acquisition, job }) => [acquisition, job?.status ?? null]),
        ownedVersions, versions, publications, requestScope];
      expectedFence = [database, settings, intent, jobs.map(({ acquisition, job }) => [acquisition.id, job?.status ?? null]),
        mount.evidence, observations.map(({ observation }) => observation), viewFence];
      if (!settings) block("settings-unavailable", "Stored storage configuration is absent; recovery never creates defaults.");
      if (!intent) block("intent-unknown", "No retained desired presence; missing files do not establish demand.");
      else if (intent.desiredPresence === "evicted") block("intentional-eviction", "Intentionally evicted; only an explicit re-request can restore demand.");
      if (!activeRequests.length) block("no-active-requests", "All requests are withdrawn or absent; recovery does not resurrect them.");
      if (mount.state !== "verified") block("mount-unavailable", mount.reason);
      if (resource.activeAcquisitions.length) block("active-acquisition", "An existing acquisition owns this target; wait or inspect its existing job.");
      if (publications.length) block("active-publication", "A publication is pending; recovery will not race its ownership.");
      if (observations.some(({ observation }) => observation.presence === "unknown")) block("canonical-unverified", "Canonical paths/types/sizes or ownership cannot be verified.");
      if (!missingSelectedIds.length && missingDemand && observations.some(({ file, observation }) => !selectedPaths.has(file.relativePath) && observation.presence === "present"))
        block("unsupported-repair", "Retained nonselected owned content survives. Safe republication/reselection requires domain verification; no replacement download was queued.");
      if (["conflict", "unknown"].includes(view.state)) block("view-unverified", "View ownership is unknown or a foreign/conflicting path exists; nothing is overwritten.");
      if (!resource.reasons.length) {
        resource.action = missingSelectedIds.length && missingDemand ? "queue-acquisition"
          : selected && view.state === "missing" ? "repair-view" : missingDemand ? "queue-acquisition" : "none";
        if (resource.action !== "none" && (files.length > 20 || database.links.length > 20 || entries.length > 20 || acquisitions.length > 20 || versions.length > 20 ||
            JSON.stringify(database).length > 65536 || JSON.stringify(view.manifest).length > 65536)) {
          block("mutation-limit", "Recovery mutation exceeds its short-fence budget (20 files/versions/acquisitions, 64 KiB metadata). Preview remains read-only.");
          resource.action = "blocked";
        }
        if (resource.action === "none") resource.reasons.push(reason("no-recovery-needed", "Selected owned bytes and view are present, or no currently aired requested episodes need acquisition."));
      }
    } catch (error) {
      resource.action = "blocked";
      if (!(error instanceof InspectionBlocked)) block("inventory-unverified", "Owned inventory is incomplete, unsupported, or exceeds the bounded read budget. No absence or recovery is inferred.");
    }
    const snapshot: Snapshot = { resource, mount, entries, view, queueRequestIds, fence: JSON.stringify(expectedFence), files: filesToFence };
    if (["repair-view", "queue-acquisition"].includes(resource.action) && this.quickFence(snapshot) !== snapshot.fence) {
      resource.action = "blocked";
      resource.canonical.state = "unknown"; resource.view.state = "unknown";
      block("stale-observation", "Database or filesystem evidence changed during preview; inspect again before acting.");
    }
    const evidence = recoveryDigest([key, dbEvidence, mount.evidence, fileEvidence, expectedFence]);
    resource.revision = recoveryDigest([resource, evidence, view.evidence]);
    return snapshot;
  }
}

class InspectionBlocked extends Error {}
