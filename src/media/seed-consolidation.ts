import type { CrossSeedScope } from "./cross-seed-state";
import type { ReuseFileIdentity } from "./reuse-plan";
import type { TorrentRef } from "./torrent-client";
import { MINIMUM_SEED_SECONDS } from "./torrent-stewardship-types";

export type SeedCoverageUnit = { scope: CrossSeedScope; id: string };
export type SeedContentObject = {
  id: string;
  identity: ReuseFileIdentity;
  /** Reported allocation, not proof of exclusive extents or space that unlinking will free. */
  allocatedBytes: number | null;
  verified: boolean;
  activeUse: boolean | null;
  activeWriter: boolean | null;
};
export type SeedRegistration = {
  id: string;
  ref: TorrentRef;
  registeredAt: string;
  objectIds: readonly string[];
  /** A semantic coverage proof must name all objects needed for that whole unit. */
  coverage: readonly { unit: SeedCoverageUnit; objectIds: readonly string[]; verified: boolean }[];
  /** Independent proof of the entire release's episode inventory, not just its requested subset. */
  singleTvEpisode?: SeedCoverageUnit;
  ownershipVerified: boolean;
  activeUse: boolean | null;
  actualSeedSeconds: number | null;
  seedCounterRegressed: boolean;
  evidenceCurrent: boolean;
  trackersComplete: boolean;
  trackers: readonly {
    /** Normalized provider/tracker identity, never an announce URL or passkey. */
    id: string;
    state: "live" | "unregistered" | "unknown";
    repeatedUnregistration: boolean;
  }[];
};
export type SeedLibrarySelection = {
  libraryId: string;
  scope: CrossSeedScope;
  objectIds: readonly string[];
  tvPublication?: {
    episodeId: string;
    versionId: number;
    /** The exact version/acquisition source binding, not another registration of the same objects. */
    sourceRegistrationId: string;
    state: "active" | "pending";
    /** Plex currently verifies this exact episode and its published paths to objectIds. */
    plexVerifiedCurrent: boolean;
  };
};
export type SeedSeasonPackEvidence = {
  targetId: number;
  seasonId: string;
  registrationId: string;
  publicationVersionId: number;
  /** Authoritative season roster, independent of what was requested or downloaded. */
  expectedEpisodeIds: readonly string[];
  seasonInventoryComplete: boolean;
  /** The registration's episode mappings account for every episode in the torrent. */
  packEpisodeInventoryComplete: boolean;
  /** Current full-torrent completion and byte verification, not wanted-files completion. */
  fullTorrentVerified: boolean;
};
type SeasonPackAnchor = Pick<SeedSeasonPackEvidence, "targetId" | "seasonId" | "registrationId" | "publicationVersionId">;
export type SeedConsolidationInput = {
  inventory: { objectsComplete: boolean; registrationsComplete: boolean; libraryComplete: boolean; activeUsesComplete: boolean };
  objects: readonly SeedContentObject[];
  librarySelections: readonly SeedLibrarySelection[];
  requestedCoverage: readonly SeedCoverageUnit[];
  registrations: readonly SeedRegistration[];
  seasonPacks?: readonly SeedSeasonPackEvidence[];
};
export type SeedConsolidationPlan = {
  mode: "plan-only";
  destructiveActionsAllowed: false;
  status: "planned" | "blocked";
  inventoryBlockers: string[];
  minimumSeedSeconds: number;
  /** The input selections, copied without choosing a different encode, edition or version. */
  librarySelections: SeedConsolidationInput["librarySelections"];
  registrations: {
    id: string; ref: TorrentRef; action: "retain" | "retire";
    reason: "protected" | "redundant-storage" | "all-trackers-unregistered" | "superseded-by-selected-season-pack";
    supersededBy?: SeasonPackAnchor;
    blockers: string[];
    uniqueRequestedCoverage: string[];
    uniqueLiveTrackerCoverage: string[];
  }[];
  objects: { id: string; action: "retain" | "reclaim-candidate"; blockers: string[] }[];
  uncoveredRequestedCoverage: string[];
  accounting: {
    /** Count each canonical object once; identical hashes at different inodes stay separate. */
    objectBytes: number | null;
    retainedObjectBytes: number | null;
    registrationReferenceBytes: number | null;
    retainedRegistrationReferenceBytes: number | null;
    duplicateRegistrationReferenceBytes: number | null;
    reclaimCandidateLogicalBytes: number;
    reclaimCandidateAllocatedBytesUpperBound: number | null;
    liveTrackerCoveragePairsBefore: number;
    liveTrackerCoveragePairsAfter: number;
  };
};

const validId = (id: string) => typeof id === "string" && id.trim() === id && id.length > 0;
const count = (value: number) => Number.isSafeInteger(value) && value >= 0;
const key = (unit: SeedCoverageUnit) => JSON.stringify([unit.scope.media, unit.scope.targetId, unit.id]);
const validUnit = (unit: SeedCoverageUnit) => unit && unit.scope &&
  ["movie", "tv", "audio", "ebook"].includes(unit.scope.media) &&
  Number.isSafeInteger(unit.scope.targetId) && unit.scope.targetId > 0 && validId(unit.id);
const sorted = (values: Iterable<string>) => [...values].sort();
const unique = (values: readonly string[]) => new Set(values).size === values.length;
function sameIds(left: readonly string[], right: readonly string[]) {
  const rightIds = new Set(right);
  return left.length === right.length && left.every(id => rightIds.has(id));
}
function sum(values: readonly (number | null)[]): number | null {
  if (values.some(value => value === null || !count(value))) return null;
  const total = values.reduce<number>((result, value) => result + value!, 0);
  return count(total) ? total : null;
}

function selectedSeasonPacks(input: SeedConsolidationInput, objects: ReadonlyMap<string, SeedContentObject>) {
  const packs = input.seasonPacks ?? [];
  const registrations = new Map(input.registrations.map(row => [row.id, row]));
  const selections = new Map<string, SeedLibrarySelection[]>();
  for (const selection of input.librarySelections) {
    if (selection.scope.media !== "tv" || !selection.tvPublication) continue;
    const episode = key({ scope: selection.scope, id: selection.tvPublication.episodeId });
    selections.set(episode, [...(selections.get(episode) ?? []), selection]);
  }
  const seasonKey = (pack: SeedSeasonPackEvidence) => JSON.stringify([pack.targetId, pack.seasonId]);
  const seasonCounts = new Map<string, number>();
  for (const pack of packs) seasonCounts.set(seasonKey(pack), (seasonCounts.get(seasonKey(pack)) ?? 0) + 1);

  const qualified = packs.flatMap(pack => {
    const { targetId, seasonId, registrationId, publicationVersionId, expectedEpisodeIds } = pack;
    const registration = registrations.get(registrationId);
    if (!validId(seasonId) || seasonCounts.get(seasonKey(pack)) !== 1 ||
      !Number.isSafeInteger(publicationVersionId) || publicationVersionId <= 0 ||
      pack.seasonInventoryComplete !== true || pack.packEpisodeInventoryComplete !== true || pack.fullTorrentVerified !== true ||
      !expectedEpisodeIds.length || !expectedEpisodeIds.every(validId) || !unique(expectedEpisodeIds) ||
      !registration || registration.singleTvEpisode || registration.ownershipVerified !== true || registration.evidenceCurrent !== true ||
      registration.objectIds.some(id => objects.get(id)?.verified !== true || objects.get(id)?.activeWriter !== false)) return [];
    const episodes = expectedEpisodeIds.map(id => ({ scope: { media: "tv" as const, targetId }, id }));
    if (!episodes.every(validUnit)) return [];
    const episodeKeys = episodes.map(key);
    const coverage = new Map(registration.coverage.map(row => [key(row.unit), row]));
    if (coverage.size !== episodeKeys.length || !episodeKeys.every(episode => {
      const proof = coverage.get(episode);
      const rows = selections.get(episode);
      const selected = rows?.length === 1 ? rows[0] : undefined;
      const publication = selected?.tvPublication;
      return proof?.verified === true && selected && publication?.state === "active" &&
        publication.plexVerifiedCurrent === true && publication.versionId === publicationVersionId &&
        publication.sourceRegistrationId === registrationId &&
        sameIds(proof.objectIds, selected.objectIds);
    })) return [];
    return [{ anchor: { targetId, seasonId, registrationId, publicationVersionId }, episodeKeys }];
  });

  const episodeCounts = new Map<string, number>();
  for (const { episodeKeys } of qualified) for (const episode of episodeKeys)
    episodeCounts.set(episode, (episodeCounts.get(episode) ?? 0) + 1);
  return qualified.filter(({ episodeKeys }) => episodeKeys.every(episode => episodeCounts.get(episode) === 1));
}

/** Pure proposal only. The caller supplies a complete, coherent, independently verified inventory. */
export function planSeedConsolidation(input: SeedConsolidationInput): SeedConsolidationPlan {
  const inventoryBlockers: string[] = [];
  const block = (message: string) => inventoryBlockers.push(message);
  for (const field of ["objectsComplete", "registrationsComplete", "libraryComplete", "activeUsesComplete"] as const)
    if (input.inventory?.[field] !== true) block(`incomplete-inventory:${field}`);
  const objects = new Map(input.objects.map(object => [object.id, object]));
  const physical = new Set<string>();
  if (objects.size !== input.objects.length) block("duplicate-object-id");
  for (const object of input.objects) {
    const identity = object.identity;
    if (!validId(object.id) || !identity || !count(identity.size) || !/^\d+$/.test(identity.device) ||
      !/^\d+$/.test(identity.inode) || !/^\d+$/.test(identity.mtimeNs) || !/^\d+$/.test(identity.ctimeNs) ||
      (identity.sha256 != null && !/^[a-f0-9]{64}$/.test(identity.sha256)) ||
      (object.allocatedBytes !== null && !count(object.allocatedBytes))) block(`invalid-object:${object.id}`);
    if (object.verified !== true) block(`unverified-object:${object.id}`);
    const inode = JSON.stringify([identity?.device, identity?.inode]);
    if (physical.has(inode)) block(`ambiguous-physical-alias:${object.id}`);
    physical.add(inode);
  }
  const validateObjects = (ids: readonly string[], subject: string) => {
    if (!unique(ids)) block(`duplicate-reference:${subject}`);
    for (const id of ids) if (!objects.has(id)) block(`missing-object:${subject}:${id}`);
  };
  const selectedObjects = new Set<string>();
  const selectionIds = new Set<string>();
  for (const selection of input.librarySelections) {
    const unit = { scope: selection.scope, id: selection.libraryId };
    if (!validUnit(unit) || selectionIds.has(key(unit)) || !selection.objectIds.length) block(`invalid-library-selection:${selection.libraryId}`);
    selectionIds.add(key(unit));
    validateObjects(selection.objectIds, `library:${selection.libraryId}`);
    for (const id of selection.objectIds) selectedObjects.add(id);
  }
  const requested = new Set<string>();
  for (const unit of input.requestedCoverage) {
    if (!validUnit(unit) || requested.has(key(unit))) block("invalid-requested-coverage");
    requested.add(key(unit));
  }
  const registrationIds = new Set<string>(), refs = new Set<string>();
  for (const registration of input.registrations) {
    const ref = JSON.stringify([registration.ref.clientId, registration.ref.nativeId]);
    if (!validId(registration.id) || !validId(registration.ref.clientId) || !validId(registration.ref.nativeId) ||
      registrationIds.has(registration.id) || refs.has(ref) || !registration.objectIds.length ||
      !Number.isFinite(Date.parse(registration.registeredAt)) ||
      (registration.actualSeedSeconds !== null && !count(registration.actualSeedSeconds))) block(`invalid-registration:${registration.id}`);
    registrationIds.add(registration.id); refs.add(ref);
    validateObjects(registration.objectIds, `registration:${registration.id}`);
    const units = new Set<string>(), trackerIds = new Set<string>();
    for (const coverage of registration.coverage) {
      if (!validUnit(coverage.unit) || units.has(key(coverage.unit)) || !coverage.objectIds.length ||
        coverage.objectIds.some(id => !registration.objectIds.includes(id))) block(`invalid-coverage:${registration.id}`);
      units.add(key(coverage.unit));
      validateObjects(coverage.objectIds, `coverage:${registration.id}`);
    }
    for (const tracker of registration.trackers) {
      if (!validId(tracker.id) || trackerIds.has(tracker.id) || !["live", "unregistered", "unknown"].includes(tracker.state))
        block(`invalid-tracker:${registration.id}`);
      trackerIds.add(tracker.id);
    }
  }
  if (sum(input.objects.map(object => object.identity?.size ?? null)) === null) block("unknown-or-overflowing-object-bytes");
  const blocked = inventoryBlockers.length > 0;
  const registrations = [...input.registrations].sort((a, b) => a.id.localeCompare(b.id));
  const retained = new Set(registrations.map(registration => registration.id));
  const packs = blocked ? [] : selectedSeasonPacks(input, objects);
  const anchors = new Set(packs.map(pack => pack.anchor.registrationId));
  const packByEpisode = new Map(packs.flatMap(({ anchor, episodeKeys }) => episodeKeys.map(episode => [episode, anchor] as const)));
  const supersedingPack = (registration: SeedRegistration) => {
    const unit = registration.singleTvEpisode;
    if (!unit || !validUnit(unit) || unit.scope.media !== "tv" || registration.coverage.length !== 1 ||
      registration.coverage[0]!.verified !== true || key(registration.coverage[0]!.unit) !== key(unit)) return undefined;
    return packByEpisode.get(key(unit));
  };
  const coverageOf = (registration: SeedRegistration) => new Set(registration.coverage.filter(coverage =>
    registration.ownershipVerified === true && registration.evidenceCurrent === true && coverage.verified === true &&
    coverage.objectIds.every(id => objects.get(id)?.verified === true && objects.get(id)?.activeWriter === false))
    .map(coverage => key(coverage.unit)));
  const coverage = new Map(registrations.map(registration => [registration.id, coverageOf(registration)]));
  const liveTrackers = (registration: SeedRegistration) => registration.evidenceCurrent === true
    ? registration.trackers.filter(tracker => tracker.state === "live").map(tracker => tracker.id) : [];
  const unregistered = (registration: SeedRegistration) => registration.evidenceCurrent === true && registration.trackersComplete === true &&
    registration.trackers.length > 0 && registration.trackers.every(tracker =>
      tracker.state === "unregistered" && tracker.repeatedUnregistration === true);
  const baseBlockers = (registration: SeedRegistration, pack: SeasonPackAnchor | undefined) => {
    const blockers: string[] = [];
    if (registration.ownershipVerified !== true) blockers.push("ownership-unverified");
    if (registration.evidenceCurrent !== true) blockers.push("stale-registration-evidence");
    if (registration.activeUse !== false) blockers.push(registration.activeUse ? "active-registration-use" : "registration-use-unknown");
    // Known object readers can outlive a record-only pack retirement; unknown use still blocks it.
    if (registration.objectIds.some(id => {
      const use = objects.get(id)?.activeUse;
      return use !== false && !(pack && use === true);
    })) blockers.push("object-use-active-or-unknown");
    if (registration.objectIds.some(id => objects.get(id)?.activeWriter !== false)) blockers.push("object-writer-active-or-unknown");
    if (registration.coverage.some(unit => !unit.verified)) blockers.push("coverage-unverified");
    if (pack || !unregistered(registration)) {
      if (registration.seedCounterRegressed !== false || registration.actualSeedSeconds === null) blockers.push("seed-time-unknown");
      else if (registration.actualSeedSeconds < MINIMUM_SEED_SECONDS) blockers.push("minimum-seeding-obligation");
    }
    return blockers;
  };
  const decisions = new Map<string, SeedConsolidationPlan["registrations"][number]>();
  // Retire sequentially, newest first: two redundant variants cannot each rely on the other's retirement.
  const candidates = [...registrations].sort((a, b) => Date.parse(b.registeredAt) - Date.parse(a.registeredAt) || a.id.localeCompare(b.id));
  for (const registration of candidates) {
    const pack = supersedingPack(registration);
    const other = registrations.filter(row => row.id !== registration.id && retained.has(row.id));
    const uniqueCoverage = sorted([...coverage.get(registration.id)!].filter(unit => requested.has(unit) &&
      !other.some(row => coverage.get(row.id)!.has(unit))));
    const uniqueTrackers = sorted([...coverage.get(registration.id)!].filter(unit => requested.has(unit)).flatMap(unit =>
      liveTrackers(registration).filter(tracker => !other.some(row => coverage.get(row.id)!.has(unit) &&
        liveTrackers(row).includes(tracker))).map(tracker => JSON.stringify([unit, tracker]))));
    const protectedObjects = new Set([...selectedObjects, ...other.flatMap(row => [...row.objectIds])]);
    const additionalBytes = registration.objectIds.some(id => !protectedObjects.has(id) && (objects.get(id)?.identity.size ?? 0) > 0);
    const blockers = blocked ? ["inventory-blocked"] : baseBlockers(registration, pack);
    if (!blocked) {
      if (anchors.has(registration.id)) blockers.push("selected-complete-season-pack");
      if (uniqueCoverage.length) blockers.push("unique-requested-coverage");
      if (!pack && uniqueTrackers.length) blockers.push("unique-live-tracker-coverage");
      if (!pack && !unregistered(registration) && !additionalBytes)
        blockers.push(liveTrackers(registration).length ? "live-coverage-without-extra-storage" : "no-storage-reclaim-benefit");
    }
    const action = blockers.length ? "retain" : "retire";
    if (action === "retire") retained.delete(registration.id);
    decisions.set(registration.id, { id: registration.id, ref: { ...registration.ref }, action,
      reason: action === "retain" ? "protected" : pack ? "superseded-by-selected-season-pack" :
        unregistered(registration) ? "all-trackers-unregistered" : "redundant-storage",
      ...(action === "retire" && pack ? { supersededBy: { ...pack } } : {}),
      blockers, uniqueRequestedCoverage: uniqueCoverage, uniqueLiveTrackerCoverage: uniqueTrackers });
  }
  const objectDecisions = [...input.objects].sort((a, b) => a.id.localeCompare(b.id)).map(object => {
    const blockers = blocked ? ["inventory-blocked"] : [];
    if (selectedObjects.has(object.id)) blockers.push("selected-library-content");
    if (registrations.some(registration => retained.has(registration.id) && registration.objectIds.includes(object.id))) blockers.push("retained-registration-reference");
    if (object.activeUse !== false) blockers.push(object.activeUse ? "active-object-use" : "object-use-unknown");
    if (object.activeWriter !== false) blockers.push(object.activeWriter ? "active-writer" : "writer-unknown");
    return { id: object.id, action: blockers.length ? "retain" as const : "reclaim-candidate" as const, blockers };
  });
  const reclaim = objectDecisions.filter(object => object.action === "reclaim-candidate").map(object => objects.get(object.id)!);
  const retainedObjects = objectDecisions.filter(object => object.action === "retain").map(object => objects.get(object.id)!);
  const registrationBytes = (rows: readonly SeedRegistration[]) => sum(rows.flatMap(row => row.objectIds.map(id => objects.get(id)?.identity.size ?? null)));
  const referenced = new Set(registrations.flatMap(registration => [...registration.objectIds]));
  const referenceBytes = registrationBytes(registrations);
  const uniqueReferenceBytes = sum([...referenced].map(id => objects.get(id)?.identity.size ?? null));
  const trackerPairs = (rows: readonly SeedRegistration[]) => new Set(rows.flatMap(row =>
    [...coverage.get(row.id)!].filter(unit => requested.has(unit)).flatMap(unit =>
      liveTrackers(row).map(tracker => JSON.stringify([unit, tracker]))))).size;
  return {
    mode: "plan-only", destructiveActionsAllowed: false, status: blocked ? "blocked" : "planned", inventoryBlockers,
    minimumSeedSeconds: MINIMUM_SEED_SECONDS, librarySelections: structuredClone(input.librarySelections),
    registrations: registrations.map(registration => decisions.get(registration.id)!), objects: objectDecisions,
    uncoveredRequestedCoverage: sorted([...requested].filter(unit => !registrations.some(row => retained.has(row.id) && coverage.get(row.id)!.has(unit)))),
    accounting: {
      objectBytes: blocked ? null : sum(input.objects.map(object => object.identity.size)),
      retainedObjectBytes: blocked ? null : sum(retainedObjects.map(object => object.identity.size)),
      registrationReferenceBytes: blocked ? null : referenceBytes,
      retainedRegistrationReferenceBytes: blocked ? null : registrationBytes(registrations.filter(row => retained.has(row.id))),
      duplicateRegistrationReferenceBytes: blocked || referenceBytes === null || uniqueReferenceBytes === null ? null : referenceBytes - uniqueReferenceBytes,
      reclaimCandidateLogicalBytes: sum(reclaim.map(object => object.identity.size)) ?? 0,
      reclaimCandidateAllocatedBytesUpperBound: sum(reclaim.map(object => object.allocatedBytes)),
      liveTrackerCoveragePairsBefore: trackerPairs(registrations),
      liveTrackerCoveragePairsAfter: trackerPairs(registrations.filter(row => retained.has(row.id))),
    },
  };
}
