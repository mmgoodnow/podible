import { expect, test } from "bun:test";
import { planSeedConsolidation, type SeedConsolidationInput, type SeedContentObject, type SeedCoverageUnit,
  type SeedLibrarySelection, type SeedRegistration, type SeedSeasonPackEvidence } from "../../src/media/seed-consolidation";
import { MINIMUM_SEED_SECONDS } from "../../src/media/torrent-stewardship-types";

let inode = 0;
const unit = (id = "E01", media: SeedCoverageUnit["scope"]["media"] = "tv"): SeedCoverageUnit => ({ id, scope: { media, targetId: 1 } });
const object = (id: string, patch: Partial<SeedContentObject> = {}): SeedContentObject => ({
  id, identity: { device: "1", inode: String(++inode), size: 100, mtimeNs: "1", ctimeNs: "1", sha256: "a".repeat(64) },
  allocatedBytes: 100, verified: true, activeUse: false, activeWriter: false, ...patch,
});
function registration(id: string, objectIds: string[], patch: Partial<SeedRegistration> = {}): SeedRegistration {
  return { id, ref: { clientId: "private", nativeId: id }, registeredAt: "2026-01-01T00:00:00Z", objectIds,
    coverage: [{ unit: unit(), objectIds, verified: true }], ownershipVerified: true, activeUse: false,
    actualSeedSeconds: MINIMUM_SEED_SECONDS, seedCounterRegressed: false, evidenceCurrent: true, trackersComplete: true,
    trackers: [{ id: "tracker-a", state: "live", repeatedUnregistration: false }], ...patch };
}
type MutableInput = Omit<SeedConsolidationInput, "objects" | "registrations" | "librarySelections" | "requestedCoverage" | "seasonPacks"> & {
  objects: SeedContentObject[]; registrations: SeedRegistration[];
  librarySelections: SeedLibrarySelection[];
  requestedCoverage: SeedCoverageUnit[];
  seasonPacks?: SeedSeasonPackEvidence[];
};
function input(objects: SeedContentObject[], registrations: SeedRegistration[], selected = objects.map(row => row.id)): MutableInput {
  return { inventory: { objectsComplete: true, registrationsComplete: true, libraryComplete: true, activeUsesComplete: true },
    objects, registrations, requestedCoverage: [unit()],
    librarySelections: selected.length ? [{ libraryId: "chosen-edition", scope: unit().scope, objectIds: selected }] : [] };
}
function variants() {
  const old = object("episode"), pack = object("pack-file");
  pack.identity.sha256 = "b".repeat(64); // Same size, different bytes: not a reusable canonical object.
  return input([old, pack], [registration("old-episode", [old.id]), registration("new-pack", [pack.id], {
    registeredAt: "2026-02-01T00:00:00Z",
  })], [pack.id]);
}
const decision = (plan: ReturnType<typeof planSeedConsolidation>, id: string) => plan.registrations.find(row => row.id === id)!;
const objectDecision = (plan: ReturnType<typeof planSeedConsolidation>, id: string) => plan.objects.find(row => row.id === id)!;

const live = (id: string) => ({ id, state: "live", repeatedUnregistration: false } as const);
const dead = { id: "tracker-gone", state: "unregistered", repeatedUnregistration: true } as const;
function selectedSeason(shared = [true, false, false]) {
  const episodes = [unit("E01"), unit("E02"), unit("E03")];
  const packObjects = episodes.map(({ id }) => object(`pack-${id}`));
  const singleObjects = episodes.map(({ id }, index) => shared[index] ? packObjects[index]! : object(`single-${id}`));
  const coverage = (episode: SeedCoverageUnit, id: string) => ({ unit: episode, objectIds: [id], verified: true });
  const pack = registration("season-pack", packObjects.map(row => row.id), {
    registeredAt: "2026-03-01T00:00:00Z",
    coverage: episodes.map((episode, index) => coverage(episode, packObjects[index]!.id)),
    trackers: [live("pack-tracker")],
  });
  const singles = episodes.map((episode, index) => registration(`single-${episode.id}`, [singleObjects[index]!.id], {
    singleTvEpisode: episode, coverage: [coverage(episode, singleObjects[index]!.id)], trackers: [live("episode-tracker")],
  }));
  const fixture = input([...new Set([...packObjects, ...singleObjects])], [...singles, pack], []);
  fixture.requestedCoverage = episodes;
  fixture.librarySelections = pack.coverage.map(({ unit: episode, objectIds }) => ({
    libraryId: `plex-${episode.id}`, scope: episode.scope, objectIds,
    tvPublication: { episodeId: episode.id, versionId: 42, sourceRegistrationId: pack.id, state: "active", plexVerifiedCurrent: true },
  }));
  fixture.seasonPacks = [{ targetId: 1, seasonId: "provider-season-1", registrationId: pack.id, publicationVersionId: 42,
    expectedEpisodeIds: episodes.map(episode => episode.id), seasonInventoryComplete: true, packEpisodeInventoryComplete: true,
    fullTorrentVerified: true }];
  return { fixture, pack, singles, packObjects, singleObjects };
}

test("a selected complete pack supersedes singles even with shared bytes or unique live tracker coverage", () => {
  const { fixture, pack, singles, packObjects, singleObjects } = selectedSeason();
  singleObjects[2]!.activeUse = true;
  const plan = planSeedConsolidation(fixture);
  expect(plan).toMatchObject({ status: "planned", mode: "plan-only", destructiveActionsAllowed: false, uncoveredRequestedCoverage: [] });
  for (const single of singles) expect(decision(plan, single.id)).toMatchObject({ action: "retire",
    reason: "superseded-by-selected-season-pack", supersededBy: { registrationId: pack.id, targetId: 1,
      seasonId: "provider-season-1", publicationVersionId: 42 }, blockers: [], uniqueLiveTrackerCoverage: [expect.any(String)] });
  expect(decision(plan, pack.id).blockers).toContain("selected-complete-season-pack");
  for (const bytes of packObjects) expect(objectDecision(plan, bytes.id).blockers).toEqual(["selected-library-content", "retained-registration-reference"]);
  expect(objectDecision(plan, singleObjects[1]!.id).action).toBe("reclaim-candidate");
  expect(objectDecision(plan, singleObjects[2]!.id).blockers).toEqual(["active-object-use"]);
  expect(plan.accounting).toMatchObject({ liveTrackerCoveragePairsBefore: 6, liveTrackerCoveragePairsAfter: 3, reclaimCandidateLogicalBytes: 100 });
});

test("pack supersession always requires 30 actual seed days and keeps registration and writer fences", () => {
  const controls: [Partial<SeedRegistration>, Partial<SeedContentObject>, string][] = [
    [{ actualSeedSeconds: MINIMUM_SEED_SECONDS - 1 }, {}, "minimum-seeding-obligation"],
    [{ actualSeedSeconds: 0, trackers: [dead] }, {}, "minimum-seeding-obligation"],
    [{ actualSeedSeconds: null, trackers: [dead] }, {}, "seed-time-unknown"],
    [{ seedCounterRegressed: true }, {}, "seed-time-unknown"],
    [{ ownershipVerified: false }, {}, "ownership-unverified"],
    [{ evidenceCurrent: false }, {}, "stale-registration-evidence"],
    [{ activeUse: true }, {}, "active-registration-use"],
    [{ activeUse: null }, {}, "registration-use-unknown"],
    [{}, { activeUse: null }, "object-use-active-or-unknown"],
    [{}, { activeWriter: true }, "object-writer-active-or-unknown"],
    [{}, { activeWriter: null }, "object-writer-active-or-unknown"],
  ];
  for (const [registrationPatch, objectPatch, blocker] of controls) {
    const { fixture, singles, singleObjects } = selectedSeason();
    Object.assign(singles[1]!, { registeredAt: "2000-01-01T00:00:00Z" }, registrationPatch);
    Object.assign(singleObjects[1]!, objectPatch);
    const plan = planSeedConsolidation(fixture);
    expect(decision(plan, singles[1]!.id)).toMatchObject({ action: "retain", blockers: expect.arrayContaining([blocker]) });
    expect(objectDecision(plan, singleObjects[1]!.id).action).toBe("retain");
  }
});

test("pack policy requires the entire authoritative roster and a current exact Plex publication for every episode", () => {
  const controls: Record<string, (state: ReturnType<typeof selectedSeason>) => void> = {
    "no pack evidence": ({ fixture }) => { delete fixture.seasonPacks; },
    "incomplete season roster": ({ fixture }) => { fixture.seasonPacks![0]!.seasonInventoryComplete = false; },
    "incomplete pack mapping": ({ fixture }) => { fixture.seasonPacks![0]!.packEpisodeInventoryComplete = false; },
    "partial download": ({ fixture }) => { fixture.seasonPacks![0]!.fullTorrentVerified = false; },
    "pack missing an episode": ({ pack }) => { pack.coverage = pack.coverage.slice(0, 2); },
    "pack includes an extra episode": ({ fixture }) => { fixture.seasonPacks![0]!.expectedEpisodeIds = ["E01", "E02"]; },
    "duplicate roster entry": ({ fixture }) => { fixture.seasonPacks![0]!.expectedEpisodeIds = ["E01", "E01", "E02"]; },
    "wrong target": ({ fixture }) => { fixture.seasonPacks![0]!.targetId = 2; },
    "wrong registration": ({ fixture }) => { fixture.seasonPacks![0]!.registrationId = "missing"; },
    "conflicting season proofs": ({ fixture }) => { fixture.seasonPacks!.push({ ...fixture.seasonPacks![0]! }); },
    "same episode in two seasons": ({ fixture }) => { fixture.seasonPacks!.push({ ...fixture.seasonPacks![0]!, seasonId: "other-season" }); },
    "stale pack": ({ pack }) => { pack.evidenceCurrent = false; },
    "unverified pack ownership": ({ pack }) => { pack.ownershipVerified = false; },
    "unverified pack episode": ({ pack }) => { pack.coverage = pack.coverage.map(row => row.unit.id === "E03" ? { ...row, verified: false } : row); },
    "active pack writer": ({ packObjects }) => { packObjects[2]!.activeWriter = true; },
    "wrong publication version": ({ fixture }) => { fixture.librarySelections[2]!.tvPublication!.versionId++; },
    "same bytes, wrong source registration": ({ fixture, pack }) => {
      const duplicate = registration("pack-cross-seed", [...pack.objectIds], { coverage: pack.coverage });
      fixture.registrations.push(duplicate);
      fixture.seasonPacks![0]!.registrationId = duplicate.id;
    },
    "only staged": ({ fixture }) => { fixture.librarySelections[2]!.tvPublication!.state = "pending"; },
    "stale Plex verification": ({ fixture }) => { fixture.librarySelections[2]!.tvPublication!.plexVerifiedCurrent = false; },
    "missing Plex verification": ({ fixture }) => { delete fixture.librarySelections[2]!.tvPublication; },
    "ambiguous selected version": ({ fixture }) => {
      fixture.librarySelections.push({ ...fixture.librarySelections[2]!, libraryId: "other-edition" });
    },
    "same object union, different per-episode mapping": ({ fixture }) => {
      const [a, b] = fixture.librarySelections;
      [a!.objectIds, b!.objectIds] = [b!.objectIds, a!.objectIds];
    },
  };
  for (const [label, change] of Object.entries(controls)) {
    const state = selectedSeason();
    // A partial pack is not complete just because it covers every requested episode.
    state.fixture.requestedCoverage = [unit("E01"), unit("E02")];
    change(state);
    const plan = planSeedConsolidation(state.fixture);
    expect(plan.registrations.some(row => row.supersededBy), label).toBe(false);
    expect(decision(plan, "single-E01").action, label).toBe("retain");
    expect(decision(plan, "single-E02").action, label).toBe("retain");
  }
});

test("only proved singles qualify; retained registrations and other Plex selections still protect files", () => {
  const { fixture, pack, singles, singleObjects } = selectedSeason();
  const extra = object("bonus-episode");
  fixture.objects.push(extra);
  fixture.requestedCoverage.push(unit("E04"));
  singles[0]!.objectIds = [...singles[0]!.objectIds, extra.id];
  singles[0]!.coverage = [...singles[0]!.coverage, { unit: unit("E04"), objectIds: [extra.id], verified: true }];
  delete singles[1]!.singleTvEpisode;
  const used = singleObjects[2]!.id;
  fixture.registrations.push(registration("other-consumer", [used], { activeUse: true, coverage: [] }));
  fixture.librarySelections.push({ libraryId: "other-selection", scope: unit("other", "movie").scope, objectIds: [used] });
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, singles[0]!.id).blockers).toContain("unique-requested-coverage");
  expect(decision(plan, singles[1]!.id).blockers).toContain("unique-live-tracker-coverage");
  expect(decision(plan, singles[2]!.id).action).toBe("retire");
  expect(objectDecision(plan, extra.id).action).toBe("retain");
  expect(objectDecision(plan, used).blockers).toEqual(["selected-library-content", "retained-registration-reference"]);
  expect(decision(plan, pack.id).action).toBe("retain");
});

test("the pack remains the coverage anchor even when generic policy would retire its unregistered record first", () => {
  const { fixture, pack, singles } = selectedSeason();
  pack.trackers = [dead];
  pack.actualSeedSeconds = 0;
  expect(decision(planSeedConsolidation({ ...fixture, seasonPacks: [] }), pack.id).action).toBe("retire");
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, pack.id).blockers).toContain("selected-complete-season-pack");
  for (const single of singles) expect(decision(plan, single.id).action).toBe("retire");
  expect(plan.uncoveredRequestedCoverage).toEqual([]);
});

test("exact publication compares each whole episode's object set regardless of order", () => {
  const { fixture, pack, singles, packObjects } = selectedSeason();
  const part = object("second-part");
  fixture.objects.push(part);
  pack.objectIds = [...pack.objectIds, part.id];
  pack.coverage = [{ ...pack.coverage[0]!, objectIds: [packObjects[0]!.id, part.id] }, ...pack.coverage.slice(1)];
  fixture.librarySelections[0]!.objectIds = [part.id, packObjects[0]!.id];
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, singles[0]!.id).action).toBe("retire");
  expect(objectDecision(plan, part.id).action).toBe("retain");
  fixture.librarySelections[0]!.objectIds = [part.id];
  expect(decision(planSeedConsolidation(fixture), singles[0]!.id).action).toBe("retain");
});

test("generated seasons only lose tracker pairs for proved supersessions and never lose requested units or referenced objects", () => {
  const unitKey = (value: SeedCoverageUnit) => JSON.stringify([value.scope.media, value.scope.targetId, value.id]);
  const pairKey = (episode: SeedCoverageUnit, tracker: string) => JSON.stringify([unitKey(episode), tracker]);
  for (let seed = 0; seed < 64; seed++) {
    const { fixture, pack, singles, singleObjects } = selectedSeason([Boolean(seed & 1), Boolean(seed & 2), Boolean(seed & 4)]);
    const exact = !(seed & 8);
    if (!exact) fixture.librarySelections[2]!.tvPublication!.plexVerifiedCurrent = false;
    const seeded = [true, !(seed & 16), true];
    const independent = [true, true, !(seed & 32)];
    for (const [index, single] of singles.entries()) {
      single.actualSeedSeconds = seeded[index] ? MINIMUM_SEED_SECONDS : MINIMUM_SEED_SECONDS - 1;
      single.activeUse = !independent[index];
    }
    const outside = { ...unit(), scope: { media: "tv" as const, targetId: 2 } };
    fixture.requestedCoverage = [...fixture.requestedCoverage, outside];
    fixture.registrations.push(registration("outside-season", [singleObjects[0]!.id], {
      singleTvEpisode: outside, coverage: [{ unit: outside, objectIds: [singleObjects[0]!.id], verified: true }],
      trackers: [live("outside-tracker")],
    }));
    const before = structuredClone(fixture);
    const plan = planSeedConsolidation(fixture);
    expect(fixture).toEqual(before);
    expect(planSeedConsolidation({ ...fixture, objects: [...fixture.objects].reverse(), registrations: [...fixture.registrations].reverse(),
      seasonPacks: [{ ...fixture.seasonPacks![0]!, expectedEpisodeIds: [...fixture.seasonPacks![0]!.expectedEpisodeIds].reverse() }] })).toEqual(plan);
    expect(plan.uncoveredRequestedCoverage).toEqual([]);
    expect(decision(plan, pack.id).action).toBe("retain");
    expect(decision(plan, "outside-season").action).toBe("retain");
    const expectedLosses = new Set<string>();
    for (const [index, single] of singles.entries()) {
      const superseded = exact && seeded[index] && independent[index];
      const row = decision(plan, single.id);
      expect(row.action).toBe(superseded ? "retire" : "retain");
      if (superseded) {
        expect(row.supersededBy?.registrationId).toBe(pack.id);
        expectedLosses.add(pairKey(single.singleTvEpisode!, "episode-tracker"));
      }
    }
    const retained = fixture.registrations.filter(row => decision(plan, row.id).action === "retain");
    const pairs = (rows: SeedRegistration[]) => new Set(rows.flatMap(row => row.coverage.flatMap(proof =>
      row.trackers.filter(tracker => tracker.state === "live").map(tracker => pairKey(proof.unit, tracker.id)))));
    const beforePairs = pairs(fixture.registrations), afterPairs = pairs(retained);
    expect(new Set([...beforePairs].filter(pair => !afterPairs.has(pair)))).toEqual(expectedLosses);
    expect(plan.accounting.liveTrackerCoveragePairsBefore).toBe(beforePairs.size);
    expect(plan.accounting.liveTrackerCoveragePairsAfter).toBe(afterPairs.size);
    const referenced = new Set([...retained.flatMap(row => [...row.objectIds]), ...fixture.librarySelections.flatMap(row => [...row.objectIds])]);
    for (const row of plan.objects) expect(row.action).toBe(referenced.has(row.id) ? "retain" : "reclaim-candidate");
  }
});

test("episode and pack registrations sharing canonical bytes keep live free coverage without inventing duplicate storage", () => {
  const bytes = object("shared"), fixture = input([bytes], [registration("episode", [bytes.id]), registration("pack", [bytes.id], {
    trackers: [{ id: "tracker-b", state: "live", repeatedUnregistration: false }],
  })]);
  const plan = planSeedConsolidation(fixture);
  expect(plan.status).toBe("planned");
  expect(plan.destructiveActionsAllowed).toBe(false);
  expect(plan.registrations.every(row => row.action === "retain")).toBe(true);
  expect(plan.registrations.every(row => row.blockers.includes("live-coverage-without-extra-storage"))).toBe(true);
  expect(plan.accounting).toMatchObject({ objectBytes: 100, retainedObjectBytes: 100,
    registrationReferenceBytes: 200, retainedRegistrationReferenceBytes: 200,
    duplicateRegistrationReferenceBytes: 100, reclaimCandidateLogicalBytes: 0,
    liveTrackerCoveragePairsBefore: 2, liveTrackerCoveragePairsAfter: 2 });
});

test("one-byte variants remain separate objects; only the redundant unselected object's bytes become candidates", () => {
  const fixture = variants(), plan = planSeedConsolidation(fixture);
  expect(decision(plan, "old-episode")).toMatchObject({ action: "retire", reason: "redundant-storage", blockers: [] });
  expect(decision(plan, "new-pack").action).toBe("retain");
  expect(objectDecision(plan, "episode").action).toBe("reclaim-candidate");
  expect(objectDecision(plan, "pack-file").blockers).toContain("selected-library-content");
  expect(plan.accounting).toMatchObject({ objectBytes: 200, retainedObjectBytes: 100,
    duplicateRegistrationReferenceBytes: 0, reclaimCandidateLogicalBytes: 100, reclaimCandidateAllocatedBytesUpperBound: 100 });
  expect(plan.librarySelections).toEqual(fixture.librarySelections);
  // A shared SHA256 does not prove two physical objects consume only one allocation either.
  fixture.objects[1]!.identity.sha256 = fixture.objects[0]!.identity.sha256;
  expect(planSeedConsolidation(fixture).accounting.objectBytes).toBe(200);
});

test("an older pack cannot retire while it supplies a requested episode missing from the replacement pack", () => {
  const first = object("first"), second = object("second");
  const fixture = input([first, second], [registration("old-pack", [first.id, second.id], {
    coverage: [{ unit: unit("E01"), objectIds: [first.id], verified: true }, { unit: unit("E02"), objectIds: [second.id], verified: true }],
  }), registration("partial-new-pack", [second.id], {
    registeredAt: "2026-02-01T00:00:00Z", coverage: [{ unit: unit("E02"), objectIds: [second.id], verified: true }],
  })], [second.id]);
  fixture.requestedCoverage = [unit("E01"), unit("E02")];
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, "old-pack").blockers).toContain("unique-requested-coverage");
  expect(decision(plan, "old-pack").uniqueRequestedCoverage).toHaveLength(1);
  expect(objectDecision(plan, first.id).blockers).toContain("retained-registration-reference");
  expect(plan.uncoveredRequestedCoverage).toEqual([]);
  expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(0);
});

test("live and unknown trackers need 30 actual seed days, not registration age", () => {
  for (const state of ["live", "unknown"] as const) {
    for (const seconds of [0, MINIMUM_SEED_SECONDS - 1, MINIMUM_SEED_SECONDS]) {
      const fixture = variants();
      fixture.registrations[0] = { ...fixture.registrations[0]!, registeredAt: "2000-01-01T00:00:00Z", actualSeedSeconds: seconds,
        trackers: [{ id: "tracker-a", state, repeatedUnregistration: false }] };
      const row = decision(planSeedConsolidation(fixture), "old-episode");
      expect(row.action).toBe(seconds < MINIMUM_SEED_SECONDS ? "retain" : "retire");
      if (seconds < MINIMUM_SEED_SECONDS) expect(row.blockers).toContain("minimum-seeding-obligation");
    }
  }
  for (const patch of [{ actualSeedSeconds: null }, { seedCounterRegressed: true }] as Partial<SeedRegistration>[]) {
    const fixture = variants(); fixture.registrations[0] = { ...fixture.registrations[0]!, ...patch };
    expect(decision(planSeedConsolidation(fixture), "old-episode").blockers).toContain("seed-time-unknown");
  }
});

test("only current repeated proof from every tracker bypasses an impossible seed obligation", () => {
  const fixture = variants();
  fixture.registrations[0] = { ...fixture.registrations[0]!, actualSeedSeconds: 0,
    trackers: [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: true },
      { id: "tracker-b", state: "unregistered", repeatedUnregistration: true }] };
  expect(decision(planSeedConsolidation(fixture), "old-episode")).toMatchObject({ action: "retire", reason: "all-trackers-unregistered" });
  const controls: Partial<SeedRegistration>[] = [
    { evidenceCurrent: false }, { trackersComplete: false }, { trackers: [] },
    { trackers: [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: false }] },
    { trackers: [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: true }, { id: "tracker-b", state: "unknown", repeatedUnregistration: false }] },
    { trackers: [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: true }, { id: "tracker-b", state: "live", repeatedUnregistration: false }] },
  ];
  for (const patch of controls) {
    const other = structuredClone(fixture); other.registrations[0] = { ...other.registrations[0]!, ...patch };
    expect(decision(planSeedConsolidation(other), "old-episode").action).toBe("retain");
  }
});

test("unregistration never bypasses unique requested coverage or active use", () => {
  const bytes = object("only"), fixture = input([bytes], [registration("only", [bytes.id], {
    actualSeedSeconds: 0, trackers: [{ id: "gone", state: "unregistered", repeatedUnregistration: true }],
  })], []);
  expect(decision(planSeedConsolidation(fixture), "only").blockers).toContain("unique-requested-coverage");
  for (const activeUse of [true, null]) {
    const variant = variants();
    variant.registrations[0] = { ...variant.registrations[0]!, activeUse, actualSeedSeconds: 0,
      trackers: [{ id: "gone", state: "unregistered", repeatedUnregistration: true }] };
    expect(decision(planSeedConsolidation(variant), "old-episode").action).toBe("retain");
  }
});

test("reclamation requires no selected, retained-registration, active-use or writer reference", () => {
  for (const field of ["activeUse", "activeWriter"] as const) for (const value of [true, null]) {
    const bytes = object("orphan", { [field]: value }), fixture = input([bytes], [], []);
    const plan = planSeedConsolidation(fixture);
    expect(objectDecision(plan, bytes.id).action).toBe("retain");
    expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(0);
  }
  const bytes = object("orphan", { allocatedBytes: null }), fixture = input([bytes], [], []);
  const plan = planSeedConsolidation(fixture);
  expect(objectDecision(plan, bytes.id)).toMatchObject({ action: "reclaim-candidate", blockers: [] });
  expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(100);
  expect(plan.accounting.reclaimCandidateAllocatedBytesUpperBound).toBeNull();
});

test("unknown inventory, missing references and unverified objects fail closed for the entire snapshot", () => {
  const controls: Array<(fixture: MutableInput) => void> = [
    ...(["objectsComplete", "registrationsComplete", "libraryComplete", "activeUsesComplete"] as const).map(field =>
      (fixture: MutableInput) => { fixture.inventory[field] = false; }),
    fixture => { fixture.objects = fixture.objects.slice(1); },
    fixture => { fixture.objects[0]!.verified = false; },
    fixture => { fixture.librarySelections = [{ libraryId: "missing", scope: unit().scope, objectIds: ["absent"] }]; },
    fixture => { fixture.registrations[0] = { ...fixture.registrations[0]!, objectIds: [] }; },
    fixture => { fixture.objects[0]!.identity.size = Number.NaN; },
    fixture => { fixture.objects = [...fixture.objects, fixture.objects[0]!]; },
    fixture => { fixture.objects[1]!.identity.inode = fixture.objects[0]!.identity.inode; },
  ];
  for (const change of controls) {
    const fixture = variants(); change(fixture);
    const plan = planSeedConsolidation(fixture);
    expect(plan.status).toBe("blocked");
    expect(plan.inventoryBlockers.length).toBeGreaterThan(0);
    expect(plan.registrations.every(row => row.action === "retain")).toBe(true);
    expect(plan.objects.every(row => row.action === "retain")).toBe(true);
    expect(plan.accounting.objectBytes).toBeNull();
    expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(0);
  }
});

test("unknown ownership, coverage or stale evidence cannot replace a verified coverage anchor", () => {
  for (const patch of [{ ownershipVerified: false }, { evidenceCurrent: false },
    { coverage: [{ unit: unit(), objectIds: ["pack-file"], verified: false }] }] as Partial<SeedRegistration>[]) {
    const fixture = variants(); fixture.registrations[1] = { ...fixture.registrations[1]!, ...patch };
    expect(decision(planSeedConsolidation(fixture), "old-episode").blockers).toContain("unique-requested-coverage");
  }
});

test("a costly registration supplying a unique live tracker is retained despite duplicate episode coverage", () => {
  const fixture = variants();
  fixture.registrations[0] = { ...fixture.registrations[0]!, trackers: [{ id: "different-tracker", state: "live", repeatedUnregistration: false }] };
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, "old-episode").blockers).toContain("unique-live-tracker-coverage");
  expect(plan.accounting.liveTrackerCoveragePairsAfter).toBe(plan.accounting.liveTrackerCoveragePairsBefore);
});

test("selected seniority stays unchanged even if the newer registration is a pack", () => {
  const fixture = variants(); fixture.librarySelections = [{ libraryId: "original-choice", scope: unit().scope, objectIds: ["episode"] }];
  const plan = planSeedConsolidation(fixture);
  expect(plan.librarySelections).toEqual(fixture.librarySelections);
  expect(objectDecision(plan, "episode").blockers).toContain("selected-library-content");
  expect(decision(plan, "new-pack").action).toBe("retire");
});

test("coverage identities cannot cross movie, TV, audio or ebook slots with the same numeric ID", () => {
  for (const media of ["movie", "tv", "audio", "ebook"] as const) {
    const fixture = variants();
    fixture.requestedCoverage = [unit("1", media)];
    fixture.registrations[0] = { ...fixture.registrations[0]!, coverage: [{ unit: unit("1", media), objectIds: ["episode"], verified: true }] };
    fixture.registrations[1] = { ...fixture.registrations[1]!, coverage: [{ unit: unit("1", media === "ebook" ? "audio" : "ebook"), objectIds: ["pack-file"], verified: true }] };
    expect(decision(planSeedConsolidation(fixture), "old-episode").blockers).toContain("unique-requested-coverage");
  }
});

test("property-like graph cases preserve coverage, selections, byte accounting and permutation invariance", () => {
  for (let seed = 0; seed < 40; seed++) {
    const objects = Array.from({ length: 4 }, (_, index) => object(`object-${index}`));
    const registrations = Array.from({ length: 6 }, (_, index) => {
      const ids = [objects[(seed + index) % objects.length]!.id];
      return registration(`registration-${index}`, ids, {
        registeredAt: `2026-01-0${index + 1}T00:00:00Z`, actualSeedSeconds: index % 3 ? MINIMUM_SEED_SECONDS : 0,
        trackers: [{ id: `tracker-${(seed + index) % 2}`, state: "live", repeatedUnregistration: false }],
      });
    });
    const fixture = input(objects, registrations, [objects[seed % 4]!.id]);
    const before = structuredClone(fixture), plan = planSeedConsolidation(fixture);
    expect(fixture).toEqual(before);
    expect(planSeedConsolidation({ ...fixture, objects: [...objects].reverse(), registrations: [...registrations].reverse() })).toEqual(plan);
    expect(plan.uncoveredRequestedCoverage).toEqual([]);
    expect(plan.librarySelections).toEqual(fixture.librarySelections);
    expect(plan.accounting.liveTrackerCoveragePairsAfter).toBe(plan.accounting.liveTrackerCoveragePairsBefore);
    expect(plan.accounting.objectBytes).toBe(plan.accounting.retainedObjectBytes! + plan.accounting.reclaimCandidateLogicalBytes);
    const retainedIds = new Set(plan.registrations.filter(row => row.action === "retain").map(row => row.id));
    const referenced = new Set([...fixture.librarySelections.flatMap(row => [...row.objectIds]),
      ...registrations.filter(row => retainedIds.has(row.id)).flatMap(row => [...row.objectIds])]);
    for (const row of plan.objects) if (row.action === "reclaim-candidate") expect(referenced.has(row.id)).toBe(false);
  }
});

test("sequential retirement cannot retire both mutually redundant variants; changed explicit selections remain protected", () => {
  const fixture = variants(); fixture.librarySelections = [];
  const plan = planSeedConsolidation(fixture);
  expect(plan.registrations.filter(row => row.action === "retain")).toHaveLength(1);
  expect(plan.uncoveredRequestedCoverage).toEqual([]);
  const more = planSeedConsolidation({ ...fixture, librarySelections: [{ libraryId: "new-reference", scope: unit().scope, objectIds: ["pack-file"] }] });
  const oldCandidates = new Set(plan.objects.filter(row => row.action === "reclaim-candidate").map(row => row.id));
  // The retained registration can change because the explicit selection changed; both choices remain safe.
  for (const row of more.objects) if (row.action === "reclaim-candidate") {
    expect(row.id).not.toBe("pack-file");
    expect(more.registrations.some(reg => reg.action === "retain")).toBe(true);
  }
  expect(oldCandidates.size).toBe(1);
});

test("whole multi-object units cannot be replaced by unverified partial evidence or partially reclaimed", () => {
  const a = object("part-a"), b = object("part-b"), alternate = object("partial-alternate");
  const fixture = input([a, b, alternate], [
    registration("complete-unit", [a.id, b.id]),
    registration("partial-unit", [alternate.id], {
      registeredAt: "2026-02-01T00:00:00Z",
      coverage: [{ unit: unit(), objectIds: [alternate.id], verified: false }],
    }),
  ], [alternate.id]);
  const plan = planSeedConsolidation(fixture);
  expect(decision(plan, "complete-unit").blockers).toContain("unique-requested-coverage");
  expect(objectDecision(plan, a.id).action).toBe("retain");
  expect(objectDecision(plan, b.id).action).toBe("retain");
  expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(0);
  // A snapshot cannot declare a whole unit verified while one of its objects lacks byte proof.
  b.verified = false;
  const missingProof = planSeedConsolidation(fixture);
  expect(missingProof.status).toBe("blocked");
  expect(missingProof.objects.every(row => row.action === "retain")).toBe(true);
});

test("generated overlapping packs preserve whole-unit/tracker coverage, obligations and exact object-reference invariants", () => {
  const unitKey = (value: SeedCoverageUnit) => JSON.stringify([value.scope.media, value.scope.targetId, value.id]);
  const freeze = <T>(value: T): T => {
    if (value !== null && typeof value === "object") {
      for (const nested of Object.values(value)) freeze(nested);
      Object.freeze(value);
    }
    return value;
  };
  for (let seed = 0; seed < 80; seed++) {
    const units = [unit("E01"), unit("E02"), unit("E03")];
    const objects = Array.from({ length: 13 }, (_, index) => {
      const row = object(`unit-object-${index}`);
      row.identity.size = (index + seed) % 5 * 37;
      row.allocatedBytes = index % 3 ? row.identity.size : null;
      return row;
    });
    // Two different byte variants, each needing both of its objects to cover one unit.
    const objectIds = (unitIndex: number, variant: number) =>
      [objects[unitIndex * 4 + variant * 2]!.id, objects[unitIndex * 4 + variant * 2 + 1]!.id];
    const oldCoverage = units.map((value, index) => ({ unit: value, objectIds: objectIds(index, 0), verified: true }));
    const newCoverage = units.flatMap((value, index) => index === seed % 3 ? [] :
      [{ unit: value, objectIds: objectIds(index, 1), verified: true }]);
    const registrations = [
      registration("old-pack", oldCoverage.flatMap(row => row.objectIds), { coverage: oldCoverage }),
      registration("new-partial-pack", newCoverage.flatMap(row => row.objectIds), {
        registeredAt: "2026-03-01T00:00:00Z", coverage: newCoverage,
        trackers: [{ id: seed % 2 ? "tracker-a" : "tracker-b", state: "live", repeatedUnregistration: false }],
      }),
      ...units.map((value, index) => {
        const ids = objectIds(index, (seed + index) % 2);
        const dead = (seed + index) % 4 === 0;
        const trackers: SeedRegistration["trackers"] = dead
          ? [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: true }]
          : (seed + index) % 4 === 1
            ? [{ id: "tracker-a", state: "unregistered", repeatedUnregistration: true }, { id: "tracker-b", state: "unknown", repeatedUnregistration: false }]
            : [{ id: "tracker-a", state: "live", repeatedUnregistration: false }];
        return registration(`episode-${index}`, ids, { coverage: [{ unit: value, objectIds: ids, verified: true }],
          registeredAt: "2026-02-01T00:00:00Z", trackers,
          actualSeedSeconds: seed % 3 === 0 ? null : seed % 3 === 1 ? 0 : MINIMUM_SEED_SECONDS,
          activeUse: (seed + index) % 11 === 0 ? true : false,
          evidenceCurrent: (seed + index) % 13 !== 0,
          trackersComplete: (seed + index) % 7 !== 0,
        });
      }),
    ];
    if (seed % 7 === 0) objects[4]!.activeWriter = true;
    if (seed % 9 === 0) objects[12]!.activeUse = null;
    const fixture = input(objects, registrations, []);
    fixture.requestedCoverage = units;
    fixture.librarySelections = units.flatMap((value, index) => (seed + index) % 2 ? [] :
      [{ libraryId: `selected-${value.id}`, scope: value.scope, objectIds: objectIds(index, 0) }]);
    const original = structuredClone(fixture), plan = planSeedConsolidation(freeze(fixture));
    expect(plan.status).toBe("planned");
    expect(fixture).toEqual(original);
    expect(plan.librarySelections).toEqual(original.librarySelections);
    expect(planSeedConsolidation({ ...fixture, objects: [...objects].reverse(), registrations: [...registrations].reverse() })).toEqual(plan);
    const retainedIds = new Set(plan.registrations.filter(row => row.action === "retain").map(row => row.id));
    const beforeUnits = new Set<string>(), afterUnits = new Set<string>(), beforePairs = new Set<string>(), afterPairs = new Set<string>();
    for (const registration of registrations) {
      for (const proof of registration.coverage) {
        if (!registration.ownershipVerified || !registration.evidenceCurrent || !proof.verified ||
          proof.objectIds.some(id => {
            const row = objects.find(object => object.id === id)!;
            return !row.verified || row.activeWriter !== false;
          })) continue;
        const coverage = unitKey(proof.unit);
        beforeUnits.add(coverage);
        if (retainedIds.has(registration.id)) afterUnits.add(coverage);
        for (const tracker of registration.trackers) if (tracker.state === "live") {
          const pair = JSON.stringify([coverage, tracker.id]);
          beforePairs.add(pair);
          if (retainedIds.has(registration.id)) afterPairs.add(pair);
        }
      }
      if (!retainedIds.has(registration.id)) {
        const allUnregistered = registration.evidenceCurrent && registration.trackersComplete && registration.trackers.length > 0 &&
          registration.trackers.every(tracker => tracker.state === "unregistered" && tracker.repeatedUnregistration);
        expect(allUnregistered || (!registration.seedCounterRegressed && registration.actualSeedSeconds !== null &&
          registration.actualSeedSeconds >= MINIMUM_SEED_SECONDS)).toBe(true);
        expect(registration.activeUse).toBe(false);
        expect(registration.ownershipVerified).toBe(true);
        expect(registration.evidenceCurrent).toBe(true);
      }
    }
    expect([...afterUnits].sort()).toEqual([...beforeUnits].sort());
    expect([...afterPairs].sort()).toEqual([...beforePairs].sort());
    const referenced = new Set([...fixture.librarySelections.flatMap(selection => [...selection.objectIds]),
      ...registrations.filter(row => retainedIds.has(row.id)).flatMap(row => [...row.objectIds])]);
    let retainedBytes = 0, candidateBytes = 0;
    for (const row of plan.objects) {
      const object = objects.find(object => object.id === row.id)!;
      if (row.action === "reclaim-candidate") {
        expect(referenced.has(row.id)).toBe(false);
        expect(object.activeUse).toBe(false);
        expect(object.activeWriter).toBe(false);
        expect(object.verified).toBe(true);
        candidateBytes += object.identity.size;
      } else retainedBytes += object.identity.size;
    }
    expect(plan.accounting.reclaimCandidateLogicalBytes).toBe(candidateBytes);
    expect(plan.accounting.retainedObjectBytes).toBe(retainedBytes);
    expect(plan.accounting.objectBytes).toBe(retainedBytes + candidateBytes);
  }
});
