import { describe, expect, test } from "bun:test";
import {
  auditDeletion, MIN_SEED_SECONDS, recoveryDecision,
  type DeletionAuditInput, type RetentionResource, type SeedObligation,
} from "../../src/media/retention";

const resource: RetentionResource = {
  key: { namespace: "movie-target", id: "7" }, desiredPresence: "evicted",
  actualPresence: "present", revision: 3, updatedAt: "2026-11-03T00:00:00.000Z",
};
const seed = (overrides: Partial<SeedObligation> = {}): SeedObligation => ({
  id: "transmission:managed:hash", remainingSeedableCopies: 0,
  verifiedSecondsSeeded: MIN_SEED_SECONDS, requiredSecondsSeeded: 0,
  otherRequirements: "verified-satisfied", ...overrides,
});
function evidence(): DeletionAuditInput {
  return {
    resource: structuredClone(resource), activeAcquisitions: 0, watchState: "watched",
    files: [{ fileId: "device:inode:7", ownership: "verified-owned", references: "verified-unshared", verifiedReclaimableBytes: 4096 }],
    seeding: { kind: "obligations", obligations: [seed()] },
  };
}
const codes = (input: DeletionAuditInput) => auditDeletion(input).blockers.map((entry) => entry.code);
function withSeed(obligation: SeedObligation): DeletionAuditInput {
  return { ...evidence(), seeding: { kind: "obligations", obligations: [obligation] } };
}

describe("retention recovery facts", () => {
  test("unknown intent/presence needs inspection, not blind reacquisition", () => {
    expect(recoveryDecision(null).action).toBe("inspect");
    expect(recoveryDecision({ ...resource, desiredPresence: "present", actualPresence: "unknown" }).action).toBe("inspect");
    expect(recoveryDecision({ ...resource, desiredPresence: "present" }).action).toBe("none");
  });
  test.each(["present", "missing", "unknown"] as const)("intentional eviction suppresses recovery when actual is %s", (actualPresence) => {
    expect(recoveryDecision({ ...resource, actualPresence }).action).toBe("none");
  });
});

describe("conservative deletion audit", () => {
  test("reports operator-readable eligibility and desired recovery without mutating its evidence", () => {
    const input = evidence();
    const before = structuredClone(input);
    expect(auditDeletion(input)).toMatchObject({
      eligible: true, blockers: [], reclaimableBytes: 4096, desiredRecovery: { action: "none" },
    });
    expect(auditDeletion(input).summary).toContain("4096 verified reclaimable bytes; no deletion performed");
    expect(input).toEqual(before);
    expect(auditDeletion({ ...input, resource: { ...resource, desiredPresence: "present", actualPresence: "missing" } }))
      .toMatchObject({ eligible: false, reclaimableBytes: 0, desiredRecovery: { action: "recover" } });
  });

  test("requires deliberate intent, verified presence, known consumption and no active acquisitions", () => {
    expect(codes({ ...evidence(), resource: null })).toContain("intent-unknown");
    expect(codes({ ...evidence(), resource: { ...resource, desiredPresence: "present" } })).toContain("desired-present");
    for (const actualPresence of ["missing", "unknown"] as const)
      expect(codes({ ...evidence(), resource: { ...resource, actualPresence } })).toContain("presence-unverified");
    expect(codes({ ...evidence(), watchState: "unknown" })).toContain("watch-unknown");
    expect(codes({ ...evidence(), watchState: "unwatched" })).toContain("unwatched");
    expect(codes({ ...evidence(), activeAcquisitions: 1 })).toContain("active-acquisitions");
    for (const activeAcquisitions of [null, NaN, -1, 0.5, Infinity])
      expect(codes({ ...evidence(), activeAcquisitions })).toContain("acquisitions-unknown");
    const result = auditDeletion({ ...evidence(), watchState: "unknown", activeAcquisitions: 2 });
    expect(result.summary).toContain("Watch/consumption data is unknown");
    expect(result.summary).toContain("2 active acquisitions");
    expect(result.reclaimableBytes).toBe(0);
  });

  test("foreign/unknown ownership and unknown references block even with a plausible byte count", () => {
    const input = evidence();
    for (const ownership of ["foreign", "unknown"] as const) {
      input.files = [{ ...input.files[0]!, ownership }];
      expect(codes(input)).toContain("ownership-unverified");
    }
    input.files = [{ ...evidence().files[0]!, references: "unknown" }];
    expect(codes(input)).toContain("references-unverified");
  });

  test("shared references cannot be counted as reclaimable bytes, including library/seed hardlinks", () => {
    const input = evidence();
    input.files = [
      input.files[0]!,
      { ...input.files[0]!, fileId: "device:inode:8", references: "shared", verifiedReclaimableBytes: 8192 },
    ];
    const result = auditDeletion(input);
    expect(result.eligible).toBe(false);
    expect(result.reclaimableBytes).toBe(0);
    expect(result.blockers.map((entry) => entry.code)).toContain("shared-references");
    expect(result.summary).toContain("device:inode:8");
  });

  test("counts unique physical allocation bytes only and blocks duplicate identities/unknown bytes", () => {
    const input = evidence();
    input.files = [input.files[0]!, { ...input.files[0]!, fileId: "device:inode:8", verifiedReclaimableBytes: 8192 }];
    expect(auditDeletion(input).reclaimableBytes).toBe(12288);
    input.files = [input.files[0]!, input.files[0]!];
    expect(codes(input)).toContain("file-identity-invalid");
    expect(auditDeletion(input).reclaimableBytes).toBe(0);
    for (const verifiedReclaimableBytes of [null, NaN, Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
      expect(codes({ ...evidence(), files: [{ ...evidence().files[0]!, verifiedReclaimableBytes }] })).toContain("bytes-unverified");
    expect(codes({ ...evidence(), files: [] })).toContain("no-files");
    expect(codes({ ...evidence(), files: [{ ...evidence().files[0]!, fileId: "" }] })).toContain("file-identity-invalid");
    expect(codes({ ...evidence(), files: [{ ...evidence().files[0]!, verifiedReclaimableBytes: 0 }] })).toContain("no-reclaimable-bytes");
    input.files = [
      { ...evidence().files[0]!, verifiedReclaimableBytes: Number.MAX_SAFE_INTEGER },
      { ...evidence().files[0]!, fileId: "device:inode:8", verifiedReclaimableBytes: 1 },
    ];
    expect(codes(input)).toContain("bytes-unverified");
  });

  test("30 days is a verified-seconds floor, never wall-clock time since added or intent timestamps", () => {
    expect(MIN_SEED_SECONDS).toBe(2_592_000);
    const oldIntent = { ...resource, updatedAt: "2000-01-01T00:00:00.000Z" };
    expect(codes({ ...withSeed(seed({ verifiedSecondsSeeded: 0 })), resource: oldIntent })).toContain("seed-minimum");
    expect(codes(withSeed(seed({ verifiedSecondsSeeded: MIN_SEED_SECONDS - 1 })))).toContain("seed-minimum");
    expect(auditDeletion(withSeed(seed())).eligible).toBe(true);
    expect(auditDeletion(withSeed(seed({ verifiedSecondsSeeded: MIN_SEED_SECONDS + 1 }))).eligible).toBe(true);
    expect(codes(withSeed(seed({ requiredSecondsSeeded: MIN_SEED_SECONDS * 2 })))).toContain("seed-minimum");
    expect(auditDeletion(withSeed(seed({ requiredSecondsSeeded: MIN_SEED_SECONDS * 2, verifiedSecondsSeeded: MIN_SEED_SECONDS * 2 }))).eligible).toBe(true);
  });

  test("unknown or invalid seed history blocks deleting the last seed copy", () => {
    for (const verifiedSecondsSeeded of [null, NaN, -1, 1.5, Infinity]) {
      const result = auditDeletion(withSeed(seed({ verifiedSecondsSeeded })));
      expect(result.eligible).toBe(false);
      expect(result.reclaimableBytes).toBe(0);
      expect(result.blockers.map((entry) => entry.code)).toContain("seed-history-unknown");
      expect(result.summary).toContain("retain the last seed copy");
    }
  });

  test("verified surviving seed copies avoid losing the last copy; unknown copy inventory never does", () => {
    expect(auditDeletion(withSeed(seed({
      remainingSeedableCopies: 1, verifiedSecondsSeeded: null, requiredSecondsSeeded: null, otherRequirements: "unknown",
    }))).eligible).toBe(true);
    for (const remainingSeedableCopies of [null, NaN, -1, 0.5])
      expect(codes(withSeed(seed({ remainingSeedableCopies })))).toContain("seed-copies-unknown");
    const mixed = evidence();
    mixed.seeding = { kind: "obligations", obligations: [
      seed({ id: "retained", remainingSeedableCopies: 1, verifiedSecondsSeeded: null }),
      seed({ id: "last", verifiedSecondsSeeded: null }),
    ] };
    expect(auditDeletion(mixed).summary).toContain("last: verified seconds seeded are unknown");
    expect(auditDeletion(mixed).eligible).toBe(false);
  });

  test("never guesses tracker requirements, ratios, obligation inventory, or seeding inapplicability", () => {
    expect(codes(withSeed(seed({ requiredSecondsSeeded: null })))).toContain("seed-requirements-unknown");
    expect(codes(withSeed(seed({ otherRequirements: "unknown" })))).toContain("seed-requirements-unknown");
    expect(codes(withSeed(seed({ otherRequirements: "unmet" })))).toContain("seed-requirements-unmet");
    expect(codes({ ...evidence(), seeding: { kind: "unknown" } })).toContain("seed-obligations-unknown");
    expect(codes({ ...evidence(), seeding: { kind: "obligations", obligations: [] } })).toContain("seed-obligations-unknown");
    expect(codes(withSeed(seed({ id: "" })))).toContain("seed-obligations-unknown");
    expect(codes({ ...evidence(), seeding: { kind: "obligations", obligations: [seed(), seed()] } })).toContain("seed-obligations-unknown");
    expect(auditDeletion({ ...evidence(), seeding: { kind: "verified-not-applicable" } }).eligible).toBe(true);
  });
});
