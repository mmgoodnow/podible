export type ResourceKey = { namespace: string; id: string };
export type DesiredPresence = "present" | "evicted";
export type ActualPresence = "present" | "missing" | "unknown";
export type RetentionAction = "requested" | "evicted" | "observed" | "withdrawn";

export type RetentionResource = {
  key: ResourceKey;
  desiredPresence: DesiredPresence;
  actualPresence: ActualPresence;
  revision: number;
  updatedAt: string;
};
export type RetentionEvent = {
  key: ResourceKey;
  revision: number;
  action: RetentionAction;
  reason: string;
  desiredPresence: DesiredPresence;
  actualPresence: ActualPresence;
  createdAt: string;
};

export function validateResourceKey(key: ResourceKey): void {
  if (
    !key || typeof key.namespace !== "string" ||
    key.namespace.length > 64 || !/^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/.test(key.namespace) ||
    typeof key.id !== "string" || !key.id.length || key.id.length > 256 ||
    key.id.trim() !== key.id || /[\x00-\x1f\x7f]/.test(key.id)
  ) throw new TypeError("Invalid retention resource key");
}

export type RecoveryDecision = {
  action: "recover" | "none" | "inspect";
  reason: string;
};

/** A recommendation about intent, not permission to enqueue or acquire anything. */
export function recoveryDecision(resource: RetentionResource | null): RecoveryDecision {
  if (!resource) return { action: "inspect", reason: "No retained intent; do not infer demand from missing files." };
  if (resource.desiredPresence === "evicted")
    return { action: "none", reason: "Deliberately evicted; only an explicit re-request restores desired presence." };
  if (resource.actualPresence === "missing")
    return { action: "recover", reason: "Desired present but verified missing; accidental loss needs recovery." };
  if (resource.actualPresence === "unknown")
    return { action: "inspect", reason: "Desired present but actual presence is unknown; verify before recovery." };
  return { action: "none", reason: "Desired present and observed present; no recovery needed." };
}

export const MIN_SEED_SECONDS = 30 * 24 * 60 * 60;

export type DeletionFileEvidence = {
  /** Stable physical allocation identity, not a pathname or a content hash. */
  fileId: string;
  ownership: "verified-owned" | "foreign" | "unknown";
  references: "verified-unshared" | "shared" | "unknown";
  verifiedReclaimableBytes: number | null;
};
export type SeedObligation = {
  id: string;
  /** Verified surviving copies for this exact obligation, not remote swarm seeders. */
  remainingSeedableCopies: number | null;
  verifiedSecondsSeeded: number | null;
  /** Verified tracker time requirement; zero means explicitly none, null means unknown. */
  requiredSecondsSeeded: number | null;
  otherRequirements: "verified-satisfied" | "unmet" | "unknown";
};
export type DeletionAuditInput = {
  resource: RetentionResource | null;
  files: readonly DeletionFileEvidence[];
  activeAcquisitions: number | null;
  watchState: "watched" | "unwatched" | "unknown";
  seeding:
    | { kind: "verified-not-applicable" }
    | { kind: "unknown" }
    | { kind: "obligations"; obligations: readonly SeedObligation[] };
};
export type DeletionBlockerCode =
  | "intent-unknown" | "desired-present" | "presence-unverified"
  | "acquisitions-unknown" | "active-acquisitions"
  | "watch-unknown" | "unwatched" | "no-files" | "file-identity-invalid"
  | "ownership-unverified" | "shared-references" | "references-unverified"
  | "bytes-unverified" | "no-reclaimable-bytes"
  | "seed-obligations-unknown" | "seed-copies-unknown" | "seed-history-unknown"
  | "seed-requirements-unknown" | "seed-minimum" | "seed-requirements-unmet";
export type DeletionBlocker = { code: DeletionBlockerCode; message: string };
export type DeletionAudit = {
  eligible: boolean;
  summary: string;
  blockers: DeletionBlocker[];
  /** Zero for any blocked plan; this is not logical file length or partial-plan savings. */
  reclaimableBytes: number;
  desiredRecovery: RecoveryDecision;
};

const knownCount = (value: number | null): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

/** Pure, all-or-nothing evidence audit. It never observes, changes intent, or deletes. */
export function auditDeletion(input: DeletionAuditInput): DeletionAudit {
  const blockers: DeletionBlocker[] = [];
  const block = (code: DeletionBlockerCode, message: string) => blockers.push({ code, message });
  if (!input.resource) block("intent-unknown", "No retained resource intent.");
  else {
    if (input.resource.desiredPresence !== "evicted")
      block("desired-present", "Resource is still desired present; no deliberate eviction intent.");
    if (input.resource.actualPresence !== "present")
      block("presence-unverified", "Files are not verified present; inspect rather than infer reclaimable storage.");
  }
  if (!knownCount(input.activeAcquisitions))
    block("acquisitions-unknown", "Active acquisition inventory is unknown.");
  else if (input.activeAcquisitions > 0)
    block("active-acquisitions", `${input.activeAcquisitions} active acquisitions may use these files.`);
  if (input.watchState === "unwatched") block("unwatched", "Resource has not been watched/read/listened to.");
  else if (input.watchState !== "watched") block("watch-unknown", "Watch/consumption data is unknown.");

  if (!input.files.length) block("no-files", "No known-owned physical files were supplied.");
  const identities = new Set<string>();
  let bytes = 0;
  for (const file of input.files) {
    if (!file.fileId?.trim() || identities.has(file.fileId))
      block("file-identity-invalid", `Missing or duplicate physical file identity: ${file.fileId}.`);
    identities.add(file.fileId);
    if (file.ownership !== "verified-owned")
      block("ownership-unverified", `${file.fileId}: ownership is ${file.ownership}; never claim foreign or unknown files.`);
    if (file.references === "shared") block("shared-references", `${file.fileId}: references outside the removal set still exist.`);
    else if (file.references !== "verified-unshared")
      block("references-unverified", `${file.fileId}: complete reference/link inventory is unverified.`);
    if (!knownCount(file.verifiedReclaimableBytes) || !Number.isSafeInteger(bytes + file.verifiedReclaimableBytes))
      block("bytes-unverified", `${file.fileId}: actual reclaimable allocation is unknown or unsafe to count.`);
    else bytes += file.verifiedReclaimableBytes;
  }
  if (bytes === 0) block("no-reclaimable-bytes", "No positive verified reclaimable bytes; file length is not disk savings.");

  if (input.seeding.kind === "obligations") {
    if (!input.seeding.obligations.length)
      block("seed-obligations-unknown", "Empty obligation inventory is not proof that seeding is inapplicable.");
    const obligations = new Set<string>();
    for (const obligation of input.seeding.obligations) {
      if (!obligation.id?.trim() || obligations.has(obligation.id))
        block("seed-obligations-unknown", "Seed obligation identity is missing or duplicated.");
      obligations.add(obligation.id);
      if (!knownCount(obligation.remainingSeedableCopies)) {
        block("seed-copies-unknown", `${obligation.id}: surviving seedable copies are unknown.`);
        continue;
      }
      if (obligation.remainingSeedableCopies > 0) continue;
      // Only a verified surviving seed copy can bypass checks on losing the last copy.
      if (!knownCount(obligation.verifiedSecondsSeeded))
        block("seed-history-unknown", `${obligation.id}: verified seconds seeded are unknown; retain the last seed copy.`);
      if (!knownCount(obligation.requiredSecondsSeeded))
        block("seed-requirements-unknown", `${obligation.id}: tracker time requirement is unknown.`);
      if (knownCount(obligation.verifiedSecondsSeeded) &&
          obligation.verifiedSecondsSeeded < Math.max(MIN_SEED_SECONDS, obligation.requiredSecondsSeeded ?? 0))
        block("seed-minimum", `${obligation.id}: last seed copy needs at least ${Math.max(MIN_SEED_SECONDS, obligation.requiredSecondsSeeded ?? 0)} verified seconds seeded (30-day floor).`);
      if (obligation.otherRequirements === "unmet")
        block("seed-requirements-unmet", `${obligation.id}: additional seed requirements are unmet.`);
      else if (obligation.otherRequirements !== "verified-satisfied")
        block("seed-requirements-unknown", `${obligation.id}: additional seed requirements are unknown.`);
    }
  } else if (input.seeding.kind !== "verified-not-applicable") {
    block("seed-obligations-unknown", "Seed obligation inventory is unknown; do not infer it from file paths or age.");
  }
  const eligible = blockers.length === 0;
  return {
    eligible,
    summary: eligible
      ? `Eligible on supplied evidence: ${bytes} verified reclaimable bytes; no deletion performed.`
      : `Blocked: ${blockers.map((entry) => entry.message).join(" ")}`,
    blockers,
    reclaimableBytes: eligible ? bytes : 0,
    desiredRecovery: recoveryDecision(input.resource),
  };
}
