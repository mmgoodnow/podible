import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { MINIMUM_SEED_SECONDS } from "./torrent-stewardship-types";
import { retirementEvidenceSchema, type RetirementEvidence } from "./torrent-retirement-types";

export const retirementDigest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const prefix = "torrent-retirement:evidence:v1:";
const count = z.number().int().nonnegative().safe();
const trackersSchema = z.array(z.object({
  id: count, announce: z.string().min(1).max(4096), hasAnnounced: z.boolean(),
  lastAnnounceSucceeded: z.boolean(), lastAnnounceTimedOut: z.boolean(),
  lastAnnounceTime: count, lastAnnounceResult: z.string().max(4096),
})).min(1).max(64);
export function explicitlyUnregistered(message: string): boolean {
  return /^(?:unregistered torrent|torrent (?:is )?not registered|torrent not found|torrent does not exist)(?:[.!]|$)/i.test(message.trim());
}
/** Includes backup trackers: a primary-only stewardship streak cannot authorize retirement. */
export function allTrackerRetirementEvidence(value: unknown, now: number) {
  const parsed = trackersSchema.safeParse(value);
  if (!parsed.success || new Set(parsed.data.map((t) => t.id)).size !== parsed.data.length ||
      new Set(parsed.data.map((t) => t.announce)).size !== parsed.data.length) return null;
  if (parsed.data.some((t) => !t.hasAnnounced || t.lastAnnounceSucceeded || t.lastAnnounceTimedOut ||
      t.lastAnnounceTime <= 0 || t.lastAnnounceTime > now / 1000 || now / 1000 - t.lastAnnounceTime > 24 * 3600 ||
      !explicitlyUnregistered(t.lastAnnounceResult))) return null;
  const trackers = parsed.data.map((t) => ({ key: retirementDigest([t.id, t.announce]), announcedAt: t.lastAnnounceTime }))
    .sort((a, b) => a.key.localeCompare(b.key));
  return { trackers, trackerSetDigest: retirementDigest(trackers.map((t) => t.key)) };
}
export function readRetirementEvidence(repo: Pick<BooksRepo, "getJsonState">, bindingKey: string): RetirementEvidence | null {
  const parsed = retirementEvidenceSchema.safeParse(repo.getJsonState(prefix + bindingKey));
  return parsed.success ? parsed.data : null;
}
/** Synchronous, inside the collector's already-open application transaction. Raw tracker data is never persisted. */
export function recordRetirementEvidence(repo: Pick<BooksRepo, "getJsonState" | "setJsonState">, input: {
  bindingKey: string; bindingDigest: string; observedAt: string; ownershipVerified: boolean;
  complete: boolean; actualSeedSeconds: number | null; trackerStats?: unknown;
}): RetirementEvidence {
  const now = Date.parse(input.observedAt), previous = readRetirementEvidence(repo, input.bindingKey);
  if (previous && Date.parse(previous.observedAt) > now) return previous;
  const seed = count.safeParse(input.actualSeedSeconds);
  const current = input.ownershipVerified && input.complete && seed.success && seed.data >= MINIMUM_SEED_SECONDS
    ? allTrackerRetirementEvidence(input.trackerStats, now) : null;
  const compatible = current && previous && previous.bindingDigest === input.bindingDigest &&
    previous.trackerSetDigest === current.trackerSetDigest && previous.allTrackersExplicitlyUnregistered &&
    previous.actualSeedSeconds !== null && seed.success && seed.data >= previous.actualSeedSeconds &&
    now >= Date.parse(previous.observedAt) && now - Date.parse(previous.observedAt) <= 48 * 3600_000;
  const samples = compatible ? [...previous.samples] : [];
  if (current) {
    const prior = samples.at(-1);
    const advanced = !prior || current.trackers.every((tracker, index) => tracker.key === prior.trackers[index]?.key &&
      tracker.announcedAt >= prior.trackers[index]!.announcedAt + 15 * 60);
    const regressed = prior && current.trackers.some((tracker, index) => tracker.announcedAt < (prior.trackers[index]?.announcedAt ?? Infinity));
    if (regressed) samples.length = 0;
    if (advanced || regressed) samples.push({ observedAt: input.observedAt, actualSeedSeconds: seed.success ? seed.data : 0, trackers: current.trackers });
  }
  const value = retirementEvidenceSchema.parse({ bindingKey: input.bindingKey, bindingDigest: input.bindingDigest,
    observedAt: input.observedAt, trackerSetDigest: current?.trackerSetDigest ?? null, complete: input.complete,
    actualSeedSeconds: seed.success ? seed.data : null, allTrackersExplicitlyUnregistered: !!current, samples: samples.slice(-3) });
  repo.setJsonState(prefix + input.bindingKey, value);
  return value;
}
export function qualifiedRetirementEvidence(evidence: RetirementEvidence | null, bindingDigest: string, now: number): boolean {
  if (!evidence || evidence.bindingDigest !== bindingDigest || !evidence.complete || !evidence.allTrackersExplicitlyUnregistered ||
      evidence.actualSeedSeconds === null || evidence.actualSeedSeconds < MINIMUM_SEED_SECONDS || evidence.samples.length !== 3 ||
      now < Date.parse(evidence.observedAt) || now - Date.parse(evidence.observedAt) > 60 * 60_000) return false;
  const first = evidence.samples[0]!.trackers;
  return evidence.trackerSetDigest === retirementDigest(first.map((t) => t.key)) && evidence.samples.every((sample, index) =>
    sample.actualSeedSeconds >= MINIMUM_SEED_SECONDS && sample.trackers.length === first.length &&
    sample.trackers.every((tracker, i) => tracker.key === first[i]!.key && tracker.announcedAt <= now / 1000 &&
      now / 1000 - tracker.announcedAt <= 48 * 3600 && (index === 0 ||
        tracker.announcedAt >= evidence.samples[index - 1]!.trackers[i]!.announcedAt + 15 * 60)));
}
