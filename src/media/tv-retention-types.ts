import { z } from "zod";
import { retentionFilePlanSchema } from "./retention-policy-types";

export const TV_RETENTION_MAX_SEASONS = 3;
export const TV_RETENTION_MAX_EPISODES = 50;
export const TV_RETENTION_MAX_FILES = 20;
export const TV_RETENTION_DEFAULT_HASH_BYTES = 32 * 1024 ** 3;
export const TV_RETENTION_DEFAULT_TIMEOUT_MS = 60_000;
const id = z.number().int().positive().safe();
const count = z.number().int().nonnegative().safe();
export const tvRetentionSeasonSchema = z.object({
  targetId: id, scheme: z.string().regex(/^[a-z][a-z0-9-]*:[a-z][a-z0-9-]*$/).max(64).default("tvdb:aired"),
  seasonNumber: count,
}).strict();
export type TvRetentionSeason = z.output<typeof tvRetentionSeasonSchema>;
export const tvRetentionPreviewParamsSchema = z.object({
  seasons: z.array(tvRetentionSeasonSchema).min(1).max(TV_RETENTION_MAX_SEASONS).refine((seasons) =>
    new Set(seasons.map((season) => `${season.targetId}:${season.scheme}:${season.seasonNumber}`)).size === seasons.length, "Duplicate season"),
}).strict();
const blocker = z.object({ code: z.string(), message: z.string() }).strict();
const watchState = z.enum(["watched", "unwatched", "unknown"]);
export const tvRetentionAuditSchema = z.object({
  key: z.object({ namespace: z.literal("tv-season"), id: z.string() }).strict(),
  season: tvRetentionSeasonSchema, title: z.string(), workKey: z.string().nullable(),
  mode: z.literal("audit-only"), removalSupported: z.boolean(), eligible: z.boolean(),
  safetyEligible: z.boolean(), revision: z.string().regex(/^[a-f0-9]{64}$/), observedAt: z.string(),
  targetRevision: count.nullable(), desiredPresence: z.enum(["present", "evicted", "unknown"]),
  membership: z.object({ state: z.enum(["complete", "unknown"]), source: z.string(), wholeSeasonRequested: z.boolean(),
    expectedEpisodeCount: count.nullable(), episodeIds: z.array(id).max(TV_RETENTION_MAX_EPISODES) }).strict(),
  mount: z.object({ state: z.enum(["verified", "unavailable"]), reason: z.string() }).strict(),
  watch: z.object({ state: watchState, coverage: z.enum(["global-all-users", "unknown"]),
    activePlayback: z.enum(["active", "none", "unknown"]),
    episodes: z.array(z.object({ episodeId: id, episodeKey: z.string(), state: watchState,
      playCount: count.nullable(), lastViewedAt: z.string().nullable() }).strict()).max(TV_RETENTION_MAX_EPISODES),
  }).strict(),
  files: z.array(z.object({ id: z.string(), relativePath: z.string(), episodeIds: z.array(id),
    presence: z.enum(["present", "missing", "unknown"]), ownership: z.enum(["verified-owned", "unknown"]),
    references: z.enum(["verified-unshared", "shared", "unknown"]),
    lost: z.boolean(), physicalId: z.string().nullable(), linkCount: count.nullable(),
    mtimeNs: z.string().nullable(), ctimeNs: z.string().nullable(),
    allocatedBytes: count.nullable(), verifiedReclaimableBytes: count.nullable(),
  }).strict()).max(TV_RETENTION_MAX_FILES),
  seeding: z.object({ state: z.enum(["verified-independent-seed-store", "last-copy-or-unknown"]),
    obligations: z.array(z.object({ id: z.string(), remainingSeedableCopies: count.nullable(),
      verifiedSecondsSeeded: count.nullable(), requiredSecondsSeeded: count.nullable(),
      otherRequirements: z.literal("unknown") }).strict()).max(20),
  }).strict(),
  verifiedReclaimableBytes: count.nullable(), planReclaimableBytes: count,
  blockers: z.array(blocker), executionBlockers: z.array(blocker),
}).strict();
export type TvRetentionAudit = z.output<typeof tvRetentionAuditSchema>;
export const tvRetentionPreviewSchema = z.object({ mode: z.literal("audit-only"), removalSupported: z.boolean(),
  resources: z.array(tvRetentionAuditSchema).max(TV_RETENTION_MAX_SEASONS) }).strict();

export type TvRetentionInventory = {
  target: { id: number; entryId: number; revision: number } | null;
  work: { key: string; title: string; tmdbId: number | null; tvdbId: number | null } | null;
  episodes: { id: number; key: string; airDate: string | null }[];
  requests: { id: number; targetId: number; createdAt: string; withdrawnAt: string | null; episodeIds: number[] }[];
  selectedEpisodeIds: number[];
  files: { id: string; relativePath: string; sizeBytes: number; episodeIds: number[];
    references: "verified-unshared" | "shared" | "unknown"; lost: boolean }[];
  acquisitions: { id: number; jobId: number | null; finishedAt: string | null }[];
  publications: number[];
  downloads: { id: number; clientId: string; nativeId: string; locationJson: string; fileIds: string[] }[];
  lossInventoryKnown: boolean; overflow: boolean;
  lastRequestedAt: string | null;
  allSelectedEpisodeIds: number[]; allRequestedEpisodeIds: number[];
};

export const tvRetentionEvictParamsSchema = tvRetentionSeasonSchema.extend({
  expectedRevision: z.string().regex(/^[a-f0-9]{64}$/), operationId: z.string().uuid(), reason: z.string().trim().min(1).max(500),
}).strict();
export const tvRetentionEvictResultSchema = z.object({
  status: z.enum(["queued", "blocked"]), evictionId: z.string().uuid().nullable(), jobId: id.nullable(), blockers: z.array(blocker),
}).strict();
export const tvRetentionSeedProofSchema = z.object({ fileId: z.string().uuid(), bindingId: id, memberName: z.string(),
  sha256: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
export type TvRetentionSeedProof = z.output<typeof tvRetentionSeedProofSchema>;
export const tvRetentionPlanSchema = z.object({
  id: z.string().uuid(), commandHash: z.string().regex(/^[a-f0-9]{64}$/), scope: tvRetentionSeasonSchema,
  reason: z.string().min(1).max(500), settingsHash: z.string().regex(/^[a-f0-9]{64}$/), demandHash: z.string().regex(/^[a-f0-9]{64}$/),
  expectedTargetRevision: count, retiredRevision: count, expectedIntentRevision: count, intentRevision: count,
  contentId: z.string().uuid(), device: z.string(), createdAt: z.string(),
  work: z.object({ key: z.string(), title: z.string(), tmdbId: id.nullable(), tvdbId: id.nullable() }).strict(),
  episodes: z.array(z.object({ id, key: z.string(), airDate: z.string().nullable() }).strict()).min(1).max(TV_RETENTION_MAX_EPISODES),
  selected: z.array(z.object({ episodeId: id, versionId: id }).strict()).min(1).max(TV_RETENTION_MAX_EPISODES),
  files: z.array(retentionFilePlanSchema).min(1).max(TV_RETENTION_MAX_FILES),
  views: z.array(z.object({ relativePath: z.string(), contentRelativePath: z.string() }).strict()).min(1).max(TV_RETENTION_MAX_FILES),
  downloads: z.array(z.object({ id, clientId: z.string(), nativeId: z.string(), locationJson: z.string(), fileIds: z.array(z.string()) }).strict()).min(1).max(20),
  seedProofs: z.array(tvRetentionSeedProofSchema).min(1).max(400),
}).strict();
export type TvRetentionPlan = z.output<typeof tvRetentionPlanSchema>;
export type TvRetentionEviction = { plan: TvRetentionPlan; jobId: number; finishedAt: string | null; result: unknown };

