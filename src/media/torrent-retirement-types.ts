import { z } from "zod";

const digest = z.string().regex(/^[a-f0-9]{64}$/);
const count = z.number().int().nonnegative().safe();
export const retirementPreviewParamsSchema = z.object({
  source: z.enum(["books", "screen"]).default("books"), offset: count.default(0),
  limit: z.number().int().min(1).max(20).default(10),
}).strict();
export const retirementGetParamsSchema = z.object({ operationId: z.string().uuid() }).strict();
export const retirementSampleSchema = z.object({
  observedAt: z.iso.datetime(), actualSeedSeconds: count,
  trackers: z.array(z.object({ key: digest, announcedAt: count })).min(1).max(64),
});
export const retirementEvidenceSchema = z.object({
  bindingKey: digest, bindingDigest: digest, observedAt: z.iso.datetime(),
  trackerSetDigest: digest.nullable(), complete: z.boolean(), actualSeedSeconds: count.nullable(),
  allTrackersExplicitlyUnregistered: z.boolean(), samples: z.array(retirementSampleSchema).max(3),
});
export type RetirementEvidence = z.infer<typeof retirementEvidenceSchema>;
export const retirementIntentSchema = z.object({
  operationId: z.string().uuid(), source: z.enum(["books", "screen"]),
  bindingKey: digest, bindingDigest: digest, clientId: z.string(), infoHash: z.string().regex(/^[a-f0-9]{40}$/),
  phase: z.enum(["planned", "dispatching", "uncertain", "retired", "aborted"]),
  createdAt: z.iso.datetime(), updatedAt: z.iso.datetime(),
  daemonSessionDigest: digest, daemonIdentityDigest: digest, daemonRecordId: z.number().int().positive().safe(),
  proofDigest: digest, consumerRevision: z.string(), configurationDigest: digest,
  fenceId: z.string().uuid(), fenceGeneration: count, reason: z.string(),
  jobId: z.number().int().positive().safe(), executionKey: z.string().min(1),
});
export type RetirementIntent = z.infer<typeof retirementIntentSchema>;
export const retirementIntentViewSchema = retirementIntentSchema.pick({
  operationId: true, source: true, bindingKey: true, phase: true, createdAt: true, updatedAt: true, reason: true,
}).extend({ recordOnly: z.literal(true), filesDeleted: z.literal(false) });
export const retirementPreviewSchema = z.object({
  automaticConfigured: z.boolean(), recordOnly: z.literal(true), filesDeleted: z.literal(false),
  source: z.enum(["books", "screen"]), offset: count, nextOffset: count.nullable(),
  items: z.array(z.object({
    bindingKey: digest, media: z.enum(["books", "movies", "tv"]), label: z.string(),
    evidenceQualified: z.boolean(), eligible: z.boolean(), blockers: z.array(z.string()),
  })),
});
export type RetirementIntentView = z.infer<typeof retirementIntentViewSchema>;

export type RetirementBinding = {
  key: string; media: "books" | "movies" | "tv"; label: string; hash: string;
  clientId: string; directory: string; root: string; rootToken: string | null; bindingDigest: string;
};
export type RetirementConsumerSnapshot = {
  completeInventory: boolean; domainBindings: number; activeConsumers: number; activeAcquisitions: number; activePublications: number;
  bindingDigest: string; configurationDigest: string; revision: string;
};
export type RetirementClaim = { id: string; generation: number };
/** Required integration boundary. Every producer/user of this binding must honor the same durable gate. */
export interface TorrentRetirementFence {
  inspect(binding: RetirementBinding): RetirementConsumerSnapshot;
  claim(binding: RetirementBinding, operationId: string, expected: RetirementConsumerSnapshot): RetirementClaim;
  assert(binding: RetirementBinding, operationId: string, claim: RetirementClaim, expected: RetirementConsumerSnapshot): void;
  finish(binding: RetirementBinding, operationId: string, claim: RetirementClaim, outcome: "retired" | "aborted-before-dispatch"): void;
}
