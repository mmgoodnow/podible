import { z } from "zod";

export const MINIMUM_SEED_SECONDS = 30 * 24 * 60 * 60;
export const STEWARDSHIP_INTERVAL_MS = 5 * 60_000;
export const stewardshipSourceSchema = z.enum(["books", "screen"]);
export const stewardshipStatusSourceSchema = z.enum(["all", "books", "screen"]);
export const stewardshipStatusParamsSchema = z.object({
  source: stewardshipStatusSourceSchema.default("all"), offset: z.number().int().nonnegative().default(0),
  limit: z.number().int().min(1).max(50).default(25),
}).strict();
export const stewardshipCollectParamsSchema = z.object({ operationId: z.string().uuid() }).strict();
export const stewardshipObservationSchema = z.object({
  observedAt: z.iso.datetime(), bindingDigest: z.string().regex(/^[a-f0-9]{64}$/),
  ownership: z.enum(["verified-private-binding", "binding-unverified", "missing-record", "engine-unavailable"]),
  state: z.enum(["seeding", "seed-queued", "downloading", "download-queued", "checking", "paused-complete", "paused-incomplete", "unknown"]),
  complete: z.boolean().nullable(), bytesRemaining: z.number().nonnegative().nullable(),
  daemonError: z.enum(["none", "tracker-warning", "tracker-error", "storage-error", "unknown"]),
  trackerState: z.enum(["healthy", "confirmed-error", "confirmed-unregistered", "unknown"]),
  trackerCount: z.number().int().nonnegative(), trackerErrorCount: z.number().int().nonnegative(),
  trackerUnknownCount: z.number().int().nonnegative(),
  trackerSetDigest: z.string().nullable(), announcementTime: z.number().int().nonnegative().nullable(),
  actualSeedSeconds: z.number().int().nonnegative().nullable(), seedCounterRegressed: z.boolean(),
  seedProtection: z.enum(["protected", "minimum-met", "unknown"]),
  remainingSeedSeconds: z.number().int().nonnegative().nullable(),
  consistentUnregisteredAnnouncements: z.number().int().nonnegative(),
  repeatedUnregistered: z.boolean(), reason: z.string(),
});
export const stewardshipItemSchema = z.object({
  key: z.string(), media: z.enum(["books", "movies", "tv"]), label: z.string(),
  titles: z.array(z.object({ title: z.string(), href: z.string() })).max(3).default([]), titlesTruncated: z.boolean().default(false),
  technicalLabel: z.string().default(""),
  observation: stewardshipObservationSchema.nullable(), stale: z.boolean(),
  retirementAllowed: z.literal(false), retirementBlockers: z.array(z.string()),
});
export const stewardshipCollectResultSchema = z.object({
  operationId: z.string().uuid(), collectedAt: z.iso.datetime(), source: stewardshipSourceSchema,
  offset: z.number().int().nonnegative(), observedCount: z.number().int().nonnegative(),
  excludedCount: z.number().int().nonnegative(), unavailableReason: z.string().nullable(),
});
export const stewardshipStatusSchema = z.object({
  mode: z.literal("observe-only"), destructiveActionsEnabled: z.literal(false),
  minimumSeedSeconds: z.literal(MINIMUM_SEED_SECONDS), source: stewardshipStatusSourceSchema,
  offset: z.number().int().nonnegative(), sourceRows: z.number().int().nonnegative(),
  scannedCount: z.number().int().nonnegative(), excludedCount: z.number().int().nonnegative(),
  nextOffset: z.number().int().nonnegative().nullable(), engineConfigured: z.boolean(),
  unavailableReason: z.string().nullable(), lastCollection: stewardshipCollectResultSchema.nullable(),
  items: z.array(stewardshipItemSchema),
});
export type StewardshipSource = z.infer<typeof stewardshipSourceSchema>;
export type StewardshipStatusSource = z.infer<typeof stewardshipStatusSourceSchema>;
export type StewardshipObservation = z.infer<typeof stewardshipObservationSchema>;
export type StewardshipStatus = z.infer<typeof stewardshipStatusSchema>;
export type StewardshipCollectResult = z.infer<typeof stewardshipCollectResultSchema>;
export type StewardshipPanelApi = {
  status(input: z.input<typeof stewardshipStatusParamsSchema>): Promise<StewardshipStatus>;
  collect(input: z.input<typeof stewardshipCollectParamsSchema>): Promise<StewardshipCollectResult>;
};
