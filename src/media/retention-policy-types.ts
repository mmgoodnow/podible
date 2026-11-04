import { z } from "zod";

const count = z.number().int().nonnegative().safe();
const id = z.number().int().positive().safe();
export const retentionPolicySchema = z.object({
  minimumFreeBytes: count.default(20 * 1024 ** 3),
  protectedMovieTargetIds: z.array(id).max(1000).default([]),
  protectedTvTargetIds: z.array(id).max(1000).default([]),
}).strict();
export type RetentionPolicy = z.infer<typeof retentionPolicySchema>;
export const DEFAULT_RETENTION_POLICY = retentionPolicySchema.parse({});
export const RETENTION_REQUEST_GRACE_MS = 30 * 86_400_000;
export const RETENTION_MAX_TARGETS = 3;
export const RETENTION_PRESSURE_INTERVAL_MS = 5 * 60_000;
export const retentionPreviewParamsSchema = z.object({ targetId: id.optional(), afterTargetId: count.optional(), limit: id.max(100).optional() }).strict();
export const retentionEvictParamsSchema = z.object({ targetId: id, expectedRevision: id, reason: z.string().trim().min(1).max(1000) }).strict();
export const retentionEvictResultSchema = z.object({ evictionId: z.string().uuid(), jobId: id }).strict();

export const retentionFilePlanSchema = z.object({
  id: z.string().uuid(), relativePath: z.string().min(1), sizeBytes: count,
  device: z.string().regex(/^\d+$/), inode: z.string().regex(/^\d+$/),
  mtimeNs: z.string().regex(/^\d+$/), ctimeNs: z.string().regex(/^\d+$/),
  allocatedBytes: count, reclaimableBytes: count,
}).strict().refine((file) => {
  const extension = /\.(mkv|mp4|m4v|avi|mov|ts)$/.exec(file.relativePath)?.[0];
  return !!extension && file.relativePath === `files/${file.id.slice(0, 2)}/${file.id}${extension}`;
});
export const retentionEvictionPlanSchema = z.object({
  id: z.string().uuid(), targetId: id, mode: z.enum(["operator", "pressure"]), reason: z.string().trim().min(1).max(1000),
  expectedRevision: count, expectedTargetRevision: count, activeVersionId: id,
  intentRevision: id, targetRevision: id,
  settingsHash: z.string().regex(/^[a-f0-9]{64}$/), contentId: z.string().uuid(), device: z.string().regex(/^\d+$/),
  files: z.array(retentionFilePlanSchema).min(1).max(1000), createdAt: z.string().datetime(),
}).strict();
export type RetentionEvictionPlan = z.infer<typeof retentionEvictionPlanSchema>;
export type RetentionFilePlan = z.infer<typeof retentionFilePlanSchema>;
export type RetentionEviction = {
  id: string; jobId: number; plan: RetentionEvictionPlan; finishedAt: string | null; result: unknown;
};
export type RetentionDisk = {
  state: "known" | "unknown"; filesystem: string | null; device: string | null;
  totalBytes: number | null; availableBytes: number | null; observedAt: string; reason: string;
};
export const retentionDiskSchema = z.object({
  state: z.enum(["known", "unknown"]), filesystem: z.string().nullable(), device: z.string().nullable(),
  totalBytes: count.nullable(), availableBytes: count.nullable(), observedAt: z.string().datetime(), reason: z.string(),
}).strict();
export const retentionPreviewSchema = z.object({
  mode: z.literal("pressure-preview"), disk: retentionDiskSchema, pressure: z.boolean().nullable(),
  candidates: z.array(z.object({
    targetId: id, title: z.string(), eligible: z.boolean(), blockers: z.array(z.string()),
    expectedRevision: count.nullable(), expectedTargetRevision: count, reclaimableBytes: count.nullable(),
    playCount: count.nullable(), lastViewedAt: z.string().nullable(),
  }).strict()), nextAfterTargetId: id.nullable(),
}).strict();
