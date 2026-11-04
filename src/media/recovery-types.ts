import { z } from "zod";

export const RECOVERY_BATCH_LIMIT = 20;
export const RECOVERY_ROW_LIMIT = 5000;
export const RECOVERY_FILE_LIMIT = 500;
export const RECOVERY_OPERATION_LIMIT = 256;
const id = z.string().regex(/^[1-9]\d*$/).refine((value) => Number.isSafeInteger(Number(value)), "Unsafe ID");
const count = z.number().int().nonnegative().safe();
export const recoveryKeySchema = z.object({ namespace: z.enum(["movie-target", "tv-target"]), id }).strict();
export type RecoveryKey = z.infer<typeof recoveryKeySchema>;
const unique = <T extends { namespace: string; id: string }>(keys: T[]) => new Set(keys.map((key) => `${key.namespace}:${key.id}`)).size === keys.length;
export const recoveryInventoryParamsSchema = z.object({
  namespace: recoveryKeySchema.shape.namespace,
  afterTargetId: count.default(0), limit: z.number().int().min(1).max(RECOVERY_BATCH_LIMIT).default(20),
}).strict();
export const recoveryPreviewParamsSchema = z.object({
  targets: z.array(recoveryKeySchema).min(1).max(RECOVERY_BATCH_LIMIT).refine(unique, "Duplicate target"),
}).strict();
const revision = z.string().regex(/^[a-f0-9]{64}$/);
const commandAction = z.enum(["repair-view", "queue-acquisition"]);
export const recoveryRecoverParamsSchema = z.object({
  operationId: z.string().uuid(),
  targets: z.array(z.object({ key: recoveryKeySchema, expectedRevision: revision, action: commandAction }).strict())
    .min(1).max(RECOVERY_BATCH_LIMIT).refine((targets) => unique(targets.map((target) => target.key)), "Duplicate target"),
}).strict();
export const recoveryReasonSchema = z.object({ code: z.string(), message: z.string() }).strict();
const requestKey = z.object({ namespace: z.enum(["movie-request", "tv-request"]), id, active: z.boolean() }).strict();
const fileSchema = z.object({
  id: z.string(), relativePath: z.string(), sizeBytes: count,
  presence: z.enum(["present", "missing", "unknown"]),
  selected: z.boolean(),
}).strict();
export const recoveryResourceSchema = z.object({
  key: recoveryKeySchema, title: z.string(), revision,
  targetRevision: count.nullable(), intentRevision: count.nullable(),
  desiredPresence: z.enum(["present", "evicted", "unknown"]),
  requestIds: z.array(requestKey),
  mount: z.object({ state: z.enum(["verified", "unavailable"]), contentId: z.string().uuid().nullable(), reason: z.string() }).strict(),
  canonical: z.object({ state: z.enum(["present", "missing", "unknown", "not-selected"]), files: z.array(fileSchema) }).strict(),
  view: z.object({ state: z.enum(["present", "missing", "conflict", "unknown", "not-selected"]),
    files: z.array(z.object({ relativePath: z.string(), presence: z.enum(["present", "missing", "conflict", "unknown"]) }).strict()),
  }).strict(),
  activeAcquisitions: z.array(z.object({ id: count, jobId: count.nullable(), status: z.string() }).strict()),
  action: z.enum(["none", "repair-view", "queue-acquisition", "blocked"]),
  reasons: z.array(recoveryReasonSchema),
}).strict();
export type RecoveryResource = z.infer<typeof recoveryResourceSchema>;
export const recoveryPreviewSchema = z.object({ resources: z.array(recoveryResourceSchema).max(RECOVERY_BATCH_LIMIT) }).strict();
export const recoveryInventorySchema = recoveryPreviewSchema.extend({ nextAfterTargetId: count.nullable() }).strict();
export const recoveryResultSchema = z.object({
  key: recoveryKeySchema, status: z.enum(["repaired", "queued", "unchanged", "blocked"]),
  revision, jobIds: z.array(z.number().int().positive().safe()),
  reasons: z.array(recoveryReasonSchema),
}).strict();
export type RecoveryResult = z.infer<typeof recoveryResultSchema>;
export const recoveryRecoverSchema = z.object({ operationId: z.string().uuid(), results: z.array(recoveryResultSchema).max(RECOVERY_BATCH_LIMIT) }).strict();
