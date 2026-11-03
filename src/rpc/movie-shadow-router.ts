import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { emptyParamsSchema, positiveIntSchema } from "./schemas";
import { shadowCaseSchema, shadowReviewSchema } from "../movies/shadow-types";
const caseParams = z.object({ caseId: positiveIntSchema });
export const movieShadowRouter = defineRouter({
  status: defineMethod({
    auth: "admin",
    readOnly: true,
    summary:
      "Read shadow configuration and latest sync; never returns API keys.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      enabled: z.boolean(),
      radarrUrl: z.string(),
      keyConfigured: z.boolean(),
      pollIntervalMinutes: z.number(),
      lookbackDays: z.number(),
      maxCasesPerSync: z.number(),
      pathMappings: z.array(
        z.object({ remoteRoot: z.string(), localRoot: z.string() }),
      ),
      lastSync: z
        .object({
          jobId: z.number(),
          status: z.string(),
          error: z.string().nullable(),
          createdAt: z.string(),
        })
        .nullable(),
    }),
    handler(ctx) {
      const { radarrApiKey, ...settings } =
        ctx.repo.getSettings().movies.shadow;
      const job = ctx.repo.latestJobByType("movie_shadow_sync");
      return {
        ...settings,
        keyConfigured: Boolean(radarrApiKey),
        lastSync: job
          ? {
              jobId: job.id,
              status: job.status,
              error: job.error,
              createdAt: job.created_at,
            }
          : null,
      };
    },
  }),
  list: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "List independent Radarr import audit cases.",
    paramsSchema: z.object({
      cursor: positiveIntSchema.optional(),
      limit: z.number().int().min(1).max(100).default(50),
      includeHistory: z.boolean().default(false),
    }),
    resultSchema: z.object({
      items: z.array(shadowCaseSchema),
      nextCursor: z.number().nullable(),
    }),
    handler(ctx, p) {
      const items = ctx.repo.movieShadow.list(p.cursor, p.limit, p.includeHistory);
      return {
        items,
        nextCursor: items.length === p.limit ? items.at(-1)!.id : null,
      };
    },
  }),
  get: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Read shadow evidence, independent decision and human review.",
    paramsSchema: caseParams,
    resultSchema: z.object({ case: shadowCaseSchema }),
    handler(ctx, p) {
      const row = ctx.repo.movieShadow.get(p.caseId);
      if (!row) throw new RpcError(-32602, "Shadow case not found");
      return { case: row };
    },
  }),
  sync: defineMethod({
    auth: "admin",
    summary: "Queue a read-only Radarr observation pass.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({ jobId: z.number() }),
    handler(ctx) {
      if (!ctx.repo.getSettings().movies.shadow.enabled)
        throw new RpcError(-32602, "Shadow mode is disabled");
      return {
        jobId: ctx.repo.createJob({
          type: "movie_shadow_sync",
          idempotencyKey: `shadow-manual:${randomUUID()}`,
          maxAttempts: 3,
        }).id,
      };
    },
  }),
  retry: defineMethod({
    auth: "admin",
    summary: "Retry failed shadow execution without changing external media.",
    paramsSchema: caseParams,
    resultSchema: z.object({ jobId: z.number() }),
    handler(ctx, p) {
      const row = ctx.repo.movieShadow.get(p.caseId);
      if (!row?.jobId || !["failed", "cancelled"].includes(row.status))
        throw new RpcError(-32602, "Case has no failed execution to retry");
      return { jobId: ctx.repo.retryJob(row.jobId).id };
    },
  }),
  review: defineMethod({
    auth: "admin",
    summary: "Record a human verdict on a shadow case.",
    paramsSchema: caseParams.extend({
      verdict: shadowReviewSchema.shape.verdict,
      notes: z.string().max(4000),
    }),
    resultSchema: z.object({ case: shadowCaseSchema }),
    handler(ctx, p) {
      if (!ctx.repo.movieShadow.get(p.caseId))
        throw new RpcError(-32602, "Shadow case not found");
      ctx.repo.movieShadow.review(p.caseId, {
        verdict: p.verdict,
        notes: p.notes,
        userId: ctx.session!.user_id,
        reviewedAt: new Date().toISOString(),
      });
      return { case: ctx.repo.movieShadow.get(p.caseId)! };
    },
  }),
});
