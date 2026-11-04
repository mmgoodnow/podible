import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import {
  RetentionService, RetentionResourceNotFoundError,
  retentionAuditParamsSchema, retentionResourceDetailSchema,
  retentionStatusParamsSchema, retentionStatusSchema,
} from "../media/retention-service";
import { previewRetention, queueMovieEviction, retentionAdapters, type RetentionAdapters } from "../media/retention-policy";
import {
  retentionPreviewParamsSchema, retentionPreviewSchema,
  retentionEvictParamsSchema, retentionEvictResultSchema,
} from "../media/retention-policy-types";

/** Adapters are injected by server composition only, never from RPC parameters. */
export function createRetentionRouter(adapters: RetentionAdapters = {}) {
  return defineRouter({
    status: defineMethod({
      auth: "admin", readOnly: true,
      summary: "Inspect owned movie retention facts; audit-only, without changing lifecycle intent.",
      paramsSchema: retentionStatusParamsSchema, resultSchema: retentionStatusSchema,
      handler: (ctx, params) => new RetentionService(ctx.repo, retentionAdapters(adapters)).status(params),
    }),
    audit: defineMethod({
      auth: "admin", readOnly: true,
      summary: "Audit server-observed canonical movie objects; never deletes or marks a resource evicted.",
      paramsSchema: retentionAuditParamsSchema, resultSchema: retentionResourceDetailSchema,
      async handler(ctx, key) {
        try { return await new RetentionService(ctx.repo, retentionAdapters(adapters)).audit(key); }
        catch (error) {
          if (error instanceof RetentionResourceNotFoundError) throw new RpcError(-32602, "Movie target not found");
          throw error;
        }
      },
    }),
    preview: defineMethod({
      auth: "admin", readOnly: true,
      summary: "Observe disk pressure and preview eligible owned movie evictions without changing intent or files.",
      paramsSchema: retentionPreviewParamsSchema, resultSchema: retentionPreviewSchema,
      handler: (ctx, params) => previewRetention(ctx.repo, undefined, adapters, params),
    }),
    evict: defineMethod({
      auth: "admin",
      summary: "Revalidate a movie eviction and atomically record its intent and durable cleanup job; retain seed data.",
      paramsSchema: retentionEvictParamsSchema, resultSchema: retentionEvictResultSchema,
      handler: (ctx, params) => queueMovieEviction(ctx.repo, params, undefined, adapters),
    }),
  });
}

export const retentionRouter = createRetentionRouter();
