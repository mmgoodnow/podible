import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import {
  RetentionService, RetentionResourceNotFoundError,
  retentionAuditParamsSchema, retentionResourceDetailSchema,
  retentionStatusParamsSchema, retentionStatusSchema,
  type RetentionServiceAdapters,
} from "../media/retention-service";

/** Adapters are injected by server composition only, never from RPC parameters. */
export function createRetentionRouter(adapters: RetentionServiceAdapters = {}) {
  return defineRouter({
    status: defineMethod({
      auth: "admin", readOnly: true,
      summary: "Inspect owned movie retention facts; audit-only, without changing lifecycle intent.",
      paramsSchema: retentionStatusParamsSchema, resultSchema: retentionStatusSchema,
      handler: (ctx, params) => new RetentionService(ctx.repo, adapters).status(params),
    }),
    audit: defineMethod({
      auth: "admin", readOnly: true,
      summary: "Audit server-observed canonical movie objects; never deletes or marks a resource evicted.",
      paramsSchema: retentionAuditParamsSchema, resultSchema: retentionResourceDetailSchema,
      async handler(ctx, key) {
        try { return await new RetentionService(ctx.repo, adapters).audit(key); }
        catch (error) {
          if (error instanceof RetentionResourceNotFoundError) throw new RpcError(-32602, "Movie target not found");
          throw error;
        }
      },
    }),
  });
}

export const retentionRouter = createRetentionRouter();
