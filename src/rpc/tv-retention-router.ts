import { defineMethod, defineRouter } from "./framework";
import { TvRetentionService, type TvRetentionAdapters } from "../media/tv-retention";
import { tvRetentionAuditSchema, tvRetentionPreviewParamsSchema, tvRetentionPreviewSchema, tvRetentionSeasonSchema,
  tvRetentionEvictParamsSchema, tvRetentionEvictResultSchema } from "../media/tv-retention-types";

export function createTvRetentionRouter(adapters: TvRetentionAdapters = {}) {
  return defineRouter({
    audit: defineMethod({ auth: "admin", readOnly: true, paramsSchema: tvRetentionSeasonSchema, resultSchema: tvRetentionAuditSchema,
      summary: "Read-only whole-season TV retention audit; exact episode identities, global history, owned bytes and seed guards.",
      handler: (ctx, scope) => new TvRetentionService(ctx.repo, adapters).audit(scope) }),
    preview: defineMethod({ auth: "admin", readOnly: true, paramsSchema: tvRetentionPreviewParamsSchema, resultSchema: tvRetentionPreviewSchema,
      summary: "Preview at most three complete TV seasons; never delete files, change intent or queue work.",
      handler: (ctx, scope) => new TvRetentionService(ctx.repo, adapters).preview(scope) }),
    evict: defineMethod({ auth: "admin", paramsSchema: tvRetentionEvictParamsSchema, resultSchema: tvRetentionEvictResultSchema,
      summary: "Explicitly queue a fully verified single-season whole-target eviction; multi-season and partial-season removal is blocked.",
      handler: (ctx, scope) => new TvRetentionService(ctx.repo, adapters).evict(scope) }),
  });
}
export const tvRetentionRouter = createTvRetentionRouter();
