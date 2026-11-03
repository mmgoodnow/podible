import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema } from "./schemas";
import { movieRequestViewSchema } from "../movies/contracts";
import { movieRequestView } from "../movies/service";
import { seerrInstance } from "../movies/seerr";
import { dualWritePayloadSchema } from "../movies/dual-write";
import { RpcError, type RpcContext } from "./shared";
function queue(ctx: RpcContext, payload = {}) {
  const seerr = ctx.repo.getSettings().movies.seerr;
  if (!seerr.url || !seerr.apiKey)
    throw new RpcError(
      -32602,
      "Configure the Seerr request feed URL and API key first",
    );
  return {
    jobId: ctx.repo.createJob({
      type: "movie_dual_write_sync",
      payload,
      idempotencyKey: `seerr-manual:${randomUUID()}`,
      maxAttempts: 3,
    }).id,
  };
}
export const movieDualWriteRouter = defineRouter({
  status: defineMethod({
    auth: "admin",
    readOnly: true,
    summary:
      "Inspect the independent Seerr acquisition feed without credentials.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      url: z.string(),
      keyConfigured: z.boolean(),
      pollIntervalMinutes: z.number(),
      baselineId: z.number().nullable(),
      activatedAt: z.string().nullable(),
      plexSectionId: z.string(),
      lastSync: z
        .object({
          jobId: z.number(),
          status: z.string(),
          error: z.string().nullable(),
        })
        .nullable(),
    }),
    handler(ctx) {
      const s = ctx.repo.getSettings().movies,
        f = s.seerr.url
          ? ctx.repo.movieSeerr.feed(seerrInstance(s.seerr.url))
          : null,
        j = ctx.repo.latestJobByType("movie_dual_write_sync");
      return {
        url: s.seerr.url,
        keyConfigured: Boolean(s.seerr.apiKey),
        pollIntervalMinutes: s.seerr.pollIntervalMinutes,
        baselineId: f?.baselineId ?? null,
        activatedAt: f?.createdAt ?? null,
        plexSectionId: s.plexSectionId,
        lastSync: j
          ? {
              jobId: j.id,
              status: j.status,
              error: j.error
                ? "Seerr sync failed; administrators can inspect the operation log."
                : null,
            }
          : null,
      };
    },
  }),
  list: defineMethod({
    auth: "admin",
    readOnly: true,
    summary:
      "Read actual independent acquisitions and their request origin/progress.",
    paramsSchema: z.object({
      limit: z.number().int().min(1).max(100).default(50),
    }),
    resultSchema: z.object({
      items: z.array(
        z.object({
          externalRequestId: z.number(),
          requester: z.string(),
          createdAt: z.string(),
          request: movieRequestViewSchema,
        }),
      ),
    }),
    handler(ctx, p) {
      const url = ctx.repo.getSettings().movies.seerr.url;
      return {
        items: (url
          ? ctx.repo.movieSeerr.list(p.limit, seerrInstance(url))
          : []
        ).map((r) => ({
          externalRequestId: r.externalRequestId,
          requester:
            r.source.requestedBy.displayName ??
            r.source.requestedBy.username ??
            `Seerr user ${r.source.requestedBy.id}`,
          createdAt: r.createdAt,
          request: movieRequestView(ctx.repo, r.requestId),
        })),
      };
    },
  }),
  sync: defineMethod({
    auth: "admin",
    summary:
      "Consume new approved Seerr requests; first activation records a no-backfill baseline.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({ jobId: z.number() }),
    handler(ctx) {
      return queue(ctx);
    },
  }),
  backfill: defineMethod({
    auth: "admin",
    summary:
      "Explicitly backfill at most five approved movie request IDs into independent acquisition.",
    paramsSchema: dualWritePayloadSchema.required(),
    resultSchema: z.object({ jobId: z.number() }),
    handler(ctx, p) {
      return queue(ctx, p);
    },
  }),
});
