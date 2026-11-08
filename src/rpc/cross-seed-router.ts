import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { crossSeedCommandSchema } from "../media/cross-seed-state";
import { confirmedReleaseNukeSchema, reportConfirmedReleaseNuke } from "../media/release-health";
import { crossSeedPlan, queueCrossSeed, crossSeedOffer } from "../media/cross-seed";
import { fetchMovieMetainfo } from "../movies/offers";
import { startCrossSeedBackfill, crossSeedBackfillStatus, backfillStartSchema, backfillSummarySchema,
  backfillStatusParamsSchema, backfillStatusSchema, latestCrossSeedBackfill, cancelCrossSeedBackfill } from "../media/cross-seed-backfill";

const outcome = z.object({ kind: z.enum(["seeding", "skipped", "unsupported", "stalled", "incompatible"]), reason: z.string(), at: z.string(),
  planId: z.string().optional(), verification: z.enum(["assumed", "verified"]).optional(),
  torrentId: z.number().optional(), infoHash: z.string().optional(), reusedBytes: z.number().optional(), downloadedBytes: z.number().optional(), totalBytes: z.number().optional(),
  accountingVersion: z.literal(1).optional(), sidecarDownloadBytes: z.number().nonnegative().optional(),
  contentRepairBytes: z.number().nonnegative().optional(), contentFileCount: z.number().int().nonnegative().optional() });
const receipt = z.object({ command: crossSeedCommandSchema, commandHash: z.string(), jobId: z.number(), createdAt: z.string(), outcome: outcome.optional() });

export const crossSeedRouter = defineRouter({
  startBackfill: defineMethod({ auth: "admin", summary: "Explicitly snapshot and search the retained library for cross-seed candidates.",
    paramsSchema: backfillStartSchema, resultSchema: backfillSummarySchema,
    handler: (ctx, input) => startCrossSeedBackfill(ctx.repo, input) }),
  backfillStatus: defineMethod({ auth: "admin", readOnly: true, summary: "Read paginated cross-seed backfill progress without polling providers or torrents.",
    paramsSchema: backfillStatusParamsSchema, resultSchema: backfillStatusSchema,
    handler: (ctx, input) => crossSeedBackfillStatus(ctx.repo, input) }),
  backfillLatest: defineMethod({ auth: "admin", readOnly: true, summary: "Read the latest or active explicit cross-seed backfill.",
    paramsSchema: z.object({}).strict(), resultSchema: z.object({ backfill: backfillSummarySchema.nullable() }),
    handler: ctx => latestCrossSeedBackfill(ctx.repo) }),
  cancelBackfill: defineMethod({ auth: "admin", summary: "Cancel one explicit cross-seed backfill without cancelling its already queued child attempts.",
    paramsSchema: z.object({ requestId: z.string().uuid() }).strict(), resultSchema: backfillSummarySchema,
    handler: (ctx, input) => cancelCrossSeedBackfill(ctx.repo, input.requestId) }),
  reportNuked: defineMethod({ auth: "admin", summary: "Record a confirmed release defect and queue replacement without withdrawing the playable version.",
    paramsSchema: confirmedReleaseNukeSchema,
    resultSchema: z.object({ commandHash: z.string(), evidence: z.object({ source: z.object({ id: z.number() }).passthrough() }).passthrough(),
      replacements: z.array(z.object({ targetId: z.number(), acquisitionId: z.number(), jobId: z.number(), shared: z.boolean() })), idempotent: z.boolean() }),
    handler: (ctx, input) => reportConfirmedReleaseNuke(ctx.repo, input, ctx.session!.user_id) }),
  preview: defineMethod({ auth: "admin", readOnly: true,
    summary: "Estimate reuse of one retained release against registered target files. No layout, torrent or library writes.",
    paramsSchema: crossSeedCommandSchema, resultSchema: z.object({ candidateHash: z.string().optional(), status: z.string(),
      likelyReuseBytes: z.number(), totalBytes: z.number(), reasons: z.array(z.string()), verification: z.string() }),
    handler: async (ctx, command) => {
      const bytes = await (ctx.movieServices?.fetchMetainfo ?? fetchMovieMetainfo)(crossSeedOffer(ctx.repo, command));
      const plan = await crossSeedPlan(ctx.repo, ctx.repo.getSettings(), command, bytes);
      return { candidateHash: plan.candidate.identity.v1, status: plan.status, ...plan.summary, reasons: plan.reasons, verification: plan.verification };
    },
  }),
  create: defineMethod({ auth: "admin", summary: "Queue an optimistic seed-only reuse attempt without bulk hashing or changing Plex selection.",
    paramsSchema: crossSeedCommandSchema, resultSchema: receipt,
    handler: (ctx, command) => queueCrossSeed(ctx.repo, command) }),
  status: defineMethod({ auth: "admin", readOnly: true, summary: "Read bounded reuse intents and outcomes; never polls or controls the daemon.",
    paramsSchema: z.object({ operationId: z.string().uuid().optional(), limit: z.number().int().min(1).max(100).default(25) }).strict(),
    resultSchema: z.object({ operations: z.array(receipt) }),
    handler: (ctx, input) => ({ operations: input.operationId ? [ctx.repo.crossSeed.receipt(input.operationId)].filter((r): r is NonNullable<typeof r> => !!r) : ctx.repo.crossSeed.receipts(input.limit) }) }),
});
