import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { crossSeedCommandSchema } from "../media/cross-seed-state";
import { confirmedReleaseNukeSchema, reportConfirmedReleaseNuke } from "../media/release-health";
import { crossSeedPlan, queueCrossSeed } from "../media/cross-seed";
import { fetchMovieMetainfo, movieOfferFromResult } from "../movies/offers";

const outcome = z.object({ kind: z.enum(["seeding", "skipped", "unsupported", "stalled"]), reason: z.string(), at: z.string(),
  torrentId: z.number().optional(), infoHash: z.string().optional(), reusedBytes: z.number().optional(), downloadedBytes: z.number().optional(), totalBytes: z.number().optional() });
const receipt = z.object({ command: crossSeedCommandSchema, commandHash: z.string(), jobId: z.number(), createdAt: z.string(), outcome: outcome.optional() });

export const crossSeedRouter = defineRouter({
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
      const entry = ctx.repo.releaseInbox.entries([command.releaseId])[0];
      if (!entry || entry.result.mediaType !== command.scope.media) throw new Error("Unknown retained candidate");
      const bytes = await (ctx.movieServices?.fetchMetainfo ?? fetchMovieMetainfo)(movieOfferFromResult({ ...entry.result, mediaType: "movie" }));
      const plan = await crossSeedPlan(ctx.repo, ctx.repo.getSettings(), command, bytes);
      return { candidateHash: plan.candidate.identity.v1, status: plan.status, ...plan.summary, reasons: plan.reasons, verification: plan.verification };
    },
  }),
  create: defineMethod({ auth: "admin", summary: "Queue an idempotent verified CoW reuse attempt without changing Plex selection.",
    paramsSchema: crossSeedCommandSchema, resultSchema: receipt,
    handler: (ctx, command) => queueCrossSeed(ctx.repo, command) }),
  status: defineMethod({ auth: "admin", readOnly: true, summary: "Read bounded reuse intents and outcomes; never polls or controls the daemon.",
    paramsSchema: z.object({ operationId: z.string().uuid().optional(), limit: z.number().int().min(1).max(100).default(25) }).strict(),
    resultSchema: z.object({ operations: z.array(receipt) }),
    handler: (ctx, input) => ({ operations: input.operationId ? [ctx.repo.crossSeed.receipt(input.operationId)].filter((r): r is NonNullable<typeof r> => !!r) : ctx.repo.crossSeed.receipts(input.limit) }) }),
});
