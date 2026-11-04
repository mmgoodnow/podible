import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema } from "./schemas";
import { RpcError } from "./shared";
import { INDEXER_FEED_INTERVAL_MS } from "../worker/indexer-feeds";
import { feedGapDiagnosticsSchema, readFeedGapDiagnostics } from "../media/feed-gap";

export const indexerFeedsRouter = defineRouter({
  status: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Inspect recent-release feed coverage and safe provider health.",
    paramsSchema: emptyParamsSchema.strict(),
    resultSchema: z.object({
      configured: z.boolean(),
      intervalMinutes: z.number(),
      lastSync: z
        .object({
          jobId: z.number(),
          status: z.string(),
          error: z.string().nullable(),
        })
        .nullable(),
      lastMatch: z
        .object({
          jobId: z.number(),
          status: z.string(),
          error: z.string().nullable(),
        })
        .nullable(),
      lastBookMatch: z
        .object({
          jobId: z.number(),
          status: z.string(),
          error: z.string().nullable(),
        })
        .nullable(),
      feeds: z.array(
        z.object({
          id: z.string(),
          provider: z.string(),
          mediaType: z.string(),
          lastPolledAt: z.string().nullable(),
          lastSuccessAt: z.string().nullable(),
          error: z.string().nullable(),
          retryAt: z.string().nullable(),
          gapSince: z.string().nullable(),
          gapNote: z.string().nullable(),
          gapDiagnostics: feedGapDiagnosticsSchema.nullable(),
        }),
      ),
    }),
    handler(ctx) {
      const j = ctx.repo.latestJobByType("indexer_feed_sync");
      const m = ctx.repo.latestJobByType("movie_feed_match");
      const b = ctx.repo.latestJobByType("book_feed_match");
      const settings = ctx.repo.getSettings();
      return {
        configured: Boolean(settings.prowlarr.url || settings.torznab.length),
        intervalMinutes: INDEXER_FEED_INTERVAL_MS / 60_000,
        lastSync: j
          ? {
              jobId: j.id,
              status: j.status,
              error: j.error
                ? "Feed sync failed; inspect the operation log."
                : null,
            }
          : null,
        lastMatch: m
          ? {
              jobId: m.id,
              status: m.status,
              error: m.error
                ? "Release matching failed; inspect or retry the operation."
                : null,
            }
          : null,
        lastBookMatch: b
          ? {
              jobId: b.id,
              status: b.status,
              error: b.error
                ? "Book release matching failed; inspect or retry the operation."
                : null,
            }
          : null,
        feeds: ctx.repo.releaseInbox.feeds().map((f) => ({
          id: f.id,
          provider: f.provider,
          mediaType: f.media_type,
          lastPolledAt: f.last_polled_at,
          lastSuccessAt: f.last_success_at,
          error: f.last_error,
          retryAt: f.retry_at,
          gapSince: f.gap_since,
          gapNote: f.gap_note,
          gapDiagnostics: readFeedGapDiagnostics(f.gap_diagnostics_json),
        })),
      };
    },
  }),
  sync: defineMethod({
    auth: "admin",
    summary:
      "Poll configured book and movie RSS feeds without any per-title searches.",
    paramsSchema: emptyParamsSchema.strict(),
    resultSchema: z.object({ jobId: z.number() }),
    handler(ctx) {
      const settings = ctx.repo.getSettings();
      if (!settings.prowlarr.url && !settings.torznab.length)
        throw new RpcError(
          -32602,
          "Configure Prowlarr or Torznab sources before polling indexer feeds",
        );
      return {
        jobId: ctx.repo.createJob({
          type: "indexer_feed_sync",
          idempotencyKey: "rss-manual:" + randomUUID(),
          maxAttempts: 3,
        }).id,
      };
    },
  }),
});
