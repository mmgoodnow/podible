import { randomUUID } from "node:crypto";
import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { positiveIntSchema } from "./schemas";
import { tvShadowCaseSchema, tvShadowStatusSchema } from "../tv/shadow-types";

const caseParams = z.object({ caseId: positiveIntSchema }).strict();
const emptyParamsSchema = z.object({}).strict();
export const tvShadowRouter = defineRouter({
  status: defineMethod({
    auth: "admin", readOnly: true, summary: "Read TV audit configuration and last observation execution, without credentials.",
    paramsSchema: emptyParamsSchema, resultSchema: tvShadowStatusSchema,
    handler(ctx) {
      const settings = ctx.repo.getSettings(), tv = settings.tv;
      const job = ctx.repo.latestJobByType("tv_shadow_sync");
      return {
        sonarrConfigured: !!tv.sonarrUrl.trim() && !!tv.sonarrApiKey.trim(),
        agentConfigured: !!settings.agents.apiKey.trim(), pathMappingCount: tv.pathMappings.length,
        pollIntervalMinutes: tv.pollIntervalMinutes, lookbackDays: tv.lookbackDays, maxCasesPerSync: tv.maxCasesPerSync,
        lastSync: job ? { jobId: job.id, status: job.status, createdAt: job.created_at,
          error: job.error ? "TV observation failed; check the configured connection and server diagnostics" : null } : null,
      };
    },
  }),
  list: defineMethod({
    auth: "admin", readOnly: true, summary: "List immutable independent TV audit cases.",
    paramsSchema: z.object({ cursor: positiveIntSchema.optional(), limit: z.number().int().min(1).max(100).default(25), includeHistory: z.boolean().default(false) }).strict(),
    resultSchema: z.object({ items: z.array(tvShadowCaseSchema), nextCursor: positiveIntSchema.nullable() }),
    handler(ctx, p) {
      const items = ctx.repo.tvShadow.list(p.cursor, p.limit, p.includeHistory);
      return { items, nextCursor: items.length === p.limit ? items.at(-1)!.id : null };
    },
  }),
  get: defineMethod({
    auth: "admin", readOnly: true, summary: "Read TV evidence, episode mapping and independent probe-derived quality.",
    paramsSchema: caseParams, resultSchema: z.object({ case: tvShadowCaseSchema }),
    handler(ctx, p) {
      const row = ctx.repo.tvShadow.get(p.caseId);
      if (!row) throw new RpcError(-32602, "TV audit case not found");
      return { case: row };
    },
  }),
  sync: defineMethod({
    auth: "admin", summary: "Queue GET-only Sonarr observation and read-only TV evidence capture.",
    paramsSchema: emptyParamsSchema, resultSchema: z.object({ jobId: positiveIntSchema }),
    handler(ctx) {
      const tv = ctx.repo.getSettings().tv;
      if (!tv.sonarrUrl.trim() || !tv.sonarrApiKey.trim()) throw new RpcError(-32602, "Configure the server Sonarr connection first");
      return { jobId: ctx.repo.createJob({ type: "tv_shadow_sync", idempotencyKey: `tv-shadow-manual:${randomUUID()}`, maxAttempts: 3 }).id };
    },
  }),
  retry: defineMethod({
    auth: "admin", summary: "Retry a failed TV audit without modifying upstream media.",
    paramsSchema: caseParams, resultSchema: z.object({ jobId: positiveIntSchema }),
    handler(ctx, p) {
      const row = ctx.repo.tvShadow.get(p.caseId);
      if (!row?.jobId || row.result || !["failed", "cancelled"].includes(row.status))
        throw new RpcError(-32602, "Case has no failed unfinished evaluation to retry");
      return { jobId: ctx.repo.retryJob(row.jobId).id };
    },
  }),
});
