import { defineMethod, defineRouter } from "./framework";
import { countMapSchema, emptyParamsSchema } from "./schemas";
import { z } from "zod";

export const systemRouter = defineRouter({
  health: defineMethod({
    auth: "public",
    readOnly: true,
    summary: "Service health summary (job/release counts and queue size).",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      ok: z.literal(true),
      jobs: countMapSchema,
      releases: countMapSchema,
      queueSize: z.number().int().nonnegative(),
    }),
    async handler(ctx) {
      return {
        ok: true,
        ...ctx.repo.getHealthSummary(),
      };
    },
  }),

  server: defineMethod({
    auth: "public",
    readOnly: true,
    summary: "Server runtime metadata (name, runtime, uptime, time).",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      name: z.literal("podible-backend"),
      runtime: z.literal("bun"),
      uptimeMs: z.number().nonnegative(),
      now: z.string(),
    }),
    async handler(ctx) {
      return {
        name: "podible-backend",
        runtime: "bun",
        uptimeMs: Date.now() - ctx.startTime,
        now: new Date().toISOString(),
      };
    },
  }),
});
