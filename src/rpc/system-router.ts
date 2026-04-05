import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema } from "./schemas";

export const systemRouter = defineRouter({
  health: defineMethod({
    auth: "public",
    readOnly: true,
    summary: "Service health summary (job/release counts and queue size).",
    paramsSchema: emptyParamsSchema,
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
