import { defineMethod, defineRouter } from "./framework";
import { transferMetricsStatusParamsSchema, transferMetricsStatusSchema, mapTrackerSchema, trackerMappingsSchema } from "../media/transfer-metrics-types";

export const transferMetricsRouter = defineRouter({
  status: defineMethod({ auth: "admin", readOnly: true,
    summary: "Read saved cross-media transfer, seeding, tracker and reuse statistics; no daemon calls or control commands.",
    paramsSchema: transferMetricsStatusParamsSchema, resultSchema: transferMetricsStatusSchema,
    handler: (ctx, input) => ctx.repo.transferMetrics.status(input),
  }),
  mapTracker: defineMethod({ auth: "admin",
    summary: "Name a tracker hostname without storing announce credentials.",
    paramsSchema: mapTrackerSchema, resultSchema: trackerMappingsSchema,
    handler: (ctx, input) => ({ mappings: ctx.repo.transferMetrics.mapTracker(input) }),
  }),
});
