import { defineMethod, defineRouter } from "./framework";
import { transferMetricsStatusParamsSchema, transferMetricsStatusSchema } from "../media/transfer-metrics-types";
import { z } from "zod";
import { libraryInventorySchema, readLibraryInventory } from "../media/library-inventory";

export const transferMetricsRouter = defineRouter({
  status: defineMethod({ auth: "admin", readOnly: true,
    summary: "Read saved cross-media transfer, seeding, tracker and reuse statistics; no daemon calls or control commands.",
    paramsSchema: transferMetricsStatusParamsSchema, resultSchema: transferMetricsStatusSchema,
    handler: (ctx, input) => ctx.repo.transferMetrics.status(input),
  }),
  library: defineMethod({ auth: "admin", readOnly: true,
    summary: "Inspect published managed library counts and canonical file allocation; no tree scans or daemon calls.",
    paramsSchema: z.object({}).strict(), resultSchema: libraryInventorySchema,
    handler: ctx => readLibraryInventory(ctx.repo),
  }),
});
