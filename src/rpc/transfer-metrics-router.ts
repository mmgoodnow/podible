import { defineMethod, defineRouter } from "./framework";
import { transferMetricsStatusParamsSchema, transferMetricsStatusSchema } from "../media/transfer-metrics-types";
import { z } from "zod";
import { libraryInventorySchema, readLibraryInventory } from "../media/library-inventory";

const count = z.number().int().nonnegative();
const publishedCountsSchema = z.object({ books: count, ebooks: count, audiobooks: count, movies: count,
  shows: count, seasons: count, episodes: count });

export const transferMetricsRouter = defineRouter({
  published: defineMethod({ auth: "admin", readOnly: true,
    summary: "Count published library records without measuring files or contacting the download client.",
    paramsSchema: z.object({}).strict(), resultSchema: publishedCountsSchema,
    handler: ctx => publishedCountsSchema.parse(ctx.repo.transferMetrics.publishedCounts()),
  }),
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
