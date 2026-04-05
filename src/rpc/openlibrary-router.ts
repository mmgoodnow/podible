import { searchOpenLibrary } from "../library/openlibrary";

import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema, limitSchema, nonEmptyStringSchema, openLibraryCandidateSchema } from "./schemas";
import { z } from "zod";

export const openLibraryRouter = defineRouter({
  search: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Search Open Library for works to add.",
    paramsSchema: emptyParamsSchema.extend({
      q: nonEmptyStringSchema,
      limit: limitSchema.optional(),
    }),
    resultSchema: z.object({
      results: z.array(openLibraryCandidateSchema),
    }),
    async handler(_ctx, params) {
      const results = await searchOpenLibrary(params.q.trim(), Math.min(params.limit ?? 50, 50));
      return { results };
    },
  }),
});
