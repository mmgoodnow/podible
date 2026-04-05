import { searchOpenLibrary } from "../openlibrary";

import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema, limitSchema, nonEmptyStringSchema } from "./schemas";

export const openLibraryRouter = defineRouter({
  openlibrary: defineRouter({
    search: defineMethod({
      auth: "user",
      readOnly: true,
      summary: "Search Open Library for works to add.",
      paramsSchema: emptyParamsSchema.extend({
        q: nonEmptyStringSchema,
        limit: limitSchema.optional(),
      }),
      async handler(_ctx, params) {
        const results = await searchOpenLibrary(params.q.trim(), Math.min(params.limit ?? 50, 50));
        return { results };
      },
    }),
  }),
});
