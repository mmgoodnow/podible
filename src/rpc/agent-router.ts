import { selectManualImportPaths, selectSearchCandidate } from "../library/agents";
import { inspectImportPath } from "../library/importer";
import { runSearch } from "../library/service";
import { z } from "zod";

import { defineMethod, defineRouter } from "./framework";
import {
  emptyParamsSchema,
  importInspectionFileSchema,
  manualImportSelectionDecisionSchema,
  mediaSchema,
  nonEmptyStringSchema,
  optionalBooleanSchema,
  optionalPositiveIntSchema,
  optionalStringArraySchema,
  searchSelectionDecisionSchema,
} from "./schemas";
import { RpcError } from "./shared";

export const agentRouter = defineRouter({
  search: defineRouter({
    plan: defineMethod({
      auth: "admin",
      readOnly: true,
      summary: "Run search selection planning (deterministic/agent) without snatching.",
      paramsSchema: emptyParamsSchema.extend({
        query: nonEmptyStringSchema,
        media: mediaSchema,
        bookId: optionalPositiveIntSchema,
        forceAgent: optionalBooleanSchema,
        priorFailure: optionalBooleanSchema,
        rejectedUrls: optionalStringArraySchema,
      }),
      resultSchema: z.object({
        resultCount: z.number().int().nonnegative(),
        decision: searchSelectionDecisionSchema,
      }),
      async handler(ctx, params) {
        const book =
          params.bookId === undefined
            ? null
            : (() => {
                const row = ctx.repo.getBookRow(params.bookId);
                if (!row) {
                  throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
                }
                return { id: row.id, title: row.title, author: row.author };
              })();
        const results = await runSearch(ctx.repo.getSettings(), { query: params.query, media: params.media });
        const decision = await selectSearchCandidate(
          ctx.repo.getSettings(),
          {
            query: params.query,
            media: params.media,
            results,
            rejectedUrls: params.rejectedUrls ?? [],
            forceAgent: params.forceAgent ?? false,
            priorFailure: params.priorFailure ?? false,
            book,
          },
          {
            repo: ctx.repo,
          }
        );
        return {
          resultCount: results.length,
          decision,
        };
      },
    }),
  }),

  import: defineRouter({
    plan: defineMethod({
      auth: "admin",
      readOnly: true,
      summary: "Run import-file selection planning (deterministic/agent) without importing.",
      paramsSchema: emptyParamsSchema.extend({
        path: nonEmptyStringSchema,
        mediaType: mediaSchema,
        bookId: optionalPositiveIntSchema,
        forceAgent: optionalBooleanSchema,
        priorFailure: optionalBooleanSchema,
      }),
      resultSchema: z.object({
        path: z.string(),
        fileCount: z.number().int().nonnegative(),
        files: z.array(importInspectionFileSchema),
        decision: manualImportSelectionDecisionSchema,
      }),
      async handler(ctx, params) {
        const book =
          params.bookId === undefined
            ? null
            : (() => {
                const row = ctx.repo.getBookRow(params.bookId);
                if (!row) {
                  throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
                }
                return { id: row.id, title: row.title, author: row.author };
              })();
        const files = await inspectImportPath(params.path.trim());
        const decision = await selectManualImportPaths(ctx.repo.getSettings(), {
          mediaType: params.mediaType,
          files,
          forceAgent: params.forceAgent ?? false,
          priorFailure: params.priorFailure ?? false,
          book,
        });
        return {
          path: params.path.trim(),
          fileCount: files.length,
          files,
          decision,
        };
      },
    }),
  }),
});
