import { downloadCover } from "../library/covers";
import { coverUrlFromId, findOpenLibraryCoverCandidates, searchOpenLibrary } from "../library/openlibrary";

import { defineMethod, defineRouter } from "./framework";
import {
  emptyParamsSchema,
  libraryBookSchema,
  limitSchema,
  nonEmptyStringSchema,
  openLibraryCandidateSchema,
  openLibraryCoverCandidateSchema,
  positiveIntSchema,
} from "./schemas";
import { RpcError } from "./shared";
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

  covers: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List alternate Open Library cover candidates for a library book.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      limit: limitSchema.optional(),
    }),
    resultSchema: z.object({
      results: z.array(openLibraryCoverCandidateSchema),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBook(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      const results = await findOpenLibraryCoverCandidates(book, Math.min(params.limit ?? 50, 200));
      return { results };
    },
  }),

  setCover: defineMethod({
    auth: "user",
    summary: "Download and apply an Open Library cover ID to a library book.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      coverId: positiveIntSchema,
    }),
    resultSchema: z.object({
      book: libraryBookSchema,
      cover: z.object({
        coverId: positiveIntSchema,
        coverUrl: z.string(),
      }),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBook(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      const coverUrl = coverUrlFromId(params.coverId);
      const coverPath = await downloadCover(ctx.repo, book, coverUrl);
      if (!coverPath) {
        throw new RpcError(-32000, "Open Library cover not found", {
          error: "cover_not_found",
          coverId: params.coverId,
        });
      }

      ctx.repo.updateBookMetadata(book.id, { coverPath });
      const updatedBook = ctx.repo.getBook(book.id);
      if (!updatedBook) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      return {
        book: updatedBook,
        cover: {
          coverId: params.coverId,
          coverUrl,
        },
      };
    },
  }),
});
