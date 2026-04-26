import { runSearch, runSnatch } from "../library/service";
import { z } from "zod";

import { defineMethod, defineRouter } from "./framework";
import {
  emptyParamsSchema,
  mediaSchema,
  nonEmptyStringSchema,
  optionalStringSchema,
  positiveIntSchema,
  torznabResultSchema,
  releaseRowSchema,
} from "./schemas";

export const searchRouter = defineRouter({
  run: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Run Torznab search and return normalized results.",
    paramsSchema: emptyParamsSchema.extend({
      query: nonEmptyStringSchema,
      media: mediaSchema,
    }),
    resultSchema: z.object({
      results: z.array(torznabResultSchema),
    }),
    async handler(ctx, params) {
      const results = await runSearch(ctx.repo.getSettings(), {
        query: params.query.trim(),
        media: params.media,
      });
      return { results };
    },
  }),
});

export const snatchRouter = defineRouter({
  create: defineMethod({
    auth: "admin",
    summary: "Create release + download job for a chosen search result.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      provider: nonEmptyStringSchema,
      title: nonEmptyStringSchema,
      mediaType: mediaSchema,
      url: nonEmptyStringSchema,
      infoHash: optionalStringSchema,
      guid: optionalStringSchema,
      sizeBytes: z.preprocess((value) => {
        if (value === undefined || value === null || value === "") return null;
        if (typeof value === "number") return Math.trunc(value);
        return Number.parseInt(String(value), 10);
      }, z.number().int().nullable()),
      manifestationId: z.preprocess((value) => {
        if (value === undefined || value === null || value === "") return null;
        if (typeof value === "number") return Math.trunc(value);
        return Number.parseInt(String(value), 10);
      }, z.number().int().positive().nullable()),
      sequenceInManifestation: z.preprocess((value) => {
        if (value === undefined || value === null || value === "") return null;
        if (typeof value === "number") return Math.trunc(value);
        return Number.parseInt(String(value), 10);
      }, z.number().int().nonnegative().nullable()),
    }),
    resultSchema: z.object({
      release: releaseRowSchema,
      jobId: positiveIntSchema,
      idempotent: z.boolean(),
    }),
    async handler(ctx, params) {
      return runSnatch(ctx.repo, ctx.repo.getSettings(), {
        bookId: params.bookId,
        provider: params.provider,
        providerGuid: params.guid ?? null,
        title: params.title,
        mediaType: params.mediaType,
        url: params.url,
        infoHash: params.infoHash,
        sizeBytes: Number.isFinite(params.sizeBytes) ? params.sizeBytes : null,
        manifestationId: params.manifestationId,
        sequenceInManifestation: params.sequenceInManifestation,
      });
    },
  }),
});
