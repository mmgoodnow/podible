import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { catalogMovieSchema } from "../movies/types";
import { tvCatalogEntrySchema } from "../tv/types";
const id = z.number().int().positive();
export const catalogRouter = defineRouter({
  available: defineMethod({ auth: "user", readOnly: true,
    summary: "Search already-published shared movies/TV without waiting on remote catalogs or exposing anyone's requests.",
    paramsSchema: z.object({ query: z.string().trim().max(200).default(""), limit: z.number().int().min(1).max(100).default(50) }).strict(),
    resultSchema: z.object({ movies: z.array(catalogMovieSchema), tv: z.array(z.object({ entryId: id, key: z.string(), title: z.string(), year: z.number().nullable(), posterUrl: tvCatalogEntrySchema.shape.posterUrl, availableEpisodes: z.number().int().nonnegative() })), movieTruncated: z.boolean(), tvTruncated: z.boolean() }),
    handler(ctx, input) {
      const movies = ctx.repo.movies.availableCatalog(input.query, input.limit + 1);
      const tv = ctx.repo.tv.availableCatalog(input.query, input.limit + 1);
      return { movies: movies.slice(0, input.limit), tv: tv.slice(0, input.limit), movieTruncated: movies.length > input.limit, tvTruncated: tv.length > input.limit };
    } }),
  tvEntry: defineMethod({ auth: "user", readOnly: true,
    summary: "Read shared published TV work metadata without exposing private acquisition/request diagnostics.",
    paramsSchema: z.object({ entryId: id }).strict(),
    resultSchema: z.object({ entry: tvCatalogEntrySchema, availableEpisodes: z.number().int().positive() }),
    handler(ctx, input) {
      const availableEpisodes = ctx.repo.tv.availableEpisodeCount(input.entryId);
      const entry = availableEpisodes ? ctx.repo.tv.entry(input.entryId) : null;
      if (!entry) throw new RpcError(-32602, "Published TV work not found");
      return { entry, availableEpisodes };
    } }),
});
