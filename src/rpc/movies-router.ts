import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";
import { RpcError, type RpcContext } from "./shared";
import { positiveIntSchema, emptyParamsSchema } from "./schemas";
import { catalogMovieSchema } from "../movies/types";
import { movieRequestViewSchema } from "../movies/contracts";
import { movieCatalog, movieIndexers } from "../movies/runtime";
import { movieRequestView, queueMovieAcquisition } from "../movies/service";

function enabled(ctx: RpcContext) {
  if (!ctx.repo.getSettings().movies.enabled)
    throw new RpcError(-32602, "Movies are disabled by the administrator");
}
function owned(ctx: RpcContext, requestId: number) {
  enabled(ctx);
  const request = ctx.repo.movies.requestById(requestId);
  if (!request || request.user_id !== ctx.session!.user_id)
    throw new RpcError(-32003, "Movie request not found");
  return request;
}
const requestParams = z.object({ requestId: positiveIntSchema });
const requestResult = z.object({ request: movieRequestViewSchema });

export const moviesRouter = defineRouter({
  status: defineMethod({
    auth: "user",
    readOnly: true,
    summary:
      "Read movie feature availability without exposing settings secrets.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      enabled: z.boolean(),
      catalogConfigured: z.boolean(),
      acquisitionConfigured: z.boolean(),
    }),
    handler(ctx) {
      const s = ctx.repo.getSettings();
      return {
        enabled: s.movies.enabled,
        catalogConfigured: Boolean(
          ctx.movieServices || s.movies.tmdbAccessToken,
        ),
        acquisitionConfigured: Boolean(
          ctx.movieServices ||
          (s.movies.managedRoot &&
            s.movies.plexUrl &&
            s.movies.plexSectionId &&
            s.movies.plexViewPath &&
            s.agents.apiKey &&
            s.torznab.length),
        ),
      };
    },
  }),
  search: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Search the movie catalog.",
    paramsSchema: z.object({ query: z.string().trim().min(1).max(200) }),
    resultSchema: z.object({ results: z.array(catalogMovieSchema) }),
    async handler(ctx, input) {
      enabled(ctx);
      return {
        results: await (
          ctx.movieServices?.catalog ?? movieCatalog(ctx.repo.getSettings())
        ).search(input.query),
      };
    },
  }),
  get: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Read a movie and the current user's requests.",
    paramsSchema: z.object({ tmdbId: positiveIntSchema }),
    resultSchema: z.object({
      movie: catalogMovieSchema,
      requests: z.array(movieRequestViewSchema),
    }),
    async handler(ctx, input) {
      enabled(ctx);
      const movie = await (
        ctx.movieServices?.catalog ?? movieCatalog(ctx.repo.getSettings())
      ).get(input.tmdbId);
      const requests = ctx.repo.movies
        .requests(ctx.session!.user_id)
        .map((r) => movieRequestView(ctx.repo, r.id))
        .filter((r) => r.movie.tmdbId === input.tmdbId);
      return { movie, requests };
    },
  }),
  request: defineMethod({
    auth: "user",
    summary: "Request a movie, sharing compatible acquisition work.",
    paramsSchema: z.object({ tmdbId: positiveIntSchema }),
    resultSchema: requestResult,
    async handler(ctx, input) {
      enabled(ctx);
      const movie = await (
        ctx.movieServices?.catalog ?? movieCatalog(ctx.repo.getSettings())
      ).get(input.tmdbId);
      const result = ctx.repo.transaction(() => {
        const request = ctx.repo.movies.request(
          ctx.session!.user_id,
          movie,
          ctx.repo.getSettings().movies.qualityPolicy,
        );
        const view = movieRequestView(ctx.repo, request.requestId);
        if (!view.available)
          queueMovieAcquisition(ctx.repo, request.targetId, "initial");
        return movieRequestView(ctx.repo, request.requestId);
      });
      return { request: result };
    },
  }),
});
export const movieRequestsRouter = defineRouter({
  list: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List the user's movie requests and progress.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({ requests: z.array(movieRequestViewSchema) }),
    handler(ctx) {
      enabled(ctx);
      return {
        requests: ctx.repo.movies
          .requests(ctx.session!.user_id)
          .map((r) => movieRequestView(ctx.repo, r.id)),
      };
    },
  }),
  get: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Read request availability and its semantic timeline.",
    paramsSchema: requestParams,
    resultSchema: requestResult,
    handler(ctx, input) {
      owned(ctx, input.requestId);
      return { request: movieRequestView(ctx.repo, input.requestId) };
    },
  }),
  retry: defineMethod({
    auth: "user",
    summary:
      "Start a fresh movie search, retaining previous versions and downloads.",
    paramsSchema: requestParams,
    resultSchema: requestResult,
    handler(ctx, input) {
      const request = owned(ctx, input.requestId);
      if (request.withdrawn_at)
        throw new RpcError(
          -32602,
          "Request was withdrawn; request the movie again to resume",
        );
      queueMovieAcquisition(ctx.repo, request.target_id, "recovery");
      return { request: movieRequestView(ctx.repo, input.requestId) };
    },
  }),
  checkUpgrade: defineMethod({
    auth: "user",
    summary:
      "Search for a policy-eligible upgrade without removing the current version.",
    paramsSchema: requestParams,
    resultSchema: requestResult,
    handler(ctx, input) {
      const request = owned(ctx, input.requestId);
      if (request.withdrawn_at)
        throw new RpcError(-32602, "Request was withdrawn");
      queueMovieAcquisition(ctx.repo, request.target_id, "upgrade");
      return { request: movieRequestView(ctx.repo, input.requestId) };
    },
  }),
  withdraw: defineMethod({
    auth: "user",
    summary:
      "Withdraw this request; shared acquisitions continue for other users.",
    paramsSchema: requestParams,
    resultSchema: requestResult,
    handler(ctx, input) {
      const request = owned(ctx, input.requestId);
      ctx.repo.transaction(() => {
        ctx.repo.movies.withdrawRequest(request.id);
        if (!ctx.repo.movies.activeRequestCount(request.target_id)) {
          for (const a of ctx.repo.movies.acquisitions(request.target_id))
            if (a.job_id)
              ctx.repo.markJobCancelled(
                a.job_id,
                "All movie requests withdrawn",
              );
        }
      });
      return { request: movieRequestView(ctx.repo, input.requestId) };
    },
  }),
  searchReleases: defineMethod({
    auth: "user",
    readOnly: true,
    summary:
      "Search releases and return opaque, user-scoped references valid for 30 minutes.",
    paramsSchema: requestParams.extend({
      query: z.string().trim().max(200).optional(),
    }),
    resultSchema: z.object({
      searchId: z.string().uuid(),
      warnings: z.array(z.string()),
      results: z.array(
        z.object({
          index: z.number(),
          title: z.string(),
          provider: z.string(),
          sizeBytes: z.number().nullable(),
          seeders: z.number().nullable(),
        }),
      ),
    }),
    async handler(ctx, input) {
      const request = owned(ctx, input.requestId);
      const target = ctx.repo.movies.target(request.target_id)!;
      const movie = ctx.repo.movies.movie(target.movie_id)!;
      const found = await (ctx.movieServices
        ? ctx.movieServices.search(movie, input.query)
        : movieIndexers(ctx.repo.getSettings()).search(movie, input.query));
      return {
        searchId: ctx.repo.movies.saveReleaseSearch(
          ctx.session!.user_id,
          target.id,
          found.offers,
        ),
        warnings: found.warnings,
        results: found.offers.map((offer, index) => ({
          index,
          title: offer.title,
          provider: offer.provider,
          sizeBytes: offer.sizeBytes,
          seeders: offer.seeders,
        })),
      };
    },
  }),
  selectRelease: defineMethod({
    auth: "user",
    summary:
      "Acquire one previously searched release, with semantic and post-download verification.",
    paramsSchema: requestParams.extend({
      searchId: z.string().uuid(),
      resultIndex: z.number().int().nonnegative(),
    }),
    resultSchema: requestResult,
    handler(ctx, input) {
      const request = owned(ctx, input.requestId);
      if (request.withdrawn_at)
        throw new RpcError(-32602, "Request was withdrawn");
      const offer = ctx.repo.movies.searchedOffer(
        ctx.session!.user_id,
        request.target_id,
        input.searchId,
        input.resultIndex,
      );
      if (!offer)
        throw new RpcError(
          -32602,
          "Release search expired or is not accessible; search again",
        );
      const queued = queueMovieAcquisition(
        ctx.repo,
        request.target_id,
        "recovery",
        offer,
      );
      if (queued.shared)
        throw new RpcError(
          -32602,
          "An acquisition is already in progress for this movie",
        );
      return { request: movieRequestView(ctx.repo, input.requestId) };
    },
  }),
});
