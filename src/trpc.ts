import { initTRPC, TRPCError } from "@trpc/server";
import { authRouter } from "./rpc/auth-router";
import { executeMethod } from "./rpc/framework";
import { libraryRouter } from "./rpc/library-router";
import { RpcError, type RpcContext } from "./rpc/shared";
import { moviesRouter, movieRequestsRouter } from "./rpc/movies-router";
import { movieShadowRouter } from "./rpc/movie-shadow-router";
import { movieDualWriteRouter } from "./rpc/movie-dual-write-router";

const t = initTRPC.context<RpcContext>().create();
const procedure = t.procedure.use(async ({ next }) => {
  const result = await next();
  if (!result.ok && result.error.cause instanceof RpcError) {
    const cause = result.error.cause;
    throw new TRPCError({
      code:
        cause.code === -32001
          ? "UNAUTHORIZED"
          : cause.code === -32003
            ? "FORBIDDEN"
            : cause.code === -32602
              ? "BAD_REQUEST"
              : "INTERNAL_SERVER_ERROR",
      message:
        cause.code === -32000
          ? "The operation could not be completed"
          : cause.message,
      cause,
    });
  }
  return result;
});

export const appRouter = t.router({
  movieDualWrite: t.router({
    status: procedure.input(movieDualWriteRouter.routes.status.paramsSchema).output(movieDualWriteRouter.routes.status.resultSchema).query(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.status,ctx,input)),
    list: procedure.input(movieDualWriteRouter.routes.list.paramsSchema).output(movieDualWriteRouter.routes.list.resultSchema).query(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.list,ctx,input)),
    sync: procedure.input(movieDualWriteRouter.routes.sync.paramsSchema).output(movieDualWriteRouter.routes.sync.resultSchema).mutation(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.sync,ctx,input)),
    backfill: procedure.input(movieDualWriteRouter.routes.backfill.paramsSchema).output(movieDualWriteRouter.routes.backfill.resultSchema).mutation(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.backfill,ctx,input)),
  }),
  movieShadow: t.router({
    status: procedure
      .input(movieShadowRouter.routes.status.paramsSchema)
      .output(movieShadowRouter.routes.status.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.status, ctx, input),
      ),
    list: procedure
      .input(movieShadowRouter.routes.list.paramsSchema)
      .output(movieShadowRouter.routes.list.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.list, ctx, input),
      ),
    get: procedure
      .input(movieShadowRouter.routes.get.paramsSchema)
      .output(movieShadowRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.get, ctx, input),
      ),
    sync: procedure
      .input(movieShadowRouter.routes.sync.paramsSchema)
      .output(movieShadowRouter.routes.sync.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.sync, ctx, input),
      ),
    retry: procedure
      .input(movieShadowRouter.routes.retry.paramsSchema)
      .output(movieShadowRouter.routes.retry.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.retry, ctx, input),
      ),
    review: procedure
      .input(movieShadowRouter.routes.review.paramsSchema)
      .output(movieShadowRouter.routes.review.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.review, ctx, input),
      ),
  }),
  movies: t.router({
    status: procedure
      .input(moviesRouter.routes.status.paramsSchema)
      .output(moviesRouter.routes.status.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.status, ctx, input),
      ),
    search: procedure
      .input(moviesRouter.routes.search.paramsSchema)
      .output(moviesRouter.routes.search.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.search, ctx, input),
      ),
    get: procedure
      .input(moviesRouter.routes.get.paramsSchema)
      .output(moviesRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.get, ctx, input),
      ),
    request: procedure
      .input(moviesRouter.routes.request.paramsSchema)
      .output(moviesRouter.routes.request.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.request, ctx, input),
      ),
  }),
  requests: t.router({
    list: procedure
      .input(movieRequestsRouter.routes.list.paramsSchema)
      .output(movieRequestsRouter.routes.list.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.list, ctx, input),
      ),
    get: procedure
      .input(movieRequestsRouter.routes.get.paramsSchema)
      .output(movieRequestsRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.get, ctx, input),
      ),
    retry: procedure
      .input(movieRequestsRouter.routes.retry.paramsSchema)
      .output(movieRequestsRouter.routes.retry.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.retry, ctx, input),
      ),
    checkUpgrade: procedure
      .input(movieRequestsRouter.routes.checkUpgrade.paramsSchema)
      .output(movieRequestsRouter.routes.checkUpgrade.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.checkUpgrade, ctx, input),
      ),
    withdraw: procedure
      .input(movieRequestsRouter.routes.withdraw.paramsSchema)
      .output(movieRequestsRouter.routes.withdraw.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.withdraw, ctx, input),
      ),
    searchReleases: procedure
      .input(movieRequestsRouter.routes.searchReleases.paramsSchema)
      .output(movieRequestsRouter.routes.searchReleases.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.searchReleases, ctx, input),
      ),
    selectRelease: procedure
      .input(movieRequestsRouter.routes.selectRelease.paramsSchema)
      .output(movieRequestsRouter.routes.selectRelease.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.selectRelease, ctx, input),
      ),
  }),
  auth: t.router({
    me: procedure
      .input(authRouter.routes.me.paramsSchema)
      .output(authRouter.routes.me.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(authRouter.routes.me, ctx, input),
      ),
  }),
  library: t.router({
    list: procedure
      .input(libraryRouter.routes.list.paramsSchema)
      .output(libraryRouter.routes.list.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(libraryRouter.routes.list, ctx, input),
      ),
    get: procedure
      .input(libraryRouter.routes.get.paramsSchema)
      .output(libraryRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(libraryRouter.routes.get, ctx, input),
      ),
  }),
});

export type AppRouter = typeof appRouter;
