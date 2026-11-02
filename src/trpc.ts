import { initTRPC, TRPCError } from "@trpc/server";
import { authRouter } from "./rpc/auth-router";
import { executeMethod } from "./rpc/framework";
import { libraryRouter } from "./rpc/library-router";
import { RpcError, type RpcContext } from "./rpc/shared";

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
