import { ZodError, type ZodType, type output } from "zod";

import { RpcError, type RpcAuthLevel, type RpcContext } from "./shared";

type MaybePromise<T> = T | Promise<T>;

export type RpcMethodDefinition<TParams = unknown, TResult = unknown> = {
  kind: "method";
  auth: RpcAuthLevel;
  readOnly: boolean;
  summary: string;
  paramsSchema: ZodType<TParams>;
  resultSchema: ZodType<TResult>;
  handler: (ctx: RpcContext, params: TParams) => MaybePromise<TResult>;
};

export interface RpcRouterDefinition<
  TRoutes extends Record<string, RpcNode> = Record<string, RpcNode>,
> {
  kind: "router";
  routes: TRoutes;
}

export type RpcNode = RpcMethodDefinition<any, any> | RpcRouterDefinition;

export function defineMethod<TParams extends ZodType, TResult>(config: {
  auth: RpcAuthLevel;
  readOnly?: boolean;
  summary: string;
  paramsSchema: TParams;
  resultSchema: ZodType<TResult>;
  handler: (ctx: RpcContext, params: output<TParams>) => MaybePromise<TResult>;
}) {
  return {
    kind: "method" as const,
    auth: config.auth,
    readOnly: Boolean(config.readOnly),
    summary: config.summary,
    paramsSchema: config.paramsSchema,
    resultSchema: config.resultSchema,
    handler: config.handler,
  };
}

export function defineRouter<const TRoutes extends Record<string, RpcNode>>(
  routes: TRoutes,
): RpcRouterDefinition<TRoutes> {
  return {
    kind: "router",
    routes,
  };
}

export async function executeMethod<TParams, TResult>(
  method: RpcMethodDefinition<TParams, TResult>,
  ctx: RpcContext,
  params: Record<string, unknown>,
): Promise<TResult> {
  if (
    method.auth !== "public" &&
    (!ctx.session || (method.auth === "admin" && ctx.session.is_admin !== 1))
  ) {
    throw new RpcError(
      ctx.session ? -32003 : -32001,
      ctx.session ? "Forbidden" : "Unauthorized",
    );
  }
  return parseMethodResult(
    method,
    await method.handler(ctx, parseMethodParams(method, params)),
  );
}

export function flattenRouter(
  node: RpcNode,
  prefix = "",
): Record<string, RpcMethodDefinition<any, any>> {
  if (node.kind === "method") {
    if (!prefix) {
      throw new Error("Cannot flatten a method without a name");
    }
    return { [prefix]: node };
  }

  const flat: Record<string, RpcMethodDefinition<any, any>> = {};
  for (const [name, child] of Object.entries(node.routes)) {
    const nextPrefix = prefix ? `${prefix}.${name}` : name;
    Object.assign(flat, flattenRouter(child, nextPrefix));
  }
  return flat;
}

export function parseMethodParams<TParams>(
  method: RpcMethodDefinition<TParams>,
  rawParams: Record<string, unknown>,
): TParams {
  try {
    return method.paramsSchema.parse(rawParams);
  } catch (error) {
    if (error instanceof ZodError) {
      const issue = error.issues[0];
      throw new RpcError(-32602, issue?.message || "Invalid params", {
        issues: error.issues.map((entry) => ({
          path: entry.path.join("."),
          message: entry.message,
        })),
      });
    }
    throw error;
  }
}

export function parseMethodResult<TResult>(
  method: RpcMethodDefinition<any, TResult>,
  rawResult: unknown,
): TResult {
  try {
    return method.resultSchema.parse(rawResult);
  } catch (error) {
    if (error instanceof ZodError) {
      const issue = error.issues[0];
      throw new RpcError(-32603, issue?.message || "Invalid result", {
        issues: error.issues.map((entry) => ({
          path: entry.path.join("."),
          message: entry.message,
        })),
      });
    }
    throw error;
  }
}
