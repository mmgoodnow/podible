import { ZodError, type ZodType } from "zod";

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

export type RpcRouterDefinition = {
  kind: "router";
  routes: Record<string, RpcNode>;
};

export type RpcNode = RpcMethodDefinition<any, any> | RpcRouterDefinition;

export function defineMethod<TParams, TResult>(config: {
  auth: RpcAuthLevel;
  readOnly?: boolean;
  summary: string;
  paramsSchema: ZodType<TParams>;
  resultSchema: ZodType<TResult>;
  handler: (ctx: RpcContext, params: TParams) => MaybePromise<TResult>;
}): RpcMethodDefinition<TParams, TResult> {
  return {
    kind: "method",
    auth: config.auth,
    readOnly: Boolean(config.readOnly),
    summary: config.summary,
    paramsSchema: config.paramsSchema,
    resultSchema: config.resultSchema,
    handler: config.handler,
  };
}

export function defineRouter(routes: Record<string, RpcNode>): RpcRouterDefinition {
  return {
    kind: "router",
    routes,
  };
}

export function flattenRouter(node: RpcNode, prefix = ""): Record<string, RpcMethodDefinition<any, any>> {
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

export function parseMethodParams<TParams>(method: RpcMethodDefinition<TParams>, rawParams: Record<string, unknown>): TParams {
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

export function parseMethodResult<TResult>(method: RpcMethodDefinition<any, TResult>, rawResult: unknown): TResult {
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
