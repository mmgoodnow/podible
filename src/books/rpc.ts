import { adminRouter } from "./rpc/admin-router";
import { agentRouter } from "./rpc/agent-router";
import { createAuthRouter } from "./rpc/auth-router";
import { downloadsRouter } from "./rpc/downloads-router";
import { defineRouter, flattenRouter, parseMethodParams, type RpcMethodDefinition } from "./rpc/framework";
import { importRouter } from "./rpc/import-router";
import { jobsRouter } from "./rpc/jobs-router";
import { libraryRouter } from "./rpc/library-router";
import { openLibraryRouter } from "./rpc/openlibrary-router";
import { settingsRouter } from "./rpc/settings-router";
import {
  RpcError,
  parseRequest,
  type RpcAuthLevel,
  type RpcContext,
  type RpcDispatchOptions,
  type RpcFailure,
  type RpcId,
  type RpcSuccess,
} from "./rpc/shared";
import { searchRouter } from "./rpc/search-router";
import { systemRouter } from "./rpc/system-router";

function response(payload: RpcSuccess | RpcFailure): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function success(id: RpcId, result: unknown): Response {
  return response({
    jsonrpc: "2.0",
    id,
    result,
  });
}

function failure(id: RpcId, code: number, message: string, data?: unknown): Response {
  return response({
    jsonrpc: "2.0",
    id,
    error: { code, message, ...(data === undefined ? {} : { data }) },
  });
}

let methodsByName: Record<string, RpcMethodDefinition<any>> = {};
methodsByName = flattenRouter(
  defineRouter({
    ...createAuthRouter(() => methodsByName).routes,
    ...systemRouter.routes,
    ...openLibraryRouter.routes,
    ...libraryRouter.routes,
    ...settingsRouter.routes,
    ...searchRouter.routes,
    ...downloadsRouter.routes,
    ...jobsRouter.routes,
    ...importRouter.routes,
    ...agentRouter.routes,
    ...adminRouter.routes,
  })
);

function hasRpcAccess(level: RpcAuthLevel, session: RpcContext["session"]): boolean {
  if (level === "public") return true;
  if (!session) return false;
  if (level === "user") return true;
  return session.is_admin === 1;
}

async function dispatchRpcMethod(
  methodName: string,
  params: Record<string, unknown>,
  ctx: RpcContext,
  options: RpcDispatchOptions = {}
): Promise<Response> {
  const id = options.id ?? null;
  try {
    const method = methodsByName[methodName];
    if (!method || (options.readOnly && !method.readOnly)) {
      throw new RpcError(-32601, "Method not found");
    }
    if (!hasRpcAccess(method.auth, ctx.session)) {
      throw new RpcError(ctx.session ? -32003 : -32001, ctx.session ? "Forbidden" : "Unauthorized");
    }
    const parsedParams = parseMethodParams(method, params);
    const result = await method.handler(ctx, parsedParams);
    return success(id, result);
  } catch (error) {
    if (error instanceof RpcError) {
      return failure(id, error.code, error.message, error.data);
    }
    const message = (error as Error).message;
    return failure(id, -32000, message || "Application error", { message });
  }
}

export async function handleRpcRequest(request: Request, ctx: RpcContext): Promise<Response> {
  let parsed: ReturnType<typeof parseRequest> | null = null;
  try {
    const body = await request.text();
    const payload = JSON.parse(body);
    parsed = parseRequest(payload);
  } catch (error) {
    if (error instanceof SyntaxError) {
      return failure(null, -32700, "Parse error");
    }
    if (error instanceof RpcError) {
      return failure(parsed?.id ?? null, error.code, error.message, error.data);
    }
    return failure(null, -32603, "Internal error");
  }

  return dispatchRpcMethod(parsed.method, parsed.params ?? {}, ctx, { id: parsed.id });
}

export async function handleRpcMethod(
  methodName: string,
  params: Record<string, unknown>,
  ctx: RpcContext,
  options: RpcDispatchOptions = {}
): Promise<Response> {
  if (typeof methodName !== "string" || !methodName.trim()) {
    return failure(options.id ?? null, -32600, "Invalid Request");
  }
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return failure(options.id ?? null, -32600, "params must be an object");
  }
  return dispatchRpcMethod(methodName, params, ctx, options);
}

export { RpcError };
