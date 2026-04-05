import { adminRpcMethods } from "./rpc/admin-methods";
import { createPublicRpcMethods } from "./rpc/public-methods";
import {
  RpcError,
  parseRequest,
  type RpcAuthLevel,
  type RpcContext,
  type RpcDispatchOptions,
  type RpcFailure,
  type RpcId,
  type RpcMethodDefinition,
  type RpcSuccess,
} from "./rpc/shared";
import { userRpcMethods } from "./rpc/user-methods";

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

let methodsByName: Record<string, RpcMethodDefinition> = {};
methodsByName = {
  ...createPublicRpcMethods(() => methodsByName),
  ...userRpcMethods,
  ...adminRpcMethods,
};

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
    const result = await method.handler(ctx, params);
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
