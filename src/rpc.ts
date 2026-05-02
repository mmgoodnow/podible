import { adminRouter } from "./rpc/admin-router";
import { agentRouter } from "./rpc/agent-router";
import { authRouter, createHelpMethod } from "./rpc/auth-router";
import { downloadsRouter } from "./rpc/downloads-router";
import { defineRouter, flattenRouter, parseMethodParams, parseMethodResult, type RpcMethodDefinition } from "./rpc/framework";
import { importRouter } from "./rpc/import-router";
import { jobsRouter } from "./rpc/jobs-router";
import { libraryRouter, releasesRouter } from "./rpc/library-router";
import { recordRpcRequest } from "./metrics";
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
import { searchRouter, snatchRouter } from "./rpc/search-router";
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
const rootRouter = defineRouter({
  help: createHelpMethod(() => methodsByName),
  auth: authRouter,
  system: systemRouter,
  openlibrary: openLibraryRouter,
  library: libraryRouter,
  releases: releasesRouter,
  settings: settingsRouter,
  search: searchRouter,
  snatch: snatchRouter,
  downloads: downloadsRouter,
  jobs: jobsRouter,
  import: importRouter,
  agent: agentRouter,
  admin: adminRouter,
});
methodsByName = flattenRouter(rootRouter);

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
  const startedAt = performance.now();
  const id = options.id ?? null;
  const methodLabel = methodsByName[methodName] ? methodName : "unknown";
  const transport = options.transport ?? "direct";
  try {
    const method = methodsByName[methodName];
    if (!method || (options.readOnly && !method.readOnly)) {
      throw new RpcError(-32601, "Method not found");
    }
    if (!hasRpcAccess(method.auth, ctx.session)) {
      throw new RpcError(ctx.session ? -32003 : -32001, ctx.session ? "Forbidden" : "Unauthorized");
    }
    const parsedParams = parseMethodParams(method, params);
    const rawResult = await method.handler(ctx, parsedParams);
    const result = parseMethodResult(method, rawResult);
    recordRpcRequest(methodLabel, transport, "ok", null, startedAt);
    return success(id, result);
  } catch (error) {
    if (error instanceof RpcError) {
      recordRpcRequest(methodLabel, transport, "error", error.code, startedAt);
      return failure(id, error.code, error.message, error.data);
    }
    const message = (error as Error).message;
    recordRpcRequest(methodLabel, transport, "error", -32000, startedAt);
    return failure(id, -32000, message || "Application error", { message });
  }
}

export async function handleRpcRequest(request: Request, ctx: RpcContext): Promise<Response> {
  const startedAt = performance.now();
  let parsed: ReturnType<typeof parseRequest> | null = null;
  try {
    const body = await request.text();
    const payload = JSON.parse(body);
    parsed = parseRequest(payload);
  } catch (error) {
    if (error instanceof SyntaxError) {
      recordRpcRequest("parse_error", "post", "error", -32700, startedAt);
      return failure(null, -32700, "Parse error");
    }
    if (error instanceof RpcError) {
      recordRpcRequest(parsed?.method ?? "invalid_request", "post", "error", error.code, startedAt);
      return failure(parsed?.id ?? null, error.code, error.message, error.data);
    }
    recordRpcRequest("invalid_request", "post", "error", -32603, startedAt);
    return failure(null, -32603, "Internal error");
  }

  return dispatchRpcMethod(parsed.method, parsed.params ?? {}, ctx, { id: parsed.id, transport: "post" });
}

export async function handleRpcMethod(
  methodName: string,
  params: Record<string, unknown>,
  ctx: RpcContext,
  options: RpcDispatchOptions = {}
): Promise<Response> {
  const startedAt = performance.now();
  const transport = options.transport ?? "direct";
  if (typeof methodName !== "string" || !methodName.trim()) {
    recordRpcRequest("invalid_request", transport, "error", -32600, startedAt);
    return failure(options.id ?? null, -32600, "Invalid Request");
  }
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    recordRpcRequest(methodsByName[methodName] ? methodName : "unknown", transport, "error", -32600, startedAt);
    return failure(options.id ?? null, -32600, "params must be an object");
  }
  return dispatchRpcMethod(methodName, params, ctx, options);
}

export { RpcError };
