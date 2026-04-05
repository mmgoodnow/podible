import { Hono } from "hono";

import { BooksRepo } from "../repo";
import { handleRpcMethod, handleRpcRequest } from "../rpc";
import { setLogSuffix, type HttpEnv } from "./middleware";

export function createRpcRoutes(repo: BooksRepo, startTime: number): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.post("/rpc", async (c) => {
    try {
      const cloned = await c.req.raw.clone().text();
      const payload = JSON.parse(cloned) as { method?: unknown };
      if (typeof payload.method === "string" && payload.method.trim()) {
        setLogSuffix(c, `rpc=${payload.method.trim()}`);
      }
    } catch {
      // ignore parse errors in logging path; handler will return JSON-RPC parse errors.
    }
    return handleRpcRequest(c.req.raw, {
      repo,
      startTime,
      request: c.req.raw,
      session: c.get("session"),
    });
  });

  app.get("/rpc/:namespace/:method", async (c) => {
    const methodName = `${c.req.param("namespace")}.${c.req.param("method")}`;
    setLogSuffix(c, `rpc=${methodName}`);

    const params: Record<string, unknown> = {};
    for (const [key, value] of new URL(c.req.url).searchParams.entries()) {
      const existing = params[key];
      if (existing === undefined) {
        params[key] = value;
        continue;
      }
      if (Array.isArray(existing)) {
        existing.push(value);
      } else {
        params[key] = [existing, value];
      }
    }

    return handleRpcMethod(methodName, params, {
      repo,
      startTime,
      request: c.req.raw,
      session: c.get("session"),
    }, { id: null, readOnly: true });
  });

  return app;
}
