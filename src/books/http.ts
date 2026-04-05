import { Hono } from "hono";

import { BooksRepo } from "./repo";
import { createAdminRoutes } from "./http/admin-routes";
import { createAssetRoutes } from "./http/asset-routes";
import { createAuthRoutes } from "./http/auth-routes";
import { createFeedRoutes } from "./http/feed-routes";
import { createRequestContextMiddleware, type HttpEnv } from "./http/middleware";
import { createRpcRoutes } from "./http/rpc-routes";
import { createUserRoutes } from "./http/user-routes";
import { json } from "./http/route-helpers";

export function createPodibleFetchHandler(repo: BooksRepo, startTime: number): (request: Request) => Promise<Response> {
  const app = new Hono<HttpEnv>();

  app.use("*", createRequestContextMiddleware(repo));

  app.route("/", createAuthRoutes(repo));
  app.route("/", createUserRoutes(repo));
  app.route("/", createAdminRoutes(repo));
  app.route("/", createRpcRoutes(repo, startTime));
  app.route("/", createAssetRoutes(repo));
  app.route("/", createFeedRoutes(repo));

  app.notFound(() => new Response("Not found", { status: 404 }));
  app.onError((error) => json({ error: (error as Error).message }, 400));

  return async (request: Request) => app.fetch(request);
}
