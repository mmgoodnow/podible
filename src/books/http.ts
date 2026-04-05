import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";

import { BooksRepo } from "./repo";
import { createAdminRoutes } from "./http/admin-routes";
import {
  createAssetsIndexRoutes,
  createChaptersRoutes,
  createCoverRoutes,
  createEbookRoutes,
  createStreamRoutes,
  createTranscriptsRoutes,
} from "./http/asset-routes";
import { createAppAuthRoutes, createLoginRoutes, createLogoutRoutes } from "./http/auth-routes";
import { createFeedRoutes } from "./http/feed-routes";
import { createRequestContextMiddleware, type HttpEnv } from "./http/middleware";
import { createRpcRoutes } from "./http/rpc-routes";
import { createActivityRoutes, createAddRoutes, createBookRoutes, createHomeRoutes, createLibraryRoutes } from "./http/user-routes";

export function createPodibleFetchHandler(repo: BooksRepo, startTime: number): (request: Request) => Promise<Response> {
  const app = new Hono<HttpEnv>();

  app.use("*", createRequestContextMiddleware(repo));

  app.route("/", createHomeRoutes(repo));
  app.route("/login", createLoginRoutes(repo));
  app.route("/logout", createLogoutRoutes(repo));
  app.route("/auth/app", createAppAuthRoutes(repo));
  app.route("/library", createLibraryRoutes(repo));
  app.route("/add", createAddRoutes(repo));
  app.route("/book", createBookRoutes(repo));
  app.route("/activity", createActivityRoutes(repo));
  app.route("/admin", createAdminRoutes(repo));
  app.route("/rpc", createRpcRoutes(repo, startTime));
  app.route("/assets", createAssetsIndexRoutes(repo));
  app.route("/stream", createStreamRoutes(repo));
  app.route("/chapters", createChaptersRoutes(repo));
  app.route("/transcripts", createTranscriptsRoutes(repo));
  app.route("/covers", createCoverRoutes(repo));
  app.route("/ebook", createEbookRoutes(repo));
  app.route("/", createFeedRoutes(repo));

  app.onError((error, c) => {
    if (error instanceof HTTPException) {
      return error.getResponse();
    }
    console.error(error);
    return c.json({ error: "Internal server error" }, 500);
  });

  return async (request: Request) => app.fetch(request);
}
