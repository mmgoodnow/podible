import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";

import { BUILD_INFO, type BuildInfo } from "./build-info";
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
import {
  createAppAuthRoutes,
  createLoginRoutes,
  createLogoutRoutes,
} from "./http/auth-routes";
import { createFeedRoutes } from "./http/feed-routes";
import {
  createRequestContextMiddleware,
  type HttpEnv,
} from "./http/middleware";
import { createRpcRoutes } from "./http/rpc-routes";
import { createTrpcRoutes } from "./http/trpc-routes";
import { createSpaRoutes } from "./http/spa-routes";
import { createActivityRoutes } from "./http/activity-routes";

export function createPodibleFetchHandler(
  repo: BooksRepo,
  startTime: number,
  buildInfo: BuildInfo | null = BUILD_INFO,
  dependencies: {
    movieServices?: import("./movies/operations").MovieServices;
    bookTorrentClient?: import("./media/torrent-client").TorrentClient;
  } = {},
): (request: Request) => Promise<Response> {
  const app = new Hono<HttpEnv>();

  app.use("*", createRequestContextMiddleware(repo));

  const spa = createSpaRoutes();
  for (const route of [
    "/",
    "/library",
    "/add",
    "/book/:bookId",
    "/series",
    "/author",
    "/movies",
    "/movies/:id",
    "/requests",
    "/requests/:id",
    "/movie-shadow",
    "/movie-shadow/:id",
    "/movie-dual-write",
  ])
    app.get(route, (c) => spa.fetch(c.req.raw));
  app.route("/login", createLoginRoutes(repo));
  app.route("/logout", createLogoutRoutes(repo));
  app.route("/auth/app", createAppAuthRoutes(repo));
  app.route("/activity", createActivityRoutes(repo));
  app.route("/admin", createAdminRoutes(repo, startTime, buildInfo));
  app.route("/rpc", createRpcRoutes(repo, startTime, dependencies));
  app.route("/api/trpc", createTrpcRoutes(repo, startTime, dependencies));
  app.route("/app", spa);
  app.route("/web-assets", spa);
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
