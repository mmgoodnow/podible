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
import { createMetricsRoutes } from "./http/metrics-routes";
import { createAutobrrRoutes } from "./http/autobrr-routes";
import { createTrailerRoutes } from "./http/trailer-routes";
import { createPlaybackRoutes } from "./http/playback-routes";
import { createDeviceLoginRoutes } from "./http/device-login";
import { AUTOBRR_WEBHOOK_PATH, type AutobrrConfig } from "./media/autobrr";
import { AcquisitionEvents } from "./runtime/acquisition-events";

export function createPodibleFetchHandler(
  repo: BooksRepo,
  startTime: number,
  buildInfo: BuildInfo | null = BUILD_INFO,
  dependencies: {
    movieServices?: import("./movies/operations").MovieServices;
    bookTorrentClient?: import("./media/torrent-client").TorrentClient;
    metricsTokenDigestPath?: string;
    autobrr?: AutobrrConfig;
    acquisitionEvents?: AcquisitionEvents;
    acquisitionTransmission?: import("./rpc/admin-torrents").AdminTransmissionReader;
  } = {},
): (request: Request) => Promise<Response> {
  const app = new Hono<HttpEnv>();

  app.use("*", createRequestContextMiddleware(repo));
  const acquisitionEvents = dependencies.acquisitionEvents ?? new AcquisitionEvents();

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
    "/tv/:id",
    "/tv/requests/:id",
    "/tv/library/:id",
    "/movie-shadow",
    "/movie-shadow/:id",
    "/movie-dual-write",
    "/tv-sync",
    "/retention",
    "/recovery",
    "/stewardship",
    "/tv-retention",
    "/settings",
    "/users",
    "/ops",
    "/login",
    "/login/plex/loading",
    "/login/plex/result",
  ]) {
    app.get(route, (c) => spa.fetch(c.req.raw));
    if (route !== "/") app.get(`${route}/`, (c) => spa.fetch(c.req.raw));
  }
  app.route("/login", createLoginRoutes(repo));
  app.route("/logout", createLogoutRoutes(repo));
  app.route("/auth/app", createAppAuthRoutes(repo));
  app.route("/auth/device", createDeviceLoginRoutes(repo));
  app.route("/activity", createActivityRoutes(repo));
  app.route("/admin", createAdminRoutes(repo, startTime, buildInfo));
  app.route("/rpc", createRpcRoutes(repo, startTime, dependencies));
  app.route("/api/trpc", createTrpcRoutes(repo, startTime, { ...dependencies, acquisitionEvents }));
  app.route("/api/trailers", createTrailerRoutes(repo));
  app.route("/api/playback", createPlaybackRoutes(repo));
  app.route("/metrics", createMetricsRoutes(repo, dependencies.metricsTokenDigestPath));
  if (dependencies.autobrr)
    app.route(AUTOBRR_WEBHOOK_PATH, createAutobrrRoutes(repo, dependencies.autobrr));
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
