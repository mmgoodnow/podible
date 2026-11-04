import { fetchRequestHandler } from "@trpc/server/adapters/fetch";
import { Hono } from "hono";
import type { BooksRepo } from "../repo";
import { appRouter } from "../trpc";
import type { HttpEnv } from "./middleware";

export function createTrpcRoutes(repo: BooksRepo, startTime: number, dependencies: {
  movieServices?: import("../movies/operations").MovieServices;
  bookTorrentClient?: import("../media/torrent-client").TorrentClient;
} = {}) {
  const app = new Hono<HttpEnv>();
  app.all("*", async (c) => {
    const request = c.req.raw;
    if (request.method === "POST") {
      const origin = request.headers.get("origin");
      if (
        (origin !== null && origin !== new URL(request.url).origin) ||
        request.headers.get("sec-fetch-site") === "cross-site"
      ) {
        return c.json({ error: "Cross-origin mutation rejected" }, 403);
      }
    }
    c.header("Cache-Control", "no-store");
    return fetchRequestHandler({
      endpoint: "/api/trpc",
      req: request,
      router: appRouter,
      createContext: () => ({
        ...dependencies,
        repo,
        startTime,
        request,
        session: c.get("session"),
      }),
    });
  });
  return app;
}
