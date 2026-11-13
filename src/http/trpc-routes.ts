import { fetchRequestHandler } from "@trpc/server/adapters/fetch";
import { TRPCError } from "@trpc/server";
import { Hono } from "hono";
import type { BooksRepo } from "../repo";
import { appRouter } from "../trpc";
import type { HttpEnv } from "./middleware";

export function createTrpcRoutes(repo: BooksRepo, startTime: number, dependencies: {
  movieServices?: import("../movies/operations").MovieServices;
  bookTorrentClient?: import("../media/torrent-client").TorrentClient;
  acquisitionEvents?: import("../runtime/acquisition-events").AcquisitionEvents;
  acquisitionTransmission?: import("../rpc/admin-torrents").AdminTransmissionReader;
} = {}) {
  const app = new Hono<HttpEnv>();
  app.all("*", async (c) => {
    const request = c.req.raw;
    const response = await fetchRequestHandler({
      endpoint: "/api/trpc",
      req: request,
      router: appRouter,
      createContext: () => {
        if (request.method === "POST") {
          const external = new URL(request.url);
          const proto = request.headers.get("x-forwarded-proto");
          // The TLS terminator must overwrite this header and preserve Host.
          // Never trust a forwarded host or a list of conflicting protocols.
          if (proto === "https" || proto === "http") external.protocol = `${proto}:`;
          const origin = request.headers.get("origin");
          if ((proto !== null && proto !== "https" && proto !== "http") ||
              (origin !== null && origin !== external.origin) ||
              request.headers.get("sec-fetch-site") === "cross-site") {
            // Let tRPC encode both batch and non-batch errors for its clients.
            throw new TRPCError({ code: "FORBIDDEN", message: "Cross-origin mutation rejected" });
          }
        }
        return { ...dependencies, repo, startTime, request, session: c.get("session") };
      },
    });
    response.headers.set("Cache-Control", response.headers.get("Content-Type")?.startsWith("text/event-stream")
      ? "no-store, no-transform" : "no-store");
    return response;
  });
  return app;
}
