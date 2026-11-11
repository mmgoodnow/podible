// Loopback-only tvOS fixture: synthetic catalog and session, real Plex trailer route.
import { Hono } from "hono";
import { hashSessionToken } from "../../src/auth";
import { openDatabase } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createRequestContextMiddleware, getCurrentSession, type HttpEnv } from "../../src/http/middleware";
import { createTrailerRoutes } from "../../src/http/trailer-routes";
import { createTrpcRoutes } from "../../src/http/trpc-routes";
import { AcquisitionEvents } from "../../src/runtime/acquisition-events";

const [keyFile, certFile, plexPort, movieSection, showSection] = process.argv.slice(2);
if (!keyFile || !certFile || !plexPort || !/^\d{1,5}$/.test(plexPort) || !movieSection || !showSection ||
    !process.env.PODIBLE_PLEX_TRAILERS) {
  throw new Error("Provide TLS key, certificate, loopback Plex port, movie and TV section IDs, and PODIBLE_PLEX_TRAILERS.");
}
const repo = new BooksRepo(openDatabase(":memory:"));
const settings = repo.getSettings();
repo.updateSettings({ ...settings, movies: { ...settings.movies, plexUrl: `http://127.0.0.1:${plexPort}`,
  plexToken: "isolated-fixture", plexSectionId: movieSection }, tv: { ...settings.tv, plexSectionId: showSection } });
const user = repo.upsertUser({ provider: "local", providerUserId: "fixture", username: "trailer-fixture", isAdmin: false });
repo.createSession(user.id, hashSessionToken("trailer-fixture-viewer"), null, "app");

const movie = { tmdbId: 10378, title: "Big Buck Bunny", year: 2008 };
const noTrailer = { tmdbId: 550, title: "Unapproved Trailer Fixture", year: 1999 };
const show = { tmdbId: 2316, title: "The Office (US)", year: 2005 };
const app = new Hono<HttpEnv>();
app.use("*", createRequestContextMiddleware(repo));
app.route("/api/trailers", createTrailerRoutes(repo));
app.route("/api/trpc", createTrpcRoutes(repo, Date.now(), { acquisitionEvents: new AcquisitionEvents() }));
app.post("/rpc", async (c) => {
  if (!getCurrentSession(c)) return c.body(null, 401);
  const { method, id, params } = await c.req.json();
  let result: unknown;
  switch (method) {
    case "catalog.available": result = { movies: [], tv: [] }; break;
    case "catalog.discovery": result = { movies: { items: [movie, noTrailer] }, tv: { items: [show] } }; break;
    case "movies.get": result = { movie: params.tmdbId === movie.tmdbId ? movie : noTrailer, requests: [], availableInLibrary: true }; break;
    case "movies.status": result = { catalogConfigured: true, acquisitionConfigured: false }; break;
    case "tv.details": result = { series: { ...show, overview: "", seasons: [] }, seasons: [], specials: [] }; break;
    default: return c.json({ jsonrpc: "2.0", id, error: { code: -32601 } });
  }
  return c.json({ jsonrpc: "2.0", id, result });
});
Bun.serve({
  hostname: "127.0.0.1", port: 43243, fetch: app.fetch,
  tls: { key: Bun.file(keyFile), cert: Bun.file(certFile) },
});
console.log("tvOS trailer fixture listening at https://localhost:43243");
