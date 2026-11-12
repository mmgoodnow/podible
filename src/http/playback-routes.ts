import { Hono } from "hono";
import { getCookie } from "hono/cookie";
import { playbackCookie, playbackIdSchema, playbackService } from "../media/playback-service";
import { PlaybackError } from "../media/plex-playback";
import type { BooksRepo } from "../repo";
import type { HttpEnv } from "./middleware";

export function createPlaybackRoutes(repo: BooksRepo) {
  const app = new Hono<HttpEnv>();
  const headers = { "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff" };
  app.onError((error) => new Response(null, { status: error instanceof PlaybackError ? error.status : 502, headers }));
  app.get("/file/:name", async (c) => {
    const name = c.req.param("name");
    const id = playbackIdSchema.safeParse(name.endsWith(".mp4") ? name.slice(0, -4) : "");
    if (!id.success) return c.body(null, 404, headers);
    const response = await playbackService(repo).stream(c.req.raw, id.data, getCookie(c, playbackCookie));
    for (const [name, value] of Object.entries(headers)) response.headers.set(name, value);
    return response;
  });
  return app;
}
