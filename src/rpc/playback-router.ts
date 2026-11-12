import { z } from "zod";
import { playbackIdSchema, playbackService, playbackShowSchema, playbackTargetSchema } from "../media/playback-service";
import { PlaybackError } from "../media/plex-playback";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";

async function safe<T>(fn: () => T | Promise<T>): Promise<T> {
  try { return await fn(); }
  catch (error) {
    // Never forward upstream responses, URLs, or credentials to the RPC client or logs.
    switch (error instanceof PlaybackError ? error.status : null) {
      case 401: throw new RpcError(-32001, "Sign in on this Apple TV again.");
      case 403: throw new RpcError(-32003, "This Apple TV session does not have Plex playback access.");
      case 404: throw new RpcError(-32044, "No exact Plex match is available for playback.");
      case 415: throw new RpcError(-32015, "This edition does not have a supported playback stream.");
      case 429: throw new RpcError(-32029, "Too many playback sessions. Close a player and try again.");
      default: throw new RpcError(-32050, "Plex playback is unavailable.");
    }
  }
}

const id = z.number().int().positive();
export const playbackRouter = defineRouter({
  showEpisodes: defineMethod({
    auth: "user", readOnly: true, summary: "List published episodes visible to this app session's Plex viewer.",
    paramsSchema: playbackShowSchema,
    resultSchema: z.object({ entryId: id, episodes: z.array(z.object({ id, title: z.string(),
      season: z.number().int().nonnegative().nullable(), episode: z.number().int().nonnegative().nullable(), playable: z.boolean() })) }),
    handler: (ctx, input) => safe(() => playbackService(ctx.repo).showEpisodes(ctx.request, input)),
  }),
  prepare: defineMethod({
    auth: "user", summary: "Create a session-bound playback grant for one movie or episode.",
    paramsSchema: playbackTargetSchema,
    resultSchema: z.object({ id: playbackIdSchema, path: z.string(), credential: z.string().regex(/^[A-Za-z0-9_-]{43}$/),
      expiresInSeconds: z.number().int().positive() }),
    handler: (ctx, input) => safe(() => playbackService(ctx.repo).prepare(ctx.request, input)),
  }),
  close: defineMethod({
    auth: "user", summary: "Close a playback grant belonging to this app session.",
    paramsSchema: z.strictObject({ id: playbackIdSchema }), resultSchema: z.object({ ok: z.literal(true) }),
    handler: (ctx, { id }) => safe(() => playbackService(ctx.repo).close(ctx.request, id)),
  }),
});
