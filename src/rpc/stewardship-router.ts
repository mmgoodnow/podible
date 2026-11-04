import { defineMethod, defineRouter } from "./framework";
import { TorrentStewardship, type StewardshipAdapters } from "../media/torrent-stewardship";
import { stewardshipStatusParamsSchema, stewardshipStatusSchema, stewardshipCollectParamsSchema,
  stewardshipCollectResultSchema } from "../media/torrent-stewardship-types";

export function createStewardshipRouter(adapters: StewardshipAdapters = {}) {
  return defineRouter({
    status: defineMethod({ auth: "admin", readOnly: true,
      summary: "Read durable private-torrent health and seed protection; never collects or writes on page load.",
      paramsSchema: stewardshipStatusParamsSchema, resultSchema: stewardshipStatusSchema,
      handler: (ctx, input) => new TorrentStewardship(ctx.repo, adapters).status(input),
    }),
    collect: defineMethod({ auth: "admin",
      summary: "Read the next bounded private Transmission batch and persist sanitized observations; never sends control commands.",
      paramsSchema: stewardshipCollectParamsSchema, resultSchema: stewardshipCollectResultSchema,
      handler: (ctx, input) => new TorrentStewardship(ctx.repo, adapters).collect(input),
    }),
  });
}
export const stewardshipRouter = createStewardshipRouter();
