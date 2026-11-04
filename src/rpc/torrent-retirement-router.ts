import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";
import { TorrentRetirement, type TorrentRetirementAdapters } from "../media/torrent-retirement";
import { createOrphanRetirementFence } from "../media/torrent-retirement-admission";
import { retirementGetParamsSchema, retirementIntentViewSchema, retirementPreviewParamsSchema, retirementPreviewSchema } from "../media/torrent-retirement-types";

/** Read-only admin inspection. Retirement runs only under owned durable execution, never inside an HTTP mutation. */
export function createTorrentRetirementRouter(adapters: TorrentRetirementAdapters = {}) {
  return defineRouter({
    preview: defineMethod({ auth: "admin", readOnly: true,
      summary: "Preview narrow orphan-only retirement evidence, configuration and consumer blockers; never removes records.",
      paramsSchema: retirementPreviewParamsSchema, resultSchema: retirementPreviewSchema,
      handler: (ctx, input) => new TorrentRetirement(ctx.repo, { ...adapters,
        fence: adapters.fence ?? createOrphanRetirementFence(ctx.repo, {
          globalMaintenanceHeld: () => false, readIdentityDigest: adapters.readIdentityDigest,
        }),
      }).preview(input),
    }),
    get: defineMethod({ auth: "admin", readOnly: true, summary: "Read durable record-only retirement intent without raw daemon identity, paths or tracker data.",
      paramsSchema: retirementGetParamsSchema, resultSchema: retirementIntentViewSchema,
      handler(ctx, input) {
        const intent = new TorrentRetirement(ctx.repo, adapters).get(input.operationId);
        if (!intent) throw new RpcError(-32602, "Torrent retirement intent not found");
        return intent;
      },
    }),
  });
}
export const torrentRetirementRouter = createTorrentRetirementRouter();
