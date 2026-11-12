import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";

const title = z.object({ kind: z.enum(["movie", "tv"]), tmdbId: z.number().int().positive().safe() }).strict();
const watch = z.object({ status: z.enum(["unavailable", "eligible", "watching"]) });

export const notificationsRouter = defineRouter({
  watchStatus: defineMethod({
    auth: "user", readOnly: true, summary: "Check whether to watch this title without adding a media request.",
    paramsSchema: title, resultSchema: watch,
    handler: (ctx, p) => ({ status: ctx.repo.notifications.watchStatus(ctx.session!.user_id, p) }),
  }),
  setWatch: defineMethod({
    auth: "user", summary: "Opt in or out of availability email for already-requested media.",
    paramsSchema: title.extend({ enabled: z.boolean() }), resultSchema: watch,
    handler: (ctx, p) => ({ status: ctx.repo.notifications.setWatch(ctx.session!.user_id, p, p.enabled) }),
  }),
});
