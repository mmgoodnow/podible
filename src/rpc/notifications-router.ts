import { z } from "zod";
import { defineMethod, defineRouter } from "./framework";

const noticeId = z.string().regex(/^[0-9a-f]{64}$/);
const title = z.object({ kind: z.enum(["movie", "tv"]), tmdbId: z.number().int().positive().safe() }).strict();
const watch = z.object({ status: z.enum(["unavailable", "eligible", "watching"]) });

export const notificationsRouter = defineRouter({
  list: defineMethod({
    auth: "user", readOnly: true, summary: "Read the current user's Plex availability inbox.",
    paramsSchema: z.object({ before: noticeId.optional() }).strict(),
    resultSchema: z.object({ items: z.array(z.object({ id: noticeId, title: z.string(), episode: z.string().nullable(),
      href: z.string(), createdAt: z.string(), readAt: z.string().nullable() })), next: noticeId.nullable() }),
    handler: (ctx, p) => ctx.repo.notifications.list(ctx.session!.user_id, p.before),
  }),
  markRead: defineMethod({
    auth: "user", summary: "Mark one of the current user's notifications read.",
    paramsSchema: z.object({ id: noticeId }).strict(), resultSchema: z.object({ ok: z.boolean() }),
    handler: (ctx, p) => ({ ok: ctx.repo.notifications.markRead(ctx.session!.user_id, p.id) }),
  }),
  watchStatus: defineMethod({
    auth: "user", readOnly: true, summary: "Check whether to watch this title without adding a media request.",
    paramsSchema: title, resultSchema: watch,
    handler: (ctx, p) => ({ status: ctx.repo.notifications.watchStatus(ctx.session!.user_id, p) }),
  }),
  setWatch: defineMethod({
    auth: "user", summary: "Opt in or out of in-app availability notices for already-requested media.",
    paramsSchema: title.extend({ enabled: z.boolean() }), resultSchema: watch,
    handler: (ctx, p) => ({ status: ctx.repo.notifications.setWatch(ctx.session!.user_id, p, p.enabled) }),
  }),
});
