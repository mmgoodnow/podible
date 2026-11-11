import path from "node:path";
import { z } from "zod";
import { resolveLibraryStorage } from "../settings";
import { indexerDiscoveryStatus } from "../media/indexer-discovery";
import { RpcError } from "./shared";

import { defineMethod, defineRouter } from "./framework";
import { appSettingsSchema, emptyParamsSchema } from "./schemas";

const notifications = appSettingsSchema.shape.notifications;
const updateSettingsSchema = appSettingsSchema.extend({
  notifications: notifications.extend({
    email: notifications.shape.email.optional(),
    pushover: notifications.shape.pushover.partial({ lifecycleEnabled: true }),
  }),
});

export const settingsRouter = defineRouter({
  indexers: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "List discovered Prowlarr indexers without endpoints or credentials.",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      state: z.enum(["not-configured", "available", "unavailable"]),
      indexers: z.array(z.object({
        id: z.number(), name: z.string(),
        search: z.array(z.enum(["audio", "ebook", "movie", "tv"])),
        feeds: z.array(z.enum(["audio", "ebook", "movie", "tv"])),
        retryAt: z.number().nullable(),
      })),
      duplicates: z.array(z.object({ index: z.number(), name: z.string() })),
    }),
    handler: ctx => indexerDiscoveryStatus(ctx.repo.getSettings()),
  }),
  get: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Read current application settings.",
    paramsSchema: emptyParamsSchema,
    resultSchema: appSettingsSchema,
    async handler(ctx) {
      return ctx.repo.getSettings();
    },
  }),

  update: defineMethod({
    auth: "admin",
    summary: "Replace application settings.",
    paramsSchema: emptyParamsSchema.extend({
      settings: updateSettingsSchema,
    }),
    resultSchema: appSettingsSchema,
    handler(ctx, params) {
      return ctx.repo.transaction(() => {
        const current = ctx.repo.getSettings();
        const incoming = params.settings;
        const root = incoming.storageRoot?.trim();
        if (root && (!path.isAbsolute(root) || path.resolve(root) === path.parse(root).root))
          throw new RpcError(-32602, "Choose an absolute library directory, not the filesystem root.");
        if (current.storageRoot && !root)
          throw new RpcError(-32602, "A library root cannot be cleared.");
        const next = resolveLibraryStorage({
          ...incoming, storageRoot: root || undefined, libraryRoot: current.libraryRoot,
          notifications: {
            delivery: incoming.notifications.delivery ?? current.notifications.delivery,
            email: incoming.notifications.email ?? current.notifications.email,
            pushover: {
              ...incoming.notifications.pushover,
              lifecycleEnabled: incoming.notifications.pushover.lifecycleEnabled ?? current.notifications.pushover.lifecycleEnabled,
            },
          },
          movies: { ...incoming.movies, managedRoot: current.movies.managedRoot, plexViewPath: current.movies.plexViewPath },
          tv: { ...incoming.tv, managedRoot: current.tv.managedRoot, plexViewPath: current.tv.plexViewPath },
        });
        if (next.storageRoot !== current.storageRoot && ctx.repo.hasLibraryStorageHistory())
          throw new RpcError(-32602, "Changing the library root requires a storage migration.");
        return ctx.repo.updateSettings(next);
      });
    },
  }),
});
