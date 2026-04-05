import type { AppSettings } from "../types";

import { defineMethod, defineRouter } from "./framework";
import { appSettingsSchema, emptyParamsSchema } from "./schemas";

export const settingsRouter = defineRouter({
  settings: defineRouter({
    get: defineMethod({
      auth: "admin",
      readOnly: true,
      summary: "Read current application settings.",
      paramsSchema: emptyParamsSchema,
      async handler(ctx) {
        return ctx.repo.getSettings();
      },
    }),

    update: defineMethod({
      auth: "admin",
      summary: "Replace application settings.",
      paramsSchema: emptyParamsSchema.extend({
        settings: appSettingsSchema,
      }),
      async handler(ctx, params) {
        return ctx.repo.updateSettings(params.settings as AppSettings);
      },
    }),
  }),
});
