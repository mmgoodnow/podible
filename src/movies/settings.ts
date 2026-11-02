import { z } from "zod";
import { defaultMovieQualityPolicy, movieQualityPolicySchema } from "./quality";

export const movieSettingsSchema = z
  .object({
    enabled: z.boolean().default(false),
    tmdbAccessToken: z.string().default(""),
    managedRoot: z.string().default(""),
    plexUrl: z.string().default(""),
    plexToken: z.string().default(""),
    plexSectionId: z.string().default(""),
    plexViewPath: z.string().default(""),
    automaticUpgrades: z.boolean().default(true),
    searchIntervalHours: z.number().min(1).max(168).default(24),
    maximumReleaseBytes: z
      .number()
      .int()
      .positive()
      .default(40 * 1024 ** 3),
    qualityPolicy: movieQualityPolicySchema.default(defaultMovieQualityPolicy),
  })
  .strict();
export type MovieSettings = z.infer<typeof movieSettingsSchema>;
export function defaultMovieSettings(): MovieSettings {
  return movieSettingsSchema.parse({});
}
