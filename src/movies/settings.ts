import { z } from "zod";
import { defaultMovieQualityPolicy, movieQualityPolicySchema } from "./quality";
import { seerrSettingsSchema } from "./seerr";

export const movieShadowSettingsSchema = z
  .object({
    enabled: z.boolean().default(false),
    radarrUrl: z.string().default(""),
    radarrApiKey: z.string().default(""),
    pollIntervalMinutes: z.number().int().min(1).max(1440).default(5),
    lookbackDays: z.number().int().min(1).max(90).default(7),
    maxCasesPerSync: z.number().int().min(1).max(50).default(5),
    pathMappings: z
      .array(
        z
          .object({
            remoteRoot: z.string().min(1),
            localRoot: z.string().min(1),
          })
          .strict(),
      )
      .default([]),
  })
  .strict();

export const movieSettingsSchema = z
  .object({
    shadow: movieShadowSettingsSchema.prefault({}),
    seerr: seerrSettingsSchema.prefault({}),
    tmdbAccessToken: z.string().default(""),
    managedRoot: z.string().default(""),
    plexUrl: z.string().default(""),
    plexToken: z.string().default(""),
    plexSectionId: z.string().default(""),
    plexViewPath: z.string().default(""),
    crossSeedPreference: z.enum(["off", "tie-break"]).default("tie-break"),
    maximumReleaseBytes: z
      .number()
      .int()
      .positive()
      .safe()
      .nullable()
      .default(null),
    qualityPolicy: movieQualityPolicySchema.default(defaultMovieQualityPolicy),
  })
  .strict();
export type MovieSettings = z.infer<typeof movieSettingsSchema>;
export function defaultMovieSettings(): MovieSettings {
  return movieSettingsSchema.parse({});
}
