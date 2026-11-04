import { z } from "zod";

export const tvSettingsSchema = z.object({
  sonarrUrl: z.string().default(""),
  sonarrApiKey: z.string().default(""),
  managedRoot: z.string().default(""),
  plexSectionId: z.string().default(""),
  plexViewPath: z.string().default(""),
  pollIntervalMinutes: z.number().int().min(1).max(60).default(1),
  lookbackDays: z.number().int().min(1).max(90).default(7),
  maxCasesPerSync: z.number().int().min(1).max(50).default(5),
  maximumReleaseBytes: z.number().int().positive().safe().default(40 * 1024 ** 3),
  pathMappings: z.array(z.object({ remoteRoot: z.string().min(1), localRoot: z.string().min(1) }).strict()).default([]),
}).strict();
export type TvSettings = z.infer<typeof tvSettingsSchema>;
