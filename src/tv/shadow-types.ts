import { z } from "zod";
import { movieFactsSchema, movieQualityPolicySchema } from "../movies/quality";
import { movieProbeSchema } from "../movies/probe";
import { tvCatalogEntrySchema, tvCatalogEpisodeSchema, tvNumberingSchema } from "./types";

const id = z.number().int().positive();
const episodeSchema = tvCatalogEpisodeSchema.safeExtend({
  id,
  identity: z.object({ stability: z.enum(["provider", "ephemeral"]), source: z.string().min(1) }),
  numbering: z.array(tvNumberingSchema.safeExtend({ source: z.string().min(1) })),
});
export const tvShadowObservationSchema = z.object({
  key: z.string().min(1),
  sourceKey: z.string().min(1).optional(),
  warnings: z.array(z.string()).max(1000).optional(),
  entry: tvCatalogEntrySchema,
  episodeKeys: z.array(z.string().min(1)).min(1),
  releaseTitle: z.string(),
  sourcePaths: z.array(z.string()).max(10),
  libraryPath: z.string().nullable(),
  observedAt: z.string().datetime({ offset: true }),
  sonarr: z.object({ outcome: z.enum(["imported", "blocked"]), messages: z.array(z.string()) }),
});
export const tvShadowInputSchema = z.object({
  observation: tvShadowObservationSchema,
  entry: tvCatalogEntrySchema.extend({ id, episodes: z.array(episodeSchema) }),
  episodeIds: z.array(id).min(1),
  policy: movieQualityPolicySchema,
  files: z.array(z.object({
    path: z.string(), name: z.string(), sizeBytes: z.number().int().nonnegative(),
    mtimeMs: z.number(), ctimeMs: z.number(), device: z.number(), inode: z.number(),
  })).max(50),
  source: z.enum(["download", "library-fallback", "unavailable"]),
  warnings: z.array(z.string()),
}).superRefine((input, ctx) => {
  const keys = new Set(input.observation.episodeKeys);
  const wanted = input.entry.episodes.filter((episode) => keys.has(episode.key));
  if (input.entry.key !== input.observation.entry.key ||
      keys.size !== input.observation.episodeKeys.length || wanted.length !== keys.size ||
      new Set(input.episodeIds).size !== input.episodeIds.length ||
      input.episodeIds.length !== wanted.length ||
      !wanted.every((episode) => input.episodeIds.includes(episode.id)))
    ctx.addIssue({ code: "custom", message: "Shadow episode identity does not match the captured entry membership" });
});
export type TvShadowInput = z.infer<typeof tvShadowInputSchema>;
export const tvShadowQualitySchema = z.object({
  eligibility: z.enum(["eligible", "ineligible", "needs-verification"]),
  reasons: z.array(z.string()),
});
export const tvShadowResultSchema = z.object({
  decision: z.enum(["import", "reject", "needs-verification", "unobservable"]),
  reason: z.string().max(4000),
  files: z.array(z.object({
    index: z.number().int().nonnegative(), name: z.string(), episodeIds: z.array(id).min(1),
    reason: z.string(), facts: movieFactsSchema, quality: tvShadowQualitySchema,
  })),
  unmappedEpisodeIds: z.array(id),
  probes: z.array(z.object({ index: z.number().int().nonnegative(), name: z.string(), probe: movieProbeSchema })),
  probeErrors: z.array(z.object({ name: z.string(), message: z.string() })),
  model: z.string(),
  evaluatedAt: z.string().datetime({ offset: true }),
});
export type TvShadowResult = z.infer<typeof tvShadowResultSchema>;
export const tvShadowCaseSchema = z.object({
  id, createdAt: z.string(), jobId: id.nullable(), status: z.string(), error: z.string().nullable(),
  input: tvShadowInputSchema, result: tvShadowResultSchema.nullable(),
});
export type TvShadowCase = z.infer<typeof tvShadowCaseSchema>;
export const tvShadowStatusSchema = z.object({
  sonarrConfigured: z.boolean(), agentConfigured: z.boolean(), pathMappingCount: z.number().int().nonnegative(),
  pollIntervalMinutes: z.number(), lookbackDays: z.number(), maxCasesPerSync: z.number(),
  lastSync: z.object({ jobId: id, status: z.string(), error: z.string().nullable(), createdAt: z.string() }).nullable(),
});
