import { z } from "zod";
import { catalogMovieSchema } from "./types";
import { movieQualityPolicySchema, movieFactsSchema } from "./quality";
import { movieProbeSchema } from "./probe";

export const shadowObservationSchema = z.object({
  key: z.string(),
  movie: catalogMovieSchema,
  releaseTitle: z.string(),
  sourcePaths: z.array(z.string()),
  libraryPath: z.string().nullable().optional(),
  observedAt: z.string(),
  radarr: z.object({
    outcome: z.enum(["imported", "blocked"]),
    messages: z.array(z.string()),
  }),
});
export type ShadowObservation = z.infer<typeof shadowObservationSchema>;
export const shadowInputSchema = z.object({
  observation: shadowObservationSchema,
  policy: movieQualityPolicySchema,
  files: z.array(
    z.object({
      path: z.string(),
      name: z.string(),
      sizeBytes: z.number(),
      mtimeMs: z.number(),
    }),
  ),
  source: z.enum(["download", "library-fallback", "unavailable"]),
  warnings: z.array(z.string()),
});
export type ShadowInput = z.infer<typeof shadowInputSchema>;
export const shadowResultSchema = z.object({
  decision: z.enum(["import", "reject", "needs-verification", "unobservable"]),
  selectedFile: z.string().nullable(),
  reason: z.string(),
  facts: movieFactsSchema.nullable(),
  quality: z
    .object({ eligibility: z.string(), reasons: z.array(z.string()) })
    .nullable(),
  probes: z.array(
    z.object({ index: z.number(), name: z.string(), probe: movieProbeSchema }),
  ),
  probeErrors: z.array(z.object({ name: z.string(), message: z.string() })),
  model: z.string(),
  evaluatedAt: z.string(),
});
export type ShadowResult = z.infer<typeof shadowResultSchema>;
export const shadowReviewSchema = z.object({
  verdict: z.enum([
    "podible-correct",
    "radarr-correct",
    "both-correct",
    "both-wrong",
    "uncertain",
  ]),
  notes: z.string().max(4000),
  userId: z.number(),
  reviewedAt: z.string(),
});
export const shadowCaseSchema = z.object({
  id: z.number(),
  createdAt: z.string(),
  jobId: z.number().nullable(),
  status: z.string(),
  error: z.string().nullable(),
  input: shadowInputSchema,
  result: shadowResultSchema.nullable(),
  review: shadowReviewSchema.nullable(),
});
