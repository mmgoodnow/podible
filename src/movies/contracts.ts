import { z } from "zod";
import { movieEventSchema, movieSchema, movieStages } from "./types";
export const movieRequestViewSchema = z.object({
  id: z.number(),
  targetId: z.number(),
  movie: movieSchema,
  createdAt: z.string(),
  state: z.enum(["queued", "running", "blocked", "available", "withdrawn"]),
  available: z.boolean(),
  upgrading: z.boolean(),
  stage: z.enum(movieStages).nullable(),
  job: z
    .object({
      id: z.number(),
      status: z.enum(["queued", "running", "succeeded", "failed", "cancelled"]),
      error: z.string().nullable(),
    })
    .nullable(),
  quality: z
    .object({
      eligibility: z.enum(["eligible", "ineligible", "needs-verification"]),
      tier: z.number().nullable(),
      preferenceScore: z.number(),
      reasons: z.array(z.string()),
      breakdown: z.array(
        z.object({ rule: z.string(), contribution: z.number() }),
      ),
    })
    .nullable(),
  events: z.array(movieEventSchema),
  decisionNote: z.string().nullable(),
  download: z
    .object({
      bytesDone: z.number().nullable(),
      sizeBytes: z.number().nullable(),
      bytesPerSecond: z.number().nullable(),
    })
    .nullable(),
});
export type MovieRequestView = z.infer<typeof movieRequestViewSchema>;
