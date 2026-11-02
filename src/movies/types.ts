import { z } from "zod";

export const movieSchema = z.object({
  id: z.number().int().positive(),
  tmdbId: z.number().int().positive(),
  title: z.string(),
  originalTitle: z.string(),
  year: z.number().int().nullable(),
  overview: z.string(),
  posterUrl: z.string().nullable(),
  originalLanguage: z.string(),
  imdbId: z.string().nullable(),
});
export const catalogMovieSchema = movieSchema.omit({ id: true });
export type CatalogMovie = z.infer<typeof catalogMovieSchema>;
export type Movie = z.infer<typeof movieSchema>;

export const movieStages = [
  "requested",
  "searching",
  "selecting",
  "downloading",
  "verifying",
  "publishing",
  "available",
  "blocked",
  "replacing",
  "retained",
] as const;
export type MovieStage = (typeof movieStages)[number];
export const movieEventSchema = z.object({
  id: z.number(),
  stage: z.enum(movieStages),
  message: z.string(),
  createdAt: z.string(),
});
