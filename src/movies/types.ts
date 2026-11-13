import { z } from "zod";
import { backdropUrlSchema } from "../media/tmdb-artwork";

export const movieSchema = z.object({
  id: z.number().int().positive(),
  tmdbId: z.number().int().positive(),
  title: z.string(),
  originalTitle: z.string(),
  year: z.number().int().nullable(),
  overview: z.string(),
  posterUrl: z.string().nullable(),
  backdropUrl: backdropUrlSchema,
  originalLanguage: z.string(),
  imdbId: z.string().nullable(),
  runtimeMinutes: z.number().positive().nullable().optional(),
  releaseDate: z.iso.date().optional(),
});
export const catalogMovieSchema = movieSchema.omit({ id: true });
export type CatalogMovie = z.infer<typeof catalogMovieSchema>;
export type Movie = z.infer<typeof movieSchema>;

export const moviePageExtrasSchema = z.object({
  runtimeMinutes: z.number().int().positive().nullable(),
  genres: z.array(z.string()).max(8),
  directors: z.array(z.string()).max(8),
  cast: z.array(z.object({
    id: z.number().int().positive(), name: z.string(), character: z.string(), profileUrl: z.string().nullable(),
  })).max(12),
  related: z.array(catalogMovieSchema.pick({ tmdbId: true, title: true, year: true, posterUrl: true })).max(12),
});
export type MoviePageExtras = z.infer<typeof moviePageExtrasSchema>;

export const movieStages = [
  "requested",
  "searching",
  "selecting",
  "downloading",
  "verifying",
  "publishing",
  "available",
  "blocked",
  "waiting",
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
