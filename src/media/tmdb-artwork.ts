import { z } from "zod";

export function tmdbImageUrl(path: string | null | undefined, size: "w185" | "w342" | "w1280") {
  return path && /^\/[a-zA-Z0-9._-]+$/.test(path) ? `https://image.tmdb.org/t/p/${size}${path}` : null;
}

export const backdropUrlSchema = z.string()
  .regex(/^https:\/\/image\.tmdb\.org\/t\/p\/w1280\/[a-zA-Z0-9._-]+$/)
  .nullable().catch(null).optional();
