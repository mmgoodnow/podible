import { z } from "zod";
import { backdropUrlSchema } from "../media/tmdb-artwork";

/** Only public provider artwork, never credentialed/local Sonarr media-cover URLs. */
export function tvPosterUrl(value: unknown): string | null {
  if (typeof value !== "string" || value.length > 4096) return null;
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.port || url.search || url.hash) return null;
    const allowed = url.hostname === "image.tmdb.org" ? /^\/t\/p\/(?:w\d+|original)\/[a-zA-Z0-9._-]+$/.test(url.pathname)
      : ["artworks.thetvdb.com", "thetvdb.com", "www.thetvdb.com"].includes(url.hostname) && url.pathname.startsWith("/banners/");
    return allowed ? url.href : null;
  } catch { return null; }
}
export const tvPosterUrlSchema = z.string().refine(value => tvPosterUrl(value) !== null).nullable().catch(null).default(null);
export function tvPosterFromImages(images: unknown): string | null {
  if (!Array.isArray(images) || images.length > 100) return null;
  for (const image of images) {
    if (!image || typeof image !== "object" || typeof image.coverType !== "string" || image.coverType.toLowerCase() !== "poster") continue;
    const poster = tvPosterUrl(image.remoteUrl) ?? tvPosterUrl(image.url);
    if (poster) return poster;
  }
  return null;
}

export const tvProviders = ["tmdb", "tvdb", "anilist", "anidb", "sonarr"] as const;
export const tvCatalogKeySchema = z.string().regex(
  /^(?:(?:tvdb|tmdb|anilist|anidb):[1-9]\d*|sonarr:.+:[1-9]\d*)$/,
  "Expected an explicit provider entry key",
);
export const tvExternalRefSchema = z.object({
  provider: z.enum(tvProviders),
  id: z.string().min(1),
  instance: z.string().min(1).nullable().default(null),
  source: z.string().min(1),
}).refine((ref) => ref.provider !== "sonarr" || ref.instance !== null, {
  message: "Sonarr references require an instance",
});
export const tvNumberingSchema = z.object({
  scheme: z.string().min(1),
  season: z.number().int().nonnegative().nullable(),
  episode: z.number().int().nonnegative().nullable(),
  absolute: z.number().int().positive().nullable(),
  source: z.string().min(1).optional(),
}).refine((n) => n.season !== null || n.episode !== null || n.absolute !== null, {
  message: "A numbering map needs at least one provider number",
});
export const tvCatalogEpisodeSchema = z.object({
  key: z.string().regex(
    /^(?:(?:tvdb|tmdb|anilist|anidb|sonarr):.+|ephemeral:(?:tvdb|tmdb|anilist|anidb|sonarr):.+)$/,
    "Expected a provider episode ID or an explicit ephemeral provider key",
  ),
  title: z.string(),
  airDate: z.string().nullable(),
  runtimeMinutes: z.number().positive().nullable(),
  numbering: z.array(tvNumberingSchema),
  refs: z.array(tvExternalRefSchema).default([]),
  identity: z.object({
    stability: z.enum(["provider", "ephemeral"]),
    source: z.string().min(1),
  }).optional(),
  correspondences: z.array(z.object({
    provider: z.literal("tmdb"), workId: z.number().int().positive(), episodeId: z.number().int().positive(),
    season: z.number().int().nonnegative(), episode: z.number().int().positive(),
    fingerprint: z.string().regex(/^[a-f0-9]{64}$/), method: z.enum(["provider_id", "title_date", "agent"]),
    contextFingerprint: z.string().regex(/^[a-f0-9]{64}$/).optional(),
    reason: z.string().min(1).max(2000), model: z.string().nullable(),
  })).optional(),
}).refine((ep) => !ep.key.startsWith("ephemeral:") || ep.identity?.stability !== "provider", {
  message: "An ephemeral episode key cannot claim provider-stable identity",
});
export const tvRelationSchema = z.object({
  key: tvCatalogKeySchema,
  title: z.string(),
  relation: z.string().min(1),
  source: z.string().min(1),
});
export const tvCatalogEntrySchema = z.object({
  key: tvCatalogKeySchema,
  tvdbId: z.number().int().positive().nullable(),
  tmdbId: z.number().int().positive().nullable(),
  title: z.string().min(1),
  aliases: z.array(z.string()),
  year: z.number().int().nullable(),
  posterUrl: tvPosterUrlSchema,
  backdropUrl: backdropUrlSchema,
  // Provider enrichment hint only. Work hierarchy/relations/numbering are generic, not gated by animation.
  anime: z.boolean(),
  episodes: z.array(tvCatalogEpisodeSchema),
  relations: z.array(tvRelationSchema),
  refs: z.array(tvExternalRefSchema).default([]),
  source: z.string().min(1).optional(),
});

// Provider numbering and aggregate series references never participate in identity.
export type TvCatalogEntry = z.input<typeof tvCatalogEntrySchema>;
export type TvCatalogEpisode = z.input<typeof tvCatalogEpisodeSchema>;
export type TvExternalRef = z.output<typeof tvExternalRefSchema>;
export type TvEpisode = Omit<z.output<typeof tvCatalogEpisodeSchema>, "identity" | "numbering"> & {
  id: number;
  identity: { stability: "provider" | "ephemeral"; source: string };
  numbering: (z.output<typeof tvNumberingSchema> & { source: string })[];
};
export type TvEntry = Omit<z.output<typeof tvCatalogEntrySchema>, "episodes"> & {
  id: number;
  episodes: TvEpisode[];
};
export type TvPolicy = Record<string, unknown>;
export type TvTarget = {
  id: number;
  entry_id: number;
  policy_json: string;
  revision: number;
  created_at: string;
};
export type TvRequest = {
  id: number;
  user_id: number;
  target_id: number;
  request_key: string;
  withdrawn_at: string | null;
  created_at: string;
};
export type TvRequestOrigin = {
  instance: string;
  externalRequestId: number;
  source: unknown;
};
export type TvRequestResult = {
  requestId: number;
  targetId: number;
  entryId: number;
  episodeIds: number[];
};
export type TvAcquisition = {
  id: number;
  target_id: number;
  request_id: number | null;
  reason: "initial" | "upgrade" | "recovery";
  job_id: number | null;
  decision_json: string | null;
  finished_at: string | null;
  created_at: string;
};
export const tvContentFileSchema = z.object({
  id: z.string().min(1),
  relativePath: z.string().min(1).refine(
    (p) => !p.startsWith("/") && !p.includes("\\") && p.split("/").every((s) => s !== ".." && s !== "." && s !== ""),
    "Content path must remain relative to managed content",
  ),
  sizeBytes: z.number().int().nonnegative().safe(),
  probe: z.unknown(),
  episodeIds: z.array(z.number().int().positive()).min(1),
});
export type TvContentFile = z.infer<typeof tvContentFileSchema>;
export type TvVersion = {
  id: number;
  entryId: number;
  targetId: number;
  acquisitionId: number;
  facts: unknown;
  files: TvContentFile[];
  episodeIds: number[];
};
export type TvPublication = {
  target_id: number;
  version_id: number;
  expected_revision: number;
  created_at: string;
};
export type TvActiveEpisode = { episode_id: number; version_id: number };
export const tvStages = [
  "requested", "searching", "selecting", "downloading", "verifying",
  "publishing", "available", "blocked", "replacing", "retained",
] as const;
export type TvStage = (typeof tvStages)[number];
export type TvEvent = {
  id: number;
  acquisitionId: number | null;
  stage: TvStage;
  message: string;
  createdAt: string;
};
