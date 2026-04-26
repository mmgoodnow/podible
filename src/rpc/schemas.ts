import { z } from "zod";

import type { AppSettings } from "../app-types";

const emptyToUndefined = (value: unknown) => {
  if (value === undefined || value === null || value === "") return undefined;
  return value;
};

const booleanishSchema = z.union([
  z.boolean(),
  z.enum(["true", "false"]).transform((value) => value === "true"),
]);

export const anyParamsSchema = z.object({}).catchall(z.unknown());
export const emptyParamsSchema = z.object({}).catchall(z.unknown());
export const nonEmptyStringSchema = z.string().trim().min(1);
export const optionalStringSchema = z.preprocess(emptyToUndefined, z.string().optional());
export const stringArraySchema = z.array(nonEmptyStringSchema);
export const optionalStringArraySchema = z.preprocess((value) => {
  const normalized = emptyToUndefined(value);
  if (normalized === undefined) return undefined;
  return Array.isArray(normalized) ? normalized : [normalized];
}, stringArraySchema.optional());

export const positiveIntSchema = z.coerce.number().int().positive();
export const optionalPositiveIntSchema = z.preprocess(emptyToUndefined, positiveIntSchema.optional());
export const positiveIntArraySchema = z.array(positiveIntSchema);
export const optionalPositiveIntArraySchema = z.preprocess((value) => {
  const normalized = emptyToUndefined(value);
  if (normalized === undefined) return undefined;
  return Array.isArray(normalized) ? normalized : [normalized];
}, positiveIntArraySchema.optional());

export const optionalBooleanSchema = z.preprocess(emptyToUndefined, booleanishSchema.optional());
export const limitSchema = z.preprocess(emptyToUndefined, z.coerce.number().int().positive().max(200).optional()).transform((value) => value ?? 50);

export const mediaSchema = z.enum(["audio", "ebook"]);
export const mediaSelectionSchema = z
  .preprocess((value) => {
    const normalized = emptyToUndefined(value);
    if (normalized === undefined) return ["audio", "ebook"];
    return Array.isArray(normalized) ? normalized : [normalized];
  }, z.array(mediaSchema).min(1))
  .transform((value) => Array.from(new Set(value)));

export const jobTypeSchema = z.enum([
  "full_library_refresh",
  "acquire",
  "download",
  "import",
  "reconcile",
  "chapter_analysis",
]);

export const appSettingsSchema: z.ZodType<AppSettings> = z.object({
  torznab: z.array(
    z.object({
      name: z.string(),
      baseUrl: z.string(),
      apiKey: z.string().optional(),
      categories: z
        .object({
          audio: z.string().optional(),
          ebook: z.string().optional(),
        })
        .optional(),
    })
  ),
  rtorrent: z.object({
    transport: z.literal("http-xmlrpc"),
    url: z.string(),
    username: z.string().optional(),
    password: z.string().optional(),
    downloadPath: z.string().optional(),
  }),
  libraryRoot: z.string(),
  polling: z.object({
    rtorrentMs: z.number(),
  }),
  recovery: z.object({
    stalledTorrentMinutes: z.number(),
  }),
  feed: z.object({
    title: z.string(),
    author: z.string(),
  }),
  auth: z.object({
    mode: z.literal("plex"),
    appRedirectURIs: z.array(z.string()),
    plex: z.object({
      productName: z.string(),
      ownerToken: z.string(),
      machineId: z.string(),
    }),
  }),
  agents: z.object({
    provider: z.literal("openai-responses"),
    model: z.string(),
    apiKey: z.string(),
    editionPreference: z.string(),
    lowConfidenceThreshold: z.number(),
    timeoutMs: z.number(),
  }),
  notifications: z.object({
    pushover: z.object({
      enabled: z.boolean(),
      apiToken: z.string(),
      userKey: z.string(),
    }),
  }),
});

export const stringRecordSchema = z.record(z.string(), z.string());
export const countMapSchema = z.record(z.string(), z.number().int().nonnegative());
export const anyObjectSchema = z.object({}).passthrough();
export const okResultSchema = z.object({ ok: z.literal(true) });
export const jobIdResultSchema = z.object({ jobId: positiveIntSchema });
export const decisionModeSchema = z.enum(["deterministic", "agent"]);
export const decisionTriggerSchema = z.enum(["none", "forced", "prior_failure", "low_confidence"]);

export const libraryBookSchema = z.object({
  id: positiveIntSchema,
  title: z.string(),
  author: z.string(),
  coverUrl: z.string().nullable(),
  durationMs: z.number().int().nullable(),
  wordCount: z.number().int().nullable(),
  addedAt: z.string(),
  updatedAt: z.string(),
  publishedAt: z.string().nullable(),
  description: z.string().nullable(),
  descriptionHtml: z.string().nullable(),
  language: z.string().nullable(),
  identifiers: stringRecordSchema,
  audioStatus: z.enum(["wanted", "snatched", "downloading", "downloaded", "imported", "error"]),
  ebookStatus: z.enum(["wanted", "snatched", "downloading", "downloaded", "imported", "error"]),
  status: z.enum(["wanted", "snatched", "downloading", "downloaded", "imported", "error", "partial"]),
  fullPseudoProgress: z.number(),
});

export const libraryPlaybackSchema = z.object({
  audio: z
    .object({
      manifestationId: positiveIntSchema,
      label: z.string().nullable(),
      editionNote: z.string().nullable(),
      streamUrl: z.string(),
      chaptersUrl: z.string(),
      transcriptUrl: z.string().nullable(),
      mimeType: z.string(),
      durationMs: z.number().int().nullable(),
      sizeBytes: z.number().int().nonnegative(),
    })
    .nullable(),
  ebook: z
    .object({
      assetId: positiveIntSchema,
      downloadUrl: z.string(),
      mimeType: z.string(),
      sizeBytes: z.number().int().nonnegative(),
    })
    .nullable(),
});

export const libraryBookWithPlaybackSchema = libraryBookSchema.extend({
  playback: libraryPlaybackSchema,
});

export const releaseRowSchema = z.object({
  id: positiveIntSchema,
  book_id: positiveIntSchema,
  provider: z.string(),
  provider_guid: z.string().nullable(),
  title: z.string(),
  media_type: mediaSchema,
  info_hash: z.string(),
  size_bytes: z.number().int().nullable(),
  url: z.string(),
  snatched_at: z.string(),
  status: z.enum(["snatched", "downloading", "downloaded", "imported", "failed"]),
  error: z.string().nullable(),
  updated_at: z.string(),
});

export const assetRowSchema = z.object({
  id: positiveIntSchema,
  book_id: positiveIntSchema,
  kind: z.enum(["single", "multi", "ebook"]),
  mime: z.string(),
  total_size: z.number().int().nonnegative(),
  duration_ms: z.number().int().nullable(),
  source_release_id: z.number().int().positive().nullable(),
  created_at: z.string(),
  updated_at: z.string(),
});

export const jobRowSchema = z.object({
  id: positiveIntSchema,
  type: jobTypeSchema,
  status: z.enum(["queued", "running", "succeeded", "failed", "cancelled"]),
  book_id: z.number().int().positive().nullable(),
  book_title: z.string().nullable().optional(),
  release_id: z.number().int().positive().nullable(),
  payload_json: z.string().nullable(),
  error: z.string().nullable(),
  attempt_count: z.number().int().nonnegative(),
  max_attempts: z.number().int().positive(),
  next_run_at: z.string().nullable(),
  created_at: z.string(),
  updated_at: z.string(),
});

export const importInspectionFileSchema = z.object({
  sourcePath: z.string(),
  relativePath: z.string(),
  ext: z.string(),
  size: z.number().int().nonnegative(),
  mtimeMs: z.number().nonnegative(),
  supportedAudio: z.boolean(),
  supportedEbook: z.boolean(),
});

export const torznabResultSchema = z.object({
  title: z.string(),
  provider: z.string(),
  mediaType: mediaSchema,
  sizeBytes: z.number().int().nullable(),
  url: z.string(),
  guid: z.string().nullable(),
  infoHash: z.string().nullable(),
  seeders: z.number().int().nullable(),
  leechers: z.number().int().nullable(),
  raw: anyObjectSchema,
});

export const searchSelectionDecisionSchema = z.object({
  selections: z.array(
    z.object({
      manifestation: z.object({
        label: z.string().nullable(),
        editionNote: z.string().nullable(),
      }),
      parts: z.array(torznabResultSchema),
    })
  ),
  confidence: z.number(),
  mode: decisionModeSchema,
  trigger: decisionTriggerSchema,
  reason: z.string(),
  error: z.string().nullable(),
});

export const manualImportSelectionDecisionSchema = z.object({
  selectedPaths: z.array(z.string()),
  confidence: z.number(),
  mode: decisionModeSchema,
  trigger: decisionTriggerSchema,
  reason: z.string(),
  error: z.string().nullable(),
});

export const openLibraryCandidateSchema = z.object({
  openLibraryKey: z.string(),
  title: z.string(),
  author: z.string(),
  publishedAt: z.string().optional(),
  language: z.string().optional(),
  coverId: z.number().int().positive().optional(),
  identifiers: stringRecordSchema,
});

export const openLibraryCoverCandidateSchema = z.object({
  coverId: positiveIntSchema,
  coverUrl: z.string(),
  source: z.enum(["work", "edition"]),
  openLibraryKey: z.string(),
  editionKey: z.string().optional(),
  title: z.string().optional(),
  publishDate: z.string().optional(),
  publisher: z.string().optional(),
  language: z.string().optional(),
  isbn: z.string().optional(),
});

export const userProfileSchema = z.object({
  id: positiveIntSchema,
  provider: z.enum(["plex", "local"]),
  username: z.string(),
  displayName: z.string().nullable(),
  thumbUrl: z.string().nullable(),
  isAdmin: z.boolean(),
});

export const sessionSummarySchema = z.object({
  kind: z.enum(["browser", "app"]),
  expiresAt: z.string(),
});

export const downloadProgressSchema = z.object({
  bytesDone: z.number().int().nullable(),
  sizeBytes: z.number().int().nullable(),
  leftBytes: z.number().int().nullable(),
  downRate: z.number().int().nullable(),
  fraction: z.number().nullable(),
  percent: z.number().int().nullable(),
});

export const downloadRpcViewSchema = z.object({
  job_id: positiveIntSchema,
  job_type: jobTypeSchema,
  job_status: z.enum(["queued", "running", "succeeded", "failed", "cancelled"]),
  job_error: z.string().nullable(),
  release_id: z.number().int().positive().nullable(),
  release_status: z.enum(["snatched", "downloading", "downloaded", "imported", "failed"]).nullable(),
  release_error: z.string().nullable(),
  media_type: mediaSchema.nullable(),
  info_hash: z.string().nullable(),
  book_id: z.number().int().positive().nullable(),
  fullPseudoProgress: z.number(),
  downloadProgress: downloadProgressSchema.optional(),
});
