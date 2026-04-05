import { z } from "zod";

import type { AppSettings } from "../types";

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
