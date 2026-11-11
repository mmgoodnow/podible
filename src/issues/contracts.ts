import { z } from "zod";

const id = z.number().int().positive();
const movie = z.strictObject({ media: z.literal("movie"), tmdbId: id });
const show = z.strictObject({ media: z.literal("tv"), entryId: id });
export const screenIssueTitleSchema = z.discriminatedUnion("media", [movie, show]);
const subject = z.discriminatedUnion("media", [movie, show.extend({ episodeId: id.optional() })]);
const category = z.enum(["playback", "audio-subtitles", "wrong-content", "missing", "other"]);
const finding = z.enum(["content-loss-recorded", "publication-recorded", "no-publication-recorded"]);

export const MAX_SCREEN_ISSUE_DESCRIPTION = 1000;
const description = z.string().max(MAX_SCREEN_ISSUE_DESCRIPTION).trim().min(1)
  .refine(value => !/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(value), "Description contains unsupported control characters");
export const createScreenIssueSchema = z.strictObject({ subject, category, description });
const page = { beforeId: id.optional(), limit: z.number().int().min(1).max(50).default(20) };
export const listScreenIssuesSchema = z.strictObject({ ...page, title: screenIssueTitleSchema.optional() });
export const screenIssueInboxSchema = z.strictObject(page);
export const screenIssueSchema = z.object({
  id, subject, title: z.string(), episodeTitle: z.string().nullable(), category, description,
  status: z.literal("open"), createdAt: z.iso.datetime(),
  triage: z.object({ finding, checkedAt: z.iso.datetime() }),
});
export const screenIssuePageSchema = z.object({ reports: z.array(screenIssueSchema), nextBeforeId: id.nullable() });

export type ScreenIssueTitle = z.infer<typeof screenIssueTitleSchema>;
export type ScreenIssueSubject = z.infer<typeof subject>;
export type ScreenIssueCategory = z.infer<typeof category>;
export type ScreenIssueFinding = z.infer<typeof finding>;
export type CreateScreenIssue = z.infer<typeof createScreenIssueSchema>;
export type ScreenIssue = z.infer<typeof screenIssueSchema>;
export type ListScreenIssues = z.infer<typeof listScreenIssuesSchema>;
export type ScreenIssuePage = z.infer<typeof screenIssuePageSchema>;
