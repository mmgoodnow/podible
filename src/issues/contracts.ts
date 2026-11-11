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

export const screenIssueEvidenceSchema = z.strictObject({
  scope: z.enum(["movie", "tv-show", "tv-episode"]),
  category,
  filedFinding: finding,
  currentFinding: finding,
});
export const screenIssueDecisionSchema = z.strictObject({
  focus: z.enum(["recorded-content-loss", "publication-gap", "library-availability-unverified", "playback-unverified",
    "tracks-unverified", "identity-unverified", "inconclusive"]),
});
export const screenIssueCheckForFocus = {
  "recorded-content-loss": "preview-recovery",
  "publication-gap": "preview-recovery",
  "library-availability-unverified": "preview-recovery",
  "playback-unverified": "check-plex-playback",
  "tracks-unverified": "check-plex-tracks",
  "identity-unverified": "verify-content-identity",
  "inconclusive": "manual-review",
} as const satisfies Record<z.infer<typeof screenIssueDecisionSchema>["focus"], string>;
export const screenIssueAssessmentSchema = screenIssueDecisionSchema.extend({
  evidence: screenIssueEvidenceSchema,
  checkedAt: z.iso.datetime(),
  suggestedCheck: z.enum(["preview-recovery", "check-plex-playback", "check-plex-tracks", "verify-content-identity", "manual-review"]),
}).refine(({ evidence, focus, suggestedCheck }) => {
  if (screenIssueCheckForFocus[focus] !== suggestedCheck) return false;
  const published = evidence.currentFinding === "publication-recorded";
  switch (focus) {
    case "recorded-content-loss": return evidence.currentFinding === "content-loss-recorded";
    case "publication-gap": return evidence.currentFinding === "no-publication-recorded";
    case "library-availability-unverified":
    case "playback-unverified": return published && (evidence.category === "playback" || evidence.category === "missing");
    case "tracks-unverified": return published && evidence.category === "audio-subtitles";
    case "identity-unverified": return published && evidence.category === "wrong-content";
    case "inconclusive": return true;
  }
}, "Assessment is not supported by the evidence");
export const screenIssueInboxPageSchema = screenIssuePageSchema.extend({
  reports: z.array(screenIssueSchema.extend({ assessment: screenIssueAssessmentSchema.nullable() })),
});
export const diagnoseScreenIssueSchema = z.strictObject({ reportId: id });
export const screenIssueDiagnosisRequestResultSchema = z.discriminatedUnion("status", [
  z.strictObject({ status: z.literal("assessed"), assessment: screenIssueAssessmentSchema }),
  z.strictObject({ status: z.literal("job"), jobId: id }),
]);

export type ScreenIssueTitle = z.infer<typeof screenIssueTitleSchema>;
export type ScreenIssueSubject = z.infer<typeof subject>;
export type ScreenIssueCategory = z.infer<typeof category>;
export type ScreenIssueFinding = z.infer<typeof finding>;
export type CreateScreenIssue = z.infer<typeof createScreenIssueSchema>;
export type ScreenIssue = z.infer<typeof screenIssueSchema>;
export type ListScreenIssues = z.infer<typeof listScreenIssuesSchema>;
export type ScreenIssuePage = z.infer<typeof screenIssuePageSchema>;
export type ScreenIssueEvidence = z.infer<typeof screenIssueEvidenceSchema>;
export type ScreenIssueAssessment = z.infer<typeof screenIssueAssessmentSchema>;
export type ScreenIssueInboxPage = z.infer<typeof screenIssueInboxPageSchema>;
