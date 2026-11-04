import { z } from "zod";

const count = z.number().int().nonnegative().safe();
export const feedGapDiagnosticsSchema = z.object({
  version: z.literal(1),
  detectedAt: z.iso.datetime(),
  reason: z.enum(["history-exhausted", "pagination-unsupported", "repeated-page", "page-budget-exhausted"]),
  previousSuccessAt: z.iso.datetime().nullable(),
  previousPollAt: z.iso.datetime().nullable(),
  previousPollFailed: z.boolean(),
  pagesFetched: count.max(10), resultsFetched: count, distinctResults: count,
  anchorsExpected: count.max(5), pageSize: z.literal(100), pageBudget: z.literal(10),
  supportsPagination: z.boolean().nullable(), reportedTotal: count.nullable(),
}).strict();
export type FeedGapDiagnostics = z.infer<typeof feedGapDiagnosticsSchema>;

export function readFeedGapDiagnostics(raw: string | null | undefined): FeedGapDiagnostics | null {
  if (!raw || raw.length > 4096) return null;
  try {
    const parsed = feedGapDiagnosticsSchema.safeParse(JSON.parse(raw));
    return parsed.success ? parsed.data : null;
  } catch { return null; }
}

export const feedGapReasons: Record<FeedGapDiagnostics["reason"], string> = {
  "history-exhausted": "The provider's available history ended before any saved checkpoint was found.",
  "pagination-unsupported": "The provider does not support paging further back.",
  "repeated-page": "The provider repeated a page's leading release before a saved checkpoint was found.",
  "page-budget-exhausted": "The 10-page retrieval limit was reached before any saved checkpoint was found.",
};
