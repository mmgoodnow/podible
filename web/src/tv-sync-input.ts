export function parseTvCanaryBackfill(ids: string, limit: string):
  | { value: { backfillRequestIds: number[]; episodeLimit: number }; error: null }
  | { value: null; error: string } {
  const parts = ids.trim().split(/[,\s]+/).filter(Boolean);
  if (!parts.length || parts.length > 5 || parts.some((part) => !/^[1-9]\d*$/.test(part) || !Number.isSafeInteger(Number(part))))
    return { value: null, error: "Enter one to five positive Seerr request IDs." };
  const backfillRequestIds = parts.map(Number);
  if (new Set(backfillRequestIds).size !== backfillRequestIds.length)
    return { value: null, error: "Each Seerr request ID must appear once." };
  const episodeLimit = Number(limit);
  if (!/^[1-9]\d*$/.test(limit) || !Number.isSafeInteger(episodeLimit) || episodeLimit > 50)
    return { value: null, error: "Episode limit must be a whole number from 1 to 50." };
  return { value: { backfillRequestIds, episodeLimit }, error: null };
}
