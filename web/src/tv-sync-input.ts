export function parseTvSeasonBackfill(ids: string):
  | { value: { backfillRequestIds: number[] }; error: null }
  | { value: null; error: string } {
  const parts = ids.trim().split(/[,\s]+/).filter(Boolean);
  if (!parts.length || parts.length > 5 || parts.some((part) => !/^[1-9]\d*$/.test(part) || !Number.isSafeInteger(Number(part))))
    return { value: null, error: "Enter one to five positive Seerr request IDs." };
  const backfillRequestIds = parts.map(Number);
  if (new Set(backfillRequestIds).size !== backfillRequestIds.length)
    return { value: null, error: "Each Seerr request ID must appear once." };
  return { value: { backfillRequestIds }, error: null };
}
