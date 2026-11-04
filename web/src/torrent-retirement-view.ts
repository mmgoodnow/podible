import type { z } from "zod";
import type { retirementPreviewSchema } from "../../src/media/torrent-retirement-types";

export type RetirementPreviewItem = z.infer<typeof retirementPreviewSchema>["items"][number];
export function retirementView(items: readonly RetirementPreviewItem[]) {
  const attention: RetirementPreviewItem[] = [], unknown: RetirementPreviewItem[] = [];
  let protectedCount = 0, completeCount = 0;
  for (const item of items) {
    if (item.eligible || item.disposition === "pending") attention.push(item);
    else if (item.disposition === "protected") protectedCount++;
    else if (item.disposition === "complete") completeCount++;
    else unknown.push(item);
  }
  return { attention, unknown, protectedCount, completeCount };
}
